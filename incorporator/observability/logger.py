"""Multiplex logging architecture and wrapper subclass for Incorporator."""

import asyncio
import atexit
import json
import logging
import queue
import re
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Type, TypeVar, Union

from ..base import _UNSET, Incorporator
from ..list import IncorporatorList
from .wave import Wave  # re-exported — ``from .logger import Wave`` keeps working

TLoggedIncorporator = TypeVar("TLoggedIncorporator", bound="LoggedIncorporator")

# Global registry to prevent duplicate background threads if a class is dynamically rebuilt
_ACTIVE_LISTENERS: Dict[str, QueueListener] = {}
MAX_LOG_THREADS = 50  # Hard OS limit constraint

__all__ = [
    "LoggedIncorporator",
    "Wave",
    # The remaining public symbols are picked up automatically by ``from logger import *``
    # if any caller uses that pattern; explicit list keeps Wave + LoggedIncorporator
    # discoverable in IDE auto-import suggestions.
]


# ---------------------------------------------------------------------------
# Secret redaction for log output
# ---------------------------------------------------------------------------

# Matches `<key>=<value>` query-string-style auth params. The value group is
# replaced with a placeholder. Case-insensitive. Stops at `&`, whitespace, or
# end of string.
_REDACT_QS_PATTERN = re.compile(r"(?i)(api[_-]?key|token|secret|password|bearer|authorization)=([^&\s\"']+)")


def _redact(text: str) -> str:
    """Scrub common credential patterns from log-bound strings.

    Targets query-string auth (`?api_key=abc&token=xyz`) — the most likely
    place a secret slips into a URL that ends up in ``failed_sources`` or
    an HTTP error message. Full traceback scrubbing is intentionally out
    of scope; instance-level secrets are the developer's responsibility
    once they enter ``exc_info``.

    Returns ``text`` unchanged when no patterns match — cheap fast path
    for typical wave lines.
    """
    return _REDACT_QS_PATTERN.sub(r"\1=***REDACTED***", text)


def _route_wave_to_log(cls: Type[Any], wave: "Wave") -> None:
    """Adapter shared by :meth:`LoggedIncorporator.stream` and ``fjord``.

    - Routes waves with ``failed_sources`` to ``error.log``.
    - Routes waves with ``rows_processed > 0`` (and no failures) to ``info``.
    - Zero-row, zero-failure waves are skipped (noise).
    - Attaches the structured ``wave`` dump as a record extra so
      :class:`JSONFormatter` writes it as a top-level JSON key alongside
      ``meta`` — :meth:`LoggingMixin.get_error` callers can read
      ``record["wave"]`` directly.
    - Applies :func:`_redact` to the human-readable message *and* the
      ``failed_sources`` list inside the dumped wave. The ``Wave``
      yielded back to the caller is untouched.
    """
    dump = wave.model_dump(mode="json")
    dump["failed_sources"] = [_redact(s) for s in dump.get("failed_sources", [])]

    extra = {
        "meta": wave.log_meta(),
        "wave": dump,
        "is_api": False,
    }

    if wave.failed_sources:
        msg = f"{wave.operation} chunk {wave.chunk_index} encountered failures: {dump['failed_sources']}"
        cls_logger = cls._get_cls_logger() if hasattr(cls, "_get_cls_logger") else logging.getLogger(cls.__name__)
        if cls_logger.isEnabledFor(logging.ERROR):
            cls_logger.error(msg, extra=extra)
    elif wave.rows_processed > 0:
        msg = (
            f"{wave.operation} chunk {wave.chunk_index} complete: "
            f"{wave.rows_processed} rows in {wave.processing_time_sec:.3f}s."
        )
        cls_logger = cls._get_cls_logger() if hasattr(cls, "_get_cls_logger") else logging.getLogger(cls.__name__)
        if cls_logger.isEnabledFor(logging.INFO):
            cls_logger.info(msg, extra=extra)


def _cleanup_listeners() -> None:
    """Gracefully shuts down all background logging threads on application exit.

    Guards against listeners that were registered but never started
    (``_thread is None``) or already stopped — Python 3.11's
    ``QueueListener.stop()`` unconditionally calls ``self._thread.join()``
    and raises ``AttributeError`` in both cases.
    """
    for listener in _ACTIVE_LISTENERS.values():
        if getattr(listener, "_thread", None) is not None:
            try:
                listener.stop()
            except Exception:
                # atexit must never raise — swallow any stop-time errors
                # (e.g. listener already stopped on a parallel thread).
                pass
    _ACTIVE_LISTENERS.clear()


atexit.register(_cleanup_listeners)


def _safe_log_filename(prefix: str, suffix: str) -> str:
    """Sanitizes strings and routes all files to a dedicated 'logs/' directory."""
    # Ensure logs directory exists (Works seamlessly with Pytest monkeypatch.chdir)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    clean_prefix = re.sub(r"[^a-zA-Z0-9_-]", "_", prefix)
    return str(log_dir / f"{clean_prefix}_{suffix}")


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON Lines for easy dynamic parsing."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "level": record.levelname,
            "msg": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        if hasattr(record, "meta"):
            log_obj["meta"] = record.meta
        # Structured wave payload attached by _route_wave_to_log — surfaces as
        # a top-level JSON key so get_error() consumers can read record["wave"].
        if hasattr(record, "wave"):
            log_obj["wave"] = record.wave
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


class APIFilter(logging.Filter):
    """Ensures only API-tagged traffic reaches the api.log."""

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "is_api", False))


class StandardFilter(logging.Filter):
    """Prevents API-tagged traffic from cluttering the main error.log."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not bool(getattr(record, "is_api", False))


def setup_class_logger(cls: Type[Any]) -> None:
    """Configures JSON-formatted, non-blocking logging for a dynamic subclass."""
    cls_name = getattr(cls, "__name__", "UnknownClass")
    logger = logging.getLogger(cls_name)

    # 1. Prevent duplicate listener threads for cached classes
    if cls_name in _ACTIVE_LISTENERS:
        return

    # 2. Prevent handler stacking if the Python runtime cached the logger object internally
    if logger.handlers:
        return

    logger.setLevel(logging.DEBUG)
    formatter = JSONFormatter()

    # Log Rotation Settings: Max 5MB per file, keeping 3 backups (Max ~15MB total per log type)
    max_bytes = 5 * 1024 * 1024
    backup_count = 3

    # Setup Disk Handlers
    debug_fh = RotatingFileHandler(
        _safe_log_filename(cls_name, "debug.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    debug_fh.setLevel(logging.DEBUG)
    debug_fh.setFormatter(formatter)

    error_fh = RotatingFileHandler(
        _safe_log_filename(cls_name, "error.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_fh.setLevel(logging.INFO)
    error_fh.addFilter(StandardFilter())
    error_fh.setFormatter(formatter)

    api_fh = RotatingFileHandler(
        _safe_log_filename(cls_name, "api.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    api_fh.setLevel(logging.INFO)
    api_fh.addFilter(APIFilter())
    api_fh.setFormatter(formatter)

    # 3. Multi-Threading Queue Setup (Non-Blocking Event Loop)
    log_queue: queue.SimpleQueue[Any] = queue.SimpleQueue()
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    # Evict oldest listener BEFORE starting new one to keep thread count <= MAX_LOG_THREADS
    if len(_ACTIVE_LISTENERS) >= MAX_LOG_THREADS:
        oldest_key = next(iter(_ACTIVE_LISTENERS))
        logger.warning(
            "Max log threads (%d) reached; evicting oldest listener for %r to make room for %r.",
            MAX_LOG_THREADS,
            oldest_key,
            cls_name,
        )
        old_listener = _ACTIVE_LISTENERS.pop(oldest_key)
        # Guard against listeners whose _thread was already cleared (re-stop) —
        # Python 3.11 QueueListener.stop() raises AttributeError in that case.
        if getattr(old_listener, "_thread", None) is not None:
            try:
                old_listener.stop()
            except Exception:
                pass

    listener = QueueListener(log_queue, debug_fh, error_fh, api_fh, respect_handler_level=True)
    listener.start()

    _ACTIVE_LISTENERS[cls_name] = listener


class LoggingMixin:
    """Provides targeted logging methods and error retrieval to instances and classes."""

    @classmethod
    async def get_error(cls) -> List[Dict[str, Any]]:
        """Read every error this class has logged and return it as parsed records.

        Tails ``logs/<ClassName>_error.log`` and returns each JSON line as
        a dict.  Useful for post-run inspection of a stream/fjord daemon,
        for retry orchestrators that want to re-fan failed URLs, and for
        unit tests that assert on logged failure shape.

        Each record contains at minimum:

        - ``level``: ``"ERROR"``
        - ``msg``: human-readable message
        - ``meta``: flat ``key:"value"`` summary (class, identity, origin)
        - ``wave``: the full :class:`Wave` dump as a dict (when the error
          came from a pipeline tick — chunk index, rows, failed sources,
          processing time, etc.)
        - ``timestamp`` and other standard ``logging`` fields

        Safe to call when no errors have been logged yet — returns an
        empty list rather than raising.  Disk read runs in a worker
        thread via :func:`asyncio.to_thread` so the event loop is never
        blocked.
        """

        def _read_disk() -> List[Dict[str, Any]]:
            filename = _safe_log_filename(cls.__name__, "error.log")
            path = Path(filename).resolve()

            if not path.is_file():
                return []

            errors: List[Dict[str, Any]] = []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                errors.append(json.loads(line))
                            except json.JSONDecodeError:
                                pass
            except OSError:
                pass  # Treat disk read failures as "no errors yet"
            return errors

        return await asyncio.to_thread(_read_disk)

    # --- CLASS-LEVEL LOGGING (For Factory Methods like export) ---

    @classmethod
    def _get_cls_logger(cls) -> logging.Logger:
        return logging.getLogger(cls.__name__)

    @classmethod
    def log_cls_info(cls, msg: str) -> None:
        """Write an INFO-level message to this class's ``api.log`` file.

        Use from inside :classmethod:`classmethod` factories (where no
        ``self`` is available) to record lifecycle events.  The record
        carries a ``class:"<Name>"`` meta field for later retrieval.
        Silently noops when the class's logger isn't configured for INFO
        — safe to sprinkle through code paths that might run before
        ``enable_logging=True`` ever fires.
        """
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.INFO):
            meta_str = f'class:"{cls.__name__}"'
            logger.info(msg, extra={"meta": meta_str, "is_api": False})

    @classmethod
    def log_cls_error(cls, msg: str, exc_info: bool = False) -> None:
        """Write an ERROR-level message to this class's ``error.log`` file.

        Class-level counterpart to :meth:`log_error` — use from inside
        ``@classmethod`` factories to record failures without a ``self``.
        Pass ``exc_info=True`` to attach the active exception's traceback
        (suitable for use inside ``except`` blocks).  Retrievable via
        :meth:`get_error`.
        """
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.ERROR):
            meta_str = f'class:"{cls.__name__}"'
            logger.error(msg, exc_info=exc_info, extra={"meta": meta_str, "is_api": False})

    # --- INSTANCE-LEVEL LOGGING ---

    def log_meta(self) -> str:
        """Build the ``meta`` string attached to every log record this instance writes.

        Returns a flat ``key:"value"`` summary containing the class name,
        primary key (``inc_code``), display name (``inc_name``), and origin
        URL/file.  Surfaces under ``record["meta"]`` in the JSON log lines
        so :meth:`get_error` consumers can identify which instance a
        message came from without scanning the whole record.

        Override on a subclass if you want extra identity fields in the
        meta string — keep the ``key:"value"`` shape so existing log
        consumers stay compatible.
        """
        cls = self.__class__
        cls_name = getattr(cls, "__name__", "UnknownClass")
        return (
            f'class:"{cls_name}", '
            f'inc_code:"{getattr(self, "inc_code", None)}", inc_name:"{getattr(self, "inc_name", None)}", '
            f'file:"{getattr(cls, "inc_file", None)}", url:"{getattr(cls, "inc_url", None)}"'
        )

    def _get_logger(self) -> logging.Logger:
        return logging.getLogger(self.__class__.__name__)

    def log_debug(self, msg: str) -> None:
        """Write a DEBUG-level message to this class's ``debug.log`` file.

        Use for verbose tracing that you don't want in production but
        still want available when something goes wrong.  The record
        carries :meth:`log_meta` so you can grep by ``inc_code`` later.
        Silently noops when DEBUG isn't enabled — cheap to leave in.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_info(self, msg: str) -> None:
        """Write an INFO-level message to this class's ``api.log`` file.

        Default channel for "things happened" messages tied to a specific
        instance.  Pairs with :meth:`log_error` for the failure case and
        :meth:`log_api` for outbound HTTP request tracing.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_error(self, msg: str, exc_info: bool = False) -> None:
        """Write an ERROR-level message to this class's ``error.log`` file.

        Use inside ``except`` blocks with ``exc_info=True`` to attach the
        active traceback to the record.  Retrievable later via
        :meth:`get_error`, which returns this and every other error the
        class has logged as a list of parsed records.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.ERROR):
            logger.error(msg, exc_info=exc_info, extra={"meta": self.log_meta(), "is_api": False})

    def log_api(self, msg: str) -> None:
        """Write an INFO-level message tagged as an outbound HTTP event.

        The ``is_api: True`` flag on the record lets log handlers route
        request/response traces to a separate sink from generic info
        messages — useful when you want a clean audit trail of every
        outbound call this instance made without the surrounding lifecycle
        noise.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": True})


class LoggedIncorporator(LoggingMixin, Incorporator):
    """Drop-in replacement for :class:`Incorporator` that writes structured logs.

    Use ``LoggedIncorporator`` instead of ``Incorporator`` whenever you want
    production observability — every pipeline call writes JSON-line records
    to disk under ``logs/<ClassName>_{api,error,debug}.log``, queried
    afterwards via :meth:`get_error`.

    Logging is **opt-in per call** via ``enable_logging=True``::

        class Launch(LoggedIncorporator):
            pass

        async for wave in Launch.stream(
            incorp_params={"inc_url": "..."},
            enable_logging=True,
        ):
            handle(wave)

        # Later, in any process with the same logs/ dir:
        failures = await Launch.get_error()

    All public verbs (:meth:`incorp`, :meth:`refresh`, :meth:`export`,
    :meth:`stream`, :meth:`fjord`) accept the same kwargs as their
    :class:`Incorporator` counterparts plus ``enable_logging``.  Disk I/O
    runs through a ``QueueHandler`` background thread so the event loop
    never blocks on log writes.
    """

    @classmethod
    async def incorp(
        cls: Type[TLoggedIncorporator], *args: Any, enable_logging: bool = False, **kwargs: Any
    ) -> Union[TLoggedIncorporator, IncorporatorList[TLoggedIncorporator]]:
        """Fetch + parse + register, with optional per-class log file writing.

        Identical to :meth:`Incorporator.incorp` (all kwargs forwarded
        unchanged) except for one extra option:

        Args:
            enable_logging: When ``True``, the call also wires up a
                ``QueueHandler``-backed logger for this class.  Subsequent
                instance-level log calls (``self.log_info(...)``, etc.)
                and any failures during the fetch land in
                ``logs/<ClassName>_*.log``.  Off by default for parity
                with :class:`Incorporator`.

        Returns:
            Same return shape as :meth:`Incorporator.incorp` — a single
            instance for a single record, or an :class:`IncorporatorList`
            for multi-record sources.
        """

        if enable_logging:
            setup_class_logger(cls)

        result = await super().incorp(*args, **kwargs)

        if enable_logging:
            if isinstance(result, list) and result:
                setup_class_logger(result[0].__class__)
            elif not isinstance(result, list):
                setup_class_logger(result.__class__)

        return result

    @classmethod
    async def refresh(
        cls: Type[TLoggedIncorporator], *args: Any, enable_logging: bool = False, **kwargs: Any
    ) -> Union[TLoggedIncorporator, IncorporatorList[TLoggedIncorporator]]:
        """Re-fetch live data into existing instances, with optional log writing.

        Identical to :meth:`Incorporator.refresh` (all kwargs forwarded
        unchanged) except for the extra ``enable_logging`` option, which
        wires up the per-class disk logger so subsequent
        ``self.log_info(...)`` / ``self.log_error(...)`` calls land in
        ``logs/<ClassName>_*.log``.
        """
        result = await super().refresh(*args, **kwargs)

        if enable_logging:
            if isinstance(result, list):
                if result:
                    setup_class_logger(result[0].__class__)
            else:
                setup_class_logger(result.__class__)

        return result

    @classmethod
    async def export(cls: Type[TLoggedIncorporator], *, enable_logging: bool = False, **kwargs: Any) -> None:
        """Serialise instances to a file, with optional log-bracketed lifecycle.

        Identical to :meth:`Incorporator.export` (all kwargs forwarded
        unchanged).  When ``enable_logging=True`` is also passed, the
        export is bracketed by ``"Initiating export..."`` and
        ``"Export process completed successfully."`` info-log entries on
        ``logs/<ClassName>_api.log``, and any raised exception is logged
        to ``logs/<ClassName>_error.log`` with the traceback attached
        before re-raising.
        """
        if enable_logging:
            setup_class_logger(cls)
            cls.log_cls_info(f"Initiating export process with kwargs={kwargs}")

        try:
            await super().export(**kwargs)
            if enable_logging:
                cls.log_cls_info("Export process completed successfully.")
        except Exception as e:
            if enable_logging:
                cls.log_cls_error(f"Export process failed: {str(e)}", exc_info=True)
            raise

    @classmethod
    async def stream(
        cls: Type[TLoggedIncorporator],
        incorp_params: Dict[str, Any],
        refresh_params: Optional[Dict[str, Any]] = _UNSET,
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Any] = None,
        outflow: Optional[Any] = None,
        enable_logging: bool = False,
    ) -> AsyncGenerator[Wave, None]:
        """Long-running pipeline, with each tick mirrored to disk on opt-in.

        Identical to :meth:`Incorporator.stream` (every kwarg forwarded
        unchanged).  When ``enable_logging=True`` is also passed:

        - Every :class:`Wave` yielded by the engine is also written to
          ``logs/<ClassName>_api.log`` (successful chunks) or
          ``logs/<ClassName>_error.log`` (chunks with ``failed_sources``).
          Each log record carries the full Pydantic dump under the
          ``wave`` key, accessible later via :meth:`get_error`.
        - Fatal pipeline failures land in ``logs/<ClassName>_error.log``
          with the traceback before re-raising to the caller.

        The Wave itself is yielded to the caller **before** any disk write
        completes — the QueueHandler thread handles the write
        asynchronously, so the async-for loop is never blocked on I/O.
        """
        if enable_logging:
            setup_class_logger(cls)
            cls.log_cls_info("Initiating autonomous stream orchestration.")

        try:
            async for wave in super().stream(
                incorp_params=incorp_params,
                refresh_params=refresh_params,
                export_params=export_params,
                poll_interval=poll_interval,
                stateful_polling=stateful_polling,
                refresh_interval=refresh_interval,
                export_interval=export_interval,
                inflow=inflow,
                outflow=outflow,
            ):
                if enable_logging:
                    _route_wave_to_log(cls, wave)

                # Yield downstream to the caller natively
                yield wave

            if enable_logging:
                cls.log_cls_info("Stream process completed gracefully.")

        except Exception as e:
            # Catch catastrophic framework failures outside the base loop
            if enable_logging:
                cls.log_cls_error(f"Fatal Stream Pipeline Error: {str(e)}", exc_info=True)
            raise

    @classmethod
    async def fjord(
        cls,
        stream_params: List[Dict[str, Any]],
        outflow: Any,
        export_params: Dict[str, Any],
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Any] = None,
        enable_logging: bool = False,
    ) -> AsyncGenerator[Wave, None]:
        """Multi-source pipeline, with each tick mirrored to disk on opt-in.

        Identical to :meth:`Incorporator.fjord` (every kwarg forwarded
        unchanged).  When ``enable_logging=True`` is also passed:

        - Every :class:`Wave` yielded by the fjord engine — including
          per-source ``"fjord_refresh:<Class>"`` and
          ``"outflow:<DynamicClass>"`` operations — is mirrored to
          ``logs/<ClassName>_api.log`` (throughput) or
          ``logs/<ClassName>_error.log`` (failures).  All waves log
          under *this* class regardless of which source produced them,
          so :meth:`get_error` returns the full pipeline's error history
          from one call.
        - Fatal failures land in the error log with traceback attached
          before re-raising.
        """
        if enable_logging:
            setup_class_logger(cls)
            cls.log_cls_info("Initiating fjord orchestration.")

        try:
            async for wave in super().fjord(
                stream_params=stream_params,
                outflow=outflow,
                export_params=export_params,
                refresh_interval=refresh_interval,
                export_interval=export_interval,
                inflow=inflow,
            ):
                if enable_logging:
                    _route_wave_to_log(cls, wave)

                yield wave

            if enable_logging:
                cls.log_cls_info("Fjord process completed gracefully.")

        except Exception as e:
            if enable_logging:
                cls.log_cls_error(f"Fatal Fjord Pipeline Error: {str(e)}", exc_info=True)
            raise
