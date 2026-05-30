"""Multiplex logging architecture and wrapper subclass for Incorporator."""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import queue
import re
from collections.abc import AsyncGenerator
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from ..base import _UNSET, Incorporator
from ..list import IncorporatorList
from .wave import Wave  # re-exported — ``from .logger import Wave`` keeps working

if TYPE_CHECKING:
    from ..rejects import RejectEntry
    from .tideweaver.tide import Tide

TLoggedIncorporator = TypeVar("TLoggedIncorporator", bound="LoggedIncorporator")

# Global registry to prevent duplicate background threads if a class is dynamically rebuilt
_ACTIVE_LISTENERS: dict[str, QueueListener] = {}
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


def _emit_payload(
    logger_name: str,
    level: int,
    msg: str,
    payload_key: str,
    payload: dict[str, Any],
    meta: str,
    *,
    is_api: bool = False,
    is_tide: bool = False,
) -> None:
    """Shared emission tail for _route_wave / _route_tide / _route_reject.

    Builds the ``extra`` dict, checks ``isEnabledFor``, and emits the record.
    Callers supply the resolved ``logger_name`` so this function has no
    dependency on any class reference.

    Args:
        logger_name: Name passed to :func:`logging.getLogger`.
        level: ``logging`` level constant (e.g. ``logging.ERROR``).
        msg: Human-readable log message.
        payload_key: Top-level JSON key for the structured payload (``"wave"``,
            ``"tide"``, or ``"reject"``).
        payload: The serialised model dict to attach under ``payload_key``.
        meta: Flat ``key:"value"`` summary string for the ``meta`` field.
        is_api: When ``True``, routes the record to ``api.log`` via
            :class:`APIFilter`.
        is_tide: When ``True``, adds ``is_tide=True`` so :class:`TideFilter`
            routes the record to ``tide.log``.
    """
    extra: dict[str, Any] = {"meta": meta, payload_key: payload, "is_api": is_api}
    if is_tide:
        extra["is_tide"] = True
    logger = logging.getLogger(logger_name)
    if logger.isEnabledFor(level):
        logger.log(level, msg, extra=extra)


def _route_wave_to_log(cls_name: str, wave: Wave) -> None:
    """Route a single Wave to the appropriate log level based on its outcome.

    Shared adapter used by :meth:`LoggedIncorporator.stream` and ``fjord``. The
    routing rules:

    - Waves with ``failed_sources`` → ``error.log``.
    - Successful waves with ``rows_processed > 0`` → ``info``.
    - Zero-row, zero-failure waves are skipped (noise).

    Attaches the structured ``wave`` dump as a record extra so
    :class:`JSONFormatter` writes it as a top-level JSON key alongside ``meta``;
    :meth:`LoggingMixin.get_error` callers can read ``record["wave"]`` directly.
    Applies :func:`_redact` to the human-readable message *and* the
    ``failed_sources`` list inside the dumped wave. The ``Wave`` yielded back to
    the caller is untouched.

    Args:
        cls_name: Logger name — ``cls.__name__`` from the calling verb wrapper.
        wave: The :class:`Wave` record yielded by the pipeline.
    """
    dump = wave.model_dump(mode="json")
    dump["failed_sources"] = [_redact(s) for s in dump.get("failed_sources", [])]

    if wave.failed_sources:
        msg = f"{wave.operation} chunk {wave.chunk_index} encountered failures: {dump['failed_sources']}"
        _emit_payload(cls_name, logging.ERROR, msg, "wave", dump, wave.log_meta())
    elif wave.rows_processed > 0:
        msg = (
            f"{wave.operation} chunk {wave.chunk_index} complete: "
            f"{wave.rows_processed} rows in {wave.processing_time_sec:.3f}s."
        )
        _emit_payload(cls_name, logging.INFO, msg, "wave", dump, wave.log_meta())


def _route_tide_to_log(cls_name: str, tide: Tide) -> None:
    """Route one Tide record to info/error/debug based on its outcomes.

    Mirrors the routing shape of :func:`_route_wave_to_log` but operates on
    scheduler-pass records rather than chunk waves.  Routing rules:

    - Passes with ``canal_rejects_added > 0`` or error-class skip reasons
      (``"surge_halted"``, ``"skip_ahead"``) → ``error``.
    - Passes where at least one current fired → ``info``.
    - No-op passes (nothing fired, no errors) → ``debug``.

    All tide records also carry ``is_tide=True`` so :class:`TideFilter` routes
    them to ``tide.log`` for single-file readback by :meth:`LoggedTideweaver.get_tides`.
    Tide records continue to flow into ``debug.log`` and (when fired/errored)
    ``error.log`` — ``debug.log`` remains the superset.

    Args:
        cls_name: Logger name — typically ``LoggedTideweaver._logger_name``.
        tide: The :class:`Tide` record yielded by :meth:`Tideweaver.run`.
    """
    dump = tide.model_dump(mode="json")
    meta = tide.log_meta()

    has_error_skips = any(reason in ("surge_halted", "skip_ahead") for _, reason in tide.skipped)

    if tide.canal_rejects_added > 0 or has_error_skips:
        error_reasons = [reason for _, reason in tide.skipped if reason in ("surge_halted", "skip_ahead")]
        msg = f"tide {tide.tide_number}: {tide.canal_rejects_added} canal reject(s), skipped reasons {error_reasons}"
        _emit_payload(cls_name, logging.ERROR, msg, "tide", dump, meta, is_tide=True)
    elif len(tide.fired) > 0:
        msg = (
            f"tide {tide.tide_number}: fired {len(tide.fired)}, skipped {len(tide.skipped)}, "
            f"rejects {tide.canal_rejects_added}, duration {tide.duration_sec:.3f}s"
        )
        _emit_payload(cls_name, logging.INFO, msg, "tide", dump, meta, is_tide=True)
    else:
        msg = f"tide {tide.tide_number}: no-op pass (nothing fired), duration {tide.duration_sec:.3f}s"
        _emit_payload(cls_name, logging.DEBUG, msg, "tide", dump, meta, is_tide=True)


def _route_reject_to_log(cls_name: str, reject: RejectEntry) -> None:
    """Route one RejectEntry to error log with structured edge and HTTP metadata.

    Always logs at ERROR level — every :class:`RejectEntry` represents a
    scheduler-level skip or HTTP failure that the caller's retry logic
    should be able to observe.  The ``reject`` dict is attached as a
    top-level JSON key so :meth:`LoggingMixin.get_error` consumers can
    inspect the full structured record without parsing the message string.

    Args:
        cls_name: Logger name — typically ``LoggedTideweaver._logger_name``.
        reject: The :class:`RejectEntry` to route.
    """
    meta = (
        f'class:"{cls_name}", source:"{reject.source}", error_kind:"{reject.error_kind}", '
        f'from:"{reject.from_name}", to:"{reject.to_name}", host:"{reject.host}", '
        f"status_code:{reject.status_code}"
    )
    maybe_edge = f" ({reject.from_name}->{reject.to_name})" if reject.from_name else ""
    maybe_status = f" [HTTP {reject.status_code}]" if reject.status_code else ""
    msg = f"{reject.error_kind}: {reject.source}{maybe_edge}{maybe_status}"
    _emit_payload(cls_name, logging.ERROR, msg, "reject", reject.model_dump(mode="json"), meta)


def _cleanup_listeners() -> None:
    """Gracefully shuts down all background logging threads on application exit.

    Guards against listeners that were registered but never started
    (``_thread is None``) or already stopped — Python 3.11's
    ``QueueListener.stop()`` unconditionally calls ``self._thread.join()``
    and raises ``AttributeError`` in both cases.
    """
    for listener in _ACTIVE_LISTENERS.values():
        if getattr(listener, "_thread", None) is not None:
            # atexit must never raise — swallow any stop-time errors (e.g.
            # listener already stopped on a parallel thread).
            try:
                listener.stop()
            except Exception:  # noqa: S110, BLE001 — see comment above
                pass
    _ACTIVE_LISTENERS.clear()


atexit.register(_cleanup_listeners)


def _safe_log_filename(prefix: str, suffix: str) -> str:
    """Sanitises class names and routes all log files to a dedicated logs directory.

    Resolution order for the log directory:

    1. ``INCORPORATOR_LOG_DIR`` environment variable — preferred for
       container deployments (e.g. ``/var/log/incorporator``) and any
       caller whose current working directory isn't a useful log target.
    2. ``./logs`` relative to the process CWD — back-compat default for
       local development.

    The directory is created lazily on first call.
    """
    raw = os.environ.get("INCORPORATOR_LOG_DIR")
    log_dir = Path(raw) if raw else Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    clean_prefix = re.sub(r"[^a-zA-Z0-9_-]", "_", prefix)
    return str(log_dir / f"{clean_prefix}_{suffix}")


def _read_filtered(filename: str, key: str) -> list[dict[str, Any]]:
    """Read every JSONL record from ``filename`` that contains ``key`` as a top-level key.

    Skips lines that fail JSON decoding or lack the requested key.  Safe to
    call when ``filename`` does not yet exist — returns ``[]`` rather than
    raising.  OSError (e.g. permission denied) is silently swallowed so
    callers can treat disk-read failures as "no records yet".

    Args:
        filename: Absolute or CWD-relative path to a JSONL log file.
        key: Top-level JSON key that must be present in a record for it
            to be included in the result.

    Returns:
        List of dicts — every record that contains ``key``, in file order.
    """
    path = Path(filename).resolve()
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        rec = json.loads(line)
                        if key in rec:
                            records.append(rec)
                    except json.JSONDecodeError:
                        pass
    except OSError:
        pass
    return records


class JSONFormatter(logging.Formatter):
    """Emit one JSON-line record per log call for grep, aggregators, and structured retrieval via :meth:`get_rejects`.

    `jq`, log aggregators, and :meth:`LoggingMixin.get_error` all read
    these records back without a regex.  Wired automatically onto every
    rotating handler set up by :func:`setup_class_logger` — DXs don't
    instantiate this directly.  Each record is a complete dict with
    ``level``, ``msg``, ``time``, plus optional ``meta`` / ``wave`` /
    ``exc_info`` keys depending on what the caller attached as ``extra``.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
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
        if hasattr(record, "tide"):
            log_obj["tide"] = record.tide
        if hasattr(record, "reject"):
            log_obj["reject"] = record.reject
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


class APIFilter(logging.Filter):
    """Routes :meth:`LoggingMixin.log_api` records to ``api.log`` and blocks them from ``error.log``.

    Half of the API/standard split — paired with :class:`StandardFilter`
    to keep outbound HTTP audit traces in their own file separate from
    generic lifecycle info.  Activated by the ``is_api: True`` extra
    that :meth:`LoggingMixin.log_api` attaches to every record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "is_api", False))


class StandardFilter(logging.Filter):
    """Blocks API-tagged records from ``error.log`` so the audit-trail stream stays out of the generic error stream.

    The flip side of :class:`APIFilter` — together they enforce the
    intent split: anything carrying ``is_api: True`` lives only in
    ``api.log``; everything else flows to ``error.log`` (filtered
    further by the file handler's level).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return not bool(getattr(record, "is_api", False))


class TideFilter(logging.Filter):
    """Routes ``is_tide=True`` records to ``tide.log`` for single-file :meth:`LoggedTideweaver.get_tides` reads.

    Mirrors the :class:`APIFilter` / :class:`StandardFilter` pattern.
    Activated by the ``is_tide: True`` extra that :func:`_route_tide_to_log`
    attaches to every tide record.  Tide records continue to flow into
    ``debug.log`` (superset) and ``error.log`` (fired/errored tides) —
    this filter only selects them for an additional dedicated file that
    eliminates the cross-file dedup loop in ``get_tides()``.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "is_tide", False))


def setup_class_logger(cls: str | type[Any]) -> None:
    """Configures JSON-formatted, non-blocking logging for a dynamic subclass or named logger.

    Accepts either a class (the typical case — ``setup_class_logger(MyClass)``)
    or a plain string name (used by :class:`LoggedTideweaver` to set up a logger
    with a caller-chosen name rather than a class ``__name__``).

    Args:
        cls: Either a class whose ``__name__`` is used as the logger key, or a
            plain string to use directly.  The string path is used by
            :class:`~incorporator.observability.tideweaver.LoggedTideweaver`.
    """
    if isinstance(cls, str):
        cls_name = cls
    else:
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

    tide_fh = RotatingFileHandler(
        _safe_log_filename(cls_name, "tide.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    tide_fh.setLevel(logging.DEBUG)
    tide_fh.addFilter(TideFilter())
    tide_fh.setFormatter(formatter)

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
        # Eviction must not abort caller, so swallow any stop-time error.
        if getattr(old_listener, "_thread", None) is not None:
            try:
                old_listener.stop()
            except Exception:  # noqa: S110, BLE001 — see comment above
                pass

    listener = QueueListener(log_queue, debug_fh, error_fh, api_fh, tide_fh, respect_handler_level=True)
    listener.start()

    _ACTIVE_LISTENERS[cls_name] = listener


class LoggingMixin:
    """Escape-hatch mixin that adds structured per-instance and per-class logging methods to any Incorporator subclass.

    Most DXs reach for :class:`LoggedIncorporator` instead — it already
    blends this mixin with :class:`Incorporator` and wires the verb
    wrappers.  Subclass ``LoggingMixin`` directly only when you want
    ``log_debug`` / ``log_info`` / ``log_error`` / ``log_api`` /
    ``log_cls_info`` / ``log_cls_error`` plus :meth:`get_error`
    retrieval on a custom Incorporator subclass without inheriting the
    full verb-wrapper machinery.

    Example::

        from incorporator import Incorporator
        from incorporator.observability.logger import (
            LoggingMixin, setup_class_logger,
        )

        class Audited(LoggingMixin, Incorporator):
            pass

        setup_class_logger(Audited)
        instance.log_info("backtest prep started")
        failures = await Audited.get_error()

    All methods silently noop when the class hasn't been wired through
    :func:`setup_class_logger` yet, so calls are cheap to leave in
    code paths that may run before logging is enabled.  Records land in
    rotating JSONL files at ``logs/<ClassName>_{api,error,debug}.log``
    via a ``QueueHandler``-backed background thread, so the event loop
    is never blocked on disk I/O.
    """

    @classmethod
    async def get_error(cls) -> list[dict[str, Any]]:
        """Pull every error this class has logged for a retry pass over rejects after an overnight pipeline finishes.

        Reach for ``get_error()`` when a stream or fjord daemon has
        drained against a flaky source and you want a structured list
        of failures to feed back into a retry loop, post-mortem
        inspection, or a unit test asserting on logged failure shape.

        Example::

            errors = await Launch.get_error()
            for rec in errors:
                wave = rec.get("wave", {})
                for url in wave.get("failed_sources", []):
                    await retry_queue.put(url)

        Each record contains at minimum:

        - ``level``: ``"ERROR"``
        - ``msg``: human-readable message
        - ``meta``: flat ``key:"value"`` summary (class, identity, origin)
        - ``wave``: the full :class:`Wave` dump as a dict (when the error
          came from a pipeline wave — chunk index, rows, failed sources,
          processing time, etc.)
        - ``time`` and other standard ``logging`` fields

        Safe to call when no errors have been logged yet — returns an
        empty list rather than raising.  Tails
        ``logs/<ClassName>_error.log`` in a worker thread via
        :func:`asyncio.to_thread` so the event loop is never blocked.
        """

        def _read_disk() -> list[dict[str, Any]]:
            filename = _safe_log_filename(cls.__name__, "error.log")
            path = Path(filename).resolve()

            if not path.is_file():
                return []

            errors: list[dict[str, Any]] = []
            try:
                with open(path, encoding="utf-8") as f:
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

    @classmethod
    async def get_rejects(cls) -> list[dict[str, Any]]:
        """Pull every reject this class has logged from ``logs/<ClassName>_error.log``.

        Reach for ``get_rejects()`` after an overnight pipeline to iterate
        over every HTTP failure, fjord seed error, or canal skip that was
        serialised to the error log as a structured ``reject`` record.

        Example::

            rejects = await Launch.get_rejects()
            for rec in rejects:
                reject = rec["reject"]
                print(reject["source"], reject["error_kind"])

        Records contain a top-level ``"reject"`` key whose value matches the
        :class:`~incorporator.rejects.RejectEntry` model dump.  The full
        error log is **not** returned — only records that carry a ``"reject"``
        key (as written by :func:`_route_reject_to_log`).  Safe to call when
        no rejects have been logged yet — returns an empty list rather than
        raising.  Tails ``logs/<ClassName>_error.log`` in a worker thread via
        :func:`asyncio.to_thread` so the event loop is never blocked.
        """
        filename = _safe_log_filename(cls.__name__, "error.log")
        return await asyncio.to_thread(_read_filtered, filename, "reject")

    # --- CLASS-LEVEL LOGGING (For Factory Methods like export) ---

    @classmethod
    def _get_cls_logger(cls) -> logging.Logger:
        return logging.getLogger(cls.__name__)

    @classmethod
    def log_cls_info(cls, msg: str) -> None:
        """Record a lifecycle event from inside a ``@classmethod`` factory where ``self`` isn't available.

        Rare in user code; common in framework helpers like
        :meth:`LoggedIncorporator.export` that bracket a pipeline run
        with ``"Initiating ..."`` / ``"... completed"`` entries before
        any instance exists.

        Example::

            @classmethod
            async def my_factory(cls):
                cls.log_cls_info("Factory run starting")

        The record lands in ``logs/<ClassName>_api.log`` carrying a
        ``class:"<Name>"`` meta field.  Silently noops when the class's
        logger isn't configured for INFO — safe to sprinkle through
        code paths that might run before ``enable_logging=True`` ever
        fires.
        """
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.INFO):
            meta_str = f'class:"{cls.__name__}"'
            logger.info(msg, extra={"meta": meta_str, "is_api": False})

    @classmethod
    def log_cls_error(cls, msg: str, exc_info: bool = False) -> None:
        """Record a failure from inside a ``@classmethod`` factory.

        Use this from any class method where ``self`` isn't available,
        optionally attaching the active traceback via ``exc_info=True``.
        Class-level counterpart to :meth:`log_error` — use inside a
        factory's ``except`` block to capture failures before an
        instance has been constructed.  Retrievable later via
        :meth:`get_error`.

        Example::

            try:
                await super().export(**kwargs)
            except Exception as e:
                cls.log_cls_error(f"Export failed: {e}", exc_info=True)
                raise

        The record lands in ``logs/<ClassName>_error.log`` carrying a
        ``class:"<Name>"`` meta field and (when ``exc_info=True``) the
        formatted traceback under the ``exc_info`` key.  Silently noops
        when the class's logger isn't configured for ERROR.
        """
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.ERROR):
            meta_str = f'class:"{cls.__name__}"'
            logger.error(msg, exc_info=exc_info, extra={"meta": meta_str, "is_api": False})

    # --- INSTANCE-LEVEL LOGGING ---

    def log_meta(self) -> str:
        """Stamp a wave or custom log call with this instance's identity.

        Call from inside an ``outflow(state)`` or other hot-path callback
        to attach the per-instance metadata to a custom record.
        Returns the flat ``key:"value"`` summary that every
        instance-level ``log_*`` method auto-attaches to its records —
        useful when you want to surface that same metadata on a record
        you're emitting through a different channel (e.g. inside a
        custom ``outflow`` that writes its own log line).

        Example::

            async def outflow(state):
                row = next(iter(state.values()))
                row.log_info(f"Outflow stamping {row.log_meta()}")

        The string contains the class name, primary key (``inc_code``),
        display name (``inc_name``), and origin URL/file.  Surfaces
        under ``record["meta"]`` in the JSON log lines so
        :meth:`get_error` consumers can identify which instance a
        message came from without scanning the whole record.  Override
        on a subclass to add extra identity fields — keep the
        ``key:"value"`` shape so existing log consumers stay
        compatible.
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
        """Verbose per-instance tracing — keep on in dev, off in prod.

        Doubles as a post-mortem trail when something later goes wrong.
        Reach for ``self.log_debug(...)`` to leave breadcrumbs through
        a parser or a custom ``outflow`` so that, weeks later, a
        production failure can be replayed by tailing
        ``logs/<ClassName>_debug.log``.

        Example::

            self.log_debug(f"Parsed {len(rows)} rows from {self.inc_url}")

        The record carries :meth:`log_meta` so you can grep by
        ``inc_code`` later.  Silently noops when DEBUG isn't enabled —
        cheap to leave in.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_info(self, msg: str) -> None:
        """Mark per-instance lifecycle events worth keeping in the log.

        Examples: fetch started, 100 rows parsed, daemon resumed after a pause.

        The default channel for "things happened" messages tied to a
        specific instance.  Pairs with :meth:`log_error` for the failure
        case and :meth:`log_api` for outbound HTTP request tracing.

        Example::

            self.log_info(f"Fetched {len(rows)} rows for {self.inc_code}")

        The record lands in ``logs/<ClassName>_api.log`` with
        :meth:`log_meta` attached.  Silently noops when INFO isn't
        enabled.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_error(self, msg: str, exc_info: bool = False) -> None:
        """Capture caught exceptions and recoverable failures on an instance.

        A retry loop can pick them up via :meth:`get_error` later.
        Reach for ``self.log_error(..., exc_info=True)`` inside
        ``except`` blocks to attach the active traceback to the record.
        Retrievable later via :meth:`get_error`, which returns this and
        every other error the class has logged as a list of parsed
        records suitable for a retry loop fed from rejects.

        Example::

            try:
                await self._fetch_chunk(url)
            except httpx.HTTPError as e:
                self.log_error(f"Chunk fetch failed: {e}", exc_info=True)

        The record lands in ``logs/<ClassName>_error.log`` with
        :meth:`log_meta` and (when ``exc_info=True``) the formatted
        traceback under the ``exc_info`` key.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.ERROR):
            logger.error(msg, exc_info=exc_info, extra={"meta": self.log_meta(), "is_api": False})

    def log_api(self, msg: str) -> None:
        """Maintain a clean outbound HTTP request/response audit trail separate from generic lifecycle info.

        Reach for ``self.log_api(...)`` when you want a grep-able
        record of every outbound call this instance made without the
        surrounding lifecycle noise — handy when a flaky upstream
        forces you to reconstruct a request sequence after the fact.

        Example::

            self.log_api(f"GET {url} -> {response.status_code}")

        The ``is_api: True`` flag on the record routes it to
        ``logs/<ClassName>_api.log`` via :class:`APIFilter` and keeps
        it out of ``error.log`` via :class:`StandardFilter`.
        """
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": True})


class LoggedIncorporator(LoggingMixin, Incorporator):
    """Drop-in for :class:`Incorporator` with structured JSON-line logs.

    Swap in when an overnight pipeline needs records you can grep, ship to
    an aggregator, or feed into a retry loop fed from rejects.  Subclass
    ``LoggedIncorporator`` exactly like ``Incorporator``, then
    pass ``enable_logging=True`` on any verb call to wire up rotating
    JSONL files at ``logs/<ClassName>_{api,error,debug}.log``.  Every
    wave, every caught exception, and every ``self.log_*`` call from
    inside your code lands on disk for post-mortem inspection or
    automated retry orchestration.

    Example::

        from incorporator import LoggedIncorporator

        class Launch(LoggedIncorporator):
            pass

        async for wave in Launch.stream(
            incorp_params={"inc_url": "https://api.example.com/launches"},
            enable_logging=True,
        ):
            handle(wave)

        # Later, in any process with the same logs/ dir:
        failures = await Launch.get_error()
        for rec in failures:
            await retry_queue.put(rec["wave"]["failed_sources"])

    Public verbs you'll reach for:

    - :meth:`incorp` / :meth:`refresh` / :meth:`export` — one-shot
      verbs with optional bracketed lifecycle logging.
    - :meth:`stream` / :meth:`fjord` — long-running daemons that
      mirror every yielded :class:`Wave` to disk.
    - :meth:`get_error` — replay logged failures as parsed records.

    Logging is **opt-in per call** via ``enable_logging=True`` —
    default-off keeps wire-compatible parity with :class:`Incorporator`.
    Disk I/O runs through a ``QueueHandler``-backed background thread
    so the async event loop never blocks on log writes, and rotating
    handlers cap each file at ~5 MB × 3 backups.
    """

    @classmethod
    async def incorp(
        cls: type[TLoggedIncorporator], *args: Any, enable_logging: bool = False, **kwargs: Any
    ) -> TLoggedIncorporator | IncorporatorList[TLoggedIncorporator]:
        """Production-observable variant of :meth:`Incorporator.incorp`.

        Fetch + parse + register, with an ``enable_logging=True`` opt-in for
        JSON-line logs to disk.

        Reach for this wrapper when the very first fetch into a fresh
        subclass already matters for your audit trail — typically the
        cold-boot leg of a stream or fjord daemon, or a one-shot pull
        that has to survive a post-mortem.

        Example::

            class Launch(LoggedIncorporator):
                pass

            launches = await Launch.incorp(
                "https://api.example.com/launches",
                inc_code="id",
                enable_logging=True,
            )

        Args:
            enable_logging: When ``True``, wires up the per-class
                ``QueueHandler`` logger so subsequent
                ``self.log_info(...)`` calls and any failures during
                the fetch land in ``logs/<ClassName>_*.log``.  Off by
                default for parity with :class:`Incorporator`.
            *args: Forwarded to :meth:`Incorporator.incorp`.
            **kwargs: Forwarded to :meth:`Incorporator.incorp`.

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
        cls: type[TLoggedIncorporator], *args: Any, enable_logging: bool = False, **kwargs: Any
    ) -> TLoggedIncorporator | IncorporatorList[TLoggedIncorporator]:
        """Production-observable variant of :meth:`Incorporator.refresh`.

        Re-fetch live data into existing instances, with an
        ``enable_logging=True`` opt-in for JSON-line logs to disk.

        Reach for this wrapper on a manual one-shot mark-to-market
        re-fetch when you want the refresh leg recorded — useful when
        a scheduled cron runs ``refresh()`` and you need to prove
        afterwards what data was current at what time.

        Example::

            await Launch.refresh(enable_logging=True)
            errors = await Launch.get_error()

        Args:
            enable_logging: When ``True``, wires up the per-class
                ``QueueHandler`` logger so subsequent
                ``self.log_info(...)`` / ``self.log_error(...)`` calls
                land in ``logs/<ClassName>_*.log``.  Off by default.
            *args: Forwarded to :meth:`Incorporator.refresh`.
            **kwargs: Forwarded to :meth:`Incorporator.refresh`.

        Returns:
            Same as :meth:`Incorporator.refresh` — a single instance or an
            :class:`IncorporatorList`.
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
    async def export(cls: type[TLoggedIncorporator], *, enable_logging: bool = False, **kwargs: Any) -> None:
        """Production-observable variant of :meth:`Incorporator.export`.

        Serialise the object graph to disk, with an
        ``enable_logging=True`` opt-in that brackets the run with INFO
        entries and captures any raised exception.

        Reach for this wrapper inside scheduled batch jobs where the
        export is the deliverable and a silent failure would go
        unnoticed until the consumer notices stale data the next day.

        Example::

            await Launch.export(
                file_type="parquet",
                filename="launches",
                enable_logging=True,
            )

        Args:
            enable_logging: When ``True``, brackets the export with
                ``"Initiating export..."`` / ``"Export process
                completed successfully."`` INFO entries on
                ``logs/<ClassName>_api.log``, and routes any raised
                exception to ``logs/<ClassName>_error.log`` with the
                traceback attached before re-raising.  Off by default.
            **kwargs: Forwarded to :meth:`Incorporator.export`.

        Returns:
            ``None``.
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
    async def stream(  # type: ignore[override]
        cls: type[TLoggedIncorporator],
        incorp_params: dict[str, Any],
        refresh_params: dict[str, Any] | None = _UNSET,
        export_params: dict[str, Any] | None = None,
        poll_interval: float | None = None,
        stateful_polling: bool = False,
        refresh_interval: float | None = None,
        export_interval: float | None = None,
        inflow: str | Path | None = None,
        outflow: str | Path | None = None,
        enable_logging: bool = False,
        adapt_chunk_size: bool = False,
        chunk_size_min: int = 100,
        chunk_size_max: int = 100_000,
        target_min_sec: float = 0.030,
        target_max_sec: float = 0.100,
    ) -> AsyncGenerator[Wave, None]:
        """Production-observable variant of :meth:`Incorporator.stream`.

        Overnight chunked drain with an ``enable_logging=True`` opt-in
        that mirrors every yielded :class:`Wave` to JSON-line logs on
        disk.

        Reach for this wrapper on the unattended overnight drain you
        intend to grep over the next morning — every successful chunk,
        every failed source, and every fatal pipeline error lands on
        disk while the caller keeps consuming waves in real time.

        Example::

            async for wave in Launch.stream(
                incorp_params={"inc_url": "https://api.example.com/launches"},
                poll_interval=300,
                enable_logging=True,
            ):
                handle(wave)

            failures = await Launch.get_error()

        Args:
            enable_logging: When ``True``, mirrors every :class:`Wave`
                to ``logs/<ClassName>_api.log`` (successful chunks) or
                ``logs/<ClassName>_error.log`` (chunks with
                ``failed_sources``), and routes fatal pipeline failures
                to the error log with traceback before re-raising.
                Off by default.

        All other kwargs are forwarded unchanged to :meth:`Incorporator.stream`.

        Each log record carries the full Pydantic dump under the
        ``wave`` key, accessible later via :meth:`get_error`.  The
        Wave itself is yielded to the caller **before** any disk write
        completes — the QueueHandler thread handles the write
        asynchronously, so the async-for loop is never blocked on I/O.

        Yields:
            :class:`Wave` — same shape as :meth:`Incorporator.stream`.
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
                adapt_chunk_size=adapt_chunk_size,
                chunk_size_min=chunk_size_min,
                chunk_size_max=chunk_size_max,
                target_min_sec=target_min_sec,
                target_max_sec=target_max_sec,
            ):
                if enable_logging:
                    _route_wave_to_log(cls.__name__, wave)

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
        stream_params: list[dict[str, Any]],
        outflow: Any,
        export_params: dict[str, Any],
        refresh_interval: float | None = None,
        export_interval: float | None = None,
        inflow: Any | None = None,
        enable_logging: bool = False,
    ) -> AsyncGenerator[Wave, None]:
        """Production-observable variant of :meth:`Incorporator.fjord`.

        Live stateful multi-source daemon with an
        ``enable_logging=True`` opt-in that mirrors every yielded
        :class:`Wave` to JSON-line logs on disk.

        Reach for this wrapper when the fjord fuses N concurrent
        sources through your ``outflow(state)`` and you need a single
        unified audit log — every source's waves and every outflow
        emission land under *this* class regardless of which source
        produced them, so one :meth:`get_error` call returns the full
        pipeline's error history.

        Example::

            async for wave in Combined.fjord(
                stream_params=[
                    {"incorp_params": {"inc_url": prices_url}},
                    {"incorp_params": {"inc_url": orders_url}},
                ],
                outflow=my_fuse_fn,
                export_params={"file_type": "parquet", "filename": "fused"},
                enable_logging=True,
            ):
                handle(wave)

        Args:
            enable_logging: When ``True``, mirrors every :class:`Wave`
                — including per-source ``"fjord_refresh:<Class>"`` and
                ``"outflow:<DynamicClass>"`` operations — to
                ``logs/<ClassName>_api.log`` (throughput) or
                ``logs/<ClassName>_error.log`` (failures), and routes
                fatal pipeline failures to the error log with traceback
                before re-raising.  Off by default.

        All other kwargs are forwarded unchanged to :meth:`Incorporator.fjord`.

        Yields:
            :class:`Wave` — same shape as :meth:`Incorporator.fjord`.
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
                    _route_wave_to_log(cls.__name__, wave)

                yield wave

            if enable_logging:
                cls.log_cls_info("Fjord process completed gracefully.")

        except Exception as e:
            if enable_logging:
                cls.log_cls_error(f"Fatal Fjord Pipeline Error: {str(e)}", exc_info=True)
            raise
