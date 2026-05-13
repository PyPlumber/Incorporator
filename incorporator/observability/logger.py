"""Multiplex logging architecture and wrapper subclass for Incorporator."""

import asyncio
import atexit
import json
import logging
import queue
import re
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

from ..base import Incorporator
from ..list import IncorporatorList

TLoggedIncorporator = TypeVar("TLoggedIncorporator", bound="LoggedIncorporator")

# Global registry to prevent duplicate background threads if a class is dynamically rebuilt
_ACTIVE_LISTENERS: Dict[str, QueueListener] = {}
MAX_LOG_THREADS = 50  # Hard OS limit constraint


class AuditResult(BaseModel):
    """
    Structured telemetry payload for pipeline observability.
    Merged into the logging module to unify all non-blocking IO metrics.
    """

    model_config = ConfigDict(frozen=True)

    chunk_index: int = Field(..., description="Sequential index of the current chunk.")
    operation: str = Field("stream", description="The phase: 'incorp', 'refresh', or 'export'.")
    rows_processed: int = Field(..., description="Number of rows successfully processed.")
    failed_sources: List[str] = Field(default_factory=list, description="Failed source URIs.")
    processing_time_sec: float = Field(..., description="Chunk processing duration in seconds.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def log_meta(self) -> str:
        """Compact, single-line meta string mirroring :meth:`LoggingMixin.log_meta`.

        Used by :func:`_route_audit_to_log` so audit records share the
        flat ``meta`` shape with instance-level log records. The full
        Pydantic dump is also attached as a structured ``audit`` field
        on every record (see :class:`JSONFormatter`).
        """
        return (
            f'operation:"{self.operation}", chunk_index:{self.chunk_index}, '
            f"rows:{self.rows_processed}, time_sec:{self.processing_time_sec:.3f}, "
            f"failed:{len(self.failed_sources)}"
        )


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
    for typical audit lines.
    """
    return _REDACT_QS_PATTERN.sub(r"\1=***REDACTED***", text)


def _route_audit_to_log(cls: Type[Any], audit: "AuditResult") -> None:
    """Adapter shared by :meth:`LoggedIncorporator.stream` and ``fjord``.

    - Routes audits with ``failed_sources`` to ``error.log``.
    - Routes audits with ``rows_processed > 0`` (and no failures) to ``info``.
    - Zero-row, zero-failure audits are skipped (noise).
    - Attaches the structured ``audit`` dump as a record extra so
      :class:`JSONFormatter` writes it as a top-level JSON key alongside
      ``meta`` — :meth:`LoggingMixin.get_error` callers can read
      ``record["audit"]`` directly.
    - Applies :func:`_redact` to the human-readable message *and* the
      ``failed_sources`` list inside the dumped audit. The ``AuditResult``
      yielded back to the caller is untouched.
    """
    dump = audit.model_dump(mode="json")
    dump["failed_sources"] = [_redact(s) for s in dump.get("failed_sources", [])]

    extra = {
        "meta": audit.log_meta(),
        "audit": dump,
        "is_api": False,
    }

    if audit.failed_sources:
        msg = f"{audit.operation} chunk {audit.chunk_index} encountered failures: " f"{dump['failed_sources']}"
        cls_logger = cls._get_cls_logger() if hasattr(cls, "_get_cls_logger") else logging.getLogger(cls.__name__)
        if cls_logger.isEnabledFor(logging.ERROR):
            cls_logger.error(msg, extra=extra)
    elif audit.rows_processed > 0:
        msg = (
            f"{audit.operation} chunk {audit.chunk_index} complete: "
            f"{audit.rows_processed} rows in {audit.processing_time_sec:.3f}s."
        )
        cls_logger = cls._get_cls_logger() if hasattr(cls, "_get_cls_logger") else logging.getLogger(cls.__name__)
        if cls_logger.isEnabledFor(logging.INFO):
            cls_logger.info(msg, extra=extra)


def _cleanup_listeners() -> None:
    """Gracefully shuts down all background logging threads on application exit."""
    for listener in _ACTIVE_LISTENERS.values():
        listener.stop()
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
        # Structured audit payload attached by _route_audit_to_log — surfaces as
        # a top-level JSON key so get_error() consumers can read record["audit"].
        if hasattr(record, "audit"):
            log_obj["audit"] = record.audit
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
        old_listener.stop()

    listener = QueueListener(log_queue, debug_fh, error_fh, api_fh, respect_handler_level=True)
    listener.start()

    _ACTIVE_LISTENERS[cls_name] = listener


class LoggingMixin:
    """Provides targeted logging methods and error retrieval to instances and classes."""

    @classmethod
    async def get_error(cls) -> List[Dict[str, Any]]:
        """Reads the {cls.__name__}_error.log JSON file and returns parsed log records."""

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
                pass  # Safely ignore disk errors during read attempts
            return errors

        # Execute disk read in a background worker thread (Pillar E)
        return await asyncio.to_thread(_read_disk)

    # --- CLASS-LEVEL LOGGING (For Factory Methods like export) ---

    @classmethod
    def _get_cls_logger(cls) -> logging.Logger:
        return logging.getLogger(cls.__name__)

    @classmethod
    def log_cls_info(cls, msg: str) -> None:
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.INFO):
            meta_str = f'class:"{cls.__name__}"'
            logger.info(msg, extra={"meta": meta_str, "is_api": False})

    @classmethod
    def log_cls_error(cls, msg: str, exc_info: bool = False) -> None:
        logger = cls._get_cls_logger()
        if logger.isEnabledFor(logging.ERROR):
            meta_str = f'class:"{cls.__name__}"'
            logger.error(msg, exc_info=exc_info, extra={"meta": meta_str, "is_api": False})

    # --- INSTANCE-LEVEL LOGGING ---

    def log_meta(self) -> str:
        """Generates a meta string detailing the class origin and instance identity."""
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
        logger = self._get_logger()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_info(self, msg: str) -> None:
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": False})

    def log_error(self, msg: str, exc_info: bool = False) -> None:
        logger = self._get_logger()
        if logger.isEnabledFor(logging.ERROR):
            logger.error(msg, exc_info=exc_info, extra={"meta": self.log_meta(), "is_api": False})

    def log_api(self, msg: str) -> None:
        logger = self._get_logger()
        if logger.isEnabledFor(logging.INFO):
            logger.info(msg, extra={"meta": self.log_meta(), "is_api": True})


class LoggedIncorporator(LoggingMixin, Incorporator):
    """The Incorporator Logging Wrapper Subclass."""

    @classmethod
    async def incorp(
        cls: Type[TLoggedIncorporator], *args: Any, enable_logging: bool = False, **kwargs: Any
    ) -> Union[TLoggedIncorporator, IncorporatorList[TLoggedIncorporator]]:
        """Declarative factory that sets up class-specific logging before generation."""

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
        """Hydrates an existing instance with new data, optionally enabling logs."""
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
        """Exports the class data, optionally wrapping the process in observability logs."""
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
        refresh_params: Optional[Dict[str, Any]] = None,
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Any] = None,
        outflow: Optional[Any] = None,
        enable_logging: bool = False,
    ) -> AsyncGenerator[AuditResult, None]:
        """
        Autonomous Pipeline Runner with background Telemetry Logging.
        Intercepts the chunk generator to push Audit metrics to non-blocking disk queues.
        """
        if enable_logging:
            setup_class_logger(cls)
            cls.log_cls_info("Initiating autonomous stream orchestration.")

        try:
            async for audit in super().stream(
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
                    _route_audit_to_log(cls, audit)

                # Yield downstream to the caller natively
                yield audit

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
    ) -> AsyncGenerator[AuditResult, None]:
        """Fjord wrapper that routes per-tick audits through the disk loggers.

        Mirrors :meth:`stream` — when ``enable_logging`` is set, every
        :class:`AuditResult` yielded by the fjord engine is also routed
        through :func:`_route_audit_to_log` so:

        - throughput audits land in the calling class's ``info.log`` /
          ``api.log``,
        - failures land in ``error.log``, retrievable via :meth:`get_error`,
        - the structured ``audit`` dump rides on every JSON record.

        For per-source operations (``"fjord_refresh:Coin"``, ``"outflow:CoinMarket"``)
        the audit is logged under the calling class for retrieval simplicity.
        Per-source loggers are still set up by :meth:`incorp` on first use.
        """
        if enable_logging:
            setup_class_logger(cls)
            cls.log_cls_info("Initiating fjord orchestration.")

        try:
            async for audit in super().fjord(
                stream_params=stream_params,
                outflow=outflow,
                export_params=export_params,
                refresh_interval=refresh_interval,
                export_interval=export_interval,
                inflow=inflow,
            ):
                if enable_logging:
                    _route_audit_to_log(cls, audit)

                yield audit

            if enable_logging:
                cls.log_cls_info("Fjord process completed gracefully.")

        except Exception as e:
            if enable_logging:
                cls.log_cls_error(f"Fatal Fjord Pipeline Error: {str(e)}", exc_info=True)
            raise
