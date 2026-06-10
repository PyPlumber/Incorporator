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

from .._deps import orjson as _orjson_mod
from ..base import _UNSET, Incorporator
from ..list import IncorporatorList
from ..rejects import RejectEntry
from .wave import Wave  # re-exported — ``from .logger import Wave`` keeps working

if TYPE_CHECKING:
    from ..tideweaver.tide import Tide

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
    extra: dict[str, Any] = {"meta": meta, payload_key: payload, "_payload_key": payload_key, "is_api": is_api}
    if is_tide:
        extra["is_tide"] = True
    logger = logging.getLogger(logger_name)
    if logger.isEnabledFor(level):
        logger.log(level, msg, extra=extra)


def _route_to_log(logger_name: str, record: Wave | Tide | RejectEntry, *, extra_meta: str = "") -> None:
    """Single dispatcher — derive level/msg/payload/flags from *record* and emit.

    Dispatches on the concrete type of *record* to reproduce exactly the same
    routing decisions as the four legacy ``_route_*`` functions.  Callers that
    already have the typed object should prefer this entry point; the named
    wrappers below delegate here and exist only for backward compatibility.

    Dispatch rules:

    - :class:`Wave`: failure waves → ``ERROR``, success waves → ``INFO``,
      zero-row / zero-failure waves are silently skipped.
    - :class:`Tide`: canal-reject / error-skip → ``ERROR``, fired → ``INFO``,
      no-op → ``DEBUG``; ``is_tide=True`` always set.
    - :class:`RejectEntry`: always ``ERROR``; ``is_api`` mirrors
      ``reject.is_url_traffic_error``.

    The optional *extra_meta* string is appended to the derived base meta with
    a ``", "`` separator when both are non-empty; if only one is present that
    value is used directly.

    Args:
        logger_name: Name passed to :func:`logging.getLogger` — typically
            ``cls.__name__`` for :class:`LoggedIncorporator` call sites or
            ``_logger_name`` for :class:`~incorporator.tideweaver.logged.LoggedTideweaver`.
        record: One of :class:`Wave`, :class:`Tide`, or :class:`RejectEntry`.
        extra_meta: Optional additional ``key:"value"`` context to append to
            the base meta string — e.g. ``'current:"prices"'`` when the
            scheduler routes a current's wave into the session log.

    Raises:
        TypeError: When *record* is not one of the three supported types.
    """
    # Deferred import breaks the logger → tideweaver/__init__ → logged → logger cycle.
    # tide.py itself has no logger dependency; the cycle runs through __init__.py.
    from ..tideweaver.tide import Tide as _Tide  # noqa: PLC0415

    level: int
    msg: str
    payload_key: str
    payload: dict[str, Any]
    base_meta: str
    is_api: bool = False
    is_tide: bool = False

    if isinstance(record, Wave):
        dump = record.model_dump(mode="json")
        dump["failed_sources"] = [_redact(s) for s in dump.get("failed_sources", [])]
        base_meta = record.log_meta()
        payload_key = "wave"
        payload = dump
        if record.failed_sources:
            msg = f"{record.operation} chunk {record.chunk_index} encountered failures: {dump['failed_sources']}"
            level = logging.ERROR
        elif record.rows_processed > 0:
            msg = (
                f"{record.operation} chunk {record.chunk_index} complete: "
                f"{record.rows_processed} rows in {record.processing_time_sec:.3f}s."
            )
            level = logging.INFO
        else:
            # Zero-row / zero-failure — no-op, nothing to emit.
            return
    elif isinstance(record, _Tide):
        dump = record.model_dump(mode="json")
        base_meta = record.log_meta()
        payload_key = "tide"
        payload = dump
        is_tide = True
        has_error_skips = any(reason in ("surge_halted", "skip_ahead") for _, reason in record.skipped)
        if record.canal_rejects_added > 0 or has_error_skips:
            error_reasons = [reason for _, reason in record.skipped if reason in ("surge_halted", "skip_ahead")]
            msg = (
                f"tide {record.tide_number}: {record.canal_rejects_added} canal reject(s), "
                f"skipped reasons {error_reasons}"
            )
            level = logging.ERROR
        elif len(record.fired) > 0:
            msg = (
                f"tide {record.tide_number}: fired {len(record.fired)}, skipped {len(record.skipped)}, "
                f"rejects {record.canal_rejects_added}, duration {record.duration_sec:.3f}s"
            )
            level = logging.INFO
        else:
            msg = f"tide {record.tide_number}: no-op pass (nothing fired), duration {record.duration_sec:.3f}s"
            level = logging.DEBUG
    elif isinstance(record, RejectEntry):
        base_meta = (
            f'class:"{logger_name}", source:"{record.source}", error_kind:"{record.error_kind}", '
            f'from:"{record.from_name}", to:"{record.to_name}", host:"{record.host}", '
            f"status_code:{record.status_code}"
        )
        payload_key = "reject"
        payload = record.model_dump(mode="json")
        msg = str(record)
        level = logging.ERROR
        is_api = record.is_url_traffic_error
    else:
        raise TypeError(
            f"_route_to_log: unsupported record type {type(record).__name__!r}. "
            "Expected Wave, Tide, or RejectEntry. "
            "For scheduler events use _route_scheduler_event_to_log directly."
        )

    meta = f"{base_meta}, {extra_meta}" if (base_meta and extra_meta) else (base_meta or extra_meta)
    _emit_payload(logger_name, level, msg, payload_key, payload, meta, is_api=is_api, is_tide=is_tide)


def _route_wave_to_log(cls_name: str, wave: Wave) -> None:
    """Route a single Wave to the appropriate log level based on its outcome.

    Shared adapter used by :meth:`LoggedIncorporator.stream` and ``fjord``. The
    routing rules mirror :func:`_route_to_log` for Wave records. Delegates to
    :func:`_route_to_log`.

    Args:
        cls_name: Logger name — ``cls.__name__`` from the calling verb wrapper.
        wave: The :class:`Wave` record yielded by the pipeline.
    """
    _route_to_log(cls_name, wave)


def _route_tide_to_log(cls_name: str, tide: Tide) -> None:
    """Route one Tide record to info/error/debug based on its outcomes.

    Routing rules mirror :func:`_route_to_log` for Tide records. Delegates to
    :func:`_route_to_log`.

    Args:
        cls_name: Logger name — typically ``LoggedTideweaver._logger_name``.
        tide: The :class:`Tide` record yielded by :meth:`Tideweaver.run`.
    """
    _route_to_log(cls_name, tide)


_SCHEDULER_ERROR_EVENTS: frozenset[str] = frozenset({"tick_parked", "fjord_flush_failure"})


def _route_scheduler_event_to_log(
    logger_name: str,
    event_type: str,
    current_name: str | None,
    detail: str,
    *,
    cls_name: str | None = None,
    edge: tuple[str, str] | None = None,
    tide_number: int | None = None,
) -> None:
    """Route one scheduler diagnostic event to the session's structured error log.

    Parallel to :func:`_route_tide_to_log` / :func:`_route_reject_to_log` but
    for internal scheduler events that the bare module logger previously emitted
    to stderr only (never reaching any session log file).  A Phase-4
    ``get_scheduler_events()`` reader will call
    ``_read_filtered(filename, "scheduler_event")`` against the error log to
    retrieve these records.

    **Destination choice:** ``is_tide=False`` is intentional.  Scheduler events
    are not per-pass tide summaries; they belong in the error log (``error.log``)
    where operational failures accumulate, not in ``tide.log``.  The
    ``debug.log`` superset also receives them via the level routing.

    Level mapping:

    - ``"isolated_tick_failure"`` / ``"empty_output"`` / ``"empty_parent_snapshot"``
      → ``WARNING``
    - ``"tick_parked"`` / ``"fjord_flush_failure"`` → ``ERROR``
    - ``"watershed_started"`` / ``"watershed_completed"`` → ``WARNING``

    When *current_name* is ``None`` the event is watershed-scoped; the meta
    string renders ``scope:"watershed"`` in place of ``current:"<name>"``.

    Args:
        logger_name: Named logger to emit to (the session's
            :func:`setup_class_logger`-registered name).
        event_type: Canonical event label (e.g. ``"isolated_tick_failure"``).
        current_name: :class:`~incorporator.tideweaver.current.Current`
            name where the event originated, or ``None`` for watershed-scoped
            lifecycle events (``"watershed_started"`` / ``"watershed_completed"``).
        detail: Human-readable description of the event.
        cls_name: ``current.cls.__name__`` when available.
        edge: ``(from_name, to_name)`` tuple for edge-scoped events.
        tide_number: Scheduler pass number when available.
    """
    level = logging.ERROR if event_type in _SCHEDULER_ERROR_EVENTS else logging.WARNING
    payload: dict[str, Any] = {
        "event_type": event_type,
        "current_name": current_name,
        "cls_name": cls_name,
        "edge": list(edge) if edge is not None else None,
        "tide_number": tide_number,
        "session": logger_name,
        "detail": detail,
    }
    if current_name is None:
        scope_fragment = 'scope:"watershed"'
    else:
        scope_fragment = f'current:"{current_name}"'
    meta = (
        f'logger:"{logger_name}", event_type:"{event_type}", {scope_fragment}'
        + (f', cls:"{cls_name}"' if cls_name else "")
        + (f', edge:"{edge[0]}->{edge[1]}"' if edge else "")
        + (f", tide:{tide_number}" if tide_number is not None else "")
    )
    _emit_payload(logger_name, level, detail, "scheduler_event", payload, meta)


def current_meta(current: Any) -> str:
    """Return a compact ``key:"value"`` meta string identifying a Tideweaver current.

    Stable across passes — ``current.name`` is unique within a Watershed
    (enforced by topological sort) and is the retrieval key for
    :meth:`~incorporator.tideweaver.logged.LoggedTideweaver.get_current`.

    Args:
        current: A :class:`~incorporator.tideweaver.current.Current`
            instance (typed ``Any`` to avoid a new module-graph edge from
            ``logger.py`` into ``tideweaver/current.py``).

    Returns:
        A string of the form
        ``'current:"<name>", class:"<ClassName>", code:"<name>"'``
        suitable for appending to ``_route_to_log``'s ``extra_meta`` parameter.
    """
    return f'current:"{current.name}", class:"{current.cls.__name__}", code:"{current.name}"'


def _route_reject_to_log(cls_name: str, reject: RejectEntry) -> None:
    """Route one RejectEntry to disk with structured edge and HTTP metadata.

    Always logs at ERROR level. Destination splits on
    ``reject.is_url_traffic_error``: URL internet-traffic rejects route to
    ``api.log``; all other rejects stay in ``error.log``. Delegates to
    :func:`_route_to_log`.

    Args:
        cls_name: Logger name — typically ``LoggedTideweaver._logger_name``.
        reject: The :class:`RejectEntry` to route.
    """
    _route_to_log(cls_name, reject)


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
                        rec = _orjson_mod.loads(line)
                        if key in rec:
                            records.append(rec)
                    except (json.JSONDecodeError, ValueError):
                        pass
    except OSError:
        pass
    return records


async def read_log(
    name: str,
    suffixes: list[str] | str,
    *,
    key: str | None = None,
    meta_contains: str | None = None,
) -> list[dict[str, Any]]:
    """Read and union JSONL records across one or more log-file suffixes for a named logger session.

    The single parameterised reader that all ``get_*`` methods delegate to.
    Replaces the duplicated ``_read_disk`` / ``_read_both`` closures that
    previously lived inside each method.

    For each suffix in *suffixes*, resolves the path
    ``logs/<name>_<suffix>.log`` via :func:`_safe_log_filename`, reads every
    non-empty JSONL line (orjson fast-path where available, tolerant of
    malformed lines), applies optional filters, and accumulates the matching
    records into a single unified list.  Files that do not exist or cannot be
    read (``OSError``) are silently skipped.

    Args:
        name: Logger-session name — typically ``cls.__name__`` for
            :class:`LoggingMixin` subclasses or the instance-level
            ``logger_name`` for :class:`~incorporator.tideweaver.logged.LoggedTideweaver`.
        suffixes: One or more log-file suffixes (without the ``.log``
            extension) to read and union.  A bare string is normalised to a
            single-element list.  Suffixes are processed in order; record
            order within each file is preserved.
        key: When given, only records that contain this string as a top-level
            JSON key are included.  When ``None`` (default), all records from
            the file are returned — no key-presence check is applied.
        meta_contains: When given, only records whose ``meta`` string value
            contains this substring are included.  Records that have no
            ``meta`` field (``rec.get("meta", "")`` returns ``""``) are
            excluded when this filter is active, which is the correct
            behaviour for per-current ``get_current`` queries.

    Returns:
        List of parsed record dicts, in the order they appear across the
        requested suffix files.  Returns ``[]`` when no matching records exist
        or when all log files are absent.

    Example::

        records = await read_log("PriceSession", ["error", "api"], key="reject")
        url_errors = await read_log("Launch", "api", meta_contains="abc123")
    """
    if isinstance(suffixes, str):
        suffixes = [suffixes]

    def _do_read() -> list[dict[str, Any]]:
        accumulated: list[dict[str, Any]] = []
        for suffix in suffixes:
            filename = _safe_log_filename(name, f"{suffix}.log")
            path = Path(filename).resolve()
            try:
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            rec: dict[str, Any] = _orjson_mod.loads(line)
                        except (json.JSONDecodeError, ValueError):
                            continue
                        if key is not None and key not in rec:
                            continue
                        if meta_contains is not None and meta_contains not in rec.get("meta", ""):
                            continue
                        accumulated.append(rec)
            except OSError:
                pass
        return accumulated

    return await asyncio.to_thread(_do_read)


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
        # Emit the structured payload under its own key generically.
        # _emit_payload stores the key name in record._payload_key so any new
        # payload type (e.g. "watershed_event") works without a formatter edit.
        pk = getattr(record, "_payload_key", None)
        if pk is not None and hasattr(record, pk):
            log_obj[pk] = getattr(record, pk)
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        return _orjson_mod.dumps_str(log_obj)


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
            :class:`~incorporator.tideweaver.LoggedTideweaver`.
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

        Note:
            URL internet-traffic errors (httpx transport and HTTP-status
            failures where ``RejectEntry.is_url_traffic_error`` is ``True``)
            route to ``<ClassName>_api.log`` and are accessible via
            :meth:`get_api`.  This method reads only ``error.log`` and will
            NOT return those records.  Use :meth:`get_rejects` to retrieve
            the union of rejects from both files.
        """
        return await read_log(cls.__name__, ["error"])

    @classmethod
    async def get_rejects(cls) -> list[dict[str, Any]]:
        """Pull every reject this class has logged, from both ``error.log`` and ``api.log``.

        Reach for ``get_rejects()`` after an overnight pipeline to iterate
        over every HTTP failure, fjord seed error, or canal skip that was
        serialised to the logs as a structured ``reject`` record.

        URL internet-traffic rejects (``is_url_traffic_error=True``) land in
        ``<ClassName>_api.log``; all other rejects (parse errors, fjord seed
        errors, canal-layer skips) land in ``<ClassName>_error.log``.  This
        method reads both files and returns the combined list so callers need
        not know which file a particular reject landed in.

        Example::

            rejects = await Launch.get_rejects()
            for rec in rejects:
                reject = rec["reject"]
                print(reject["source"], reject["error_kind"])

        Records contain a top-level ``"reject"`` key whose value matches the
        :class:`~incorporator.rejects.RejectEntry` model dump.  Safe to call
        when no rejects have been logged yet — returns an empty list rather than
        raising.  Reads both log files in a worker thread via
        :func:`asyncio.to_thread` so the event loop is never blocked.
        """
        return await read_log(cls.__name__, ["error", "api"], key="reject")

    @classmethod
    async def get_api(cls) -> list[dict[str, Any]]:
        """Pull all records from ``logs/<ClassName>_api.log``.

        ``api.log`` accumulates two kinds of records:

        - Hand-called :meth:`log_api` entries — outbound HTTP audit traces,
          request/response metadata, and any record the user explicitly routes
          to the API log.
        - URL internet-traffic error rejects — :class:`~incorporator.rejects.RejectEntry`
          records where ``is_url_traffic_error=True`` (httpx HTTP-status 4xx/5xx
          and transport failures).  These are routed here by
          :func:`_route_reject_to_log` so that network-level rejections are
          separated from parse errors and canal-layer skips (which stay in
          ``error.log``).

        Use :meth:`get_rejects` to retrieve the union of all reject records
        from both ``api.log`` and ``error.log``.

        Example::

            api_records = await Launch.get_api()
            for rec in api_records:
                if "reject" in rec:
                    print("URL error:", rec["reject"]["source"])
                else:
                    print("API record:", rec.get("msg"))

        Safe to call when no records have been logged yet — returns an empty
        list rather than raising.  Tails ``logs/<ClassName>_api.log`` in a
        worker thread via :func:`asyncio.to_thread` so the event loop is never
        blocked.
        """
        return await read_log(cls.__name__, ["api"])

    @classmethod
    async def get_current(cls, code: str) -> list[dict[str, Any]]:
        """Pull all records tagged with *code* in their ``meta`` field, from the debug-log superset.

        Use this for a per-current view of all structured log records that carry
        the given current code in their ``meta`` string (e.g.
        ``'code:"abc123"'``).  Reads only ``<ClassName>_debug.log`` because
        ``debug_fh`` in :func:`setup_class_logger` carries **no filter** and a
        ``DEBUG`` floor — every record that lands in ``api.log`` (via
        :class:`APIFilter`) or ``error.log`` (via :class:`StandardFilter`) also
        lands in ``debug.log``.  Reading the debug superset exclusively avoids
        the double-counting that would occur when unioning all three files,
        since each api-routed or error-routed record would then appear twice
        (once from its level file, once from ``debug.log``).

        Args:
            code: Substring to search for inside each record's ``meta`` field.
                Typically the ``code`` value embedded by
                :func:`~incorporator.observability.logger._emit_payload` as
                ``code:"<value>"``.  Records with no ``meta`` field are
                excluded.

        Returns:
            List of record dicts whose ``meta`` string contains *code*, drawn
            from ``debug.log`` only (the complete, de-duplicated per-current
            view).  Returns ``[]`` when no matching records exist.

        Example::

            records = await Launch.get_current("abc123")
            for rec in records:
                print(rec["level"], rec["msg"])
        """
        return await read_log(cls.__name__, ["debug"], meta_contains=code)

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

        The record lands in ``logs/<ClassName>_error.log`` (and the debug
        superset) carrying a ``class:"<Name>"`` meta field, because
        ``is_api=False`` routes it through :class:`StandardFilter`.
        Silently noops when the class's logger isn't configured for INFO —
        safe to sprinkle through code paths that might run before
        ``enable_logging=True`` ever fires.
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

        The record lands in ``logs/<ClassName>_error.log`` (and the debug
        superset) with :meth:`log_meta` attached, because ``is_api=False``
        routes it through :class:`StandardFilter`.  Silently noops when INFO
        isn't enabled.
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
            if isinstance(result, IncorporatorList) and result.rejects:
                for reject in result.rejects:
                    _route_reject_to_log(cls.__name__, reject)
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
        if enable_logging:
            setup_class_logger(cls)

        result = await super().refresh(*args, **kwargs)

        if enable_logging:
            if isinstance(result, IncorporatorList) and result.rejects:
                for reject in result.rejects:
                    _route_reject_to_log(cls.__name__, reject)
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
        target_min_sec: float = 0.001,
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
                    for reject in getattr(wave, "rejects", []):
                        _route_reject_to_log(cls.__name__, reject)

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
                    for reject in getattr(wave, "rejects", []):
                        _route_reject_to_log(cls.__name__, reject)

                yield wave

            if enable_logging:
                cls.log_cls_info("Fjord process completed gracefully.")

        except Exception as e:
            if enable_logging:
                cls.log_cls_error(f"Fatal Fjord Pipeline Error: {str(e)}", exc_info=True)
            raise
