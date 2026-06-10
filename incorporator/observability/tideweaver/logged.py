"""LoggedTideweaver — Tideweaver with structured JSON-line logs to disk.

Parallels :class:`~incorporator.observability.logger.LoggedIncorporator` at
``observability/logger.py``.  Wraps the async generator returned by
:meth:`Tideweaver.run` so every yielded :class:`Tide` and every accumulated
:class:`~incorporator.RejectEntry` lands on disk via the existing
:class:`~logging.handlers.QueueHandler`-backed background thread.

Example::

    from datetime import datetime, timedelta, timezone
    from incorporator.observability.tideweaver import (
        LoggedTideweaver,
        Stream,
        Watershed,
    )

    now = datetime.now(timezone.utc)
    watershed = Watershed.parallel(
        window=(now, now + timedelta(hours=4)),
        currents=[Stream(name="prices", cls=PriceClass, interval=30.0, incorp_params={})],
    )

    async for tide in LoggedTideweaver(
        watershed,
        enable_logging=True,
        logger_name="PriceSession",
    ).run():
        print(tide.fired, tide.duration_sec)

    # Later — read structured pass records from disk:
    # logs/PriceSession_tide.log   (all tides — single-file source for get_tides())
    # logs/PriceSession_error.log  (canal rejects + error-class skips; fired tides also here)
    # logs/PriceSession_debug.log  (superset — all passes including no-ops)
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from ..logger import (
    _route_reject_to_log,
    _route_scheduler_event_to_log,
    _route_tide_to_log,
    read_log,
    setup_class_logger,
)
from .scheduler import TickFactory, Tideweaver
from .tide import Tide
from .watershed import Watershed


class LoggedTideweaver(Tideweaver):
    """Drop-in for :class:`Tideweaver` with structured JSON-line logs to disk.

    Swap in when an overnight windowed session needs pass-level records you
    can grep, ship to an aggregator, or feed into a retry loop.  Subclass
    is not required — instantiate ``LoggedTideweaver`` directly with your
    :class:`Watershed` and pass ``enable_logging=True``.  Every yielded
    :class:`Tide` and every :class:`~incorporator.RejectEntry` accumulated
    during the run lands in rotating JSONL files under the logger name.

    Logging is **opt-in** via ``enable_logging=True`` — the default keeps
    wire-compatible parity with :class:`Tideweaver`.  Disk I/O runs through
    the same :class:`~logging.handlers.QueueHandler`-backed background thread
    used by :class:`~incorporator.observability.logger.LoggedIncorporator`,
    so the async event loop is never blocked on log writes.

    Canal-layer :class:`~incorporator.RejectEntry` records (penstock limits,
    surge halts, skip-aheads) are swept from :attr:`Tideweaver.rejects` in a
    ``finally`` block so they land on disk even when the run is cancelled.

    Args:
        watershed: The :class:`Watershed` plan to orchestrate.
        tick_factory: Optional override for per-current tick bodies (test
            injection only).
        pass_interval: Seconds between scheduler passes.
        enable_logging: When ``True``, wires up the named
            :class:`~logging.handlers.QueueHandler` logger and routes every
            :class:`Tide` and :class:`~incorporator.RejectEntry` to disk.
            Off by default.
        logger_name: Name used for log files and :mod:`logging` logger lookup.
            When ``None`` (default), resolves to ``watershed.name`` if set,
            otherwise falls back to ``"Tideweaver"``.  An explicit non-``None``
            value always wins.  Two :class:`LoggedTideweaver` instances sharing
            the same resolved name share one log-file set, mirroring the
            existing per-class-name behaviour for
            :class:`~incorporator.observability.logger.LoggedIncorporator`.
        log_currents: When ``True`` (default), each stream current's yielded
            :class:`~incorporator.observability.wave.Wave` records and their
            per-wave :class:`~incorporator.rejects.RejectEntry` items are routed
            to the session logs tagged with per-current meta (``code:"<name>"``).
            URL-traffic rejects land in ``api.log``; all others in
            ``error.log``/``debug.log``.  Set to ``False`` for high-frequency
            watersheds where per-wave log volume would be excessive.

    Example::

        async for tide in LoggedTideweaver(
            watershed,
            enable_logging=True,
            logger_name="MySession",
        ).run():
            process(tide)
    """

    def __init__(
        self,
        watershed: Watershed,
        *,
        tick_factory: TickFactory | None = None,
        pass_interval: float | None = None,
        backlog_backoff_factor: float = 1.0,
        enable_logging: bool = False,
        logger_name: str | None = None,
        log_currents: bool = True,
    ) -> None:
        resolved_name = logger_name or watershed.name or "Tideweaver"
        self._enable_logging = enable_logging
        self._logger_name = resolved_name
        if enable_logging:
            setup_class_logger(resolved_name)
        super().__init__(
            watershed,
            tick_factory=tick_factory,
            pass_interval=pass_interval,
            backlog_backoff_factor=backlog_backoff_factor,
            logger_name=resolved_name if enable_logging else None,
            log_currents=log_currents,
        )

    async def run(self) -> AsyncIterator[Tide]:
        """Orchestrate the watershed, routing each Tide and final RejectEntries to disk.

        Mirrors :meth:`Tideweaver.run` exactly — same yield shape, same window
        semantics, same drain-on-exit behaviour — with the addition that when
        ``enable_logging=True``:

        - ``watershed_started`` and ``watershed_completed`` lifecycle events are
          emitted to the session log via
          :func:`~incorporator.observability.logger._route_scheduler_event_to_log`
          bracketing the run.  Both are retrievable via
          :meth:`get_scheduler_events`.
        - Each yielded :class:`Tide` is routed to INFO / ERROR / DEBUG via
          :func:`~incorporator.observability.logger._route_tide_to_log`.
        - All accumulated :class:`~incorporator.RejectEntry` records are swept
          in a ``finally`` block via
          :func:`~incorporator.observability.logger._route_reject_to_log` so
          they land on disk even under cancellation.

        Yields:
            :class:`Tide` — same shape as :meth:`Tideweaver.run`.
        """
        ws_detail: str = ""
        if self._enable_logging:
            ws = self.watershed
            ws_name = ws.name or "unnamed"
            ws_win_start, ws_win_end = ws.window
            ws_detail = f"watershed={ws_name!r}, window=({ws_win_start.isoformat()}, {ws_win_end.isoformat()})"
            _route_scheduler_event_to_log(
                self._logger_name,
                "watershed_started",
                None,
                f"Watershed run started: {ws_detail}",
            )
        try:
            async for tide in super().run():
                if self._enable_logging:
                    _route_tide_to_log(self._logger_name, tide)
                yield tide
        finally:
            if self._enable_logging:
                _route_scheduler_event_to_log(
                    self._logger_name,
                    "watershed_completed",
                    None,
                    f"Watershed run completed: {ws_detail}",
                )
                # Skip rejects already routed at their tick site with per-current
                # meta (e.g. SourceLoadFailure) so they are not emitted twice.
                for reject in self.rejects:
                    if id(reject) not in self._routed_reject_ids:
                        _route_reject_to_log(self._logger_name, reject)

    @classmethod
    async def get_tides(cls, logger_name: str) -> list[dict[str, Any]]:
        """Return all tide records from ``tide.log`` for ``logger_name``.

        Reads the dedicated ``<logger_name>_tide.log`` file written by
        :class:`~incorporator.observability.logger.TideFilter` — both fired
        (INFO/ERROR) and no-op (DEBUG) tides land there, so a single-file
        read suffices.  A defensive sort by ``tide_number`` guards against
        subtle interleave when two :class:`LoggedTideweaver` instances share
        the same ``logger_name`` (documented behaviour — see class docstring).

        Args:
            logger_name: The name used when the :class:`LoggedTideweaver` was
                constructed (e.g. ``"PriceSession"``).  Controls which
                ``logs/<logger_name>_tide.log`` file is read.

        Returns:
            List of tide-record dicts sorted ascending by ``tide_number``.
            Each dict contains a top-level ``"tide"`` key whose value matches
            the :class:`~incorporator.observability.tideweaver.tide.Tide`
            model dump.  Returns an empty list when no log files exist yet.

        Example::

            tides = await LoggedTideweaver.get_tides("PriceSession")
            for rec in tides:
                t = rec["tide"]
                print(t["tide_number"], t["fired"], t["duration_sec"])
        """
        records = await read_log(logger_name, ["tide"], key="tide")
        return sorted(records, key=lambda r: r.get("tide", {}).get("tide_number", 0))

    @classmethod
    async def get_rejects(cls, logger_name: str) -> list[dict[str, Any]]:
        """Return all reject records from ``error.log`` and ``api.log`` for ``logger_name``.

        Overrides :meth:`~incorporator.observability.logger.LoggingMixin.get_rejects`
        solely for name resolution: :class:`LoggedTideweaver` uses an
        instance-level ``logger_name`` rather than ``cls.__name__``, so the
        correct log file cannot be determined from the class alone.

        URL internet-traffic rejects (``is_url_traffic_error=True``) land in
        ``<logger_name>_api.log``; all other rejects (parse errors, canal-layer
        skips, fjord seed errors) land in ``<logger_name>_error.log``.  This
        method unions both files so callers receive all rejects regardless of
        routing.

        Args:
            logger_name: The name used when the :class:`LoggedTideweaver` was
                constructed (e.g. ``"PriceSession"``).

        Returns:
            List of reject-record dicts from both
            ``logs/<logger_name>_error.log`` and ``logs/<logger_name>_api.log``.
            Each dict contains a top-level ``"reject"`` key.  Returns an empty
            list when no log files exist yet.

        Example::

            rejects = await LoggedTideweaver.get_rejects("PriceSession")
            for rec in rejects:
                print(rec["reject"]["source"], rec["reject"]["error_kind"])
        """
        return await read_log(logger_name, ["error", "api"], key="reject")

    @classmethod
    async def get_scheduler_events(cls, logger_name: str) -> list[dict[str, Any]]:
        """Return all scheduler-event records from ``error.log`` for ``logger_name``.

        Scheduler diagnostics routed by
        :func:`~incorporator.observability.logger._route_scheduler_event_to_log`
        land in the session's ``error.log`` under a top-level
        ``"scheduler_event"`` key (the router calls ``_emit_payload`` with
        ``is_tide=False`` so :class:`~incorporator.observability.logger.TideFilter`
        never diverts them to ``tide.log``).  Reading ``error.log`` filtered by
        the ``"scheduler_event"`` key is therefore the correct and complete
        source of truth for structured scheduler diagnostics.

        The event types that produce records here are:
        ``isolated_tick_failure``, ``tick_parked``, ``empty_output``,
        ``empty_parent_snapshot``, ``fjord_flush_failure``,
        ``watershed_started``, and ``watershed_completed``.

        Records are sorted ascending by ``tide_number`` (``None`` → ``0``),
        then by ``event_type``, then by ``current_name`` for deterministic
        ordering when multiple events share a tide number.

        Args:
            logger_name: The name used when the :class:`LoggedTideweaver` was
                constructed (e.g. ``"PriceSession"``).  Controls which
                ``logs/<logger_name>_error.log`` file is read.

        Returns:
            List of scheduler-event record dicts sorted ascending by
            ``tide_number``.  Each dict contains a top-level
            ``"scheduler_event"`` key whose value includes ``event_type``,
            ``current_name``, ``cls_name``, ``tide_number``, ``session``, and
            ``detail``.  Returns an empty list when no matching records exist.

        Example::

            events = await LoggedTideweaver.get_scheduler_events("PriceSession")
            for rec in events:
                evt = rec["scheduler_event"]
                print(evt["event_type"], evt["current_name"], evt["tide_number"])
        """
        records = await read_log(logger_name, ["error"], key="scheduler_event")
        return sorted(
            records,
            key=lambda r: (
                r.get("scheduler_event", {}).get("tide_number") or 0,
                r.get("scheduler_event", {}).get("event_type") or "",
                r.get("scheduler_event", {}).get("current_name") or "",
            ),
        )

    @classmethod
    async def get_current(cls, logger_name: str, code: str) -> list[dict[str, Any]]:
        """Return all records tagged with *code* in their ``meta`` field for a named session.

        Per-current view that unions ``api.log``, ``error.log``, and
        ``debug.log`` for the session identified by *logger_name*, returning
        only records whose ``meta`` string contains *code*.  Records with no
        ``meta`` field are excluded.

        Args:
            logger_name: The name used when the :class:`LoggedTideweaver` was
                constructed (e.g. ``"PriceSession"``).
            code: Substring to search for inside each record's ``meta`` field.

        Returns:
            List of record dicts whose ``meta`` contains *code*, from
            ``api.log``, ``error.log``, and ``debug.log`` in that order.
            Returns ``[]`` when no matching records exist.

        Example::

            records = await LoggedTideweaver.get_current("PriceSession", "abc123")
            for rec in records:
                print(rec["level"], rec["msg"])
        """
        return await read_log(logger_name, ["api", "error", "debug"], meta_contains=code)
