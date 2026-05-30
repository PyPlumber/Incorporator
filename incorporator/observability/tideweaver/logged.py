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

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from ..logger import _read_filtered, _route_reject_to_log, _route_tide_to_log, _safe_log_filename, setup_class_logger
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
            Defaults to ``"Tideweaver"``.  Two :class:`LoggedTideweaver`
            instances sharing the same ``logger_name`` share one log-file set,
            mirroring the existing per-class-name behaviour for
            :class:`~incorporator.observability.logger.LoggedIncorporator`.

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
        logger_name: str = "Tideweaver",
    ) -> None:
        super().__init__(
            watershed,
            tick_factory=tick_factory,
            pass_interval=pass_interval,
            backlog_backoff_factor=backlog_backoff_factor,
        )
        self._enable_logging = enable_logging
        self._logger_name = logger_name
        if enable_logging:
            setup_class_logger(logger_name)

    async def run(self) -> AsyncIterator[Tide]:
        """Orchestrate the watershed, routing each Tide and final RejectEntries to disk.

        Mirrors :meth:`Tideweaver.run` exactly — same yield shape, same window
        semantics, same drain-on-exit behaviour — with the addition that when
        ``enable_logging=True``:

        - Each yielded :class:`Tide` is routed to INFO / ERROR / DEBUG via
          :func:`~incorporator.observability.logger._route_tide_to_log`.
        - All accumulated :class:`~incorporator.RejectEntry` records are swept
          in a ``finally`` block via
          :func:`~incorporator.observability.logger._route_reject_to_log` so
          they land on disk even under cancellation.

        Yields:
            :class:`Tide` — same shape as :meth:`Tideweaver.run`.
        """
        try:
            async for tide in super().run():
                if self._enable_logging:
                    _route_tide_to_log(self._logger_name, tide)
                yield tide
        finally:
            if self._enable_logging:
                for reject in self.rejects:
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
        filename = _safe_log_filename(logger_name, "tide.log")
        records = await asyncio.to_thread(_read_filtered, filename, "tide")
        return sorted(records, key=lambda r: r.get("tide", {}).get("tide_number", 0))

    @classmethod
    async def get_rejects(cls, logger_name: str) -> list[dict[str, Any]]:
        """Return all reject records from ``error.log`` for ``logger_name``.

        Overrides :meth:`~incorporator.observability.logger.LoggingMixin.get_rejects`
        solely for name resolution: :class:`LoggedTideweaver` uses an
        instance-level ``logger_name`` rather than ``cls.__name__``, so the
        correct log file cannot be determined from the class alone.  The
        underlying read is identical — ``_read_filtered(<name>_error.log, "reject")``.

        Args:
            logger_name: The name used when the :class:`LoggedTideweaver` was
                constructed (e.g. ``"PriceSession"``).

        Returns:
            List of reject-record dicts from
            ``logs/<logger_name>_error.log``.  Each dict contains a
            top-level ``"reject"`` key.  Returns an empty list when no
            log file exists yet.

        Example::

            rejects = await LoggedTideweaver.get_rejects("PriceSession")
            for rec in rejects:
                print(rec["reject"]["source"], rec["reject"]["error_kind"])
        """
        filename = _safe_log_filename(logger_name, "error.log")
        return await asyncio.to_thread(_read_filtered, filename, "reject")
