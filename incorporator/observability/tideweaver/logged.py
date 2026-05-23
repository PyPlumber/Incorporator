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
    # logs/PriceSession_error.log  (canal rejects + error-class skips)
    # logs/PriceSession_debug.log  (all passes including no-ops)
"""

from __future__ import annotations

from typing import AsyncIterator, Optional

from ..logger import _route_reject_to_log, _route_tide_to_log, setup_class_logger
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
        tick_factory: Optional[TickFactory] = None,
        pass_interval: Optional[float] = None,
        enable_logging: bool = False,
        logger_name: str = "Tideweaver",
    ) -> None:
        super().__init__(watershed, tick_factory=tick_factory, pass_interval=pass_interval)
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
