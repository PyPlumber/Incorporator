"""Benchmark: ``_route_to_log`` per-Wave cost -- the Stage-3B BEFORE baseline.

Stage 3B will hoist the ``isEnabledFor`` gate in
``incorporator/observability/logger.py::_route_to_log`` (Wave branch) ahead
of the eager work it currently does for every record: ``record.model_dump(
mode="json")``, the ``_redact`` list-comprehension over ``failed_sources``,
``log_meta()``, and the msg f-strings -- all of which run today BEFORE
``_emit_payload``'s ``isEnabledFor`` check.  Zero-row / zero-failure waves
pay the dump-then-discard cost and then hit the early ``return`` without
ever emitting.  ``_emit_payload`` also calls ``logging.getLogger(logger_name)``
on every record.

This file measures that BEFORE state so Stage 3B's commit message can cite
a real before/after pair from the same suite.  Stage 3B must not lower
these floors -- only a Stage-3B follow-up benchmark run may report improved
numbers against this same suite.

Three scenarios:

* **Suppressed** -- logger level set above CRITICAL, no handler.  Nothing
  emits, but today's dump/redact/log_meta cost still runs first.  This is
  where 3B's win will show.
* **Emitting** -- logger level DEBUG with one ``NullHandler`` attached (the
  cheapest possible real sink).  Guards that 3B's hoist doesn't regress the
  already-emitting path.
* **Zero-row** -- rows_processed=0, failed_sources=[] waves under a
  suppressed-style logger.  Isolates the dump-then-discard cost that the
  no-op branch pays before its early ``return``.

Floors picked from local calibration (slowest of 3 runs, hardware
contention on this host means we lean toward 0.6x rather than 0.7x -- see
``tests/benchmarks/test_penstock_overhead.py`` / ``test_scheduler_pass_
overhead.py`` for the standard convention this follows).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from incorporator.observability.logger import _route_to_log
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry

N_WAVES = 20_000

# Floors calibrated from 3 local runs (slowest of 3 x ~0.6, rounded down to
# a friendly number) -- this host is a 4-core i7-3770K that regularly runs
# concurrent Claude sessions; the v1.4.2 pre-release saw rotating 3-19%
# misses from host contention alone even with generous floors, so we lean
# conservative (0.6x) here rather than trust a single run.
#
# Measured (3 runs): suppressed 47,706 / 49,746 / 48,908 waves/sec;
# emitting 17,770 / 19,102 / 18,975 waves/sec; zero-row 77,393 / 77,830 /
# 77,417 waves/sec.  Slowest-of-3: suppressed 47,706, emitting 17,770,
# zero-row 77,393.
SUPPRESSED_FLOOR_WAVES_PER_SEC = 25_000  # 47,706 x 0.6 = 28,624 -> floor 25,000
EMITTING_FLOOR_WAVES_PER_SEC = 10_000  # 17,770 x 0.6 = 10,662 -> floor 10,000
ZERO_ROW_FLOOR_WAVES_PER_SEC = 40_000  # 77,393 x 0.6 = 46,436 -> floor 40,000


@contextmanager
def _isolated_logger(name: str, level: int, *, attach_null_handler: bool = False) -> Iterator[logging.Logger]:
    """Yield a scenario-scoped logger, restoring its state on exit.

    Sets ``propagate = False`` so records never bubble into a root/caplog
    handler (which would add unrelated cost and break isolation between
    scenarios), sets the requested level, and optionally attaches a single
    ``NullHandler`` for the emitting scenario.  Restores ``propagate = True``,
    clears handlers, and resets the level to ``NOTSET`` in a ``finally`` so
    no benchmark scenario leaks logger state into another test.
    """
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    if attach_null_handler:
        logger.addHandler(logging.NullHandler())
    try:
        yield logger
    finally:
        logger.propagate = True
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)


def _make_success_wave(i: int) -> Wave:
    """Build a realistic successful Wave -- non-zero rows, no failures."""
    return Wave.model_construct(
        chunk_index=i,
        operation="stream",
        rows_processed=100,
        failed_sources=[],
        rejects=[],
        processing_time_sec=0.05,
        source_url="https://api.example.com/data",
        bytes_processed=4096,
        bytes_downloaded=2048,
        http_fetch_time_sec=0.02,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=0.01,
    )


def _make_failure_wave(i: int) -> Wave:
    """Build a realistic failure Wave -- a failed source (exercises ``_redact``) plus one RejectEntry."""
    failing_url = "https://api.example.com/data?api_key=SECRET1234&page=2"
    reject = RejectEntry.model_construct(
        source=failing_url,
        error_kind="HTTPStatusError",
        message="500 Internal Server Error",
        retry_after=None,
        wave_index=i,
        from_name=None,
        to_name=None,
        host="api.example.com",
        status_code=500,
        attempt_number=3,
        duration_sec=0.5,
        cooldown_sec=None,
        is_url_traffic_error=True,
    )
    return Wave.model_construct(
        chunk_index=i,
        operation="stream",
        rows_processed=50,
        failed_sources=[failing_url],
        rejects=[reject],
        processing_time_sec=0.08,
        source_url="https://api.example.com/data",
        bytes_processed=2048,
        bytes_downloaded=1024,
        http_fetch_time_sec=0.03,
        http_retry_count=3,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=0.01,
    )


def _make_zero_row_wave(i: int) -> Wave:
    """Build a zero-row / zero-failure Wave -- exercises the dump-then-discard no-op branch."""
    return Wave.model_construct(
        chunk_index=i,
        operation="stream",
        rows_processed=0,
        failed_sources=[],
        rejects=[],
        processing_time_sec=0.001,
        source_url=None,
        bytes_processed=None,
        bytes_downloaded=None,
        http_fetch_time_sec=None,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=None,
    )


def _build_waves(n: int) -> list[Wave]:
    """Pre-build ``n`` Waves outside any timed section -- 4:1 success:failure ratio.

    Constructed before the ``time.perf_counter()`` bracket in each scenario
    so only ``_route_to_log``'s cost is measured, matching
    ``_drive_penstock``'s fixture-then-time pattern.
    """
    waves: list[Wave] = []
    for i in range(n):
        waves.append(_make_failure_wave(i) if i % 5 == 4 else _make_success_wave(i))
    return waves


def _build_zero_row_waves(n: int) -> list[Wave]:
    """Pre-build ``n`` zero-row Waves outside any timed section."""
    return [_make_zero_row_wave(i) for i in range(n)]


def _report(name: str, logger_name: str, waves: list[Wave], floor: float) -> float:
    """Drive ``_route_to_log`` over ``waves``, print waves/sec, assert the floor.

    Returns the measured waves/sec.
    """
    t0 = time.perf_counter()
    for wave in waves:
        _route_to_log(logger_name, wave)
    elapsed = time.perf_counter() - t0
    waves_per_sec = len(waves) / elapsed
    print(f"\n  _route_to_log {name:<24} {len(waves):,} waves in {elapsed:.3f}s = {waves_per_sec:,.0f} waves/sec")
    assert waves_per_sec >= floor, (
        f"{name} dropped to {waves_per_sec:,.0f} waves/sec (floor: {floor:,.0f}). "
        "This is the Stage-3B isEnabledFor-hoist baseline suite -- a "
        "regression here means _route_to_log's per-record cost (model_dump, "
        "log_meta, _redact) grew, or the pre-guard eager work Stage 3B "
        "hoists past isEnabledFor was reintroduced."
    )
    return waves_per_sec


@pytest.mark.benchmark
def test_wave_logging_overhead_suppressed() -> None:
    """SUPPRESSED path: N Waves through _route_to_log with logging fully disabled.

    Logger level is set above CRITICAL so ``isEnabledFor`` is False for both
    the INFO-success and ERROR-failure branches -- but today's model_dump +
    _redact + log_meta cost all runs BEFORE that check is ever consulted
    inside ``_emit_payload``.  This is the pre-guard serialization waste
    Stage 3B's hoist eliminates.
    """
    waves = _build_waves(N_WAVES)
    with _isolated_logger("_bench_wave_logging_suppressed", logging.CRITICAL + 10):
        _report("suppressed", "_bench_wave_logging_suppressed", waves, SUPPRESSED_FLOOR_WAVES_PER_SEC)


@pytest.mark.benchmark
def test_wave_logging_overhead_emitting() -> None:
    """EMITTING path: N Waves through _route_to_log with logging fully enabled.

    Logger level DEBUG with a single ``NullHandler`` (its ``.handle()`` is a
    no-op override that skips filtering/formatting -- the cheapest possible
    real sink) so the measurement stays dominated by ``_route_to_log``'s own
    cost.  Guards that Stage 3B's hoist doesn't regress the already-emitting
    path.
    """
    waves = _build_waves(N_WAVES)
    with _isolated_logger("_bench_wave_logging_emitting", logging.DEBUG, attach_null_handler=True):
        _report("emitting", "_bench_wave_logging_emitting", waves, EMITTING_FLOOR_WAVES_PER_SEC)


@pytest.mark.benchmark
def test_wave_logging_overhead_zero_row() -> None:
    """Zero-row / zero-failure waves: dump-then-discard cost before the no-op early return.

    ``_route_to_log`` still pays ``model_dump`` + the ``_redact``
    comprehension + ``log_meta()`` for these waves before hitting the
    no-op branch's early ``return`` -- this scenario isolates that cost
    specifically, under a suppressed-style logger so nothing actually
    emits either way.
    """
    waves = _build_zero_row_waves(N_WAVES)
    with _isolated_logger("_bench_wave_logging_zero_row", logging.CRITICAL + 10):
        _report("zero-row", "_bench_wave_logging_zero_row", waves, ZERO_ROW_FLOOR_WAVES_PER_SEC)
