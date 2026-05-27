"""Benchmark: model_construct throughput for Wave, Tide, and RejectEntry.

Measures 1,000,000 constructions of each telemetry value type via the
``model_construct`` path (the hot path in the scheduler and stream loops).
No threshold assertion — the numbers are informational.  The conftest.py in
this directory auto-marks every test here with ``@pytest.mark.benchmark``,
so the default test run skips this suite.
"""

from __future__ import annotations

import time

import pytest

from incorporator.observability.tideweaver.tide import Tide
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry

N = 1_000_000


@pytest.mark.benchmark
def test_wave_construct_throughput() -> None:
    """1M Wave.model_construct calls — measures construction overhead after slots=True."""
    t0 = time.perf_counter()
    for i in range(N):
        Wave.model_construct(
            chunk_index=i,
            operation="stream",
            rows_processed=10,
            failed_sources=[],
            processing_time_sec=0.1,
            source_url=None,
            bytes_processed=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
        )
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  Wave.model_construct 1M: {elapsed:.3f}s = {us_per:.2f} µs/instance")


@pytest.mark.benchmark
def test_tide_construct_throughput() -> None:
    """1M Tide.model_construct calls — measures construction overhead after slots=True."""
    t0 = time.perf_counter()
    for i in range(N):
        Tide.model_construct(
            tide_number=i,
            fired=[],
            skipped=[],
            current_outcomes=[],
            duration_sec=0.01,
            wake_reason="timer",
            heap_depth=0,
            in_flight_count_at_start=0,
            canal_rejects_added=0,
            next_due_in_sec=None,
        )
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  Tide.model_construct 1M: {elapsed:.3f}s = {us_per:.2f} µs/instance")


@pytest.mark.benchmark
def test_reject_entry_construct_throughput() -> None:
    """1M RejectEntry.model_construct calls — measures construction overhead after slots=True."""
    t0 = time.perf_counter()
    for i in range(N):
        RejectEntry.model_construct(
            source="https://example.com/api",
            error_kind="Unknown",
            message="",
            retry_after=None,
            wave_index=None,
            from_name=None,
            to_name=None,
            host=None,
            status_code=None,
            attempt_number=None,
            duration_sec=None,
            cooldown_sec=None,
        )
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  RejectEntry.model_construct 1M: {elapsed:.3f}s = {us_per:.2f} µs/instance")
