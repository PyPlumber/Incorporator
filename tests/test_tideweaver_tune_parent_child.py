"""Tests for _tune_parent_child() and its dispatch wiring in tune().

Each test proves exactly one behaviour — the docstring states what behaviour
that is.
"""

from __future__ import annotations

from datetime import datetime, timezone

from incorporator.observability.tideweaver.architect import (
    _tune_parent_child,
    tune,
)
from incorporator.observability.tideweaver.tide import Tide
from incorporator.observability.wave import Wave

_UTC = timezone.utc
_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=_UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tide(fired: list[str] | None = None) -> Tide:
    """Build a minimal Tide with the given fired list."""
    return Tide.model_construct(
        tide_number=1,
        fired=fired or [],
        skipped=[],
        current_outcomes=[],
        duration_sec=0.01,
        wake_reason="timer",
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=None,
        timestamp=_NOW,
    )


def _wave(
    filter_match_count: int | None = None,
    parent_snapshot_size: int | None = None,
) -> Wave:
    """Build a minimal Wave with the given parent-child fields."""
    return Wave.model_construct(
        chunk_index=0,
        operation="stream",
        rows_processed=10,
        failed_sources=[],
        processing_time_sec=0.02,
        source_url="https://api.example.com/data",
        bytes_processed=None,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=None,
        filter_match_count=filter_match_count,
        parent_snapshot_size=parent_snapshot_size,
        timestamp=_NOW,
    )


# ---------------------------------------------------------------------------
# _tune_parent_child unit tests
# ---------------------------------------------------------------------------


def test_tune_parent_child_zero_filter_match_fires_high() -> None:
    """3 waves with filter_match_count=0 produce exactly one HIGH hint with knob='parent_filter'."""
    waves = [_wave(filter_match_count=0, parent_snapshot_size=5) for _ in range(3)]
    tides = [_tide() for _ in range(6)]
    hints = _tune_parent_child(tides, waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert h.knob == "parent_filter"
    assert h.scope == {"global": "watershed"}
    assert h.sample_size == 3


def test_tune_parent_child_zero_parent_snapshot_fires_high() -> None:
    """3 waves with parent_snapshot_size=0 produce exactly one HIGH hint with knob='parent_current'."""
    waves = [_wave(filter_match_count=None, parent_snapshot_size=0) for _ in range(3)]
    tides = [_tide() for _ in range(6)]
    hints = _tune_parent_child(tides, waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert h.knob == "parent_current"
    assert h.scope == {"global": "watershed"}
    assert h.sample_size == 3


def test_tune_parent_child_frequent_fire_zero_waves_fires_med() -> None:
    """6 tides each with fired=['child_stream'] and no waves produce a MED hint for 'child_stream'."""
    tides = [_tide(fired=["child_stream"]) for _ in range(6)]
    hints = _tune_parent_child(tides, waves=[])
    med_hints = [h for h in hints if h.severity == "med"]
    assert len(med_hints) >= 1
    h = med_hints[0]
    assert h.knob == "parent_current"
    assert h.scope == {"current": "child_stream"}
    assert "child_stream" in h.signal
    assert "6" in h.signal


def test_tune_parent_child_healthy_no_hints() -> None:
    """6 waves with non-zero parent-child fields and 6 tides produce no hints."""
    waves = [_wave(filter_match_count=3, parent_snapshot_size=10) for _ in range(6)]
    tides = [_tide(fired=["child_stream"]) for _ in range(6)]
    hints = _tune_parent_child(tides, waves)
    assert hints == []


# ---------------------------------------------------------------------------
# tune() integration check (optional 5th test)
# ---------------------------------------------------------------------------


def test_tune_dispatch_parent_child_hint_present() -> None:
    """tune(tides=tides, waves=[]) returns a TuningReport containing a parent_current hint."""
    tides = [_tide(fired=["child_stream"]) for _ in range(6)]
    report = tune(tides=tides, waves=[])
    parent_hints = [h for h in report.hints if h.knob == "parent_current"]
    assert len(parent_hints) >= 1
    assert parent_hints[0].severity == "med"
