"""Tests for Wave and Tide field-equality between validated and model_construct paths.

Verifies:
- Both construction paths produce field-equal records (same values).
- log_meta() is identical for both paths.
- model_dump() is identical for both paths.
- Frozen invariant: assigning to a field raises ValidationError.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest
from pydantic import ValidationError

from incorporator.tideweaver.current_outcome import CurrentOutcome
from incorporator.tideweaver.reasons import SkipReason, WakeReason
from incorporator.tideweaver.tide import Tide
from incorporator.observability.wave import Wave


# ---------------------------------------------------------------------------
# Wave: validated vs model_construct
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _wave_validated(**overrides: object) -> Wave:
    """Build a Wave with all new fields supplied explicitly via validated constructor."""
    kwargs = dict(
        chunk_index=1,
        operation="chunk",
        rows_processed=42,
        failed_sources=[],
        processing_time_sec=0.123,
        source_url="https://api.example.com/data",
        bytes_processed=4096,
        bytes_downloaded=None,
        http_fetch_time_sec=None,
        http_retry_count=2,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=0.005,
        timestamp=_NOW,
    )
    kwargs.update(overrides)  # type: ignore[arg-type]
    return Wave(**kwargs)  # type: ignore[arg-type]


def _wave_construct(**overrides: object) -> Wave:
    """Build the same Wave via model_construct (no validation)."""
    kwargs = dict(
        chunk_index=1,
        operation="chunk",
        rows_processed=42,
        failed_sources=[],
        processing_time_sec=0.123,
        source_url="https://api.example.com/data",
        bytes_processed=4096,
        bytes_downloaded=None,
        http_fetch_time_sec=None,
        http_retry_count=2,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=0.005,
        timestamp=_NOW,
    )
    kwargs.update(overrides)  # type: ignore[arg-type]
    return Wave.model_construct(**kwargs)  # type: ignore[arg-type]


def test_wave_validated_vs_construct_field_equal() -> None:
    """Validated and model_construct Wave produce identical field values."""
    validated = _wave_validated()
    constructed = _wave_construct()
    assert validated.chunk_index == constructed.chunk_index
    assert validated.operation == constructed.operation
    assert validated.rows_processed == constructed.rows_processed
    assert validated.failed_sources == constructed.failed_sources
    assert validated.processing_time_sec == constructed.processing_time_sec
    assert validated.source_url == constructed.source_url
    assert validated.bytes_processed == constructed.bytes_processed
    assert validated.http_retry_count == constructed.http_retry_count
    assert validated.validation_error_count == constructed.validation_error_count
    assert validated.schema_cache_hit == constructed.schema_cache_hit
    assert validated.conv_dict_time_sec == constructed.conv_dict_time_sec
    assert validated.timestamp == constructed.timestamp


def test_wave_log_meta_identical_both_paths() -> None:
    """log_meta() output is the same for validated and model_construct Wave."""
    validated = _wave_validated()
    constructed = _wave_construct()
    assert validated.log_meta() == constructed.log_meta()


def test_wave_model_dump_identical_both_paths() -> None:
    """model_dump() is the same for validated and model_construct Wave."""
    validated = _wave_validated()
    constructed = _wave_construct()
    assert validated.model_dump() == constructed.model_dump()


def test_wave_is_frozen() -> None:
    """Assigning to a Wave field raises ValidationError (frozen model)."""
    wave = _wave_validated()
    with pytest.raises(ValidationError):
        wave.chunk_index = 99  # type: ignore[misc]


def test_wave_new_fields_have_correct_defaults() -> None:
    """New fields default to the documented safe values when omitted."""
    wave = Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)
    assert wave.source_url is None
    assert wave.bytes_processed is None
    assert wave.http_retry_count == 0
    assert wave.validation_error_count == 0
    assert wave.schema_cache_hit is True
    assert wave.conv_dict_time_sec is None


# ---------------------------------------------------------------------------
# Tide: validated vs model_construct
# ---------------------------------------------------------------------------

_OUTCOMES: List[CurrentOutcome] = [
    CurrentOutcome(name="coin", status="fired"),
    CurrentOutcome(name="arb", status="skipped", reason="not_due"),
]


def _tide_validated(**overrides: object) -> Tide:
    """Build a Tide with all new fields supplied explicitly via validated constructor."""
    kwargs = dict(
        tide_number=7,
        fired=["coin"],
        skipped=[("arb", "not_due")],
        current_outcomes=_OUTCOMES,
        duration_sec=0.042,
        wake_reason="timer",
        heap_depth=3,
        in_flight_count_at_start=1,
        canal_rejects_added=0,
        next_due_in_sec=5.0,
        timestamp=_NOW,
    )
    kwargs.update(overrides)  # type: ignore[arg-type]
    return Tide(**kwargs)  # type: ignore[arg-type]


def _tide_construct(**overrides: object) -> Tide:
    """Build the same Tide via model_construct (no validation)."""
    kwargs = dict(
        tide_number=7,
        fired=["coin"],
        skipped=[("arb", SkipReason.NOT_DUE)],
        current_outcomes=_OUTCOMES,
        duration_sec=0.042,
        wake_reason=WakeReason.TIMER,
        heap_depth=3,
        in_flight_count_at_start=1,
        canal_rejects_added=0,
        next_due_in_sec=5.0,
        timestamp=_NOW,
    )
    kwargs.update(overrides)  # type: ignore[arg-type]
    return Tide.model_construct(**kwargs)  # type: ignore[arg-type]


def test_tide_validated_vs_construct_field_equal() -> None:
    """Validated and model_construct Tide produce identical field values."""
    validated = _tide_validated()
    constructed = _tide_construct()
    assert validated.tide_number == constructed.tide_number
    assert validated.fired == constructed.fired
    assert validated.skipped == constructed.skipped
    assert validated.current_outcomes == constructed.current_outcomes
    assert validated.duration_sec == constructed.duration_sec
    assert validated.wake_reason == constructed.wake_reason
    assert validated.heap_depth == constructed.heap_depth
    assert validated.in_flight_count_at_start == constructed.in_flight_count_at_start
    assert validated.canal_rejects_added == constructed.canal_rejects_added
    assert validated.next_due_in_sec == constructed.next_due_in_sec
    assert validated.timestamp == constructed.timestamp


def test_tide_log_meta_identical_both_paths() -> None:
    """log_meta() output is the same for validated and model_construct Tide."""
    validated = _tide_validated()
    constructed = _tide_construct()
    assert validated.log_meta() == constructed.log_meta()


def test_tide_model_dump_identical_both_paths() -> None:
    """model_dump(mode='json') is the same for validated and model_construct Tide.

    This verifies the @field_serializer fires in both paths, converting
    CurrentOutcome dataclasses to plain dicts.
    """
    validated = _tide_validated()
    constructed = _tide_construct()
    assert validated.model_dump(mode="json") == constructed.model_dump(mode="json")


def test_tide_is_frozen_validated() -> None:
    """Assigning to a Tide field raises ValidationError (frozen model)."""
    tide = _tide_validated()
    with pytest.raises(ValidationError):
        tide.tide_number = 99  # type: ignore[misc]


def test_tide_new_fields_have_correct_defaults() -> None:
    """New Tide fields default to the documented safe values when omitted."""
    tide = Tide(tide_number=1, duration_sec=0.0)
    assert tide.current_outcomes == []
    assert tide.wake_reason == "startup"
    assert tide.heap_depth == 0
    assert tide.in_flight_count_at_start == 0
    assert tide.canal_rejects_added == 0
    assert tide.next_due_in_sec is None


def test_tide_back_compat_fired_skipped_still_populated() -> None:
    """fired and skipped lists remain populated alongside current_outcomes — back-compat guarantee."""
    tide = _tide_validated()
    assert tide.fired == ["coin"]
    assert tide.skipped == [("arb", "not_due")]
    # current_outcomes carries the same information in structured form.
    names = {co.name for co in tide.current_outcomes}
    assert "coin" in names
    assert "arb" in names
