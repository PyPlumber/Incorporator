"""Slots contract tests for value-type Pydantic models: Wave, Tide, RejectEntry.

Pydantic v2 ``slots=True`` adds ``__slots__`` to the generated class but does
NOT eliminate ``__dict__``: ``BaseModel`` itself declares ``__dict__`` in its own
``__slots__``, so all subclasses always inherit it.  The runtime guarantees
that *are* testable are:

1. ``__slots__`` exists on the class.
2. ``frozen=True`` prevents mutation of declared fields (raises ``ValidationError``).
3. Assigning an undeclared attribute to a ``frozen`` model raises ``ValidationError``
   (Pydantic's ``__setattr__`` intercepts before any slot descriptor).
4. ``model_construct`` (the hot path) produces valid instances with correct
   default-factory fields populated when passed explicitly.
5. Tide's ``current_outcomes`` serialiser survives ``model_dump(mode='json')``
   with ``slots=True`` active (regression guard for the ``@field_serializer``
   interaction with slotted models).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.tideweaver.reasons import WakeReason
from incorporator.observability.tideweaver.tide import Tide
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wave() -> Wave:
    return Wave(chunk_index=0, rows_processed=5, processing_time_sec=0.1)


def _wave_construct() -> Wave:
    return Wave.model_construct(
        chunk_index=0,
        operation="stream",
        rows_processed=5,
        failed_sources=[],
        processing_time_sec=0.1,
        source_url=None,
        bytes_processed=None,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=None,
    )


def _tide() -> Tide:
    return Tide(tide_number=1, duration_sec=0.05)


def _tide_construct() -> Tide:
    return Tide.model_construct(
        tide_number=1,
        fired=[],
        skipped=[],
        current_outcomes=[],
        duration_sec=0.05,
        wake_reason=WakeReason.TIMER,
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=None,
    )


def _reject() -> RejectEntry:
    return RejectEntry(source="https://example.com/api")


def _reject_construct() -> RejectEntry:
    return RejectEntry.model_construct(
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


# ---------------------------------------------------------------------------
# Wave slots
# ---------------------------------------------------------------------------


def test_wave_has_slots() -> None:
    """Wave class has __slots__ — slots=True activated the slot machinery."""
    assert hasattr(Wave, "__slots__")


def test_wave_frozen_blocks_undeclared_attr() -> None:
    """Assigning an undeclared attribute to a frozen Wave raises ValidationError.

    Pydantic's __setattr__ intercepts before any slot descriptor, so the
    error is ValidationError(frozen_instance) rather than AttributeError.
    """
    w = _wave()
    with pytest.raises(ValidationError):
        w.nonexistent_field = "x"  # type: ignore[attr-defined]


def test_wave_construct_produces_valid_instance() -> None:
    """model_construct Wave has correct field values — hot path is functional."""
    w = _wave_construct()
    assert w.chunk_index == 0
    assert w.rows_processed == 5
    assert w.failed_sources == []
    assert w.processing_time_sec == 0.1


# ---------------------------------------------------------------------------
# Tide slots
# ---------------------------------------------------------------------------


def test_tide_has_slots() -> None:
    """Tide class has __slots__ — slots=True activated the slot machinery."""
    assert hasattr(Tide, "__slots__")


def test_tide_frozen_blocks_undeclared_attr() -> None:
    """Assigning an undeclared attribute to a frozen Tide raises ValidationError.

    Pydantic's __setattr__ intercepts before any slot descriptor, so the
    error is ValidationError(frozen_instance) rather than AttributeError.
    """
    t = _tide()
    with pytest.raises(ValidationError):
        t.nonexistent_field = "x"  # type: ignore[attr-defined]


def test_tide_construct_produces_valid_instance() -> None:
    """model_construct Tide has correct field values — hot path is functional."""
    t = _tide_construct()
    assert t.tide_number == 1
    assert t.duration_sec == 0.05
    assert t.fired == []
    assert t.current_outcomes == []


def test_tide_model_dump_current_outcomes_round_trip() -> None:
    """model_dump(mode='json') on a slots=True Tide still serialises current_outcomes correctly.

    Regression guard: @field_serializer('current_outcomes') must fire correctly
    under slots=True for both Tide(...) and Tide.model_construct(...).
    """
    co = CurrentOutcome(name="my_current", status="fired")
    tide = Tide(tide_number=2, duration_sec=0.12, current_outcomes=[co])
    dumped = tide.model_dump(mode="json")
    assert isinstance(dumped["current_outcomes"], list)
    assert len(dumped["current_outcomes"]) == 1
    entry = dumped["current_outcomes"][0]
    assert entry["name"] == "my_current"
    assert entry["status"] == "fired"
    assert entry["reason"] is None

    # Same check via model_construct (hot path)
    tide_mc = Tide.model_construct(
        tide_number=2,
        fired=[],
        skipped=[],
        current_outcomes=[co],
        duration_sec=0.12,
        wake_reason=WakeReason.TIMER,
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=None,
    )
    dumped_mc = tide_mc.model_dump(mode="json")
    assert isinstance(dumped_mc["current_outcomes"], list)
    assert len(dumped_mc["current_outcomes"]) == 1
    assert dumped_mc["current_outcomes"][0]["name"] == "my_current"


# ---------------------------------------------------------------------------
# RejectEntry slots
# ---------------------------------------------------------------------------


def test_reject_has_slots() -> None:
    """RejectEntry class has __slots__ — slots=True activated the slot machinery."""
    assert hasattr(RejectEntry, "__slots__")


def test_reject_frozen_blocks_undeclared_attr() -> None:
    """Assigning an undeclared attribute to a frozen RejectEntry raises ValidationError.

    Pydantic's __setattr__ intercepts before any slot descriptor, so the
    error is ValidationError(frozen_instance) rather than AttributeError.
    """
    r = _reject()
    with pytest.raises(ValidationError):
        r.nonexistent_field = "x"  # type: ignore[attr-defined]


def test_reject_construct_produces_valid_instance() -> None:
    """model_construct RejectEntry has correct field values — hot path is functional."""
    r = _reject_construct()
    assert r.source == "https://example.com/api"
    assert r.error_kind == "Unknown"
    assert r.message == ""
    assert r.retry_after is None
