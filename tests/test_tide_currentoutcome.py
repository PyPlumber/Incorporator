"""Tests for :class:`CurrentOutcome` dataclass and its integration with :class:`Tide`.

Covers:
- CurrentOutcome construction, frozen invariants, equality, and __str__.
- Tide.model_dump(mode="json") round-trip: the @field_serializer converts
  CurrentOutcome dataclasses to dicts.
- Serializer fires under both Tide(...) (validated) and Tide.model_construct(...).
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from typing import List

import pytest

from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.tideweaver.tide import Tide


# ---------------------------------------------------------------------------
# CurrentOutcome dataclass behaviour
# ---------------------------------------------------------------------------


def test_current_outcome_minimum_construction() -> None:
    """A bare CurrentOutcome with just name and status leaves optionals as defaults."""
    co = CurrentOutcome(name="coin", status="fired")
    assert co.name == "coin"
    assert co.status == "fired"
    assert co.reason is None
    assert co.bypassed_edges == ()
    assert co.in_flight_sec is None
    assert co.last_wave_at is None


def test_current_outcome_full_construction() -> None:
    """All fields round-trip cleanly."""
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    co = CurrentOutcome(
        name="arb",
        status="skipped",
        reason="still_running",
        bypassed_edges=("up1", "up2"),
        in_flight_sec=3.14,
        last_wave_at=ts,
    )
    assert co.name == "arb"
    assert co.status == "skipped"
    assert co.reason == "still_running"
    assert co.bypassed_edges == ("up1", "up2")
    assert co.in_flight_sec == pytest.approx(3.14)
    assert co.last_wave_at == ts


def test_current_outcome_is_frozen() -> None:
    """Frozen dataclass: assigning to a field raises FrozenInstanceError."""
    co = CurrentOutcome(name="x", status="fired")
    with pytest.raises(dataclasses.FrozenInstanceError):
        co.name = "mutated"  # type: ignore[misc]


def test_current_outcome_equality() -> None:
    """Two CurrentOutcomes with the same fields are equal."""
    a = CurrentOutcome(name="coin", status="fired", reason=None)
    b = CurrentOutcome(name="coin", status="fired", reason=None)
    assert a == b


def test_current_outcome_inequality() -> None:
    """Different status makes CurrentOutcomes unequal."""
    a = CurrentOutcome(name="coin", status="fired")
    b = CurrentOutcome(name="coin", status="skipped")
    assert a != b


def test_current_outcome_str_no_reason() -> None:
    """__str__ returns 'name:status' when reason is None."""
    co = CurrentOutcome(name="coin", status="fired")
    assert str(co) == "coin:fired"


def test_current_outcome_str_with_reason() -> None:
    """__str__ returns 'name:status(reason)' when reason is present."""
    co = CurrentOutcome(name="arb", status="skipped", reason="not_due")
    assert str(co) == "arb:skipped(not_due)"


def test_current_outcome_slots() -> None:
    """slots=True: attempting to set an arbitrary attribute raises an error.

    The combination of frozen=True and slots=True prevents attribute setting.
    Depending on CPython version and the attribute name, the raised exception
    may be FrozenInstanceError, AttributeError, or TypeError — the critical
    invariant is that the assignment raises rather than silently succeeding.
    """
    import dataclasses

    co = CurrentOutcome(name="x", status="fired")
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError, TypeError)):
        co.arbitrary_field = "should_fail"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tide + CurrentOutcome integration
# ---------------------------------------------------------------------------


def _sample_outcomes() -> List[CurrentOutcome]:
    """Build a small set of outcomes for Tide construction tests."""
    return [
        CurrentOutcome(name="binance", status="fired"),
        CurrentOutcome(name="kraken", status="skipped", reason="not_due"),
        CurrentOutcome(name="arb", status="still_running", in_flight_sec=2.5),
    ]


def test_tide_carries_current_outcomes_validated() -> None:
    """Tide(...) with current_outcomes stores the list correctly."""
    outcomes = _sample_outcomes()
    tide = Tide(tide_number=1, duration_sec=0.01, current_outcomes=outcomes)
    assert tide.current_outcomes == outcomes
    assert len(tide.current_outcomes) == 3


def test_tide_carries_current_outcomes_model_construct() -> None:
    """Tide.model_construct(...) with current_outcomes stores the list correctly."""
    outcomes = _sample_outcomes()
    tide = Tide.model_construct(
        tide_number=1,
        fired=["binance"],
        skipped=[("kraken", "not_due")],
        current_outcomes=outcomes,
        duration_sec=0.01,
        wake_reason="startup",
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=None,
        timestamp=datetime.now(timezone.utc),
    )
    assert tide.current_outcomes is outcomes


def test_tide_model_dump_json_serializes_current_outcomes() -> None:
    """model_dump(mode='json') converts CurrentOutcome instances to dicts via @field_serializer."""
    outcomes = [CurrentOutcome(name="coin", status="fired", bypassed_edges=("up",))]
    tide = Tide(tide_number=1, duration_sec=0.0, current_outcomes=outcomes)
    dumped = tide.model_dump(mode="json")
    assert isinstance(dumped["current_outcomes"], list)
    assert dumped["current_outcomes"][0] == {
        "name": "coin",
        "status": "fired",
        "reason": None,
        "bypassed_edges": ["up"],
        "in_flight_sec": None,
        "last_wave_at": None,
    }


def test_tide_serializer_fires_under_model_construct() -> None:
    """@field_serializer fires under Tide.model_construct — not only under validated construction.

    Pydantic v2 registers @field_serializer at class level, independent of
    validation; model_construct bypasses validation but the serializer still runs
    at dump time.
    """
    outcomes = [CurrentOutcome(name="arb", status="skipped", reason="not_due")]
    tide = Tide.model_construct(
        tide_number=2,
        fired=[],
        skipped=[("arb", "not_due")],
        current_outcomes=outcomes,
        duration_sec=0.005,
        wake_reason="timer",
        heap_depth=1,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=5.0,
        timestamp=datetime.now(timezone.utc),
    )
    dumped = tide.model_dump(mode="json")
    co_dump = dumped["current_outcomes"][0]
    assert co_dump["name"] == "arb"
    assert co_dump["status"] == "skipped"
    assert co_dump["reason"] == "not_due"


def test_tide_json_round_trip_via_json_dumps() -> None:
    """Tide.model_dump(mode='json') is JSON-serializable without further coercion."""
    outcomes = _sample_outcomes()
    tide = Tide(tide_number=5, duration_sec=0.1, current_outcomes=outcomes)
    raw = json.dumps(tide.model_dump(mode="json"))  # must not raise
    parsed = json.loads(raw)
    assert len(parsed["current_outcomes"]) == 3
    assert parsed["current_outcomes"][0]["name"] == "binance"


def test_tide_log_meta_unchanged_with_new_fields() -> None:
    """log_meta() output is unchanged — the new fields are not included in it."""
    outcomes = _sample_outcomes()
    tide = Tide(
        tide_number=3,
        fired=["binance"],
        skipped=[("kraken", "not_due")],
        current_outcomes=outcomes,
        duration_sec=0.123,
        wake_reason="timer",
    )
    meta = tide.log_meta()
    assert "tide_number:3" in meta
    assert "fired:1" in meta
    assert "skipped:1" in meta
    assert "duration_sec:0.123" in meta
    # New fields must NOT appear in log_meta (back-compat).
    assert "wake_reason" not in meta
    assert "current_outcomes" not in meta


def test_tide_is_frozen() -> None:
    """Assigning to a field on a validated Tide raises ValidationError (frozen model)."""
    from pydantic import ValidationError

    tide = Tide(tide_number=1, duration_sec=0.01)
    with pytest.raises(ValidationError):
        tide.tide_number = 99  # type: ignore[misc]


def test_tide_dataclass_asdict_round_trip() -> None:
    """dataclasses.asdict() on a CurrentOutcome produces the expected flat dict."""
    ts = datetime(2025, 6, 1, tzinfo=timezone.utc)
    co = CurrentOutcome(name="x", status="fired", last_wave_at=ts)
    d = dataclasses.asdict(co)
    assert d == {
        "name": "x",
        "status": "fired",
        "reason": None,
        "bypassed_edges": (),
        "in_flight_sec": None,
        "last_wave_at": ts,
    }


# ---------------------------------------------------------------------------
# WakeReason Literal narrowing (Item 3)
# ---------------------------------------------------------------------------


def test_tide_wake_reason_literal_accepts_valid_values() -> None:
    """Tide.model_construct accepts each of the five WakeReason literal strings without error."""
    from incorporator.observability.tideweaver.tide import WakeReason

    valid: List[WakeReason] = ["startup", "timer", "wake_event", "pass_interval", "shutdown"]
    for reason in valid:
        tide = Tide.model_construct(
            tide_number=1,
            fired=[],
            skipped=[],
            current_outcomes=[],
            duration_sec=0.01,
            wake_reason=reason,
            heap_depth=0,
            in_flight_count_at_start=0,
            canal_rejects_added=0,
            next_due_in_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert tide.wake_reason == reason


def test_tide_wake_reason_serialises_to_string() -> None:
    """model_dump(mode='json')['wake_reason'] returns the exact literal string value."""
    for reason in ("startup", "timer", "wake_event", "pass_interval", "shutdown"):
        tide = Tide(tide_number=1, duration_sec=0.0, wake_reason=reason)  # type: ignore[arg-type]
        dumped = tide.model_dump(mode="json")
        assert dumped["wake_reason"] == reason
        assert isinstance(dumped["wake_reason"], str)
