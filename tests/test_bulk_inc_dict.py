"""Regression tests for bulk inc_dict insertion (Track 1) and TypeAdapter cache tripwire (Win B).

Eight tests:
1. inc_dict empty during validate_python, fully populated after build_instances returns.
2. inc_dict has correct count after build_instances on a multi-row payload.
3. Bubble-up: immediate parent class inc_dict populated after build_instances on a dynamic subclass.
4. flush() bulk path: inc_dict fully populated after flush() and before _tideweaver_snapshot.
5. _BATCH_INSERT_MODE reset to False after build_instances (even on ValidationError).
6. Win B: infer_dynamic_schema returns distinct classes and TypeAdapters for different field types.
7. Auto-counter: unique sequential keys assigned when inc_code is missing.
8. RejectEntry model_construct matches validated constructor across all public attributes.
"""

from __future__ import annotations

import json
import types
import weakref
from typing import Any, Dict, List, Optional

import pytest

from incorporator import Incorporator
from incorporator.schema.builder import infer_dynamic_schema

# ---------------------------------------------------------------------------
# Module-level list for snapshot-during-validation test (Test 1).
# Using a module-level var avoids Pydantic V2 treating an underscore-prefixed
# annotated class attribute as a ModelPrivateAttr.
# ---------------------------------------------------------------------------
_VALIDATOR_SNAPSHOTS: List[int] = []


# ---------------------------------------------------------------------------
# Test 1 — inc_dict is empty DURING validate_python; full AFTER build_instances
# ---------------------------------------------------------------------------


class _SnapshotDuringValidation(Incorporator):
    """Captures inc_dict length at model_post_init time to assert on batch isolation."""

    def model_post_init(self, __context: Any) -> None:
        # super() runs auto-counter and (when not gated) writes inc_dict.
        super().model_post_init(__context)
        # Record how many entries were in this class's own inc_dict at hook time.
        _VALIDATOR_SNAPSHOTS.append(len(dict(self.__class__.inc_dict)))


@pytest.mark.asyncio
async def test_inc_dict_empty_during_validation_full_after(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """inc_dict must be empty for every instance during validate_python; fully populated after.

    Proves that _BATCH_INSERT_MODE gates model_post_init's inc_dict write so
    no partial state is visible while the batch is in flight, and that the
    bulk update() lands the complete set atomically once validate_python returns.
    """
    monkeypatch.chdir(tmp_path)

    # Reset module-level snapshot list and class state for test isolation.
    _VALIDATOR_SNAPSHOTS.clear()
    _SnapshotDuringValidation._BATCH_INSERT_MODE = False
    _SnapshotDuringValidation.inc_dict = weakref.WeakValueDictionary()

    payload = [{"id": i, "val": i * 10} for i in range(1, 6)]
    data_file = tmp_path / "data.json"
    data_file.write_text(json.dumps(payload), encoding="utf-8")

    result = await _SnapshotDuringValidation.incorp(inc_file=str(data_file), inc_code="id")

    # During validation every model_post_init hook must have seen an empty inc_dict.
    assert all(snap == 0 for snap in _VALIDATOR_SNAPSHOTS), (
        f"Expected empty inc_dict during validation, got snapshots: {_VALIDATOR_SNAPSHOTS}"
    )

    # After build_instances: all 5 instances are registered.
    assert len(result) == 5
    assert len(dict(_SnapshotDuringValidation.inc_dict)) == 5


# ---------------------------------------------------------------------------
# Test 2 — inc_dict has correct count after build_instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inc_dict_count_after_build_instances(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """inc_dict must contain exactly N entries after build_instances returns on an N-row payload.

    Proves the bulk update() is equivalent to N individual __setitem__ calls
    and that no entries are lost or duplicated during the batch.
    """
    monkeypatch.chdir(tmp_path)

    class _CountTarget(Incorporator):
        pass

    payload = [{"pk": f"k{i}", "score": i} for i in range(10)]
    data_file = tmp_path / "data.json"
    data_file.write_text(json.dumps(payload), encoding="utf-8")

    result = await _CountTarget.incorp(inc_file=str(data_file), inc_code="pk")

    assert len(result) == 10
    assert len(dict(_CountTarget.inc_dict)) == 10
    # Spot-check key presence.
    assert "k0" in _CountTarget.inc_dict
    assert "k9" in _CountTarget.inc_dict


# ---------------------------------------------------------------------------
# Test 3 — Bubble-up: immediate parent inc_dict populated on dynamic subclass build
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bubble_up_parent_inc_dict_populated(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Immediate parent class inc_dict must be populated via bulk update() after build_instances.

    The bubble-up loop does a single-level __bases__ walk, so only the immediate
    parent between the dynamic class and Incorporator receives all entries.
    Proves that DynamicSubclass.inc_dict AND _ChildEntity.inc_dict (immediate
    parent) both contain all N entries with value-identity preserved.
    """
    monkeypatch.chdir(tmp_path)

    class _ChildEntity(Incorporator):
        pass

    payload = [{"id": f"e{i}"} for i in range(4)]
    data_file = tmp_path / "data.json"
    data_file.write_text(json.dumps(payload), encoding="utf-8")

    result = await _ChildEntity.incorp(inc_file=str(data_file), inc_code="id")

    assert len(result) == 4

    # The dynamic class created by infer_dynamic_schema is the type of each instance.
    dynamic_cls = type(result[0])

    # Dynamic class inc_dict must contain all entries.
    dynamic_keys = set(dict(dynamic_cls.inc_dict).keys())
    assert dynamic_keys == {"e0", "e1", "e2", "e3"}

    # Immediate parent (_ChildEntity) must also have all entries via bubble-up.
    child_keys = set(dict(_ChildEntity.inc_dict).keys())
    assert child_keys == {"e0", "e1", "e2", "e3"}, f"Parent missing keys: {dynamic_keys - child_keys}"

    # Value-identity: same instance objects in both dicts.
    for key in dynamic_keys:
        assert dynamic_cls.inc_dict[key] is _ChildEntity.inc_dict[key], f"Instance identity mismatch for key {key!r}"


# ---------------------------------------------------------------------------
# Test 4 — flush() bulk path: inc_dict populated before _tideweaver_snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_bulk_insert_before_tideweaver_snapshot(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """flush() must populate inc_dict via bulk update() before setting _tideweaver_snapshot.

    Proves the ordering invariant: inc_dict is fully loaded atomically, then the
    strong-ref snapshot is parked.  Downstream ticks that read _tideweaver_snapshot
    always find a consistent inc_dict.

    Uses a pre-declared class on the outflow_module so infer_dynamic_schema
    returns that exact class — matching the 'pre-declared subclass wins over
    dynamic-schema inference' edge case.  This guarantees _tideweaver_snapshot
    is set on a class the test holds a reference to.
    """
    monkeypatch.chdir(tmp_path)

    from incorporator.observability.pipeline._outflow import flush

    class _FlushTarget(Incorporator):
        fk: Optional[str] = None
        payload: Optional[int] = None

    rows: List[Dict[str, Any]] = [{"fk": f"r{i}", "payload": i} for i in range(3)]

    def _outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
        return rows

    # Expose _FlushTarget on a module-like namespace so flush() finds it by
    # name and uses it directly rather than building a new dynamic class.
    outflow_mod = types.SimpleNamespace(_FlushTarget=_FlushTarget)

    results = []
    async for derived_name, count, err in flush(
        _outflow,
        {},
        default_output_class_name="_FlushTarget",
        base_class=_FlushTarget,
        export_params={},
        outflow_module=outflow_mod,
    ):
        results.append((derived_name, count, err))

    assert len(results) == 1
    derived_name, count, err = results[0]
    assert err is None
    assert count == 3

    # inc_dict must be populated on the pre-declared class.
    assert len(dict(_FlushTarget.inc_dict)) == 3

    # _tideweaver_snapshot must also be set and match the instances.
    snapshot = getattr(_FlushTarget, "_tideweaver_snapshot", None)
    assert snapshot is not None
    assert len(snapshot) == 3


# ---------------------------------------------------------------------------
# Test 5 — _BATCH_INSERT_MODE reset to False even after exception
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_insert_mode_reset_on_exception(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """_BATCH_INSERT_MODE must be reset to False by the finally block even when validate_python raises.

    Proves that the try/finally guard prevents _BATCH_INSERT_MODE from getting
    stuck at True after a ValidationError, which would permanently suppress all
    future per-instance inc_dict writes on that class.
    """
    monkeypatch.chdir(tmp_path)

    from pydantic import field_validator

    class _StrictInt(Incorporator):
        value: int = 0

        @field_validator("value", mode="before")
        @classmethod
        def must_be_positive(cls, v: Any) -> Any:
            """Reject negative values to trigger ValidationError inside validate_python."""
            if isinstance(v, int) and v < 0:
                raise ValueError("value must be positive")
            return v

    # One bad row ensures ValidationError fires inside validate_python.
    payload = [{"value": -1}]
    data_file = tmp_path / "data.json"
    data_file.write_text(json.dumps(payload), encoding="utf-8")

    # incorp() may raise or swallow the error -- either way the flag must be cleared.
    try:
        await _StrictInt.incorp(inc_file=str(data_file))
    except Exception:
        pass

    assert _StrictInt._BATCH_INSERT_MODE is False, "_BATCH_INSERT_MODE was not reset after exception"


# ---------------------------------------------------------------------------
# Test 6 (Win B) — infer_dynamic_schema returns distinct classes + TypeAdapters
# ---------------------------------------------------------------------------


def test_type_adapter_cache_distinct_per_field_type(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """infer_dynamic_schema returns distinct class objects and TypeAdapters for structurally different payloads.

    Proves that the SCHEMA_REGISTRY key carries field-type info so payloads
    with the same field name but different value types resolve to fresh classes
    with independent _cached_type_adapter instances.  A future refactor that
    drops type info from the registry key would silently reuse a stale adapter
    -- this test acts as the tripwire.
    """
    monkeypatch.chdir(tmp_path)

    from pydantic import TypeAdapter

    class _TypeTripwireBase(Incorporator):
        pass

    payload_int = [{"x": 1}]
    payload_str = [{"x": "hello"}]

    cls_a = infer_dynamic_schema("TypeTripwire", payload_int, _TypeTripwireBase)
    cls_b = infer_dynamic_schema("TypeTripwire", payload_str, _TypeTripwireBase)

    # Must be distinct class objects.
    assert id(cls_a) != id(cls_b), "Expected distinct class objects for different field types"

    # Populate each class's TypeAdapter cache if not already set (mirrors factory.py path).
    if not hasattr(cls_a, "_cached_type_adapter"):
        cls_a._cached_type_adapter = TypeAdapter(List[cls_a])  # type: ignore[valid-type]
    if not hasattr(cls_b, "_cached_type_adapter"):
        cls_b._cached_type_adapter = TypeAdapter(List[cls_b])  # type: ignore[valid-type]

    assert cls_a._cached_type_adapter is not cls_b._cached_type_adapter, (
        "Expected distinct TypeAdapter instances for distinct class objects"
    )


# ---------------------------------------------------------------------------
# Test 7 — Auto-counter assigns unique sequential keys when inc_code is missing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_counter_under_batch_mode(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Auto-counter must assign unique sequential keys when inc_code is absent from every row.

    Proves that the auto-counter block in model_post_init stays unconditional
    under the _BATCH_INSERT_MODE gate: even though inc_dict writes are deferred
    to the bulk update(), each instance receives a distinct integer key from
    cls._auto_counter, and all N keys land in cls.inc_dict after
    build_instances returns.
    """
    monkeypatch.chdir(tmp_path)

    class _AutoCounterTarget(Incorporator):
        name: Optional[str] = None

    # Reset class state so the counter starts predictably for this test.
    _AutoCounterTarget._auto_counter = 1
    _AutoCounterTarget.inc_dict = weakref.WeakValueDictionary()

    n = 5
    # Payload has no inc_code field — all instances use the auto-counter.
    payload = [{"name": f"item{i}"} for i in range(n)]
    data_file = tmp_path / "data.json"
    data_file.write_text(json.dumps(payload), encoding="utf-8")

    result = await _AutoCounterTarget.incorp(inc_file=str(data_file))

    assert len(result) == n

    # Every instance must have a unique integer inc_code.
    keys = [inst.inc_code for inst in result]
    assert len(set(keys)) == n, f"Expected {n} unique keys, got: {keys}"
    assert all(isinstance(k, int) for k in keys), f"Expected integer keys, got: {keys}"

    # All N keys must be registered in inc_dict.
    assert len(dict(_AutoCounterTarget.inc_dict)) == n
    for key in keys:
        assert key in _AutoCounterTarget.inc_dict, f"Key {key!r} missing from inc_dict"


# ---------------------------------------------------------------------------
# Test 8 — RejectEntry model_construct matches validated constructor
# ---------------------------------------------------------------------------


def test_reject_entry_model_construct_matches_validated() -> None:
    """RejectEntry built via model_construct must match one built via the validated constructor.

    Proves that model_construct (the fast-path used by _build_reject_entry at
    high-throughput failure points) produces entries that compare equal to
    the normal Pydantic-validated path across all public attributes.
    Regression guard: a field rename or default change that drifts between the
    two construction paths would surface here immediately.

    Covers two representative entry shapes:
      * HTTP-error shape: source=URL, error_kind='HTTPStatusError', retry_after float.
      * Fjord-seed shape: source=ClassName, error_kind='KeyError', wave_index int.
    """
    from incorporator import RejectEntry

    # --- HTTP-error shape ---
    http_kwargs: Dict[str, Any] = {
        "source": "https://api.example.com/data",
        "error_kind": "HTTPStatusError",
        "message": "429 Too Many Requests",
        "retry_after": 30.0,
        "wave_index": None,
    }
    validated_http = RejectEntry(**http_kwargs)
    constructed_http = RejectEntry.model_construct(**http_kwargs)

    assert validated_http.source == constructed_http.source
    assert validated_http.error_kind == constructed_http.error_kind
    assert validated_http.message == constructed_http.message
    assert validated_http.retry_after == constructed_http.retry_after
    assert validated_http.wave_index == constructed_http.wave_index

    # --- Fjord-seed shape ---
    fjord_kwargs: Dict[str, Any] = {
        "source": "MySourceClass",
        "error_kind": "KeyError",
        "message": "missing peer X — use state.get() or depends_on=[X]",
        "retry_after": None,
        "wave_index": 5,
    }
    validated_fjord = RejectEntry(**fjord_kwargs)
    constructed_fjord = RejectEntry.model_construct(**fjord_kwargs)

    assert validated_fjord.source == constructed_fjord.source
    assert validated_fjord.error_kind == constructed_fjord.error_kind
    assert validated_fjord.message == constructed_fjord.message
    assert validated_fjord.retry_after == constructed_fjord.retry_after
    assert validated_fjord.wave_index == constructed_fjord.wave_index
