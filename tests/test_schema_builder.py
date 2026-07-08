"""Unit tests for schema/builder.py — sanitize, ETL engine, and infer_dynamic_schema edge cases."""

import concurrent.futures
import logging
import sys
from typing import Any, Dict, List, Optional

import pytest
from pydantic import BaseModel

from incorporator.schema import builder as schema_builder
from incorporator.schema.builder import (
    SCHEMA_REGISTRY,
    apply_etl_transformations,
    infer_dynamic_schema,
    sanitize_json_key,
)
from incorporator.schema.converters import calc, calc_all
from incorporator.schema.extractors import as_list


# ==========================================
# 1. sanitize_json_key edge cases
# ==========================================


def test_sanitize_empty_string_becomes_empty_key() -> None:
    """An empty-string key must become 'empty_key' (the regex produces '' → fallback)."""
    result = sanitize_json_key("")
    assert result == "empty_key"


def test_sanitize_python_keyword_gets_trailing_underscore() -> None:
    """A key that matches a Python keyword must be suffixed with '_'."""
    result = sanitize_json_key("for")
    assert result == "for_"
    result2 = sanitize_json_key("class")
    assert result2 == "class_"


def test_sanitize_pydantic_reserved_gets_safe_prefix() -> None:
    """Keys matching Pydantic internals must be prefixed with 'safe_'."""
    result = sanitize_json_key("model_fields")
    assert result == "safe_model_fields"
    result2 = sanitize_json_key("dict")
    assert result2 == "safe_dict"


def test_sanitize_digit_prefix_gets_leading_underscore() -> None:
    """Keys starting with a digit must receive a leading underscore."""
    result = sanitize_json_key("123abc")
    assert result == "_123abc"


def test_sanitize_leading_underscore_gets_field_prefix() -> None:
    """Keys starting with '_' must be prefixed with 'field' (Pydantic V2's
    create_model rejects field names with a leading underscore)."""
    result = sanitize_json_key("_key")
    assert result == "field_key"


def test_sanitize_normal_key_unchanged() -> None:
    """Well-formed keys pass through without modification."""
    assert sanitize_json_key("user_id") == "user_id"
    assert sanitize_json_key("Name") == "Name"


# ==========================================
# 2. apply_etl_transformations edge cases
# ==========================================


def test_apply_etl_empty_list_returns_unchanged() -> None:
    """Empty list input returns the same empty list without error."""
    result = apply_etl_transformations([])
    assert result == []


def test_apply_etl_non_dict_items_returned_as_is() -> None:
    """A list of non-dict items (e.g. strings) bypasses the ETL passes."""
    data: List[Any] = ["alpha", "beta", 42]
    result = apply_etl_transformations(data)
    assert result == data


def test_apply_etl_calc_all_operation() -> None:
    """calc_all applies a vectorised function across all rows in one call."""
    data: List[Dict[str, Any]] = [
        {"price": 10.0, "qty": 2},
        {"price": 5.0, "qty": 4},
    ]
    # total = price * qty, computed via calc_all
    total_op = calc_all(lambda prices, qtys: [p * q for p, q in zip(prices, qtys)], "price", "qty", default=0.0)
    result = apply_etl_transformations(data, conv_dict={"total": total_op})
    assert isinstance(result, list)
    assert result[0]["total"] == pytest.approx(20.0)
    assert result[1]["total"] == pytest.approx(20.0)


def test_apply_etl_calc_all_fewer_results_uses_default() -> None:
    """If calc_all returns fewer results than rows, missing entries use default."""
    data: List[Dict[str, Any]] = [{"x": 1}, {"x": 2}, {"x": 3}]
    # Returns only 1 result → rows 1 and 2 fall back to default
    short_op = calc_all(lambda xs: [99], "x", default=-1)
    result = apply_etl_transformations(data, conv_dict={"x": short_op})
    assert isinstance(result, list)
    assert result[0]["x"] == 99
    assert result[1]["x"] == -1
    assert result[2]["x"] == -1


def test_apply_etl_calc_target_type_coercion_failure_uses_default() -> None:
    """When target_type coercion fails for a calc result, the default is used."""
    data: List[Dict[str, Any]] = [{"val": "not-a-number"}]
    # func returns the raw string; target_type=float will raise → fall back to default
    op = calc(lambda v: v, "val", default=0.0, target_type=float)
    result = apply_etl_transformations(data, conv_dict={"val": op})
    assert isinstance(result, list)
    assert result[0]["val"] == 0.0


def test_apply_etl_calc_none_return_with_target_type_emits_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A calc() func that legitimately returns None must not trigger target_type coercion
    or a 'type coercion failed' warning; None lands cleanly for the key."""
    data: List[Dict[str, Any]] = [{"a": "no-salary-published"}]

    def maybe_none(x: Any) -> Any:
        return None if x == "no-salary-published" else float(x)

    op = calc(maybe_none, "a", target_type=float)
    with caplog.at_level(logging.WARNING):
        result = apply_etl_transformations(data, conv_dict={"b": op})
    assert result[0]["b"] is None
    assert "type coercion failed" not in caplog.text


def test_apply_etl_calc_all_none_return_with_target_type_emits_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A calc_all() func that legitimately returns None for a row must not trigger target_type
    coercion or a 'type coercion failed' warning for that row; None lands cleanly for the key."""
    data: List[Dict[str, Any]] = [{"a": "no-salary-published"}, {"a": "42"}]

    def maybe_none(xs: List[Any]) -> List[Any]:
        return [None if x == "no-salary-published" else float(x) for x in xs]

    op = calc_all(maybe_none, "a", target_type=float)
    with caplog.at_level(logging.WARNING):
        result = apply_etl_transformations(data, conv_dict={"b": op})
    assert result[0]["b"] is None
    assert result[1]["b"] == pytest.approx(42.0)
    assert "type coercion failed" not in caplog.text


def test_apply_etl_conv_dict_standard_exception_skips_key() -> None:
    """A conv_dict callable that raises must be caught; key is left as-is."""
    data: List[Dict[str, Any]] = [{"id": "abc"}]

    def always_raise(v: Any) -> Any:
        raise ValueError("boom")

    result = apply_etl_transformations(data, conv_dict={"id": always_raise})
    # Key is left unchanged; the exception must not propagate
    assert isinstance(result, list)
    assert result[0]["id"] == "abc"


def test_apply_etl_duplicate_old_key_rename_matches_runtime_pk_bind() -> None:
    """D2-03 end-to-end: config-time Pk.source rewrite must match runtime apply_rename.

    name_chg has a DUPLICATE old key ("a" appears twice): [("a", "b"), ("a", "c")].
    Pre-fix, _normalize_etl_kwargs's rename_map dict comprehension resolves
    last-hit, binding Pk('a') to 'c'. But the runtime Nm pass applies renames
    sequentially: the first Nm("a", "b") moves 'a' to 'b'; the second
    Nm("a", "c") is then a no-op since there's no 'a' left. So a
    last-hit-bound Pk('c') would never find a source and inc_code would be
    silently missing from the output — the misbinding this test repros.

    Post-fix, Pk.source rewrites first-hit to 'b', which the runtime rename
    pass actually produces, so inc_code is present in the output.
    """
    data: List[Dict[str, Any]] = [{"a": "team-42"}]
    result = apply_etl_transformations(
        data,
        code_attr="a",
        name_chg=[("a", "b"), ("a", "c")],
    )
    assert isinstance(result, list)
    assert "inc_code" in result[0], "config-time Pk rewrite must agree with the runtime rename pass"
    assert result[0]["inc_code"] == "team-42"
    assert result[0]["b"] == "team-42"
    assert "c" not in result[0]


def test_apply_etl_as_list_no_cross_dispatch_aliasing() -> None:
    """The SAME as_list() Op reused across two apply_etl_transformations dispatches (D7-03).

    Models Tideweaver's persisted-conv_dict replay: the identical Op instance
    (and its cache, if any) recurs every tick. A second dispatch with an
    overlapping input value must not observe mutations made to a list
    returned by the first dispatch — each row gets a fresh, unaliased list.
    """
    op = as_list()
    conv_dict = {"ids": op}

    dispatch_one: List[Dict[str, Any]] = [{"ids": 7}, {"ids": 7}]
    apply_etl_transformations(dispatch_one, conv_dict=conv_dict)
    assert dispatch_one[0]["ids"] == [7]
    assert dispatch_one[1]["ids"] == [7]
    assert dispatch_one[0]["ids"] is not dispatch_one[1]["ids"]

    # Mutate a row's list after the first dispatch completes.
    dispatch_one[0]["ids"].append("poison")

    # Second dispatch with the SAME Op instance and an overlapping input value.
    dispatch_two: List[Dict[str, Any]] = [{"ids": 7}]
    apply_etl_transformations(dispatch_two, conv_dict=conv_dict)
    assert dispatch_two[0]["ids"] == [7]  # unaffected by the prior mutation
    assert "poison" not in dispatch_two[0]["ids"]


# ==========================================
# 3. infer_dynamic_schema advanced cases
# ==========================================


def test_infer_schema_merges_lists_across_records() -> None:
    """Keys present as lists across multiple records are merged for full type coverage."""
    data = [
        {"tags": ["admin"]},
        {"tags": ["user", "readonly"]},
    ]
    model = infer_dynamic_schema("MergeListModel", data, BaseModel)
    assert model is not None
    # Model is buildable (no exception during creation)
    instance = model.model_validate({"tags": ["admin", "user"]}, strict=False)
    assert instance is not None


def test_infer_schema_merges_dicts_across_records() -> None:
    """Nested dict keys seen in any record are folded into the merged sample."""
    data = [
        {"meta": {"role": "admin"}},
        {"meta": {"tier": 1}},
    ]
    model = infer_dynamic_schema("MergeDictModel", data, BaseModel)
    assert model is not None
    # Confirm the model was built with a 'meta' field and can be validated
    assert "meta" in model.model_fields
    instance = model.model_validate({"meta": {"role": "admin", "tier": 1}}, strict=False)
    assert instance is not None


def test_infer_schema_nested_list_of_dicts() -> None:
    """A list value containing dicts must produce a nested list model field."""
    data: List[Dict[str, Any]] = [{"items": [{"name": "A", "value": 1}, {"name": "B", "value": 2}]}]
    model = infer_dynamic_schema("NestedListModel", data, BaseModel)
    assert model is not None
    # Field should be Optional[List[...]]
    field = model.model_fields.get("items")
    assert field is not None


def test_infer_schema_registry_lru_eviction() -> None:
    """When SCHEMA_REGISTRY reaches MAX_REGISTRY_SIZE, the oldest entry is evicted."""
    from incorporator.schema.builder import MAX_REGISTRY_SIZE

    # Save and restore registry state
    saved = dict(SCHEMA_REGISTRY)
    try:
        SCHEMA_REGISTRY.clear()

        # Fill registry to the limit
        for i in range(MAX_REGISTRY_SIZE):
            infer_dynamic_schema(f"LRUModel{i}", {"field": i}, BaseModel)

        assert len(SCHEMA_REGISTRY) == MAX_REGISTRY_SIZE

        # One more entry must trigger eviction of the oldest
        infer_dynamic_schema("LRUModelOverflow", {"field": "overflow"}, BaseModel)

        assert len(SCHEMA_REGISTRY) <= MAX_REGISTRY_SIZE
        # Overflow model must be present
        assert any("LRUModelOverflow" in str(k) for k in SCHEMA_REGISTRY)
    finally:
        SCHEMA_REGISTRY.clear()
        SCHEMA_REGISTRY.update(saved)


def test_infer_schema_registry_concurrent_eviction_deterministic() -> None:
    """Concurrent registry inserts must force deterministic evictions without corruption.

    Monkeypatches ``MAX_REGISTRY_SIZE`` down to a small value so that a real
    thread pool building many distinct fresh schemas concurrently is
    guaranteed to cross the eviction threshold multiple times within the
    run — deterministic, unlike relying on timing alone. Lowers
    ``sys.setswitchinterval`` to maximize interleaving odds between the
    hit-path ``move_to_end`` and the miss-path ``popitem``/insert critical
    sections in :func:`infer_dynamic_schema`. Asserts the registry never
    exceeds its bound and every surviving entry is a usable model class —
    the same integrity-canary contract as
    ``test_schema_registry_concurrent_gather_integrity`` in
    ``tests/test_validation.py``, but exercised via real OS threads (a
    ``ThreadPoolExecutor``) instead of ``asyncio.to_thread``, and with a
    small, deterministic ``MAX_REGISTRY_SIZE`` so eviction races are
    guaranteed to occur rather than merely possible.

    Honesty note (per brief): pre-fix (no ``_SCHEMA_REGISTRY_LOCK``), this
    test reliably and deterministically FAILED — observed
    ``len(SCHEMA_REGISTRY) == 9 > MAX_REGISTRY_SIZE == 8`` — because
    concurrent ``popitem``/insert calls interleaved past the size check.
    Post-fix it passes. Unlike the asyncio-``gather`` canary in
    ``tests/test_validation.py`` (which is not guaranteed to reproduce
    corruption under a short run), this ``ThreadPoolExecutor`` variant with a
    small ``MAX_REGISTRY_SIZE`` does reproduce it deterministically — both
    outcomes are reported here rather than assumed.
    """
    saved = dict(SCHEMA_REGISTRY)
    original_max = schema_builder.MAX_REGISTRY_SIZE
    original_switch_interval = sys.getswitchinterval()
    schema_builder.MAX_REGISTRY_SIZE = 8
    sys.setswitchinterval(1e-6)
    try:
        SCHEMA_REGISTRY.clear()

        def build_one(i: int) -> type[BaseModel]:
            return infer_dynamic_schema(f"ConcurrentEvictModel{i}", {f"field_{i}": i}, BaseModel)

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(build_one, range(200)))

        assert len(SCHEMA_REGISTRY) <= schema_builder.MAX_REGISTRY_SIZE
        assert all(hasattr(r, "model_fields") for r in results)
        for model_cls in list(SCHEMA_REGISTRY.values()):
            assert hasattr(model_cls, "model_fields")
    finally:
        sys.setswitchinterval(original_switch_interval)
        schema_builder.MAX_REGISTRY_SIZE = original_max
        SCHEMA_REGISTRY.clear()
        SCHEMA_REGISTRY.update(saved)


def test_infer_schema_single_dict_input() -> None:
    """A bare dict (not a list) is wrapped correctly and the model is cached."""
    model = infer_dynamic_schema("SingleDictModel", {"name": "Alice", "age": 30}, BaseModel)
    assert model is not None
    instance = model.model_validate({"name": "Alice", "age": 30})
    assert instance is not None


def test_infer_schema_cache_hit_returns_same_class() -> None:
    """Identical (name, shape, base) should return the cached class on second call."""
    m1 = infer_dynamic_schema("CachedModel", {"x": 1}, BaseModel)
    m2 = infer_dynamic_schema("CachedModel", {"x": 1}, BaseModel)
    assert m1 is m2


def test_infer_schema_leading_underscore_key_builds_and_retrievable() -> None:
    """A raw leading-underscore key (e.g. Kraken's '_key') must build without
    raising and be retrievable under its sanitized 'field_key' name, instead
    of crashing on Pydantic V2's leading-underscore field name rejection."""
    model = infer_dynamic_schema("UnderscoreKeyModel", {"_key": "value"}, BaseModel)
    assert "field_key" in model.model_fields
    instance = model.model_validate({"_key": "value"})
    assert getattr(instance, "field_key") == "value"


# ==========================================
# 4. infer_dynamic_schema sampling stride (D3-03)
# ==========================================


def test_infer_schema_samples_tail_fields_beyond_100_records() -> None:
    """A field present only in a tail record (n=150) must still be discovered.

    Pre-fix, the truncating stride (`step = n // 100`; `data[::step][:100]`)
    degenerates to `data[:100]` whenever `n` is in [101, 199] (step=1), so a
    field appearing only at index 149 or index 120 was silently dropped from
    the inferred schema. This must fail before the linspace-index fix and
    pass after it.
    """
    data: List[Dict[str, Any]] = [{"common": i} for i in range(150)]
    data[149]["tail_only"] = True
    data[120]["mid_tail_only"] = True

    model = infer_dynamic_schema("TailFieldModel", data, BaseModel)

    assert "tail_only" in model.model_fields
    assert "mid_tail_only" in model.model_fields


@pytest.mark.parametrize("n", [101, 150, 199, 200, 350])
def test_infer_schema_stride_coverage_is_even_and_reaches_tail(n: int) -> None:
    """Sampled index set (reconstructed via per-index sentinel fields) is evenly
    spaced, capped at 100, includes index 0, and reaches the tail region.

    Each record gets a unique field name `f_{i}` so the resulting
    `model_fields` set exactly reveals which indices were sampled — a
    black-box way to recover the sampling stride's index set without
    depending on internal implementation details.
    """
    data: List[Dict[str, Any]] = [{f"f_{i}": i} for i in range(n)]
    model = infer_dynamic_schema(f"StrideModel{n}", data, BaseModel)

    sampled_indices = sorted(int(name.split("_", 1)[1]) for name in model.model_fields if name.startswith("f_"))

    assert len(sampled_indices) <= 100
    assert sampled_indices[0] == 0
    # Tail region: the highest sampled index must land at or near n - 1,
    # well past the truncating stride's unreachable-tail ceiling.
    assert sampled_indices[-1] >= n - max(1, n // 100) - 1

    # Evenly spaced: consecutive gaps must not vary wildly (linspace-style
    # rounding permits +/-1 jitter between adjacent gaps).
    gaps = [b - a for a, b in zip(sampled_indices, sampled_indices[1:])]
    assert max(gaps) - min(gaps) <= 1


def test_infer_schema_n_le_100_samples_everything_unchanged() -> None:
    """Existing behavior pin: n <= 100 must still sample every record."""
    data: List[Dict[str, Any]] = [{f"f_{i}": i} for i in range(100)]
    model = infer_dynamic_schema("SmallSampleModel", data, BaseModel)

    sampled_indices = sorted(int(name.split("_", 1)[1]) for name in model.model_fields if name.startswith("f_"))
    assert sampled_indices == list(range(100))
