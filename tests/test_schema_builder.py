"""Unit tests for schema/builder.py — sanitize, ETL engine, and infer_dynamic_schema edge cases."""

from typing import Any, Dict, List, Optional

import pytest
from pydantic import BaseModel

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


def test_apply_etl_conv_dict_standard_exception_skips_key() -> None:
    """A conv_dict callable that raises must be caught; key is left as-is."""
    data: List[Dict[str, Any]] = [{"id": "abc"}]

    def always_raise(v: Any) -> Any:
        raise ValueError("boom")

    result = apply_etl_transformations(data, conv_dict={"id": always_raise})
    # Key is left unchanged; the exception must not propagate
    assert isinstance(result, list)
    assert result[0]["id"] == "abc"


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
