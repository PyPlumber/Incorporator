"""Unit tests for Ex / Nm / Pk frozen-dataclass wrappers and DataPath.pop/set."""

from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from incorporator.schema.directives import Ex, Nm, Pk
from incorporator.schema.path import DataPath


# ---------------------------------------------------------------------------
# Parametrized construction / frozen / hashable / repr per wrapper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "wrapper, expected_repr",
    [
        (Ex("status"), "Ex('status')"),
        (Nm("old_key", "new_key"), "Nm('old_key', 'new_key')"),
        (Pk("id", target="code"), "Pk('id', target='code')"),
        (Pk("league.name", target="name"), "Pk('league.name', target='name')"),
    ],
)
def test_wrapper_repr(wrapper: Any, expected_repr: str) -> None:
    """Asserts __repr__ matches the declared format string for each wrapper."""
    assert repr(wrapper) == expected_repr


@pytest.mark.parametrize(
    "wrapper, attr",
    [
        (Ex("status"), "field"),
        (Nm("old_key", "new_key"), "old"),
        (Pk("id", target="code"), "source"),
    ],
)
def test_wrapper_frozen_rejects_mutation(wrapper: Any, attr: str) -> None:
    """Asserts that frozen=True prevents attribute assignment after construction."""
    with pytest.raises(FrozenInstanceError):
        setattr(wrapper, attr, "mutated")


@pytest.mark.parametrize(
    "wrapper",
    [
        Ex("status"),
        Nm("old_key", "new_key"),
        Pk("id", target="code"),
    ],
)
def test_wrapper_hashable(wrapper: Any) -> None:
    """Asserts that each wrapper is usable as a set element and dict key."""
    s = {wrapper}
    assert wrapper in s
    d = {wrapper: 1}
    assert d[wrapper] == 1

    # Two equal-fields instances must collide as dict keys (frozen __eq__ + __hash__).
    wrapper2 = replace(wrapper)
    collision = {wrapper: 1, wrapper2: 2}
    assert len(collision) == 1
    assert collision[wrapper] == 2


# ---------------------------------------------------------------------------
# Ex — exclusion directive
# ---------------------------------------------------------------------------


def test_ex_top_level_drop() -> None:
    """Asserts that Ex drops a top-level key from the record."""
    record: dict[str, Any] = {"status": "active", "id": 1}
    Ex("status").apply_drop(record)
    assert "status" not in record
    assert record["id"] == 1


def test_ex_nested_drop() -> None:
    """Asserts that Ex('a.b.c') drops only the leaf, leaving parent intact."""
    record: dict[str, Any] = {"a": {"b": {"c": 42, "d": "keep"}, "x": 1}}
    Ex("a.b.c").apply_drop(record)
    assert "c" not in record["a"]["b"]
    assert record["a"]["b"]["d"] == "keep"
    assert record["a"]["x"] == 1


def test_ex_missing_field_noop() -> None:
    """Asserts that dropping a missing top-level field is a silent no-op."""
    record: dict[str, Any] = {"id": 1}
    Ex("nonexistent").apply_drop(record)
    assert record == {"id": 1}


def test_ex_missing_intermediate_noop() -> None:
    """Asserts that a missing intermediate in a nested path is a silent no-op."""
    record: dict[str, Any] = {"a": {}}
    Ex("a.b.c").apply_drop(record)
    assert record == {"a": {}}


# ---------------------------------------------------------------------------
# Nm — rename directive
# ---------------------------------------------------------------------------


def test_nm_top_level_rename() -> None:
    """Asserts that Nm renames a top-level key correctly."""
    record: dict[str, Any] = {"external_id": 99, "name": "foo"}
    Nm("external_id", "id").apply_rename(record)
    assert record["id"] == 99
    assert "external_id" not in record
    assert record["name"] == "foo"


def test_nm_missing_source_noop() -> None:
    """Asserts that rename with a missing source key is a silent no-op."""
    record: dict[str, Any] = {"id": 1}
    Nm("nonexistent", "other").apply_rename(record)
    assert record == {"id": 1}


def test_nm_clobbers_existing_new_key() -> None:
    """Asserts that when both old and new exist, new is overwritten with old's value (builder.py:345 semantics)."""
    record: dict[str, Any] = {"old": "old_value", "new": "original_new"}
    Nm("old", "new").apply_rename(record)
    assert record["new"] == "old_value"
    assert "old" not in record


def test_nm_nested_source_to_top_level() -> None:
    """Nm("a.b", "c") moves nested leaf to top-level key."""
    record: dict[str, Any] = {"a": {"b": 99, "keep": 1}, "other": 2}
    Nm("a.b", "c").apply_rename(record)
    assert record["c"] == 99
    assert "b" not in record["a"]
    assert record["a"]["keep"] == 1


def test_nm_top_level_to_nested_target_parent_exists() -> None:
    """Nm("a", "b.c") moves top-level to nested target when parent dict exists."""
    record: dict[str, Any] = {"a": 99, "b": {}}
    Nm("a", "b.c").apply_rename(record)
    assert record["b"]["c"] == 99
    assert "a" not in record


def test_nm_top_level_to_nested_target_parent_auto_created() -> None:
    """Nm("a", "b.c") auto-creates the target parent when missing."""
    record: dict[str, Any] = {"a": 99}
    Nm("a", "b.c").apply_rename(record)
    assert record["b"]["c"] == 99
    assert "a" not in record


def test_nm_cross_parent_move_both_exist() -> None:
    """Nm("user.email", "contact.email") moves across parent dicts."""
    record: dict[str, Any] = {"user": {"email": "x@y.com", "name": "Alice"}, "contact": {}}
    Nm("user.email", "contact.email").apply_rename(record)
    assert record["contact"]["email"] == "x@y.com"
    assert "email" not in record["user"]
    assert record["user"]["name"] == "Alice"


def test_nm_cross_parent_move_target_auto_created() -> None:
    """Nm("user.email", "contact.email") auto-creates the target parent."""
    record: dict[str, Any] = {"user": {"email": "x@y.com"}}
    Nm("user.email", "contact.email").apply_rename(record)
    assert record["contact"]["email"] == "x@y.com"
    assert "email" not in record["user"]


def test_nm_nested_source_missing_intermediate_noop() -> None:
    """Nm("a.b.c", "x") is a no-op when an intermediate segment is missing."""
    record: dict[str, Any] = {"a": {}}
    Nm("a.b.c", "x").apply_rename(record)
    assert record == {"a": {}}


def test_nm_top_level_explicit_none_preserved() -> None:
    """Nm("a", "b") moves an explicit None value (does NOT skip)."""
    record: dict[str, Any] = {"a": None}
    Nm("a", "b").apply_rename(record)
    assert "a" not in record
    assert "b" in record
    assert record["b"] is None


def test_nm_nested_explicit_none_preserved() -> None:
    """Nm("a.b", "c.d") moves an explicit None value across parents."""
    record: dict[str, Any] = {"a": {"b": None}}
    Nm("a.b", "c.d").apply_rename(record)
    assert "b" not in record["a"]
    assert record["c"]["d"] is None


# ---------------------------------------------------------------------------
# Pk — PK-bind directive
# ---------------------------------------------------------------------------


def test_pk_target_code_writes_inc_code() -> None:
    """Asserts that Pk with target='code' writes the resolved value to inc_code."""
    record: dict[str, Any] = {"id": 42, "name": "Alpha"}
    Pk("id", target="code").apply_bind(record)
    assert record["inc_code"] == 42


def test_pk_target_name_writes_inc_name() -> None:
    """Asserts that Pk with target='name' writes the resolved value to inc_name."""
    record: dict[str, Any] = {"id": 42, "name": "Alpha"}
    Pk("name", target="name").apply_bind(record)
    assert record["inc_name"] == "Alpha"


def test_pk_missing_source_noop() -> None:
    """Asserts that Pk does not write inc_code when the source key is absent."""
    record: dict[str, Any] = {"name": "Alpha"}
    Pk("id", target="code").apply_bind(record)
    assert "inc_code" not in record


def test_pk_present_but_none_source_noop() -> None:
    """Asserts that Pk does not write inc_code when the resolved value is None (mirrors builder.py:303)."""
    record: dict[str, Any] = {"id": None}
    Pk("id", target="code").apply_bind(record)
    assert "inc_code" not in record


def test_pk_nested_source_path() -> None:
    """Asserts that Pk resolves a dotted nested source path correctly."""
    record: dict[str, Any] = {"league": {"name": "AL East"}, "id": 1}
    Pk("league.name", target="name").apply_bind(record)
    assert record["inc_name"] == "AL East"


def test_pk_nested_missing_intermediate_noop() -> None:
    """Asserts that Pk is a no-op when an intermediate segment in the nested path is absent."""
    record: dict[str, Any] = {"id": 1}
    Pk("league.name", target="name").apply_bind(record)
    assert "inc_name" not in record


# ---------------------------------------------------------------------------
# DataPath.pop
# ---------------------------------------------------------------------------


def test_datapath_pop_top_level() -> None:
    """Asserts that DataPath.pop removes a top-level dict key."""
    record: dict[str, Any] = {"a": 1, "b": 2}
    DataPath.parse("a").pop(record)
    assert "a" not in record
    assert record["b"] == 2


def test_datapath_pop_nested() -> None:
    """Asserts that DataPath.pop removes only the leaf in a nested path."""
    record: dict[str, Any] = {"a": {"b": {"c": 99, "d": "keep"}}}
    DataPath.parse("a.b.c").pop(record)
    assert "c" not in record["a"]["b"]
    assert record["a"]["b"]["d"] == "keep"


def test_datapath_pop_missing_top_level_noop() -> None:
    """Asserts that DataPath.pop is a no-op when the top-level key is absent."""
    record: dict[str, Any] = {"b": 2}
    DataPath.parse("a").pop(record)
    assert record == {"b": 2}


def test_datapath_pop_missing_intermediate_noop() -> None:
    """Asserts that DataPath.pop is a no-op when an intermediate segment is absent."""
    record: dict[str, Any] = {"a": {}}
    DataPath.parse("a.b.c").pop(record)
    assert record == {"a": {}}


def test_datapath_pop_non_dict_record_noop() -> None:
    """Asserts that DataPath.pop is a no-op when the root record is not a dict."""
    not_a_dict: Any = [1, 2, 3]
    DataPath.parse("a").pop(not_a_dict)
    assert not_a_dict == [1, 2, 3]


def test_datapath_pop_with_int_segment() -> None:
    """Asserts DataPath.pop walks a list-index segment to remove a dict leaf inside the list."""
    record: dict[str, Any] = {"a": [{"b": 99, "c": "keep"}]}
    DataPath.parse("a.0.b").pop(record)
    assert "b" not in record["a"][0]
    assert record["a"][0]["c"] == "keep"


def test_datapath_pop_with_int_segment_out_of_bounds_noop() -> None:
    """Asserts DataPath.pop is a silent no-op when the list index is out of range."""
    record: dict[str, Any] = {"a": [{"b": 1}]}
    DataPath.parse("a.5.b").pop(record)
    assert record == {"a": [{"b": 1}]}


# ---------------------------------------------------------------------------
# DataPath.set
# ---------------------------------------------------------------------------


def test_datapath_set_top_level() -> None:
    """Asserts that DataPath.set assigns a value to a top-level key."""
    record: dict[str, Any] = {"a": 1}
    DataPath.parse("b").set(record, 99)
    assert record["b"] == 99


def test_datapath_set_nested_parent_exists() -> None:
    """Asserts that DataPath.set assigns to a nested key when the parent dict exists."""
    record: dict[str, Any] = {"a": {"b": {}}}
    DataPath.parse("a.b.c").set(record, "written")
    assert record["a"]["b"]["c"] == "written"


def test_datapath_set_nested_parent_absent_noop() -> None:
    """Asserts that DataPath.set is a no-op when an intermediate dict is absent."""
    record: dict[str, Any] = {"a": {}}
    DataPath.parse("a.b.c").set(record, "written")
    assert "b" not in record["a"]


def test_datapath_set_non_dict_record_noop() -> None:
    """Asserts that DataPath.set is a no-op when the root record is not a dict."""
    not_a_dict: Any = [1, 2, 3]
    DataPath.parse("a").set(not_a_dict, 99)
    assert not_a_dict == [1, 2, 3]


def test_datapath_set_with_int_segment() -> None:
    """Asserts DataPath.set walks a list-index segment to write a dict leaf inside the list."""
    record: dict[str, Any] = {"a": [{"b": 1}]}
    DataPath.parse("a.0.b").set(record, 42)
    assert record["a"][0]["b"] == 42


def test_datapath_set_with_int_segment_out_of_bounds_noop() -> None:
    """Asserts DataPath.set is a silent no-op when the list index is out of range."""
    record: dict[str, Any] = {"a": [{"b": 1}]}
    DataPath.parse("a.5.b").set(record, 42)
    assert record == {"a": [{"b": 1}]}


def test_datapath_set_create_parents_nested_missing() -> None:
    """DataPath.set(create_parents=True) creates intermediate dicts."""
    record: dict[str, Any] = {}
    DataPath.parse("a.b.c").set(record, 42, create_parents=True)
    assert record["a"]["b"]["c"] == 42


def test_datapath_set_create_parents_nested_partial() -> None:
    """DataPath.set(create_parents=True) creates only missing intermediates."""
    record: dict[str, Any] = {"a": {"existing": 1}}
    DataPath.parse("a.b.c").set(record, 42, create_parents=True)
    assert record["a"]["existing"] == 1
    assert record["a"]["b"]["c"] == 42


def test_datapath_set_create_parents_false_default() -> None:
    """DataPath.set without create_parents= remains silent no-op (default)."""
    record: dict[str, Any] = {}
    DataPath.parse("a.b.c").set(record, 42)
    assert record == {}


def test_datapath_set_create_parents_non_dict_intermediate_noop() -> None:
    """DataPath.set(create_parents=True) refuses silently when intermediate is not a dict/list."""
    record: dict[str, Any] = {"a": "scalar_not_dict"}
    DataPath.parse("a.b.c").set(record, 42, create_parents=True)
    assert record == {"a": "scalar_not_dict"}


def test_datapath_set_create_parents_int_segment_in_parent_noop() -> None:
    """DataPath.set(create_parents=True) refuses silently when an int segment appears in the parent path.

    str parents before the int are created (the early-return fires at the int segment),
    but the leaf value is never written.  The caller should not rely on partial state.
    """
    record: dict[str, Any] = {}
    DataPath.parse("a.0.c").set(record, 42, create_parents=True)
    # Won't auto-create a list entry for the int segment — the value 42 is not written.
    assert "c" not in record.get("a", {}).get("0", {})


# ---------------------------------------------------------------------------
# DataPath.has
# ---------------------------------------------------------------------------


def test_datapath_has_top_level_present() -> None:
    """DataPath.has returns True for a present top-level key."""
    assert DataPath.parse("a").has({"a": 1})


def test_datapath_has_top_level_absent() -> None:
    """DataPath.has returns False for an absent top-level key."""
    assert not DataPath.parse("a").has({"b": 1})


def test_datapath_has_present_with_none_value() -> None:
    """has() returns True even when the value is explicitly None."""
    assert DataPath.parse("a").has({"a": None})


def test_datapath_has_nested_present() -> None:
    """DataPath.has returns True when the full nested path is present."""
    assert DataPath.parse("a.b.c").has({"a": {"b": {"c": 1}}})


def test_datapath_has_nested_missing_intermediate() -> None:
    """DataPath.has returns False when an intermediate segment is absent."""
    assert not DataPath.parse("a.b.c").has({"a": {}})


def test_datapath_has_non_dict_record() -> None:
    """DataPath.has returns False when the root record is not a dict."""
    assert not DataPath.parse("a").has([1, 2, 3])
