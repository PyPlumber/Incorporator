"""Unit tests for Ex / Nm / Pk frozen-dataclass wrappers and DataPath.pop/set."""

from dataclasses import FrozenInstanceError
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
