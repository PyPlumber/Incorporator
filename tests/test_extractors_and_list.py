"""Unit tests for schema/extractors.py edge cases and list.py deduplication paths."""

from types import SimpleNamespace
from typing import Any, List

import pytest

from incorporator.list import IncorporatorList, _deduplicate_extracted
from incorporator.schema.extractors import (
    as_list,
    join_all,
    link_to,
    link_to_list,
    pluck,
    split_and_get,
    sum_attributes,
)
from incorporator.schema.path import DataPath
from incorporator.schema.router import extract_parent_data


# ==========================================
# 1. sum_attributes edge cases
# ==========================================


def test_sum_attributes_skips_non_numeric_values() -> None:
    """Non-numeric values must be silently skipped; valid ones are summed."""
    result = sum_attributes(1.0, "not-a-number", None, 2.5, "also-bad")
    assert result == pytest.approx(3.5)


def test_sum_attributes_all_none_returns_zero() -> None:
    """All-None input must return 0.0 without raising."""
    assert sum_attributes(None, None) == 0.0


def test_sum_attributes_skips_type_error() -> None:
    """Values that trigger TypeError on float() must be silently skipped."""
    result = sum_attributes(10, [], {})  # list and dict → TypeError in float()
    assert result == pytest.approx(10.0)


# ==========================================
# 2. split_and_get edge cases
# ==========================================


def test_split_and_get_index_out_of_range_returns_none() -> None:
    """An out-of-range index must return None without raising."""
    splitter = split_and_get("/", index=99)
    assert splitter("a/b/c") is None


def test_split_and_get_none_input_returns_none() -> None:
    """None input must return None."""
    splitter = split_and_get("/")
    assert splitter(None) is None


def test_split_and_get_empty_string_returns_none() -> None:
    """Empty string input must return None."""
    splitter = split_and_get("/")
    assert splitter("") is None


def test_split_and_get_cast_type_failure_returns_none() -> None:
    """When cast_type raises on the extracted segment, None is returned."""
    splitter = split_and_get("/", index=0, cast_type=int)
    assert splitter("abc/def") is None  # "abc" cannot be cast to int


# ==========================================
# 3. link_to — str_key fallback
# ==========================================


def test_link_to_string_key_coercion_lookup() -> None:
    """link_to must fall back to str(key) lookup when integer key is not found directly."""
    # SimpleNamespace is not weakrefable → goes into fallback_registry
    items = [SimpleNamespace(inc_code=42, name="Alice")]
    mapper = link_to(items)

    # Look up with the string representation of the integer key
    result = mapper("42")  # str(42) == "42" — should find the item
    assert result is not None
    assert result.name == "Alice"


def test_link_to_none_key_returns_none() -> None:
    """When the lookup key resolves to None, mapper must return None."""
    items = [SimpleNamespace(inc_code=1, name="Alice")]
    mapper = link_to(items)
    assert mapper(None) is None


def test_link_to_list_non_list_input_returns_empty() -> None:
    """link_to_list must return [] when the value is not a list."""
    items = [SimpleNamespace(inc_code=1, name="Alice")]
    mapper = link_to_list(items)
    assert mapper("not-a-list") == []
    assert mapper(None) == []


# ==========================================
# 4. pluck edge cases
# ==========================================


def test_pluck_stops_when_intermediate_not_dict() -> None:
    """pluck must return None and not crash when an intermediate value is not a dict."""
    plucker = pluck("a.b.c")
    data = {"a": "flat-string-not-a-dict"}  # "a" exists but isn't a dict → can't drill to "b"
    result = plucker(data)
    assert result is None


def test_pluck_stops_when_intermediate_key_missing() -> None:
    """pluck must return None when a key in the chain is absent."""
    plucker = pluck("x.y.z")
    result = plucker({"x": {"missing": True}})  # "y" not in the nested dict
    assert result is None


def test_pluck_non_dict_top_level_returns_none() -> None:
    """When the top-level value is not a dict or list, pluck returns None.

    The outer isinstance(val, dict) gate was removed to allow list-rooted
    paths.  A plain non-traversable value (e.g. a bare string) can no longer
    be returned unchanged — the loop reaches the else branch and yields None.
    """
    plucker = pluck("anything")
    result = plucker("a plain string")
    assert result is None


# ==========================================
# 5. join_all edge cases
# ==========================================


def test_join_all_non_list_input_stringifies() -> None:
    """join_all must stringify a non-list value directly."""
    joiner = join_all(",")
    assert joiner(42) == "42"
    assert joiner("hello") == "hello"


def test_join_all_list_filters_none() -> None:
    """join_all must skip None entries in the list."""
    joiner = join_all(",")
    result = joiner([1, None, 3, None, 5])
    assert result == "1,3,5"


# ==========================================
# 6. _deduplicate_extracted — non-hashable items
# ==========================================


def test_deduplicate_extracted_non_hashable_included_as_is() -> None:
    """Non-hashable items (dicts) must be appended as-is after deduplicating hashables."""
    data: List[Any] = [1, 2, 1, {"key": "val"}, {"other": True}]
    result = _deduplicate_extracted(data)
    # Hashable integers are deduplicated; dicts appended
    assert 1 in result
    assert 2 in result
    assert result.count(1) == 1  # deduped
    assert {"key": "val"} in result
    assert {"other": True} in result


def test_deduplicate_extracted_all_hashable_deduplicates() -> None:
    """A fully hashable list must be deduplicated preserving insertion order."""
    result = _deduplicate_extracted([3, 1, 2, 1, 3])
    assert result == [3, 1, 2]


# ==========================================
# 7. IncorporatorList — GC sentinel
# ==========================================


def test_incorporator_list_gc_warn_on_gc_flag() -> None:
    """Setting _warn_on_gc=True must not raise when __del__ is called on a non-empty list."""
    from pydantic import BaseModel

    class _FakeModel(BaseModel):
        id: int = 0

    obj = _FakeModel()
    lst = IncorporatorList(_FakeModel, [obj])
    lst._warn_on_gc = True  # type: ignore[attr-defined]
    # Explicitly invoke __del__ — must not raise
    lst.__del__()


# ==========================================
# 8. H3 reshape: graph-map helpers' null-handling aligned with inc()
#
# pluck's ``chain``, link_to's ``extractor``, link_to_list's
# ``extractor``, and split_and_get's input handling all now skip the
# user-supplied callable when the source value is garbage (per
# :func:`is_garbage_value`).  Garbage → silent None — no
# "conv_dict failed" WARNING at the dispatch boundary.
# ==========================================


def test_pluck_chain_skips_on_garbage_extracted_value() -> None:
    """pluck("a.b", chain=str.lower) short-circuits to None when the path is missing.

    The chain callable is never invoked on garbage extracted values, so
    a chain of ``str.lower`` does not raise TypeError on None paths.
    """
    op = pluck("data.title", chain=str.lower)
    # Missing intermediate key
    assert op({"data": {}}) is None
    # Explicit None at the leaf
    assert op({"data": {"title": None}}) is None
    # Garbage-sentinel string at the leaf
    assert op({"data": {"title": "n/a"}}) == "n/a"  # falsy by garbage test, returned as-is

    # Real data still flows through chain.
    assert op({"data": {"title": "Hello"}}) == "hello"


def test_link_to_extractor_skips_on_garbage_fk() -> None:
    """link_to(dataset, extractor=str.upper) short-circuits to None on garbage FKs.

    Without the pre-check, ``str.upper(None)`` would raise TypeError and
    trigger a per-row WARNING at the builder.py dispatch boundary.
    """
    books = [
        SimpleNamespace(inc_code="BTC"),
        SimpleNamespace(inc_code="ETH"),
    ]
    op = link_to(books, extractor=str.upper)
    # Garbage FKs short-circuit silently.
    assert op(None) is None
    assert op("") is None
    assert op("n/a") is None
    # Real FK still routes through extractor and registry.
    assert op("btc").inc_code == "BTC"


def test_link_to_extractor_return_value_garbage_check() -> None:
    """link_to extractor returning a garbage value short-circuits to None silently.

    Symmetric output-side guard (senior-review M4): when a user-supplied
    extractor returns garbage (e.g. ``str.strip`` on whitespace-only
    input returning ``""``, or a custom extractor returning ``"n/a"``
    when it can't compute a key), short-circuit to ``None`` before
    the registry lookup.  The dict lookup wouldn't find anything either
    way, but skipping it saves the str-coercion + four lookups AND
    prevents a future warning-instrumented lookup from falsely
    surfacing this as a "missed join" when it's actually a missing FK.
    """
    books = [SimpleNamespace(inc_code="BTC"), SimpleNamespace(inc_code="ETH")]
    # Extractor that always returns empty string — a stand-in for a real
    # extractor failing to compute a key from messy input.
    op = link_to(books, extractor=lambda v: "")
    assert op("btc") is None  # extractor returned "", short-circuit

    # Extractor that returns "n/a" — common in real data cleaning fns.
    op_na = link_to(books, extractor=lambda v: "n/a")
    assert op_na("btc") is None

    # Sanity: a real-value extractor still hits the registry.
    op_ok = link_to(books, extractor=str.upper)
    assert op_ok("btc").inc_code == "BTC"


def test_link_to_list_filters_garbage_elements() -> None:
    """link_to_list filters garbage list elements before invoking the per-element linker."""
    books = [SimpleNamespace(inc_code="BTC"), SimpleNamespace(inc_code="ETH")]
    op = link_to_list(books, extractor=str.upper)
    result = op(["btc", None, "n/a", "eth", ""])
    assert [item.inc_code for item in result] == ["BTC", "ETH"]


def test_split_and_get_widens_null_check_to_garbage_set() -> None:
    """split_and_get short-circuits to None on the full garbage-value set.

    Previously the narrow check was ``value is None or value == ""``;
    now garbage strings (``"n/a"``, ``"null"``, ``"unknown"``, ``"nan"``,
    ``"undefined"``) also short-circuit to None instead of being
    attempted as a delimited path.
    """
    op = split_and_get("/", index=-1, cast_type=int)
    assert op("n/a") is None
    assert op("null") is None
    assert op("unknown") is None
    assert op("nan") is None
    assert op("undefined") is None
    # Legacy null path still works.
    assert op(None) is None
    assert op("") is None
    # Real input still parses.
    assert op("https://api.com/items/42/") == 42


# ==========================================
# 9. pluck() — integer-index list navigation
# ==========================================


def test_pluck_intermediate_list_index() -> None:
    """pluck traverses a list segment via a digit path part.

    Proves that ``"splits.0.stat"`` on ``{"splits": [{"stat": {"era": 3.2}}]}``
    correctly indexes into the list and continues drilling into the dict.
    """
    plucker = pluck("splits.0.stat")
    result = plucker({"splits": [{"stat": {"era": 3.2}}]})
    assert result == {"era": 3.2}


def test_pluck_list_rooted_value() -> None:
    """pluck handles a list as the top-level value when the first segment is a digit.

    Proves that list-rooted paths (e.g. ``"0.name"``) work now that
    the outer isinstance(val, dict) gate has been removed.
    """
    plucker = pluck("0.name")
    result = plucker([{"name": "a"}])
    assert result == "a"


def test_pluck_out_of_range_returns_none() -> None:
    """pluck returns None when a digit index exceeds the list length."""
    plucker = pluck("a.0")
    result = plucker({"a": []})
    assert result is None


def test_pluck_negative_index_returns_none() -> None:
    """pluck treats a negative index segment as a non-matching key, returning None.

    ``"-1".isdigit()`` is False, so the loop falls through to the
    ``else`` branch and short-circuits to None — negative indexing is
    intentionally unsupported.
    """
    plucker = pluck("a.-1")
    result = plucker({"a": [1, 2]})
    assert result is None


# ==========================================
# 10. DataPath — direct unit tests
# ==========================================


def test_drill_path_dict_only() -> None:
    """DataPath walks nested dicts via dot-notation."""
    assert DataPath.parse("a.b").resolve({"a": {"b": 1}}) == 1


def test_drill_path_list_digit_index() -> None:
    """DataPath uses a digit segment to index into a list."""
    assert DataPath.parse("a.1").resolve({"a": [10, 20]}) == 20


def test_drill_path_mixed_dict_and_list() -> None:
    """DataPath navigates across both dict and list nodes in a single path."""
    payload = {"splits": [{"stat": {"era": 3.2}}]}
    assert DataPath.parse("splits.0.stat").resolve(payload) == {"era": 3.2}


def test_drill_path_none_mid_walk() -> None:
    """DataPath returns None when an intermediate node is None."""
    assert DataPath.parse("a.b").resolve({"a": None}) is None


def test_drill_path_single_token() -> None:
    """DataPath with a single-segment path returns the top-level value."""
    assert DataPath.parse("x").resolve({"x": 42}) == 42


def test_drill_path_missing_key() -> None:
    """DataPath returns None for a key absent from the dict."""
    assert DataPath.parse("b").resolve({"a": 1}) is None


def test_drill_path_out_of_range_index() -> None:
    """DataPath returns None when a digit index exceeds the list length."""
    assert DataPath.parse("a.0").resolve({"a": []}) is None


def test_drill_path_empty_path_raises() -> None:
    """DataPath.parse raises ValueError for an empty path string."""
    with pytest.raises(ValueError, match="empty path string"):
        DataPath.parse("")


def test_drill_path_non_navigable_else_returns_none() -> None:
    """DataPath returns None when a non-navigable node is encountered mid-walk."""
    assert DataPath.parse("a").resolve("scalar") is None


# ==========================================
# 11. extract_parent_data — positional digit vs fanout
# ==========================================


def test_extract_parent_data_positional_digit() -> None:
    """extract_parent_data uses a digit segment as a positional index, not fanout."""
    parents = [{"results": [{"url": "x"}, {"url": "y"}]}]
    assert extract_parent_data(parents, "results.0.url") == ["x"]


def test_extract_parent_data_non_digit_fanout_unchanged() -> None:
    """extract_parent_data fans out over list items for non-digit segments (regression lock)."""
    parents = [{"results": [{"url": "x"}, {"url": "y"}]}]
    assert extract_parent_data(parents, "results.url") == ["x", "y"]


# ==========================================
# 12. IncorporatorList.failed_sources — cache identity and correctness
# ==========================================


def _make_list_with_rejects() -> IncorporatorList:  # type: ignore[type-arg]
    """Return an IncorporatorList with two RejectEntry objects."""
    from pydantic import BaseModel

    from incorporator.rejects import RejectEntry

    class _M(BaseModel):
        id: int = 0

    entries = [
        RejectEntry(source="https://api.example.com/a", error_kind="HTTP", message="404"),
        RejectEntry(source="https://api.example.com/b", error_kind="HTTP", message="500"),
    ]
    return IncorporatorList(_M, [], rejects=entries)


def test_failed_sources_cache_identity() -> None:
    """Repeated access to failed_sources returns the exact same list object (cache hit)."""
    lst = _make_list_with_rejects()
    first = lst.failed_sources
    second = lst.failed_sources
    assert first is second


def test_failed_sources_cache_correctness() -> None:
    """failed_sources matches [e.source for e in rejects] exactly."""
    lst = _make_list_with_rejects()
    assert lst.failed_sources == [e.source for e in lst.rejects]


def test_failed_sources_empty_list_cache_identity() -> None:
    """Cache identity holds even when there are no rejects (empty list case)."""
    from pydantic import BaseModel

    class _M(BaseModel):
        id: int = 0

    lst: IncorporatorList[_M] = IncorporatorList(_M, [])
    first = lst.failed_sources
    second = lst.failed_sources
    assert first is second
    assert first == []
