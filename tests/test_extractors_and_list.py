"""Unit tests for schema/extractors.py edge cases and list.py deduplication paths."""

from types import SimpleNamespace
from typing import Any, List

import pytest

from incorporator import Incorporator
from incorporator.list import IncorporatorList, _deduplicate_extracted
from incorporator.schema.converters import _EachSentinel
from incorporator.schema.extractors import (
    as_list,
    each,
    join_all,
    link_to,
    link_to_list,
    pluck,
    split_and_get,
    sum_attributes,
)
from incorporator.schema.path import DataPath
from incorporator.schema.router import extract_parent_data, resolve_declarative_routing


class Peer(Incorporator):
    """Tiny live-registry Incorporator subclass — link_to's real target shape.

    Direct construction auto-registers into ``Peer.inc_dict`` via
    ``model_post_init`` (no ``incorp()`` / network call needed), mirroring
    ``tests/test_inflow_state.py``'s bare-class pattern.
    """

    name: str | None = None


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
# 3. link_to — str_key fallback, laziness, and construction guard
# ==========================================


def test_link_to_string_key_coercion_lookup() -> None:
    """link_to must fall back to str(key) lookup when an int key isn't found directly.

    The registry entry is keyed by the STRING form of the id (e.g. the
    source data's identity field arrived as text); the lookup value is an
    INT (e.g. an FK elsewhere in the payload was already coerced). ``str(key)``
    absorbs exactly this "API returns int, registry keyed by string" mismatch.
    """
    # Bind to a local — Peer.inc_dict is a WeakValueDictionary, so an
    # unbound instance is reclaimed the moment the constructor call returns.
    alice = Peer(inc_code="42", name="Alice")
    mapper = link_to(Peer)

    result = mapper(42)  # int key; registry holds the string "42"
    assert result is alice
    assert result.name == "Alice"


def test_link_to_none_key_returns_none() -> None:
    """When the lookup key resolves to None, mapper must return None."""
    _alice = Peer(inc_code=1, name="Alice")
    mapper = link_to(Peer)
    assert mapper(None) is None


def test_link_to_list_non_list_input_returns_empty() -> None:
    """link_to_list must return [] when the value is not a list."""
    _alice = Peer(inc_code=1, name="Alice")
    mapper = link_to_list(Peer)
    assert mapper("not-a-list") == []
    assert mapper(None) == []


def test_link_to_plain_list_target_raises_type_error() -> None:
    """link_to raises TypeError at construction for an inc_dict-less target (plain list).

    Locked behavior removal (2026-07): the old eager-copy path silently
    accepted a plain ``list`` of ``SimpleNamespace``-like objects. That
    support is deliberately dropped — the target must expose a live
    ``inc_dict`` mapping (an IncorporatorList or Incorporator subclass).
    """
    with pytest.raises(TypeError, match="inc_dict"):
        link_to([SimpleNamespace(inc_code=1, name="Alice")])


def test_link_to_resolves_once_empty_target_populates() -> None:
    """link_to(EmptyPeer) built against an empty target resolves once the peer populates.

    This is the fork-landmine guard (locked decision #2): the Op must
    re-read ``dataset.inc_dict`` on EVERY call rather than caching the
    reference on first use — Incorporator._ensure_inc_dict() forks a
    subclass's inc_dict off the shared base default on that class's FIRST
    write, so caching before the first write (even lazily) would miss the
    forked dict forever.
    """

    class EmptyPeer(Incorporator):
        name: str | None = None

    op = link_to(EmptyPeer)
    assert op(1) is None  # target is still empty — no crash, resolves to None

    # Bind to a local — inc_dict is a WeakValueDictionary; an unbound instance
    # would be reclaimed before the next lookup even runs.
    peer = EmptyPeer(inc_code=1, name="Daytona")  # populates EmptyPeer.inc_dict via model_post_init

    result = op(1)  # the SAME op — proves live re-read, not a build-time snapshot
    assert result is peer
    assert result.name == "Daytona"


def test_link_to_is_pure_false_and_not_lru_cache_wrapped() -> None:
    """link_to's Op must stay is_pure=False — a lru_cache wrap would freeze a stale None forever."""

    class MutablePeer(Incorporator):
        name: str | None = None

    op = link_to(MutablePeer)
    assert op.is_pure is False

    assert op(7) is None  # not yet populated
    _talladega = MutablePeer(inc_code=7, name="Talladega")
    # If this Op were lru_cache-wrapped, the cached None from the call above
    # would be returned again here instead of the freshly-registered peer.
    assert op(7) is not None
    assert op(7).name == "Talladega"


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

    class Book(Incorporator):
        pass

    _btc = Book(inc_code="BTC")
    _eth = Book(inc_code="ETH")
    op = link_to(Book, extractor=str.upper)
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
    way, but skipping it saves the str-coercion + lookups AND prevents a
    future warning-instrumented lookup from falsely surfacing this as a
    "missed join" when it's actually a missing FK.
    """

    class Book(Incorporator):
        pass

    _btc = Book(inc_code="BTC")
    _eth = Book(inc_code="ETH")
    # Extractor that always returns empty string — a stand-in for a real
    # extractor failing to compute a key from messy input.
    op = link_to(Book, extractor=lambda v: "")
    assert op("btc") is None  # extractor returned "", short-circuit

    # Extractor that returns "n/a" — common in real data cleaning fns.
    op_na = link_to(Book, extractor=lambda v: "n/a")
    assert op_na("btc") is None

    # Sanity: a real-value extractor still hits the registry.
    op_ok = link_to(Book, extractor=str.upper)
    assert op_ok("btc").inc_code == "BTC"


def test_link_to_list_filters_garbage_elements() -> None:
    """link_to_list filters garbage list elements before invoking the per-element linker."""

    class Book(Incorporator):
        pass

    _btc = Book(inc_code="BTC")
    _eth = Book(inc_code="ETH")
    op = link_to_list(Book, extractor=str.upper)
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
# 12. DataPath — int segment on dict uses string key
# ==========================================


def test_datapath_int_segment_on_dict_uses_string_key() -> None:
    """A path like "a.0" applied to {"a": {"0": "x"}} resolves via
    string-key lookup. The int(0) segment from DataPath.parse becomes
    str("0") inside resolve() when the current node is a dict.
    """
    path = DataPath.parse("a.0")
    assert path.resolve({"a": {"0": "x"}}) == "x"
    assert path.resolve({"a": ["zero", "one"]}) == "zero"  # list branch still uses int


# ==========================================
# 13. as_list() — cross-row mutable-aliasing fix (D7-03)
# ==========================================


def test_as_list_returns_distinct_objects_across_calls() -> None:
    """as_list()'s Op must return a fresh list per invocation, even for equal scalar inputs.

    Pre-fix (is_pure=True), the Op's lru_cache wrapper returned the IDENTICAL
    list object for two calls with equal hashable scalars — mutating one row's
    list silently mutated its sibling row's list too. This must fail against
    the pre-fix code (git stash the one-line is_pure=False change and re-run).
    """
    op = as_list()
    first = op(5)
    second = op(5)
    assert first == second == [5]
    assert first is not second

    first.append("mutated")
    assert second == [5]  # sibling untouched


def test_as_list_scalar_wrapping_and_passthrough_unchanged() -> None:
    """as_list() still wraps scalars and passes lists through unchanged (behavior parity)."""
    op = as_list()
    assert op(1) == [1]
    assert op([1, 2, 3]) == [1, 2, 3]


# ==========================================
# 14. split_and_get(pure=...) — opt-out for arbitrary user cast_type (D2-05)
# ==========================================


def test_split_and_get_pure_false_invokes_cast_type_per_row() -> None:
    """split_and_get(pure=False) must invoke a stateful cast_type once PER ROW, no memoization."""
    calls: List[Any] = []

    def counting_cast(value: str) -> str:
        calls.append(value)
        return value

    op = split_and_get("/", index=-1, cast_type=counting_cast, pure=False)
    for _ in range(4):
        op("a/b/42")
    assert len(calls) == 4  # invoked every time, no caching


def test_split_and_get_default_pure_memoizes_cast_type() -> None:
    """split_and_get's default (pure=True) still memoizes cast_type — the shipped low-cardinality win."""
    calls: List[Any] = []

    def counting_cast(value: str) -> int:
        calls.append(value)
        return int(value)

    op = split_and_get("/", index=-1, cast_type=counting_cast)
    for _ in range(4):
        op("a/b/42")
    assert len(calls) == 1  # memoized — computed once for the repeated input


def test_split_and_get_garbage_short_circuits_before_cast_type() -> None:
    """Garbage inputs must short-circuit to None before the cast_type callable is ever invoked."""
    calls: List[Any] = []

    def counting_cast(value: str) -> str:
        calls.append(value)
        return value

    op = split_and_get("/", index=-1, cast_type=counting_cast, pure=False)
    for garbage in (None, "", "n/a", "null", "unknown", "nan", "undefined"):
        assert op(garbage) is None
    assert calls == []  # cast_type never invoked on garbage


# ==========================================
# 15. Declarative POST token path — as_list() unaffected by the purity fix
# ==========================================


def test_as_list_handles_unhashable_list_input_directly() -> None:
    """as_list()'s Op, called directly with a list argument, returns it unchanged.

    Pins the json_payload POST-token path (router.resolve_declarative_routing's
    ``v(extracted_data)`` call): a list argument is unhashable, so pre-fix this
    already bypassed the lru_cache via the __wrapped__ fallback in Op.__call__.
    Post-fix (is_pure=False), there is no cache wrapper at all — same observable
    behavior either way. Behavior-neutral pin, not a new capability.
    """
    op = as_list()
    payload = [1, 2, 3]
    result = op(payload)
    assert result is payload  # list passthrough, untouched


# ==========================================
# 16. each() — sentinel marker (D7-05)
# ==========================================


def test_each_returns_each_sentinel_instance() -> None:
    """each() returns an _EachSentinel instance — the marker resolve_declarative_routing switches on."""
    marker = each()
    assert isinstance(marker, _EachSentinel)


# ==========================================
# 17. as_list() — declarative routing (bulk, non-each) branch (D7-05)
# ==========================================


def test_as_list_declarative_routing_bulk_branch() -> None:
    """as_list() through resolve_declarative_routing's bulk (non-each) POST branch.

    ``as_list()``'s Op is callable with the whole extracted_data list, so it
    takes the ``built_payload`` branch (not the per-item ``each()`` fanout):
    one payload dict, replicated once per source URL.
    """
    extracted_data = ["id-1", "id-2", "id-3"]
    source_urls = ["https://api.example.com/bulk"]

    kwargs = resolve_declarative_routing(
        "Caller",
        extracted_data,
        source_urls,
        http_method="POST",
        json_payload={"ids": as_list()},
        inc_url=source_urls[0],
    )

    payload_list = kwargs["payload_list"]
    assert len(payload_list) == len(source_urls)
    assert payload_list == [{"ids": extracted_data}]


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
