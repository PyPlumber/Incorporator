"""Unit tests for the Incorporator Columnar Type Engine and URL tools."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from incorporator.schema import router
from incorporator.schema.builder import apply_etl_transformations
from incorporator.schema.converters import (
    CalcAllOp,
    CalcOp,
    GARBAGE_VALUES,
    _inc_clear_for_tests,
    calc,
    calc_all,
    inc,
    is_garbage_value,
    new,
    parses_as_datetime,
    parses_as_float,
    parses_as_int,
)
from incorporator.schema.extractors import (
    each,
    join_all,
    link_to,
    pluck,
    split_and_get,
)


def test_inc_type_ranked_engine_bools_and_dates() -> None:
    """Asserts string logic, empty value fallback, and date parsing for inc()."""
    # --- Booleans ---
    inc_bool = inc(bool)
    assert inc_bool("true") is True
    assert inc_bool("1") is True
    assert inc_bool("false") is False
    assert inc_bool("junk") is False
    assert inc_bool(None) is None  # Pipeline Null-Safety
    assert inc_bool("") is None

    # --- Dates ---
    inc_date = inc(datetime)
    # Standard ISO
    dt_iso = inc_date("2026-04-21T23:59:59Z")
    assert isinstance(dt_iso, datetime) and dt_iso.year == 2026

    # Custom Rick & Morty format fallback
    dt_rm = inc_date("December 2, 2013")
    assert isinstance(dt_rm, datetime) and dt_rm.year == 2013 and dt_rm.month == 12

    # SQL Timestamp format fallback
    dt_sql = inc_date("2026-04-22 23:59:59")
    assert isinstance(dt_sql, datetime) and dt_sql.year == 2026

    # Graceful degradation (returns None instead of crashing ETL with ValueError)
    assert inc_date("not-a-valid-date-format") is None


def test_inc_type_ranked_engine_numbers_and_dirty_data() -> None:
    """Asserts robust string-to-number casting and dirty data cleaning via fallbacks."""
    inc_int = inc(int)
    inc_flt = inc(float)

    # 1. Direct Execution & Dirty Data Cleaning
    assert inc_int("1,500") == 1500  # Strips commas via fallback
    assert inc_flt("1,500.50") == 1500.5

    # 2. Trap dirty API strings gracefully
    assert inc_int("unknown") is None
    assert inc_flt("N/A") is None
    assert inc_int(None) is None


def test_inc_new_sentinel() -> None:
    """Asserts that the 'new' sentinel safely passes any data type through."""
    inc_any = inc(new)

    assert inc_any("String") == "String"
    assert inc_any(100) == 100
    assert inc_any({"complex": "dict"}) == {"complex": "dict"}
    assert inc_any(None) is None


def test_inc_caches_closures_per_type_and_default() -> None:
    """``inc(target_type, default)`` returns the same closure on repeated calls.

    The factory is wrapped in ``functools.lru_cache``, so two callers
    constructing ``inc(int)`` independently share one closure instance.
    Different ``(target_type, default)`` pairs produce different
    closures; ``cache_info()`` exposes hits/misses for diagnostics.
    """
    _inc_clear_for_tests()

    c1 = inc(int)
    c2 = inc(int)
    c3 = inc(int, default=0)
    c4 = inc(float)

    # Same (type, default) → cache hit returns the SAME closure object.
    assert c1 is c2, "Repeated inc(int) calls must return the same closure"
    # Different default → distinct closure.
    assert c1 is not c3, "Different default must yield a different closure"
    # Different type → distinct closure.
    assert c1 is not c4, "Different target_type must yield a different closure"

    info = inc.cache_info()  # type: ignore[attr-defined]
    assert info.hits >= 1, "Repeated inc(int) call should register at least one hit"
    assert info.misses >= 3, "Three distinct cache keys should register at least three misses"

    # Behavioural sanity — the shared closure still works after cache hits.
    assert c1("42") == 42
    assert c3("garbage") == 0


def test_inc_warns_on_bare_callable_misuse(caplog: pytest.LogCaptureFixture) -> None:
    """``inc(str.upper)`` is a misuse: it must warn once and still pass values through unchanged.

    Transforms belong in ``calc``/``pluck``; ``inc`` on a bare callable used to silently no-op.
    """
    _inc_clear_for_tests()

    with caplog.at_level("WARNING", logger="incorporator.schema.converters"):
        inc_upper = inc(str.upper)
        assert inc_upper("hello") == "hello"

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1, "inc() misuse should warn exactly once at build time"
    assert "non-coercible" in warnings[0].getMessage()
    assert "calc(fn, key)" in warnings[0].getMessage()
    assert "pluck(key, chain=fn)" in warnings[0].getMessage()


def test_inc_warns_on_bare_instance_misuse(caplog: pytest.LogCaptureFixture) -> None:
    """``inc(some_instance)``, a non-type object Pydantic can't build a schema for, likewise warns.

    The value still passes through unchanged.

    Note: a plain ``lambda``/function is accepted by Pydantic's validate-call schema
    generation and takes the separate, already-covered per-row runtime warning path
    (``_ranked_converter``'s cast-failure branch), not this build-time misuse branch.
    """
    _inc_clear_for_tests()

    class _NotAType:
        pass

    with caplog.at_level("WARNING", logger="incorporator.schema.converters"):
        inc_instance = inc(_NotAType())
        assert inc_instance("hello") == "hello"
        assert inc_instance(42) == 42

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1, "inc() misuse should warn exactly once at build time"


def test_inc_new_sentinel_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    """``inc(new)`` legitimately reaches the same identity failsafe and must stay silent."""
    _inc_clear_for_tests()

    with caplog.at_level("WARNING", logger="incorporator.schema.converters"):
        inc_any = inc(new)
        assert inc_any("String") == "String"
        assert inc_any(100) == 100

    assert not [r for r in caplog.records if r.levelname == "WARNING"], (
        "inc(new) is a legitimate pass-through and must never warn"
    )


def test_inc_concrete_and_union_types_stay_silent(caplog: pytest.LogCaptureFixture) -> None:
    """Real coercible types (concrete + Pydantic union) build and coerce correctly, no warning."""
    _inc_clear_for_tests()

    with caplog.at_level("WARNING", logger="incorporator.schema.converters"):
        assert inc(int)("42") == 42
        assert inc(float)("1.5") == 1.5
        assert inc(bool)("true") is True
        assert inc(str)(42) == "42"
        dt = inc(datetime)("2026-04-21T23:59:59Z")
        assert isinstance(dt, datetime) and dt.year == 2026
        union_conv = inc(int | None)
        assert union_conv("7") == 7
        assert union_conv(None) is None

    assert not [r for r in caplog.records if r.levelname == "WARNING"], (
        "Coercible concrete/union types must never trigger the misuse warning"
    )


def test_calc_and_calc_all_markers() -> None:
    """Asserts that calc and calc_all correctly generate Columnar Op markers."""

    def dummy_math(x: float, y: float) -> float:
        return x + y

    # 1. Row-based calc
    c_op = calc(dummy_math, "mass", "gravity", default=0, target_type=float)
    assert isinstance(c_op, CalcOp)
    assert c_op.default == 0
    assert c_op.target_type is float
    assert [p.source for p in c_op.input_list] == ["mass", "gravity"]

    # 2. Batch column calc_all
    ca_op = calc_all(dummy_math, target_type=int)
    assert isinstance(ca_op, CalcAllOp)
    assert ca_op.target_type is int
    assert ca_op.input_list == []  # Defaults to empty list intelligently


def test_url_toolkit() -> None:
    """Asserts split_and_get and pluck function correctly."""

    # --- split_and_get ---
    # Because .strip('/') removes the trailing slash automatically, index is ALWAYS -1
    extractor = split_and_get(delimiter="/", index=-1, cast_type=int)

    assert extractor("https://api.com/user/101") == 101
    assert extractor("https://api.com/user/101/") == 101
    assert extractor(None) is None

    # --- pluck ---
    plucker = pluck("homeworld", chain=split_and_get(delimiter="/", index=-1, cast_type=int))

    # Test dictionary pluck (e.g. from JSON response)
    assert plucker({"name": "Earth", "homeworld": "https://api.com/planet/5/"}) == 5

    # Non-dict/non-list top-level values return None — the outer isinstance(val, dict)
    # gate was removed to support list-rooted paths; plain strings no longer fall through.
    assert plucker("https://api.com/planet/5/") is None


def test_link_to_relational_mapping() -> None:
    """Asserts relational mapping handles plain lists and nulls."""

    # Use SimpleNamespace to mock the object-oriented structure of Pydantic models
    mock_obj_1 = SimpleNamespace(inc_code=1, name="Daytona")
    mock_obj_2 = SimpleNamespace(inc_code="A100", name="Talladega")

    # Test 1: Passing a plain Python list (link_to should build the dict dynamically)
    plain_list = [mock_obj_1, mock_obj_2]
    mapper = link_to(plain_list)

    assert mapper(1) == mock_obj_1
    assert mapper("1") == mock_obj_1  # String-to-int cast fallback
    assert mapper("A100") == mock_obj_2  # Direct string match
    assert mapper(None) is None
    assert mapper("bad_id") is None

    # Test 2: Using an extractor
    extractor_mapper = link_to(plain_list, extractor=lambda x: int(x) * 1)
    assert extractor_mapper("1") == mock_obj_1


class MockObj:
    def __init__(self, **kwargs: Any):
        self.__dict__.update(kwargs)


def test_extract_parent_data() -> None:
    # 1. Standard Object Drill
    parents = [MockObj(vehicle=MockObj(vin="123")), MockObj(vehicle=MockObj(vin="456"))]
    assert router.extract_parent_data(parents, "vehicle.vin") == ["123", "456"]

    # 2. Schema Splintering (List nested inside the drill path)
    splintered_parents = [MockObj(vehicle=[MockObj(vin="A"), MockObj(vin="B")])]
    assert router.extract_parent_data(splintered_parents, "vehicle.vin") == ["A", "B"]

    # 3. Dictionary handling and Missing Keys (Graceful degradation)
    mixed_parents = [MockObj(vehicle={"vin": "DictVIN"}), MockObj(vehicle=None)]
    assert router.extract_parent_data(mixed_parents, "vehicle.vin") == ["DictVIN"]

    # 4. Deeply nested missing data
    assert router.extract_parent_data(parents, "vehicle.engine.cylinders") == []


def test_declarative_post_routing() -> None:
    """Verifies that POST tokens correctly map payloads and URLs."""
    extracted_ids = [101, 102]
    source_urls = ["https://api.com/update"]

    # 1. Test the `each()` token (N Concurrent Requests)
    kwargs_each = router.resolve_declarative_routing(
        "Test",
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="POST",
        json_payload={"id": each(), "static": "token"},
    )

    # Should multiply the URL by 2, and create 2 distinct payloads
    assert kwargs_each["inc_url"] == ["https://api.com/update", "https://api.com/update"]
    assert kwargs_each["payload_list"] == [
        {"id": 101, "static": "token"},
        {"id": 102, "static": "token"},
    ]

    # 2. Test the `join_all()` token (1 Bulk Request)
    kwargs_join = router.resolve_declarative_routing(
        "Test",
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="POST",  # 🛡️ THE FIX
        json_payload={"ids": join_all(",")},
    )

    # Should keep 1 URL, and create 1 payload with a joined string
    assert kwargs_join["payload_list"] == [{"ids": "101,102"}]
    assert len(kwargs_join["payload_list"]) == len(source_urls)

    # 3. Test Missing POST URL Exception
    with pytest.raises(ValueError, match="Missing Target URL"):
        router.resolve_declarative_routing(
            "Test",
            extracted_data=extracted_ids,
            source_urls=[],
            http_method="POST",  # 🛡️ THE FIX
            json_payload={"ids": join_all(",")},
        )


def test_declarative_each_with_no_urls_raises_missing_target_url() -> None:
    """D7-02: each() with source_urls=[] must raise, not silently passthrough.

    Before the fix, only ``len(source_urls) == 1`` triggered URL-multiplication;
    ``len(source_urls) == 0`` fell through with ``payload_list`` set and no
    ``inc_url``, degrading into the ""-placeholder path downstream and producing
    cryptic "Security Policy Violation" rejects instead of an actionable error.
    Mirrors the bulk (non-iterative) branch's existing guard verbatim so both
    produce one recognizable error family.
    """
    with pytest.raises(ValueError, match="Missing Target URL"):
        router.resolve_declarative_routing(
            "Test",
            extracted_data=[101, 102],
            source_urls=[],
            http_method="POST",
            json_payload={"id": each(), "static": "token"},
        )


# ==========================================
# Predicate exports (used by the DX Inspector)
# ==========================================


@pytest.mark.parametrize(
    "value,expected",
    [
        # Standard sentinels (matches the GARBAGE_VALUES frozenset)
        ("N/A", True),
        ("n/a", True),
        ("Unknown", True),
        ("null", True),
        ("none", True),
        ("undefined", True),
        ("nan", True),
        ("  N/A  ", True),  # leading/trailing whitespace tolerated
        # Empties
        (None, True),
        ("", True),
        # Real values that should NOT be flagged
        ("0", False),
        ("false", False),
        ("Alice", False),
        ("2022-10-05T12:00:00-04:00", False),
        (42, False),
        (0, False),
    ],
)
def test_is_garbage_value(value: Any, expected: bool) -> None:
    """is_garbage_value mirrors inc()'s internal short-circuit rule."""
    assert is_garbage_value(value) is expected


def test_garbage_values_frozenset_contents() -> None:
    """The shared GARBAGE_VALUES set must contain the canonical entries."""
    assert "n/a" in GARBAGE_VALUES
    assert "null" in GARBAGE_VALUES
    assert "unknown" in GARBAGE_VALUES
    assert "undefined" in GARBAGE_VALUES
    assert "nan" in GARBAGE_VALUES
    assert "none" in GARBAGE_VALUES


@pytest.mark.parametrize(
    "value,expected",
    [
        # The user's failing case: RFC-3339 with timezone offset
        ("2022-10-05T12:00:00-04:00", True),
        # Other common shapes the runtime parser already accepts
        ("2026-05-12T14:32:00Z", True),
        ("2008-09-20T17:23:00.000Z", True),
        ("2008-09-20T17:23:00.123456+0000", True),
        ("2008-09-20", True),  # plain date
        ("2008-09-20 17:23:00", True),  # SQL-ish
        ("December 2, 2013", True),  # custom fallback format
        # Garbage / non-dates must NOT parse
        ("not a date", False),
        ("Alice", False),
        ("", False),
        ("N/A", False),
        (None, False),
        # int/float converter ranks above datetime, so "42" must not match the date fallback.
        ("42", False),
    ],
)
def test_parses_as_datetime(value: Any, expected: bool) -> None:
    """parses_as_datetime routes through _fallback_date — the runtime contract."""
    assert parses_as_datetime(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("42", True),
        ("1,500", True),  # comma-stripping via _fallback_int
        ("-7", True),
        ("3.14", True),  # int(float()) coerces
        ("not a number", False),
        ("", False),
        ("N/A", False),
        (None, False),
    ],
)
def test_parses_as_int(value: Any, expected: bool) -> None:
    """parses_as_int routes through _fallback_int — the runtime contract."""
    assert parses_as_int(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("3.14", True),
        ("1,500.50", True),
        ("0.0", True),
        ("not a number", False),
        ("", False),
        ("Unknown", False),
        (None, False),
    ],
)
def test_parses_as_float(value: Any, expected: bool) -> None:
    """parses_as_float routes through _fallback_float — the runtime contract."""
    assert parses_as_float(value) is expected


def test_get_url_injection() -> None:
    """Verifies that GET requests natively inject IDs into {} templates."""
    extracted_ids = ["alpha", "beta"]
    source_urls = ["https://api.com/users/{}/profile"]

    kwargs = router.resolve_declarative_routing(
        "Test",
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="GET",
    )

    # It should generate N unique URLs
    assert kwargs["inc_url"] == [
        "https://api.com/users/alpha/profile",
        "https://api.com/users/beta/profile",
    ]


def test_get_non_template_plain_ids_are_silently_dropped() -> None:
    """D7-10: GET, no {} template, plain (non-URL) extracted IDs — current behavior pin.

    ``extracted_strs`` are scalar IDs (not "http"/"/"-prefixed), so
    ``valid_urls`` is empty; ``source_urls`` is non-empty so the
    ``elif not source_urls`` legacy-attribute branch is also skipped.
    Neither branch fires and ``kwargs`` is returned unchanged — the
    extracted IDs are silently dropped and ``inc_url`` stays exactly the
    caller-supplied ``source_urls``.  This pins CURRENT behavior; a
    ``logger.warning`` for this silent-drop case is a deferred design
    suggestion, not something this test enforces.
    """
    source_urls = ["https://api.example.com/base"]

    kwargs = router.resolve_declarative_routing(
        "Test",
        extracted_data=["id-1", "id-2"],
        source_urls=source_urls,
        http_method="GET",
        inc_url=source_urls[0],
    )

    assert kwargs["inc_url"] == source_urls[0], "plain IDs must be dropped; inc_url stays untouched"


def test_get_url_children_appended_after_base_url() -> None:
    """D7-10: GET extracted values that look like URLs are appended after source_urls.

    When ``extracted_strs`` entries start with ``"http"`` or ``"/"``,
    the ``valid_urls`` branch fires and the final ``inc_url`` is
    ``source_urls + valid_urls`` — the base URL(s) stay first, HATEOAS
    child URLs are appended after.
    """
    source_urls = ["https://api.example.com/base"]
    extracted_data = [
        "https://api.example.com/child/1",
        "https://api.example.com/child/2",
    ]

    kwargs = router.resolve_declarative_routing(
        "Test",
        extracted_data=extracted_data,
        source_urls=source_urls,
        http_method="GET",
        inc_url=source_urls[0],
    )

    assert kwargs["inc_url"] == source_urls + extracted_data


def test_post_each_url_count_mismatch_raises_missing_target_url() -> None:
    """D7-10: each() with >1 source_urls raises, it does not silently pass through.

    Only ``len(source_urls) == 1`` takes the URL-multiplication branch;
    any other count (0 or >1) is ambiguous for a per-item fan-out POST
    and raises ``ValueError("Missing Target URL")`` — mirrors the bulk
    branch's guard.  Empirically re-verified: this is NOT a silent
    url-count x payload-count mismatch pass-through.
    """
    with pytest.raises(ValueError, match="Missing Target URL"):
        router.resolve_declarative_routing(
            "Test",
            extracted_data=["id-1", "id-2", "id-3"],
            source_urls=["https://api.example.com/1", "https://api.example.com/2"],
            http_method="POST",
            json_payload={"id": each()},
        )


def test_post_empty_form_payload_falls_through_to_json_payload() -> None:
    """D7-10: form_payload={} is falsy — json_payload wins per `or` short-circuit."""
    source_urls = ["https://api.example.com/base"]

    kwargs = router.resolve_declarative_routing(
        "Test",
        extracted_data=["id-1", "id-2"],
        source_urls=source_urls,
        http_method="POST",
        form_payload={},
        json_payload={"x": 1},
    )

    assert kwargs["payload_list"] == [{"x": 1}]
    assert len(kwargs["payload_list"]) == len(source_urls)


# ---------------------------------------------------------------------------
# H3 reshape: calc/calc_all null-handling aligned with inc().
#
# The framework now pre-checks ``is_garbage_value`` on every input before
# invoking the user's ``func``.  When ALL inputs are garbage the func is
# skipped entirely — silent default-out, no warning.  When at least one
# input is real and the func raises on it, the WARNING is preserved
# (genuine anomaly).
# ---------------------------------------------------------------------------


def _run_calc(op: Any, rows: list) -> list:
    """Tiny driver: run a CalcOp/CalcAllOp through the builder's dispatch loop.

    Avoids spinning up a full Incorporator.incorp() call for these focused
    regression tests.
    """
    from incorporator.schema.builder import apply_etl_transformations

    apply_etl_transformations(rows, conv_dict={"out": op}, excl_lst=None, name_chg=None)
    return rows


def test_calc_short_circuits_when_all_inputs_garbage(caplog: pytest.LogCaptureFixture) -> None:
    """All-garbage input → silent default; no warning emitted on the calc path."""
    op = calc(str.lower, "title", default="", target_type=str)
    rows = [{"title": None}, {"title": ""}, {"title": "n/a"}, {"title": "Null"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert all(r["out"] == "" for r in result), "every garbage row should default-out"
    assert not [r for r in caplog.records if "calc failed" in r.getMessage()], (
        "garbage rows must not trigger a calc warning"
    )


def test_calc_still_warns_on_real_func_failure(caplog: pytest.LogCaptureFixture) -> None:
    """At least one real input + func raises on it → WARNING preserved."""

    def explode(x: Any) -> int:
        return int(x)  # raises on non-numeric strings

    op = calc(explode, "id", default=-1, target_type=int)
    rows = [{"id": "not-a-number"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert result[0]["out"] == -1
    assert any("calc failed" in r.getMessage() for r in caplog.records), (
        "real func failure on real data must still warn"
    )


def test_calc_real_data_passes_through_unchanged(caplog: pytest.LogCaptureFixture) -> None:
    """Real-data rows still produce the func's result; the pre-check does not interfere."""
    op = calc(str.lower, "title", default="", target_type=str)
    rows = [{"title": "Hello"}, {"title": None}, {"title": "WORLD"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert result[0]["out"] == "hello"
    assert result[1]["out"] == ""  # None → default
    assert result[2]["out"] == "world"
    assert not [r for r in caplog.records if "calc failed" in r.getMessage()]


def test_calc_none_return_on_real_input_is_stored_as_is(caplog: pytest.LogCaptureFixture) -> None:
    """D2-06: a genuine ``None`` return from ``func`` on real input is NOT replaced by ``default``.

    Pins the chosen (behavior-preserving) contract: ``default`` only fires
    on all-garbage input or a func raise — never on a real ``None`` return.
    The docstring previously over-promised "raises or returns None"; this
    test locks in the actual dispatcher behavior at builder.py Pass 2.
    """

    def returns_none(x: Any) -> None:
        return None

    op = calc(returns_none, "id", default="sentinel")
    rows = [{"id": "real-value"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert result[0]["out"] is None, "a genuine None return on real input must be stored as-is, not defaulted"
    assert not [r for r in caplog.records if "calc failed" in r.getMessage()]


def test_calc_all_none_element_in_same_length_list_is_stored_as_is(caplog: pytest.LogCaptureFixture) -> None:
    """D2-06 calc_all twin: a ``None`` element inside a same-length returned list is stored as-is.

    ``default`` only fires on all-garbage input, a func raise, or a
    genuinely short returned list — not on a ``None`` element within a
    correctly-sized list.
    """

    def one_none(ids: list[Any]) -> list[Any]:
        return [None if i == v else v for i, v in enumerate(ids)]

    op = calc_all(one_none, "id", default="sentinel")
    rows = [{"id": "a"}, {"id": 1}, {"id": "c"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert result[0]["out"] == "a"
    assert result[1]["out"] is None, "a None element in a same-length list must be stored as-is, not defaulted"
    assert result[2]["out"] == "c"
    assert not [r for r in caplog.records if "calc_all failed" in r.getMessage()]


def test_calc_all_short_circuits_when_every_cell_garbage(caplog: pytest.LogCaptureFixture) -> None:
    """All-garbage across every column → silent per-row default; no warning."""

    def should_not_run(scores: list) -> list:
        raise AssertionError("calc_all func must not run when every cell is garbage")

    op = calc_all(should_not_run, "score", default=0, target_type=int)
    rows = [{"score": None}, {"score": ""}, {"score": "n/a"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert [r["out"] for r in result] == [0, 0, 0]
    assert not [r for r in caplog.records if "calc_all failed" in r.getMessage()]


def test_calc_garbage_default_with_incompatible_target_type(caplog: pytest.LogCaptureFixture) -> None:
    """D2-01 (resolved: symmetric, no code change) — calc's all-garbage short-circuit.

    Pins the resolved-correct behaviour: when every input is garbage, calc
    short-circuits to ``default`` silently at the func-invocation level (no
    "calc failed" warning — the func is never called). But target_type
    coercion is still attempted on that default per calc()'s documented
    contract ("target_type: Optional type the result is coerced to" makes no
    func-vs-default distinction). Here default=None and target_type=int, so
    int(None) raises TypeError inside the coercion try/except; the coercion
    branch catches it, re-falls-back to default (None again), and correctly
    emits the "calc type coercion failed" WARNING. This is a KEEP, not a bug:
    the warning is the only signal that the user's declared default is
    incompatible with target_type — see D2-01 resolution (original review's
    claimed CalcOp/CalcAllOp asymmetry did not match the code at HEAD).
    """
    op = calc(str.lower, "title", default=None, target_type=int)
    rows = [{"title": None}, {"title": ""}, {"title": "n/a"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert all(r["out"] is None for r in result), "coercion failure on default must fall back to default"
    assert not [r for r in caplog.records if "calc failed" in r.getMessage()], (
        "garbage rows must not trigger the func-invocation warning"
    )
    assert [r for r in caplog.records if "calc type coercion failed" in r.getMessage()], (
        "an incompatible default must still trigger the coercion-failure warning"
    )


def test_calc_all_garbage_default_with_incompatible_target_type(caplog: pytest.LogCaptureFixture) -> None:
    """D2-01 calc_all twin — same resolved-correct, symmetric behaviour as calc().

    All cells garbage → calc_all short-circuits to [default] * len(rows)
    silently (no "calc_all failed" warning). target_type=int is still
    applied to each row's default value; int(None) raises, the coercion
    branch falls back to default again, and the "calc_all type coercion
    failed" warning fires per row. Symmetric with CalcOp — see D2-01
    resolution (KEEP, not a bug).
    """

    def should_not_run(titles: list) -> list:
        raise AssertionError("calc_all func must not run when every cell is garbage")

    op = calc_all(should_not_run, "title", default=None, target_type=int)
    rows = [{"title": None}, {"title": ""}, {"title": "n/a"}]
    with caplog.at_level("WARNING", logger="incorporator.schema.builder"):
        result = _run_calc(op, rows)
    assert all(r["out"] is None for r in result), "coercion failure on default must fall back to default"
    assert not [r for r in caplog.records if "calc_all failed" in r.getMessage()], (
        "garbage rows must not trigger the func-invocation warning"
    )
    assert [r for r in caplog.records if "calc_all type coercion failed" in r.getMessage()], (
        "an incompatible default must still trigger the coercion-failure warning"
    )


# ---------------------------------------------------------------------------
# Bundle G: dotted-path input keys for calc, calc_all, and inc_code binding.
# ---------------------------------------------------------------------------


def _double_each(values: list[float]) -> list[float]:
    """Return each value doubled — calc_all helper for test_calc_all_dotted_input_keys."""
    return [v * 2 for v in values]


def test_calc_dotted_input_key() -> None:
    """calc reads from a nested sub-dict when the input key contains dot-notation."""
    rows = [{"team": {"name": "cubs"}}]
    apply_etl_transformations(
        rows,
        conv_dict={"team_name": calc(str.upper, "team.name", default="", target_type=str)},
    )
    assert rows[0]["team_name"] == "CUBS"


def test_calc_all_dotted_input_keys() -> None:
    """calc_all reads from nested sub-dicts when input keys contain dot-notation."""
    rows = [{"team": {"score": 3}}, {"team": {"score": 5}}]
    apply_etl_transformations(
        rows,
        conv_dict={"score_doubled": calc_all(_double_each, "team.score", default=0)},
    )
    assert rows[0]["score_doubled"] == 6
    assert rows[1]["score_doubled"] == 10


def test_inc_code_dotted_attr_binds_pk() -> None:
    """inc_code with a dotted path drills into nested dicts to produce the PK value."""
    rows = [{"team": {"id": "cub1"}}]
    apply_etl_transformations(rows, code_attr="team.id")
    assert rows[0]["inc_code"] == "cub1"


# ---------------------------------------------------------------------------
# Adaptive lru_cache wrapping via pure=True on calc() / inc() ops.
# ---------------------------------------------------------------------------


def test_calc_pure_true_returns_correct_values() -> None:
    """calc(pure=True) with low-cardinality inputs still produces correct results.

    Pins that lru_cache wrapping at construction (is_pure=True) does not corrupt
    output values.  Uses a category column with two distinct values repeated across
    20 rows; lru_cache is unconditional for pure=True so only 2 unique calls reach
    the function body.
    """
    call_count = 0

    def tag_upper(v: str) -> str:
        nonlocal call_count
        call_count += 1
        return v.upper()

    op = calc(tag_upper, "cat", default="", target_type=str, pure=True)
    rows = [{"cat": "alpha" if i % 2 == 0 else "beta"} for i in range(20)]
    apply_etl_transformations(rows, conv_dict={"out": op})

    assert all(r["out"] in ("ALPHA", "BETA") for r in rows), "cache must not corrupt output"
    # lru_cache at construction means the function body is only called once per unique input.
    assert call_count <= 2, f"pure=True should cache repeated inputs; got {call_count} calls"


def test_inc_low_cardinality_column_cached_and_correct() -> None:
    """inc(int) on a low-cardinality column is lru_cache-wrapped at construction and returns correct values.

    Op.is_pure is True by construction for inc(), so _func is wrapped with
    lru_cache(maxsize=10_000) when the Op is created.  This test pins that the
    cached path still converts values correctly and does not raise on repeated
    identical inputs.
    """
    op = inc(int)
    rows = [{"n": "1" if i % 3 == 0 else "2"} for i in range(30)]
    apply_etl_transformations(rows, conv_dict={"n": op})

    assert all(r["n"] in (1, 2) for r in rows), "cached inc(int) must still produce correct int output"


def test_calc_pure_true_is_default() -> None:
    """calc without pure= uses pure=True by default and still produces correct results."""
    op = calc(str.lower, "label", default="", target_type=str)
    assert op.is_pure is True

    rows = [{"label": "FOO"}, {"label": "BAR"}, {"label": None}]
    apply_etl_transformations(rows, conv_dict={"out": op})
    assert rows[0]["out"] == "foo"
    assert rows[1]["out"] == "bar"
    assert rows[2]["out"] == ""


# ---------------------------------------------------------------------------
# Op direct-construction quirks (D7-06 / D7-09) — is_pure=False re-invocation,
# unhashable-arg __wrapped__ fallback, and the double-invocation quirk when
# the user body itself raises TypeError on a hashable arg.
# ---------------------------------------------------------------------------


def test_op_is_pure_false_runs_body_every_call() -> None:
    """Op(..., is_pure=False) is never lru_cache-wrapped — the body runs every call."""
    from incorporator.schema.converters import Op

    calls: list[int] = []

    def body(v: int) -> int:
        calls.append(v)
        return v * 2

    op = Op(body, is_pure=False)
    for _ in range(3):
        assert op(5) == 10
    assert calls == [5, 5, 5], "is_pure=False must invoke the body every call, no memoization"


def test_op_unhashable_arg_routes_through_wrapped_fallback() -> None:
    """Op(..., is_pure=True) called with an unhashable arg falls back to __wrapped__, no raise.

    lru_cache raises TypeError trying to hash a list key; Op.__call__'s
    except-TypeError branch recovers by calling self._func.__wrapped__
    (the un-cached original) directly — same as link_to_list/as_list
    receiving a list argument.
    """
    from incorporator.schema.converters import Op

    calls: list[list[int]] = []

    def body(v: list[int]) -> list[int]:
        calls.append(v)
        return list(v)

    op = Op(body, is_pure=True)
    payload = [1, 2, 3]
    result = op(payload)

    assert result == payload
    assert calls == [payload], "unhashable arg must reach the body exactly once via __wrapped__"


def test_op_user_typeerror_on_hashable_arg_propagates_on_first_call() -> None:
    """A body TypeError on a HASHABLE arg propagates on the first call — no silent re-invoke.

    Op.__call__ now pre-checks ``hash(val)`` before delegating to the
    lru_cache-wrapped ``_func``.  A hashable arg means any TypeError raised
    from that call is the user's own — it must propagate immediately, and
    the body must run exactly once.  Only a genuinely unhashable arg (list,
    dict) routes to ``__wrapped__`` as a fallback (see
    ``test_op_unhashable_arg_routes_through_wrapped_fallback``).
    """
    from incorporator.schema.converters import Op

    calls: list[int] = []

    def body(v: int) -> int:
        calls.append(v)
        raise TypeError(f"boom on call #{len(calls)}")

    op = Op(body, is_pure=True)
    with pytest.raises(TypeError, match=r"boom on call #1"):
        op(5)

    assert calls == [5], "hashable-arg TypeError must propagate on the first call, body invoked exactly once"
