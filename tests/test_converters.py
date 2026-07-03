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
