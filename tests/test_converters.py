"""Unit tests for the Incorporator Columnar Type Engine and URL tools."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from incorporator.schema import router
from incorporator.schema.converters import (
    CalcAllOp,
    CalcOp,
    GARBAGE_VALUES,
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
    inc.cache_clear()  # type: ignore[attr-defined]

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
    assert c_op.input_list == ["mass", "gravity"]

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

    # Test flat string fallback (e.g. passing a raw string through the converter)
    assert plucker("https://api.com/planet/5/") == 5


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
