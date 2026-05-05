"""Unit tests for the Incorporator Columnar Type Engine and URL tools."""

import pytest
from typing import Any
from datetime import datetime
from types import SimpleNamespace

from incorporator.methods.converters import (
    CalcAllOp,
    CalcOp,
    calc,
    calc_all,
    flt,
    inc,
    link_to,
    new,
    pluck,
    split_and_get,
    each,
    join_all
)
from incorporator.base import Incorporator




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
    inc_flt = inc(flt)  # Utilizing the new flt alias

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


def test_calc_and_calc_all_markers() -> None:
    """Asserts that calc and calc_all correctly generate Columnar Op markers."""

    def dummy_math(x: float, y: float) -> float:
        return x + y

    # 1. Row-based calc
    c_op = calc(dummy_math, "mass", "gravity", default=0, target_type=flt)
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
    extractor = split_and_get(delimiter='/', index=-1, cast_type=int)

    assert extractor("https://api.com/user/101") == 101
    assert extractor("https://api.com/user/101/") == 101
    assert extractor(None) is None

    # --- pluck ---
    plucker = pluck("homeworld", chain=split_and_get(delimiter='/', index=-1, cast_type=int))

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
    parents =[MockObj(vehicle=MockObj(vin="123")), MockObj(vehicle=MockObj(vin="456"))]
    assert Incorporator._extract_parent_data(parents, "vehicle.vin") == ["123", "456"]

    # 2. Schema Splintering (List nested inside the drill path)
    splintered_parents =[MockObj(vehicle=[MockObj(vin="A"), MockObj(vin="B")])]
    assert Incorporator._extract_parent_data(splintered_parents, "vehicle.vin") ==["A", "B"]

    # 3. Dictionary handling and Missing Keys (Graceful degradation)
    mixed_parents =[MockObj(vehicle={"vin": "DictVIN"}), MockObj(vehicle=None)]
    assert Incorporator._extract_parent_data(mixed_parents, "vehicle.vin") == ["DictVIN"]

    # 4. Deeply nested missing data
    assert Incorporator._extract_parent_data(parents, "vehicle.engine.cylinders") ==[]


def test_declarative_post_routing() -> None:
    """Verifies that POST tokens correctly map payloads and URLs."""
    extracted_ids = [101, 102]
    source_urls = ["https://api.com/update"]

    # 1. Test the `each()` token (N Concurrent Requests)
    kwargs_each = Incorporator._resolve_declarative_routing(
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="POST",  # 🛡️ THE FIX: Use canonical internal kwargs
        json_payload={"id": each(), "static": "token"}
    )

    # Should multiply the URL by 2, and create 2 distinct payloads
    assert kwargs_each["inc_url"] == ["https://api.com/update", "https://api.com/update"]
    assert kwargs_each["payload_list"] == [{"id": 101, "static": "token"}, {"id": 102, "static": "token"}]

    # 2. Test the `join_all()` token (1 Bulk Request)
    kwargs_join = Incorporator._resolve_declarative_routing(
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="POST",  # 🛡️ THE FIX
        json_payload={"ids": join_all(",")}
    )

    # Should keep 1 URL, and create 1 payload with a joined string
    assert kwargs_join["payload_list"] == [{"ids": "101,102"}]
    assert len(kwargs_join["payload_list"]) == len(source_urls)

    # 3. Test Missing POST URL Exception
    with pytest.raises(ValueError, match="Missing Target URL"):
        Incorporator._resolve_declarative_routing(
            extracted_data=extracted_ids,
            source_urls=[],
            http_method="POST",  # 🛡️ THE FIX
            json_payload={"ids": join_all(",")}
        )


def test_get_url_injection() -> None:
    """Verifies that GET requests natively inject IDs into {} templates."""
    extracted_ids = ["alpha", "beta"]
    source_urls = ["https://api.com/users/{}/profile"]

    kwargs = Incorporator._resolve_declarative_routing(
        extracted_data=extracted_ids,
        source_urls=source_urls,
        http_method="GET"  # 🛡️ THE FIX
    )

    # It should generate N unique URLs
    assert kwargs["inc_url"] == [
        "https://api.com/users/alpha/profile",
        "https://api.com/users/beta/profile"
    ]