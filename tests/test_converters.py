"""Unit tests for the Incorporator Columnar Type Engine and URL tools."""

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from incorporator.methods.converters import (
    CalcAllOp,
    CalcOp,
    calc,
    calc_all,
    extract_url_id,
    flt,
    inc,
    link_to,
    new,
    pluck,
    split_and_get
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
    c_op = calc(dummy_math, default=0, type=flt, input_list=["mass", "gravity"])
    assert isinstance(c_op, CalcOp)
    assert c_op.default == 0
    assert c_op.target_type is float
    assert c_op.input_list == ["mass", "gravity"]

    # 2. Batch column calc_all
    ca_op = calc_all(dummy_math, type=int)
    assert isinstance(ca_op, CalcAllOp)
    assert ca_op.target_type is int
    assert ca_op.input_list == []  # Defaults to empty list intelligently


def test_url_toolkit() -> None:
    """Asserts extract_url_id and pluck function correctly."""

    # --- extract_url_id ---
    extractor = extract_url_id(int)
    assert extractor("https://api.com/user/101") == 101
    assert extractor("https://api.com/user/101/") == 101  # Defends against trailing slashes
    assert extractor(None) is None

    # --- pluck ---
    plucker = pluck("url", chain=extract_url_id(int))
    # Test dictionary pluck
    assert plucker({"name": "Earth", "url": "https://api.com/planet/5/"}) == 5
    # Test flat string fallback
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