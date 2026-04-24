"""Unit tests for built-in functional converters and URL tools."""

import json
from datetime import datetime
from types import SimpleNamespace
import pytest

from incorporator import (
    to_bool,
    to_date,
    to_int,
    to_float,
    split_and_get,
    cast_list_items,
    default_if_null,
    link_to,
    link_to_list,
    extract_url_id,
    pluck,
    json_path_extractor
)


def test_to_bool_null_safety() -> None:
    """Asserts string logic and empty value fallback."""
    assert to_bool("true") is True
    assert to_bool("1") is True
    assert to_bool("false") is False
    assert to_bool("junk") is False
    assert to_bool(None) is False


def test_to_date_universal_parser() -> None:
    """Asserts ISO-8601 parsing and universal string format fallbacks."""
    # Standard ISO
    dt_iso = to_date("2026-04-21T23:59:59Z")
    assert isinstance(dt_iso, datetime) and dt_iso.year == 2026

    # Custom Rick & Morty format fallback
    dt_rm = to_date("December 2, 2013")
    assert isinstance(dt_rm, datetime) and dt_rm.year == 2013 and dt_rm.month == 12

    # SQL Timestamp format fallback
    dt_sql = to_date("2026-04-22 23:59:59")
    assert isinstance(dt_sql, datetime) and dt_sql.year == 2026

    assert to_date(None) is None

    with pytest.raises(ValueError):
        to_date("not-a-valid-date-format")


def test_to_int_and_float_dirty_data_and_math() -> None:
    """Asserts robust string-to-number casting, dirty data cleaning, and math factories."""

    # 1. Direct Execution & Dirty Data Cleaning
    assert to_int("1,500") == 1500  # Strips commas
    assert to_float("1,500.50") == 1500.5  # Strips commas
    assert to_int("unknown") is None  # Traps dirty strings
    assert to_float("N/A") is None  # Traps dirty strings
    assert to_int(None) is None

    # 2. Factory Mode with Math Strings!
    # Fahrenheit conversion: (x * 1.8) + 32
    celsius_to_fahrenheit = to_float(math="(x * 1.8) + 32")
    assert celsius_to_fahrenheit(0) == 32.0
    assert celsius_to_fahrenheit("100") == 212.0

    # Math with defaults for missing data
    discount_calc = to_int(math="round(x * 0.8)", default=0)
    assert discount_calc("100") == 80
    assert discount_calc("unknown") == 0  # Falls back to default safely!


def test_url_toolkit() -> None:
    """Asserts extract_url_id, pluck, and json_path_extractor function correctly."""

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

    # --- json_path_extractor ---
    json_data = json.dumps({"info": {"next": "https://api.com/?page=2"}})
    paginator = json_path_extractor("info", "next")
    assert paginator(json_data) == "https://api.com/?page=2"
    assert paginator(json.dumps({"info": {"next": None}})) is None


def test_link_to_relational_mapping() -> None:
    """Asserts relational mapping handles plain lists, WeakValueDictionaries, and nulls."""

    # Use SimpleNamespace to mock the object-oriented structure of Pydantic models
    # UPDATED: Use inc_code to match the newly refactored base API contract.
    mock_obj_1 = SimpleNamespace(inc_code=1, name="Daytona")
    mock_obj_2 = SimpleNamespace(inc_code="A100", name="Talladega")

    # Test 1: Passing a plain Python list (link_to should build the dict dynamically)
    plain_list =[mock_obj_1, mock_obj_2]
    mapper = link_to(plain_list)

    assert mapper(1) == mock_obj_1
    assert mapper("1") == mock_obj_1  # String-to-int cast fallback
    assert mapper("A100") == mock_obj_2  # Direct string match
    assert mapper(None) is None
    assert mapper("bad_id") is None

    # Test 2: Using an extractor
    extractor_mapper = link_to(plain_list, extractor=lambda x: int(x) * 1)
    assert extractor_mapper("1") == mock_obj_1