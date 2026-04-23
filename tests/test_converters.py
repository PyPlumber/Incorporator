"""Unit tests for built-in functional converters."""

import pytest
from datetime import datetime
from incorporator import (
    to_bool,
    to_date,
    to_int,
    to_float,
    split_and_get,
    cast_list_items,
    default_if_null
)


def test_to_bool_null_safety() -> None:
    """Asserts string logic and empty value fallback."""
    assert to_bool("true") is True
    assert to_bool("Y") is True
    assert to_bool("1") is True
    assert to_bool("false") is False
    assert to_bool("junk") is False
    assert to_bool(None) is False
    assert to_bool("") is False


def test_to_date_null_safety() -> None:
    """Asserts ISO-8601 parsing and Z-Zulu timezone correction."""
    dt = to_date("2026-04-21T23:59:59Z")
    assert isinstance(dt, datetime)
    assert dt.year == 2026

    assert to_date(None) is None
    assert to_date("") is None

    with pytest.raises(ValueError):
        to_date("not-a-valid-date")


def test_to_int_and_float_null_safety() -> None:
    """Asserts robust string-to-number casting."""
    assert to_int("10.5") == 10  # float-string to int works
    assert to_int(None) is None
    assert to_int("junk") is None

    assert to_float("1500.50") == 1500.5
    assert to_float(None) is None
    assert to_float("junk") is None


def test_split_and_get_null_safety() -> None:
    """Asserts URL splitting handles trailing slashes and nulls."""
    extractor = split_and_get(delimiter='/', index=-1)

    assert extractor("https://api.com/user/101") == "101"
    # Testing the trailing slash defense
    assert extractor("https://api.com/user/101/") == "101"

    assert extractor(None) is None
    assert extractor("") is None


def test_cast_list_items_null_safety() -> None:
    """Asserts list casting strips Nulls and handles singular elements."""
    caster = cast_list_items(int)

    assert caster(["1", "2", None, "4", ""]) == [1, 2, 4]
    assert caster("10") == [10]  # Graceful fallback if passed a string instead of a list
    assert caster(None) == []


def test_default_if_null() -> None:
    """Asserts substitution of default values for None or empty strings."""
    defaulter = default_if_null("N/A")

    assert defaulter("Valid") == "Valid"
    assert defaulter(None) == "N/A"
    assert defaulter("") == "N/A"