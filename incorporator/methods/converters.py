"""Built-in data converters and lambda wrappers for Incorporator.

These functions abstract away messy lambda syntax and are 100% "Null-Safe".
They gracefully handle None or empty strings to prevent ETL pipeline crashes.
Designed to be passed into the 'conv_dict' parameter during Dynamic Class Building.
"""

from datetime import datetime
from typing import Any, Callable, List, Optional


# ==========================================
# DIRECT CASTERS (Usage in conv_dict: {'key': to_bool})
# ==========================================
def to_bool(value: Any) -> bool:
    """Safely converts strings ('true', '1', 'yes') to booleans. Returns False if empty."""
    if isinstance(value, bool):
        return value
    if not value:
        return False

    truthy_values = {'true', '1', 'yes', 'y', 't', 'on'}
    return str(value).strip().lower() in truthy_values


def to_date(value: Any) -> Optional[datetime]:
    """Parses ISO-8601 strings into datetime objects. Returns None if empty."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value

    try:
        safe_str = str(value).strip().replace('Z', '+00:00')
        return datetime.fromisoformat(safe_str)
    except ValueError as e:
        raise ValueError(f"Could not parse '{value}' into a datetime object: {e}")


def to_int(value: Any) -> Optional[int]:
    """Safely converts strings/floats to integers. Returns None if empty."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))  # float() first in case it's a string like "10.0"
    except (ValueError, TypeError):
        return None


def to_float(value: Any) -> Optional[float]:
    """Safely converts strings to floats. Returns None if empty."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# ==========================================
# WRAPPERS (Usage in conv_dict: {'key': split_and_get('/')})
# ==========================================

def split_and_get(delimiter: str = '/', index: int = -1) -> Callable[[Any], Optional[str]]:
    """Splits a string and grabs a specific index. Returns None if empty."""

    def _splitter(value: Any) -> Optional[str]:
        if not value:
            return None
        try:
            return str(value).strip(delimiter).split(delimiter)[index]
        except IndexError:
            return None

    return _splitter


def cast_list_items(cast_type: Callable[[Any], Any]) -> Callable[[Any], List[Any]]:
    """Casts every item in a list to a specific type, safely dropping Nulls."""

    def _caster(lst: Any) -> List[Any]:
        if not lst:
            return[]
        if not isinstance(lst, list):
            # Gracefully handle a single item that should have been a list
            return [cast_type(lst)]

        # Comprehension ignores None items to prevent casting errors
        return[cast_type(item) for item in lst if item is not None and item != ""]

    return _caster


def default_if_null(default_value: Any) -> Callable[[Any], Any]:
    """Substitutes a default value if the input is None or an empty string."""

    def _defaulter(value: Any) -> Any:
        return default_value if value is None or value == "" else value

    return _defaulter


def link_to(dataset: Any) -> Callable[[Any], Any]:
    """Generates a null-safe relational mapper for conv_dict.
    Safely connects foreign keys to an IncorporatorList's codeDict.
    """
    registry = getattr(dataset, "codeDict", {})

    def _mapper(key: Any) -> Any:
        if not key:
            return None
        if key in registry:
            return registry[key]
        try:
            return registry.get(int(key))
        except (ValueError, TypeError):
            return None

    return _mapper