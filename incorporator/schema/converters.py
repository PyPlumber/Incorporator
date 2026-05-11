"""
Built-in Type-Ranked Conversion Engine for Incorporator.

Provides the `inc()`, `calc()`, and `calc_all()` syntax for Attribute-Based processing.
Includes a Ranked Dictionary of fallbacks to guarantee 100% "Null-Safe" ETL pipelines.
"""

import functools
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pydantic import TypeAdapter

logger = logging.getLogger(__name__)

# ==========================================
# 1. DX SENTINELS
# ==========================================


class _NewSentinel:
    """Explicit marker to indicate an attribute must be generated from scratch."""

    pass


new = _NewSentinel()


class _EachSentinel:
    """Marker to distribute extracted list items across concurrent POST requests."""

    pass


# ==========================================
# 2. COLUMNAR ETL MARKERS (Context-Aware)
# ==========================================
class CalcOp:
    """Marker indicating an operation that requires multiple values from the current row."""

    __slots__ = ("func", "default", "target_type", "input_list")

    def __init__(self, func: Callable[..., Any], default: Any, target_type: Any, input_list: List[str]):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = input_list


class CalcAllOp:
    """Marker indicating an operation that requires full array processing down the column."""

    __slots__ = ("func", "default", "target_type", "input_list")

    def __init__(self, func: Callable[..., Any], default: Any, target_type: Any, input_list: List[str]):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = input_list


def calc(func: Callable[..., Any], *input_keys: str, default: Any = None, target_type: Any = None) -> CalcOp:
    """Creates a multi-input row calculation."""
    return CalcOp(func, default, target_type, list(input_keys))


def calc_all(func: Callable[..., Any], *input_keys: str, default: Any = None, target_type: Any = None) -> CalcAllOp:
    """Creates a batch/array calculation down an entire column."""
    return CalcAllOp(func, default, target_type, list(input_keys))


# ==========================================
# 3. RANKED FALLBACK STRATEGIES
# ==========================================
def _fallback_bool(value: Any) -> bool:
    if not value:
        return False
    truthy_values = {"true", "1", "yes", "y", "t", "on"}
    return str(value).strip().lower() in truthy_values


def _fallback_date(value: Any) -> datetime:
    safe_str = str(value).strip().replace("Z", "+00:00")

    # Fast C-level ISO parsing (Python 3.11+) interceptor
    try:
        return datetime.fromisoformat(safe_str)
    except ValueError:
        pass

    fallback_formats = [
        "%B %d, %Y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%b %d, %Y",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%a, %d %b %Y %H:%M:%S %Z",
    ]
    for fmt in fallback_formats:
        try:
            return datetime.strptime(safe_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"All date fallbacks failed for '{value}'.")


def _fallback_int(value: Any) -> int:
    clean_val = str(value).strip().replace(",", "") if isinstance(value, str) else value
    return int(float(clean_val))


def _fallback_float(value: Any) -> float:
    clean_val = str(value).strip().replace(",", "") if isinstance(value, str) else value
    return float(clean_val)


# The Global Ranked Dictionary Engine
RANKED_CONVERTERS: Dict[Any, List[Callable[[Any], Any]]] = {
    bool: [TypeAdapter(bool).validate_python, _fallback_bool],
    datetime: [TypeAdapter(datetime).validate_python, _fallback_date],
    int: [TypeAdapter(int).validate_python, _fallback_int],
    float: [TypeAdapter(float).validate_python, _fallback_float],
    str: [TypeAdapter(str).validate_python, str],
}


# ==========================================
# THE INC() FACTORY
# ==========================================
@functools.lru_cache(maxsize=128)
def _get_cached_adapter(actual_type: Any) -> Optional[TypeAdapter[Any]]:
    try:
        return TypeAdapter(actual_type)
    except Exception:
        return None


def inc(target_type: Any, default: Any = None) -> Callable[[Any], Any]:
    """
    Returns a Context-Aware, Type-Ranked validation closure.
    Now supports `default` fallbacks for missing data or failed conversions!
    """
    # 1. The 'new' mapping: If 'new', accept ANY valid Python type.
    actual_type = Any if (target_type is new or isinstance(target_type, _NewSentinel)) else target_type

    # 2. Instantiate the adapter EXACTLY ONCE
    adapter = _get_cached_adapter(actual_type) if actual_type is not Any else None

    ranks = RANKED_CONVERTERS.get(actual_type, [])
    if adapter:
        ranks = [adapter.validate_python] + [r for r in ranks if r != adapter.validate_python]

    if not ranks:
        ranks = [lambda x: x]  # Failsafe pass-through

    def _ranked_converter(val: Any) -> Any:
        # Instantly catch None, empties, and known API garbage
        if val is None or val == "":
            return default

        if isinstance(val, str) and val.strip().lower() in {
            "unknown",
            "n/a",
            "none",
            "null",
            "undefined",
            "nan",
        }:
            return default

        last_error = None
        for func in ranks:
            try:
                return func(val)
            except Exception as e:
                last_error = e
                continue

        # Real anomalies (e.g. trying to cast "Apple" to a float) will still throw a helpful warning
        logger.warning(
            f"Incorporator Type Engine: Failed to cast '{val}' into {getattr(actual_type, '__name__', str(actual_type))}. "  # noqa: E501
            f"(Last error: {last_error}). "
            f"Tip: If this is expected dirty data, use `inc(..., default=...)` "
            f"to silence this warning and fallback gracefully."
        )
        return default

    return _ranked_converter
