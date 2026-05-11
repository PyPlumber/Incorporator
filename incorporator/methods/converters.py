"""
Built-in Type-Ranked Conversion Engine for Incorporator.

Provides the `inc()`, `calc()`, and `calc_all()` syntax for Attribute-Based processing.
Includes a Ranked Dictionary of fallbacks to guarantee 100% "Null-Safe" ETL pipelines.
"""

import collections.abc
import functools
import logging
import weakref
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pydantic import TypeAdapter

from .format_utils import FormatType, to_python_type

logger = logging.getLogger(__name__)

# ==========================================
# 1. DX SENTINELS & ALIASES
# ==========================================
flt = float


class _NewSentinel:
    """Explicit marker to indicate an attribute must be generated from scratch."""

    pass


new = _NewSentinel()


class _EachSentinel:
    """Marker to distribute extracted list items across concurrent POST requests."""

    pass


# ==========================================
# COMMON CALC() FUNCTIONS (Built-ins)
# ==========================================
def sum_attributes(*args: Any) -> float:
    """Example built-in calc function: Safely sums multiple attributes with zero string-allocation overhead."""
    total = 0.0
    for x in args:
        if x is not None:
            try:
                # C-level speed cast. Faster and lighter than string manipulation.
                total += float(x)
            except (ValueError, TypeError):
                pass
    return total


def split_and_get(
    delimiter: str = "/", index: int = -1, cast_type: Optional[Callable[[Any], Any]] = None
) -> Callable[[Any], Any]:
    def _splitter(value: Any) -> Any:
        if value is None or value == "":
            return None
        try:
            result = str(value).strip(delimiter).split(delimiter)[index]
            return cast_type(result) if cast_type else result
        except (IndexError, ValueError, TypeError):
            return None

    return _splitter


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


# ==========================================
# 5. GRAPH & EXTRACTORS
# ==========================================


def link_to(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """Maps relational data using a memory-safe hybrid internal cache."""

    # 1. Primary Cache: OOM-Safe for production Incorporator/Pydantic objects
    registry: "weakref.WeakValueDictionary[Any, Any]" = weakref.WeakValueDictionary()

    # 2. Fallback Cache: Strong references for tests (SimpleNamespace) or non-weakref classes
    fallback_registry: Dict[Any, Any] = {}

    def _add_to_cache(k: Any, v: Any) -> None:
        try:
            # Attempt to set the weakref
            registry[k] = v
            registry[str(k)] = v  # Shadow string map
        except TypeError:
            # Alert the user if they're bypassing the weakref safety net
            if not fallback_registry:
                logger.debug(
                    "link_to: Using strong-ref fallback cache. "
                    "Warning: Passing vast arrays of non-weakrefable objects (e.g. built-in dicts) may cause memory blowouts."  # noqa: E501
                )
            fallback_registry[k] = v
            fallback_registry[str(k)] = v

    # Build the cache
    if isinstance(dataset, list):
        for item in dataset:
            code = getattr(item, "inc_code", None)
            if code is not None:
                _add_to_cache(code, item)
    else:
        # Failsafe for single objects or IncorporatorLists that already have inc_dict
        reg = getattr(dataset, "inc_dict", {})
        if isinstance(reg, collections.abc.Mapping):
            for k, v in reg.items():
                _add_to_cache(k, v)

    def _mapper(val: Any) -> Any:
        key = extractor(val) if extractor is not None else val
        if key is None:
            return None

        # O(1) Instant Lookup (Check Weak Registry first, then Fallback)
        if key in registry:
            return registry[key]
        if key in fallback_registry:
            return fallback_registry[key]

        # Ultimate Type-Splinter defense (Strings)
        str_key = str(key)
        if str_key in registry:
            return registry[str_key]
        if str_key in fallback_registry:
            return fallback_registry[str_key]

        return None

    return _mapper


def link_to_list(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], List[Any]]:
    """Automatically maps a list of foreign keys to their corresponding objects."""
    base_linker = link_to(dataset, extractor)

    def _mapper(val_list: Any) -> List[Any]:
        if not isinstance(val_list, list):
            return []
        return [obj for v in val_list if (obj := base_linker(v)) is not None]

    return _mapper


def pluck(key: str, chain: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """
    Extracts nested dictionary values via dot-notation drilling.
    Supports chaining an additional converter logic (e.g., pluck('a.b', chain=int)).
    """
    parts = key.split(".")

    def _plucker(val: Any) -> Any:
        extracted = val

        if isinstance(val, dict):
            for part in parts:
                if not isinstance(extracted, dict):
                    extracted = None
                    break

                extracted = extracted.get(part)
                if extracted is None:
                    break

        return chain(extracted) if chain else extracted

    return _plucker


# ==========================================
# DECLARATIVE PAYLOAD TOKENS (POST/PUT)
# ==========================================


def each() -> _EachSentinel:
    """
    POST Token: Triggers N Concurrent Iterative Requests.
    Maps an extracted list of parent IDs row-by-row into the payload dictionary.
    """
    return _EachSentinel()


def join_all(delimiter: str = ",") -> Callable[[Any], str]:
    """
    POST Token: Triggers 1 Bulk Batch Request.
    Takes a list of extracted parent IDs and joins them into a single delimited string.
    """

    def _joiner(data: Any) -> str:
        if not isinstance(data, list):
            return str(data)
        return delimiter.join(str(x) for x in data if x is not None)

    return _joiner


def as_list() -> Callable[[Any], List[Any]]:
    """
    POST Token: Triggers 1 Bulk Batch Request.
    Injects the raw extracted list of parent IDs directly into a JSON Array.
    """
    return lambda data: data if isinstance(data, list) else [data]


# ==========================================
# FORMAT-AWARE VALUE COERCION
# ==========================================


def coerce_avro_value(val: Any, avro_type: str) -> Any:
    """Coerce a value to the Python type expected by the given Avro type string.

    Uses the FORMAT_TO_PYTHON type bridge and the ranked inc() converter
    so coercion failures degrade gracefully to None rather than crashing.
    """
    if val is None:
        return None
    python_type = to_python_type(FormatType.AVRO, avro_type)
    if isinstance(val, python_type):
        return val
    return inc(python_type, default=None)(val)
