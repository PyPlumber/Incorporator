"""
Built-in Type-Ranked Conversion Engine for Incorporator.

Provides the `inc()`, `calc()`, and `calc_all()` syntax for Attribute-Based processing.
Includes a Ranked Dictionary of fallbacks to guarantee 100% "Null-Safe" ETL pipelines.
"""

import collections.abc
import logging
from datetime import datetime
from typing import (
    Any, Callable, Dict, List, Optional, TypeVar
)

from pydantic import TypeAdapter

logger = logging.getLogger(__name__)

T = TypeVar('T')

# ==========================================
# 1. DX SENTINELS & ALIASES
# ==========================================
flt = float


class _NewSentinel:
    """Explicit marker to indicate an attribute must be generated from scratch."""
    pass


new = _NewSentinel()


# ==========================================
# COMMON CALC() FUNCTIONS (Built-ins)
# ==========================================
def sum_attributes(*args: Any) -> float:
    """Example built-in calc function: Safely sums multiple attributes."""
    return sum(float(x) for x in args if x is not None and str(x).replace('.','',1).isdigit())

def split_and_get(delimiter: str = '/', index: int = -1, cast_type: Optional[Callable[[Any], Any]] = None) -> Callable[
    [Any], Any]:
    def _splitter(value: Any) -> Any:
        if not value: return None
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
    __slots__ = ('func', 'default', 'target_type', 'input_list')

    def __init__(self, func: Callable[..., Any], default: Any, target_type: Any, input_list: List[str]):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = input_list


class CalcAllOp:
    """Marker indicating an operation that requires full array processing down the column."""
    __slots__ = ('func', 'default', 'target_type', 'input_list')

    def __init__(self, func: Callable[..., Any], default: Any, target_type: Any, input_list: List[str]):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = input_list


def calc(
        func: Callable[..., Any],
        marker: Any = None,
        *,
        default: Any = None,
        type: Any = None,
        input_list: Optional[List[str]] = None
) -> CalcOp:
    """Creates a multi-input row calculation."""
    return CalcOp(func, default, type, input_list or [])


def calc_all(
        func: Callable[..., Any],
        marker: Any = None,
        *,
        default: Any = None,
        type: Any = None,
        input_list: Optional[List[str]] = None
) -> CalcAllOp:
    """Creates a batch/array calculation down an entire column."""
    return CalcAllOp(func, default, type, input_list or [])


# ==========================================
# 3. RANKED FALLBACK STRATEGIES
# ==========================================
def _fallback_bool(value: Any) -> bool:
    if not value:
        return False
    truthy_values = {'true', '1', 'yes', 'y', 't', 'on'}
    return str(value).strip().lower() in truthy_values


def _fallback_date(value: Any) -> datetime:
    safe_str = str(value).strip().replace('Z', '+00:00')
    fallback_formats = [
        "%B %d, %Y", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y",
        "%Y/%m/%d", "%d %b %Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S.%f",
        "%a, %d %b %Y %H:%M:%S %Z",
    ]
    for fmt in fallback_formats:
        try:
            return datetime.strptime(safe_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"All date fallbacks failed for '{value}'.")


def _fallback_int(value: Any) -> int:
    clean_val = str(value).strip().lower() if isinstance(value, str) else value
    if isinstance(clean_val, str):
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            raise ValueError("Null-equivalent string encountered.")
        clean_val = clean_val.replace(",", "")
    return int(float(clean_val))


def _fallback_float(value: Any) -> float:
    clean_val = str(value).strip().lower() if isinstance(value, str) else value
    if isinstance(clean_val, str):
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            raise ValueError("Null-equivalent string encountered.")
        clean_val = clean_val.replace(",", "")
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

def inc(target_type: Any) -> Callable[[Any], Any]:
    """
    Returns a Context-Aware, Type-Ranked validation closure.
    """
    # 1. The 'new' mapping: If 'new', accept ANY valid Python type.
    actual_type = Any if (target_type is new or isinstance(target_type, _NewSentinel)) else target_type

    # 2. Instantiate the adapter EXACTLY ONCE
    try:
        adapter: Optional[TypeAdapter[Any]] = TypeAdapter(actual_type)
    except Exception:
        adapter = None

    ranks = RANKED_CONVERTERS.get(actual_type, [])
    if adapter:
        ranks = [adapter.validate_python] + [r for r in ranks if r != adapter.validate_python]

    if not ranks:
        ranks = [lambda x: x]  # Failsafe pass-through

    def _ranked_converter(val: Any) -> Any:
        if val is None or val == "":
            return None

        last_error = None
        for func in ranks:
            try:
                return func(val)
            except Exception as e:
                last_error = e
                continue

        logger.warning(
            f"Incorporator Type Engine: Failed to convert '{val}' into {actual_type}. "
            f"Last error: {last_error}"
        )
        return None

    return _ranked_converter


# ==========================================
# 5. GRAPH & EXTRACTORS
# ==========================================

def link_to(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """Maps relational data using a rock-solid internal dictionary cache."""

    # 1. Build a hyper-resilient strong dictionary using the physical list items
    registry: Dict[Any, Any] = {}

    if isinstance(dataset, list):
        for item in dataset:
            # Grab the ID directly off the living object!
            code = getattr(item, 'inc_code', None)
            if code is not None:
                registry[code] = item
                registry[str(code)] = item  # Shadow string map to prevent Type Splintering!
    else:
        # Failsafe for single objects
        reg = getattr(dataset, "inc_dict", {})
        if isinstance(reg, collections.abc.Mapping):
            registry = {**reg, **{str(k): v for k, v in reg.items()}}

    def _mapper(val: Any) -> Any:
        key = extractor(val) if extractor is not None else val
        if key is None:
            return None

        # O(1) Instant Lookup using our guaranteed strong registry
        if key in registry:
            return registry[key]

        # Ultimate Type-Splinter defense
        if str(key) in registry:
            return registry[str(key)]

        return None

    return _mapper


def link_to_list(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], List[Any]]:
    base_linker = link_to(dataset, extractor)

    def _mapper(val_list: Any) -> List[Any]:
        if not isinstance(val_list, list): return []
        return [obj for v in val_list if (obj := base_linker(v)) is not None]

    return _mapper


def extract_url_id(cast_type: Callable[[Any], Any] = int) -> Callable[[Any], Any]:
    def _extractor(url_str: Any) -> Any:
        if not isinstance(url_str, str) or not url_str: return None
        try:
            return cast_type(url_str.strip('/').split('/')[-1])
        except (ValueError, TypeError, IndexError):
            return None

    return _extractor


def pluck(key: str, chain: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    def _plucker(val: Any) -> Any:
        extracted = val.get(key) if isinstance(val, dict) else val
        return chain(extracted) if chain else extracted

    return _plucker