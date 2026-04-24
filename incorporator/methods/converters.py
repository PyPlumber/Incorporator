"""Built-in data converters and lambda wrappers for Incorporator.

These functions abstract away messy lambda syntax and are 100% "Null-Safe".
They gracefully handle None or empty strings to prevent ETL pipeline crashes.
Designed to be passed into the 'conv_dict' parameter during Dynamic Class Building.
"""

import json
import collections.abc
from datetime import datetime
from typing import Any, Callable, List, Optional, cast


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
    """Parses standard ISO-8601 and various common date strings into datetime objects.
    Returns None if empty.
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value

    safe_str = str(value).strip().replace('Z', '+00:00')

    # 1. Try standard ISO-8601 (Fastest native path)
    try:
        return datetime.fromisoformat(safe_str)
    except ValueError:
        pass

    # 2. Universal Fallback Patterns
    fallback_formats =[
        "%B %d, %Y",                 # Long: December 2, 2013
        "%Y-%m-%d %H:%M:%S",         # SQL Timestamps: 2026-04-22 23:59:59
        "%m/%d/%Y",                  # US Short: 04/22/2026
        "%d/%m/%Y",                  # EU Short: 22/04/2026
        "%Y/%m/%d",                  # Asian Short: 2026/04/22
        "%d %b %Y",                  # 22 Apr 2026
        "%b %d, %Y",                 # Apr 22, 2026
        "%Y-%m-%dT%H:%M:%S.%f",      # ISO with truncated timezone
        "%a, %d %b %Y %H:%M:%S %Z",  # RFC 2822 / HTTP headers: Wed, 22 Apr 2026 23:59:59 GMT
    ]

    for fmt in fallback_formats:
        try:
            return datetime.strptime(safe_str, fmt)
        except ValueError:
            continue

    # If all formats fail, raise the standard error our pipeline expects
    raise ValueError(f"Could not parse '{value}' into a datetime object using any known format.")


def to_int(
        value: Any = "__INCORP_FACTORY__",
        *,
        math: Optional[str] = None,
        default: Optional[int] = None
) -> Any:
    """
    Safely converts strings/floats to integers. Strips commas and handles 'unknown'/'n/a'.
    If invoked without a value, returns a factory function for Declarative ETL.
    Accepts a 'math' string expression where 'x' is the value (e.g., math="(x * 1.8) + 32").
    """
    # [DSA OPTIMIZATION]: Pre-compile the math expression into bytecode to avoid
    # O(N) compilation overhead during the schema building loop.
    compiled_math = compile(math, "<string>", "eval") if math else None

    # 1. FACTORY MODE: Configuring the converter for the conv_dict
    if value == "__INCORP_FACTORY__":
        def _factory(val: Any) -> Optional[int]:
            if val is None or val == "":
                return default

            if isinstance(val, str):
                clean_val = str(val).strip().lower()
                if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
                    return default
                clean_val = clean_val.replace(",", "")
            else:
                clean_val = val

            try:
                result = float(clean_val)
                if compiled_math:
                    # Secure Sandboxed Evaluation using pre-compiled bytecode
                    safe_env = {"x": result, "abs": abs, "round": round, "min": min, "max": max}
                    result = float(eval(compiled_math, {"__builtins__": {}}, safe_env))
                return int(result)
            except Exception:
                # Catches ZeroDivisionError, SyntaxError, or ValueError safely
                return default

        return _factory

    # 2. EXECUTION MODE: Processing the actual data (Direct call)
    if value is None or value == "":
        return default

    if isinstance(value, str):
        clean_val = str(value).strip().lower()
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            return default
        clean_val = clean_val.replace(",", "")
    else:
        clean_val = value

    try:
        result = float(clean_val)
        if compiled_math:
            safe_env = {"x": result, "abs": abs, "round": round, "min": min, "max": max}
            result = float(eval(compiled_math, {"__builtins__": {}}, safe_env))
        return int(result)
    except Exception:
        return default


def to_float(
        value: Any = "__INCORP_FACTORY__",
        *,
        math: Optional[str] = None,
        default: Optional[float] = None
) -> Any:
    """
    Safely converts strings to floats. Strips commas and handles 'unknown'/'n/a'.
    If invoked without a value, returns a factory function for Declarative ETL.
    Accepts a 'math' string expression where 'x' is the value (e.g., math="x / 10").
    """
    # [DSA OPTIMIZATION]: Pre-compile the math expression into bytecode
    compiled_math = compile(math, "<string>", "eval") if math else None

    # 1. FACTORY MODE: Configuring the converter for the conv_dict
    if value == "__INCORP_FACTORY__":
        def _factory(val: Any) -> Optional[float]:
            if val is None or val == "":
                return default

            if isinstance(val, str):
                clean_val = str(val).strip().lower()
                if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
                    return default
                clean_val = clean_val.replace(",", "")
            else:
                clean_val = val

            try:
                result = float(clean_val)
                if compiled_math:
                    # Secure Sandboxed Evaluation using pre-compiled bytecode
                    safe_env = {"x": result, "abs": abs, "round": round, "min": min, "max": max}
                    result = float(eval(compiled_math, {"__builtins__": {}}, safe_env))
                return result
            except Exception:
                # Catches ZeroDivisionError, SyntaxError, or ValueError safely
                return default

        return _factory

    # 2. EXECUTION MODE: Processing the actual data (Direct call)
    if value is None or value == "":
        return default

    if isinstance(value, str):
        clean_val = str(value).strip().lower()
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            return default
        clean_val = clean_val.replace(",", "")
    else:
        clean_val = value

    try:
        result = float(clean_val)
        if compiled_math:
            safe_env = {"x": result, "abs": abs, "round": round, "min": min, "max": max}
            result = float(eval(compiled_math, {"__builtins__": {}}, safe_env))
        return result
    except Exception:
        return default


# ==========================================
# WRAPPERS (Usage in conv_dict: {'key': split_and_get('/')})
# ==========================================

def split_and_get(
        delimiter: str = '/',
        index: int = -1,
        cast_type: Optional[Callable[[Any], Any]] = None
) -> Callable[[Any], Any]:
    """Splits a string, grabs a specific index, and optionally casts it. Returns None if empty."""

    def _splitter(value: Any) -> Any:
        if not value:
            return None
        try:
            result = str(value).strip(delimiter).split(delimiter)[index]
            return cast_type(result) if cast_type is not None else result
        except (IndexError, ValueError, TypeError):
            return None

    return _splitter


def cast_list_items(cast_type: Callable[[Any], Any]) -> Callable[[Any], List[Any]]:
    """Casts every item in a list to a specific type, safely dropping Nulls."""

    def _caster(lst: Any) -> List[Any]:
        if not lst:
            return []
        if not isinstance(lst, list):
            return[cast_type(lst)]
        return[cast_type(item) for item in lst if item is not None and item != ""]

    return _caster


def default_if_null(default_value: Any) -> Callable[[Any], Any]:
    """Substitutes a default value if the input is None or an empty string."""

    def _defaulter(value: Any) -> Any:
        return default_value if value is None or value == "" else value

    return _defaulter


def link_to(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """
    Generates a null-safe relational mapper for conv_dict.
    If given a list of objects, it builds a unified registry to protect against schema splintering.
    """
    # If the dataset is any kind of list (including IncorporatorList), build a unified lookup dict.
    # This protects against dynamically generated objects splintering into isolated class registries.
    if isinstance(dataset, list):
        registry = {
            getattr(item, 'inc_code'): item
            for item in dataset
            if getattr(item, 'inc_code', None) is not None
        }
    else:
        # Fallback for single class/module references
        registry = getattr(dataset, "codeDict", {})

    if not isinstance(registry, collections.abc.Mapping):
        registry = {}

    def _mapper(val: Any) -> Any:
        # Pre-process the value if an extractor was provided
        key = extractor(val) if extractor is not None else val

        if key is None:
            return None
        if key in registry:
            return registry[key]
        try:
            # Final fallback for string-to-int casting
            return registry.get(int(key))
        except (ValueError, TypeError):
            return None

    return _mapper


def link_to_list(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], List[Any]]:
    """Maps an array of foreign keys to an array of Incorporator objects."""
    base_linker = link_to(dataset, extractor)

    def _mapper(val_list: Any) -> List[Any]:
        if not isinstance(val_list, list):
            return[]
        # Applies the base_linker to every item, discarding None results
        return[obj for v in val_list if (obj := base_linker(v)) is not None]

    return _mapper


# ==========================================
# URL & NESTED DATA TOOLS
# ==========================================

def json_path_extractor(*keys: str) -> Callable[[str], Optional[str]]:
    """
    Creates a pagination extractor that drills into a JSON body using a sequence of keys.
    Example: json_path_extractor('info', 'next') finds data['info']['next'].
    """
    def _extractor(raw_json_str: str) -> Optional[str]:
        try:
            data = json.loads(raw_json_str)
            for key in keys:
                if isinstance(data, dict):
                    data = data.get(key)
                else:
                    return None
            return str(data) if data else None
        except Exception:
            return None
    return _extractor


def extract_url_id(cast_type: Callable[[Any], Any] = int) -> Callable[[Any], Any]:
    """
    Extracts the trailing ID from a REST URL (e.g., '.../character/1/' -> 1).
    """
    def _extractor(url_str: Any) -> Any:
        if not isinstance(url_str, str) or not url_str:
            return None
        try:
            clean_str = url_str.strip('/')
            result = clean_str.split('/')[-1]
            return cast_type(result) if cast_type is not None else result
        except (ValueError, TypeError, IndexError):
            return None
    return _extractor


def pluck(key: str, chain: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """
    Extracts a specific key from a nested dictionary, falling back to the raw value.
    Optionally chains the result into another converter (like extract_url_id).
    """
    def _plucker(val: Any) -> Any:
        extracted = val.get(key) if isinstance(val, dict) else val
        if chain:
            return chain(extracted)
        return extracted
    return _plucker