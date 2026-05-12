"""
Graph extraction, relational linking, and declarative POST tokens for Incorporator.

Provides link_to, link_to_list, pluck, each, join_all, as_list, and utility functions
for navigating relational data and building concurrent request payloads.
"""

import collections.abc
import logging
import weakref
from typing import Any, Callable, Dict, List, Optional

from .converters import _EachSentinel

logger = logging.getLogger(__name__)


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
# GRAPH & EXTRACTORS
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
            # Alert on every miss so memory pressure from large non-weakrefable datasets
            # is visible in logs rather than silently accumulating.
            logger.debug(
                "link_to: strong-ref fallback cache miss for key %r — "
                "object is not weakrefable. Large non-weakrefable datasets (e.g. built-in dicts) "
                "will not be garbage-collected until the enclosing scope exits.",
                k,
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
