"""Graph extraction, relational linking, and declarative POST tokens.

Provides ``link_to``, ``link_to_list``, ``pluck``, ``each``, ``join_all``,
``as_list``, and utility functions for navigating relational data and building
concurrent request payloads.  Every converter in this module is designed for
use in ``conv_dict`` or ``json_payload`` / ``form_payload`` kwargs.
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
    """Sum any number of values, treating non-numeric and ``None`` as zero.

    A ready-made reducer for :func:`calc` — pair it with field names that
    hold numeric strings or floats and you get a safe total regardless of
    whether the API returns the values as ``int``, ``"42"``, or ``None``::

        from incorporator import calc, sum_attributes

        await User.incorp(
            inc_url="...",
            conv_dict={
                "total": calc(sum_attributes, "subtotal", "tax", "tip"),
            },
        )
    """
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
    """Split a string by ``delimiter`` and return one position.

    Common pattern for extracting an ID from the tail of a HATEOAS URL::

        # "https://api.example.com/pokemon/25/" → 25
        conv_dict={
            "id": split_and_get("/", index=-2, cast_type=int),
        }

    Args:
        delimiter: Character(s) to split on.  Surrounding occurrences are
            stripped before the split so ``"/foo/"`` and ``"foo"`` behave
            identically.
        index: Position to return from the resulting list — negative
            indices count from the end (default ``-1`` returns the last
            non-empty part).
        cast_type: Optional callable applied to the extracted string
            (e.g. ``int`` to convert a numeric ID).

    Returns a closure for use in ``conv_dict``.  ``None`` / empty values
    pass through as ``None``; out-of-range indices and failed casts also
    return ``None`` rather than raising.
    """

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
    """Join one source's foreign-key field to another source's instances.

    Pass an :class:`IncorporatorList` (or any object with an ``inc_dict``)
    and a value from the current row will be looked up in that list's
    registry — turning a string ID into the actual instance::

        binance_books = await BinanceBook.incorp(inc_url="...", inc_code="symbol")

        assets = await Asset.incorp(
            inc_url="...",
            conv_dict={
                # "BTC" → binance_books.inc_dict["BTC"]  (the actual record)
                "live_book": link_to(binance_books),
            },
        )

    Args:
        dataset: The right-hand side of the join.  Typically an
            :class:`IncorporatorList`; any object with an ``inc_dict``
            mapping works too.
        extractor: Optional transformer applied to the current row's
            value before the lookup — useful when the FK needs reshaping
            (e.g. uppercase + suffix to match a stock-ticker format)::

                conv_dict={
                    "binance_pair": link_to(books, extractor=lambda sym: f"{sym.upper()}USDT"),
                }

    Returns:
        A converter closure.  Unmatched keys resolve to ``None`` — never
        raises.  The lookup tries the key as-is and also its ``str()``
        form to absorb the common "API returns int, registry keyed by
        string" mismatch.

    For lists of foreign keys (e.g. tags → tag objects) use
    :func:`link_to_list`.
    """

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
    """Plural variant of :func:`link_to` — resolve a list of foreign keys to objects.

    Use when the source field is itself a list of IDs (e.g. ``tag_ids``,
    ``author_uuids``).  Returns a list of matched instances; unmatched
    keys are filtered out silently.

    ::

        articles = await Article.incorp(
            inc_url="...",
            conv_dict={
                # "tag_ids": ["python", "etl"] → [Tag(python), Tag(etl)]
                "tags": link_to_list(tags),
            },
        )
    """
    base_linker = link_to(dataset, extractor)

    def _mapper(val_list: Any) -> List[Any]:
        if not isinstance(val_list, list):
            return []
        return [obj for v in val_list if (obj := base_linker(v)) is not None]

    return _mapper


def pluck(key: str, chain: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    """Drill into a nested dict and return one value by dotted path.

    Common pattern for lifting a deeply-nested field up to a top-level
    attribute on the resulting object::

        # Source row: {"pad": {"location": {"name": "Kennedy SC"}}}
        # Target:     launch.pad_name == "Kennedy SC"

        await Launch.incorp(
            inc_url="...",
            conv_dict={
                "pad_name": pluck("pad.location.name"),
            },
        )

    Args:
        key: Dot-separated path to the value (e.g. ``"a.b.c"``).
        chain: Optional callable applied to the extracted value (e.g.
            ``int`` or another converter token like ``inc(datetime)``).

    Returns a converter closure.  Missing path segments resolve to ``None``
    rather than raising — drilling through ``{"a": None}`` for path
    ``"a.b"`` returns ``None`` safely.
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
    """POST-payload token: send one HTTP request per extracted parent ID.

    Place inside a ``json_payload`` / ``form_payload`` dict when you want
    :meth:`Incorporator.incorp` to fan out **N concurrent POSTs** — one
    per row in the parent dataset — each carrying the corresponding
    parent ID at that position::

        results = await Decoded.incorp(
            inc_url="https://api.example.com/decode",
            inc_parent=invoices,
            inc_child="vehicle_id",
            http_method="POST",
            json_payload={"vehicle_id": each(), "format": "json"},
        )

    Pair with :func:`join_all` (one bulk request) or :func:`as_list`
    (one request carrying an array) when the target endpoint accepts a
    batch shape — your choice of token controls the request count.
    """
    return _EachSentinel()


def join_all(delimiter: str = ",") -> Callable[[Any], str]:
    """POST-payload token: send one bulk request with all parent IDs joined.

    Place inside a ``json_payload`` / ``form_payload`` dict to collapse
    every extracted parent ID into a **single delimited string** the
    target endpoint can scan in one request::

        specs = await NHTSASpec.incorp(
            inc_url="https://vpic.nhtsa.dot.gov/.../DecodeVINValuesBatch/",
            inc_parent=invoices,
            inc_child="Vehicle.VIN",
            http_method="POST",
            payload_type="form",
            form_payload={"data": join_all(";"), "format": "json"},
        )

    Args:
        delimiter: Separator between IDs.  Default ``","``; common
            alternatives are ``";"`` and ``"|"`` depending on the API.

    Returns a converter closure.  Non-list inputs pass through as
    ``str(value)``.

    See :func:`each` (N requests) and :func:`as_list` (one request, JSON
    array body) for the other request-count patterns.
    """

    def _joiner(data: Any) -> str:
        if not isinstance(data, list):
            return str(data)
        return delimiter.join(str(x) for x in data if x is not None)

    return _joiner


def as_list() -> Callable[[Any], List[Any]]:
    """POST-payload token: send one bulk request carrying parent IDs as a JSON array.

    Place inside a ``json_payload`` dict to ship every extracted parent ID
    in a **single request** with the IDs as a JSON list — the natural
    shape for endpoints that accept a typed array body::

        results = await Endpoint.incorp(
            inc_url="https://api.example.com/bulk",
            inc_parent=invoices,
            inc_child="id",
            http_method="POST",
            json_payload={"ids": as_list()},   # → {"ids": [1, 2, 3, ...]}
        )

    Returns a converter closure.  Scalar inputs are wrapped in a
    single-element list.

    See :func:`each` (N requests) and :func:`join_all` (one request,
    delimited string) for the other request-count patterns.
    """
    return lambda data: data if isinstance(data, list) else [data]
