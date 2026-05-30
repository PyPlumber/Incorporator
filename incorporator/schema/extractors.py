"""Graph extraction, relational linking, and declarative POST tokens.

Provides ``link_to``, ``link_to_list``, ``pluck``, ``each``, ``join_all``,
``as_list``, and utility functions for navigating relational data and building
concurrent request payloads.  Every converter in this module is designed for
use in ``conv_dict`` or ``json_payload`` / ``form_payload`` kwargs.
"""

from __future__ import annotations

import collections.abc
import logging
import weakref
from collections.abc import Callable
from typing import Any

from .converters import _EachSentinel, is_garbage_value
from .path import DataPath

logger = logging.getLogger(__name__)


# ==========================================
# COMMON CALC() FUNCTIONS (Built-ins)
# ==========================================
def sum_attributes(*args: Any) -> float:
    """Ready-made reducer for :func:`calc` — safely sum N fields, treating ``None`` and non-numeric as zero.

    Reach for it whenever you'd otherwise write a 3-line try/except to
    total a handful of row fields: PokéAPI Base Stat Total, revenue
    sums across line items, point-totals in fantasy NASCAR scoring.
    Numeric strings (``"42"``), floats, ints, and ``None`` all mix
    safely — anything that can't be cast contributes zero rather than
    raising.

    Example::

        from incorporator import calc, sum_attributes

        await Pokemon.incorp(
            inc_url="...",
            conv_dict={
                "total_stats": calc(sum_attributes, "hp", "attack", "defense", "speed"),
            },
        )

    Args:
        *args: Values to sum.  Any number of positional arguments are
            accepted; ``None`` and non-numeric values contribute zero.

    Returns:
        The sum as a ``float``.  Returns ``0.0`` when all inputs are
        ``None`` or non-numeric.
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
    delimiter: str = "/", index: int = -1, cast_type: Callable[[Any], Any] | None = None
) -> Callable[[Any], Any]:
    """Extract an ID from a delimited string — the HATEOAS URL-tail / colon-separated-key one-liner.

    Reach for it whenever an API hands back a delimited value and the
    useful bit is one position inside: ``"/api/items/42/"`` (HATEOAS
    URL, the ``42`` is the FK), ``"namespace:resource:id"`` (composite
    key), a single column inside a CSV-style cell.

    Example::

        # "https://pokeapi.co/api/v2/pokemon/25/" → 25
        await Move.incorp(
            inc_url="...",
            conv_dict={
                "pokemon_id": split_and_get("/", index=-2, cast_type=int),
            },
        )

    Args:
        delimiter: Character(s) to split on.  Surrounding occurrences are
            stripped before the split so ``"/foo/"`` and ``"foo"`` behave
            identically.
        index: Position to return from the resulting list — negative
            indices count from the end (default ``-1`` returns the last
            non-empty part).
        cast_type: Optional callable applied to the extracted string
            (e.g. ``int`` to convert a numeric ID).

    Returns a closure for use in ``conv_dict``.  Garbage values
    (``None``, ``""``, ``"N/A"``, ``"null"``, ``"unknown"``, ``"nan"``,
    ``"undefined"`` — see :func:`is_garbage_value`) pass through as
    ``None``; out-of-range indices and failed casts also return ``None``
    rather than raising.
    """

    def _splitter(value: Any) -> Any:
        # Align with inc()'s null contract via :func:`is_garbage_value`:
        # ``None``, ``""``, and the canonical garbage set (``"n/a"``,
        # ``"null"``, ``"unknown"``, ``"nan"``, ``"undefined"``) all
        # short-circuit to ``None`` without entering the split/cast path.
        if is_garbage_value(value):
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


def link_to(dataset: Any, extractor: Callable[[Any], Any] | None = None) -> Callable[[Any], Any]:
    """SQL-style JOIN as a one-liner — replace a foreign-key value with the actual instance.

    Reach for it whenever the row has an FK and the related dataset is
    already on hand: SpaceX launches carry a ``rocket`` UUID and you
    want the actual :class:`Rocket` instance; CoinGecko assets carry a
    ``symbol`` and you want the live Binance book.  The lookup goes
    through the other class's :attr:`inc_dict`, so it's an O(1) hit per
    row — no quadratic scan.

    Example::

        rockets = await Rocket.incorp(inc_url="...", inc_code="id")

        launches = await Launch.incorp(
            inc_url="...",
            conv_dict={
                # "5e9d0d95eda69973a809d1ec" → rockets.inc_dict["5e9d..."]  (Rocket instance)
                "rocket": link_to(rockets),
            },
        )
        # Before: launch.rocket == "5e9d0d95eda69973a809d1ec"
        # After:  launch.rocket is the actual Rocket instance — launch.rocket.name works

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

    **Null handling.**  The optional ``extractor`` callable is only
    invoked when the source value passes :func:`is_garbage_value` —
    garbage FKs short-circuit to ``None`` without entering the extractor
    (otherwise an ``extractor`` like ``str.upper`` would raise on a None
    FK and trigger a per-row WARNING at the dispatch boundary).

    For lists of foreign keys (e.g. tags → tag objects) use
    :func:`link_to_list`.
    """

    # 1. Primary Cache: OOM-Safe for production Incorporator/Pydantic objects
    registry: weakref.WeakValueDictionary[Any, Any] = weakref.WeakValueDictionary()

    # 2. Fallback Cache: Strong references for tests (SimpleNamespace) or non-weakref classes
    fallback_registry: dict[Any, Any] = {}

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
        # Align with inc()'s null-handling contract: garbage input
        # short-circuits to ``None`` BEFORE invoking the optional
        # ``extractor`` callable.  Without this pre-check, a None FK +
        # an extractor like ``str.upper`` would raise TypeError, get
        # caught at the builder.py dispatch boundary, and emit a
        # "conv_dict failed" WARNING on every garbage row.
        if is_garbage_value(val):
            return None
        key = extractor(val) if extractor is not None else val
        # Symmetric output-side guard: when an extractor returns
        # garbage (``None``, ``""``, ``"N/A"``, etc.), short-circuit
        # to ``None`` instead of attempting the registry lookup.
        # The dict lookup itself wouldn't find anything, but garbage
        # keys would still cost the str-coercion + four lookups
        # below — and a future warning-instrumented lookup would
        # falsely surface this as a "missed join" when it's actually
        # a missing FK.
        if is_garbage_value(key):
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


def link_to_list(dataset: Any, extractor: Callable[[Any], Any] | None = None) -> Callable[[Any], list[Any]]:
    """1-to-N JOIN — resolve a list of foreign-key IDs to the corresponding instances.

    Reach for it whenever the source field is itself a list of IDs:
    a SpaceX launch has ``payloads: list[str]`` of payload UUIDs, an
    article has ``tag_ids: list[str]``, a player has a roster of
    ``team_member_uuids``.  Same registry-backed lookup as
    :func:`link_to`, applied element-wise — unmatched individual keys
    are silently dropped from the result.

    Example::

        payloads = await Payload.incorp(inc_url="...", inc_code="id")

        launches = await Launch.incorp(
            inc_url="...",
            conv_dict={
                # ["payload_a", "payload_b"] → [Payload(a), Payload(b)]
                "payloads": link_to_list(payloads),
            },
        )

    Args:
        dataset: The right-hand side of the join — same contract as
            :func:`link_to`.  Typically an :class:`IncorporatorList`; any
            object with an ``inc_dict`` mapping works too.
        extractor: Optional transformer applied to each element before the
            lookup — same contract as :func:`link_to`.

    Returns:
        A converter closure that accepts a list of foreign keys and returns
        a list of matched objects.  Non-list inputs return an empty list;
        unmatched individual keys are silently omitted.

    **Null handling.**  Garbage list elements (per :func:`is_garbage_value`)
    are filtered before the per-element lookup.  Mirrors :func:`link_to`'s
    extractor pre-check.
    """
    base_linker = link_to(dataset, extractor)

    def _mapper(val_list: Any) -> list[Any]:
        if not isinstance(val_list, list):
            return []
        # Per-element garbage filter mirrors link_to's pre-check.  The
        # inner base_linker also pre-checks for safety, but skipping the
        # closure invocation entirely is the cheaper path on garbage-heavy
        # lists.
        return [obj for v in val_list if not is_garbage_value(v) and (obj := base_linker(v)) is not None]

    return _mapper


def pluck(key: str, chain: Callable[[Any], Any] | None = None) -> Callable[[Any], Any]:
    """Lift a deeply-nested field to a top-level attribute using a dot-notation path.

    Reach for it whenever the API buries the value you actually want
    inside two or three layers of envelope: JSON:API-style
    ``{"data": {"attributes": {"price": 42}}}``, SpaceX
    ``{"pad": {"location": {"name": "Kennedy SC"}}}``, anything with a
    ``meta`` / ``attributes`` / ``links`` wrapper.  Missing path
    segments resolve to ``None`` rather than raising, so partially
    populated rows don't blow up the build.

    Example::

        # Source row: {"data": {"attributes": {"price": 42}}}
        # Target:     asset.price == 42

        await Asset.incorp(
            inc_url="...",
            conv_dict={
                "price": pluck("data.attributes.price"),
            },
        )

    Args:
        key: Dot-separated path to the value (e.g. ``"a.b.c"`` or ``"splits.0.stat"`` for list indexing).
        chain: Optional callable applied to the extracted value (e.g.
            ``int`` or another converter token like ``inc(datetime)``).

    Returns a converter closure.  Missing path segments resolve to ``None``
    rather than raising — drilling through ``{"a": None}`` for path
    ``"a.b"`` returns ``None`` safely.

    **Null handling.**  The optional ``chain`` callable is only invoked
    when the extracted value passes :func:`is_garbage_value` — missing
    path segments / garbage leaf values short-circuit to ``None``
    without entering the chain callable.  Lets you compose stdlib
    callables (``pluck("data.title", chain=str.lower)``) without writing
    a defensive null guard.
    """

    _path = DataPath.parse(key)

    def _plucker(val: Any) -> Any:
        extracted = _path.resolve(val)
        if chain is None or is_garbage_value(extracted):
            return extracted
        return chain(extracted)

    return _plucker


# ==========================================
# DECLARATIVE PAYLOAD TOKENS (POST/PUT)
# ==========================================


def each() -> _EachSentinel:
    """Fan out N POST requests, one per parent ID — for APIs that won't accept a bulk body.

    Reach for it when the target endpoint takes exactly one ID per
    call: the NHTSA VPIC ``DecodeVin/`` endpoint accepts one VIN per
    request, plenty of older REST APIs reject batch payloads outright.
    You have 200 VINs in :attr:`IncorporatorList.inc_dict` from the
    parent dataset; ``each()`` says "do 200 concurrent POSTs and stitch
    the results back into one :class:`IncorporatorList`."

    Example::

        results = await Decoded.incorp(
            inc_url="https://vpic.nhtsa.dot.gov/.../DecodeVin/",
            inc_parent=invoices,
            inc_child="Vehicle.VIN",
            http_method="POST",
            json_payload={"vin": each(), "format": "json"},
        )

    Pair with :func:`join_all` (one bulk request with a delimited
    string) or :func:`as_list` (one bulk request carrying a JSON array)
    when the endpoint accepts a batch shape — your choice of token is
    what controls the request count.
    """
    return _EachSentinel()


def join_all(delimiter: str = ",") -> Callable[[Any], str]:
    """Collapse all parent IDs into one delimited string for a single bulk POST.

    Reach for it when the endpoint supports a delimited-batch shape:
    the NHTSA VPIC ``DecodeVINValuesBatch/`` endpoint takes
    ``vin1;vin2;vin3``, plenty of older audit APIs accept
    comma-separated ID lists.  One HTTP request total, the IDs
    collapsed into the string format the server expects — the right
    choice when ``each`` (N requests) would be wasteful and ``as_list``
    (JSON array body) isn't what the endpoint accepts.

    Example::

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


def as_list() -> Callable[[Any], list[Any]]:
    """Ship all parent IDs in one POST as a JSON array — the natural shape for typed REST endpoints.

    Reach for it when the endpoint expects ``{"ids": [1, 2, 3]}`` (or
    any other JSON-array-bodied bulk POST) — the dominant shape for
    modern typed REST APIs and the natural fit for bulk-POST audit
    endpoints.  One HTTP request total, IDs delivered as a proper JSON
    list — the right choice when ``each`` (N requests) would be
    wasteful and ``join_all`` (delimited string) would force the server
    to re-parse a stringified payload.

    Example::

        results = await Audit.incorp(
            inc_url="https://api.example.com/bulk-audit",
            inc_parent=invoices,
            inc_child="id",
            http_method="POST",
            json_payload={"ids": as_list()},   # → {"ids": [1, 2, 3, ...]}
        )

    Returns:
        A converter closure.  Scalar inputs are wrapped in a
        single-element list.

    See :func:`each` (N requests) and :func:`join_all` (one request,
    delimited string) for the other request-count patterns.
    """
    return lambda data: data if isinstance(data, list) else [data]
