"""Graph extraction, relational linking, and declarative POST tokens.

Provides ``link_to``, ``link_to_list``, ``pluck``, ``each``, ``join_all``,
``as_list``, and utility functions for navigating relational data and building
concurrent request payloads.  Every converter in this module is designed for
use in ``conv_dict`` or ``json_payload`` / ``form_payload`` kwargs.
"""

from __future__ import annotations

import collections.abc
import logging
from collections.abc import Callable
from typing import Any

from .converters import Op, _EachSentinel, is_garbage_value
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
    delimiter: str = "/", index: int = -1, cast_type: Callable[[Any], Any] | None = None, pure: bool = True
) -> Op:
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
            part).  An all-delimiter or empty-tail input (e.g. ``"//"``)
            strips down to ``""`` before the split, so the selected
            position can itself be the empty string rather than ``None``.
        cast_type: Optional callable applied to the extracted string
            (e.g. ``int`` to convert a numeric ID).
        pure: Defaults to ``True`` — the split/strip logic is always pure, and
            the shipped ``cast_type`` values (``int``, ``float``, ``str``) are
            pure builtins, so identical extracted substrings are computed once
            and the result reused (the dispatcher's adaptive lru_cache wrapping
            for low-cardinality input tuples).  Pass ``pure=False`` explicitly
            when your ``cast_type`` must run for its side effects on every row
            (not just once per unique extracted substring) or returns a mutable
            object that must not be shared across rows.

    Returns an :class:`~incorporator.schema.converters.Op` for use in ``conv_dict``.  Garbage values
    (``None``, ``""``, ``"N/A"``, ``"null"``, ``"unknown"``, ``"nan"``,
    ``"undefined"`` — see :func:`is_garbage_value`) pass through as
    ``None``; out-of-range indices and failed casts also return ``None``
    rather than raising.
    """

    def _split(value: Any) -> Any:
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

    return Op(_split, input_keys=(), is_pure=pure)


# ==========================================
# GRAPH & EXTRACTORS
# ==========================================


def link_to(dataset: Any, extractor: Callable[[Any], Any] | None = None) -> Op:
    """SQL-style JOIN as a one-liner — replace a foreign-key value with the actual instance.

    Reach for it whenever the row has an FK and the related dataset is
    already on hand: SpaceX launches carry a ``rocket`` UUID and you
    want the actual :class:`Rocket` instance; CoinGecko assets carry a
    ``symbol`` and you want the live Binance book.  The lookup goes
    through the other class's :attr:`inc_dict`, so it's an O(1) hit per
    row — no quadratic scan.

    **Lazy and live, not a snapshot.**  ``link_to()`` does not copy
    ``dataset`` at construction time — it stores the reference and
    re-reads ``dataset.inc_dict`` on every lookup.  This means a
    ``link_to(PeerClass)`` built before ``PeerClass`` has ever ticked
    (e.g. a JSON-config ``"link_to(PeerClass)"`` token resolved at
    config-load time) resolves to ``None`` on the first few calls and
    then starts resolving correctly the moment ``PeerClass`` populates
    its registry — it never gets permanently stuck empty.

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
        dataset: The right-hand side of the join — an
            :class:`IncorporatorList`, an :class:`Incorporator` subclass, or
            any object exposing a live ``inc_dict`` mapping.  Raises
            :class:`TypeError` at construction if ``dataset`` has no
            ``inc_dict`` attribute (a plain ``list`` is **not** accepted —
            build an :class:`IncorporatorList` via ``incorp()`` instead).
        extractor: Optional transformer applied to the current row's
            value before the lookup — useful when the FK needs reshaping
            (e.g. uppercase + suffix to match a stock-ticker format)::

                conv_dict={
                    "binance_pair": link_to(books, extractor=lambda sym: f"{sym.upper()}USDT"),
                }

    Returns:
        An :class:`~incorporator.schema.converters.Op` instance.  Unmatched keys resolve to
        ``None`` — never raises.  The lookup tries the key as-is and also its ``str()``
        form to absorb the common "API returns int, registry keyed by
        string" mismatch.

    **Null handling.**  The optional ``extractor`` callable is only
    invoked when the source value passes :func:`is_garbage_value` —
    garbage FKs short-circuit to ``None`` without entering the extractor
    (otherwise an ``extractor`` like ``str.upper`` would raise on a None
    FK and trigger a per-row WARNING at the dispatch boundary).

    **Liveness and GC contract.**  Every call reads ``dataset.inc_dict``
    fresh — there is no cached snapshot to go stale.  For an
    :class:`IncorporatorList` target, the closure's strong reference to
    the list keeps its rows alive for as long as the ``Op`` itself is
    alive (stronger than a point-in-time copy).  For a bare
    :class:`Incorporator` subclass target, row liveness depends on
    *something else* holding a strong reference to each instance — the
    same class-level :class:`weakref.WeakValueDictionary` race that
    already applies anywhere ``Cls.inc_dict`` is read directly. A
    garbage-collected or absent key resolves to ``None``, never raises.

    For lists of foreign keys (e.g. tags → tag objects) use
    :func:`link_to_list`.
    """
    if not isinstance(getattr(dataset, "inc_dict", None), collections.abc.Mapping):
        raise TypeError(
            f"link_to() requires a target with a live 'inc_dict' mapping — pass the "
            f"IncorporatorList returned by incorp() or an Incorporator subclass, not "
            f"{type(dataset).__name__!r}. A plain list is not accepted."
        )

    def _lookup(val: Any) -> Any:
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
        # keys would still cost the str-coercion + lookups below — and
        # a future warning-instrumented lookup would falsely surface
        # this as a "missed join" when it's actually a missing FK.
        if is_garbage_value(key):
            return None

        # Re-read dataset.inc_dict on EVERY call — never cache it, not
        # even lazily on first call. A class's inc_dict starts as the
        # shared base WeakValueDictionary and forks into a per-class dict
        # on that class's FIRST write (Incorporator._ensure_inc_dict()).
        # A link_to() built before the peer's first tick would cache the
        # pre-fork object and miss every entry the peer ever registers.
        reg = getattr(dataset, "inc_dict", {})
        value = reg.get(key)
        if value is not None:
            return value
        # Ultimate Type-Splinter defense (Strings)
        return reg.get(str(key))

    # is_pure=False is load-bearing, not a leftover default: this lookup is
    # non-referentially-transparent by design (None before the peer ticks,
    # the actual object after) — lru_cache-wrapping it would freeze the
    # very "before it ticks" None the lazy design exists to fix.
    return Op(_lookup, input_keys=(), is_pure=False)


def link_to_list(dataset: Any, extractor: Callable[[Any], Any] | None = None) -> Op:
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
        An :class:`~incorporator.schema.converters.Op` instance that accepts a list of foreign keys
        and returns a list of matched objects.  Non-list inputs return an empty list;
        unmatched individual keys are silently omitted.

    **Null handling.**  Garbage list elements (per :func:`is_garbage_value`)
    are filtered before the per-element lookup.  Mirrors :func:`link_to`'s
    extractor pre-check.
    """
    base_op = link_to(dataset, extractor)

    def _list_lookup(val_list: Any) -> list[Any]:
        if not isinstance(val_list, list):
            return []
        # Per-element garbage filter mirrors link_to's pre-check.  The
        # inner base_op also pre-checks for safety, but skipping the
        # call entirely is the cheaper path on garbage-heavy lists.
        return [obj for v in val_list if not is_garbage_value(v) and (obj := base_op(v)) is not None]

    return Op(_list_lookup, input_keys=(), is_pure=False)


def pluck(key: str, chain: Callable[[Any], Any] | None = None) -> Op:
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

    Returns an :class:`~incorporator.schema.converters.Op` instance.  Missing path segments resolve
    to ``None`` rather than raising — drilling through ``{"a": None}`` for path
    ``"a.b"`` returns ``None`` safely.

    **Null handling.**  The optional ``chain`` callable is only invoked
    when the extracted value does *not* pass :func:`is_garbage_value`.
    Missing path segments resolve to ``None`` (via :meth:`DataPath.resolve`);
    a garbage-sentinel leaf value (e.g. ``"n/a"``, ``"unknown"``) is
    returned unchanged, not coerced to ``None``, and in either case the
    chain callable is never entered.  Lets you compose stdlib callables
    (``pluck("data.title", chain=str.lower)``) without writing a
    defensive null guard.

    For a flat-field transform (no nested extraction) use :func:`~incorporator.schema.converters.calc`
    (fn, key).
    """
    path = DataPath.parse(key)

    def _pluck(val: Any) -> Any:
        extracted = path.resolve(val)
        if chain is None or is_garbage_value(extracted):
            return extracted
        return chain(extracted)

    return Op(_pluck, input_keys=(key,), is_pure=True, whole_row=True)


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


def join_all(delimiter: str = ",") -> Op:
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

    Returns an :class:`~incorporator.schema.converters.Op` instance.  Non-list inputs pass through as
    ``str(value)``.

    See :func:`each` (N requests) and :func:`as_list` (one request, JSON
    array body) for the other request-count patterns.
    """

    def _join(data: Any) -> str:
        if not isinstance(data, list):
            return str(data)
        return delimiter.join(str(x) for x in data if x is not None)

    return Op(_join, input_keys=(), is_pure=True)


def as_list() -> Op:
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
        An :class:`~incorporator.schema.converters.Op` instance.  Scalar inputs are wrapped in a
        single-element list.  Each call returns a fresh, per-row list —
        results are never shared or aliased across rows, even for repeated
        equal scalar inputs.

    See :func:`each` (N requests) and :func:`join_all` (one request,
    delimited string) for the other request-count patterns.
    """

    def _wrap_or_pass(data: Any) -> list[Any]:
        return data if isinstance(data, list) else [data]

    return Op(_wrap_or_pass, input_keys=(), is_pure=False)
