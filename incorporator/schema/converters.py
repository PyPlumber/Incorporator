"""Type-ranked conversion engine: ``inc()``, ``calc()``, and ``calc_all()``.

Provides ranked fallback converters that guarantee null-safe ETL pipelines.
Every converter tries the Pydantic TypeAdapter first, then format-specific
fallbacks (ISO-8601 for ``datetime``, comma-stripped strings for ``int`` /
``float``, truthy-string normalisation for ``bool``).  Garbage values
(``"N/A"``, ``"null"``, empty string) short-circuit to ``default`` without
entering the fallback chain.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from pydantic import TypeAdapter

from .kind import DataKind
from .path import DataPath

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

    __slots__ = ("func", "default", "target_type", "input_list", "is_pure")

    def __init__(
        self, func: Callable[..., Any], default: Any, target_type: Any, input_list: list[str], pure: bool = False
    ):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = [DataPath.parse(dep) for dep in input_list]
        self.is_pure = pure
        if pure:
            self.func = functools.lru_cache(maxsize=10_000)(func)


class CalcAllOp:
    """Marker indicating an operation that requires full array processing down the column."""

    __slots__ = ("func", "default", "target_type", "input_list", "is_pure")

    def __init__(
        self, func: Callable[..., Any], default: Any, target_type: Any, input_list: list[str], pure: bool = False
    ):
        self.func = func
        self.default = default
        self.target_type = target_type
        self.input_list = [DataPath.parse(dep) for dep in input_list]
        self.is_pure = pure


class Op:
    """Generic conv_dict marker — wraps a closure with dispatcher-visible metadata.

    Used by the 7 light-state converters (inc/pluck/link_to/link_to_list/
    split_and_get/join_all/as_list) so they expose ``input_keys``, ``is_pure``,
    and ``whole_row`` to the apply_etl_transformations dispatcher without
    requiring a dedicated class each.  CalcOp / CalcAllOp keep their own
    dedicated shapes because their multi-field state benefits from named
    attribute access in the dispatcher's calc-specific branches.

    When ``is_pure=True`` and ``whole_row=False``, ``_func`` is replaced at
    construction with ``functools.lru_cache(maxsize=10_000)(func)``.  The
    10_000 maxsize is a memory bound — caps cache growth in long-running
    streams. Not a tuning knob; the value is conservative.  The
    ``not whole_row`` guard is required because pluck's func operates on whole
    dicts which are not hashable; lru_cache would TypeError on every call.

    **`is_pure=True` is a caller-asserted claim, not a framework-verified
    one.**  Passing ``is_pure=True`` with a closure that captures mutable
    state, calls ``datetime.now()`` / ``uuid.uuid4()``, reads env vars, or
    performs I/O will cache its first per-input result and skip subsequent
    invocations — side effects only fire on cache miss.  Pass
    ``is_pure=False`` whenever the closure is not referentially transparent.
    """

    __slots__ = ("_func", "input_keys", "is_pure", "whole_row")

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        input_keys: tuple[str, ...] = (),
        is_pure: bool = True,
        whole_row: bool = False,
    ) -> None:
        self._func = func
        self.input_keys = input_keys
        self.is_pure = is_pure
        self.whole_row = whole_row
        if is_pure and not whole_row:
            self._func = functools.lru_cache(maxsize=10_000)(func)

    def __call__(self, val: Any) -> Any:
        try:
            return self._func(val)
        except TypeError:
            # lru_cache raises TypeError on unhashable arguments (e.g. dict, list).
            # Fall back to the unwrapped callable so callers that pass unhashable
            # values (join_all receiving a list, inc(new) receiving a dict) still work.
            wrapped = getattr(self._func, "__wrapped__", None)
            if wrapped is not None:
                return wrapped(val)
            raise


def calc(
    func: Callable[..., Any],
    *input_keys: str,
    default: Any = None,
    target_type: Any = None,
    pure: bool = True,
) -> CalcOp:
    """Synthesise a derived field per row from one or more source fields.

    Use it whenever the value you want lives in the row but the API
    doesn't ship it directly — PokéAPI Base Stat Total
    (``hp + attack + defense + ...``), cross-exchange spread in basis
    points (``(ask - bid) / mid * 10_000``), full names, percentages,
    any per-row aggregation.  Drop the return value into a ``conv_dict``
    entry; the framework calls ``func`` once per row with the named
    source fields as positional arguments.

    Example::

        def spread_bps(bid: float, ask: float) -> float:
            mid = (bid + ask) / 2
            return (ask - bid) / mid * 10_000

        books = await Book.incorp(
            inc_url="...",
            conv_dict={
                "spread_bps": calc(spread_bps, "bid", "ask", target_type=float),
            },
        )

    Args:
        func: Callable invoked once per row.  Receives one positional
            argument per name in ``input_keys`` (missing keys arrive
            as ``None``).
        *input_keys: Source field names whose values are passed to ``func``.
        default: Value used when ``func`` raises or returns ``None``.
        target_type: Optional type the result is coerced to (``int``,
            ``float``, ...).
        pure: Defaults to ``True`` — the conv_dict layer is a data-transform
            layer, and side-effect lambdas (``datetime.now()``, logging, DB
            writes, network calls) are an anti-pattern here.  Pass
            ``pure=False`` explicitly when your ``func`` must run for its side
            effects on every row, not just once per unique input.  The default
            enables the dispatcher's adaptive lru_cache wrapping for
            low-cardinality input tuples.

    Returns:
        A :class:`CalcOp` marker — store it in ``conv_dict``; the engine
        unwraps it during instance construction.

    **Null handling.**  Missing or garbage input values (``None``,
    ``""``, ``"N/A"``, ``"null"``, ``"unknown"``, ``"nan"``,
    ``"undefined"``) are detected via :func:`is_garbage_value` BEFORE
    ``func`` is called.  When EVERY ``input_keys`` value is garbage,
    ``calc`` short-circuits to ``default`` silently — no warning
    emitted.  ``func`` is only invoked when at least one input is real
    data; if it raises on that real data, the warning fires and
    ``default`` is used.

    This mirrors :func:`inc`'s null-handling contract: in both, the
    caller never has to write ``lambda v: v.lower() if v else ""`` —
    the framework guards the null path itself.  Prefer the canonical
    lambda-free form::

        calc(str.lower, "title", default="", target_type=str)

    over the explicit-null-guard lambda; same behaviour, no log noise.

    Input keys support dot-notation drilling, including integer list indices:
    ``calc(func, "stat.avg", "stat.obp")`` reads from nested sub-dicts.
    Any key of the form ``"a.b.0.c"`` drills through mixed dict/list structures
    using :class:`~incorporator.schema.path.DataPath`.

    For column-wide aggregation (a single call across every row) use
    :func:`calc_all` instead.
    """
    return CalcOp(func, default, target_type, list(input_keys), pure=pure)


def calc_all(
    func: Callable[..., Any],
    *input_keys: str,
    default: Any = None,
    target_type: Any = None,
    pure: bool = True,
) -> CalcAllOp:
    """Window-aggregation pass — compute a per-row value that depends on **every** row in one shot.

    Reach for this when the answer for row N requires knowing rows
    ``0..M``: market-cap rank percentile, z-score against the dataset
    mean, normalisations, dense ranks.  Contrast with :func:`calc`,
    which is invoked once per row in isolation — ``calc_all`` is
    invoked once total with the full column lists, and must return one
    value per row in input order.

    Example::

        def rank_by_score(scores):
            ranked = sorted(enumerate(scores), key=lambda p: -p[1])
            ranks = {idx: r + 1 for r, (idx, _) in enumerate(ranked)}
            return [ranks[i] for i in range(len(scores))]

        players = await Player.incorp(
            inc_url="...",
            conv_dict={"rank": calc_all(rank_by_score, "score")},
        )

    Args:
        func: Callable invoked **once total** with one positional argument
            per input key.  Each positional is a ``list`` of every row's
            value for that key.  Must return a list with one element per
            row, in the same order.
        *input_keys: Source field names.
        default: Per-row fallback when the returned list is shorter than
            the row count or contains ``None``.
        target_type: Optional coercion type applied per row.
        pure: Defaults to ``True`` — the conv_dict layer is a data-transform
            layer, and side-effect lambdas (``datetime.now()``, logging, DB
            writes, network calls) are an anti-pattern here.  Pass
            ``pure=False`` explicitly when your ``func`` must run for its side
            effects on every row, not just once per unique input.  The default
            enables the dispatcher's adaptive lru_cache wrapping for
            low-cardinality input tuples.  Note: ``calc_all`` invokes ``func``
            exactly once per dispatch with full column lists, so the cache
            wrapping does not currently fire — the flag is stored for API
            symmetry with :func:`calc` and reserved for future column-level
            optimisations.

    Returns:
        A :class:`CalcAllOp` marker — store it in ``conv_dict``.

    **Null handling.**  When every cell across every input column is
    garbage (per :func:`is_garbage_value`), ``calc_all`` short-circuits
    to ``[default] * len(rows)`` silently — no warning emitted.  ``func``
    is only invoked when at least one cell is real data; if it raises
    on that real data, the warning fires and the per-row default is
    used.  Symmetric with :func:`calc`'s row-level contract.

    Input keys support dot-notation drilling, including integer list indices:
    ``calc_all(func, "stat.avg", "stat.obp")`` reads from nested sub-dicts.
    Any key of the form ``"a.b.0.c"`` drills through mixed dict/list structures
    using :class:`~incorporator.schema.path.DataPath`.

    For per-row computation use :func:`calc` instead.
    """
    return CalcAllOp(func, default, target_type, list(input_keys), pure=pure)


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
        # ISO with compact (no-colon) timezone offset, e.g. "+0000".  Python
        # 3.11+'s fromisoformat() accepts this; 3.9/3.10 do not, so we catch
        # it via strptime+%z which is permissive across all 3.x versions.
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
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


# Values that every fallback should treat as "missing" rather than try to parse.
# Single source of truth so the DX Inspector and the runtime converter agree on
# what counts as junk.
GARBAGE_VALUES: frozenset[str] = frozenset({"unknown", "n/a", "none", "null", "undefined", "nan"})


def is_garbage_value(value: Any) -> bool:
    """Return True when ``value`` should be treated as missing.

    Matches the rejection rule baked into :func:`inc`'s ranked converter:
    ``None``, empty string, and the canonical garbage-string set
    (``"unknown"``, ``"n/a"``, ``"none"``, ``"null"``, ``"undefined"``,
    ``"nan"``) are all treated as missing data and short-circuit to the
    converter's ``default``.

    Exposed so the DX Inspector (and any future inference tooling) can
    check "would the runtime ignore this?" without re-implementing the
    rule.

    Args:
        value: The value to test.  Strings are lowercased and stripped
            before matching against the garbage set.

    Returns:
        ``True`` if the value is ``None``, empty, or a known garbage
        sentinel; ``False`` otherwise.
    """
    if value is None or value == "":
        return True
    return isinstance(value, str) and value.strip().lower() in GARBAGE_VALUES


def classify(value: Any) -> DataKind:
    """Walk the :class:`DataKind` ladder; return the most specific kind ``value`` satisfies.

    Used by ``parses_as_*`` wrappers and the inspector cascade in
    :mod:`incorporator.tools.inspector`.  Single pass replaces the
    four-predicate cascade that each wrapper used to run independently.

    Args:
        value: The raw value to classify.  May be any Python object.

    Returns:
        The most specific :class:`DataKind` that fits ``value``, from
        ``GARBAGE`` (most specific) toward ``STRING`` / ``OBJECT`` (least
        specific).
    """
    if is_garbage_value(value):
        return DataKind.GARBAGE
    if isinstance(value, bool):
        return DataKind.BOOL
    try:
        _fallback_int(value)
        return DataKind.INT
    except (TypeError, ValueError, OverflowError):
        # OverflowError: int(float("inf")) — "inf" parses as float but not int.
        pass
    try:
        _fallback_float(value)
        return DataKind.FLOAT
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        _fallback_date(value)
        return DataKind.DATETIME
    except (TypeError, ValueError):
        pass
    if isinstance(value, (dict, list)):
        return DataKind.OBJECT
    return DataKind.STRING


def parses_as_datetime(value: Any) -> bool:
    """Return ``True`` if ``value`` would parse as a ``datetime``.

    The DX Inspector calls this to decide whether to suggest
    ``inc(datetime)`` in ``conv_dict``. Routes through the same parser the
    runtime uses, so the inspector's advice is structurally aligned with
    what an actual ``incorp()`` call would accept.

    Args:
        value: The raw value to test.

    Returns:
        True if :func:`classify` categorises ``value`` as
        :attr:`DataKind.DATETIME` (i.e., the value parses as a datetime
        via the unified type ladder; garbage values short-circuit to
        False).
    """
    return classify(value) == DataKind.DATETIME


def parses_as_int(value: Any) -> bool:
    """Return ``True`` if ``value`` would parse as an ``int``.

    Mirrors :func:`parses_as_datetime` for integer coercion candidates.

    Args:
        value: The raw value to test.

    Returns:
        True if :func:`classify` categorises ``value`` as
        :attr:`DataKind.INT` (garbage values short-circuit to False).
    """
    return classify(value) == DataKind.INT


def parses_as_float(value: Any) -> bool:
    """Return ``True`` if ``value`` would parse as a ``float``.

    Mirrors :func:`parses_as_datetime` for float coercion candidates.  An
    integer value also returns ``True`` because every integer is a valid
    float — ``classify`` returns ``INT`` for ``42``, and this predicate
    accepts both ``INT`` and ``FLOAT``.

    Args:
        value: The raw value to test.

    Returns:
        True if :func:`classify` categorises ``value`` as
        :attr:`DataKind.FLOAT` OR :attr:`DataKind.INT` — integers
        satisfy the float predicate (back-compat: the pre-classify
        implementation accepted any value that ``float()`` could parse,
        which includes int-valued strings). Garbage values short-circuit
        to False.
    """
    return classify(value) in (DataKind.INT, DataKind.FLOAT)


# The Global Ranked Dictionary Engine
RANKED_CONVERTERS: dict[Any, list[Callable[[Any], Any]]] = {
    bool: [TypeAdapter(bool).validate_python, _fallback_bool],
    datetime: [TypeAdapter(datetime).validate_python, _fallback_date],
    int: [TypeAdapter(int).validate_python, _fallback_int],
    float: [TypeAdapter(float).validate_python, _fallback_float],
    str: [TypeAdapter(str).validate_python, str],
}


# ==========================================
# THE INC() FACTORY
# ==========================================
@functools.lru_cache(maxsize=4096)
def _get_cached_adapter(actual_type: Any) -> TypeAdapter[Any] | None:
    """Per-type ``TypeAdapter`` factory, memoised.

    The cache is keyed by ``actual_type`` (X | None, X | Y, list[int],
    custom Pydantic models, etc).  Cardinality is bounded by program
    structure — number of distinct types passed to ``inc()`` across the
    process lifetime — so 4096 comfortably absorbs Tideweaver topologies
    with many derived classes.  Was 128, which evicted under realistic load
    and forced a Pydantic-core rebuild on every miss.
    """
    try:
        return TypeAdapter(actual_type)
    except Exception:
        return None


@functools.lru_cache(maxsize=128)
def inc(target_type: Any, default: Any = None) -> Op:
    """Type-coercion workhorse for ``conv_dict`` — turn messy API values into clean Python types.

    Reach for ``inc(SomeType)`` whenever an API returns numeric strings,
    ISO-8601 timestamps, inconsistent boolean encodings, or the usual
    garbage-value family (``"N/A"``, ``"null"``, ``"unknown"``, empty
    string).  ``"42"`` becomes ``42``, ``"2026-05-12T14:32:00Z"`` becomes
    a ``datetime``, ``"true"`` / ``"yes"`` / ``1`` becomes ``True``, and
    garbage values silently fall through to ``default`` so the row never
    fails Pydantic validation on a single bad cell.

    Example::

        from datetime import datetime

        launches = await Launch.incorp(
            inc_url="...",
            conv_dict={
                "price": inc(float),                    # "120.5" → 120.5
                "created_at": inc(datetime),            # ISO-8601 → datetime
                "is_active": inc(bool, default=False),  # "yes" → True
            },
        )

    Args:
        target_type: The Python type to coerce values to (``int``, ``float``,
            ``bool``, ``str``, ``datetime``, or pass :data:`new` to accept
            whatever shape the API hands back).
        default: Value returned when the source is missing, empty, or a
            known garbage value; also returned when coercion raises.
            **Must be hashable** — the factory is cached on
            ``(target_type, default)`` via :func:`functools.lru_cache`
            so passing a list or dict raises ``TypeError`` at call time.
            All idiomatic defaults (``None``, ``0``, ``0.0``, ``False``,
            and strings) are hashable, so this is rarely binding.

    Returns:
        An :class:`Op` instance suitable for placing in ``conv_dict``.
        Repeated calls with the same ``(target_type, default)`` return the
        **same** instance (via :func:`functools.lru_cache`); the instance is
        referentially transparent so sharing is safe.

    Under the hood ``inc()`` builds a ranked converter chain: the
    Pydantic ``TypeAdapter`` is tried first, then a type-specific
    fallback (ISO-8601 parser for ``datetime``, comma-stripping for
    ``int`` / ``float``, truthy-string normalisation for ``bool``).
    Only when every rank raises does the warning fire and ``default``
    return.

    This layers on top of the existing per-type :func:`_get_cached_adapter`
    cache and saves the ``TypeAdapter`` rebuild cost in long-running pipelines
    that re-construct ``conv_dict`` per tick.
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
        # Instantly catch None, empties, and known API garbage.  The GARBAGE_VALUES
        # set is shared with the DX Inspector so suggestions and runtime agree.
        if is_garbage_value(val):
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

    return Op(_ranked_converter, input_keys=(), is_pure=True)


def _inc_clear_for_tests() -> None:
    """Evict every ``Op`` instance cached by the ``inc()`` factory.

    Each ``inc()`` call constructs an ``Op`` whose ``_func`` is wrapped
    in ``functools.lru_cache(maxsize=10_000)`` when ``is_pure=True``.
    That inner lru_cache lives on the Op instance.  Evicting ``inc()``'s
    outer cache drops the Op instances; held references that captured
    an Op BEFORE this call retain their populated inner cache.  Benchmarks
    asserting hit counts from zero must call this AND discard any captured
    Op references before re-calling ``inc()``.

    The wrapper hides the public ``inc.cache_clear`` surface that
    ``@functools.lru_cache`` exposes by default.
    """
    inc.cache_clear()
