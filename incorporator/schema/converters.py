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
    """Compute one field's value per row from one or more source fields.

    Drop the return value into a ``conv_dict`` entry; the framework calls
    ``func`` once per row, passing the named source fields as positional
    arguments::

        def full_name(first, last):
            return f"{first} {last}"

        users = await User.incorp(
            inc_url="...",
            conv_dict={
                "name": calc(full_name, "first_name", "last_name"),
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

    Returns:
        A :class:`CalcOp` marker — store it in ``conv_dict``; the engine
        unwraps it during instance construction.

    For column-wide aggregation (a single call across every row) use
    :func:`calc_all` instead.
    """
    return CalcOp(func, default, target_type, list(input_keys))


def calc_all(func: Callable[..., Any], *input_keys: str, default: Any = None, target_type: Any = None) -> CalcAllOp:
    """Compute one field's value from the **entire column** in a single call.

    Like :func:`calc` but ``func`` is invoked **once** with the full list
    of values across every row — use for window aggregations, ranking,
    or any reduction that needs the whole column::

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

    Returns:
        A :class:`CalcAllOp` marker — store it in ``conv_dict``.

    For per-row computation use :func:`calc` instead.
    """
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
    """
    if value is None or value == "":
        return True
    return isinstance(value, str) and value.strip().lower() in GARBAGE_VALUES


def parses_as_datetime(value: Any) -> bool:
    """Return True if :func:`_fallback_date` would successfully parse ``value``.

    The DX Inspector calls this to decide whether to suggest
    ``inc(datetime)`` in ``conv_dict``. Routes through the same parser the
    runtime uses, so the inspector's advice is structurally aligned with
    what an actual ``incorp()`` call would accept.
    """
    if is_garbage_value(value):
        return False
    try:
        _fallback_date(value)
        return True
    except Exception:
        return False


def parses_as_int(value: Any) -> bool:
    """Return True if :func:`_fallback_int` would successfully parse ``value``.

    Mirrors :func:`parses_as_datetime` for integer coercion candidates.
    """
    if is_garbage_value(value):
        return False
    try:
        _fallback_int(value)
        return True
    except Exception:
        return False


def parses_as_float(value: Any) -> bool:
    """Return True if :func:`_fallback_float` would successfully parse ``value``.

    Mirrors :func:`parses_as_datetime` for float coercion candidates.
    """
    if is_garbage_value(value):
        return False
    try:
        _fallback_float(value)
        return True
    except Exception:
        return False


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
    """Coerce a raw API value into a Python type — the workhorse of ``conv_dict``.

    Use ``inc(SomeType)`` in :meth:`Incorporator.incorp`'s ``conv_dict`` to
    convert every value of that field to ``SomeType`` before Pydantic sees
    it.  Handles the common pain points of messy API payloads:

    - Strings that are really numbers (``"42"`` → ``42``).
    - ISO-8601 timestamps (``"2026-05-12T14:32:00Z"`` → ``datetime``).
    - Boolean-shaped strings (``"yes"``, ``"true"``, ``1`` → ``True``).
    - The standard garbage-value family (``"N/A"``, ``"null"``,
      ``"unknown"``, empty string) silently becomes ``default``.

    Example::

        from datetime import datetime

        launches = await Launch.incorp(
            inc_url="...",
            conv_dict={
                "net": inc(datetime),          # ISO-8601 → datetime
                "altitude_m": inc(float),      # "120.5" → 120.5
                "is_recovered": inc(bool, default=False),
            },
        )

    Args:
        target_type: The Python type to coerce values to (``int``, ``float``,
            ``bool``, ``str``, ``datetime``, or pass :data:`new` to accept
            whatever shape the API hands back).
        default: Value returned when the source is missing, empty, or a
            known garbage value; also returned when coercion raises.

    Returns:
        A converter closure suitable for placing in ``conv_dict``.
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

    return _ranked_converter
