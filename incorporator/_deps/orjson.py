"""Centralised probe + metadata for orjson."""

from __future__ import annotations

import json as _stdlib_json
from typing import Any, cast

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

        return orjson
    except ImportError:
        return None


ORJSON = _probe()

META = DepInfo(
    name="orjson",
    extra="speedups",
    category=Category.SPEEDUP,
    description="Fast JSON serialiser/deserialiser (Rust-backed, GIL-free)",
    version_spec=">=3.9",
    is_available=ORJSON is not None,
    module=ORJSON,
)


def dumps_str(obj: Any) -> str:
    """Serialise *obj* to a JSON string, preferring orjson when available.

    Args:
        obj: Any JSON-serialisable Python object.

    Returns:
        A JSON-encoded string.
    """
    if ORJSON is not None:
        return cast(bytes, ORJSON.dumps(obj)).decode("utf-8")
    return _stdlib_json.dumps(obj)


def dumps_bytes(obj: Any, *, indent: int = 0) -> bytes:
    """Serialise *obj* to JSON bytes, preferring orjson when available.

    Args:
        obj: Any JSON-serialisable Python object.
        indent: Indentation level. orjson only supports 2; stdlib accepts any int.

    Returns:
        UTF-8–encoded JSON bytes.
    """
    if ORJSON is not None:
        opt = ORJSON.OPT_INDENT_2 if indent else 0
        return cast(bytes, ORJSON.dumps(obj, option=opt))
    return _stdlib_json.dumps(obj, indent=indent or None).encode("utf-8")


def loads(raw: bytes | str) -> Any:
    """Decode a JSON document, preferring orjson when available.

    Args:
        raw: A JSON-encoded ``bytes`` or ``str`` value.

    Returns:
        The decoded Python object (dict, list, str, int, float, bool, or None).
    """
    if ORJSON is not None:
        return ORJSON.loads(raw)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return _stdlib_json.loads(raw)
