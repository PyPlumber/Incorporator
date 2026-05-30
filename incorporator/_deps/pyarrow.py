"""Centralised probe + metadata for pyarrow."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import pyarrow  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return pyarrow
    except ImportError:
        return None


PYARROW = _probe()

META = DepInfo(
    name="pyarrow",
    extra="parquet",
    category=Category.FORMAT,
    description="Parquet / ORC / Feather columnar I/O (~30 MB wheel)",
    version_spec=">=14.0",
    is_available=PYARROW is not None,
    module=PYARROW,
    include_in_all=False,
)
