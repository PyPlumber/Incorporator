"""Centralised probe + metadata for openpyxl."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import openpyxl  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return openpyxl
    except ImportError:
        return None


OPENPYXL = _probe()

META = DepInfo(
    name="openpyxl",
    extra="xlsx",
    category=Category.FORMAT,
    description="Read/write Excel .xlsx workbooks (pure Python)",
    version_spec=">=3.1",
    is_available=OPENPYXL is not None,
    module=OPENPYXL,
)
