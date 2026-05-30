"""Centralised probe + metadata for lxml."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import lxml.etree as lxml_etree  # type: ignore[import-untyped, import-not-found, unused-ignore]

        return lxml_etree
    except ImportError:
        return None


LXML_ETREE = _probe()

META = DepInfo(
    name="lxml",
    extra="speedups",
    category=Category.SPEEDUP,
    description="Fast XML/HTML parser with XXE-safe defaults (libxml2-backed)",
    version_spec=">=4.9",
    is_available=LXML_ETREE is not None,
    module=LXML_ETREE,
)
