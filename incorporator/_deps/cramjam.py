"""Centralised probe + metadata for cramjam."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import cramjam  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return cramjam
    except ImportError:
        return None


CRAMJAM = _probe()

META = DepInfo(
    name="cramjam",
    extra="speedups",
    category=Category.SPEEDUP,
    description="Rust-backed compression codecs: zstd, lz4, snappy, brotli",
    version_spec=">=2.7",
    is_available=CRAMJAM is not None,
    module=CRAMJAM,
)
