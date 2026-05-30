"""Centralised probe + metadata for typer."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import typer  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return typer
    except ImportError:
        return None


TYPER = _probe()

META = DepInfo(
    name="typer",
    extra="orchestrate",
    category=Category.ORCHESTRATE,
    description="CLI framework for the ``incorporator`` command (Click-based)",
    version_spec=">=0.9.0",
    is_available=TYPER is not None,
    module=TYPER,
)
