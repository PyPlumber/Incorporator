"""Centralised probe + metadata for prefect."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import prefect  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return prefect
    except ImportError:
        return None


PREFECT = _probe()

META = DepInfo(
    name="prefect",
    extra="orchestrate",
    category=Category.ORCHESTRATE,
    description="Workflow orchestration engine (@flow / @task decorators)",
    version_spec=">=2.10.0",
    is_available=PREFECT is not None,
    module=PREFECT,
)
