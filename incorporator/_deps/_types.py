"""Category enum and DepInfo dataclass — cycle-break base for _deps subpackage."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Category(str, Enum):
    """Functional grouping for an optional dependency."""

    SPEEDUP = "speedup"
    FORMAT = "format"
    ORCHESTRATE = "orchestrate"
    PLATFORM_FIX = "platform_fix"


@dataclass(frozen=True, slots=True)
class DepInfo:
    """Metadata record for one optional dependency.

    Args:
        name: PyPI / import name of the package.
        extra: The ``[project.optional-dependencies]`` key that installs it.
        category: Functional grouping (speedup, format, orchestrate, platform_fix).
        description: One-line human summary shown by ``list_deps()``.
        version_spec: Minimum version constraint string (e.g. ``">=3.9"``).
        is_available: ``True`` when the package can be imported at runtime.
        module: The imported module object, or ``None`` when unavailable.
        platform_marker: Optional PEP 508 marker (e.g. ``"sys_platform == 'win32'"``).
        include_in_all: Whether this dep appears in the ``[all]`` extra.
    """

    name: str
    extra: str
    category: Category
    description: str
    version_spec: str
    is_available: bool
    module: Any
    platform_marker: str | None = None
    include_in_all: bool = True
