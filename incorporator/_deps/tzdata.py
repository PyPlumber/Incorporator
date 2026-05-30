"""Registry-only metadata for tzdata (Windows ORC compat shim)."""

from __future__ import annotations

import importlib.util
import sys

from ._types import Category, DepInfo

_IS_WINDOWS = sys.platform == "win32"
_FOUND = importlib.util.find_spec("tzdata") is not None if _IS_WINDOWS else False

META = DepInfo(
    name="tzdata",
    extra="parquet",
    category=Category.PLATFORM_FIX,
    description="Windows ORC reader compat (provides /usr/share/zoneinfo)",
    version_spec=">=2024.1",
    is_available=_FOUND,
    module=None,
    platform_marker="sys_platform == 'win32'",
    include_in_all=False,
)
