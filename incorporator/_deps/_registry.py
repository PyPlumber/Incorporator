"""Aggregation functions for the _deps subpackage.

``list_deps()`` dynamically imports each ``_deps/<dep>.py`` module inside the
function body to prevent circular imports at module-import time. Each dep
module imports ONLY from ``_types``; ``_registry`` is the only consumer that
touches all dep modules.
"""

from __future__ import annotations

import importlib

from ._types import DepInfo

# Names must match the ``_deps/<name>.py`` filenames exactly.
_DEP_MODULE_NAMES: list[str] = [
    "orjson",
    "lxml",
    "cramjam",
    "fastavro",
    "pyarrow",
    "openpyxl",
    "typer",
    "prefect",
    "tzdata",
]


def list_deps() -> list[DepInfo]:
    """Return a ``DepInfo`` entry for every registered optional dependency.

    Modules are imported lazily inside this function so the registry never
    creates a cycle at package-import time.

    Returns:
        List of :class:`~incorporator._deps._types.DepInfo` objects, one per
        registered optional dependency, in declaration order.
    """
    result: list[DepInfo] = []
    for name in _DEP_MODULE_NAMES:
        mod = importlib.import_module(f"incorporator._deps.{name}")
        result.append(mod.META)
    return result


def install_hint(dep_name: str) -> str:
    """Return a ``pip install`` hint string for the named dependency.

    Args:
        dep_name: The PyPI / import name of the package (e.g. ``"orjson"``).

    Returns:
        A human-readable install hint such as
        ``"pip install incorporator[speedups]"``, or a generic hint when the
        dependency is not registered.
    """
    for info in list_deps():
        if info.name == dep_name:
            return f"pip install incorporator[{info.extra}]"
    return f"pip install {dep_name}"
