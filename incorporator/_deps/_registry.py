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


def _extra_for_module(module_name: str) -> str:
    """Derive the install-extra for a (possibly dotted submodule) module name.

    Normalises submodule names (``"pyarrow.orc"``, ``"lxml.html"``) to their
    top-level package before looking up ``DepInfo.extra``, so every submodule
    of a registered dep resolves to the same extra as the parent package.

    Only the single dep actually requested is imported/probed here — unlike
    :func:`list_deps`, which walks every registered dep and would trigger a
    real import of each one's underlying package.

    Args:
        module_name: The module name a caller tried to import, e.g.
            ``"pyarrow"``, ``"pyarrow.orc"``, or ``"lxml.html"``.

    Returns:
        The registered ``DepInfo.extra`` for the module's top-level package,
        or ``module_name`` unchanged when the top-level package isn't a
        registered optional dependency.
    """
    top_level = module_name.split(".", 1)[0]
    try:
        mod = importlib.import_module(f"incorporator._deps.{top_level}")
    except ImportError:
        return module_name
    meta = getattr(mod, "META", None)
    if meta is None:
        return module_name
    return str(meta.extra)
