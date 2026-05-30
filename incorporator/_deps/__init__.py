"""Optional-dependency registry for Incorporator.

Each sub-module (``orjson``, ``lxml``, etc.) probes for its package at import
time and exposes a module-level constant (``ORJSON``, ``LXML_ETREE``, …) that
is either the imported module or ``None``.  Callers read the constant inside
function bodies so ``monkeypatch.setattr`` fixtures are effective per-call.

Public API::

    from incorporator._deps import orjson as _orjson_mod
    # inside a function:
    orjson = _orjson_mod.ORJSON

    from incorporator._deps import list_deps, install_hint
    for info in list_deps():
        print(info.name, info.is_available)
"""

from __future__ import annotations

from ._registry import install_hint, list_deps
from ._types import Category, DepInfo

__all__ = [
    "Category",
    "DepInfo",
    "list_deps",
    "install_hint",
]
