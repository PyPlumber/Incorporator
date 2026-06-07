"""Shared test-helper utilities for the Incorporator test suite.

Not a conftest — functions here must be imported explicitly so pytest's
fixture discovery does not try to auto-use them.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_sidecar(path: Path, unique_key: str) -> ModuleType:
    """Load a Python sidecar file via importlib, registered under *unique_key*.

    Registering the module under a unique ``sys.modules`` key prevents the
    ``sys.modules['outflow']`` collision that occurs when multiple test files
    each load their own ``outflow.py`` — the second loader silently receives
    the first test's module.  Using a distinct key per sidecar (e.g.
    ``"nascar_fantasy_outflow"``, ``"mlb_pulse_outflow"``) isolates them
    completely.

    If the key is already present in ``sys.modules`` (e.g. from a previous
    test session in the same process), it is returned directly — importlib
    already cached it under the unique key.

    Args:
        path: Absolute path to the ``.py`` file to load.
        unique_key: The ``sys.modules`` registration key.  Must be globally
            unique across all sidecar loads in the test suite.

    Returns:
        The loaded module object.

    Raises:
        ImportError: If importlib cannot build a module spec from *path*.
        FileNotFoundError: If *path* does not exist.
    """
    cached = sys.modules.get(unique_key)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(unique_key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot build module spec from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[unique_key] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod
