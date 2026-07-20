"""Shared test-helper utilities for the Incorporator test suite.

Not a conftest — functions here must be imported explicitly so pytest's
fixture discovery does not try to auto-use them.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

from incorporator.usercode import load_user_module


def load_sidecar(path: Path, unique_key: str) -> ModuleType:
    """Load a Python sidecar file, delegating to ``usercode.load_user_module``.

    ``unique_key`` is accepted for backward-compatibility with existing call
    sites but is otherwise unused — module identity is now governed entirely
    by :func:`incorporator.usercode.load_user_module`'s own contract:
    a session-wide cache keyed on the *resolved path* (so two calls with the
    same physical file return the SAME module object and only exec once), a
    ``__main__`` short-circuit, and an automatic ``sys.path`` insert of the
    sidecar's parent directory so a bare sibling ``import`` resolves without
    a hand-rolled ``sys.path.insert`` guard in the sidecar file.

    No two sidecar files loaded through this helper across the test suite
    currently share a physical path, so this delegation is behavior-preserving
    for existing tests. A future test file loading an already-loaded sidecar
    path will now share the cached module object (and its exec-time side
    effects fire once, not per call) — unlike the old per-key registration
    scheme, which always re-executed under a fresh key.

    Args:
        path: Absolute path to the ``.py`` file to load.
        unique_key: Unused; retained for call-site compatibility.

    Returns:
        The loaded module object.

    Raises:
        ImportError: If the file cannot be loaded as a Python module.
        FileNotFoundError: If *path* does not exist.
    """
    return load_user_module(path)
