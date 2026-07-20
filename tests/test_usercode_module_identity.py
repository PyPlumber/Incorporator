"""Module-identity guarantees of `usercode.load_user_module`.

Covers the two concerns fixed at the loader layer:

* MODULE IDENTITY — the same physical ``.py`` file resolves to the SAME
  module object (and therefore the SAME class objects) no matter which
  framework entry point loads it, and shares identity with a user's direct
  ``python entry.py`` run (the ``__main__`` path).
* SIDECAR-RELATIVE IMPORTS — a sidecar loaded through the framework can
  import sibling modules from its own directory without a hand-written
  ``sys.path.insert(0, str(Path(__file__).parent))`` guard.

``sys.path``/``sys.modules`` mutation is process-global, so every test here
runs under an autouse fixture that snapshots and restores both around each
test — otherwise a module registered by one test (or a swapped
``sys.modules["__main__"]``) would leak into later tests in the same
process.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from incorporator.usercode import load_outflow_module, load_user_module


@pytest.fixture(autouse=True)
def _isolate_sys_state() -> Iterator[None]:
    """Snapshot/restore ``sys.path`` and ``sys.modules`` around each test.

    ``load_user_module`` permanently inserts a sidecar's parent directory
    into ``sys.path`` and registers modules under path-derived
    ``sys.modules`` keys — both process-global side effects that must not
    leak between tests (or between this file and the rest of the suite).
    """
    original_path = list(sys.path)
    original_modules = dict(sys.modules)
    original_main = sys.modules.get("__main__")
    yield
    sys.path[:] = original_path
    if original_main is not None:
        sys.modules["__main__"] = original_main
    for name in list(sys.modules):
        if name not in original_modules:
            del sys.modules[name]


def test_same_file_multiple_entry_points_share_one_module_object(tmp_path: Path) -> None:
    """Two different framework entry points loading the same file get the IDENTICAL module.

    Also proves the file's top level only executes once (``IMPORT_COUNT``
    stays at 1) even though it's loaded via both `load_user_module` and
    `load_outflow_module`.
    """
    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "IMPORT_COUNT = globals().get('IMPORT_COUNT', 0) + 1\n\n"
        "class Marker:\n"
        "    pass\n\n"
        "def outflow(state):\n"
        "    return {}\n",
        encoding="utf-8",
    )

    module_direct = load_user_module(outflow_py)
    _, module_via_loader = load_outflow_module(outflow_py)

    assert module_direct is module_via_loader
    assert module_direct.Marker is module_via_loader.Marker
    assert module_direct.IMPORT_COUNT == 1


def test_main_entry_point_shares_class_identity_with_framework_load(tmp_path: Path) -> None:
    """A file already running as `__main__` shares class objects with a by-path framework load.

    Simulates a direct `python entry.py` run by manually registering a
    module under `sys.modules["__main__"]`, then confirms
    `load_user_module` returns that SAME module (not a second exec) when
    asked to load the identical path.
    """
    entry_py = tmp_path / "entry.py"
    entry_py.write_text("class Widget:\n    pass\n", encoding="utf-8")

    spec = importlib.util.spec_from_file_location("__main__", entry_py)
    assert spec is not None and spec.loader is not None
    main_module = importlib.util.module_from_spec(spec)
    sys.modules["__main__"] = main_module
    spec.loader.exec_module(main_module)

    loaded = load_user_module(entry_py)

    assert loaded is main_module
    assert loaded.Widget is main_module.Widget


def test_sidecar_imports_sibling_without_manual_syspath_guard(tmp_path: Path) -> None:
    """A sidecar's bare top-level `import sibling` resolves with no `sys.path` guard in the file.

    Proves the loader auto-inserts the sidecar's own directory so hand-
    written `sys.path.insert(0, str(Path(__file__).parent))` guards in
    example sidecars become harmless no-ops rather than required scaffolding.
    """
    sibling_py = tmp_path / "sibling.py"
    sibling_py.write_text("VALUE = 42\n", encoding="utf-8")

    sidecar_py = tmp_path / "sidecar.py"
    sidecar_py.write_text(
        "import sibling\n\ndef outflow(state):\n    return {'value': sibling.VALUE}\n",
        encoding="utf-8",
    )

    module = load_user_module(sidecar_py)

    assert module.sibling.VALUE == 42
