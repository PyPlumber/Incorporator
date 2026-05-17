"""Tests for the IncorporatorList pass-through fast path in ``flush()``.

When ``outflow(state)`` returns the live :class:`IncorporatorList` of the
derived class, the engine MUST NOT clear+rebuild ``cls.inc_dict``.  Doing so
would erase the instances that ``_refresh_daemon`` mutates in place — a
silent identity regression that breaks anything holding refs into the
registry between waves (most notably the stateful-stream shim).
"""

from typing import Any, Dict

import pytest

from incorporator import Incorporator
from incorporator.list import IncorporatorList
from incorporator.observability.pipeline._outflow import flush


class _PassthroughTarget(Incorporator):
    inc_code: Any = None
    name: str = ""


@pytest.mark.asyncio
async def test_flush_passthrough_preserves_instance_identity(tmp_path: Any) -> None:
    """outflow returning the live IncorporatorList must reuse existing instances.

    Pre-fix: ``flush()`` cleared inc_dict and re-materialised from rows
    every export tick — even when the rows WERE the live instances.  This
    broke the stateful-stream contract (refresh mutates in place; export
    sees the same Python objects) when single-source-stateful was routed
    through fjord.
    """
    out_file = tmp_path / "snapshot.ndjson"

    # Seed two instances directly into the class registry.
    inst_a = _PassthroughTarget(inc_code="a", name="Alice")
    inst_b = _PassthroughTarget(inc_code="b", name="Bob")
    live_list: IncorporatorList[_PassthroughTarget] = IncorporatorList(_PassthroughTarget, [inst_a, inst_b])

    id_a_before = id(_PassthroughTarget.inc_dict["a"])
    id_b_before = id(_PassthroughTarget.inc_dict["b"])

    # outflow returns the same live IncorporatorList — pure pass-through.
    def _outflow(state: Dict[str, Any]) -> Dict[str, IncorporatorList[_PassthroughTarget]]:
        return {"_PassthroughTarget": state["_PassthroughTarget"]}

    # outflow_module exposes the pre-declared class so flush() reuses it
    # instead of inferring a dynamic schema.
    import types as _types

    module = _types.ModuleType("_test_module")
    setattr(module, "_PassthroughTarget", _PassthroughTarget)

    results = []
    async for derived_name, count, err in flush(
        _outflow,
        {"_PassthroughTarget": live_list},
        default_output_class_name="_PassthroughTarget",
        base_class=Incorporator,
        export_params={"file_path": str(out_file)},
        outflow_module=module,
    ):
        results.append((derived_name, count, err))

    assert results == [("_PassthroughTarget", 2, None)]
    # The fast path must NOT clear+rebuild — the registry entries must be
    # the exact same Python objects from before the flush.
    assert id(_PassthroughTarget.inc_dict["a"]) == id_a_before
    assert id(_PassthroughTarget.inc_dict["b"]) == id_b_before
    # And the export must have actually happened.
    assert out_file.exists()


@pytest.mark.asyncio
async def test_flush_rebuilds_on_non_passthrough_return(tmp_path: Any) -> None:
    """outflow returning row dicts (legacy path) still rebuilds, as expected."""
    out_file = tmp_path / "rebuilt.ndjson"

    # Seed registry first.
    _PassthroughTarget(inc_code="a", name="Alice")

    def _outflow(state: Dict[str, Any]) -> list:
        # Plain list of dicts — the legacy contract; rebuild expected.
        return [{"inc_code": "z", "name": "Zelda"}]

    import types as _types

    module = _types.ModuleType("_test_module")
    setattr(module, "_PassthroughTarget", _PassthroughTarget)

    async for derived_name, count, err in flush(
        _outflow,
        {"_PassthroughTarget": []},
        default_output_class_name="_PassthroughTarget",
        base_class=Incorporator,
        export_params={"file_path": str(out_file)},
        outflow_module=module,
    ):
        assert derived_name == "_PassthroughTarget"
        assert count == 1
        assert err is None

    # Old keys are gone (registry rebuild semantics for the non-passthrough path).
    assert "a" not in _PassthroughTarget.inc_dict
    assert "z" in _PassthroughTarget.inc_dict
