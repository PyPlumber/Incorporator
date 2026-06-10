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
from incorporator.pipeline.outflow import flush


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


def test_normalise_single_key_dict_matching_default_is_single_output() -> None:
    """Identity-outflow shape from the stateful-stream shim is morally single-output.

    ``stream(stateful_polling=True)`` synthesises an identity outflow that
    returns ``{cls_name: state[cls_name]}`` — a single-key dict whose key
    matches the default output class name.  Treating this as multi-output
    would emit a spurious "multi-output dict but export_params is single-
    output" warning at ``_resolve_export_params_for`` when the caller's
    ``export_params`` (correctly) uses the single-shape ``file_path`` form.

    The fix in ``_normalise_outflow_return`` returns ``is_multi=False`` for
    this degenerate shape so the warning fires only for genuine multi-output.
    """
    from incorporator.pipeline.outflow import _normalise_outflow_return

    grouped, is_multi = _normalise_outflow_return(
        {"BinancePair": [{"id": 1}, {"id": 2}]},
        default_class_name="BinancePair",
    )
    assert grouped == {"BinancePair": [{"id": 1}, {"id": 2}]}
    assert is_multi is False


def test_normalise_multi_key_dict_is_multi_output() -> None:
    """Genuine multi-output (two class keys) still flags is_multi=True."""
    from incorporator.pipeline.outflow import _normalise_outflow_return

    _, is_multi = _normalise_outflow_return(
        {"FantasyTeam": [{"id": 1}], "Manufacturer": [{"id": 2}]},
        default_class_name="FantasyTeam",
    )
    assert is_multi is True


def test_normalise_single_key_dict_not_matching_default_is_multi_output() -> None:
    """Single-key dict whose key DOESN'T match the default class name is still
    a multi-output config (the user picked an unexpected name).  Don't suppress
    the warning in that case — it correctly flags the mismatch."""
    from incorporator.pipeline.outflow import _normalise_outflow_return

    _, is_multi = _normalise_outflow_return(
        {"UnexpectedClass": [{"id": 1}]},
        default_class_name="ExpectedClass",
    )
    assert is_multi is True


# ----------------------------------------------------------------------
# _warn_on_bare_user_class — silent-data-loss diagnostic
# ----------------------------------------------------------------------


def test_bare_user_class_warns_on_dropped_fields(caplog: pytest.LogCaptureFixture) -> None:
    """A bare class declaration whose row carries unknown fields gets one WARNING.

    ``class BareRace(Incorporator): pass`` declares zero new fields.  With
    Pydantic V2's default extra='ignore', a row like {"id": 1, "name": "X"}
    has every non-base field silently dropped on model_validate.  The warning
    surfaces the data loss with a concrete fix suggestion.  Subsequent calls
    with the SAME class are suppressed (one-time-per-class dedup).
    """
    import logging

    from incorporator.pipeline.outflow import (
        _BARE_CLASS_WARNED,
        _warn_on_bare_user_class,
    )

    class BareRace(Incorporator):  # bare — no extra fields
        pass

    # Belt-and-suspenders: clear the dedup set in case another test ran first.
    _BARE_CLASS_WARNED.discard(id(BareRace))

    caplog.set_level(logging.WARNING, logger="incorporator.pipeline.outflow")
    _warn_on_bare_user_class(BareRace, Incorporator, {"id": 1, "name": "Alice", "speed": 200})

    matching = [r for r in caplog.records if "BareRace" in r.getMessage()]
    assert len(matching) == 1, f"expected 1 warning, got {len(matching)}: {[r.getMessage() for r in caplog.records]}"
    assert "silently dropped" in matching[0].getMessage()
    assert "name" in matching[0].getMessage()
    assert "speed" in matching[0].getMessage()

    # Second call with the same class is suppressed.
    caplog.clear()
    _warn_on_bare_user_class(BareRace, Incorporator, {"id": 2, "name": "Bob"})
    matching = [r for r in caplog.records if "BareRace" in r.getMessage()]
    assert matching == []


def test_user_class_with_fields_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    """A user class that declares fields beyond the base does NOT trigger the warning."""
    import logging

    from incorporator.pipeline.outflow import _warn_on_bare_user_class

    class TypedRace(Incorporator):
        name: str = ""  # one explicit field — user opted in to inference suppression

    caplog.set_level(logging.WARNING, logger="incorporator.pipeline.outflow")
    _warn_on_bare_user_class(TypedRace, Incorporator, {"name": "Alice", "speed": 200})

    matching = [r for r in caplog.records if "TypedRace" in r.getMessage()]
    assert matching == []


def test_bare_class_with_extra_allow_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    """A bare class that opts into extra='allow' is safe — fields land in __pydantic_extra__."""
    import logging

    from pydantic import ConfigDict

    from incorporator.pipeline.outflow import _warn_on_bare_user_class

    class ExtraAllowRace(Incorporator):
        model_config = ConfigDict(extra="allow")

    caplog.set_level(logging.WARNING, logger="incorporator.pipeline.outflow")
    _warn_on_bare_user_class(ExtraAllowRace, Incorporator, {"id": 1, "name": "Alice", "speed": 200})

    matching = [r for r in caplog.records if "ExtraAllowRace" in r.getMessage()]
    assert matching == []
