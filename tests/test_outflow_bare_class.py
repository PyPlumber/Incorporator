"""Regression tests for G4: bare declared output class + extra-key rows fall through to inference.

Two tests:

(a) Direct ``flush()`` call — bare class + rows with undeclared keys must yield
    instances that retain ALL row fields via ``infer_dynamic_schema``, not just
    the base three.

(b) ``_tick_fjord`` path — monkeypatched ``load_outflow_module`` returns a bare
    class + rows with undeclared keys; the derived class parked on
    ``_tideweaver_snapshot`` must retain all fields.
"""

from __future__ import annotations

import types
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from incorporator import Incorporator
from incorporator.pipeline import outflow as outflow_module_ref
from incorporator.pipeline.outflow import _BARE_CLASS_WARNED, flush
from incorporator.tideweaver import Fjord, Stream, Watershed
from incorporator.tideweaver.current import Fjord as FjordCls


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 6, 7, 12, 0, 0, tzinfo=timezone.utc)

_EXTRA_ROWS: list[dict[str, Any]] = [
    {"inc_code": "1", "inc_name": "alpha", "last_rcd": _NOW, "score": 42, "team": "Red"},
    {"inc_code": "2", "inc_name": "beta", "last_rcd": _NOW, "score": 17, "team": "Blue"},
]


class _BareOutput(Incorporator):
    """Bare declared output class — declares no fields beyond the base three."""


def _reset_bare_class() -> None:
    """Wipe registry, snapshot, and dedup-warning state for _BareOutput."""
    _BareOutput.inc_dict.clear()
    if "_tideweaver_snapshot" in _BareOutput.__dict__:
        try:
            delattr(_BareOutput, "_tideweaver_snapshot")
        except AttributeError:
            pass
    # Clear the one-time dedup guard so each test gets a fresh warning run.
    _BARE_CLASS_WARNED.discard(id(_BareOutput))


def _make_fake_outflow_module() -> types.ModuleType:
    """Build a minimal fake module exposing _BareOutput under its class name."""
    mod = types.ModuleType("_fake_outflow")
    setattr(mod, "_BareOutput", _BareOutput)
    return mod


# ---------------------------------------------------------------------------
# Part (a) — direct flush() call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_bare_class_extra_keys_uses_inference(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Bare declared output class + rows with undeclared keys falls through to inference.

    Proves that when flush() is called with a bare declared output class and
    rows carrying fields beyond the base three, the instances parked on
    _tideweaver_snapshot retain all extra fields (score, team) rather than
    being silently dropped by Pydantic's extra='ignore'.

    Before the G4 fix: _BareOutput.model_validate(row) was called, silently
    dropping 'score' and 'team'. After: infer_dynamic_schema is called, and
    the resulting instances carry all row fields.
    """
    monkeypatch.chdir(tmp_path)
    _reset_bare_class()

    fake_mod = _make_fake_outflow_module()
    rows = list(_EXTRA_ROWS)

    def outflow_fn(state: dict[str, Any]) -> list[dict[str, Any]]:
        return rows

    instances_collected: list[Any] = []
    with caplog.at_level("WARNING", logger="incorporator.pipeline.outflow"):
        async for derived_name, count, err in flush(
            outflow_fn,
            state={},
            default_output_class_name="_BareOutput",
            base_class=_BareOutput,
            export_params={},
            outflow_module=fake_mod,
        ):
            assert err is None, f"flush() raised on derived class {derived_name!r}: {err}"
            assert count == 2, f"expected 2 rows, got {count}"
            snapshot = getattr(_BareOutput, "_tideweaver_snapshot", None)
            if snapshot is None:
                # The inferred dynamic class is NOT _BareOutput itself —
                # find it via the registered class's _tideweaver_snapshot.
                # Use the instances from model_validate on the inferred class.
                pass

    # The derived class built by inference is NOT _BareOutput (it has extra
    # fields), so _tideweaver_snapshot is parked on the inferred class, not
    # _BareOutput.  Retrieve it from the inferred class via the builder cache.
    from incorporator.schema.builder import infer_dynamic_schema

    inferred_cls = infer_dynamic_schema("_BareOutput", rows, _BareOutput)
    snapshot = getattr(inferred_cls, "_tideweaver_snapshot", None)
    assert snapshot is not None, "_tideweaver_snapshot must be parked on the inferred class"
    assert len(snapshot) == 2, f"expected 2 instances, got {len(snapshot)}"

    inst0 = snapshot[0]
    assert getattr(inst0, "score", None) == 42, (
        f"'score' field dropped — bare-class data loss not fixed; inst={inst0!r}"
    )
    assert getattr(inst0, "team", None) == "Red", (
        f"'team' field dropped — bare-class data loss not fixed; inst={inst0!r}"
    )

    # The WARNING must have fired exactly once (the dedup guard keeps it once-per-class).
    warn_msgs = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("_BareOutput" in m for m in warn_msgs), (
        f"_warn_on_bare_user_class WARNING must fire when inference is triggered; got: {warn_msgs}"
    )


# ---------------------------------------------------------------------------
# Part (b) — _tick_fjord path
# ---------------------------------------------------------------------------


class _UpstreamForFjord(Incorporator):
    """Upstream Incorporator class whose snapshot the Fjord reads."""


class _BareOutputB(Incorporator):
    """Second bare declared output class for the _tick_fjord path test.

    Declared separately from _BareOutput to avoid cross-test dedup-guard
    interference.
    """


def _reset_registries_b() -> None:
    """Wipe all state touched by part (b)."""
    for cls in (_UpstreamForFjord, _BareOutputB):
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass
    _BARE_CLASS_WARNED.discard(id(_BareOutputB))


def _make_stub_scheduler_b(
    upstream_current: Stream,
    fjord_current: FjordCls,
) -> Any:
    """Minimal Tideweaver stub exposing only what _tick_fjord needs."""
    stub = MagicMock()
    by_name = {c.name: c for c in (upstream_current, fjord_current)}
    stub._currents_by_name = by_name
    stub._upstream = {fjord_current.name: [(upstream_current.name, MagicMock())]}
    stub._edge_state = {}
    stub._transitive_upstreams = MagicMock(return_value=[upstream_current.name])

    ws_stub = MagicMock()
    ws_stub.outflow = "/tmp/_unused_outflow.py"
    stub.watershed = ws_stub
    return stub


@pytest.mark.asyncio
async def test_tick_fjord_bare_class_extra_keys_retains_all_fields(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_tick_fjord path with bare declared output class + extra-key rows retains all fields.

    Proves that when Tideweaver._tick_fjord resolves a bare declared output
    class from the outflow module and rows carry undeclared keys, the
    _tideweaver_snapshot parked on the inferred class retains 'score' and
    'team' — not just the base three.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries_b()

    rows_b: list[dict[str, Any]] = [
        {"inc_code": "10", "inc_name": "gamma", "last_rcd": _NOW, "score": 99, "team": "Green"},
    ]

    fake_mod_b = types.ModuleType("_fake_outflow_b")
    setattr(fake_mod_b, "_BareOutputB", _BareOutputB)

    def outflow_fn_b(state: dict[str, Any]) -> list[dict[str, Any]]:
        return rows_b

    def stub_load_outflow(_path: Any) -> tuple[Any, Any]:
        return (outflow_fn_b, fake_mod_b)

    monkeypatch.setattr("incorporator.usercode.load_outflow_module", stub_load_outflow)
    # Clear the dedup guard in case a prior run hit it.
    _BARE_CLASS_WARNED.discard(id(_BareOutputB))

    upstream_current = Stream(
        name="up_b",
        cls=_UpstreamForFjord,
        interval=1.0,
        incorp_params={"inc_file": "x"},
    )
    fjord_current = Fjord(
        name="fjord_b",
        cls=_BareOutputB,
        interval=1.0,
        export_params={},
    )

    scheduler = _make_stub_scheduler_b(upstream_current, fjord_current)
    from incorporator.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord_current)

    # The inferred class (not _BareOutputB itself) holds the snapshot.
    from incorporator.schema.builder import infer_dynamic_schema

    inferred_cls = infer_dynamic_schema("_BareOutputB", rows_b, _BareOutputB)
    snapshot = getattr(inferred_cls, "_tideweaver_snapshot", None)
    assert snapshot is not None, (
        "_tideweaver_snapshot must be parked on the inferred class after _tick_fjord"
    )
    assert len(snapshot) == 1, f"expected 1 instance, got {len(snapshot)}"

    inst = snapshot[0]
    assert getattr(inst, "score", None) == 99, (
        f"'score' dropped by bare-class path in _tick_fjord; inst={inst!r}"
    )
    assert getattr(inst, "team", None) == "Green", (
        f"'team' dropped by bare-class path in _tick_fjord; inst={inst!r}"
    )
