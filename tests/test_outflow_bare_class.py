"""Regression tests: bare declared output class + extra-key rows fall through to inference.

flush() infers the dynamic class using the user's own declared bare class as the
inference base (not the fjord's generic ``base_class``) — so the built instances
register into the user's class registry, exactly like ``incorp()`` on a bare
source class.  Covers:

(a) Direct ``flush()`` call with ``base_class=Incorporator`` (the real
    ``Incorporator.fjord()`` shape) — bare class + rows with undeclared keys must
    yield instances retaining ALL row fields, reachable via
    ``UserCls.inc_dict``, with NO warning fired.

(b) ``_tick_fjord`` path — monkeypatched ``load_outflow_module`` returns a bare
    class + rows with undeclared keys; the derived class parked on
    ``_tideweaver_snapshot`` must retain all fields (this path already passed
    ``base_class=current.cls``, i.e. the user's own class, so it was correct
    before the fix too — kept as a no-regression check).

(c) Regression: declared-fields class + dict rows — extras still dropped per
    Pydantic's default ``extra='ignore'``.

(d) Regression: instance-row flow through a bare class — unchanged;
    ``infer_dynamic_schema`` only samples dict rows.

The one-shot ``Incorporator.fjord()`` DX check (a bare declared output class
whose ``inc_dict`` is populated after the engine completes) lives in
``tests/test_fjord.py`` (``test_fjord_bare_declared_output_class_populates_own_inc_dict``)
since it reuses that file's existing mocked-HTTP fjord-engine harness rather
than inventing a new one here.
"""

from __future__ import annotations

import types
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from incorporator import Incorporator
from incorporator.list import IncorporatorList
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

    Uses ``base_class=Incorporator`` — the real shape ``Incorporator.fjord()``
    passes into ``flush()`` — so this reproduces the production defect rather
    than the coincidentally-correct Tideweaver ``_tick_fjord`` shape (part b).

    Proves that when flush() is called with a bare declared output class and
    rows carrying fields beyond the base three, the built instances retain all
    extra fields (score, team) AND are reachable via the user's own class
    registry (``_BareOutput.inc_dict``) — not just a sibling inferred class
    unrelated to the user's declaration.  No warning fires: nothing was
    silently dropped.
    """
    monkeypatch.chdir(tmp_path)
    _reset_bare_class()

    fake_mod = _make_fake_outflow_module()
    rows = list(_EXTRA_ROWS)

    def outflow_fn(state: dict[str, Any]) -> list[dict[str, Any]]:
        return rows

    with caplog.at_level("WARNING", logger="incorporator.pipeline.outflow"):
        async for derived_name, count, err in flush(
            outflow_fn,
            state={},
            default_output_class_name="_BareOutput",
            base_class=Incorporator,
            export_params={},
            outflow_module=fake_mod,
        ):
            assert err is None, f"flush() raised on derived class {derived_name!r}: {err}"
            assert count == 2, f"expected 2 rows, got {count}"

    # The instances must be reachable via the user's own declared class —
    # this is the whole point of the fix: infer with user_cls as the base
    # so the base-registration block forks instances into _BareOutput.inc_dict.
    values = list(_BareOutput.inc_dict.values())
    assert len(values) == 2, f"expected 2 instances registered on _BareOutput.inc_dict, got {len(values)}"

    inst0 = next(v for v in values if v.inc_code == "1")
    assert getattr(inst0, "score", None) == 42, (
        f"'score' field dropped — bare-class data loss not fixed; inst={inst0!r}"
    )
    assert getattr(inst0, "team", None) == "Red", (
        f"'team' field dropped — bare-class data loss not fixed; inst={inst0!r}"
    )

    # No _warn_on_bare_user_class WARNING: inference keeps every row field, so
    # nothing is silently dropped.  (An unrelated "no matching export_params
    # key" warning DOES fire here since export_params={} — filter for the
    # bare-class-specific message, not just any warning mentioning the name.)
    warn_msgs = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert not any("silently dropped" in m for m in warn_msgs), (
        f"no _warn_on_bare_user_class WARNING should fire once inference preserves all fields; got: {warn_msgs}"
    )
    assert id(_BareOutput) not in _BARE_CLASS_WARNED


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
    assert snapshot is not None, "_tideweaver_snapshot must be parked on the inferred class after _tick_fjord"
    assert len(snapshot) == 1, f"expected 1 instance, got {len(snapshot)}"

    inst = snapshot[0]
    assert getattr(inst, "score", None) == 99, f"'score' dropped by bare-class path in _tick_fjord; inst={inst!r}"
    assert getattr(inst, "team", None) == "Green", f"'team' dropped by bare-class path in _tick_fjord; inst={inst!r}"

    # base_class == user_cls on this path already (current.cls), so the
    # base-registration block was already firing pre-fix -- verify it still
    # does.
    values_b = list(_BareOutputB.inc_dict.values())
    assert len(values_b) == 1, f"expected 1 instance registered on _BareOutputB.inc_dict, got {len(values_b)}"


# ---------------------------------------------------------------------------
# Part (c) — regression: declared-fields class + dict rows unchanged
# ---------------------------------------------------------------------------


class _DeclaredOutput(Incorporator):
    """Non-bare declared output class -- has one real field beyond the base three."""

    score: int = 0


@pytest.mark.asyncio
async def test_flush_declared_fields_class_still_drops_undeclared_extras(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A declared-fields class (extra_fields non-empty) is unaffected by the bare-class fix.

    ``score`` is declared explicitly, so flush() uses ``_DeclaredOutput`` directly
    for ``model_validate`` (the ``if extra_fields or allows_extra:`` arm, not the
    bare-class inference arm this pass touches).  Pydantic's default
    ``extra='ignore'`` still drops any row key the class didn't declare.
    """
    monkeypatch.chdir(tmp_path)
    _DeclaredOutput.inc_dict.clear()

    fake_mod = types.ModuleType("_fake_outflow_declared")
    setattr(fake_mod, "_DeclaredOutput", _DeclaredOutput)

    def outflow_fn(state: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"inc_code": "1", "score": 10, "extra_field": "drop-me"}]

    async for derived_name, count, err in flush(
        outflow_fn,
        state={},
        default_output_class_name="_DeclaredOutput",
        base_class=Incorporator,
        export_params={},
        outflow_module=fake_mod,
    ):
        assert err is None, f"flush() raised on derived class {derived_name!r}: {err}"
        assert count == 1

    inst = _DeclaredOutput.inc_dict["1"]
    assert inst.score == 10
    assert not hasattr(inst, "extra_field"), "declared-fields class must still drop undeclared row keys"


# ---------------------------------------------------------------------------
# Part (d) — regression: instance-row path through a bare class is unchanged
# ---------------------------------------------------------------------------


class _BareReceiver(Incorporator):
    """Bare declared output class fed instance rows (not dicts)."""


class _SourceModel(Incorporator):
    """Stand-in for a source class whose instances flow through as 'rows'.

    ``extra='allow'`` so extra fields set at construction time survive on the
    instance for ``model_dump()`` to see -- mirrors a source class already
    built via inference during ``incorp()``/``refresh()``.
    """

    model_config = {"extra": "allow"}


@pytest.mark.asyncio
async def test_flush_instance_rows_through_bare_class_still_fails(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Instance-row (non-dict) flow through a bare declared class is unaffected by this fix.

    Mirrors the documented crash in ``examples/08-streaming-daemon/outflow.py``:
    a bare declared receiver class fed pre-built model instances (rather than
    dict rows) from a mismatched class still fails at ``model_validate`` --
    ``infer_dynamic_schema``'s sampler only reads ``dict`` rows (see
    ``incorporator/schema/builder.py``), so instance rows contribute nothing
    to the inferred schema regardless of which base class inference uses.
    This pass changes ONLY the inference base for dict rows; instance-row
    behavior is untouched.
    """
    monkeypatch.chdir(tmp_path)
    _BareReceiver.inc_dict.clear()
    _BARE_CLASS_WARNED.discard(id(_BareReceiver))

    fake_mod = types.ModuleType("_fake_outflow_instance_rows")
    setattr(fake_mod, "_BareReceiver", _BareReceiver)

    src_inst = _SourceModel(inc_code="1", inc_name="a", last_rcd=_NOW, score=42, team="Red")
    rows = IncorporatorList(_SourceModel, [src_inst])

    def outflow_fn(state: dict[str, Any]) -> dict[str, Any]:
        return {"_BareReceiver": rows}

    async for derived_name, count, err in flush(
        outflow_fn,
        state={},
        default_output_class_name="_BareReceiver",
        base_class=Incorporator,
        export_params={},
        outflow_module=fake_mod,
    ):
        assert derived_name == "_BareReceiver"
        assert count == 0
        assert err is not None, "instance rows through a bare class must still fail, unchanged by this pass"
        assert "validation error" in str(err).lower()
