"""Tests for Chain 3: parent_current / parent_filter forwarding from watershed.json.

Six tests covering the JSON-to-Stream/Fjord bridge and operator sigils:

1. _build_current Stream branch forwards parent_current + parent_filter.
2. _build_current Fjord branch forwards parent_currents + parent_filters.
3. resolve_tokens resolves @operator_eq in a list to operator.eq.
4. Malformed parent_filter (non-callable op) raises ValueError at _build_current time.
5. validate_watershed_config emits WARNING for unknown key; _doc_* keys are silent.
6. End-to-end: load_watershed on watershed.json produces a Stream with the AL East filter.
"""

from __future__ import annotations

import operator
import types
from pathlib import Path
from typing import Any

import pytest

from incorporator import Incorporator
from incorporator.cli.tokens import resolve_tokens
from incorporator.observability.tideweaver.config import _build_current
from incorporator.observability.tideweaver.current import Fjord, Stream


# ---------------------------------------------------------------------------
# Minimal stub Incorporator subclasses
# ---------------------------------------------------------------------------


class _ChildCls(Incorporator):
    """Stub child-current class for config tests."""


class _FjordCls(Incorporator):
    """Stub class for Fjord config tests."""


class _UpstreamAFjord(Incorporator):
    """Stub upstream class for Fjord parent test."""


def _make_mock_module(**classes: type[Incorporator]) -> types.ModuleType:
    """Build a throwaway module object exposing stub Incorporator subclasses."""
    mod = types.ModuleType("_mock_outflow")
    for name, cls in classes.items():
        setattr(mod, name, cls)
    return mod


# ---------------------------------------------------------------------------
# Test 1 — _build_current Stream branch forwards parent_current + parent_filter
# ---------------------------------------------------------------------------


def test_build_current_stream_forwards_parent_fields() -> None:
    """_build_current returns a Stream with parent_current and parent_filter populated.

    Proves that when an entry dict carries both parent_current (str) and a
    pre-resolved parent_filter callable tuple, _build_current passes them
    through to the Stream constructor without dropping either field.
    """
    mock_outflow = _make_mock_module(_ChildCls=_ChildCls)
    entry: dict[str, Any] = {
        "name": "child",
        "class": "_ChildCls",
        "verb": "stream",
        "interval": 10.0,
        "parent_current": "parent",
        "parent_filter": ("division_id", operator.eq, 201),
        "incorp_params": {"inc_url": "http://x/{}", "inc_child": "inc_code"},
    }
    result = _build_current(entry, mock_outflow, None)
    assert isinstance(result, Stream)
    assert result.parent_current == "parent"
    assert result.parent_filter == ("division_id", operator.eq, 201)


# ---------------------------------------------------------------------------
# Test 2 — _build_current Fjord branch forwards parent_currents + parent_filters
# ---------------------------------------------------------------------------


def test_build_current_fjord_forwards_parent_fields() -> None:
    """_build_current returns a Fjord with parent_currents and parent_filters populated.

    Proves that when an entry dict carries both parent_currents (list) and a
    pre-resolved parent_filters dict, _build_current passes them through to
    the Fjord constructor unchanged.
    """
    mock_outflow = _make_mock_module(_FjordCls=_FjordCls)
    entry: dict[str, Any] = {
        "name": "flush",
        "class": "_FjordCls",
        "verb": "fjord",
        "interval": 30.0,
        "parent_currents": ["upstream_a"],
        "parent_filters": {"upstream_a": ("score", operator.ge, 10)},
        "export_params": {"file_path": "out.ndjson"},
    }
    result = _build_current(entry, mock_outflow, None)
    assert isinstance(result, Fjord)
    assert result.parent_currents == ["upstream_a"]
    assert result.parent_filters == {"upstream_a": ("score", operator.ge, 10)}


# ---------------------------------------------------------------------------
# Test 3 — resolve_tokens resolves @operator_eq in a list
# ---------------------------------------------------------------------------


def test_resolve_tokens_at_operator_eq_in_list() -> None:
    """resolve_tokens resolves @operator_eq to operator.eq when embedded in a list.

    Proves that the @-sigil form works for the parent_filter pattern:
    ["division_id", "@operator_eq", 201] → ["division_id", operator.eq, 201].
    The surrounding string and integer elements must pass through unchanged.
    """
    raw = {"parent_filter": ["division_id", "@operator_eq", 201]}
    resolved = resolve_tokens(raw)
    pf = resolved["parent_filter"]
    assert isinstance(pf, list)
    assert pf[0] == "division_id"
    assert pf[1] is operator.eq
    assert pf[2] == 201


# ---------------------------------------------------------------------------
# Test 4 — malformed parent_filter raises at _build_current time
# ---------------------------------------------------------------------------


def test_build_current_rejects_non_callable_parent_filter_op() -> None:
    """_build_current raises ValueError when parent_filter tuple has a non-callable op.

    Proves that Stream._validate_parent_filter catches the broken tuple at
    construction time so config errors surface immediately rather than at
    tick time with an obscure AttributeError.
    """
    mock_outflow = _make_mock_module(_ChildCls=_ChildCls)
    entry: dict[str, Any] = {
        "name": "child",
        "class": "_ChildCls",
        "verb": "stream",
        "interval": 10.0,
        "parent_current": "parent",
        # second element is a string, not a callable — must be rejected
        "parent_filter": ("division_id", "equals", 201),
        "incorp_params": {"inc_url": "http://x/{}", "inc_child": "inc_code"},
    }
    with pytest.raises(ValueError, match="parent_filter tuple"):
        _build_current(entry, mock_outflow, None)


# ---------------------------------------------------------------------------
# Test 5 — validate_watershed_config emits WARNING for unknown keys
# ---------------------------------------------------------------------------


def test_validate_watershed_config_warns_unknown_key(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """validate_watershed_config emits a WARNING for unknown current keys; _doc_* keys are silent.

    Proves that a typo like 'paarent_current' in a current entry generates exactly
    one WARNING mentioning the key, while keys starting with '_' (comment/doc keys)
    produce no WARNING.
    """
    import logging

    from incorporator.cli.validate import validate_watershed_config

    # Write a minimal outflow sidecar so build_watershed can resolve classes.
    outflow_src = (
        "from incorporator import Incorporator\n"
        "class HeadCls(Incorporator): pass\n"
        "class TailCls(Incorporator): pass\n"
        "def outflow(state): return []\n"
    )
    (tmp_path / "outflow.py").write_text(outflow_src, encoding="utf-8")

    config: dict[str, Any] = {
        "window": {
            "start": "2026-01-01T00:00:00+00:00",
            "end": "2026-01-01T01:00:00+00:00",
        },
        "shape": "chain",
        "outflow": "outflow.py",
        "currents": [
            {
                "name": "head",
                "class": "HeadCls",
                "verb": "stream",
                "interval": 30.0,
                "paarent_current": "typo_key",
                "_doc_note_": "this comment key must not trigger a warning",
                "incorp_params": {"inc_url": "http://x", "inc_code": "id"},
            }
        ],
    }

    with caplog.at_level(logging.WARNING, logger="incorporator.cli.validate"):
        errors = validate_watershed_config(config, tmp_path)

    assert errors == [], f"Expected no errors; got: {errors}"

    warning_msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    unknown_warnings = [m for m in warning_msgs if "paarent_current" in str(m)]
    doc_warnings = [m for m in warning_msgs if "_doc_note_" in str(m)]

    assert len(unknown_warnings) == 1, f"Expected exactly one WARNING for 'paarent_current'; got: {warning_msgs}"
    assert len(doc_warnings) == 0, f"_doc_* key must not trigger WARNING; got: {doc_warnings}"


# ---------------------------------------------------------------------------
# Test 6 — end-to-end: load_watershed on watershed.json produces Stream with AL East filter
# ---------------------------------------------------------------------------


def test_load_watershed_mlb_pulse_hitting_has_al_east_filter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """load_watershed on watershed.json builds a Stream with parent_filter=operator.eq for AL East.

    Proves the full chain: JSON @operator_eq sigil → resolve_tokens → _build_current
    → Stream.parent_filter == ("division_id", operator.eq, 201) for the hitting current.

    The pulse_outflow.py sidecar is replaced by a lightweight mock module so the
    test does not depend on the real MLB outflow registering host throttles.
    """
    monkeypatch.chdir(tmp_path)

    # Build a mock outflow module that exposes the six MLB classes.
    mock_module = types.ModuleType("mock_pulse_outflow")
    for cls_name in ("MLBSchedule", "MLBAllTeam", "MLBStandings", "MLBHitting", "MLBPitching", "TeamPulseCard"):

        class _DynCls(Incorporator):
            pass

        _DynCls.__name__ = cls_name
        _DynCls.__qualname__ = cls_name
        setattr(mock_module, cls_name, _DynCls)

    def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    mock_module.outflow = outflow  # type: ignore[attr-defined]

    # Patch load_user_module in the config module to return our mock sidecar.
    from incorporator.observability.tideweaver import config as cfg_mod

    monkeypatch.setattr(cfg_mod, "load_user_module", lambda _path, **_kw: mock_module)

    watershed_json = Path(__file__).parent.parent / "examples" / "appendix" / "mlb-pulse" / "watershed.json"
    assert watershed_json.is_file(), f"watershed.json not found at {watershed_json}"

    from incorporator.observability.tideweaver.config import load_watershed

    ws = load_watershed(watershed_json)

    hitting = next((c for c in ws.currents if c.name == "hitting"), None)
    assert hitting is not None, "hitting current not found in watershed"
    assert isinstance(hitting, Stream), f"hitting must be a Stream; got {type(hitting)}"
    assert hitting.parent_current == "all_teams"
    assert isinstance(hitting.parent_filter, tuple), f"parent_filter must be a tuple; got {hitting.parent_filter!r}"
    attr, op, val = hitting.parent_filter
    assert attr == "division_id"
    assert op is operator.eq
    assert val == 201
