"""Acceptance tests: the CLI's real ``tideweaver run`` seam resolves conv_dict
tokens against BOTH inflow and outflow sidecar public names.

Completes the fix started in commit 221b16c, which made
:func:`incorporator.tideweaver.config.load_watershed` union extra_names from
inflow+outflow sidecars, but left ``incorporator.cli.runners._load_pipeline_config``
— the function the real ``incorporator tideweaver run <config>.json`` command
uses (``_run_tideweaver`` -> ``_load_pipeline_config`` -> ``build_watershed``,
never ``load_watershed``) — unioning inflow only.  These tests exercise
``_load_pipeline_config`` directly: the actual CLI seam, not the Python-API
``load_watershed`` path exercised by ``tests/test_tideweaver.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from incorporator.cli.runners import _load_pipeline_config


def _write_sidecar(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def _watershed_json_body(**overrides: Any) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "chain",
        "gate_mode": "hard",
        "drain_timeout": 5,
        "currents": [
            {
                "name": "laps",
                "class": "LapData",
                "verb": "stream",
                "interval": 30,
                "incorp_params": {"conv_dict": {"name": "@upper_name"}},
            },
            {"name": "pits", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    body.update(overrides)
    return body


def test_outflow_only_conv_dict_resolves_through_cli_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An outflow-only watershed.json (no 'inflow' field) resolves a conv_dict
    token that references a PUBLIC helper defined in outflow.py, via the SAME
    entry point the ``incorporator tideweaver run`` command uses
    (``_load_pipeline_config``), not the ``load_watershed`` Python-API path.

    Before this fix, ``_load_pipeline_config`` unioned 'inflow' sidecar names
    only, so this exact shape (natural for a Tideweaver diamond with one
    outflow.py sidecar and no separate inflow.py) left "@upper_name" as an
    unresolved raw string / TokenResolutionError on the real CLI path.
    """
    monkeypatch.chdir(tmp_path)
    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "def upper_name(v):\n    return str(v).upper()\n"
        "def outflow(state):\n    return []\n",
    )
    body = _watershed_json_body(outflow="outflow.py")
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")

    resolved = _load_pipeline_config(cfg)

    [laps] = [c for c in resolved["currents"] if c["name"] == "laps"]
    fn = laps["incorp_params"]["conv_dict"]["name"]
    assert callable(fn)
    assert not isinstance(fn, str)
    assert fn("abc") == "ABC"


def test_inflow_only_conv_dict_still_resolves_through_cli_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Back-compat: an inflow-only config (the pre-existing supported shape)
    still resolves a conv_dict token against the inflow sidecar's public
    helper through ``_load_pipeline_config``, unchanged by the refactor.
    """
    monkeypatch.chdir(tmp_path)
    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
    )
    _write_sidecar(
        tmp_path / "inflow.py",
        "def upper_name(v):\n    return str(v).upper()\n",
    )
    body = _watershed_json_body(outflow="outflow.py", inflow="inflow.py")
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")

    resolved = _load_pipeline_config(cfg)

    [laps] = [c for c in resolved["currents"] if c["name"] == "laps"]
    fn = laps["incorp_params"]["conv_dict"]["name"]
    assert callable(fn)
    assert not isinstance(fn, str)
    assert fn("abc") == "ABC"


def test_builtin_token_only_conv_dict_unchanged_through_cli_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Back-compat: a config using only built-in tokens (no sidecar helper
    reference) in conv_dict resolves identically through ``_load_pipeline_config``
    — no sidecar-union regression for the common no-inflow, no-custom-token case.
    """
    monkeypatch.chdir(tmp_path)
    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
    )
    body = _watershed_json_body(outflow="outflow.py")
    body["currents"][0]["incorp_params"] = {"conv_dict": {"created_at": "inc(datetime)"}}
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")

    resolved = _load_pipeline_config(cfg)

    [laps] = [c for c in resolved["currents"] if c["name"] == "laps"]
    fn = laps["incorp_params"]["conv_dict"]["created_at"]
    assert callable(fn)
