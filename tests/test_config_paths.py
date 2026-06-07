"""Unit tests for incorporator.io.config_paths.

Covers:
- resolve_config_paths: INPUT fields become config-dir-relative; absolute
  paths are untouched; OUTPUT fields (file_path, archive_target) are
  unchanged; URLs (inc_url, new_url) are unchanged; nested
  stream_params[].incorp_params.inc_file and refresh_params.new_file are
  rebased; watershed current entries (head/tail/source/middle/sinks/
  currents) are rebased; already-absolute inputs are idempotent.
- resolve_output_path: returns a resolved Path and auto-creates parent dirs.
- G2 regression: a config with a relative inc_file resolves correctly
  when the CWD is different from the config file's directory.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from incorporator.io.config_paths import resolve_config_paths, resolve_output_path


# ---------------------------------------------------------------------------
# resolve_config_paths — top-level fields
# ---------------------------------------------------------------------------


def test_resolve_config_paths_rebases_relative_inflow(tmp_path: Path) -> None:
    """Relative 'inflow' field resolves to config-dir, not CWD."""
    config = {"inflow": "inflow.py", "incorp_params": {"inc_url": "https://x"}}
    result = resolve_config_paths(config, tmp_path)
    expected = str((tmp_path / "inflow.py").resolve())
    assert result["inflow"] == expected


def test_resolve_config_paths_rebases_relative_outflow(tmp_path: Path) -> None:
    """Relative 'outflow' field resolves to config-dir, not CWD."""
    config = {"outflow": "outflow.py", "incorp_params": {"inc_url": "https://x"}}
    result = resolve_config_paths(config, tmp_path)
    expected = str((tmp_path / "outflow.py").resolve())
    assert result["outflow"] == expected


def test_resolve_config_paths_absolute_inflow_untouched(tmp_path: Path) -> None:
    """Absolute inflow path passes through unchanged."""
    abs_path = str(tmp_path / "inflow.py")
    config = {"inflow": abs_path, "incorp_params": {"inc_url": "https://x"}}
    result = resolve_config_paths(config, tmp_path / "subdir")
    assert result["inflow"] == abs_path


def test_resolve_config_paths_url_fields_untouched(tmp_path: Path) -> None:
    """inc_url and new_url are never rebased."""
    config = {
        "incorp_params": {"inc_url": "https://api.example.com/data"},
        "refresh_params": {"new_url": "https://api.example.com/refresh"},
    }
    result = resolve_config_paths(config, tmp_path)
    assert result["incorp_params"]["inc_url"] == "https://api.example.com/data"
    assert result["refresh_params"]["new_url"] == "https://api.example.com/refresh"


def test_resolve_config_paths_output_fields_untouched(tmp_path: Path) -> None:
    """export_params.file_path and archive_target are NOT rebased."""
    config = {
        "incorp_params": {"inc_url": "https://x"},
        "export_params": {"file_path": "data/output.ndjson", "archive_target": "archive/out.ndjson"},
    }
    result = resolve_config_paths(config, tmp_path)
    # These must be left as-is (CWD-relative OUTPUT paths).
    assert result["export_params"]["file_path"] == "data/output.ndjson"
    assert result["export_params"]["archive_target"] == "archive/out.ndjson"


# ---------------------------------------------------------------------------
# resolve_config_paths — incorp_params and refresh_params
# ---------------------------------------------------------------------------


def test_resolve_config_paths_rebases_inc_file(tmp_path: Path) -> None:
    """incorp_params.inc_file is rebased to config-dir."""
    config = {"incorp_params": {"inc_file": "data.json", "inc_code": "id"}}
    result = resolve_config_paths(config, tmp_path)
    expected = str((tmp_path / "data.json").resolve())
    assert result["incorp_params"]["inc_file"] == expected


def test_resolve_config_paths_rebases_inc_files_list(tmp_path: Path) -> None:
    """incorp_params.inc_files list entries are each rebased."""
    config = {"incorp_params": {"inc_files": ["a.json", "b.json"], "inc_code": "id"}}
    result = resolve_config_paths(config, tmp_path)
    assert result["incorp_params"]["inc_files"] == [
        str((tmp_path / "a.json").resolve()),
        str((tmp_path / "b.json").resolve()),
    ]


def test_resolve_config_paths_rebases_new_file(tmp_path: Path) -> None:
    """refresh_params.new_file is rebased to config-dir."""
    config = {
        "incorp_params": {"inc_url": "https://x"},
        "refresh_params": {"new_file": "refresh.json"},
    }
    result = resolve_config_paths(config, tmp_path)
    expected = str((tmp_path / "refresh.json").resolve())
    assert result["refresh_params"]["new_file"] == expected


# ---------------------------------------------------------------------------
# resolve_config_paths — stream_params[] (fjord-style)
# ---------------------------------------------------------------------------


def test_resolve_config_paths_rebases_stream_params_inc_file(tmp_path: Path) -> None:
    """stream_params[].incorp_params.inc_file is rebased per-entry."""
    config = {
        "outflow": "outflow.py",
        "stream_params": [
            {"cls_name": "A", "incorp_params": {"inc_file": "source_a.json"}},
            {"cls_name": "B", "incorp_params": {"inc_url": "https://x"}},
        ],
    }
    result = resolve_config_paths(config, tmp_path)
    assert result["stream_params"][0]["incorp_params"]["inc_file"] == str((tmp_path / "source_a.json").resolve())
    # URL-only entry left unchanged.
    assert result["stream_params"][1]["incorp_params"].get("inc_file") is None
    assert result["stream_params"][1]["incorp_params"]["inc_url"] == "https://x"


def test_resolve_config_paths_rebases_stream_params_new_file(tmp_path: Path) -> None:
    """stream_params[].refresh_params.new_file is rebased per-entry."""
    config = {
        "outflow": "outflow.py",
        "stream_params": [
            {
                "cls_name": "A",
                "incorp_params": {"inc_url": "https://x"},
                "refresh_params": {"new_file": "refresh.json"},
            }
        ],
    }
    result = resolve_config_paths(config, tmp_path)
    assert result["stream_params"][0]["refresh_params"]["new_file"] == str((tmp_path / "refresh.json").resolve())


# ---------------------------------------------------------------------------
# resolve_config_paths — watershed current entries
# ---------------------------------------------------------------------------


def test_resolve_config_paths_rebases_watershed_currents(tmp_path: Path) -> None:
    """currents[] incorp_params.inc_file is rebased in watershed configs."""
    config = {
        "window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z"},
        "shape": "parallel",
        "currents": [
            {
                "name": "loader",
                "class": "Loader",
                "verb": "stream",
                "interval": 30,
                "incorp_params": {"inc_file": "loader.json", "inc_code": "id"},
            }
        ],
    }
    result = resolve_config_paths(config, tmp_path)
    expected = str((tmp_path / "loader.json").resolve())
    assert result["currents"][0]["incorp_params"]["inc_file"] == expected


def test_resolve_config_paths_rebases_head_tail_source(tmp_path: Path) -> None:
    """head/tail/source single-dict entries are rebased."""
    config = {
        "window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T01:00:00Z"},
        "shape": "diamond",
        "head": {"name": "h", "class": "H", "verb": "stream", "interval": 10, "incorp_params": {"inc_file": "h.json"}},
        "tail": {"name": "t", "class": "T", "verb": "fjord", "interval": 10, "export_params": {}},
        "middle": [],
    }
    result = resolve_config_paths(config, tmp_path)
    assert result["head"]["incorp_params"]["inc_file"] == str((tmp_path / "h.json").resolve())
    # tail has no incorp_params — should not error.
    assert "incorp_params" not in result["tail"]


def test_resolve_config_paths_rebases_current_sidecar_inflow_outflow(tmp_path: Path) -> None:
    """Per-current inflow/outflow fields inside currents[] are rebased."""
    config = {
        "shape": "parallel",
        "currents": [
            {
                "name": "c",
                "class": "C",
                "verb": "fjord",
                "interval": 10,
                "inflow": "c_inflow.py",
                "outflow": "c_outflow.py",
                "export_params": {},
            }
        ],
    }
    result = resolve_config_paths(config, tmp_path)
    assert result["currents"][0]["inflow"] == str((tmp_path / "c_inflow.py").resolve())
    assert result["currents"][0]["outflow"] == str((tmp_path / "c_outflow.py").resolve())


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_resolve_config_paths_idempotent(tmp_path: Path) -> None:
    """Calling resolve_config_paths twice produces the same result."""
    config = {
        "inflow": "inflow.py",
        "incorp_params": {"inc_file": "data.json", "inc_url": "https://x"},
        "refresh_params": {"new_file": "refresh.json"},
    }
    once = resolve_config_paths(config, tmp_path)
    twice = resolve_config_paths(once, tmp_path)
    assert once == twice


def test_resolve_config_paths_does_not_mutate_original(tmp_path: Path) -> None:
    """The original config dict is not mutated."""
    config = {"inflow": "inflow.py", "incorp_params": {"inc_file": "data.json"}}
    original_inflow = config["inflow"]
    original_inc_file = config["incorp_params"]["inc_file"]
    resolve_config_paths(config, tmp_path)
    assert config["inflow"] == original_inflow
    assert config["incorp_params"]["inc_file"] == original_inc_file


# ---------------------------------------------------------------------------
# CWD-independence: relative inc_file resolves to config-dir, not CWD
# ---------------------------------------------------------------------------


def test_resolve_config_paths_resolves_to_config_dir_not_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A relative inc_file resolves to the config dir even from a different CWD.

    Proves the Docker use-case: config mounted at /app/config, writable
    runtime dir is /app/data, process WORKDIR=/app — the runtime chdir'd
    to /app but config_dir is /app/config.
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    cwd_dir = tmp_path / "runtime"
    cwd_dir.mkdir()
    monkeypatch.chdir(cwd_dir)

    config = {"incorp_params": {"inc_file": "source.json", "inc_code": "id"}}
    result = resolve_config_paths(config, config_dir)

    expected = str((config_dir / "source.json").resolve())
    assert result["incorp_params"]["inc_file"] == expected
    # Must NOT be relative to the CWD.
    cwd_relative = str((cwd_dir / "source.json").resolve())
    assert result["incorp_params"]["inc_file"] != cwd_relative


def test_resolve_config_paths_stream_params_cwd_independence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """stream_params[].incorp_params.inc_file resolves to config-dir from a different CWD."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)

    config = {
        "outflow": "outflow.py",
        "stream_params": [{"cls_name": "A", "incorp_params": {"inc_file": "a.json"}}],
    }
    result = resolve_config_paths(config, config_dir)
    expected = str((config_dir / "a.json").resolve())
    assert result["stream_params"][0]["incorp_params"]["inc_file"] == expected


def test_resolve_config_paths_watershed_currents_cwd_independence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Watershed current incorp_params.inc_file resolves to config-dir from a different CWD."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)

    config = {
        "shape": "parallel",
        "currents": [
            {
                "name": "loader",
                "class": "Loader",
                "verb": "stream",
                "interval": 30,
                "incorp_params": {"inc_file": "loader.json", "inc_code": "id"},
            }
        ],
    }
    result = resolve_config_paths(config, config_dir)
    expected = str((config_dir / "loader.json").resolve())
    assert result["currents"][0]["incorp_params"]["inc_file"] == expected


# ---------------------------------------------------------------------------
# resolve_output_path
# ---------------------------------------------------------------------------


def test_resolve_output_path_creates_parent_dir(tmp_path: Path) -> None:
    """resolve_output_path auto-creates missing parent directories."""
    target = tmp_path / "nested" / "deep" / "heartbeat.txt"
    result = resolve_output_path(target)
    assert result.parent.is_dir()
    assert result == target.resolve()


def test_resolve_output_path_existing_dir_ok(tmp_path: Path) -> None:
    """resolve_output_path does not error when parent already exists."""
    result = resolve_output_path(tmp_path / "out.txt")
    assert result.parent == tmp_path.resolve()


def test_resolve_output_path_string_input(tmp_path: Path) -> None:
    """resolve_output_path accepts a string path."""
    result = resolve_output_path(str(tmp_path / "nested" / "out.txt"))
    assert result.parent.is_dir()
