"""Unit tests for incorporator.cli.validate (validators called in isolation)."""

from pathlib import Path
from typing import Any, Dict

from incorporator.cli.validate import (
    autodetect_type,
    validate_config,
    validate_fjord_config,
    validate_stream_config,
)


def test_autodetect_stream() -> None:
    assert autodetect_type({"incorp_params": {"inc_url": "x"}}) == "stream"


def test_autodetect_fjord() -> None:
    assert autodetect_type({"outflow": "x.py", "stream_params": []}) == "fjord"


def test_autodetect_defaults_to_stream_when_ambiguous() -> None:
    # Neither distinctive key present — fall back to stream and let the
    # validator surface the actual missing keys.
    assert autodetect_type({}) == "stream"


def test_validate_stream_ok(tmp_path: Path) -> None:
    cfg: Dict[str, Any] = {"incorp_params": {"inc_url": "https://x", "inc_code": "id"}}
    errs = validate_stream_config(cfg, tmp_path)
    assert errs == []


def test_validate_stream_missing_incorp_params(tmp_path: Path) -> None:
    errs = validate_stream_config({}, tmp_path)
    assert any("incorp_params" in e for e in errs)


def test_validate_stream_no_source_key(tmp_path: Path) -> None:
    errs = validate_stream_config({"incorp_params": {"inc_code": "id"}}, tmp_path)
    assert any("source key" in e for e in errs)


def test_validate_stream_non_dict_refresh_params(tmp_path: Path) -> None:
    cfg = {"incorp_params": {"inc_url": "https://x"}, "refresh_params": "not-a-dict"}
    errs = validate_stream_config(cfg, tmp_path)
    assert any("refresh_params" in e for e in errs)


def test_validate_stream_non_numeric_interval(tmp_path: Path) -> None:
    cfg = {"incorp_params": {"inc_url": "https://x"}, "refresh_interval": "soon"}
    errs = validate_stream_config(cfg, tmp_path)
    assert any("refresh_interval" in e for e in errs)


# ----- fjord -----


FJORD_OK_SRC = """
from incorporator import Incorporator

class A(Incorporator): pass

def outflow(state):
    return []
"""


def _write_fjord(tmp_path: Path, module_src: str = FJORD_OK_SRC) -> tuple[Dict[str, Any], Path]:
    module = tmp_path / "valid_fjord.py"
    module.write_text(module_src, encoding="utf-8")
    cfg: Dict[str, Any] = {
        "outflow": "valid_fjord.py",
        "stream_params": [{"cls_name": "A", "incorp_params": {"inc_url": "https://x"}}],
        "export_params": {"file_path": str(tmp_path / "out.ndjson")},
    }
    return cfg, module


def test_validate_fjord_ok(tmp_path: Path) -> None:
    cfg, _ = _write_fjord(tmp_path)
    errs = validate_fjord_config(cfg, tmp_path)
    assert errs == []


def test_validate_fjord_missing_outflow_key(tmp_path: Path) -> None:
    errs = validate_fjord_config({"stream_params": [], "export_params": {}}, tmp_path)
    assert any("outflow" in e for e in errs)


def test_validate_fjord_outflow_not_found(tmp_path: Path) -> None:
    cfg = {
        "outflow": "ghost.py",
        "stream_params": [{"cls_name": "A", "incorp_params": {}}],
        "export_params": {"file_path": "out.ndjson"},
    }
    errs = validate_fjord_config(cfg, tmp_path)
    assert any("not found" in e for e in errs)


def test_validate_fjord_outflow_missing(tmp_path: Path) -> None:
    src = "from incorporator import Incorporator\nclass A(Incorporator): pass\n"
    cfg, _ = _write_fjord(tmp_path, module_src=src)
    errs = validate_fjord_config(cfg, tmp_path)
    assert any("outflow" in e.lower() for e in errs)


def test_validate_fjord_outflow_wrong_arity(tmp_path: Path) -> None:
    src = "from incorporator import Incorporator\nclass A(Incorporator): pass\ndef outflow(state, extra): return []\n"
    cfg, _ = _write_fjord(tmp_path, module_src=src)
    errs = validate_fjord_config(cfg, tmp_path)
    assert any("exactly 1 parameter" in e for e in errs)


def test_validate_fjord_unknown_cls_name(tmp_path: Path) -> None:
    cfg, _ = _write_fjord(tmp_path)
    cfg["stream_params"] = [{"cls_name": "DoesNotExist", "incorp_params": {"inc_url": "https://x"}}]
    errs = validate_fjord_config(cfg, tmp_path)
    assert any("DoesNotExist" in e for e in errs)


def test_validate_config_dispatches_to_fjord(tmp_path: Path) -> None:
    cfg, _ = _write_fjord(tmp_path)
    detected, errs = validate_config(cfg, tmp_path)
    assert detected == "fjord"
    assert errs == []


# ----- inflow + outflow on stream -----


def test_validate_stream_inflow_missing_file(tmp_path: Path) -> None:
    cfg = {"incorp_params": {"inc_url": "https://x", "inc_code": "id"}, "inflow": "nope.py"}
    errs = validate_stream_config(cfg, tmp_path)
    assert any("inflow file not found" in e for e in errs)


def test_validate_stream_inflow_import_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad_inflow.py"
    bad.write_text("def x(:\n", encoding="utf-8")
    cfg = {"incorp_params": {"inc_url": "https://x", "inc_code": "id"}, "inflow": "bad_inflow.py"}
    errs = validate_stream_config(cfg, tmp_path)
    assert any("inflow file failed to import" in e for e in errs)


def test_validate_stream_inflow_ok(tmp_path: Path) -> None:
    good = tmp_path / "inflow.py"
    good.write_text("def calculate(x):\n    return x\n", encoding="utf-8")
    cfg = {"incorp_params": {"inc_url": "https://x", "inc_code": "id"}, "inflow": "inflow.py"}
    errs = validate_stream_config(cfg, tmp_path)
    assert errs == []


def test_validate_stream_outflow_without_stateful_polling_errors(tmp_path: Path) -> None:
    """outflow on stream requires stateful_polling=true."""
    good = tmp_path / "outflow.py"
    good.write_text("from incorporator import Incorporator\nclass MyData(Incorporator): pass\n", encoding="utf-8")
    cfg = {
        "incorp_params": {"inc_url": "https://x", "inc_code": "id"},
        "outflow": "outflow.py",
        # stateful_polling deliberately absent (default false)
    }
    errs = validate_stream_config(cfg, tmp_path)
    assert any("stateful_polling" in e for e in errs)


def test_validate_stream_outflow_with_stateful_polling_ok(tmp_path: Path) -> None:
    good = tmp_path / "outflow.py"
    good.write_text("from incorporator import Incorporator\nclass MyData(Incorporator): pass\n", encoding="utf-8")
    cfg = {
        "incorp_params": {"inc_url": "https://x", "inc_code": "id"},
        "outflow": "outflow.py",
        "stateful_polling": True,
    }
    errs = validate_stream_config(cfg, tmp_path)
    assert errs == []


def test_validate_fjord_accepts_outflow_canonical_key(tmp_path: Path) -> None:
    """fjord pipeline.json declares the outflow path with the canonical 'outflow' key."""
    module = tmp_path / "outflow.py"
    module.write_text(FJORD_OK_SRC, encoding="utf-8")
    cfg = {
        "outflow": "outflow.py",
        "stream_params": [{"cls_name": "A", "incorp_params": {"inc_url": "https://x"}}],
        "export_params": {"file_path": str(tmp_path / "out.ndjson")},
    }
    errs = validate_fjord_config(cfg, tmp_path)
    assert errs == []


# ----- inc_file existence checks via config-dir resolution -----


def test_validate_stream_inc_file_exists_at_config_dir(tmp_path: Path) -> None:
    """validate PASSES when inc_file exists at the config dir even from another CWD.

    Proves the Docker policy: config at /app/config, runtime at /app.
    validate resolves inc_file relative to config_dir, not CWD.
    """
    (tmp_path / "data.json").write_text("[{\"id\": 1}]", encoding="utf-8")
    cfg = {
        "incorp_params": {"inc_file": "data.json", "inc_code": "id"},
    }
    errs = validate_stream_config(cfg, tmp_path)
    assert errs == []


def test_validate_stream_inc_file_missing_at_config_dir(tmp_path: Path) -> None:
    """validate FAILS when inc_file does not exist at the config dir.

    Proves the A1 bug is caught: validate and run now agree on where
    INPUT files must live (config-dir) — validate no longer silently
    passes when the file is missing.
    """
    cfg = {
        "incorp_params": {"inc_file": "ghost.json", "inc_code": "id"},
    }
    errs = validate_stream_config(cfg, tmp_path)
    assert any("inc_file" in e and "not found" in e for e in errs), errs


def test_validate_stream_output_file_path_not_required_to_preexist(tmp_path: Path) -> None:
    """validate does NOT require export_params.file_path to exist (it's an OUTPUT)."""
    cfg = {
        "incorp_params": {"inc_url": "https://x", "inc_code": "id"},
        "export_params": {"file_path": "data/output.ndjson"},
    }
    errs = validate_stream_config(cfg, tmp_path)
    # No error about file_path not existing — OUTPUT files are CWD-relative and
    # are created at runtime by the handler.
    assert not any("file_path" in e and "not found" in e for e in errs)
