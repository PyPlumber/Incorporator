"""Unit tests for FeatherHandler — round-trip, schema-hint write, type
preservation, and missing-dep error path. pyarrow is an optional extra so the
whole module is skipped if not installed."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType, infer_format
from incorporator.io.handlers import _HANDLERS
from incorporator.io.handlers.columnar import FeatherHandler

pytest.importorskip("pyarrow")


DUMMY_DATA: List[Dict[str, Any]] = [
    {"id": 1, "name": "Alice", "score": 95.5, "is_active": True, "tags": ["admin", "user"]},
    {"id": 2, "name": "Bob", "score": None, "is_active": False, "tags": ["user"]},
    {"id": 3, "name": "Carol", "score": 88.0, "is_active": True, "tags": []},
]

SCHEMA_HINT: Dict[str, Any] = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "score": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "is_active": {"type": "boolean"},
        "tags": {"type": "array"},
    }
}


def test_feather_write_then_parse_round_trip(tmp_path: Path) -> None:
    """Write 3 rows, parse back, every field intact."""
    feather_path = tmp_path / "users.feather"
    handler = FeatherHandler()
    handler.write(DUMMY_DATA, feather_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    assert feather_path.exists()

    rows = handler.parse(feather_path)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == pytest.approx(95.5)
    assert rows[0]["is_active"] is True
    assert rows[0]["tags"] == ["admin", "user"]
    assert rows[1]["score"] is None
    assert rows[2]["tags"] == []


def test_feather_write_without_schema_hint(tmp_path: Path) -> None:
    """No-schema-hint path: pyarrow native inference must figure out column types."""
    feather_path = tmp_path / "no_hint.feather"
    handler = FeatherHandler()
    handler.write(DUMMY_DATA, feather_path)
    rows = handler.parse(feather_path)
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"id", "name", "score", "is_active", "tags"}


def test_feather_parse_from_bytes(tmp_path: Path) -> None:
    """Bytes-source parse path must work (e.g. from an HTTP response)."""
    feather_path = tmp_path / "src.feather"
    handler = FeatherHandler()
    handler.write(DUMMY_DATA, feather_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    raw = feather_path.read_bytes()
    rows = handler.parse(raw)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"


def test_infer_format_feather_extensions() -> None:
    """infer_format() must recognise .feather, .arrow, .ipc."""
    assert infer_format("data.feather") == FormatType.FEATHER
    assert infer_format("data.arrow") == FormatType.FEATHER
    assert infer_format("data.ipc") == FormatType.FEATHER
    assert infer_format("DATA.FEATHER") == FormatType.FEATHER


def test_feather_handler_registered_in_dispatch() -> None:
    """FormatType.FEATHER must be in _HANDLERS and bound to FeatherHandler."""
    assert FormatType.FEATHER in _HANDLERS
    assert isinstance(_HANDLERS[FormatType.FEATHER], FeatherHandler)


def test_feather_parse_rejects_str_source() -> None:
    """parse() must reject raw strings — needs Path or bytes."""
    handler = FeatherHandler()
    with pytest.raises(IncorporatorFormatError, match="raw bytes or a physical Path"):
        handler.parse("not a path")  # type: ignore[arg-type]


def test_feather_write_rejects_append_mode(tmp_path: Path) -> None:
    """Feather V2 has no append API — must reject append mode."""
    handler = FeatherHandler()
    with pytest.raises(IncorporatorFormatError, match="do not support O\\(1\\) streaming appends"):
        handler.write(DUMMY_DATA, tmp_path / "x.feather", if_exists="append")


def test_feather_parse_corrupted_file_raises(tmp_path: Path) -> None:
    """A non-feather file must raise IncorporatorFormatError."""
    bad = tmp_path / "fake.feather"
    bad.write_text("not a feather file", encoding="utf-8")
    with pytest.raises(IncorporatorFormatError, match="Feather Read Error"):
        FeatherHandler().parse(bad)


def test_feather_parse_missing_pyarrow_message(tmp_path: Path) -> None:
    """Missing pyarrow surfaces the [parquet] extras flag in the error message."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyarrow.feather":
            raise ImportError("simulated missing pyarrow")
        return real_import(name, *args, **kwargs)

    fake_path = tmp_path / "x.feather"
    fake_path.write_bytes(b"ARROW1")

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[parquet\]"):
            FeatherHandler().parse(fake_path)


def test_feather_write_missing_pyarrow_message(tmp_path: Path) -> None:
    """Same guard on the write path."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyarrow.feather":
            raise ImportError("simulated missing pyarrow")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[parquet\]"):
            FeatherHandler().write(DUMMY_DATA, tmp_path / "out.feather")
