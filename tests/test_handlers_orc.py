"""Unit tests for OrcHandler — round-trip, type preservation, error paths."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType, infer_format
from incorporator.io.handlers import _HANDLERS
from incorporator.io.handlers.columnar import OrcHandler

pytest.importorskip("pyarrow")
# pyarrow.orc support is platform-sensitive; skip on platforms where it's not built.
pytest.importorskip("pyarrow.orc")


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


def test_orc_write_then_parse_round_trip(tmp_path: Path) -> None:
    """Round-trip with schema hint."""
    orc_path = tmp_path / "users.orc"
    handler = OrcHandler()
    handler.write(DUMMY_DATA, orc_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    assert orc_path.exists()

    rows = handler.parse(orc_path)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == pytest.approx(95.5)
    assert rows[0]["is_active"] is True
    assert rows[0]["tags"] == ["admin", "user"]
    assert rows[1]["score"] is None
    assert rows[2]["tags"] == []


def test_orc_write_without_schema_hint(tmp_path: Path) -> None:
    """No-hint path uses pyarrow native inference."""
    orc_path = tmp_path / "no_hint.orc"
    handler = OrcHandler()
    handler.write(DUMMY_DATA, orc_path)
    rows = handler.parse(orc_path)
    assert len(rows) == 3


def test_orc_parse_from_bytes(tmp_path: Path) -> None:
    """Bytes-source parse must work."""
    orc_path = tmp_path / "src.orc"
    handler = OrcHandler()
    handler.write(DUMMY_DATA, orc_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    raw = orc_path.read_bytes()
    rows = handler.parse(raw)
    assert len(rows) == 3


def test_infer_format_orc_extension() -> None:
    assert infer_format("data.orc") == FormatType.ORC
    assert infer_format("DATA.ORC") == FormatType.ORC


def test_orc_handler_registered_in_dispatch() -> None:
    assert FormatType.ORC in _HANDLERS
    assert isinstance(_HANDLERS[FormatType.ORC], OrcHandler)


def test_orc_parse_rejects_str_source() -> None:
    handler = OrcHandler()
    with pytest.raises(IncorporatorFormatError, match="raw bytes or a physical Path"):
        handler.parse("not a path")  # type: ignore[arg-type]


def test_orc_write_rejects_append_mode(tmp_path: Path) -> None:
    handler = OrcHandler()
    with pytest.raises(IncorporatorFormatError, match="do not support O\\(1\\) streaming appends"):
        handler.write(DUMMY_DATA, tmp_path / "x.orc", if_exists="append")


def test_orc_parse_corrupted_file_raises(tmp_path: Path) -> None:
    bad = tmp_path / "fake.orc"
    bad.write_text("not an orc file", encoding="utf-8")
    with pytest.raises(IncorporatorFormatError, match="ORC Read Error"):
        OrcHandler().parse(bad)


def test_orc_parse_missing_pyarrow_message(tmp_path: Path) -> None:
    """Missing pyarrow.orc surfaces the [parquet] extras flag."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        # 'from pyarrow import orc' resolves through pyarrow; intercept the submodule.
        if name == "pyarrow":
            raise ImportError("simulated missing pyarrow")
        return real_import(name, *args, **kwargs)

    fake_path = tmp_path / "x.orc"
    fake_path.write_bytes(b"ORC\x00")

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[parquet\]"):
            OrcHandler().parse(fake_path)
