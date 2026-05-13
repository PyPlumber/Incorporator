"""Unit tests for ParquetHandler — round-trip, schema-hint write, mixed types,
empty input guard, and the missing-dep error path. pyarrow is an optional extra
so the whole module is skipped if not installed."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType, infer_format
from incorporator.io.handlers import _HANDLERS
from incorporator.io.handlers.columnar import ParquetHandler

pytest.importorskip("pyarrow")


DUMMY_DATA: List[Dict[str, Any]] = [
    {"id": 1, "name": "Alice", "score": 95.5, "is_active": True, "tags": ["admin", "user"]},
    {"id": 2, "name": "Bob", "score": None, "is_active": False, "tags": ["user"]},
    {"id": 3, "name": "Carol", "score": 88.0, "is_active": True, "tags": []},
]

# Pydantic-style JSON-schema hint for the schema-aware write path
SCHEMA_HINT: Dict[str, Any] = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "score": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "is_active": {"type": "boolean"},
        "tags": {"type": "array"},  # flattened to string via serialize_nested
    }
}


# ==========================================
# 1. ROUND-TRIP & TYPE PRESERVATION
# ==========================================


def test_parquet_write_then_parse_round_trip(tmp_path: Path) -> None:
    """Write 3 rows with a schema hint, parse back, every field intact."""
    pq_path = tmp_path / "users.parquet"
    handler = ParquetHandler()

    handler.write(DUMMY_DATA, pq_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))

    assert pq_path.exists(), "Parquet file was not created"

    rows = handler.parse(pq_path)
    assert len(rows) == 3

    alice = rows[0]
    assert alice["id"] == 1
    assert alice["name"] == "Alice"
    assert alice["score"] == pytest.approx(95.5)
    assert alice["is_active"] is True
    # tags is a list → serialize_nested encodes it as JSON, deserialize_nested
    # recovers the native list on read.
    assert alice["tags"] == ["admin", "user"]


def test_parquet_preserves_none_for_missing_score(tmp_path: Path) -> None:
    """Bob's score is None — must round-trip as None, not 0.0 or the string 'None'."""
    pq_path = tmp_path / "users.parquet"
    handler = ParquetHandler()
    handler.write(DUMMY_DATA, pq_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    rows = handler.parse(pq_path)
    assert rows[1]["score"] is None


def test_parquet_empty_list_serialised_and_recovered(tmp_path: Path) -> None:
    """Carol's tags=[] must round-trip as []."""
    pq_path = tmp_path / "users.parquet"
    handler = ParquetHandler()
    handler.write(DUMMY_DATA, pq_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))
    rows = handler.parse(pq_path)
    assert rows[2]["tags"] == []


# ==========================================
# 2. NO-SCHEMA-HINT FALLBACK PATH
# ==========================================


def test_parquet_write_without_schema_hint_infers_columns(tmp_path: Path) -> None:
    """When all_field_names is absent, the writer must materialize, then infer all keys."""
    pq_path = tmp_path / "no_hint.parquet"
    handler = ParquetHandler()
    handler.write(DUMMY_DATA, pq_path)  # no schema hint at all
    rows = handler.parse(pq_path)
    assert len(rows) == 3
    expected_cols = {"id", "name", "score", "is_active", "tags"}
    assert set(rows[0].keys()) == expected_cols


# ==========================================
# 3. BYTES-SOURCE PARSE PATH
# ==========================================


def test_parquet_parse_from_bytes(tmp_path: Path) -> None:
    """ParquetHandler.parse must accept raw bytes (e.g. from an HTTP response)."""
    pq_path = tmp_path / "src.parquet"
    handler = ParquetHandler()
    handler.write(DUMMY_DATA, pq_path, pydantic_schema=SCHEMA_HINT, all_field_names=list(SCHEMA_HINT["properties"]))

    raw_bytes = pq_path.read_bytes()
    rows = handler.parse(raw_bytes)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"


# ==========================================
# 4. FORMAT INFERENCE & DISPATCH
# ==========================================


def test_infer_format_parquet_extensions() -> None:
    """infer_format() must recognise .parquet and .pq."""
    assert infer_format("sales.parquet") == FormatType.PARQUET
    assert infer_format("sales.pq") == FormatType.PARQUET
    assert infer_format("SALES.PARQUET") == FormatType.PARQUET  # case-insensitive


def test_parquet_handler_registered_in_dispatch() -> None:
    """The handler dispatch must include the new PARQUET entry."""
    assert FormatType.PARQUET in _HANDLERS
    assert isinstance(_HANDLERS[FormatType.PARQUET], ParquetHandler)


# ==========================================
# 5. ERROR PATHS
# ==========================================


def test_parquet_parse_rejects_str_source(tmp_path: Path) -> None:
    """ParquetHandler.parse must reject raw strings — needs Path or bytes."""
    handler = ParquetHandler()
    with pytest.raises(IncorporatorFormatError, match="raw bytes or a physical Path"):
        handler.parse("not a path")  # type: ignore[arg-type]


def test_parquet_write_rejects_append_mode(tmp_path: Path) -> None:
    """Parquet has a footer index — append must be rejected."""
    pq_path = tmp_path / "append.parquet"
    handler = ParquetHandler()
    with pytest.raises(IncorporatorFormatError, match="do not support O\\(1\\) streaming appends"):
        handler.write(DUMMY_DATA, pq_path, if_exists="append")


def test_parquet_parse_corrupted_file_raises(tmp_path: Path) -> None:
    """A non-parquet file masquerading as parquet must raise IncorporatorFormatError."""
    bad_path = tmp_path / "not_really.parquet"
    bad_path.write_text("this is not a real parquet file", encoding="utf-8")
    handler = ParquetHandler()
    with pytest.raises(IncorporatorFormatError, match="Parquet Read Error"):
        handler.parse(bad_path)


# ==========================================
# 6. MISSING OPTIONAL DEP MESSAGES
# ==========================================


def test_parquet_parse_missing_pyarrow_message(tmp_path: Path) -> None:
    """When pyarrow is missing the error must point to the extras flag."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyarrow.parquet" or name == "pyarrow":
            raise ImportError("simulated missing pyarrow")
        return real_import(name, *args, **kwargs)

    fake_path = tmp_path / "x.parquet"
    fake_path.write_bytes(b"PAR1")

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[parquet\]"):
            ParquetHandler().parse(fake_path)


def test_parquet_write_missing_pyarrow_message(tmp_path: Path) -> None:
    """Same guard on the write path."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in ("pyarrow", "pyarrow.parquet"):
            raise ImportError("simulated missing pyarrow")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[parquet\]"):
            ParquetHandler().write(DUMMY_DATA, tmp_path / "out.parquet")
