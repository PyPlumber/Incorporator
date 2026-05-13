"""Unit tests for ExcelHandler — round-trip, header inference, mixed types, and
the missing-dep error path. openpyxl is an optional extra so the whole module
is skipped if not installed."""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType, infer_format
from incorporator.io.handlers import _HANDLERS
from incorporator.io.handlers.spreadsheet import ExcelHandler

pytest.importorskip("openpyxl")


DUMMY_DATA: List[Dict[str, Any]] = [
    {"id": 1, "name": "Alice", "score": 95.5, "is_active": True, "tags": ["admin", "user"]},
    {"id": 2, "name": "Bob", "score": None, "is_active": False, "tags": ["user"]},
    {"id": 3, "name": "Carol", "score": 88.0, "is_active": True, "tags": []},
]


# ==========================================
# 1. ROUND-TRIP & TYPE PRESERVATION
# ==========================================


def test_excel_write_then_parse_round_trip(tmp_path: Path) -> None:
    """Write 3 rows, parse back, and confirm every field comes back."""
    xlsx_path = tmp_path / "users.xlsx"
    handler = ExcelHandler()

    handler.write(DUMMY_DATA, xlsx_path)

    assert xlsx_path.exists(), "xlsx file was not created"

    rows = handler.parse(xlsx_path)
    assert len(rows) == 3

    alice = rows[0]
    assert alice["id"] == 1
    assert alice["name"] == "Alice"
    assert alice["score"] == pytest.approx(95.5)
    assert alice["is_active"] is True
    # tags is a list → serialize_nested encodes it as a JSON string, then
    # deserialize_nested re-parses it on read.
    assert alice["tags"] == ["admin", "user"]


def test_excel_preserves_none_for_missing_score(tmp_path: Path) -> None:
    """Bob's score is None — it must round-trip as None, not the string 'None'."""
    xlsx_path = tmp_path / "users.xlsx"
    handler = ExcelHandler()
    handler.write(DUMMY_DATA, xlsx_path)
    rows = handler.parse(xlsx_path)
    bob = rows[1]
    assert bob["score"] is None


def test_excel_empty_list_serialised_and_recovered(tmp_path: Path) -> None:
    """Carol's tags=[] must round-trip as an empty list, not None or '[]' string."""
    xlsx_path = tmp_path / "users.xlsx"
    handler = ExcelHandler()
    handler.write(DUMMY_DATA, xlsx_path)
    rows = handler.parse(xlsx_path)
    carol = rows[2]
    assert carol["tags"] == []


# ==========================================
# 2. HEADER INFERENCE
# ==========================================


def test_excel_header_inference_from_row_one(tmp_path: Path) -> None:
    """When no all_field_names hint is given, headers come from the union of keys."""
    xlsx_path = tmp_path / "mixed.xlsx"
    handler = ExcelHandler()
    handler.write(DUMMY_DATA, xlsx_path)
    rows = handler.parse(xlsx_path)
    # Every row must have the same key set (whatever the writer chose)
    assert set(rows[0].keys()) == set(rows[1].keys()) == set(rows[2].keys())
    expected = {"id", "name", "score", "is_active", "tags"}
    assert set(rows[0].keys()) == expected


def test_excel_explicit_field_names_drive_order(tmp_path: Path) -> None:
    """When all_field_names is passed, the column order matches it exactly."""
    xlsx_path = tmp_path / "ordered.xlsx"
    handler = ExcelHandler()
    ordered = ["score", "name", "id"]
    handler.write(DUMMY_DATA, xlsx_path, all_field_names=ordered)

    # Re-parse and confirm header order
    rows = handler.parse(xlsx_path)
    assert list(rows[0].keys()) == ordered


# ==========================================
# 3. FORMAT INFERENCE & DISPATCH
# ==========================================


def test_infer_format_xlsx_extension() -> None:
    """infer_format() must map .xlsx to FormatType.XLSX."""
    assert infer_format("report.xlsx") == FormatType.XLSX
    assert infer_format("report.xlsm") == FormatType.XLSX
    assert infer_format("REPORT.XLSX") == FormatType.XLSX  # case-insensitive


def test_excel_handler_registered_in_dispatch() -> None:
    """The handler dispatch dict must include the new XLSX entry."""
    assert FormatType.XLSX in _HANDLERS
    assert isinstance(_HANDLERS[FormatType.XLSX], ExcelHandler)


# ==========================================
# 4. ERROR PATHS
# ==========================================


def test_excel_parse_rejects_non_path_source(tmp_path: Path) -> None:
    """ExcelHandler.parse must reject raw bytes/strings — needs a Path."""
    handler = ExcelHandler()
    with pytest.raises(IncorporatorFormatError, match="physical Path"):
        handler.parse(b"raw bytes data")  # type: ignore[arg-type]


def test_excel_write_rejects_append_mode(tmp_path: Path) -> None:
    """xlsx is a monolithic format — append mode must raise the standard error."""
    xlsx_path = tmp_path / "append.xlsx"
    handler = ExcelHandler()
    with pytest.raises(IncorporatorFormatError, match="do not support O\\(1\\) streaming appends"):
        handler.write(DUMMY_DATA, xlsx_path, if_exists="append")


def test_excel_parse_corrupted_file_raises(tmp_path: Path) -> None:
    """A non-xlsx file masquerading as xlsx must raise IncorporatorFormatError."""
    bad_path = tmp_path / "not_really.xlsx"
    bad_path.write_text("this is not a real xlsx file", encoding="utf-8")
    handler = ExcelHandler()
    with pytest.raises(IncorporatorFormatError, match="Excel Read Error"):
        handler.parse(bad_path)


def test_excel_parse_blank_workbook_returns_empty(tmp_path: Path) -> None:
    """A workbook with zero rows must return an empty list, not raise."""
    import openpyxl

    blank_path = tmp_path / "blank.xlsx"
    wb = openpyxl.Workbook()
    wb.save(str(blank_path))

    rows = ExcelHandler().parse(blank_path)
    assert rows == []


# ==========================================
# 5. MISSING OPTIONAL DEP MESSAGES
# ==========================================


def test_excel_parse_missing_openpyxl_message(tmp_path: Path) -> None:
    """When openpyxl is missing the error message must point to the extras flag."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "openpyxl":
            raise ImportError("simulated missing openpyxl")
        return real_import(name, *args, **kwargs)

    fake_path = tmp_path / "x.xlsx"
    fake_path.write_bytes(b"PK\x03\x04")  # placeholder — never actually opened

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[xlsx\]"):
            ExcelHandler().parse(fake_path)


def test_excel_write_missing_openpyxl_message(tmp_path: Path) -> None:
    """Same guard on the write path."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "openpyxl":
            raise ImportError("simulated missing openpyxl")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[xlsx\]"):
            ExcelHandler().write(DUMMY_DATA, tmp_path / "out.xlsx")
