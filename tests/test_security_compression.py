"""Phase 2 — security regressions for compression + formula injection.

Three threats addressed:
  1. ZIP path traversal (a/k/a "ZIP slip") — malicious member names like
     '../../etc/passwd' could escape the extraction scope.  Pre-fix, TAR
     was guarded but ZIP was not.
  2. Decompression bombs — 1 KB of compressed data expanding to gigabytes.
     Pre-fix, native streams / cramjam read the entire decompressed
     payload into memory with no size cap.
  3. CSV / XLSX formula injection — cells starting with '=', '@', '+',
     '-' execute as formulas in Excel / LibreOffice / Google Sheets.
     Pre-fix, the framework wrote them raw.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.compression import (
    DEFAULT_MAX_DECOMPRESSED_BYTES,
    _enforce_size_cap,
    _validate_archive_member_names,
    CompressionType,
)
from incorporator.io.handlers._base import _neutralise_formula_injection


# ==========================================
# 1. Archive path traversal — ZIP + TAR
# ==========================================


def test_validate_archive_member_names_blocks_dotdot() -> None:
    """Member names with '..' segments must be rejected for BOTH archive kinds."""
    with pytest.raises(IncorporatorFormatError, match="path traversal blocked"):
        _validate_archive_member_names(["../../etc/passwd"], archive_kind="ZIP")
    with pytest.raises(IncorporatorFormatError, match="path traversal blocked"):
        _validate_archive_member_names(["../etc/shadow"], archive_kind="TAR")


def test_validate_archive_member_names_blocks_absolute_paths() -> None:
    """Absolute paths (POSIX and Windows drive letters) must be rejected."""
    with pytest.raises(IncorporatorFormatError, match="absolute path"):
        _validate_archive_member_names(["/etc/passwd"], archive_kind="ZIP")
    with pytest.raises(IncorporatorFormatError, match="absolute path"):
        _validate_archive_member_names([r"C:\Windows\system32\config\SAM"], archive_kind="ZIP")
    with pytest.raises(IncorporatorFormatError, match="absolute path"):
        _validate_archive_member_names([r"\boot.ini"], archive_kind="ZIP")


def test_validate_archive_member_names_accepts_safe_names() -> None:
    """Normal relative member names pass through unchanged."""
    _validate_archive_member_names(
        ["data.json", "sub/data.json", "deep/nested/data.json"],
        archive_kind="ZIP",
    )                                                  # no raise


def test_zip_with_traversal_member_rejected_at_decompress() -> None:
    """End-to-end: a crafted ZIP with '../escape' member triggers the guard."""
    from incorporator.io.compression import _decompress_archive
    from incorporator.io.formats import FormatType

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("../../escape.json", b'{"x": 1}')

    with pytest.raises(IncorporatorFormatError, match="path traversal blocked"):
        _decompress_archive(
            buf.getvalue(),
            CompressionType.ZIP,
            FormatType.JSON,
            archive_target=None,
        )


# ==========================================
# 2. Decompression bomb cap
# ==========================================


def test_enforce_size_cap_default_limit() -> None:
    """A payload above 1 GB triggers the bomb-cap raise."""
    with pytest.raises(IncorporatorFormatError, match="Decompression bomb blocked"):
        _enforce_size_cap(DEFAULT_MAX_DECOMPRESSED_BYTES + 1, CompressionType.GZIP)


def test_enforce_size_cap_under_limit_passes() -> None:
    """Payloads under the cap pass through without raising."""
    _enforce_size_cap(1024, CompressionType.GZIP)       # no raise


def test_enforce_size_cap_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """INCORPORATOR_MAX_DECOMPRESSED_BYTES env var overrides the default."""
    monkeypatch.setenv("INCORPORATOR_MAX_DECOMPRESSED_BYTES", "1024")
    with pytest.raises(IncorporatorFormatError, match="2,048"):
        _enforce_size_cap(2048, CompressionType.GZIP)


def test_enforce_size_cap_invalid_env_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Garbage env-var values fall back to the default 1 GB cap."""
    monkeypatch.setenv("INCORPORATOR_MAX_DECOMPRESSED_BYTES", "not-a-number")
    _enforce_size_cap(1024, CompressionType.GZIP)       # still passes (under 1 GB)


# ==========================================
# 3. CSV / XLSX formula-injection neutralisation
# ==========================================


@pytest.mark.parametrize("payload", ["=1+1", "=cmd|'/c calc'!A1", "@SUM(A1:A99)", "+5", "-5"])
def test_neutralise_formula_injection_prefixes_dangerous_strings(payload: str) -> None:
    """Strings starting with formula chars get a single-quote prefix."""
    assert _neutralise_formula_injection(payload) == "'" + payload


def test_neutralise_formula_injection_passes_safe_values() -> None:
    """Normal strings, empty, None, numbers — none are altered."""
    assert _neutralise_formula_injection("Bitcoin") == "Bitcoin"
    assert _neutralise_formula_injection("") == ""
    assert _neutralise_formula_injection(None) is None
    assert _neutralise_formula_injection(42) == 42
    assert _neutralise_formula_injection(3.14) == 3.14
    assert _neutralise_formula_injection(True) is True


def test_neutralise_formula_injection_handles_whitespace_control_chars() -> None:
    """Leading tab / carriage return also evaluate as formulas in some spreadsheets."""
    assert _neutralise_formula_injection("\tmalicious") == "'\tmalicious"
    assert _neutralise_formula_injection("\rmalicious") == "'\rmalicious"


def test_csv_export_neutralises_formula_cells_by_default(tmp_path: Path) -> None:
    """End-to-end CSV write applies formula neutralisation."""
    from incorporator.io.handlers.delimited import CSVHandler

    out = tmp_path / "out.csv"
    CSVHandler().write(
        [
            {"name": "Alice", "formula": "=cmd|'/c calc'!A1"},
            {"name": "Bob", "formula": "normal value"},
        ],
        out,
        all_field_names=["name", "formula"],
    )

    text = out.read_text(encoding="utf-8")
    assert "'=cmd|" in text or "\"'=cmd" in text       # prefixed
    assert "normal value" in text                       # unchanged


def test_csv_export_opt_out_writes_raw(tmp_path: Path) -> None:
    """csv_safe_formulas=False emits cells verbatim — for trusted consumers."""
    from incorporator.io.handlers.delimited import CSVHandler

    out = tmp_path / "out.csv"
    CSVHandler().write(
        [{"name": "Alice", "formula": "=SUM(A1:A9)"}],
        out,
        all_field_names=["name", "formula"],
        csv_safe_formulas=False,
    )
    text = out.read_text(encoding="utf-8")
    assert "=SUM" in text
    assert "'=SUM" not in text
