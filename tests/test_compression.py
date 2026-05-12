import json
from pathlib import Path

import pytest

from incorporator.io.compression import (
    CompressionType,
    _find_target_in_archive,
    compress_file,
    decompress_data,
    infer_compression,
)
from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType  # 🛡️ IMPORT FORMAT TYPE

# The dummy JSON we will use to test data integrity
DUMMY_JSON = json.dumps([{"id": "NAV", "st": {"pos": [12, 44], "ok": 1}}])


# ==========================================
# 1. INFERENCE & UTILITY TESTS
# ==========================================
def test_infer_compression() -> None:
    """Verifies that file extensions are accurately mapped to CompressionTypes."""
    assert infer_compression("data.json.gz") == CompressionType.GZIP
    assert infer_compression("https://api.com/dump.ZIP") == CompressionType.ZIP
    assert infer_compression("file.tar") == CompressionType.TAR
    assert infer_compression("data.csv") is None


def test_find_target_in_archive() -> None:
    """Verifies that zip/tar extractors intelligently and strictly target data files."""
    # Happy path: Ignore Mac junk and text files, find the JSON
    names = ["__MACOSX/._data.json", "metadata.txt", "data.json"]
    assert _find_target_in_archive(names, active_format=FormatType.JSON) == "data.json"

    # Explicit Target: Find a specific file even if multiple exist!
    names_multi = ["2025.json", "2026.json"]
    assert (
        _find_target_in_archive(names_multi, active_format=FormatType.JSON, archive_target="2026.json") == "2026.json"
    )

    # Error path: Ambiguous archives crash gracefully instead of guessing!
    with pytest.raises(IncorporatorFormatError, match="multiple valid"):
        _find_target_in_archive(["1.json", "2.json"], active_format=FormatType.JSON)

    # Error path: Missing format crashes gracefully
    with pytest.raises(IncorporatorFormatError, match="contains no files matching"):
        _find_target_in_archive(["readme.md", "image.png"], active_format=FormatType.JSON)


# ==========================================
# 2. DISK I/O ROUND-TRIP TESTS
# ==========================================
@pytest.mark.parametrize(
    "comp_type",
    [
        CompressionType.GZIP,
        CompressionType.BZ2,
        CompressionType.LZMA,
        CompressionType.ZIP,
        CompressionType.TAR,
        CompressionType.TGZ,
    ],
)
def test_compression_roundtrip_file(tmp_path: Path, comp_type: CompressionType) -> None:
    """Tests writing an archive to disk, and decompressing directly from disk."""

    # 1. Create our raw source file in the temporary directory
    src_file = tmp_path / "telemetry.json"
    src_file.write_text(DUMMY_JSON, encoding="utf-8")

    # 2. Compress it!
    out_path_str = compress_file(str(src_file), comp_type)
    out_path = Path(out_path_str)

    assert out_path.exists()
    assert out_path.name == f"telemetry.json.{comp_type.value}"

    # 3. Decompress it (File Mode) - 🛡️ Pass active_format!
    decompressed_text = decompress_data(str(out_path), path_hint=str(out_path), active_format=FormatType.JSON)

    assert decompressed_text == DUMMY_JSON


# ==========================================
# 3. MEMORY I/O ROUND-TRIP TESTS
# ==========================================
@pytest.mark.parametrize(
    "comp_type",
    [
        CompressionType.GZIP,
        CompressionType.BZ2,
        CompressionType.LZMA,
        CompressionType.ZIP,
        CompressionType.TAR,
        CompressionType.TGZ,
    ],
)
def test_compression_roundtrip_memory(tmp_path: Path, comp_type: CompressionType) -> None:
    """Tests decompressing raw binary HTTP bytes entirely in RAM."""

    # 1. Setup: Create a compressed file to simulate a server
    src_file = tmp_path / "telemetry.json"
    src_file.write_text(DUMMY_JSON, encoding="utf-8")
    out_path_str = compress_file(str(src_file), comp_type)
    out_path = Path(out_path_str)

    # 2. Read the raw bytes (Simulating `httpx.Response.read()`)
    raw_bytes = out_path.read_bytes()

    # 3. Decompress it (Memory Mode) - 🛡️ Pass active_format!
    decompressed_text = decompress_data(raw_bytes, path_hint=str(out_path), active_format=FormatType.JSON)

    assert decompressed_text == DUMMY_JSON


# ==========================================
# 4. ERROR HANDLING TESTS
# ==========================================
def test_compress_missing_file(tmp_path: Path) -> None:
    """Ensures we catch missing files before the compression engines crash."""
    with pytest.raises(IncorporatorFormatError, match="Cannot compress missing file"):
        compress_file(str(tmp_path / "does_not_exist.json"), CompressionType.GZIP)


def test_decompress_invalid_data() -> None:
    """Ensures corrupted byte streams are caught and recast to domain errors."""
    garbage_bytes = b"this is definitely not a gzip file"

    # 🛡️ Pass active_format!
    with pytest.raises(IncorporatorFormatError, match="Failed to decompress"):
        decompress_data(garbage_bytes, path_hint="fake.gz", active_format=FormatType.JSON)


# ==========================================
# 5. CRAMJAM-BACKED ROUND-TRIPS (optional [cramjam] extra)
# ==========================================
@pytest.mark.parametrize(
    "comp_type",
    [
        CompressionType.ZSTD,
        CompressionType.LZ4,
        CompressionType.SNAPPY,
        CompressionType.BROTLI,
    ],
)
def test_cramjam_compression_roundtrip(tmp_path: Path, comp_type: CompressionType) -> None:
    """Cramjam-backed compression types must round-trip cleanly when the extra is installed."""
    pytest.importorskip("cramjam")

    src_file = tmp_path / "telemetry.json"
    src_file.write_text(DUMMY_JSON, encoding="utf-8")
    out_path = Path(compress_file(str(src_file), comp_type))

    assert out_path.exists()
    assert out_path.name == f"telemetry.json.{comp_type.value}"

    raw_bytes = out_path.read_bytes()
    decompressed = decompress_data(raw_bytes, path_hint=str(out_path), active_format=FormatType.JSON)
    assert decompressed == DUMMY_JSON


# ==========================================
# 6. COMPRESS_FILE WITH STRING COMP_TYPE
# ==========================================


def test_compress_file_accepts_string_comp_type(tmp_path: Path) -> None:
    """compress_file must accept a plain string compression type and convert it."""
    src = tmp_path / "input.json"
    src.write_text(DUMMY_JSON, encoding="utf-8")

    out_path = compress_file(str(src), "gz")  # string, not CompressionType enum

    assert Path(out_path).exists()
    assert out_path.endswith(".gz")


def test_compress_file_invalid_string_comp_type(tmp_path: Path) -> None:
    """An unrecognised string compression type must raise IncorporatorFormatError."""
    src = tmp_path / "input.json"
    src.write_text(DUMMY_JSON, encoding="utf-8")

    with pytest.raises(IncorporatorFormatError, match="Unsupported compression type"):
        compress_file(str(src), "rar")


# ==========================================
# 7. ARCHIVE BINARY TARGET (SQLite inside ZIP)
# ==========================================


def test_archive_with_binary_target(tmp_path: Path) -> None:
    """decompress_data must return raw bytes when extracting a SQLite .db from a ZIP archive."""
    import zipfile

    # Build a minimal SQLite database in memory (the magic header is enough for this test)
    db_content = b"SQLite format 3\x00" + b"\x00" * 84  # 100-byte SQLite header stub

    db_file = tmp_path / "data.db"
    db_file.write_bytes(db_content)

    zip_file = tmp_path / "archive.zip"
    with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(db_file, arcname="data.db")

    raw_bytes = zip_file.read_bytes()

    result = decompress_data(raw_bytes, path_hint="archive.zip", active_format=FormatType.SQLITE, archive_target="data.db")

    assert isinstance(result, bytes), "Binary format must return bytes, not str"
    assert result == db_content


# ==========================================
# 7. ROUTER COVERAGE INVARIANT
# ==========================================
def test_router_coverage_invariant() -> None:
    """_assert_router_coverage must pass at import time — every CompressionType
    member must be present in both _DECOMPRESS_ROUTER and _COMPRESS_ROUTER.

    A missing entry would have raised RuntimeError when the module loaded.
    This test simply re-runs the check to keep the invariant visible.
    """
    from incorporator.io.compression import (
        _COMPRESS_ROUTER,
        _DECOMPRESS_ROUTER,
        _assert_router_coverage,
    )

    _assert_router_coverage()  # must not raise
    assert set(_DECOMPRESS_ROUTER.keys()) == set(CompressionType)
    assert set(_COMPRESS_ROUTER.keys()) == set(CompressionType)
