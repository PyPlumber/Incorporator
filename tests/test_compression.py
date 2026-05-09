import json
from pathlib import Path

import pytest

from incorporator.methods.compression import (
    CompressionType,
    _find_target_in_archive,
    compress_file,
    decompress_data,
    infer_compression,
)
from incorporator.methods.exceptions import IncorporatorFormatError
from incorporator.methods.format_parsers import FormatType  # 🛡️ IMPORT FORMAT TYPE

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
    assert _find_target_in_archive(names_multi, active_format=FormatType.JSON,
                                   archive_target="2026.json") == "2026.json"

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
    "comp_type", [
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
    "comp_type", [
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