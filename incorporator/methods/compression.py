"""
Modular Compression Engine for Incorporator.
Utilizes a Strategy Pattern to safely route between Native Python and Rust-backed Cramjam algorithms.
"""

import bz2
import gzip
import io
import lzma
import shutil
import tarfile
import zipfile
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Optional, Union, cast

from .format_parsers import FormatType
from .exceptions import IncorporatorFormatError

class CompressionType(str, Enum):
    # Native Streams
    GZIP = "gz"
    BZ2 = "bz2"
    XZ = "xz"
    LZMA = "lzma"
    # Native Archives
    ZIP = "zip"
    TAR = "tar"
    TGZ = "tgz"
    # Cramjam Plugins
    ZSTD = "zst"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    BROTLI = "br"

def infer_compression(path_or_url: str) -> Optional[CompressionType]:
    path_lower = str(path_or_url).lower()
    for comp in CompressionType:
        if path_lower.endswith(f".{comp.value}"):
            return comp
    return None


def _find_target_in_archive(
        names: list[str],
        active_format: FormatType,
        archive_target: Optional[str] = None
) -> str:
    """Finds a target file in an archive safely and predictably."""

    # 1. If the user explicitly asks for a file, grab it or crash!
    if archive_target:
        if archive_target in names:
            return archive_target
        raise IncorporatorFormatError(f"Target '{archive_target}' not found in archive.")

    # 2. Otherwise, look exclusively for files matching the expected format
    ext_map = {
        FormatType.JSON: (".json",),
        FormatType.NDJSON: (".ndjson", ".jsonl"),
        FormatType.CSV: (".csv",),
        FormatType.TSV: (".tsv",),
        FormatType.PSV: (".psv",),
        FormatType.XML: (".xml",),
        FormatType.SQLITE: (".db", ".sqlite", ".sqlite3"),
        FormatType.AVRO: (".avro",)
    }
    valid_exts = ext_map.get(active_format, (".json",))

    matches = [n for n in names if n.lower().endswith(valid_exts) and not n.startswith("__MACOSX")]

    if not matches:
        raise IncorporatorFormatError(f"Archive contains no files matching {active_format.value}.")

    if len(matches) > 1:
        # 3. Prevent unpredictable guessing!
        raise IncorporatorFormatError(
            f"Archive contains multiple valid {active_format.value} files {matches}. "
            f"Please specify which one to extract using the 'archive_target' kwarg."
        )

    return matches[0]


# ==========================================
# DECOMPRESSION STRATEGIES
# ==========================================
# ==========================================
# DECOMPRESSION STRATEGIES
# ==========================================

def _decompress_native_stream(
        data: Union[str, bytes], comp_type: CompressionType, active_format: FormatType, archive_target: Optional[str]
) -> str:
    """Handles 1-to-1 native Python compression algorithms."""
    if isinstance(data, str):
        path = Path(data).resolve()
        if comp_type == CompressionType.GZIP:
            with gzip.open(path, "rt", encoding="utf-8") as gz_f:
                return str(gz_f.read())
        elif comp_type == CompressionType.BZ2:
            with bz2.open(path, "rt", encoding="utf-8") as bz_f:
                return str(bz_f.read())
        elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
            with lzma.open(path, "rt", encoding="utf-8") as lz_f:
                return str(lz_f.read())

    elif isinstance(data, bytes):
        if comp_type == CompressionType.GZIP:
            return gzip.decompress(data).decode("utf-8")
        elif comp_type == CompressionType.BZ2:
            return bz2.decompress(data).decode("utf-8")
        elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
            return lzma.decompress(data).decode("utf-8")

    raise IncorporatorFormatError(f"Unsupported native stream type: {comp_type}")


def _decompress_archive(
        data: Union[str, bytes], comp_type: CompressionType, active_format: FormatType, archive_target: Optional[str]
) -> str:
    """Handles multi-file archives, seeking out the specific target data file safely."""
    if comp_type == CompressionType.ZIP:
        file_obj = Path(data).resolve() if isinstance(data, str) else io.BytesIO(data)
        with zipfile.ZipFile(file_obj, "r") as zf:
            # 🛡️ FIX: Pass the format and target securely down to the finder!
            zip_target = _find_target_in_archive(zf.namelist(), active_format, archive_target)
            with zf.open(zip_target) as zip_io:
                return zip_io.read().decode("utf-8")

    if comp_type in (CompressionType.TAR, CompressionType.TGZ):
        file_args = {"name": Path(data).resolve()} if isinstance(data, str) else {"fileobj": io.BytesIO(data)}
        with tarfile.open(**file_args, mode="r:*") as tf:
            # For strict safety, we must read the headers to ensure no ambiguity exists in the tarball
            members = [m for m in tf.getmembers() if m.isfile()]
            names = [m.name for m in members]

            # 🛡️ FIX: Pass the format and target securely down to the finder!
            tar_target_name = _find_target_in_archive(names, active_format, archive_target)
            tar_target = next(m for m in members if m.name == tar_target_name)

            tar_io = tf.extractfile(tar_target)
            if tar_io:
                return tar_io.read().decode("utf-8")
            raise IncorporatorFormatError("Failed to extract target from Tar archive.")

    raise IncorporatorFormatError(f"Unsupported archive: {comp_type}")


def _decompress_cramjam(
        data: Union[str, bytes], comp_type: CompressionType, active_format: FormatType, archive_target: Optional[str]
) -> str:
    """Lazy-loads Cramjam Rust bindings for high-performance enterprise algorithms."""
    try:
        import cramjam  # type: ignore[import-not-found]

        if comp_type == CompressionType.ZSTD:
            cj_engine = cramjam.zstd.decompress
        elif comp_type == CompressionType.LZ4:
            cj_engine = cramjam.lz4.decompress
        elif comp_type == CompressionType.SNAPPY:
            cj_engine = cramjam.snappy.decompress
        elif comp_type == CompressionType.BROTLI:
            cj_engine = cramjam.brotli.decompress
        else:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")

        if isinstance(data, str):
            with open(Path(data).resolve(), "rb") as f:
                return cast(bytes, cj_engine(f.read())).decode("utf-8")
        return cast(bytes, cj_engine(data)).decode("utf-8")

    except ImportError:
        raise IncorporatorFormatError(f"{comp_type.value} requires cramjam. Run: pip install incorporator[cramjam]")


# ==========================================
# COMPRESSION STRATEGIES
# ==========================================

def _compress_native_stream(src: Path, out_path: Path, comp_type: CompressionType) -> None:
    if comp_type == CompressionType.GZIP:
        with open(src, "rb") as f_in_gz, gzip.open(out_path, "wb") as f_out_gz:
            shutil.copyfileobj(f_in_gz, f_out_gz)
    elif comp_type == CompressionType.BZ2:
        with open(src, "rb") as f_in_bz, bz2.open(out_path, "wb") as f_out_bz:
            shutil.copyfileobj(f_in_bz, f_out_bz)
    elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
        with open(src, "rb") as f_in_lz, lzma.open(out_path, "wb") as f_out_lz:
            shutil.copyfileobj(f_in_lz, f_out_lz)


def _compress_archive(src: Path, out_path: Path, comp_type: CompressionType) -> None:
    if comp_type == CompressionType.ZIP:
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(src, arcname=src.name)
    elif comp_type == CompressionType.TGZ:
        with tarfile.open(out_path, "w:gz") as tf_gz:
            tf_gz.add(src, arcname=src.name)
    elif comp_type == CompressionType.TAR:
        with tarfile.open(out_path, "w") as tf_w:
            tf_w.add(src, arcname=src.name)


def _compress_cramjam(src: Path, out_path: Path, comp_type: CompressionType) -> None:
    try:
        import cramjam

        if comp_type == CompressionType.ZSTD:
            cj_engine = cramjam.zstd.compress
        elif comp_type == CompressionType.LZ4:
            cj_engine = cramjam.lz4.compress
        elif comp_type == CompressionType.SNAPPY:
            cj_engine = cramjam.snappy.compress
        elif comp_type == CompressionType.BROTLI:
            cj_engine = cramjam.brotli.compress
        else:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")

        with open(src, "rb") as f_in, open(out_path, "wb") as f_out:
            # Cast to bytes so mypy allows the file write
            f_out.write(cast(bytes, cj_engine(f_in.read())))

    except ImportError:
        raise IncorporatorFormatError(f"{comp_type.value} requires cramjam. Run: pip install incorporator[cramjam]")


# ==========================================
# REGISTRY & PUBLIC API
# ==========================================

_DECOMPRESS_ROUTER: Dict[CompressionType, Callable[[Union[str, bytes], CompressionType, FormatType, Optional[str]], str]] = {
    CompressionType.GZIP: _decompress_native_stream,
    CompressionType.BZ2: _decompress_native_stream,
    CompressionType.XZ: _decompress_native_stream,
    CompressionType.LZMA: _decompress_native_stream,
    CompressionType.ZIP: _decompress_archive,
    CompressionType.TAR: _decompress_archive,
    CompressionType.TGZ: _decompress_archive,
    CompressionType.ZSTD: _decompress_cramjam,
    CompressionType.LZ4: _decompress_cramjam,
    CompressionType.SNAPPY: _decompress_cramjam,
    CompressionType.BROTLI: _decompress_cramjam,
}

_COMPRESS_ROUTER: Dict[CompressionType, Callable[[Path, Path, CompressionType], None]] = {
    CompressionType.GZIP: _compress_native_stream,
    CompressionType.BZ2: _compress_native_stream,
    CompressionType.XZ: _compress_native_stream,
    CompressionType.LZMA: _compress_native_stream,
    CompressionType.ZIP: _compress_archive,
    CompressionType.TAR: _compress_archive,
    CompressionType.TGZ: _compress_archive,
    CompressionType.ZSTD: _compress_cramjam,
    CompressionType.LZ4: _compress_cramjam,
    CompressionType.SNAPPY: _compress_cramjam,
    CompressionType.BROTLI: _compress_cramjam,
}


def decompress_data(
        data: Union[str, bytes],
        path_hint: str,
        active_format: FormatType,
        archive_target: Optional[str] = None
) -> str:
    """Public API to transparently decompress data."""
    comp_type = infer_compression(path_hint)

    if not comp_type:
        return data.decode("utf-8") if isinstance(data, bytes) else str(data)

    try:
        handler = _DECOMPRESS_ROUTER[comp_type]
        return handler(data, comp_type, active_format, archive_target)
    except Exception as e:
        raise IncorporatorFormatError(f"Failed to decompress {comp_type.value} data: {e}") from e


def compress_file(source_path: str, comp_type: Union[str, CompressionType]) -> str:
    """Public API to compress a local file, automatically removing the uncompressed source."""
    src = Path(source_path).resolve()
    if not src.is_file():
        raise IncorporatorFormatError(f"Cannot compress missing file: {source_path}")

    if isinstance(comp_type, str):
        try:
            comp_type = CompressionType(comp_type.lower())
        except ValueError:
            raise IncorporatorFormatError(f"Unsupported compression type: {comp_type}")

    out_path = src.with_suffix(src.suffix + f".{comp_type.value}")

    try:
        handler = _COMPRESS_ROUTER[comp_type]
        handler(src, out_path, comp_type)
    except Exception as e:
        raise IncorporatorFormatError(f"Failed to compress {src.name} to {comp_type.value}: {e}") from e

    try:
        src.unlink()  # Free disk space
    except OSError:
        pass

    return str(out_path)