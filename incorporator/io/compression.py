"""
Modular Compression Engine for Incorporator.
Utilizes a Strategy Pattern to safely route between Native Python and Rust-backed Cramjam algorithms.
"""

import bz2
import functools
import gzip
import io
import lzma
import shutil
import tarfile
import zipfile
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, cast

from ..exceptions import IncorporatorFormatError
from .formats import FormatType


class CompressionType(str, Enum):
    """Every compression / archive format Incorporator can transparently decompress.

    Members fall into three families:

    * **Native streams** (always available): ``GZIP``, ``BZ2``, ``XZ``, ``LZMA``.
    * **Native archives** (always available): ``ZIP``, ``TAR``, ``TGZ`` —
      multi-file archives; pass ``archive_target="name"`` to select a specific
      member, or rely on extension-based auto-selection.
    * **Cramjam plugins** (requires ``[cramjam]`` extra): ``ZSTD``, ``LZ4``,
      ``SNAPPY``, ``BROTLI`` — Rust-backed for throughput.

    The enum value is the canonical file-extension suffix (without the dot)
    used by :func:`infer_compression` to detect compression from a path.
    """

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


# cramjam submodule names differ from our CompressionType enum values
_CRAMJAM_MODULE_MAP: Dict[CompressionType, str] = {
    CompressionType.ZSTD: "zstd",
    CompressionType.LZ4: "lz4",
    CompressionType.SNAPPY: "snappy",
    CompressionType.BROTLI: "brotli",
}


@functools.lru_cache(maxsize=4096)
def infer_compression(path_or_url: str) -> Optional[CompressionType]:
    """Detect the compression type from a file path or URL by its extension.

    Returns the matching :class:`CompressionType` member, or ``None`` when the
    path has no recognised compression suffix (e.g. plain ``.json`` /
    ``.csv``). Case-insensitive — ``data.JSON.GZ`` resolves to ``GZIP``.

    Cached: same callers / cardinality story as ``infer_format``.
    """
    path_lower = str(path_or_url).lower()
    for comp in CompressionType:
        if path_lower.endswith(f".{comp.value}"):
            return comp
    return None


def _is_binary(active_format: FormatType) -> bool:
    """Helper to determine if the target format requires raw bytes bypass."""
    return active_format in (FormatType.SQLITE, FormatType.AVRO)


def _find_target_in_archive(names: list[str], active_format: FormatType, archive_target: Optional[str] = None) -> str:
    """Finds a target file in an archive safely and predictably."""
    if archive_target:
        if archive_target in names:
            return archive_target
        raise IncorporatorFormatError(f"Target '{archive_target}' not found in archive.")

    ext_map = {
        FormatType.JSON: (".json",),
        FormatType.NDJSON: (".ndjson", ".jsonl"),
        FormatType.CSV: (".csv",),
        FormatType.TSV: (".tsv",),
        FormatType.PSV: (".psv",),
        FormatType.XML: (".xml",),
        FormatType.SQLITE: (".db", ".sqlite", ".sqlite3"),
        FormatType.AVRO: (".avro",),
    }
    valid_exts = ext_map.get(active_format, (".json",))

    # Path traversal & MACOSX junk protection
    matches = [n for n in names if n.lower().endswith(valid_exts) and not n.startswith("__MACOSX") and ".." not in n]

    if not matches:
        raise IncorporatorFormatError(f"Archive contains no files matching {active_format.value}.")

    if len(matches) > 1:
        raise IncorporatorFormatError(
            f"Archive contains multiple valid {active_format.value} files {matches}. "
            f"Please specify which one to extract using the 'archive_target' kwarg."
        )

    return matches[0]


# ==========================================
# DECOMPRESSION STRATEGIES
# ==========================================


# Hard ceiling on decompressed payload size.  Defends against decompression
# bombs (gzip / zstd / brotli payloads where 1 KB of compressed data expands
# to gigabytes).  Override via the INCORPORATOR_MAX_DECOMPRESSED_BYTES env
# var when the workload legitimately exceeds 1 GB; default is conservative.
import os as _os  # noqa: E402

DEFAULT_MAX_DECOMPRESSED_BYTES: int = 1 * 1024 * 1024 * 1024  # 1 GB


def _max_decompressed_bytes() -> int:
    """Read the bomb-cap ceiling from the env each call.

    Per-call read (not cached) so test fixtures can monkeypatch the env var
    without re-importing the module.  The check fires only on
    decompression, which is rare relative to other hot paths — the env
    lookup cost is negligible there.
    """
    raw = _os.environ.get("INCORPORATOR_MAX_DECOMPRESSED_BYTES")
    if not raw:
        return DEFAULT_MAX_DECOMPRESSED_BYTES
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_MAX_DECOMPRESSED_BYTES


def _enforce_size_cap(decompressed_size: int, comp_type: CompressionType) -> None:
    """Raise when a decompressed payload exceeds the configured cap.

    Called by every decompression strategy AFTER the bytes are produced
    (the native streams + cramjam don't expose a streaming size-bounded
    API).  This is post-hoc but still bounds RAM since the worst case is
    one bomb payload's worth — better than letting a 1-byte input become
    a 100 GB explosion.
    """
    cap = _max_decompressed_bytes()
    if decompressed_size > cap:
        raise IncorporatorFormatError(
            f"Decompression bomb blocked: {comp_type.value!r} payload would "
            f"expand to {decompressed_size:,} bytes (cap = {cap:,}). "
            f"Set INCORPORATOR_MAX_DECOMPRESSED_BYTES if the workload "
            f"legitimately needs more headroom."
        )


def _decompress_native_stream(
    data: Union[str, bytes],
    comp_type: CompressionType,
    active_format: FormatType,
    archive_target: Optional[str],
) -> Union[str, bytes]:
    """Handles 1-to-1 native Python compression algorithms with Binary Bypass."""
    is_bin = _is_binary(active_format)
    mode = "rb" if is_bin else "rt"
    encoding = None if is_bin else "utf-8"

    if isinstance(data, str):
        path = Path(data).resolve()
        try:
            if comp_type == CompressionType.GZIP:
                with gzip.open(path, mode, encoding=encoding) as gz_f:
                    decompressed = gz_f.read()
            elif comp_type == CompressionType.BZ2:
                with bz2.open(path, mode, encoding=encoding) as bz_f:
                    decompressed = bz_f.read()
            elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
                with lzma.open(path, mode, encoding=encoding) as lz_f:
                    decompressed = lz_f.read()
            else:
                raise IncorporatorFormatError(f"Unsupported native stream type: {comp_type}")
            _enforce_size_cap(
                len(decompressed) if isinstance(decompressed, (bytes, str)) else 0,
                comp_type,
            )
            return decompressed
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"Native stream extraction failed: {e}") from e

    elif isinstance(data, bytes):
        if comp_type == CompressionType.GZIP:
            raw = gzip.decompress(data)
        elif comp_type == CompressionType.BZ2:
            raw = bz2.decompress(data)
        elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
            raw = lzma.decompress(data)
        else:
            raise IncorporatorFormatError(f"Unsupported native stream type: {comp_type}")
        _enforce_size_cap(len(raw), comp_type)

        return raw if is_bin else raw.decode("utf-8")

    raise IncorporatorFormatError("Data must be a filepath string or bytes.")


def _validate_archive_member_names(names: List[str], archive_kind: str) -> None:
    """Guard against archive path traversal for ZIP and TAR alike.

    Validates every member name against a resolved safe temp directory so
    ``../../etc/passwd`` and ``/etc/passwd``-style names are blocked before
    any extraction takes place.  Also flags absolute paths and Windows-style
    drive prefixes that ``Path.resolve()`` may not catch on POSIX.

    ``archive_kind`` is used only for the error message ("ZIP" vs "TAR").
    """
    import tempfile

    safe_dir = Path(tempfile.mkdtemp()).resolve()
    try:
        for name in names:
            # Explicit absolute / drive-prefix rejection: Path.resolve()
            # collapses POSIX absolutes ("/etc/passwd") into safe_dir on
            # some platforms; reject them up-front for clarity.
            if name.startswith(("/", "\\")) or (len(name) >= 2 and name[1] == ":"):
                raise IncorporatorFormatError(f"{archive_kind} path traversal blocked (absolute path): {name!r}")
            member_path = (safe_dir / name).resolve()
            if not str(member_path).startswith(str(safe_dir)):
                raise IncorporatorFormatError(f"{archive_kind} path traversal blocked: {name!r}")
    finally:
        # Clean up the temp directory immediately — we only needed it for resolve()
        shutil.rmtree(safe_dir, ignore_errors=True)


def _validate_tar_members(members: List[Any]) -> None:
    """Backwards-compatible wrapper that funnels TAR members through the
    shared archive-name validator.  Kept as a thin alias so existing call
    sites stay untouched.
    """
    _validate_archive_member_names([m.name for m in members], archive_kind="TAR")


def _decompress_archive(
    data: Union[str, bytes],
    comp_type: CompressionType,
    active_format: FormatType,
    archive_target: Optional[str],
) -> Union[str, bytes]:
    """Handles multi-file archives, seeking out the specific target safely."""
    is_bin = _is_binary(active_format)

    if comp_type == CompressionType.ZIP:
        file_obj = Path(data).resolve() if isinstance(data, str) else io.BytesIO(data)
        with zipfile.ZipFile(file_obj, "r") as zf:
            # Block ZIP slip / path traversal before extraction.  Mirrors the
            # TAR validator at lines above — pre-fix, ZIP had no equivalent
            # guard, so a malicious member name like '../../etc/passwd' could
            # escape the extraction scope.
            _validate_archive_member_names(zf.namelist(), archive_kind="ZIP")
            zip_target = _find_target_in_archive(zf.namelist(), active_format, archive_target)
            with zf.open(zip_target) as zip_io:
                raw_bytes = zip_io.read()
            _enforce_size_cap(len(raw_bytes), comp_type)
            return raw_bytes if is_bin else raw_bytes.decode("utf-8")

    if comp_type in (CompressionType.TAR, CompressionType.TGZ):
        file_args = {"name": Path(data).resolve()} if isinstance(data, str) else {"fileobj": io.BytesIO(data)}
        with tarfile.open(**file_args, mode="r:*") as tf:
            members = [m for m in tf.getmembers() if m.isfile()]
            _validate_tar_members(members)  # Block path traversal before any extraction
            names = [m.name for m in members]

            tar_target_name = _find_target_in_archive(names, active_format, archive_target)
            tar_target = next(m for m in members if m.name == tar_target_name)

            tar_io = tf.extractfile(tar_target)
            if tar_io:
                raw_bytes = tar_io.read()
                _enforce_size_cap(len(raw_bytes), comp_type)
                return raw_bytes if is_bin else raw_bytes.decode("utf-8")
            raise IncorporatorFormatError("Failed to extract target from Tar archive.")

    raise IncorporatorFormatError(f"Unsupported archive: {comp_type}")


def _decompress_cramjam(
    data: Union[str, bytes],
    comp_type: CompressionType,
    active_format: FormatType,
    archive_target: Optional[str],
) -> Union[str, bytes]:
    """Lazy-loads Cramjam Rust bindings with structural binary bypass."""
    try:
        import cramjam  # type: ignore[import-not-found, import-untyped, unused-ignore]

        module_name = _CRAMJAM_MODULE_MAP.get(comp_type)
        if not module_name:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")
        cj_module = getattr(cramjam, module_name, None)
        if not cj_module:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")

        is_bin = _is_binary(active_format)

        if isinstance(data, str):
            with open(Path(data).resolve(), "rb") as f:
                # cramjam ≥2.x returns a Buffer object, not plain bytes — wrap in bytes()
                raw_bytes = bytes(cj_module.decompress(f.read()))
            _enforce_size_cap(len(raw_bytes), comp_type)
            return raw_bytes if is_bin else raw_bytes.decode("utf-8")

        # cramjam ≥2.x returns a Buffer object, not plain bytes — wrap in bytes()
        raw_bytes = bytes(cj_module.decompress(data))
        _enforce_size_cap(len(raw_bytes), comp_type)
        return raw_bytes if is_bin else raw_bytes.decode("utf-8")

    except ImportError:
        raise IncorporatorFormatError(
            f"{comp_type.value} requires cramjam. Run: pip install incorporator[cramjam]"
        ) from None


# ==========================================
# COMPRESSION STRATEGIES
# ==========================================


def _compress_native_stream(src: Path, out_path: Path, comp_type: CompressionType) -> None:
    """Uses shutil.copyfileobj to stream directly from disk to disk (OOM safe)."""
    if comp_type == CompressionType.GZIP:
        with open(src, "rb") as f_in, gzip.open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    elif comp_type == CompressionType.BZ2:
        with open(src, "rb") as f_in, bz2.open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    elif comp_type in (CompressionType.XZ, CompressionType.LZMA):
        with open(src, "rb") as f_in, lzma.open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


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
    """
    OOM Safe Implementation: Uses 1MB chunked reading with Cramjam's streaming
    Compressor objects if available, gracefully degrading if required.
    """
    try:
        import cramjam

        module_name = _CRAMJAM_MODULE_MAP.get(comp_type)
        if not module_name:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")
        cj_module = getattr(cramjam, module_name, None)
        if not cj_module:
            raise IncorporatorFormatError(f"Unsupported cramjam format: {comp_type}")

        with open(src, "rb") as f_in, open(out_path, "wb") as f_out:
            if hasattr(cj_module, "Compressor"):
                compressor = cj_module.Compressor()
                while chunk := f_in.read(1024 * 1024):  # 1MB Chunks
                    # cramjam ≥2.x: compress() returns int (bytes consumed), not bytes.
                    # Compressed output is retrieved via finish() / flush() at the end.
                    compressor.compress(chunk)

                # Drain the compressor output buffer
                if hasattr(compressor, "finish"):
                    f_out.write(bytes(compressor.finish()))
                elif hasattr(compressor, "flush"):
                    f_out.write(bytes(compressor.flush()))
            else:
                # Fallback for older cramjam installations
                f_out.write(cast(bytes, cj_module.compress(f_in.read())))

    except ImportError:
        raise IncorporatorFormatError(
            f"{_CRAMJAM_MODULE_MAP.get(comp_type, comp_type.value)} requires cramjam. "
            f"Run: pip install incorporator[cramjam]"
        ) from None


# ==========================================
# REGISTRY & PUBLIC API
# ==========================================

_DECOMPRESS_ROUTER: Dict[
    CompressionType,
    Callable[[Union[str, bytes], CompressionType, FormatType, Optional[str]], Union[str, bytes]],
] = {
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


def _assert_router_coverage() -> None:
    """Validate at import time that both routers cover every CompressionType member.

    Raises RuntimeError on startup rather than a silent KeyError at runtime
    when a newly added CompressionType is missing from a router.
    """
    missing_decomp = [m for m in CompressionType if m not in _DECOMPRESS_ROUTER]
    missing_comp = [m for m in CompressionType if m not in _COMPRESS_ROUTER]
    if missing_decomp:
        raise RuntimeError(f"_DECOMPRESS_ROUTER missing entries: {missing_decomp}")
    if missing_comp:
        raise RuntimeError(f"_COMPRESS_ROUTER missing entries: {missing_comp}")


_assert_router_coverage()


def decompress_data(
    data: Union[str, bytes],
    path_hint: str,
    active_format: FormatType,
    archive_target: Optional[str] = None,
) -> Union[str, bytes]:
    """Public API to transparently decompress data."""
    comp_type = infer_compression(path_hint)

    if not comp_type:
        if _is_binary(active_format):
            return data if isinstance(data, bytes) else str(data).encode("utf-8")
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
            raise IncorporatorFormatError(f"Unsupported compression type: {comp_type}") from None

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
