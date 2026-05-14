"""
Format Utility Algorithms for Incorporator.
Contains purely functional data sanitization, recursion, and format inference.
"""

import json
import re
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, Union

from ..exceptions import IncorporatorFormatError


class FormatType(str, Enum):
    """Strict enumeration of supported data formats."""

    JSON = "json"
    NDJSON = "ndjson"
    CSV = "csv"
    TSV = "tsv"
    PSV = "psv"
    XML = "xml"
    SQLITE = "sqlite"
    AVRO = "avro"
    XLSX = "xlsx"
    PARQUET = "parquet"
    FEATHER = "feather"
    ORC = "orc"
    HTML = "html"


# ── (FormatType, format-type-string) → Python type ──────────────────────
FORMAT_TO_PYTHON: Dict[Tuple[FormatType, str], type] = {
    # JSON Schema type strings
    (FormatType.JSON, "boolean"): bool,
    (FormatType.JSON, "integer"): int,
    (FormatType.JSON, "number"): float,
    (FormatType.JSON, "string"): str,
    (FormatType.JSON, "array"): list,
    (FormatType.JSON, "object"): dict,
    (FormatType.JSON, "null"): type(None),
    # Avro type strings
    (FormatType.AVRO, "null"): type(None),
    (FormatType.AVRO, "boolean"): bool,
    (FormatType.AVRO, "int"): int,
    (FormatType.AVRO, "long"): int,
    (FormatType.AVRO, "float"): float,
    (FormatType.AVRO, "double"): float,
    (FormatType.AVRO, "bytes"): bytes,
    (FormatType.AVRO, "string"): str,
    # Parquet logical types (pyarrow's str() of pa.DataType — int64, double, etc.)
    (FormatType.PARQUET, "bool"): bool,
    (FormatType.PARQUET, "int32"): int,
    (FormatType.PARQUET, "int64"): int,
    (FormatType.PARQUET, "float"): float,
    (FormatType.PARQUET, "double"): float,
    (FormatType.PARQUET, "string"): str,
    (FormatType.PARQUET, "binary"): bytes,
    (FormatType.PARQUET, "null"): type(None),
    # Feather (Arrow IPC) and ORC share the same Arrow logical type system.
    (FormatType.FEATHER, "bool"): bool,
    (FormatType.FEATHER, "int32"): int,
    (FormatType.FEATHER, "int64"): int,
    (FormatType.FEATHER, "float"): float,
    (FormatType.FEATHER, "double"): float,
    (FormatType.FEATHER, "string"): str,
    (FormatType.FEATHER, "binary"): bytes,
    (FormatType.FEATHER, "null"): type(None),
    (FormatType.ORC, "bool"): bool,
    (FormatType.ORC, "int32"): int,
    (FormatType.ORC, "int64"): int,
    (FormatType.ORC, "float"): float,
    (FormatType.ORC, "double"): float,
    (FormatType.ORC, "string"): str,
    (FormatType.ORC, "binary"): bytes,
    (FormatType.ORC, "null"): type(None),
}

# ── (FormatType, Python type) → canonical format-type-string ────────────
# bool is a subclass of int — entries for bool must exist so type(True) hits
# (fmt, bool) before any (fmt, int) logic. Each key is exact type(), so no
# inheritance ambiguity in practice, but explicit entries keep intent clear.
# Canonical Avro choice: long over int, double over float (wider range).
PYTHON_TO_FORMAT: Dict[Tuple[FormatType, type], str] = {
    # JSON Schema
    (FormatType.JSON, bool): "boolean",
    (FormatType.JSON, int): "integer",
    (FormatType.JSON, float): "number",
    (FormatType.JSON, str): "string",
    (FormatType.JSON, list): "array",
    (FormatType.JSON, dict): "object",
    (FormatType.JSON, type(None)): "null",
    # Avro
    (FormatType.AVRO, bool): "boolean",
    (FormatType.AVRO, int): "long",
    (FormatType.AVRO, float): "double",
    (FormatType.AVRO, str): "string",
    (FormatType.AVRO, bytes): "bytes",
    (FormatType.AVRO, list): "string",
    (FormatType.AVRO, dict): "string",
    (FormatType.AVRO, type(None)): "null",
    # Parquet — canonical widths. int64 over int32, double over float (wider range).
    # Nested types (list/dict) are flattened to JSON strings, mirroring CSV/Avro
    # behaviour so Parquet round-trips deterministically across the type bridge.
    (FormatType.PARQUET, bool): "bool",
    (FormatType.PARQUET, int): "int64",
    (FormatType.PARQUET, float): "double",
    (FormatType.PARQUET, str): "string",
    (FormatType.PARQUET, bytes): "binary",
    (FormatType.PARQUET, list): "string",
    (FormatType.PARQUET, dict): "string",
    (FormatType.PARQUET, type(None)): "null",
    # Feather (Arrow IPC) — identical canonical widths to Parquet.
    (FormatType.FEATHER, bool): "bool",
    (FormatType.FEATHER, int): "int64",
    (FormatType.FEATHER, float): "double",
    (FormatType.FEATHER, str): "string",
    (FormatType.FEATHER, bytes): "binary",
    (FormatType.FEATHER, list): "string",
    (FormatType.FEATHER, dict): "string",
    (FormatType.FEATHER, type(None)): "null",
    # ORC — same canonical widths.
    (FormatType.ORC, bool): "bool",
    (FormatType.ORC, int): "int64",
    (FormatType.ORC, float): "double",
    (FormatType.ORC, str): "string",
    (FormatType.ORC, bytes): "binary",
    (FormatType.ORC, list): "string",
    (FormatType.ORC, dict): "string",
    (FormatType.ORC, type(None)): "null",
}


def to_python_type(fmt: FormatType, type_str: str, default: type = str) -> type:
    """Return the Python type for a format-specific type string."""
    return FORMAT_TO_PYTHON.get((fmt, type_str), default)


def _to_format_type(fmt: FormatType, python_type: type, default: str = "string") -> str:
    """Return the canonical format type string for a Python type.

    Internal — currently only consumed indirectly via :func:`convert_type`'s
    PYTHON_TO_FORMAT lookup.  Kept as a named helper so future format
    backends can monkey-patch it without touching the registry.
    """
    return PYTHON_TO_FORMAT.get((fmt, python_type), default)


def convert_type(type_str: str, from_fmt: FormatType, to_fmt: FormatType, default: str = "string") -> str:
    """Translate a type string between two format type systems via the Python type bridge.

    Example: convert_type("integer", FormatType.JSON, FormatType.AVRO) → "long"
    To add a new format, extend FORMAT_TO_PYTHON and PYTHON_TO_FORMAT only.
    """
    python_type = FORMAT_TO_PYTHON.get((from_fmt, type_str))
    if python_type is None:
        return default
    return PYTHON_TO_FORMAT.get((to_fmt, python_type), default)


def infer_format(path_or_url: str) -> FormatType:
    """Helper to auto-detect format from a file extension or URL."""
    path_lower = str(path_or_url).lower()

    for comp in [
        ".gz",
        ".bz2",
        ".xz",
        ".lzma",
        ".zip",
        ".tar",
        ".tgz",
        ".zst",
        ".lz4",
        ".snappy",
        ".br",
    ]:
        if path_lower.endswith(comp):
            path_lower = path_lower[: -len(comp)]
            break

    if path_lower.endswith((".ndjson", ".jsonl")):
        return FormatType.NDJSON
    if path_lower.endswith(".tsv"):
        return FormatType.TSV
    if path_lower.endswith(".psv"):
        return FormatType.PSV
    if path_lower.endswith(".csv"):
        return FormatType.CSV
    if path_lower.endswith(".xml"):
        return FormatType.XML
    if path_lower.endswith((".db", ".sqlite", ".sqlite3")):
        return FormatType.SQLITE
    if path_lower.endswith(".avro"):
        return FormatType.AVRO
    if path_lower.endswith((".xlsx", ".xlsm")):
        return FormatType.XLSX
    if path_lower.endswith((".parquet", ".pq")):
        return FormatType.PARQUET
    if path_lower.endswith((".feather", ".arrow", ".ipc")):
        return FormatType.FEATHER
    if path_lower.endswith(".orc"):
        return FormatType.ORC
    if path_lower.endswith((".html", ".htm")):
        return FormatType.HTML
    return FormatType.JSON


def ensure_string(source: Union[str, bytes, Path]) -> str:
    """Fallback guard for legacy formatters that haven't been optimized for stream buffers."""
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, bytes):
        return source.decode("utf-8")
    return source


def serialize_nested(val: Any) -> Any:
    """Safely serializes nested lists/dicts to JSON strings for flat format exports."""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return val


def deserialize_nested(val: Any) -> Any:
    """MODULAR HELPER: Shared O(1) auto-unflattening for both CSV and SQLite."""
    if isinstance(val, str) and len(val) >= 2:
        if (val.startswith("{") and val.endswith("}")) or (val.startswith("[") and val.endswith("]")):
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                pass
    return val


def xml_to_dict(element: Any, force_list: Optional[Set[str]] = None) -> Dict[str, Any]:
    """Recursively converts an XML ElementTree (standard or lxml) into a Python dictionary.

    Tag-shape consistency: by default a tag that appears once becomes a
    scalar; repeated siblings become a list.  This causes shape drift when
    the *same* tag is sometimes single, sometimes multiple, across payloads.
    Pass ``force_list={"item", "row"}`` to force those tag names to always
    be lists — even when count is 1 — so downstream Pydantic schema
    inference sees a stable shape.
    """
    force_list = force_list or set()
    result: Dict[str, Any] = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        child_dict: Dict[str, Any] = {}
        for child in children:
            child_result = xml_to_dict(child, force_list=force_list)
            for key, val in child_result.items():
                if key in child_dict:
                    if not isinstance(child_dict[key], list):
                        child_dict[key] = [child_dict[key]]
                    child_dict[key].append(val)
                elif key in force_list:
                    # Force-list tags always start as a one-element list so
                    # downstream consumers see a stable list shape regardless
                    # of how many siblings the source XML actually has.
                    child_dict[key] = [val]
                else:
                    child_dict[key] = val
        if element.attrib:
            child_dict.update(element.attrib)
        result = {element.tag: child_dict}
    elif element.text and element.text.strip():
        val_text = element.text.strip()
        if element.attrib:
            leaf_dict: Dict[str, Any] = dict(element.attrib)
            leaf_dict["text"] = val_text
            result = {element.tag: leaf_dict}
        else:
            result = {element.tag: val_text}

    return result


def check_xml_security(raw_data: str) -> None:
    """Pre-flight check to block DTDs and Entities (XXE) without external dependencies."""
    if re.search(r"<!(?:DOCTYPE|ENTITY)|%[a-zA-Z_][\w.-]*;", raw_data, re.IGNORECASE | re.DOTALL):
        raise IncorporatorFormatError(
            "Security Policy Violation: XML DTDs and External Entities (XXE) are strictly "
            "blocked to prevent 'Billion Laughs' memory exhaustion attacks."
        )
