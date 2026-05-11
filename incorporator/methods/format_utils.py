"""
Format Utility Algorithms for Incorporator.
Contains purely functional data sanitization, recursion, and format inference.
"""

import json
import re
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Union

from .exceptions import IncorporatorFormatError


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


def xml_to_dict(element: Any) -> Dict[str, Any]:
    """Recursively converts an XML ElementTree (standard or lxml) into a Python dictionary."""
    result: Dict[str, Any] = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        child_dict: Dict[str, Any] = {}
        for child in children:
            child_result = xml_to_dict(child)
            for key, val in child_result.items():
                if key in child_dict:
                    if not isinstance(child_dict[key], list):
                        child_dict[key] = [child_dict[key]]
                    child_dict[key].append(val)
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
    if re.search(r"<!(?:DOCTYPE|ENTITY)", raw_data, re.IGNORECASE):
        raise IncorporatorFormatError(
            "Security Policy Violation: XML DTDs and External Entities (XXE) are strictly "
            "blocked to prevent 'Billion Laughs' memory exhaustion attacks."
        )
