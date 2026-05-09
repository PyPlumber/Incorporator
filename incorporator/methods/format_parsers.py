"""Native zero-bloat format converters and handlers for Incorporator."""

import asyncio
import logging
import csv
import io
import json
import re
import sqlite3
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Union, cast

from .exceptions import IncorporatorFormatError
from .schema_builder import sanitize_json_key

logger = logging.getLogger(__name__)

# GLOBAL SQLITE ADAPTERS: Let the C-driver auto-convert types! Zero Python loops required.
sqlite3.register_adapter(bool, int)
sqlite3.register_adapter(dict, lambda d: json.dumps(d))
sqlite3.register_adapter(list, lambda l: json.dumps(l))

def _deserialize_nested(val: Any) -> Any:
    """MODULAR HELPER: Shared O(1) auto-unflattening for both CSV and SQLite."""
    if isinstance(val, str) and val and ((val[0] == '{' and val[-1] == '}') or (val[0] == '[' and val[-1] == ']')):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            pass
    return val

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

    for comp in [".gz", ".bz2", ".xz", ".lzma", ".zip", ".tar", ".tgz", ".zst", ".lz4", ".snappy", ".br"]:
        if path_lower.endswith(comp):
            path_lower = path_lower[:-len(comp)]
            break

    if path_lower.endswith((".ndjson", ".jsonl")): return FormatType.NDJSON
    if path_lower.endswith(".tsv"): return FormatType.TSV
    if path_lower.endswith(".psv"): return FormatType.PSV
    if path_lower.endswith(".csv"): return FormatType.CSV
    if path_lower.endswith(".xml"): return FormatType.XML
    if path_lower.endswith((".db", ".sqlite", ".sqlite3")): return FormatType.SQLITE
    if path_lower.endswith(".avro"): return FormatType.AVRO
    return FormatType.JSON

def _ensure_string(source: Union[str, bytes, Path]) -> str:
    """Safety guard: Guarantees legacy text parsers always receive a UTF-8 string."""
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8")
    if isinstance(source, bytes):
        return source.decode("utf-8")
    return source

def _serialize_nested(val: Any) -> Any:
    """Safely serializes nested lists/dicts to JSON strings for flat format exports."""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return val


def _xml_to_dict(element: ET.Element) -> Dict[str, Any]:
    """Recursively converts an XML ElementTree into a Python dictionary."""
    result: Dict[str, Any] = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        child_dict: Dict[str, Any] = {}
        for child in children:
            child_result = _xml_to_dict(child)
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


def _check_xml_security(raw_data: str) -> None:
    """Pre-flight check to block DTDs and Entities (XXE) without external dependencies."""
    if re.search(r"<!(?:DOCTYPE|ENTITY)", raw_data, re.IGNORECASE):
        raise IncorporatorFormatError(
            "Security Policy Violation: XML DTDs and External Entities (XXE) are strictly "
            "blocked to prevent 'Billion Laughs' memory exhaustion attacks."
        )


class BaseFormatHandler(ABC):
    """Abstract Strategy for parsing and writing different data formats."""

    @abstractmethod
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Parses a string, raw bytes, or physical file path into dictionaries."""
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Writes dictionary data to disk in the target format."""
        pass


class JSONHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        raw_data = _ensure_string(source)
        try:
            # Cast the json.loads output so mypy knows it's a dict/list
            return cast(Union[Dict[str, Any], List[Dict[str, Any]]], json.loads(raw_data))
        except json.JSONDecodeError as e:
            raise IncorporatorFormatError(f"Invalid JSON: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        try:
            path = Path(file_path).resolve()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except OSError as e:
            raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e


class NDJSONHandler(BaseFormatHandler):
    """Strategy for Newline-Delimited JSON (.ndjson / .jsonl)."""

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        raw_data = _ensure_string(source)
        rows: List[Dict[str, Any]] = []
        for line_num, line in enumerate(raw_data.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise IncorporatorFormatError(f"Invalid NDJSON on line {line_num}: {e}")
        return rows

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return
        try:
            path = Path(file_path).resolve()
            with open(path, 'w', encoding='utf-8') as f:
                for item in data:
                    f.write(json.dumps(item) + '\n')
        except OSError as e:
            raise IncorporatorFormatError(f"NDJSON File IO Error on {file_path}: {e}") from e


class CSVHandler(BaseFormatHandler):
    """Strategy for delimited flat files (CSV, TSV, PSV)."""

    def __init__(self, delimiter: str = ',') -> None:
        self.delimiter = delimiter

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        raw_data = _ensure_string(source)
        try:
            # Inject the custom delimiter into the DictReader
            reader = csv.DictReader(io.StringIO(raw_data), delimiter=self.delimiter)
            rows: List[Dict[str, Any]] = []

            for row in reader:
                parsed_row: Dict[str, Any] = {}
                for k, v in row.items():
                    safe_k = str(k) if k is not None else "unknown_column"

                    if v and (
                            (v[0] == '{' and v[-1] == '}') or
                            (v[0] == '[' and v[-1] == ']')
                    ):
                        try:
                            parsed_row[safe_k] = json.loads(v)
                        except json.JSONDecodeError:
                            parsed_row[safe_k] = v
                    else:
                        parsed_row[safe_k] = v

                rows.append(parsed_row)
            return rows
        except csv.Error as e:
            raise IncorporatorFormatError(f"Invalid Delimited Format: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return

        try:
            path = Path(file_path).resolve()
            with open(path, 'w', encoding='utf-8', newline='') as f:
                processed_data = [{k: _serialize_nested(v) for k, v in row.items()} for row in data]

                # 🛡️ Inject the custom delimiter into the DictWriter
                writer = csv.DictWriter(f, fieldnames=list(processed_data[0].keys()), delimiter=self.delimiter)
                writer.writeheader()
                writer.writerows(processed_data)
        except OSError as e:
            raise IncorporatorFormatError(f"Delimited File IO Error on {file_path}: {e}") from e


class XMLHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        raw_data = _ensure_string(source)
        _check_xml_security(raw_data)
        try:
            # DSA: Try O(1) memory parsing first without .strip() string duplication
            root = ET.fromstring(raw_data)
            return _xml_to_dict(root)
        except ET.ParseError:
            try:
                # Fallback: Only duplicate memory if strict parser chokes on leading whitespace
                root = ET.fromstring(raw_data.strip())  # noqa: S314
                return _xml_to_dict(root)
            except ET.ParseError as e:
                raise IncorporatorFormatError(f"Invalid XML: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        try:
            path = Path(file_path).resolve()
            with open(path, "w", encoding="utf-8") as f:
                root = ET.Element("root")
                for item in data:
                    item_el = ET.SubElement(root, "item")
                    for key, val in item.items():
                        clean_key = str(key).replace(" ", "_")
                        # Tag names must not start with a number in XML
                        if clean_key and clean_key[0].isdigit():
                            clean_key = f"_{clean_key}"

                        child = ET.SubElement(item_el, clean_key)
                        safe_val = _serialize_nested(val)
                        child.text = str(safe_val) if safe_val is not None else ""

                tree = ET.ElementTree(root)
                tree.write(f, encoding="unicode")
        except OSError as e:
            raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e


class SQLiteHandler(BaseFormatHandler):
    """Strategy for reading and writing local SQLite binary databases."""

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        if not isinstance(source, Path):
            raise IncorporatorFormatError("SQLiteHandler requires a physical Path object.")

        query = kwargs.get("sql_query")
        if not query:
            raise IncorporatorFormatError(
                "Reading from SQLite requires an 'sql_query' kwarg (e.g., 'SELECT * FROM table').")

        try:
            with sqlite3.connect(source) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query)

                rows: List[Dict[str, Any]] = []
                for row in cursor.fetchall():
                    parsed_row = dict(row)
                    # Reuse the shared unflattening helper
                    for k, v in parsed_row.items():
                        parsed_row[k] = _deserialize_nested(v)
                    rows.append(parsed_row)

                return rows

        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Read Error: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return

        table_name = kwargs.get("sql_table", "incorporator_export")
        if_exists = kwargs.get("if_exists", "replace")
        path = Path(file_path).resolve()

        try:
            with sqlite3.connect(path) as conn:
                cursor = conn.cursor()

                if if_exists == "replace":
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                elif if_exists == "fail":
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if cursor.fetchone():
                        raise IncorporatorFormatError(f"Table '{table_name}' already exists in {path.name}.")

                # LEVERAGING SCHEMA_BUILDER: Ensure column names perfectly match Pydantic PEP 8 rules
                keys = list(data[0].keys())
                safe_columns = [f'"{sanitize_json_key(k)}"' for k in keys]

                # SQLite natively uses Dynamic Typing (Manifest Typing). We don't need to guess INTEGER vs TEXT!
                create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(safe_columns)})'
                cursor.execute(create_stmt)

                # C-SPEED INSERTION: Our global adapters handle lists, dicts, and bools instantly.
                placeholders = ", ".join(["?"] * len(keys))
                insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

                # Extract pure tuples without running manual type-checking loops
                processed_data = [tuple(row.get(k) for k in keys) for row in data]

                cursor.executemany(insert_stmt, processed_data)
                conn.commit()

        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Write Error: {e}")


class AvroHandler(BaseFormatHandler):
    """Optional Strategy for reading and writing Apache Avro binary streams."""

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            import fastavro
        except ImportError:
            raise IncorporatorFormatError("fastavro not installed. Run: pip install incorporator[avro]")

        try:
            if isinstance(source, Path):
                with open(source, "rb") as f:
                    records = list(fastavro.reader(f))
            elif isinstance(source, bytes):
                records = list(fastavro.reader(io.BytesIO(source)))
            else:
                raise IncorporatorFormatError("AvroHandler requires raw bytes or a physical Path object.")

            # MODULAR SYMMETRY: Auto-unflatten nested JSON strings back into Python objects
            rows: List[Dict[str, Any]] = []
            for raw_row in records:
                if not isinstance(raw_row, dict):
                    continue

                valid_row = cast(Dict[str, Any], raw_row)

                parsed_row = {}
                for k, v in valid_row.items():
                    parsed_row[k] = _deserialize_nested(v)
                rows.append(parsed_row)

            return rows

        except Exception as e:
            raise IncorporatorFormatError(f"Avro Read Error: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return

        try:
            import fastavro
        except ImportError:
            raise IncorporatorFormatError("fastavro not installed. Run: pip install incorporator[avro]")

        path = Path(file_path).resolve()

        # 1. APPLY MODULAR PIPELINE: Sanitize keys & Serialize nested structures FIRST
        processed_data = []
        for row in data:
            processed_row = {}
            for k, v in row.items():
                safe_k = sanitize_json_key(k)
                processed_row[safe_k] = _serialize_nested(v)
            processed_data.append(processed_row)

        # 2. INFER SCHEMA FROM PYDANTIC: Use the strict types enforced by converters.py!
        pydantic_schema = kwargs.get("pydantic_schema", {})
        properties = pydantic_schema.get("properties", {})

        fields = []
        for k, prop in properties.items():
            # Extract type (Pydantic V2 wraps Optional types in 'anyOf')
            json_type = prop.get("type")
            if not json_type and "anyOf" in prop:
                for sub in prop["anyOf"]:
                    if sub.get("type") and sub.get("type") != "null":
                        json_type = sub.get("type")
                        break

            # Map JSON Schema types natively to Avro primitives
            if json_type == "integer":
                a_type = "long"
            elif json_type == "number":
                a_type = "double"
            elif json_type == "boolean":
                a_type = "boolean"
            else:
                a_type = "string"

            fields.append({"name": sanitize_json_key(k), "type": ["null", a_type]})

        record_name = sanitize_json_key(kwargs.get("sql_table", "IncorporatorRecord"))
        parsed_schema = fastavro.parse_schema({
            "doc": "Auto-generated by Incorporator",
            "name": record_name,
            "type": "record",
            "fields": fields
        })

        # 3. WRITE: Execute the binary stream
        try:
            with open(path, "wb") as f:
                fastavro.writer(f, parsed_schema, processed_data)
        except Exception as e:
            raise IncorporatorFormatError(f"Avro Write Error: {e}")

# Registry mapping Enums to their respective Handler instances
_HANDLERS: Dict[FormatType, BaseFormatHandler] = {
    FormatType.JSON: JSONHandler(),
    FormatType.NDJSON: NDJSONHandler(),
    FormatType.CSV: CSVHandler(delimiter=','),
    FormatType.TSV: CSVHandler(delimiter='\t'),
    FormatType.PSV: CSVHandler(delimiter='|'),
    FormatType.XML: XMLHandler(),
    FormatType.SQLITE: SQLiteHandler(),
    FormatType.AVRO: AvroHandler(),
}

async def parse_source_data(
        source: Union[str, bytes, Path],
        format_type: FormatType,
        **kwargs: Any
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Asynchronously routes raw inputs to the correct parser strategy."""
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported format: '{format_type}'.")

    try:
        return await asyncio.to_thread(handler.parse, source, **kwargs)

    except Exception as e:
        snippet = str(source).strip()[:60].replace('\n', ' ')
        logger.warning(
            f"⚠️ PARSE FAILED for format '{format_type}'. "
            f"The payload may be malformed (e.g., corrupted file or HTML firewall). "
            f"\n   Error: {e}\n   Received snippet: {snippet!r}..."
        )
        return []


async def write_destination_data(
        data: List[Dict[str, Any]],
        file_path: Union[str, Path],
        format_type: FormatType,
        **kwargs: Any
) -> None:
    """Asynchronously routes dictionary data to the correct writer strategy."""
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported export format: '{format_type}'.")

    # Offload I/O-bound writing to a background thread to protect the event loop
    await asyncio.to_thread(handler.write, data, file_path, **kwargs)
