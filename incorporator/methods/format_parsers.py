"""Native zero-bloat format I/O handlers and strategies for Incorporator."""

import asyncio
import csv
import io
import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, List, TextIO, Union, cast

from .exceptions import IncorporatorFormatError
from .format_utils import (
    FormatType,
    check_xml_security,
    deserialize_nested,
    ensure_string,
    infer_format,
    serialize_nested,
    xml_to_dict,
)
from .schema_builder import sanitize_json_key

logger = logging.getLogger(__name__)

# To prevent breaking changes to base.py, network.py, or compression.py,
# we explicitly re-export the format tools they expect to find here.
__all__ = ["FormatType", "infer_format", "parse_source_data", "write_destination_data"]


def _raise_if_append_unsupported(kwargs: Dict[str, Any], format_name: str) -> None:
    if kwargs.get("if_exists") == "append":
        raise IncorporatorFormatError(
            f"Monolithic formats ({format_name}) do not support O(1) streaming appends. "
            "Please stream to NDJSON, CSV, SQLite, or Avro instead."
        )


class BaseFormatHandler(ABC):
    """Abstract Strategy for parsing and writing different data formats."""

    @abstractmethod
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        pass


class JSONHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

            if isinstance(source, Path):
                raw_data = source.read_bytes()
            elif isinstance(source, str):
                raw_data = source.encode("utf-8")
            else:
                raw_data = source

            return cast(Union[Dict[str, Any], List[Dict[str, Any]]], orjson.loads(raw_data))

        except ImportError:
            import json

            raw_text = ensure_string(source)
            try:
                return cast(Union[Dict[str, Any], List[Dict[str, Any]]], json.loads(raw_text))
            except json.JSONDecodeError as e:
                raise IncorporatorFormatError(f"Invalid JSON: {e}") from e

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        _raise_if_append_unsupported(kwargs, "JSON")
        path = Path(file_path).resolve()
        try:
            import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

            path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
        except ImportError:
            import json

            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            except OSError as e:
                raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e


class NDJSONHandler(BaseFormatHandler):
    def _parse_stream(self, stream: Union[TextIO, List[str]]) -> List[Dict[str, Any]]:
        import json

        rows: List[Dict[str, Any]] = []
        for line_num, line in enumerate(stream, start=1):
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                rows.append(json.loads(clean_line))
            except json.JSONDecodeError as e:
                raise IncorporatorFormatError(f"Invalid NDJSON on line {line_num}: {e}") from e
        return rows

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(raw_data.splitlines())

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        import json

        if not data:
            return
        try:
            path = Path(file_path).resolve()
            mode = "a" if kwargs.get("if_exists") == "append" else "w"
            with open(path, mode, encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item) + "\n")
        except OSError as e:
            raise IncorporatorFormatError(f"NDJSON File IO Error on {file_path}: {e}") from e


class CSVHandler(BaseFormatHandler):
    def __init__(self, delimiter: str = ",") -> None:
        self.delimiter = delimiter

    def _parse_stream(self, stream: Union[TextIO, io.StringIO], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            reader = csv.DictReader(stream, delimiter=self.delimiter)
            rows: List[Dict[str, Any]] = []

            for row in reader:
                parsed_row: Dict[str, Any] = {}
                for k, v in row.items():
                    safe_k = str(k) if k is not None else "unknown_column"
                    parsed_row[safe_k] = deserialize_nested(v)
                rows.append(parsed_row)
            return rows
        except csv.Error as e:
            raise IncorporatorFormatError(f"Invalid Delimited Format: {e}") from e

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f, **kwargs)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(io.StringIO(raw_data), **kwargs)

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return
        try:
            path = Path(file_path).resolve()
            is_append = kwargs.get("if_exists") == "append"
            mode = "a" if is_append else "w"

            # Only write headers if we are creating a new file
            write_headers = not (is_append and path.exists() and path.stat().st_size > 0)

            with open(path, mode, encoding="utf-8", newline="") as f:
                processed_gen = ({k: serialize_nested(v) for k, v in row.items()} for row in data)
                writer = csv.DictWriter(f, fieldnames=list(data[0].keys()), delimiter=self.delimiter)

                if write_headers:
                    writer.writeheader()

                writer.writerows(processed_gen)
        except OSError as e:
            raise IncorporatorFormatError(f"Delimited File IO Error on {file_path}: {e}") from e


class XMLHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            raw_bytes = (
                source.read_bytes()
                if isinstance(source, Path)
                else source.encode("utf-8")
                if isinstance(source, str)
                else source
            )
            parser = lxml_ET.XMLParser(resolve_entities=False, no_network=True)

            try:
                root = lxml_ET.fromstring(raw_bytes, parser=parser)
                return xml_to_dict(root)
            except lxml_ET.ParseError:
                root = lxml_ET.fromstring(raw_bytes.strip(), parser=parser)
                return xml_to_dict(root)

        except ImportError:
            import xml.etree.ElementTree as ET

            raw_str = ensure_string(source)
            check_xml_security(raw_str)

            try:
                root = ET.fromstring(raw_str)  # noqa: S314
                return xml_to_dict(root)
            except ET.ParseError:
                try:
                    root = ET.fromstring(raw_str.strip())  # noqa: S314
                    return xml_to_dict(root)
                except ET.ParseError as e:
                    raise IncorporatorFormatError(f"Invalid XML: {e}") from e

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        _raise_if_append_unsupported(kwargs, "XML")
        path = Path(file_path).resolve()
        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            root = lxml_ET.Element("root")
            for item in data:
                item_el = lxml_ET.SubElement(root, "item")
                for key, val in item.items():
                    clean_key = str(key).replace(" ", "_")
                    if clean_key and clean_key[0].isdigit():
                        clean_key = f"_{clean_key}"
                    child = lxml_ET.SubElement(item_el, clean_key)
                    safe_val = serialize_nested(val)
                    child.text = str(safe_val) if safe_val is not None else ""

            tree = lxml_ET.ElementTree(root)
            tree.write(str(path), encoding="utf-8", xml_declaration=True, pretty_print=True)

        except ImportError:
            import xml.etree.ElementTree as ET

            try:
                with open(path, "w", encoding="utf-8") as f:
                    root = ET.Element("root")
                    for item in data:
                        item_el = ET.SubElement(root, "item")
                        for key, val in item.items():
                            clean_key = str(key).replace(" ", "_")
                            if clean_key and clean_key[0].isdigit():
                                clean_key = f"_{clean_key}"
                            child = ET.SubElement(item_el, clean_key)
                            safe_val = serialize_nested(val)
                            child.text = str(safe_val) if safe_val is not None else ""

                    tree = ET.ElementTree(root)
                    tree.write(f, encoding="unicode")
            except OSError as e:
                raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e


class SQLiteHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        if not isinstance(source, Path):
            raise IncorporatorFormatError("SQLiteHandler requires a physical Path object.")

        query = kwargs.get("sql_query")
        if not query:
            raise IncorporatorFormatError("Reading from SQLite requires an 'sql_query' kwarg.")

        sql_params = kwargs.get("sql_params", ())

        try:
            conn = sqlite3.connect(source)
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query, sql_params)

                rows: List[Dict[str, Any]] = []
                # Iterate directly over the cursor to avoid the fetchall() memory bomb
                for row in cursor:
                    parsed_row = dict(row)
                    for k, v in parsed_row.items():
                        parsed_row[k] = deserialize_nested(v)
                    rows.append(parsed_row)

                return rows
            finally:
                conn.close()
        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Read Error: {e}") from e

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return

        table_name = kwargs.get("sql_table", "incorporator_export")
        if_exists = kwargs.get("if_exists", "replace")
        path = Path(file_path).resolve()

        try:
            conn = sqlite3.connect(path)
            try:
                with conn:
                    cursor = conn.cursor()

                    if if_exists == "replace":
                        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    elif if_exists == "fail":
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (table_name,),
                        )
                        if cursor.fetchone():
                            raise IncorporatorFormatError(f"Table '{table_name}' already exists in {path.name}.")

                    keys = list(data[0].keys())
                    safe_columns = [f'"{sanitize_json_key(k)}"' for k in keys]

                    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(safe_columns)})'
                    cursor.execute(create_stmt)

                    placeholders = ", ".join(["?"] * len(keys))
                    insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders})'  # noqa: S608

                    # Generator expression () yields tuples 1-by-1 to the C-driver
                    processed_gen = (
                        tuple(
                            int(v) if isinstance(v, bool) else serialize_nested(v) for v in (row.get(k) for k in keys)
                        )
                        for row in data
                    )

                    cursor.executemany(insert_stmt, processed_gen)
            finally:
                conn.close()
        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Write Error: {e}") from e


class AvroHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            import fastavro  # type: ignore[import-untyped, import-not-found, unused-ignore]
        except ImportError:
            raise IncorporatorFormatError("fastavro not installed. Run: pip install incorporator[avro]") from None

        try:
            rows: List[Dict[str, Any]] = []

            # Iterate the fastavro reader directly, bypassing the list() memory bomb
            if isinstance(source, Path):
                with open(source, "rb") as f:
                    for raw_row in fastavro.reader(f):
                        if isinstance(raw_row, dict):
                            rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})
            elif isinstance(source, bytes):
                import io

                for raw_row in fastavro.reader(io.BytesIO(source)):
                    if isinstance(raw_row, dict):
                        rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})
            else:
                raise IncorporatorFormatError("AvroHandler requires raw bytes or a physical Path object.")

            return rows

        except Exception as e:
            raise IncorporatorFormatError(f"Avro Read Error: {e}") from e

    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        if not data:
            return

        try:
            import fastavro  # type: ignore[import-untyped, import-not-found, unused-ignore]
        except ImportError:
            raise IncorporatorFormatError("fastavro not installed. Run: pip install incorporator[avro]") from None

        path = Path(file_path).resolve()
        pydantic_schema = kwargs.get("pydantic_schema", {})
        properties = pydantic_schema.get("properties", {})

        fields = []
        expected_types = {}

        for k, prop in properties.items():
            json_type = prop.get("type")
            if not json_type and "anyOf" in prop:
                for sub in prop["anyOf"]:
                    if sub.get("type") and sub.get("type") != "null":
                        json_type = sub.get("type")
                        break

            if json_type == "integer":
                a_type = "long"
            elif json_type == "number":
                a_type = "double"
            elif json_type == "boolean":
                a_type = "boolean"
            else:
                a_type = "string"

            safe_k = sanitize_json_key(k)
            fields.append({"name": safe_k, "type": ["null", a_type]})
            expected_types[safe_k] = a_type

        record_name = sanitize_json_key(kwargs.get("sql_table", "IncorporatorRecord"))
        parsed_schema = fastavro.parse_schema(
            {
                "doc": "Auto-generated by Incorporator",
                "name": record_name,
                "type": "record",
                "fields": fields,
            }
        )

        # Yield dicts to fastavro 1-by-1 to prevent duplicating the dataset in RAM
        def _record_generator() -> Iterator[Dict[str, Any]]:
            for row in data:
                processed_row = {}
                for k, v in row.items():
                    safe_k = sanitize_json_key(k)
                    val = serialize_nested(v)

                    if val is not None:
                        exp_type = expected_types.get(safe_k, "string")
                        if exp_type == "string" and not isinstance(val, str):
                            val = str(val)
                        elif exp_type == "long" and not isinstance(val, int):
                            try:
                                val = int(val)
                            except (ValueError, TypeError):
                                val = None
                        elif exp_type == "double" and not isinstance(val, float):
                            try:
                                val = float(val)
                            except (ValueError, TypeError):
                                val = None
                        elif exp_type == "boolean" and not isinstance(val, bool):
                            val = bool(val)

                    processed_row[safe_k] = val
                yield processed_row

        try:
            is_append = kwargs.get("if_exists") == "append"
            # fastavro supports native appends via 'a+b' mode
            mode = "a+b" if is_append and path.exists() else "wb"

            with open(path, mode) as f:
                fastavro.writer(f, parsed_schema, _record_generator())
        except Exception as e:
            raise IncorporatorFormatError(f"Avro Write Error: {e}") from e


_HANDLERS: Dict[FormatType, BaseFormatHandler] = {
    FormatType.JSON: JSONHandler(),
    FormatType.NDJSON: NDJSONHandler(),
    FormatType.CSV: CSVHandler(delimiter=","),
    FormatType.TSV: CSVHandler(delimiter="\t"),
    FormatType.PSV: CSVHandler(delimiter="|"),
    FormatType.XML: XMLHandler(),
    FormatType.SQLITE: SQLiteHandler(),
    FormatType.AVRO: AvroHandler(),
}


async def parse_source_data(
    source: Union[str, bytes, Path, List[Any], Dict[str, Any]], format_type: FormatType, **kwargs: Any
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    if isinstance(source, list):
        return cast(List[Dict[str, Any]], source)
    if isinstance(source, dict):
        return source

    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported format: '{format_type}'.")

    try:
        return await asyncio.to_thread(handler.parse, source, **kwargs)
    except Exception as e:
        snippet = str(source).strip()[:60].replace("\n", " ")
        logger.warning(
            f"⚠️ PARSE FAILED for format '{format_type}'. "
            f"The payload may be malformed (e.g., corrupted file or HTML firewall). "
            f"\n   Error: {e}\n   Received snippet: {snippet!r}..."
        )
        return []


async def write_destination_data(
    data: List[Dict[str, Any]], file_path: Union[str, Path], format_type: FormatType, **kwargs: Any
) -> None:
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported export format: '{format_type}'.")

    await asyncio.to_thread(handler.write, data, file_path, **kwargs)
