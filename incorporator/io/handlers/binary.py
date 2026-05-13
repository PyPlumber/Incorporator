"""Binary and database format handlers: SQLite and Apache Avro."""

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Union

from ...exceptions import IncorporatorFormatError
from ...schema.builder import sanitize_json_key
from ...schema.converters import inc
from ..formats import FormatType, convert_type, deserialize_nested, serialize_nested, to_python_type
from ._base import BaseFormatHandler

logger = logging.getLogger(__name__)


def coerce_avro_value(val: Any, avro_type: str) -> Any:
    """Coerce a value to the Python type expected by the given Avro type string.

    Uses the FORMAT_TO_PYTHON type bridge and the ranked inc() converter
    so coercion failures degrade gracefully to None rather than crashing.
    """
    if val is None:
        return None
    python_type = to_python_type(FormatType.AVRO, avro_type)
    if isinstance(val, python_type):
        return val
    return inc(python_type, default=None)(val)


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

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        table_name = sanitize_json_key(kwargs.get("sql_table", "incorporator_export"))
        if_exists = kwargs.get("if_exists", "replace")
        path = Path(file_path).resolve()

        explicit_keys: List[str] = kwargs.get("all_field_names") or []
        data_iter: Iterable[Dict[str, Any]]

        if not explicit_keys:
            # No schema hint (e.g. called outside export()): must materialize to discover columns.
            rows_list: List[Dict[str, Any]] = list(data)
            explicit_keys = list(rows_list[0].keys()) if rows_list else []
            data_iter = iter(rows_list)
        else:
            data_iter = data

        if not explicit_keys:
            return  # truly empty even after materialization

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

                    safe_columns = [f'"{sanitize_json_key(k)}"' for k in explicit_keys]
                    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(safe_columns)})'
                    cursor.execute(create_stmt)

                    placeholders = ", ".join(["?"] * len(explicit_keys))
                    insert_stmt = f'INSERT INTO "{table_name}" VALUES ({placeholders})'  # noqa: S608

                    # Generator expression yields tuples 1-by-1 to the C-driver.
                    # SQLite has no native BOOLEAN type: True → 1, False → 0.
                    # Re-reading the database will return integers (1/0), not booleans.
                    # This is documented, stable behaviour — consumers should cast explicitly.
                    processed_gen = (
                        tuple(
                            int(v) if isinstance(v, bool) else serialize_nested(v)
                            for v in (row.get(k) for k in explicit_keys)
                        )
                        for row in data_iter
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

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
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

            a_type = convert_type(json_type or "", FormatType.JSON, FormatType.AVRO)
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

        # Yield dicts to fastavro 1-by-1 to prevent duplicating the dataset in RAM.
        # Sanitised-key + expected-type lookups are cached on first occurrence:
        # after the first row, every subsequent key access is an O(1) dict hit
        # instead of a full sanitize_json_key() call.  This eliminates the only
        # significant per-row × per-key CPU cost in the Avro write path.
        sanitized_key_cache: Dict[str, str] = {}

        def _record_generator() -> Iterator[Dict[str, Any]]:
            for row in data:
                processed_row = {}
                for k, v in row.items():
                    safe_k = sanitized_key_cache.get(k)
                    if safe_k is None:
                        safe_k = sanitize_json_key(k)
                        sanitized_key_cache[k] = safe_k
                    val = serialize_nested(v)

                    if val is not None:
                        exp_type = expected_types.get(safe_k, "string")
                        val = coerce_avro_value(val, exp_type)

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
