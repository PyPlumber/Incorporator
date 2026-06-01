"""Binary and database format handlers: SQLite and Apache Avro."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from ...exceptions import IncorporatorFormatError
from ...schema.builder import sanitize_json_key
from ...schema.converters import inc
from ..formats import FormatType, convert_type, deserialize_nested, serialize_nested, to_python_type
from ._base import BaseFormatHandler, _require_optional

logger = logging.getLogger(__name__)

# Avro schema metadata key used to round-trip original field names.  Avro
# requires field names to match ``[A-Za-z_][A-Za-z0-9_]*``, so we sanitise on
# write (``user-id → user_id``) — but without remembering the original we
# can't restore the user's column names on read.  Storing the map as a
# JSON-string under a custom schema attribute keeps it portable across all
# Avro readers (fastavro preserves unknown attributes verbatim) and
# backwards-compatible (absence of the key falls back to current behaviour).
_AVRO_ORIGINAL_NAMES_KEY = "__incorporator_original_names__"


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
    """Parse and write SQLite ``.db`` / ``.sqlite`` files.

    Reads require a physical file path (SQLite has no in-memory wire format)
    and a ``sql_query`` kwarg selecting the rows to materialize. Writes target
    a table named by the ``sql_table`` kwarg (default ``incorporator_export``)
    and honour ``if_exists`` semantics (``"replace"`` / ``"append"`` / ``"fail"``).

    SQLite has no native ``BOOLEAN`` type: ``True``/``False`` are stored as
    integers ``1``/``0``. Reading the column back yields integers — consumers
    that need bools should cast explicitly.
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> list[dict[str, Any]]:
        """Execute ``sql_query`` against the SQLite file and return rows as dicts.

        Iterates the cursor directly (no ``fetchall()`` memory bomb), so the
        read path is bounded regardless of result-set size.

        Boolean recovery: SQLite has no native BOOLEAN type — writes encode
        ``True``/``False`` as ``1``/``0`` ints.  Pass ``sql_bool_columns=
        ["col_a", "col_b"]`` to recover the original bool semantics on read;
        any value of ``0`` / ``1`` in those columns is cast back to
        ``False`` / ``True``.  Without the kwarg, ints come back as ints
        (documented behaviour — the column type is genuinely ambiguous).
        """
        if not isinstance(source, Path):
            raise IncorporatorFormatError("SQLiteHandler requires a physical Path object.")

        query = kwargs.get("sql_query")
        if not query:
            raise IncorporatorFormatError("Reading from SQLite requires an 'sql_query' kwarg.")

        sql_params = kwargs.get("sql_params", ())
        bool_columns: frozenset[str] = frozenset(kwargs.get("sql_bool_columns", ()))

        try:
            conn = sqlite3.connect(source)
            try:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query, sql_params)

                rows: list[dict[str, Any]] = []
                # Iterate directly over the cursor to avoid the fetchall() memory bomb
                for row in cursor:
                    parsed_row = dict(row)
                    for k, v in parsed_row.items():
                        if k in bool_columns and v in (0, 1):
                            parsed_row[k] = bool(v)
                        else:
                            parsed_row[k] = deserialize_nested(v)
                    rows.append(parsed_row)

                return rows
            finally:
                conn.close()
        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Read Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows into a SQLite table via ``executemany``.

        Honours ``sql_table``, ``if_exists`` (``"replace"`` / ``"append"`` /
        ``"fail"``), and ``all_field_names`` (column order hint). Rows are
        yielded one-by-one to the C driver so memory stays O(1) for arbitrarily
        large input streams.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        table_name = sanitize_json_key(kwargs.get("sql_table", "incorporator_export"))
        if_exists = kwargs.get("if_exists", "replace")
        # Dispatcher pre-resolves the path; handlers always receive an absolute Path.
        path = file_path if isinstance(file_path, Path) else Path(file_path)

        explicit_keys: list[str] = kwargs.get("all_field_names") or []
        data_iter: Iterable[dict[str, Any]]

        if not explicit_keys:
            # No schema hint (e.g. called outside export()): must materialize to discover columns.
            rows_list: list[dict[str, Any]] = list(data)
            explicit_keys = list(rows_list[0].keys()) if rows_list else []
            data_iter = iter(rows_list)
        else:
            data_iter = data

        if not explicit_keys:
            return  # truly empty even after materialization

        try:
            # ``isolation_level=None`` disables Python sqlite3's implicit-transaction
            # wrapper.  Why: the default (``isolation_level=""``) causes DDL
            # statements (DROP TABLE, CREATE TABLE) to auto-commit BEFORE the
            # implicit BEGIN that wraps the subsequent INSERT.  A crash during
            # INSERT then leaves the table dropped + un-restored — the user's
            # pre-write data is gone with no rollback path.
            #
            # Driving the transaction explicitly with BEGIN IMMEDIATE / COMMIT /
            # ROLLBACK makes the entire DROP+CREATE+INSERT sequence atomic — a
            # mid-write crash rolls back the DROP too, so the old table survives.
            # IMMEDIATE acquires the write lock at BEGIN time so a concurrent
            # writer can't sneak in between our DROP and INSERT either.
            conn = sqlite3.connect(path, isolation_level=None)
            try:
                cursor = conn.cursor()
                cursor.execute("BEGIN IMMEDIATE")
                try:
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
                    cursor.execute("COMMIT")
                except BaseException:
                    # Any failure — including KeyboardInterrupt — must roll
                    # back the DROP so the old table is restored.
                    try:
                        cursor.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise
            finally:
                conn.close()
        except sqlite3.Error as e:
            raise IncorporatorFormatError(f"SQLite Write Error: {e}") from e


class AvroHandler(BaseFormatHandler):
    """Parse and write Apache Avro binary files via ``fastavro``.

    Lazy-imports ``fastavro`` on first use. Raises a clear
    ``IncorporatorFormatError`` pointing to ``pip install incorporator[avro]``
    when the optional dep is missing. Append mode is supported natively via
    ``fastavro``'s ``a+b`` writer.
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> list[dict[str, Any]]:
        """Read an Avro file or byte buffer and yield rows as dicts.

        Iterates the ``fastavro.reader`` block-by-block — no ``list()``
        materialisation — so memory stays bounded for arbitrarily large Avro
        files.
        """
        fastavro = _require_optional("fastavro")

        try:
            rows: list[dict[str, Any]] = []

            # Helper: extract the {safe_name: original_name} rename map from the
            # writer schema's custom metadata, if present.  fastavro exposes the
            # writer schema via reader.writer_schema as a dict.  The map was
            # serialised as a JSON string on write (Avro requires string-valued
            # custom attributes); decode here and reverse the direction so we
            # can rename keys back on each row.
            def _build_rename_map(reader: Any) -> dict[str, str]:
                schema = getattr(reader, "writer_schema", None)
                if not isinstance(schema, dict):
                    return {}
                raw = schema.get(_AVRO_ORIGINAL_NAMES_KEY)
                if not isinstance(raw, str):
                    return {}
                try:
                    decoded = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    return {}
                if not isinstance(decoded, dict):
                    return {}
                # Stored as ``original → safe`` on write.  Reverse to
                # ``safe → original`` so each row's key swap is one dict lookup.
                return {safe: original for original, safe in decoded.items() if isinstance(safe, str)}

            def _hydrate_row(raw_row: dict[str, Any], rename_map: dict[str, str]) -> dict[str, Any]:
                if rename_map:
                    return {rename_map.get(k, k): deserialize_nested(v) for k, v in raw_row.items()}
                return {k: deserialize_nested(v) for k, v in raw_row.items()}

            # Iterate the fastavro reader directly, bypassing the list() memory bomb
            if isinstance(source, Path):
                with open(source, "rb") as f:
                    reader = fastavro.reader(f)
                    rename_map = _build_rename_map(reader)
                    for raw_row in reader:
                        if isinstance(raw_row, dict):
                            rows.append(_hydrate_row(raw_row, rename_map))
            elif isinstance(source, bytes):
                import io

                reader = fastavro.reader(io.BytesIO(source))
                rename_map = _build_rename_map(reader)
                for raw_row in reader:
                    if isinstance(raw_row, dict):
                        rows.append(_hydrate_row(raw_row, rename_map))
            else:
                raise IncorporatorFormatError("AvroHandler requires raw bytes or a physical Path object.")

            return rows

        except Exception as e:
            raise IncorporatorFormatError(f"Avro Read Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows into an Avro file using the dataset's Pydantic schema.

        Builds the Avro record schema from ``pydantic_schema`` via the
        JSON-schema→Avro type bridge, then yields rows one-by-one to
        ``fastavro.writer`` so memory stays O(1). When ``if_exists="append"``
        and the file already exists, uses ``fastavro``'s native ``a+b`` mode.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        fastavro = _require_optional("fastavro")

        # Dispatcher pre-resolves the path; handlers always receive an absolute Path.
        path = file_path if isinstance(file_path, Path) else Path(file_path)
        pydantic_schema = kwargs.get("pydantic_schema", {})
        properties = pydantic_schema.get("properties", {})

        fields = []
        expected_types = {}
        # Build the {original_name: safe_name} map so the reader can restore
        # the user's column names on round-trip.  Only entries where sanitising
        # actually changed the name are recorded — keeps the schema metadata
        # minimal for the common all-clean-names case.
        original_names_map: dict[str, str] = {}

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
            if safe_k != k:
                original_names_map[k] = safe_k

        record_name = sanitize_json_key(kwargs.get("sql_table", "IncorporatorRecord"))
        schema_dict: dict[str, Any] = {
            "doc": "Auto-generated by Incorporator",
            "name": record_name,
            "type": "record",
            "fields": fields,
        }
        # Custom attribute — fastavro preserves it verbatim in the Avro file
        # header, and our reader checks for it to reverse the sanitisation
        # on parse.  Backwards-compatible: older files / non-Incorporator
        # readers simply ignore the unknown attribute.
        if original_names_map:
            schema_dict[_AVRO_ORIGINAL_NAMES_KEY] = json.dumps(original_names_map, sort_keys=True)

        parsed_schema = fastavro.parse_schema(schema_dict)

        # Yield dicts to fastavro 1-by-1 to prevent duplicating the dataset in RAM.
        # Sanitised-key + expected-type lookups are cached on first occurrence:
        # after the first row, every subsequent key access is an O(1) dict hit
        # instead of a full sanitize_json_key() call.  This eliminates the only
        # significant per-row × per-key CPU cost in the Avro write path.
        sanitized_key_cache: dict[str, str] = {}

        def _record_generator() -> Iterator[dict[str, Any]]:
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
