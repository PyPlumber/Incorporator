"""Columnar format handlers: Apache Parquet, Feather (Arrow IPC), and ORC.

All three rest on the same ``pyarrow`` dep — installing ``incorporator[parquet]``
gives you all three formats. This is deliberate: pyarrow is the single largest
optional dep and we want to maximize its leverage.

Parquet is the data-lake format (S3, BigQuery, Snowflake, DuckDB, Spark).
Feather is the data-science interchange format (pandas/polars/R/Julia). ORC is
the Hadoop/Hive ecosystem columnar format. All three reuse the same row
coercion, schema-from-Pydantic logic, and type bridge tables.

``pyarrow`` is lazy-imported inside ``parse()`` / ``write()`` so importing this
module never pulls the ~30 MB Apache Arrow runtime at framework import time.
This mirrors ``AvroHandler``'s pattern. The dep is deliberately excluded from
``incorporator[all]`` — users opt in explicitly via ``incorporator[parquet]``.

Design choices:

* **Read path:** ``pq.read_table(...).to_pylist()`` — single-shot, simple, fast.
  Parquet files are typically chunked at the row-group level by the writer, so
  the streaming benefit is on the write side, not the read side.
* **Write path:** uses ``ParquetWriter`` with row-group batching (1024 rows
  per batch). This holds at most one row group in memory regardless of total
  dataset size — same O(1) memory profile as ``AvroHandler``'s generator path.
* **Nested types:** lists/dicts are flattened to JSON strings via
  ``serialize_nested`` — Parquet has native list/struct types, but supporting
  them properly requires schema inference at the Arrow level, which is out of
  scope for v1. Round-trips via JSON strings stay correct.
* **Append mode:** rejected. Parquet files have a footer-based index; safe
  appends require writing a new file and stitching via Hive partitioning or
  Arrow dataset APIs. Out of scope for v1.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Union

from ...exceptions import IncorporatorFormatError
from ..formats import FormatType, convert_type, deserialize_nested, serialize_nested
from ._base import BaseFormatHandler, _raise_if_append_unsupported

logger = logging.getLogger(__name__)

# Row-group batch size for the streaming write path. 1024 is a common pyarrow
# default — large enough that columnar encoding is efficient, small enough that
# memory stays bounded for arbitrarily large input streams.
_WRITE_BATCH_ROWS = 1024


def _arrow_type_for(name: str, properties: Dict[str, Any]) -> Any:
    """Map a Pydantic JSON-schema property to a pyarrow DataType.

    Falls back to pa.string() for unknown/nullable-union shapes. We route via
    the FORMAT bridge in formats.py so adding new logical types is a one-line
    change to the bridge tables rather than this function.
    """
    import pyarrow as pa

    prop = properties.get(name, {})
    json_type = prop.get("type")

    # Pydantic encodes Optional[X] as anyOf: [{type: X}, {type: null}]. Walk it.
    if not json_type and "anyOf" in prop:
        for sub in prop["anyOf"]:
            if sub.get("type") and sub.get("type") != "null":
                json_type = sub.get("type")
                break

    parquet_type_str = convert_type(json_type or "", FormatType.JSON, FormatType.PARQUET)

    type_map: Dict[str, Any] = {
        "bool": pa.bool_(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "string": pa.string(),
        "binary": pa.binary(),
        "null": pa.null(),
    }
    return type_map.get(parquet_type_str, pa.string())


def _materialize_table(data: Iterable[Dict[str, Any]], kwargs: Dict[str, Any]) -> Any:
    """Coerce an iterable of dicts into a single pyarrow.Table.

    Shared by FeatherHandler and OrcHandler — both formats are written
    one-shot (no streaming writer API), so we materialize once. ParquetHandler
    does NOT use this helper; it streams via ParquetWriter for O(1) memory.

    Honours the same kwargs as ParquetHandler.write:
        * pydantic_schema → explicit Arrow schema
        * all_field_names → explicit column order
    """
    import pyarrow as pa

    pydantic_schema = kwargs.get("pydantic_schema", {})
    properties: Dict[str, Any] = pydantic_schema.get("properties", {})
    explicit_keys: List[str] = kwargs.get("all_field_names") or list(properties.keys())

    rows_list: List[Dict[str, Any]] = list(data)
    if not rows_list:
        return None
    if not explicit_keys:
        explicit_keys = list(dict.fromkeys(k for row in rows_list for k in row))

    coerced = [
        {k: (serialize_nested(row.get(k)) if row.get(k) is not None else None) for k in explicit_keys}
        for row in rows_list
    ]

    if properties:
        schema = pa.schema([pa.field(k, _arrow_type_for(k, properties), nullable=True) for k in explicit_keys])
        return pa.Table.from_pylist(coerced, schema=schema)
    return pa.Table.from_pylist(coerced)


class ParquetHandler(BaseFormatHandler):
    """Parse and write .parquet files using pyarrow.

    Lazy-imports pyarrow on first use. Raises a clear ``IncorporatorFormatError``
    pointing to ``pip install incorporator[parquet]`` when the optional dep is
    missing.
    """

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise IncorporatorFormatError("pyarrow not installed. Run: pip install incorporator[parquet]") from None

        try:
            if isinstance(source, Path):
                table = pq.read_table(source)  # type: ignore[no-untyped-call]
            elif isinstance(source, bytes):
                import io

                table = pq.read_table(io.BytesIO(source))  # type: ignore[no-untyped-call]
            else:
                raise IncorporatorFormatError("ParquetHandler requires raw bytes or a physical Path object.")

            # to_pylist() yields a list of dicts keyed by column name. Apply
            # deserialize_nested so values written as JSON strings (lists/dicts)
            # come back as native Python types on round-trip.
            rows: List[Dict[str, Any]] = []
            for raw_row in table.to_pylist():
                rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})
            return rows
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"Parquet Read Error: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        # Parquet files have a footer index — safe append requires Hive-style
        # partitioning, which is out of scope. Users who need append should
        # stream to NDJSON instead.
        _raise_if_append_unsupported(kwargs, "Parquet")

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise IncorporatorFormatError("pyarrow not installed. Run: pip install incorporator[parquet]") from None

        path = Path(file_path).resolve()
        compression = kwargs.get("parquet_compression", "snappy")  # pyarrow's default
        pydantic_schema = kwargs.get("pydantic_schema", {})
        properties: Dict[str, Any] = pydantic_schema.get("properties", {})
        explicit_keys: List[str] = kwargs.get("all_field_names") or list(properties.keys())

        data_iter: Iterable[Dict[str, Any]]

        if not explicit_keys:
            # No schema hint: materialize first batch to discover columns. We
            # could use Arrow's schema inference on a single chunk, but a single
            # materialization of the first batch keeps the code mirror-image
            # of the SQLite/CSV handlers.
            rows_list: List[Dict[str, Any]] = list(data)
            if not rows_list:
                return
            explicit_keys = list(dict.fromkeys(k for row in rows_list for k in row))
            data_iter = iter(rows_list)
        else:
            data_iter = data

        # Two-mode schema strategy:
        #   1. Pydantic schema present → build an explicit Arrow schema up-front
        #      from the JSON-schema type bridge. Fastest, deterministic types.
        #   2. No hint → infer the schema from the first batch via pyarrow's
        #      native inference. Slightly slower but always type-correct.
        explicit_schema = (
            pa.schema([pa.field(k, _arrow_type_for(k, properties), nullable=True) for k in explicit_keys])
            if properties
            else None
        )

        def _coerce_row(row: Dict[str, Any]) -> Dict[str, Any]:
            """Flatten nested types to JSON strings; preserve scalars + None."""
            out: Dict[str, Any] = {}
            for k in explicit_keys:
                val = row.get(k)
                # serialize_nested flattens dict/list to JSON; passes scalars through.
                out[k] = serialize_nested(val) if val is not None else None
            return out

        def _batched_iter(rows: Iterable[Dict[str, Any]], batch_size: int) -> Iterator[List[Dict[str, Any]]]:
            """Yield batches of size <= batch_size. Holds only the current batch in RAM."""
            batch: List[Dict[str, Any]] = []
            for row in rows:
                batch.append(_coerce_row(row))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        try:
            writer: Any = None
            try:
                for batch in _batched_iter(data_iter, _WRITE_BATCH_ROWS):
                    if explicit_schema is not None:
                        table = pa.Table.from_pylist(batch, schema=explicit_schema)
                    else:
                        # Native inference from the first batch — its schema seeds
                        # the writer and is reused for every subsequent batch.
                        table = pa.Table.from_pylist(batch)
                    if writer is None:
                        writer = pq.ParquetWriter(path, table.schema, compression=compression)  # type: ignore[no-untyped-call]
                    writer.write_table(table)
            finally:
                if writer is not None:
                    writer.close()
        except Exception as e:
            raise IncorporatorFormatError(f"Parquet Write Error on {file_path}: {e}") from e


class FeatherHandler(BaseFormatHandler):
    """Parse and write Feather V2 (Apache Arrow IPC) files using pyarrow.

    Feather is the fastest interchange format for data-science workflows —
    memory-mapped reads with zero deserialization. The pyarrow.feather API is
    one-shot (no streaming writer), so writes materialize the dataset before
    flushing. For datasets large enough to exceed RAM, use Parquet instead.

    Compression defaults to LZ4 (Feather V2's native default).
    """

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            import pyarrow.feather as feather
        except ImportError:
            raise IncorporatorFormatError("pyarrow not installed. Run: pip install incorporator[parquet]") from None

        try:
            if isinstance(source, Path):
                table = feather.read_table(source)  # type: ignore[no-untyped-call]
            elif isinstance(source, bytes):
                import io

                table = feather.read_table(io.BytesIO(source))  # type: ignore[no-untyped-call]
            else:
                raise IncorporatorFormatError("FeatherHandler requires raw bytes or a physical Path object.")

            rows: List[Dict[str, Any]] = []
            for raw_row in table.to_pylist():
                rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})
            return rows
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"Feather Read Error: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        # Feather V2 has no streaming writer — append is not supported.
        _raise_if_append_unsupported(kwargs, "Feather/Arrow IPC")

        try:
            import pyarrow.feather as feather
        except ImportError:
            raise IncorporatorFormatError("pyarrow not installed. Run: pip install incorporator[parquet]") from None

        path = Path(file_path).resolve()
        compression = kwargs.get("feather_compression", "lz4")  # Feather V2 default

        try:
            table = _materialize_table(data, kwargs)
            if table is None:
                return  # nothing to write
            feather.write_feather(table, str(path), compression=compression)  # type: ignore[no-untyped-call]
        except Exception as e:
            raise IncorporatorFormatError(f"Feather Write Error on {file_path}: {e}") from e


class OrcHandler(BaseFormatHandler):
    """Parse and write Apache ORC files using pyarrow.

    ORC is the columnar format of the Hadoop/Hive ecosystem (Trino, Presto,
    Hive, Spark on Hadoop). The pyarrow.orc API is one-shot — writes
    materialize the dataset before flushing. Use Parquet for streaming.

    Note: pyarrow's ORC support has historically been platform-sensitive on
    Windows. The handler reports a clear error if pyarrow.orc fails to import
    even though pyarrow itself loaded successfully.
    """

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            from pyarrow import orc
        except ImportError:
            raise IncorporatorFormatError(
                "pyarrow.orc not available. Run: pip install incorporator[parquet] "
                "(ORC support requires pyarrow with libarrow_orc; on some platforms "
                "this may need pyarrow built from source)."
            ) from None

        try:
            if isinstance(source, Path):
                orc_file = orc.ORCFile(source)  # type: ignore[no-untyped-call]
            elif isinstance(source, bytes):
                import io

                orc_file = orc.ORCFile(io.BytesIO(source))  # type: ignore[no-untyped-call]
            else:
                raise IncorporatorFormatError("OrcHandler requires raw bytes or a physical Path object.")

            table = orc_file.read()  # type: ignore[no-untyped-call]
            rows: List[Dict[str, Any]] = []
            for raw_row in table.to_pylist():
                rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})
            return rows
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"ORC Read Error: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        # ORC has no streaming writer in pyarrow — append is not supported.
        _raise_if_append_unsupported(kwargs, "ORC")

        try:
            from pyarrow import orc
        except ImportError:
            raise IncorporatorFormatError(
                "pyarrow.orc not available. Run: pip install incorporator[parquet] "
                "(ORC support requires pyarrow with libarrow_orc)."
            ) from None

        path = Path(file_path).resolve()

        try:
            table = _materialize_table(data, kwargs)
            if table is None:
                return  # nothing to write
            orc.write_table(table, str(path))  # type: ignore[no-untyped-call]
        except Exception as e:
            raise IncorporatorFormatError(f"ORC Write Error on {file_path}: {e}") from e
