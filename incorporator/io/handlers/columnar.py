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

from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

from ...exceptions import IncorporatorFormatError
from ..formats import FormatType, convert_type, serialize_nested
from ._base import BaseFormatHandler, _raise_if_append_unsupported, _require_optional, atomic_write_path

logger = logging.getLogger(__name__)

# Row-group batch size for the streaming write path. 1024 is a common pyarrow
# default — large enough that columnar encoding is efficient, small enough that
# memory stays bounded for arbitrarily large input streams.
_WRITE_BATCH_ROWS = 1024
_SMALL_TABLE_THRESHOLD = 64  # below this, pure-Python scan beats Arrow compute dispatch

# Scalar types that bypass serialize_nested entirely — direct append.
# type(v) in frozenset is C-level equality + hash, avoiding isinstance's
# MRO walk. Subclass-of-int semantics don't apply here (Arrow-write rows
# carry pure builtins).
_SCALAR_TYPES = frozenset({str, int, float, bool})


def _stream_columnar_write(
    data: Iterable[dict[str, Any]],
    file_path: str | Path,
    kwargs: dict[str, Any],
    *,
    format_label: str,
    sink_factory: Callable[[Path], contextlib.AbstractContextManager[Any]],
    build_writer: Callable[[Any, Any], Any],
    write_batch: Callable[[Any, Any], None],
) -> None:
    """Shared write scaffold for the three columnar handlers (Parquet/Feather/ORC).

    The three formats share the same lifecycle — peek the first Arrow batch
    from ``_stream_arrow_batches``, atomically write to a sibling tempfile,
    construct a writer (with the first batch's schema), loop the remaining
    batches, close, rename.  They differ on:

    - **Sink factory.** Parquet/ORC accept a path-string directly (wrapped
      in ``contextlib.nullcontext`` at the callsite); Feather wraps the
      path in ``pa.OSFile``.
    - **Writer factory.** Each format constructs its own writer class with
      format-specific kwargs (compression, IpcWriteOptions, etc.).  ORC's
      writer infers schema from the first ``write()`` call rather than
      taking it at construction — its ``build_writer`` callback simply
      ignores the ``schema`` argument.
    - **Per-batch write method.** Parquet/Feather use ``writer.write_table``;
      ORC uses ``writer.write``.

    The append-mode guard, dep-presence sentinels, and per-format error
    messages stay at the callsite — this helper covers only the
    peek/atomic-write/loop/close scaffold.

    Args:
        data: Iterable of row dicts to encode.
        file_path: Final destination; the atomic tempfile is a sibling and
            is renamed on success.
        kwargs: Format kwargs (``pydantic_schema``, compression hints, etc.)
            forwarded to :func:`_stream_arrow_batches`.
        format_label: Used in the wrapped ``IncorporatorFormatError`` message
            ("Parquet" / "Feather" / "ORC").
        sink_factory: ``Path -> ContextManager[sink]``.  The ``sink`` value
            is whatever ``build_writer`` expects as its first positional arg.
        build_writer: ``(sink, schema) -> writer``.  The returned writer
            must support ``.close()``.
        write_batch: ``(writer, batch) -> None``.
    """
    path = file_path if isinstance(file_path, Path) else Path(file_path)
    try:
        batches_iter = _stream_arrow_batches(data, kwargs)
        try:
            first_batch = next(batches_iter)
        except StopIteration:
            return  # nothing to write — empty input
        # Atomic write — build to a sibling tempfile, rename on success.
        # A mid-stream crash leaves the prior file intact instead of a
        # half-written file with a missing footer.
        with atomic_write_path(path) as tmp_path:
            with sink_factory(tmp_path) as sink:
                writer = build_writer(sink, first_batch.schema)
                try:
                    write_batch(writer, first_batch)
                    for batch in batches_iter:
                        write_batch(writer, batch)
                finally:
                    writer.close()
    except Exception as e:
        raise IncorporatorFormatError(f"{format_label} Write Error on {file_path}: {e}") from e


def _coerce_columnar_source(source: Any, handler_name: str) -> Any:
    """Coerce ``Path``/``bytes`` parse sources into a pyarrow-readable handle.

    Shared by Parquet/Feather/ORC parse() — pyarrow's columnar readers accept
    a filesystem path or a binary file-like object.  Centralising the
    ``isinstance`` ladder here means one place to add new source shapes (e.g.
    ``memoryview``) and a single uniform error message when the caller passes
    something unsupported.  We do **not** read the file via ``ensure_bytes``
    here because pyarrow's path-based readers memory-map the file, which is
    materially cheaper than a full Python-side read for multi-GB inputs.
    """
    if isinstance(source, Path):
        return source
    if isinstance(source, bytes):
        import io

        return io.BytesIO(source)
    raise IncorporatorFormatError(f"{handler_name} requires raw bytes or a physical Path object.")


def _extract_logical_type_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Pull the opt-in Parquet/Feather/ORC logical-type kwargs into a single dict.

    Shared by ParquetHandler, FeatherHandler, and OrcHandler so all three
    columnar formats accept the same vocabulary for decimal128 and
    timezone-aware timestamps.  Absent kwargs fall back to defaults that
    match pyarrow's own conventions (precision 38 / scale 18, microseconds,
    UTC) — the user only sets what they care about.
    """
    decimal_cols = kwargs.get("parquet_decimal_columns")
    timestamp_cols = kwargs.get("parquet_timestamp_columns")
    return {
        "decimal_columns": set(decimal_cols) if decimal_cols else None,
        "decimal_precision": int(kwargs.get("parquet_decimal_precision", 38)),
        "decimal_scale": int(kwargs.get("parquet_decimal_scale", 18)),
        "timestamp_columns": set(timestamp_cols) if timestamp_cols else None,
        "timestamp_unit": kwargs.get("parquet_timestamp_unit", "us"),
        "timestamp_tz": kwargs.get("parquet_timestamp_tz", "UTC"),
    }


def _arrow_type_for(
    name: str,
    properties: dict[str, Any],
    *,
    decimal_columns: set[str] | None = None,
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    timestamp_columns: set[str] | None = None,
    timestamp_unit: str = "us",
    timestamp_tz: str | None = "UTC",
) -> Any:
    """Map a Pydantic JSON-schema property to a pyarrow DataType.

    Falls back to pa.string() for unknown/nullable-union shapes. We route via
    the FORMAT bridge in formats.py so adding new logical types is a one-line
    change to the bridge tables rather than this function.

    **Logical-type kwargs** (optional, opt-in by column name):

    - ``decimal_columns``: column names that should be encoded as
      ``pa.decimal128(precision, scale)`` instead of falling back to
      string-via-JSON-schema-number. Without this hint, ``Decimal`` Pydantic
      fields lose precision (Arrow's ``float64`` can't represent
      ``Decimal("123.4567890123456789")`` faithfully).  Default
      precision 38 / scale 18 covers all real-world monetary data.
    - ``timestamp_columns``: column names to encode as
      ``pa.timestamp(unit, tz)`` rather than letting Pydantic's
      ``"format": "date-time"`` collapse to ``pa.string()``.  Preserves
      timezone information across the Parquet round-trip; without it,
      ``datetime(..., tzinfo=timezone.utc)`` writes as ISO string and
      reads back as ``str``.

    Both are pure additive opt-ins — handlers that don't pass the kwargs
    behave exactly as before.
    """
    import pyarrow as pa

    # Explicit logical-type hints win over JSON-schema introspection.  Users
    # know their Pydantic schema better than we can guess from the dict.
    if decimal_columns and name in decimal_columns:
        return pa.decimal128(decimal_precision, decimal_scale)
    if timestamp_columns and name in timestamp_columns:
        return pa.timestamp(timestamp_unit, tz=timestamp_tz)

    prop = properties.get(name, {})
    json_type = prop.get("type")

    # Pydantic encodes X | None as anyOf: [{type: X}, {type: null}]. Walk it.
    if not json_type and "anyOf" in prop:
        for sub in prop["anyOf"]:
            if sub.get("type") and sub.get("type") != "null":
                json_type = sub.get("type")
                break

    parquet_type_str = convert_type(json_type or "", FormatType.JSON, FormatType.PARQUET)

    type_map: dict[str, Any] = {
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


def _table_to_dicts(table: Any) -> list[dict[str, Any]]:
    """Convert a ``pyarrow.Table`` to ``List[Dict]`` with the minimum allocations.

    Shared parse-path helper for ParquetHandler, FeatherHandler, and OrcHandler —
    all three previously did the same naive loop:

        rows = []
        for raw_row in table.to_pylist():
            rows.append({k: deserialize_nested(v) for k, v in raw_row.items()})

    That allocated ``2 × N`` dicts (one in ``to_pylist`` + one in the
    comprehension) and called ``deserialize_nested`` on every cell — even
    int/float/bool columns where it's a no-op ``isinstance`` check.

    This helper does two things differently:

    1. **In-place mutation** — uses the dicts ``to_pylist()`` already allocated.
       Cuts dict allocations in half.  Mutating values mid-iteration is safe
       in Python; we never add or remove keys.
    2. **Schema-aware iteration** — uses ``table.schema`` to identify which
       columns are strings.  Only those can possibly contain JSON-encoded
       nested data (``serialize_nested`` writes lists/dicts as JSON strings),
       so int/float/bool/null columns are skipped entirely.

    The net effect on a 4-column dataset with 1 string column is roughly
    halving the Python-side parse cost.  Real win scales with the
    non-string fraction of the schema.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    rows: list[dict[str, Any]] = table.to_pylist()

    # Find which columns might contain JSON-serialised nested values.
    # serialize_nested() flattens dict/list -> JSON string, so only string
    # columns are candidates for re-hydration.
    string_cols = [field.name for field in table.schema if pa.types.is_string(field.type)]
    if not string_cols:
        return rows

    # The pyarrow trick: push the "does this cell start with { or [?" check
    # down to Arrow's vectorised C compute engine.  Returns a boolean mask
    # over the column in microseconds.  Then pc.any() — also C-level —
    # tells us in O(1) whether the column has ANY candidates worth visiting
    # from Python.
    #
    # Real-world payoff: most string columns (names, descriptions, ids,
    # statuses) never contain JSON.  This lets us skip the entire per-row
    # Python loop for those columns — only the columns with at least one
    # plausible JSON cell pay the per-row cost, and even then only the
    # flagged rows are touched.
    for col_name in string_cols:
        if len(rows) < _SMALL_TABLE_THRESHOLD:
            # Fast-path: skip Arrow compute on tiny tables (paginated API batches, fixtures, tests).
            # Arrow's pc.starts_with/or_/any dispatch overhead exceeds plain-Python loop cost
            # under 64 rows.
            for i in range(len(rows)):
                v = rows[i].get(col_name)
                if (
                    isinstance(v, str)
                    and len(v) >= 2
                    and v[0] in "{["
                    and ((v[-1] == "}") if v[0] == "{" else (v[-1] == "]"))
                ):
                    try:
                        rows[i][col_name] = json.loads(v)
                    except (json.JSONDecodeError, ValueError):
                        pass
            continue

        col = table.column(col_name)
        could_be_json_mask = pc.or_(  # type: ignore[attr-defined]
            pc.starts_with(col, "{"),  # type: ignore[attr-defined]
            pc.starts_with(col, "["),  # type: ignore[attr-defined]
        )
        if not pc.any(could_be_json_mask).as_py():  # type: ignore[attr-defined]
            continue

        # Iterate only flagged indices to avoid materialising a full N-length
        # boolean mask + per-row skip when JSON cells are a sparse minority.
        flagged_indices = pc.indices_nonzero(could_be_json_mask).to_pylist()  # type: ignore[attr-defined]
        for i in flagged_indices:
            v = rows[i][col_name]
            if len(v) >= 2 and ((v[0] == "{" and v[-1] == "}") or (v[0] == "[" and v[-1] == "]")):
                try:
                    rows[i][col_name] = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    pass
    return rows


def _build_columnar_schema(
    explicit_keys: list[str],
    properties: dict[str, Any],
    kwargs: dict[str, Any],
) -> Any | None:
    """Build an explicit pyarrow schema from the Pydantic JSON-schema properties.

    Returns ``None`` when no JSON-schema hint is available — callers fall
    back to native pyarrow type inference on the first batch in that case.
    """
    if not properties:
        return None
    import pyarrow as pa

    logical_kwargs = _extract_logical_type_kwargs(kwargs)
    return pa.schema(
        [pa.field(k, _arrow_type_for(k, properties, **logical_kwargs), nullable=True) for k in explicit_keys]
    )


def _coerce_batch(batch: list[dict[str, Any]], explicit_keys: list[str]) -> list[dict[str, Any]]:
    """Apply serialize_nested to one batch — nested lists/dicts → JSON strings."""

    def _coerce(v: Any) -> Any:
        if v is None or type(v) in _SCALAR_TYPES:
            return v
        return serialize_nested(v)

    return [{k: _coerce(row.get(k)) for k in explicit_keys} for row in batch]


def _batched_dicts(
    rows: Iterable[dict[str, Any]], explicit_keys: list[str], batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    """Yield ``batch_size``-row windows of coerced dicts; holds one batch in RAM."""
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield _coerce_batch(batch, explicit_keys)
            batch = []
    if batch:
        yield _coerce_batch(batch, explicit_keys)


def _materialize_table(data: Iterable[dict[str, Any]], kwargs: dict[str, Any]) -> Any:
    """**DEPRECATED** — retained for any external caller; new code should stream.

    Coerce an iterable of dicts into a single pyarrow.Table.  Loads the full
    dataset into memory before returning, so multi-GB inputs OOM.  The
    streaming pattern in ``_stream_arrow_batches`` is the preferred path —
    it holds at most one row-group in RAM regardless of total size.
    """
    import pyarrow as pa

    pydantic_schema = kwargs.get("pydantic_schema", {})
    properties: dict[str, Any] = pydantic_schema.get("properties", {})
    explicit_keys: list[str] = kwargs.get("all_field_names") or list(properties.keys())

    rows_list: list[dict[str, Any]] = list(data)
    if not rows_list:
        return None
    if not explicit_keys:
        explicit_keys = list(dict.fromkeys(k for row in rows_list for k in row))

    coerced = _coerce_batch(rows_list, explicit_keys)
    schema = _build_columnar_schema(explicit_keys, properties, kwargs)
    if schema is not None:
        return pa.Table.from_pylist(coerced, schema=schema)
    return pa.Table.from_pylist(coerced)


def _batched_columns(
    rows: Iterable[dict[str, Any]], explicit_keys: list[str], batch_size: int
) -> Iterator[dict[str, list[Any]]]:
    """Yield ``batch_size``-row windows as **column-oriented** dicts.

    Why column-oriented: ``pa.Table.from_pydict`` is materially faster than
    the row-oriented ``from_pylist`` (~2.4× on a representative payload —
    see ``tests/benchmarks/test_parquet_throughput.py``) because Arrow's
    internal memory layout is columnar.  Even paying the row→column pivot
    cost here (one append per cell) we come out well ahead because Arrow
    avoids the per-row dict-unpack on its side.  Used by Parquet, Feather
    and ORC writers so all three columnar formats share the same speedup.
    """
    cols: dict[str, list[Any]] = {k: [] for k in explicit_keys}
    batch_rows = 0
    for row in rows:
        for k in explicit_keys:
            v = row.get(k)
            if v is None or type(v) in _SCALAR_TYPES:
                cols[k].append(v)
            else:
                cols[k].append(serialize_nested(v))
        batch_rows += 1
        if batch_rows >= batch_size:
            yield cols
            cols = {k: [] for k in explicit_keys}
            batch_rows = 0
    if batch_rows:
        yield cols


def _stream_arrow_batches(
    data: Iterable[dict[str, Any]],
    kwargs: dict[str, Any],
) -> Iterator[Any]:
    """Yield ``pyarrow.Table`` batches, each capped at ``_WRITE_BATCH_ROWS`` rows.

    Shared by every columnar write path (Parquet / Feather / ORC) so all
    three formats share one batching, one schema-inference, and one
    row→column pivot.  Holds at most one batch (1024 rows by default) in
    RAM at any moment — multi-GB inputs no longer OOM.

    Schema handling:
    - If ``pydantic_schema`` is present, build an explicit Arrow schema
      up-front and apply it to every batch.  Fastest path, deterministic
      types, no per-batch inference cost.
    - Without it, the first batch carries the inferred schema and every
      subsequent batch reuses that inference path (pyarrow promotes types
      naturally across batches via ``Table.from_pydict``).

    Empty input yields nothing.
    """
    import pyarrow as pa

    pydantic_schema = kwargs.get("pydantic_schema", {})
    properties: dict[str, Any] = pydantic_schema.get("properties", {})
    explicit_keys: list[str] = kwargs.get("all_field_names") or list(properties.keys())

    # Iterator-vs-list dance: when we don't have an explicit-keys hint we
    # need to peek at the first row to discover its columns.  Pulling one row
    # with ``next()`` and chaining it back keeps the rest of the iterator
    # untouched and streaming.
    data_iter: Iterator[dict[str, Any]] = iter(data)
    if not explicit_keys:
        try:
            first_row = next(data_iter)
        except StopIteration:
            return
        explicit_keys = list(first_row.keys())
        import itertools

        data_iter = itertools.chain([first_row], data_iter)

    explicit_schema = _build_columnar_schema(explicit_keys, properties, kwargs)

    for batch_cols in _batched_columns(data_iter, explicit_keys, _WRITE_BATCH_ROWS):
        if explicit_schema is not None:
            yield pa.Table.from_pydict(batch_cols, schema=explicit_schema)
        else:
            yield pa.Table.from_pydict(batch_cols)


class ParquetHandler(BaseFormatHandler):
    """Parse and write .parquet files using pyarrow.

    Lazy-imports pyarrow on first use. Raises a clear ``IncorporatorFormatError``
    pointing to ``pip install incorporator[parquet]`` when the optional dep is
    missing.
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> list[dict[str, Any]]:
        """Read a Parquet file or byte buffer and yield rows as dicts.

        Uses ``pq.read_table().to_pylist()`` for the single-shot read path,
        then routes through ``_table_to_dicts`` for vectorised JSON-prefix
        detection — only string columns that actually contain JSON-encoded
        nested data pay the per-row Python parse cost.
        """
        pq = _require_optional("pyarrow.parquet")

        try:
            table = pq.read_table(_coerce_columnar_source(source, "ParquetHandler"))

            # _table_to_dicts mutates the dicts to_pylist() already allocated
            # and only touches string columns (where serialize_nested could
            # have JSON-encoded nested values).  Eliminates the double-dict
            # allocation and the per-cell isinstance(str) check.
            return _table_to_dicts(table)
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"Parquet Read Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows to a Parquet file in 1024-row Arrow batches.

        Uses ``ParquetWriter`` so memory holds at most one row group at a
        time — O(1) regardless of total dataset size. Honours
        ``parquet_compression`` (default ``"snappy"``) and ``pydantic_schema``
        (drives explicit Arrow types; without it, schema is inferred from
        the first batch). Append mode is rejected — Parquet's footer index
        makes safe appends require Hive-style partitioning.
        """
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        # Parquet files have a footer index — safe append requires Hive-style
        # partitioning, which is out of scope. Users who need append should
        # stream to NDJSON instead.
        _raise_if_append_unsupported(kwargs, "Parquet")

        _require_optional("pyarrow")  # dep-presence sentinel — same error as feather/orc
        pq = _require_optional("pyarrow.parquet")

        compression = kwargs.get("parquet_compression", "snappy")  # pyarrow's default

        _stream_columnar_write(
            data,
            file_path,
            kwargs,
            format_label="Parquet",
            sink_factory=lambda p: contextlib.nullcontext(p),  # ParquetWriter takes a Path directly
            build_writer=lambda sink, schema: pq.ParquetWriter(sink, schema, compression=compression),
            write_batch=lambda w, b: w.write_table(b),
        )


class FeatherHandler(BaseFormatHandler):
    """Parse and write Feather V2 (Apache Arrow IPC) files using pyarrow.

    Feather is the fastest interchange format for data-science workflows —
    memory-mapped reads with zero deserialization. The pyarrow.feather API is
    one-shot (no streaming writer), so writes materialize the dataset before
    flushing. For datasets large enough to exceed RAM, use Parquet instead.

    Compression defaults to LZ4 (Feather V2's native default).
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> list[dict[str, Any]]:
        """Read a Feather V2 file or byte buffer and yield rows as dicts.

        Uses memory-mapped reads where possible (Feather's headline feature).
        Routes through the same ``_table_to_dicts`` helper as Parquet/ORC so
        JSON-encoded nested cells re-hydrate consistently across columnar
        formats.
        """
        feather = _require_optional("pyarrow.feather")

        try:
            table = feather.read_table(_coerce_columnar_source(source, "FeatherHandler"))
            return _table_to_dicts(table)
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"Feather Read Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows into a Feather V2 (Arrow IPC) file in 1024-row batches.

        Feather V2 is Arrow IPC file format under the hood — pyarrow exposes a
        ``RecordBatchFileWriter`` via :func:`pyarrow.ipc.new_file` that
        accepts incremental batches.  Memory stays O(1) regardless of total
        dataset size, matching the Parquet write path.

        Honours ``feather_compression`` (default ``"lz4"``, Feather V2's
        native default) and ``pydantic_schema`` (drives explicit Arrow types).
        Append mode is rejected — the IPC file format has no streaming
        append API; users who need accumulating writes should use NDJSON.
        """
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        # Feather V2 has no append-friendly format spec — the file header is
        # written before the data and contains a footer index at close time.
        _raise_if_append_unsupported(kwargs, "Feather/Arrow IPC")

        pa = _require_optional("pyarrow")
        _require_optional("pyarrow.feather")  # dep-presence sentinel — same error message as the read path

        compression_str = kwargs.get("feather_compression", "lz4")  # Feather V2 default
        # pyarrow.ipc.new_file accepts an ipc.IpcWriteOptions struct rather than a
        # raw compression string.  Build it here so the kwarg API stays identical
        # to the previous feather.write_feather call site.
        options = pa.ipc.IpcWriteOptions(compression=compression_str) if compression_str else None

        _stream_columnar_write(
            data,
            file_path,
            kwargs,
            format_label="Feather",
            sink_factory=lambda p: pa.OSFile(str(p), "wb"),
            build_writer=lambda sink, schema: pa.ipc.new_file(sink, schema, options=options),
            write_batch=lambda w, b: w.write_table(b),
        )


class OrcHandler(BaseFormatHandler):
    """Parse and write Apache ORC files using pyarrow.

    ORC is the columnar format of the Hadoop/Hive ecosystem (Trino, Presto,
    Hive, Spark on Hadoop). The pyarrow.orc API is one-shot — writes
    materialize the dataset before flushing. Use Parquet for streaming.

    Note: pyarrow's ORC support has historically been platform-sensitive on
    Windows. The handler reports a clear error if pyarrow.orc fails to import
    even though pyarrow itself loaded successfully.
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> list[dict[str, Any]]:
        """Read an ORC file or byte buffer and yield rows as dicts.

        Routes through the same ``_table_to_dicts`` helper as Parquet/Feather
        for consistent JSON-encoded nested-cell re-hydration.
        """
        try:
            from pyarrow import orc
        except ImportError:
            raise IncorporatorFormatError(
                "pyarrow.orc not available. Run: pip install incorporator[parquet] "
                "(ORC support requires pyarrow with libarrow_orc; on some platforms "
                "this may need pyarrow built from source)."
            ) from None

        try:
            orc_file = orc.ORCFile(_coerce_columnar_source(source, "OrcHandler"))  # type: ignore[no-untyped-call]
            table = orc_file.read()  # type: ignore[no-untyped-call]
            return _table_to_dicts(table)
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"ORC Read Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows into an ORC file in 1024-row batches.

        pyarrow's ``ORCWriter`` accepts incremental ``write_table`` calls,
        so memory stays O(1) regardless of total dataset size — matching
        the Parquet and Feather streaming write paths.

        Honours ``pydantic_schema`` (drives explicit Arrow types). Append mode
        is rejected — ORC's stripe layout requires a single writer session.
        """
        # Empty guard handled centrally by _peek_iterable in handlers/__init__.py.
        _raise_if_append_unsupported(kwargs, "ORC")

        try:
            from pyarrow import orc
        except ImportError:
            raise IncorporatorFormatError(
                "pyarrow.orc not available. Run: pip install incorporator[parquet] "
                "(ORC support requires pyarrow with libarrow_orc)."
            ) from None

        # pyarrow's ORCWriter signature is ``ORCWriter(where, *, ...)`` — schema
        # is inferred from the first ``write()`` call rather than passed at
        # construction.  All subsequent batches must match that schema, which
        # is guaranteed by ``_stream_arrow_batches`` when an explicit
        # pydantic_schema is present, and by pyarrow's own promotion rules
        # in the inference path.  The build_writer lambda below ignores the
        # ``schema`` argument that ``_stream_columnar_write`` passes.
        _stream_columnar_write(
            data,
            file_path,
            kwargs,
            format_label="ORC",
            sink_factory=lambda p: contextlib.nullcontext(str(p)),
            build_writer=lambda sink, _schema: orc.ORCWriter(sink),  # type: ignore[no-untyped-call]
            write_batch=lambda w, b: w.write(b),
        )
