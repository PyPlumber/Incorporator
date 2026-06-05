***

# Formats & Compression Cheat Sheet

`incorp()` infers the parsing strategy and decompression algorithm from the file
extension — whether you are reading from a URL or a local file. No format
adapter classes to instantiate, no `format=` kwarg required in the common case.

The same `incorp()` call works whether the source is a JSON API, a gzipped
CSV, a ZIP archive, or a Parquet column store — the parsing surface never
changes.

---

## Supported Data Formats

Incorporator natively supports flat text files, streaming logs, and binary databases.

| Format | File Extensions | Installation | Notes |
| :--- | :--- | :--- | :--- |
| **JSON** | `.json` | *Native* | Default format. Deeply nested schemas are converted to Python objects without a class definition. |
| **NDJSON** | `.ndjson`, `.jsonl` | *Native* | Newline-Delimited JSON. Well-suited for streaming logs and append-mode Tideweaver exports. |
| **CSV** | `.csv` | *Native* | Includes auto-unflattening (parses JSON strings inside CSV cells back to dicts/lists on read). |
| **TSV** | `.tsv` | *Native* | Tab-Separated Values. Uses the CSV engine configured for `\t`. |
| **PSV** | `.psv` | *Native* | Pipe-Separated Values. Uses the CSV engine configured for `\|`. |
| **XML** | `.xml` | *Native* | Hardened against XXE and Billion Laughs. Pre-flight regex blocks DTDs and external entities; lxml parser uses `resolve_entities=False` when available. |
| **HTML Tables** | `.html`, `.htm` | `[speedups]` | Extracts `<table>` elements via `lxml`. Each row becomes an Incorporator instance; mismatched columns are handled gracefully. |
| **SQLite** | `.db`, `.sqlite`, `.sqlite3` | *Native* | Executes `SELECT` and bulk `INSERT` statements via the stdlib `sqlite3` C extension. |
| **Excel** | `.xlsx`, `.xlsm` | `[xlsx]` | Pure-Python via `openpyxl` (~250 KB). Reads/writes worksheets with header rows auto-detected. |
| **Apache Avro** | `.avro` | `[avro]` | Requires `pip install incorporator[avro]`. Converts Pydantic schemas to strict binary Avro schemas on write. |
| **Apache Parquet** | `.parquet`, `.pq` | `[parquet]` | Columnar format for data lakes / warehouses. Uses `pyarrow` (heavyweight — opt-in only). |
| **Feather / Arrow IPC** | `.feather`, `.arrow`, `.ipc` | `[parquet]` | Zero-copy columnar interchange. Shares the `pyarrow` install with Parquet. |
| **Apache ORC** | `.orc` | `[parquet]` | Columnar format also via `pyarrow`. |

---

## Supported Compression & Archives

If a URL or file path ends with a recognised compression extension, Incorporator
decompresses the bytes before handing them to the format parser.

*Example:* `https://api.com/dump.csv.gz` is downloaded, decompressed via `gzip`,
and parsed as CSV — one `incorp()` call, no manual streaming.

| Algorithm | Extensions | Installation | Notes |
| :--- | :--- | :--- | :--- |
| **Gzip** | `.gz` | *Native* | Streamed decompression in RAM. |
| **Bzip2** | `.bz2` | *Native* | Higher compression ratio; native standard library. |
| **LZMA / XZ** | `.lzma`, `.xz` | *Native* | Higher compression ratio than gzip/bz2; native standard library. |
| **ZIP** | `.zip` | *Native* | Multi-file archive. Scans for the target data file (see below). |
| **Tarball** | `.tar`, `.tgz` | *Native* | Multi-file archive. Extracts only the target file — no full-archive decompression. |
| **Zstandard** | `.zst` | `[speedups]` | Requires `pip install incorporator[speedups]`. Rust bindings via `cramjam`. |
| **LZ4** | `.lz4` | `[speedups]` | Requires `pip install incorporator[speedups]`. Rust bindings via `cramjam`. |
| **Snappy** | `.snappy` | `[speedups]` | Requires `pip install incorporator[speedups]`. Common in columnar pipeline stacks. |
| **Brotli** | `.br` | `[speedups]` | Requires `pip install incorporator[speedups]`. High-density web compression. |

---

## How Archive Extraction Works

`.zip` and `.tar` are directories, not just compressed streams.

If you point `incorp()` to an archive, the engine:

1. Opens the archive in memory.
2. Skips `__MACOSX` system folders and any member name containing path-traversal
   sequences (validated against a temporary resolved directory — ZIP slip blocked
   pre-extraction, not post).
3. Scans the directory tree for a file matching the target format. The search
   covers 8 data-format families: JSON, NDJSON, CSV, TSV, PSV, XML, SQLite, and
   Avro. If more than one matching file is found, an error is raised — you must
   pass `archive_target="data.json"` to disambiguate.
4. Extracts only that file and passes it to the format parser.

A decompression-bomb cap of **1 GB** applies to all algorithms. Raise it for
legitimate large payloads via the env var:

```bash
export INCORPORATOR_MAX_DECOMPRESSED_BYTES=5368709120  # 5 GB
```

---

## Installing Optional Extras

The base installation (`pip install incorporator`) uses only the standard library
and `httpx` / `pydantic` / `tenacity`. Format-specific extras opt in:

```bash
# GIL-releasing C/Rust parsing (orjson + lxml), Rust compression
# (Zstandard, LZ4, Snappy, Brotli via cramjam), and orjson serialisation
# in the logger pipeline — all in one extra.
pip install incorporator[speedups]

# Apache Avro read/write via fastavro
pip install incorporator[avro]

# Excel (.xlsx) read/write via openpyxl (pure-Python, lightweight)
pip install incorporator[xlsx]

# Parquet, Feather/Arrow IPC, and ORC via pyarrow (~50 MB)
pip install incorporator[parquet]

# All optional format extras (excludes [parquet] — opt in explicitly)
pip install incorporator[all]
```

---

## Cross-Format Type Bridge

Every supported format runs through a single, **public** type-bridge
contract — the `FORMAT_TO_PYTHON` and `PYTHON_TO_FORMAT` dicts in
[`incorporator/io/formats.py`](../incorporator/io/formats.py). Two
small dictionaries are the single source of truth for how each
Python type encodes into every wire format, so you can predict a
round-trip without grepping the source.

### The Rosetta Stone

How each Python type lands in each supported format:

| Python | JSON Schema | Avro | SQLite | Parquet | Feather | ORC |
|---|---|---|---|---|---|---|
| `bool` | `boolean` | `boolean` | `INTEGER` (0/1, recover via `sql_bool_columns`) | `bool` | `bool` | `bool` |
| `int` | `integer` | `long` | `INTEGER` | `int64` | `int64` | `int64` |
| `float` | `number` | `double` | `REAL` | `double` | `double` | `double` |
| `str` | `string` | `string` | `TEXT` | `string` | `string` | `string` |
| `bytes` | — | `bytes` | `BLOB` | `binary` | `binary` | `binary` |
| `list` / `dict` | `array` / `object` | `string` (JSON-encoded) | `TEXT` (JSON-encoded) | `string` (JSON-encoded) | same | same |
| `Decimal` | `string` *(format=decimal)* | — | — | `decimal128(38, 18)` *(opt-in)* | same | same |
| `datetime` *(tz-aware)* | `string` *(format=date-time)* | — | — | `timestamp[us, UTC]` *(opt-in)* | same | same |
| `None` | `null` | `null` | `NULL` | `null` | `null` | `null` |

CSV / TSV / PSV inherit JSON Schema's row contract: every cell is
serialised as a string with `serialize_nested()` round-tripping
`dict` / `list` cells through JSON encoding. As of v1.2.3,
`serialize_nested()` encodes those cells through orjson when
`[speedups]` is installed (falling back to stdlib `json` otherwise) —
round-trip-equivalent, though orjson emits whitespace-free JSON.

### Round-Trip Preservation Notes

The bridge handles a handful of format-specific quirks transparently.
Each is opt-in or opt-out via a kwarg you'll see in the handler docs:

* **Avro field names** — Avro rejects hyphens, numeric prefixes, and
  most non-identifier characters. Names like `"user-id"` are
  sanitised to `"user_id"` on write and restored to the original
  on read via a `__incorporator_original_names__` schema metadata
  entry. Cross-tool consumers (other Avro readers) see the
  sanitised names; Incorporator-to-Incorporator round-trips are
  fully lossless.
* **SQLite booleans** — SQLite has no native BOOLEAN type and stores
  `True` / `False` as integer `1` / `0`. Pass
  `sql_bool_columns=["is_active", "is_paid"]` on read to recover
  the bool semantics. Without the kwarg, those columns come back as
  `int` (documented behaviour — the column type is genuinely
  ambiguous in SQLite's storage layer).
* **Parquet `Decimal` / tz-aware `datetime`** — JSON Schema doesn't
  distinguish these from generic numbers / strings, so the columnar
  writer defaults to `pa.string()`. Opt in via
  `parquet_decimal_columns=["amount"]` /
  `parquet_timestamp_columns=["recorded_at"]`. Precision / scale /
  timestamp unit / timezone are configurable
  (`parquet_decimal_precision` default 38,
  `parquet_decimal_scale` default 18,
  `parquet_timestamp_unit` default `us`,
  `parquet_timestamp_tz` default `"UTC"`).
* **XML tag shape drift** — when the same tag appears once in one
  document and multiple times in another, the default parser yields
  a scalar in case A and a list in case B. Pass
  `xml_force_list=["item", "row"]` to force those tags to always
  be lists.
* **CSV empty cells** — default `csv_empty_as_none=True` maps
  blank cells to Python `None` so `T | None` semantics work as
  expected. Opt out with `csv_empty_as_none=False` when the empty
  string is genuinely meaningful.
* **CSV / Excel formula injection** — cells whose string value
  starts with `=` / `@` / `+` / `-` execute as formulas
  when opened in Excel, LibreOffice Calc, or Google Sheets.
  Defence-in-depth: those cells get a single-quote prefix on write
  (`csv_safe_formulas` and `xlsx_safe_formulas` both default to
  `True`). Set either to `False` for raw passthrough when the
  consumer is a non-spreadsheet tool.
* **Append-safety predicate** — `FormatType.is_append_safe` is the
  canonical public check for whether a format's write handler accepts
  `if_exists="append"`. Returns `True` for NDJSON / CSV / TSV /
  PSV / SQLite / Avro (record-oriented); `False` for JSON / XML /
  XLSX / Parquet / Feather / ORC / HTML (monolithic — writing one
  chunk overwrites the prior file). `stream()` / `fjord()` /
  Tideweaver consult this at engine-selection time and reject
  impossible combos before any data moves.

### Adding a New Format

The two-step recipe:

1. **Add the type-bridge entries** in
   [`incorporator/io/formats.py`](../incorporator/io/formats.py):
   * One row per Python type in `FORMAT_TO_PYTHON`
     (`(FormatType.NEW, "format_type_string"): python_type`)
   * One reverse row per Python type in `PYTHON_TO_FORMAT`
     (`(FormatType.NEW, python_type): "format_type_string"`)
   * Add the enum value to `FormatType` and the extension
     matcher in `infer_format()`.
2. **Register a handler** in
   [`incorporator/io/handlers/__init__.py`](../incorporator/io/handlers/__init__.py):
   * Subclass `BaseFormatHandler` (in
     `incorporator/io/handlers/_base.py`) with concrete
     `parse(self, source, **kwargs)` and
     `write(self, data, file_path, **kwargs)` methods.
   * Add an entry to `_HANDLERS` so the dispatcher routes the new
     extension correctly.
   * If the format requires a third-party library, use
     `_require_optional("lib_name")` from
     `incorporator/io/handlers/_base.py` — the helper raises a
     uniform "pip install" error message when the dep is missing.

That's the whole contract. No other file needs to know the new
format exists — `incorp()`, `refresh()`, `export()`, `stream()`,
and `fjord()` all route through the dispatcher and the type bridge.

### Design Rationale — Why Not a Third-Party Schema Library?

An audit evaluated ten public libraries that overlap this
type-bridge surface area:
[`pyarrow`](https://arrow.apache.org/docs/python/) (already a
dep), [`frictionless-py`](https://framework.frictionlessdata.io/),
[Pydantic v2's `model_json_schema()`](https://docs.pydantic.dev/),
[`dlt`](https://dlthub.com/), [`DuckDB`](https://duckdb.org/),
[`Pandera`](https://pandera.readthedocs.io/),
[`msgspec`](https://jcristharif.com/msgspec/),
[`SQLAlchemy 2.0`](https://www.sqlalchemy.org/),
[`py-avro-schema`](https://github.com/jpmorganchase/py-avro-schema),
[`datacontract-cli`](https://datacontract.com/).

**No single library covers all four surfaces** the bridge handles
(static type mapping, value coercion, sample-based schema inference,
round-trip preservation hacks). Adopting any of the heavyweights
either doubles install size (SQLAlchemy alone adds ~15 MB) or
requires re-architecting around the upstream's canonical schema
(dlt, datacontract-cli). The trade-off doesn't earn the
~250 LOC of bridge code it would replace.

The bridge stays in-house because it's small, centralised,
dep-light, and aligned with Pydantic v2's type model. The two
`FORMAT_TO_PYTHON` / `PYTHON_TO_FORMAT` dicts are 83 entries
total (FORMAT_TO_PYTHON: 42, PYTHON_TO_FORMAT: 41) — small enough
to read end-to-end in one sitting.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Build a snapshot warehouse with append-friendly formats | [Tutorial 3 — Universal Formats](../examples/03-universal-formats/README.md) |
| Land columnar Parquet at the end of an orchestration window | [Appendix — Parquet Snapshots in a Tideweaver Window](../examples/appendix/tideweaver-parquet-snapshots/README.md) |
| Round-trip JSON ↔ Avro ↔ SQLite with nested reconstruction | [Tutorial 2 — Data Lake Pivot](../examples/02-data-lake-pivot/README.md) |
| Stream a file too big to fit in RAM | [Streaming & Pagination Deep Dive](./streaming_and_pagination.md) |
| Tune per-format throughput | [Performance Guide](./performance.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/formats_and_compression.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
