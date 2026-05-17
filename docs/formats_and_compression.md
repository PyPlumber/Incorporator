***

# 🗄️ Formats & Compression Cheat Sheet

Incorporator is built on a **Format Agnostic Engine**. Whether you are reading from a URL (`inc_url`) or a local file (`inc_file`), the framework dynamically infers the parsing strategy and decompression algorithm entirely from the file extension.

You never have to write custom extraction loops, and the API syntax *never* changes between formats.

---

## 📄 Supported Data Formats

Incorporator natively supports flat text files, streaming logs, and binary databases.

| Format | File Extensions | Installation | Notes |
| :--- | :--- | :--- | :--- |
| **JSON** | `.json` | *Native* | The default format. Deeply nested schemas are instantly converted to Python objects. |
| **NDJSON** | `.ndjson`, `.jsonl` | *Native* | Newline-Delimited JSON. Excellent for parsing massive streaming logs. |
| **CSV** | `.csv` | *Native* | Includes $O(1)$ Auto-Unflattening (safely parses JSON strings inside CSV columns). |
| **TSV** | `.tsv` | *Native* | Tab-Separated Values. Uses the CSV engine configured for `\t`. |
| **PSV** | `.psv` | *Native* | Pipe-Separated Values. Uses the CSV engine configured for `\|`. |
| **XML** | `.xml` | *Native* | Hardened against XXE attacks (external-entity injection, Billion Laughs DoS). Falls back to `defusedxml` semantics on the stdlib parser. |
| **HTML Tables** | `.html`, `.htm` | `[speedups]` | Extracts `<table>` elements via `lxml`. Each row becomes an Incorporator instance; mismatched columns are handled gracefully. |
| **SQLite** | `.db`, `.sqlite*` | *Native* | Natively executes `SELECT` and bulk `INSERT` statements at C-speed. |
| **Excel** | `.xlsx` | `[xlsx]` | Pure-Python via `openpyxl` (~250 KB). Reads/writes worksheets with header rows auto-detected. |
| **Apache Avro** | `.avro` | `[avro]` | Requires `pip install incorporator[avro]`. Converts Pydantic to strict binary schemas. |
| **Apache Parquet** | `.parquet` | `[parquet]` | Columnar format for data lakes / warehouses. Uses `pyarrow` (heavyweight — opt-in only). |
| **Feather / Arrow IPC** | `.feather`, `.arrow` | `[parquet]` | Zero-copy columnar interchange. Shares the `pyarrow` install with Parquet. |
| **Apache ORC** | `.orc` | `[parquet]` | Hadoop / Hive columnar format. Also via `pyarrow`. |

---

## 🗜️ Supported Compression & Archives

If a URL or File ends with a recognized compression extension, Incorporator will automatically decompress the bytes in a background thread before handing them to the data parser. 

*Example:* `https://api.com/dump.csv.gz` will be natively downloaded, decompressed via `gzip`, and parsed as a `csv`.

| Algorithm | Extensions | Installation | Notes |
| :--- | :--- | :--- | :--- |
| **Gzip** | `.gz` | *Native* | Streamed decompressed securely in RAM. |
| **Bzip2** | `.bz2` | *Native* | Highly compressed, native standard library support. |
| **LZMA / XZ** | `.lzma`, `.xz` | *Native* | Modern compression natively supported by Python. |
| **ZIP** | `.zip` | *Native* | Directory archive. Automatically searches for the valid data file (see below). |
| **Tarball** | `.tar`, `.tgz` | *Native* | Directory archive. Uses $O(1)$ linear iteration for lightning-fast extraction. |
| **Zstandard** | `.zst` | `[speedups]` | Requires `pip install incorporator[speedups]`. Ultra-fast Rust bindings via `cramjam`. |
| **LZ4** | `.lz4` | `[speedups]` | Requires `pip install incorporator[speedups]`. Ultra-fast Rust bindings via `cramjam`. |
| **Snappy** | `.snappy` | `[speedups]` | Requires `pip install incorporator[speedups]`. Standard for Hadoop environments. |
| **Brotli** | `.br` | `[speedups]` | Requires `pip install incorporator[speedups]`. High-density web compression. |

---

## ✨ How "Archive Extraction" Works

Formats like `.zip` and `.tar` are not just compressed streams; they are directories containing multiple files. 

If you point Incorporator to an archive (e.g., `await MyClass.incorp("data_dump_2026.zip")`), the framework performs the following magic entirely in the background:
1. It unzips the archive in memory.
2. It ignores system junk files (like `__MACOSX` folders or hidden `.DS_Store` files).
3. It scans the directory tree until it finds the **very first valid data file** (ending in `.json`, `.csv`, or `.xml`).
4. It extracts only that specific file and passes it to the format parser.

This means you can hand Incorporator a 50GB ZIP file containing thousands of images and a single `data.json` file, and it will flawlessly extract and parse the JSON without you writing a single line of `zipfile` boilerplate!

---

## 💼 Installing Enterprise Plugins

Incorporator strictly adheres to a "Zero-Bloat" philosophy. The base installation (`pip install incorporator`) is incredibly lightweight and only uses standard Python libraries.

If you are a Data Engineer working with massive Kafka streams or Hadoop clusters, you can opt-in to our Big Data plugins:

```bash
# Unlocks GIL-releasing orjson + lxml parsing AND Rust-backed Zstandard, LZ4,
# Snappy, and Brotli compression via cramjam — all bundled in one extra.
pip install incorporator[speedups]

# Unlocks binary Apache Avro streams via the fastavro library
pip install incorporator[avro]

# Unlocks Excel (.xlsx) read/write via openpyxl (pure-Python, lightweight)
pip install incorporator[xlsx]

# Unlocks Parquet, Feather/Arrow, and ORC via pyarrow (heavyweight, ~30 MB)
pip install incorporator[parquet]

# Installs the complete Big Data suite (excludes [parquet] — opt in explicitly)
pip install incorporator[all]
```

---

## 🔄 Cross-Format Type Bridge

Every supported format runs through a single, **public** type-bridge
contract — the ``FORMAT_TO_PYTHON`` and ``PYTHON_TO_FORMAT`` dicts in
[`incorporator/io/formats.py`](../incorporator/io/formats.py).  Two
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
``dict`` / ``list`` cells through JSON encoding.

### Round-Trip Preservation Notes

The bridge handles a handful of format-specific quirks transparently.
Each is opt-in or opt-out via a kwarg you'll see in the handler docs:

* **Avro field names** — Avro rejects hyphens, numeric prefixes, and
  most non-identifier characters.  Names like ``"user-id"`` are
  sanitised to ``"user_id"`` on write and restored to the original
  on read via a ``__incorporator_original_names__`` schema metadata
  entry.  Cross-tool consumers (other Avro readers) see the
  sanitised names; Incorporator-to-Incorporator round-trips are
  fully lossless.
* **SQLite booleans** — SQLite has no native BOOLEAN type and stores
  ``True`` / ``False`` as integer ``1`` / ``0``.  Pass
  ``sql_bool_columns=["is_active", "is_paid"]`` on read to recover
  the bool semantics.  Without the kwarg, those columns come back as
  ``int`` (documented behaviour — the column type is genuinely
  ambiguous in SQLite's storage layer).
* **Parquet `Decimal` / tz-aware `datetime`** — JSON Schema doesn't
  distinguish these from generic numbers / strings, so the columnar
  writer defaults to ``pa.string()``.  Opt in via
  ``parquet_decimal_columns=["amount"]`` /
  ``parquet_timestamp_columns=["recorded_at"]``.  Precision / scale /
  timestamp unit / timezone are configurable
  (``parquet_decimal_precision`` default 38,
  ``parquet_decimal_scale`` default 18,
  ``parquet_timestamp_unit`` default ``us``,
  ``parquet_timestamp_tz`` default ``"UTC"``).
* **XML tag shape drift** — when the same tag appears once in one
  document and multiple times in another, the default parser would
  yield a scalar in case A and a list in case B — a real headache
  for downstream schema inference.  Pass
  ``xml_force_list=["item", "row"]`` to force those tags to always
  be lists.
* **CSV empty cells** — default ``csv_empty_as_none=True`` maps
  blank cells to Python ``None`` so Pydantic's ``Optional[T]``
  semantics work as users expect.  Opt out with
  ``csv_empty_as_none=False`` when the empty string is genuinely
  meaningful.
* **CSV / Excel formula injection** — cells whose string value
  starts with ``=`` / ``@`` / ``+`` / ``-`` execute as formulas
  when opened in Excel, LibreOffice Calc, or Google Sheets.
  Defence-in-depth: those cells get a single-quote prefix on write
  (``csv_safe_formulas`` and ``xlsx_safe_formulas`` both default to
  ``True``).  Set either to ``False`` for raw passthrough when the
  consumer is known to be a non-spreadsheet tool.

### Adding a New Format

The two-step recipe (well under an hour for most formats):

1. **Add the type-bridge entries** in
   [`incorporator/io/formats.py`](../incorporator/io/formats.py):
   * One row per Python type in ``FORMAT_TO_PYTHON``
     (``(FormatType.NEW, "format_type_string"): python_type``)
   * One reverse row per Python type in ``PYTHON_TO_FORMAT``
     (``(FormatType.NEW, python_type): "format_type_string"``)
   * Add the enum value to ``FormatType`` and the extension
     matcher in ``infer_format()``.
2. **Register a handler** in
   [`incorporator/io/handlers/__init__.py`](../incorporator/io/handlers/__init__.py):
   * Subclass ``BaseFormatHandler`` (in
     ``incorporator/io/handlers/_base.py``) with concrete
     ``parse(self, source, **kwargs)`` and
     ``write(self, data, file_path, **kwargs)`` methods.
   * Add an entry to ``_HANDLERS`` so the dispatcher routes the new
     extension correctly.
   * If the format has a mandatory third-party library, use
     ``_require_optional("lib_name")`` from
     ``incorporator/io/handlers/_base.py`` — the helper raises a
     uniform "pip install" error message when the dep is missing.

That's the whole contract.  No other file needs to know the new
format exists — `incorp()`, `refresh()`, `export()`, `stream()`,
and `fjord()` all route through the dispatcher and the type bridge.

### Design Rationale — Why Not a Third-Party Schema Library?

The audit phase evaluated ten public libraries that overlap our
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
round-trip preservation hacks).  Adopting any of the heavyweights
either doubles install size (SQLAlchemy alone adds ~15 MB) or
requires re-architecting around the upstream's canonical schema
(dlt, datacontract-cli).  The trade-off doesn't earn the
~250 LOC of bridge code it would replace.

We keep the bridge in-house because it's small, centralised,
dep-light, and aligned with Pydantic v2's type model.  The two
``FORMAT_TO_PYTHON`` / ``PYTHON_TO_FORMAT`` dicts are 105 entries
total — small enough to read end-to-end in one sitting.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Build a snapshot warehouse with append-friendly formats | [Tutorial 2 — Universal Formats](./2_universal_formats.md) |
| Land columnar Parquet at the end of an orchestration window | [Appendix — Parquet Snapshots in a Tideweaver Window](./appendix/tideweaver_parquet_snapshots.md) |
| Round-trip JSON ↔ Avro ↔ SQLite with nested reconstruction | [Appendix — Data Lake Pivot](./appendix/data_lake_pivot.md) |
| Stream a file too big to fit in RAM | [Streaming & Pagination Deep Dive](./streaming_and_pagination.md) |
| Tune per-format throughput | [Performance Guide](./performance.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/formats_and_compression.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)