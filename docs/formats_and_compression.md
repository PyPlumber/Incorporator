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

## 🪄 How "Archive Extraction" Works

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