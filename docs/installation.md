# 📦 Installation & Environment Setup

The base package depends on three libraries: the Python Standard Library, Pydantic, and HTTPX. 

**Requires Python 3.10+.**  v1.2.1 dropped 3.9 support (the package
uses `@dataclass(slots=True)` and PEP 604 union syntax internally).
CI runs against **3.10 / 3.11 / 3.13** on Ubuntu and Windows.

For environments processing gigabytes of data, Incorporator supports lazy-loaded **Rust and C extensions** that release the Python Global Interpreter Lock (GIL).

---

## 1. The Base Installation (Standard)
Best for general use, IoT devices, API exploration, and standard ETL jobs.

```bash
pip install incorporator
```
**What this installs:**
* `pydantic (>=2.0)`: Rust-backed schema validation via `pydantic-core`.
* `httpx`: For asynchronous, multiplexed network requests.
* `tenacity`: For exponential backoff and network resilience.

*(Note: The base installation still natively supports JSON, CSV, XML, and SQLite, falling back on Python's highly optimized standard library).*

---

## 2. Optional Extras

Optional extras add format support or GIL-free parsing backends. Install only what your environment needs.

### 🚀 The Speedups Flag (GIL-Free Parsing + Rust Compression)
```bash
pip install incorporator[speedups]
```
**What this installs:** `orjson`, `lxml`, `cramjam`
* **GIL-free parsing:** Standard `json` and `xml` libraries hold the GIL during parsing. Installing `[speedups]` routes JSON through `orjson` and XML/HTML through `lxml`, releasing the GIL so other threads run during large payload parses. This also activates orjson in `LoggedTideweaver`'s `JSONFormatter` and in the `_read_filtered()` replay path — log write and replay throughput scales with the same GIL-free serialisation (`logger.py:309, 270`).
* **Rust-backed compression:** `cramjam` adds `zstd`, `lz4`, `snappy`, and `brotli` on top of the stdlib-supported `gzip`, `bz2`, `lzma`, `zip`, and `tar`.

### 🐘 The Avro Flag
```bash
pip install incorporator[avro]
```
**What this installs:** `fastavro`
* **Use this for:** Apache Avro binary stream read/write — the format used by Kafka producers and Hadoop-ecosystem pipelines.

### 📊 The Spreadsheet Flag
```bash
pip install incorporator[xlsx]
```
**What this installs:** `openpyxl` (pure-Python, ~250 KB).
* **Use this for:** `.xlsx` workbook read/write. At ~250 KB, openpyxl adds negligible install footprint.

### ⚡ The Columnar Data-Lake Flag
```bash
pip install incorporator[parquet]
```
**What this installs:** `pyarrow` (~30 MB — heavyweight; deliberately **not** in `[all]`), plus `tzdata` on Windows (pyarrow's ORC reader hardcodes `/usr/share/zoneinfo` lookups, which Windows lacks).
* **Use this for:** Apache Parquet, Feather / Arrow IPC, and ORC read/write. Required for data-lake and warehouse interoperability.

### 🛠️ The Orchestration Flag
```bash
pip install incorporator[orchestrate]
```
**What this installs:** `typer`, `prefect`.
* **Use this for:** The `incorporator stream`, `incorporator fjord`, `incorporator tideweaver`, `incorporator validate`, `incorporator init`, and `incorporator deps` CLI subcommands, plus the pre-built Prefect `@flow` wrappers (see `deployment.md`).

### 📖 The Docs Flag (Contributors Only)
```bash
pip install incorporator[docs]
```
**What this installs:** `pdoc`.
* **Why you need it:** Builds the auto-generated library reference site from the Google-style docstrings in the source. See `library_reference.md`.

---

## 3. The "Install Everything" Flag
If you are developing locally or running on a cloud server without strict dependency constraints, you can install all optional extras at once:

```bash
pip install incorporator[all]
```

**Note:** `[all]` deliberately excludes `[parquet]` (pyarrow is ~30 MB) and
`[docs]` (contributor-only). Opt into those explicitly when you need them.

---

## 4. Checking What's Installed
Not sure which extras actually landed? Ask Incorporator directly:

```bash
incorporator deps
```
Prints a table of every optional dependency — its category, extra, install status (`✓`/`✗`), and the `pip install` hint to fix anything missing. Filter with `--missing` (only what's absent), `--category speedup|format|orchestrate|platform_fix`, or `--json` for scripting.

*(This subcommand rides on Typer, so it requires `[orchestrate]` like the rest of the CLI.)*

---

## Next Steps
Now that Incorporator is installed, head over to [**Tutorial 1 — First Steps**](../examples/01-first-steps/README.md) to map your first API!

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/installation.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
