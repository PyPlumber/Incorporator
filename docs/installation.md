# 📦 Installation & Environment Setup

Incorporator is designed with a **"Zero-Bloat"** philosophy. The base package relies purely on the Python Standard Library, Pydantic, and HTTPX. 

However, for enterprise environments processing gigabytes of data, Incorporator supports lazy-loaded **Rust and C extensions** that unlock native hyperthreading and bypass the Python Global Interpreter Lock (GIL).

---

## 1. The Base Installation (Standard)
Best for general use, IoT devices, API exploration, and standard ETL jobs.

```bash
pip install incorporator
```
**What this installs:**
* `pydantic (>=2.0)`: For blazing-fast Rust-backed schema validation.
* `httpx`: For asynchronous, multiplexed network requests.
* `tenacity`: For exponential backoff and network resilience.

*(Note: The base installation still natively supports JSON, CSV, XML, and SQLite, falling back on Python's highly optimized standard library).*

---

## 2. Enterprise Performance Flags (Optional)

If you are deploying Incorporator to process multi-gigabyte data streams, we highly recommend utilizing our optional installation flags.

### 🚀 The Speedups Flag (GIL-Free Hyperthreading)
```bash
pip install incorporator[speedups]
```
**What this installs:** `orjson`, `lxml`
* **Why you need it:** Standard Python `json` and `xml` libraries hold the Global Interpreter Lock (GIL). If you download a 500MB payload, the main Event Loop freezes while parsing it. Installing `[speedups]` automatically routes Incorporator through Rust/C backends, releasing the GIL and allowing your OS to parse multiple massive payloads across different CPU cores simultaneously.

### 🗜️ The Compression Flag
```bash
pip install incorporator[cramjam]
```
**What this installs:** `cramjam`
* **Why you need it:** While Incorporator natively supports `gzip`, `bz2`, `zip`, and `tar`, installing `cramjam` unlocks ultra-fast, Rust-backed chunk streaming for `zstd`, `lz4`, `snappy`, and `brotli`. It ensures O(1) memory safety when decompressing massive server logs.

### 🐘 The Big Data Flag
```bash
pip install incorporator[avro]
```
**What this installs:** `fastavro`
* **Why you need it:** Unlocks native read/write support for Apache Avro binary streams, heavily utilized in Hadoop, Kafka, and enterprise data lakes.

### 📊 The Spreadsheet Flag
```bash
pip install incorporator[xlsx]
```
**What this installs:** `openpyxl` (pure-Python, ~250 KB).
* **Why you need it:** Unlocks read/write for `.xlsx` workbooks so you can hand business stakeholders a spreadsheet straight from an API call. Lightweight enough to fit the microclient identity.

### 🪶 The Columnar Data-Lake Flag
```bash
pip install incorporator[parquet]
```
**What this installs:** `pyarrow` (~30 MB — heavyweight; deliberately **not** in `[all]`).
* **Why you need it:** Unlocks Apache **Parquet**, **Feather / Arrow IPC**, and **ORC** read/write. Required for data-lake and warehouse interoperability.

### 🛠️ The Orchestration Flag
```bash
pip install incorporator[orchestrate]
```
**What this installs:** `typer`, `prefect`.
* **Why you need it:** Unlocks the `incorporator stream` and `incorporator fjord` CLI subcommands plus the pre-built Prefect `@flow` wrappers (see `deployment.md`).

### 📖 The Docs Flag (Contributors Only)
```bash
pip install incorporator[docs]
```
**What this installs:** `pdoc`.
* **Why you need it:** Builds the auto-generated API reference site from the Google-style docstrings in the source. See `api_reference.md`.

---

## 3. The "Install Everything" Flag
If you are developing locally or running on a cloud server without strict dependency constraints, you can install the bundled enterprise tools at once:

```bash
pip install incorporator[all]
```

**Note:** `[all]` deliberately excludes `[parquet]` (pyarrow is ~30 MB) and
`[docs]` (contributor-only). Opt into those explicitly when you need them.

---

## Next Steps
Now that Incorporator is installed, head over to our [**Quick Setup Tutorial**](./1_quick_setup.md) to map your first API!
```

### Why this document works:
1. **It builds trust:** Senior engineers hate "magic" dependencies. By explicitly stating *why* `orjson` and `lxml` are used (to release the GIL), you immediately prove that this framework is built by performance experts.
2. **It keeps the README clean:** The README stays focused on code examples and features, while this document handles the pedantic environment-setup details.