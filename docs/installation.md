# 📦 Installation & Environment Setup

Incorporator is designed with a **"Zero-Bloat"** philosophy. The base package relies purely on the Python Standard Library, Pydantic, and HTTPX. 

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

## 2. Enterprise Performance Flags (Optional)

If you are deploying Incorporator to process multi-gigabyte data streams, we highly recommend utilizing our optional installation flags.

### 🚀 The Speedups Flag (GIL-Free Hyperthreading + Rust Compression)
```bash
pip install incorporator[speedups]
```
**What this installs:** `orjson`, `lxml`, `cramjam`
* **Why you need it:** Standard Python `json` and `xml` libraries hold the Global Interpreter Lock (GIL). If you download a 500MB payload, the main Event Loop freezes while parsing it. Installing `[speedups]` routes Incorporator through Rust/C backends — `orjson` for JSON, `lxml` for XML/HTML — releasing the GIL and parsing massive payloads across multiple CPU cores simultaneously.
* **Bonus: ultra-fast compression.** The same extra also installs `cramjam`, which unlocks Rust-backed streaming for `zstd`, `lz4`, `snappy`, and `brotli` on top of the natively-supported `gzip`, `bz2`, `lzma`, `zip`, and `tar`. One install, both wins.

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

### ⚡ The Columnar Data-Lake Flag
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
* **Why you need it:** Unlocks the `incorporator stream`, `incorporator fjord`, and `incorporator tideweaver` CLI subcommands plus the pre-built Prefect `@flow` wrappers (see `deployment.md`).

### 📖 The Docs Flag (Contributors Only)
```bash
pip install incorporator[docs]
```
**What this installs:** `pdoc`.
* **Why you need it:** Builds the auto-generated library reference site from the Google-style docstrings in the source. See `library_reference.md`.

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
Now that Incorporator is installed, head over to [**Tutorial 1 — First Steps**](../examples/01-first-steps/README.md) to map your first API!

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/installation.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
