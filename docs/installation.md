# 📦 Installation & Environment Setup

Incorporator is designed with a **"Zero-Bloat"** philosophy. The base package relies purely on the Python Standard Library, Pydantic, and HTTPX. 

However, for enterprise environments processing data, Incorporator supports lazy-loaded **Rust and C extensions** that unlock native hyperthreading and bypass the Python Global Interpreter Lock (GIL).

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

---

## 3. The "Install Everything" Flag
If you are developing locally or running on a cloud server without strict dependency constraints, you can install the entire suite of enterprise tools at once:

```bash
pip install incorporator[all]
```
