# 💖 Acknowledgements & Open Source Thanks

**Incorporator** was built on the philosophy of "Zero-Bloat" and maximum leverage. We did not reinvent the wheel; instead, we orchestrated some of the finest, most battle-tested open-source libraries in the Python ecosystem into a single unified gateway.

We want to express our deepest gratitude to the maintainers, contributors, and communities behind the following projects.

---

### 🏛️ The Core Pillars
These three libraries form the absolute bedrock of Incorporator's baseline installation.

* **[Pydantic](https://pydantic.dev/)** *(by Samuel Colvin & Contributors)*
  * **How we use it:** Pydantic V2 and its Rust-backed `pydantic-core` are the engine behind our "Tolerant Metaprogramming Shield." We utilize `create_model` and `TypeAdapter` extensively to compile dynamic, type-safe Python classes at runtime. Thank you for making Python typing so incredibly fast and powerful.
* **[HTTPX](https://www.python-httpx.org/)** *(by Encode / Tom Christie & Contributors)*
  * **How we use it:** The beating heart of our asynchronous network orchestrator. We rely on HTTPX's native connection pooling, `asyncio` sliding-window concurrency, and pristine Request/Response API to multiplex thousands of API calls without blocking the event loop.
* **[Tenacity](https://tenacity.readthedocs.io/)** *(by Julien Danjou & Contributors)*
  * **How we use it:** The invisible guardian of our network layer. Tenacity provides the flawless exponential backoff and jitter logic that allows Incorporator to gracefully absorb HTTP 429s and 5xx errors without crashing user applications.

---

### 🚀 The "Hyperthreading & Big Data" Heroes
For enterprise users who install `incorporator[all]`, these C and Rust extensions are lazy-loaded to bypass the Python Global Interpreter Lock (GIL) and unlock true multi-core processing.

* **[orjson](https://github.com/ijl/orjson)** *(by ijl & Contributors)*
  * **How we use it:** By dropping the Python GIL, `orjson` allows Incorporator to deserialize multi-gigabyte API payloads on background threads across multiple CPU cores simultaneously. It is a masterpiece of Rust engineering.
* **[lxml](https://lxml.de/)** *(by lxml dev team)*
  * **How we use it:** Provides our `XMLHandler` with blazing-fast, GIL-free XML parsing. Crucially, it provides the native C-level security flags (`resolve_entities=False`) that protect Incorporator users from XXE "Billion Laughs" attacks.
* **[cramjam](https://github.com/milesgranger/cramjam)** *(by Miles Granger & Contributors)*
  * **How we use it:** Unlocks highly memory-efficient, chunked Rust-bindings for `zstd`, `lz4`, `snappy`, and `brotli` decompression, protecting our users from Out-Of-Memory (OOM) leaks when downloading massive archives.
* **[fastavro](https://fastavro.readthedocs.io/)** *(by fastavro dev team)*
  * **How we use it:** Bridges the gap between flexible JSON APIs and strict Hadoop/Kafka data lakes by providing lightning-fast Cython bindings for reading and writing Apache Avro streams.

---

### 🛠️ The Quality Assurance Tooling
To guarantee `--strict` typing compliance and maintain a flawless test suite, we rely on the industry standards:

* **[pytest](https://pytest.org/) & [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)**: For providing the fixtures, async loop management, and mocking capabilities that keep our orchestrator bug-free.
* **[mypy](https://mypy-lang.org/)**: For enforcing the strict static typing boundaries that keep Incorporator predictable and safe for enterprise deployment.

---
*To the thousands of unnamed open-source contributors who maintain the Python Standard Library (`asyncio`, `sqlite3`, `csv`, `queue`): **Thank you for building the language we love.***
