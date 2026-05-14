***

# 🚀 Incorporator

**A schema-free data mapper that turns JSON, XML, or CSV into a unified Python object graph with dot-notation and access-at-runtime.**

<!-- DISTRIBUTION -->
[![PyPI version](https://img.shields.io/pypi/v/incorporator?color=blue)](https://pypi.org/project/incorporator/)
[![Python Versions](https://img.shields.io/pypi/pyversions/incorporator.svg)](https://pypi.org/project/incorporator/)
[![Downloads](https://img.shields.io/pypi/dm/incorporator?color=blue)](https://pypi.org/project/incorporator/)

<!-- CODE QUALITY -->
[![mypy: strict](https://img.shields.io/badge/mypy-strict-blue.svg)](https://mypy.readthedocs.io/en/stable/command_line.html#cmdoption-mypy-strict)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Linter: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

<!-- STACK -->
[![Pydantic v2](https://img.shields.io/badge/pydantic-v2.0+-e92063.svg)](https://pydantic.dev/)
[![HTTPX](https://img.shields.io/badge/httpx-async-blue.svg)](https://www.python-httpx.org/)

<!-- OPEN SOURCE -->
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub stars](https://img.shields.io/github/stars/PyPlumber/incorporator?color=yellow&label=stars)](https://github.com/PyPlumber/incorporator/stargazers)

### ✨ Highlights
* **Works with unpredictable JSON APIs**—and effortlessly digests XML, CSV, NDJSON, SQLite, and columnar Parquet—without writing a single line of schema.
* **Turns raw data into native Python objects instantly**, bypassing the need for manual model definitions or brittle classes.
* **Handles changing JSON structures at runtime**, absorbing missing keys or mutating data types without throwing validation errors.
* **Harnesses Pydantic and HTTPX** under the hood without forcing you to write data classes, connection poolers, or pagination `while` loops.

### 🎯 Use this when:
* You are working with evolving, undocumented, or heavily nested JSON APIs.
* You need a universal bridge to map legacy XML, flat CSVs, or columnar Parquet into the exact same Python object graph.
* You are exhausted by writing boilerplate models and validation logic just to explore a new data source.
* You need to extract deeply nested web data, transform it, and pivot it straight into a local SQL database or columnar data lake.

---

## 🛠️ How it Works: Zero-Schema Ingestion

Imagine receiving this spacecraft telemetry JSON. Notice how the nested `"st"` dictionary **changes its structure completely** for every subsystem (`pos` vs `sig` vs `bat`). Standard parsers would crash instantly.

**The Input (`telemetry.json`):**
```json
[
  {"id":"NAV", "st":{"pos":[12,44], "ok":1}},
  {"id":"COM", "st":{"sig":78, "ok":1}},
  {"id":"PWR", "st":{"bat":92, "ok":1}},
  {"id":"THR", "st":{"lvl":63, "ok":0}}
]
```

**The Incorporator Way:**
Feed it the unpredictable JSON. Incorporator dynamically unifies the changing structures into a single object graph and gives you instant dot-notation access.

```python
import asyncio
from incorporator import Incorporator

class System(Incorporator): pass     # Subclass; everything else hangs off it.

async def main():
    # 1. Parse unpredictable JSON directly into Python objects. No models defined!
    systems = await System.incorp(
        inc_file="telemetry.json",
        inc_code="id" # Sets 'id' as the O(1) Memory Registry lookup key
    )

    # 2. Instantly access the unified Python object graph via dot-notation
    print(f"Navigation Position: {systems.inc_dict['NAV'].st.pos}")   # Output: [12, 44]
    print(f"Power Battery Level: {systems.inc_dict['PWR'].st.bat}%")  # Output: 92%

    # 3. Interpret and manipulate data effortlessly at runtime
    thr = systems.inc_dict["THR"]
    if not thr.st.ok:
        print(f"⚠️ THRUST FAILURE! Efficiency dropped to {thr.st.lvl}")

asyncio.run(main())
```

### 🤷‍♂️ Wait, what if my data isn't JSON?
It doesn't matter. Incorporator automatically infers the format from the URL or file extension. The syntax **never changes**.

Out of the box: **JSON, NDJSON, CSV, TSV, PSV, XML, SQLite, and HTML** (HTML is parse-only). Opt-in extras unlock **Apache Parquet, Feather (Arrow IPC), ORC, Apache Avro, and Excel (XLSX)** — same `incorp()` / `export()` surface, no syntax changes.

If that exact same telemetry data comes from a legacy system as XML or CSV:
```python
# The syntax doesn't change for XML...
systems_xml = await System.incorp(inc_file="telemetry.xml", inc_code="id")
print(systems_xml.inc_dict["NAV"].st.pos) # Output:['12', '44']

# ...and it works instantly for CSV, TSV, or streaming NDJSON logs!
systems_csv = await System.incorp(inc_file="telemetry.csv", inc_code="id")
```

---

## 📦 Installation

Built on Pydantic V2 metaprogramming, HTTPX, and Tenacity. No system dependencies.

```bash
pip install incorporator
```
*Core dependencies: `pydantic (>=2.0)`, `httpx`, `tenacity`.*

Opt in to format and performance extras as you need them:
```bash
pip install incorporator[speedups]    # orjson + lxml + cramjam (GIL-releasing parsers, Rust compression)
pip install incorporator[parquet]     # pyarrow — unlocks Parquet, Feather, and ORC
pip install incorporator[avro]        # fastavro — Apache Avro binary streams
pip install incorporator[xlsx]        # openpyxl — Excel (.xlsx) read/write
pip install incorporator[orchestrate] # typer + prefect — CLI + Prefect task wrappers
pip install incorporator[all]         # everything except [parquet] (pyarrow is ~30 MB — opt in explicitly)
```

---

## 🧰 The Verbs

Every method you'll call on an `Incorporator` subclass, in order of increasing power.

### `incorp()` — fetch, parse, build the object graph

```python
class Launch(Incorporator): pass

launches = await Launch.incorp(inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/")
print(launches[0].name)
```
→ [Tutorial: Space Devs walkthrough](./docs/1_quick_setup.md)

### `test()` — let the framework write your `incorp()` kwargs for you

```python
await Launch.test(inc_url="https://api.unknown.com/v1/users")
# Prints payload tree + suggested inc_code, rec_path, conv_dict.
```

### `refresh()` — re-fetch live data into existing instances

```python
await Launch.refresh(instance=launches)
```

### `export()` — serialise to any format

CSV, JSON, NDJSON, XML, SQLite, Parquet, Feather, ORC, Avro, XLSX. All share the same call.
```python
await Launch.export(instance=launches, file_path="launches.parquet")
```
→ [Formats & compression cheat sheet](./docs/formats_and_compression.md)

### `stream()` — a long-running data pipeline

Periodic fetch + optional stateful refresh + optional periodic export, running as a daemon. The kwargs **are** the pipeline definition. A `Wave` per chunk is the built-in observability stream — a DX bonus, not the purpose.

```python
async for wave in Launch.stream(
    incorp_params={"inc_url": "https://ll.thespacedevs.com/2.2.0/launch/upcoming/"},
    refresh_interval=60,                              # re-fetch every 60s
    export_params={"file_path": "launches.parquet"},
    export_interval=300,                              # flush to disk every 5 min
):
    if wave.failed_sources: print(wave)               # observability bonus
```
→ [Streaming & pagination guide](./docs/streaming_and_pagination.md)

### `fjord()` — a multi-source data pipeline

Fans out across N concurrent sources, fuses them through a user-defined `outflow(state)` function, exports the combined output.

```python
async for wave in Pipeline.fjord(
    stream_params=[
        {"cls": Coin,  "incorp_params": {"inc_url": "..."}, "refresh_interval": 30},
        {"cls": Order, "incorp_params": {"inc_url": "..."}, "refresh_interval": 5},
    ],
    outflow="outflow.py",                             # defines outflow(state) -> list[dict]
    export_params={"file_path": "fusion.parquet"},
):
    if wave.failed_sources: print(wave)
```
→ [Multi-graph mapping guide](./docs/3_graph_mapping.md)

### `display()` — REPL debug print

```python
launches[0].display()   # <Launch id="..." name="...">
```

`stream()` and `fjord()` are the production verbs — and they're what the CLI runs against a `pipeline.json`.

---

## 🚀 From Code to Production — CLI & Docker

The CLI runs the same `stream()` / `fjord()` engines from a `pipeline.json`. No Python required for single- or multi-source ETLs.

| Command | What it does |
|---------|--------------|
| `incorporator init --type stream` | Scaffold a starter `pipeline.json` (use `--type fjord` for multi-source + `outflow.py`). |
| `incorporator validate pipeline.json` | Structural check before you ship — no network calls. |
| `incorporator stream pipeline.json` | Run a stream pipeline. |
| `incorporator fjord pipeline.json` | Run a multi-source fjord pipeline. |

```bash
incorporator init --type stream --output-dir .
# Edit pipeline.json (inc_url, headers, export_params, ...)
incorporator validate pipeline.json
incorporator stream pipeline.json                # one-shot
# ...or run it as a Dockerised daemon:
cp .env.example .env && mkdir -p config data logs && mv pipeline.json config/
docker compose up -d && docker compose logs -f
```

Secrets stay out of `pipeline.json` — use `${API_KEY}` for env vars or `${file:/run/secrets/api_key}` for Docker / Kubernetes Secrets mounts.

→ [CLI reference](./docs/cli_and_configuration.md) · [Deployment & secrets guide](./docs/deployment.md)

---

## 🛠 Resilience & Batteries Included

* **GIL-free hyperthreading** via the `[speedups]` extra (orjson, lxml). → [Installation](./docs/installation.md)
* **Invisible decompression** for `.gz`, `.bz2`, `.lzma`, `.zip`, `.tar` payloads — automatic, no extra calls. → [Formats](./docs/formats_and_compression.md)
* **Connection pooling + retries + DLQ** — HTTP/2-multiplexed `httpx.AsyncClient`, Tenacity exponential backoff, failed URLs surfaced via `wave.failed_sources`. → [Library reference](./docs/library_reference.md)
* **Zero-OOM `IncorporatorList`** backed by a `WeakValueDictionary` for O(1) lookups without GC pressure. → [Streaming](./docs/streaming_and_pagination.md)
* **Non-blocking observability** — subclass `LoggedIncorporator`; logs flow through a `QueueHandler` so disk I/O never blocks the event loop. → [Library reference](./docs/library_reference.md)
* **Cross-format round-tripping** — JSON ↔ Parquet ↔ SQLite ↔ Avro ↔ CSV ↔ XML, all share the same `export()` surface. → [Data lake pivot](./docs/5_data_lake_pivot.md)

---

## 📚 Documentation & Examples

The best way to learn Incorporator is through guided tutorials and the auto-generated library reference. Runnable code lives under [`/examples`](./examples).

### Guided Tutorials — `incorp()` Patterns
* [🚀 **Space Devs Tutorial**](./docs/1_quick_setup.md) — Pagination, simple ETL, and type casting.
* [⚡️ **Pokédex Power Rankings**](./docs/2_advanced_etl_calc.md) — Deep enrichment (HATEOAS) and array reductions.
* [📊 **Stablecoin Dashboard**](./docs/3_graph_mapping.md) — Multi-API graph fusion and relational data binding.
* [🕵️‍♂️ **Shady Jimmy's Ledger**](./docs/4_xml_post_auditing.md) — XML ingestion, O(1) memory audits, and declarative bulk POSTs.
* [🐘 **Data Lake Pivot**](./docs/5_data_lake_pivot.md) — Bridging JSON to Avro & SQLite.

### Production-Verb Walkthroughs
* [🌊 **Streaming Daemon**](./docs/6_streaming_daemon.md) — `stream()` end-to-end: refresh cadences, export intervals, signal-aware shutdown.
* [🌊 **Multi-Source Fjord**](./docs/7_multi_source_fjord.md) — `fjord()` end-to-end: fusing two feeds through a user `outflow(state)` function.
* [🕵️‍♂️ **DX Inspector**](./docs/8_dx_inspector.md) — `test()` end-to-end: let the framework write your `incorp()` kwargs.

### Reference
* [📖 **Library Reference** (pdoc)](./docs/library_reference.md) — every public class, method, converter, and paginator, rendered from the source docstrings.
* [⚡ **Performance Characteristics**](./docs/performance.md) — measured throughput per format (both directions) + the automatic engine optimisations.

---

## 🤝 Philosophy & Contributing
Incorporator is built on strict OOP principles, non-blocking observability, and a forgiving metaprogramming shield. We trap standard library exceptions (`JSONDecodeError`, `httpx.HTTPStatusError`) and gracefully recast them as domain errors. Your event loop is safe with us.

Contributions: see [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the dev install, quality bar, and architecture conventions. Security disclosures: see [`SECURITY.md`](./SECURITY.md). Release notes: [`CHANGELOG.md`](./CHANGELOG.md).
