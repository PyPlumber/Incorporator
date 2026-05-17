***

# 🚀 Incorporator

**A schema-free data mapper that turns JSON, XML, or CSV into a unified Python object graph with dot-notation and access-at-runtime — plus an in-process orchestrator (Tideweaver) for multi-source pipelines.**

<!-- DISTRIBUTION -->
[![PyPI version](https://img.shields.io/pypi/v/incorporator?color=blue)](https://pypi.org/project/incorporator/)
[![Python Versions](https://img.shields.io/pypi/pyversions/incorporator.svg)](https://pypi.org/project/incorporator/)
[![Downloads](https://img.shields.io/pypi/dm/incorporator?color=blue)](https://pypi.org/project/incorporator/)

<!-- CODE QUALITY -->
[![CI](https://github.com/PyPlumber/incorporator/actions/workflows/ci.yml/badge.svg)](https://github.com/PyPlumber/incorporator/actions/workflows/ci.yml)
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
* **Works with unpredictable JSON APIs** — and digests XML, CSV, NDJSON, SQLite, Parquet without a line of schema.
* **Native Python objects instantly**, no manual model definitions.
* **Handles changing structures at runtime** — missing keys and mutating types absorbed without validation errors.
* **Pydantic + HTTPX under the hood** — no data classes, connection poolers, or pagination loops to write.

---

## 🛠️ How it Works: Zero-Schema Ingestion

Imagine this telemetry JSON. The nested `"st"` dictionary **changes structure** for every subsystem (`pos` vs `sig` vs `bat`). Standard parsers would crash.

```json
[
  {"id":"NAV", "st":{"pos":[12,44], "ok":1}},
  {"id":"COM", "st":{"sig":78, "ok":1}},
  {"id":"PWR", "st":{"bat":92, "ok":1}},
  {"id":"THR", "st":{"lvl":63, "ok":0}}
]
```

Feed it the unpredictable JSON. Incorporator unifies the changing structures into a single object graph:

```python
import asyncio
from incorporator import Incorporator

class System(Incorporator): pass     # Subclass; everything else hangs off it.

async def main():
    systems = await System.incorp(inc_file="telemetry.json", inc_code="id")
    print(systems.inc_dict["NAV"].st.pos)   # [12, 44]
    print(systems.inc_dict["PWR"].st.bat)   # 92

    thr = systems.inc_dict["THR"]
    if not thr.st.ok:
        print(f"⚠️ THRUST FAILURE! Efficiency dropped to {thr.st.lvl}")

asyncio.run(main())
```

The format is inferred from the URL or file extension. The syntax **never changes** for XML, CSV, NDJSON, SQLite, Parquet, Feather, ORC, Avro, or XLSX — same `incorp()` / `export()` surface.

---

## 📦 Installation

Built on Pydantic V2 metaprogramming, HTTPX, and Tenacity. No system dependencies.

```bash
pip install incorporator                  # core
pip install incorporator[speedups]        # orjson + lxml + cramjam
pip install incorporator[parquet]         # pyarrow — Parquet, Feather, ORC
pip install incorporator[avro]            # fastavro
pip install incorporator[orchestrate]     # typer + prefect — CLI + Prefect wrappers
pip install incorporator[all]             # everything except [parquet]
```

---

## 🧰 The Verbs + One Orchestrator

Every method you'll call on an `Incorporator` subclass, plus the windowed orchestrator.

### `incorp()` — fetch, parse, build the object graph

```python
class Launch(Incorporator): pass

launches = await Launch.incorp(inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/")
print(launches[0].name)
```
→ [Tutorial 1 — First Steps](./docs/1_first_steps.md)

### `test()` — let the framework write your `incorp()` kwargs

```python
await Launch.test(inc_url="https://api.unknown.com/v1/users")
# Prints payload tree + suggested inc_code, rec_path, conv_dict.
```

### `refresh()` — re-fetch live data into existing instances

```python
await Launch.refresh(instance=launches)
```
The seed call's network context (`params`, `headers`, `rec_path`, `conv_dict`, ...) auto-replays.

### `export()` — serialise to any format

`await Launch.export(instance=launches, file_path="launches.parquet")` — JSON, NDJSON, CSV, XML, SQLite, Parquet, Feather, ORC, Avro, XLSX. → [Formats & compression](./docs/formats_and_compression.md)

### `stream()` — a long-running data pipeline

Periodic fetch + optional refresh + optional periodic export as a daemon. A `Wave` per chunk is the built-in observability stream.

```python
async for wave in Launch.stream(
    incorp_params={"inc_url": "https://ll.thespacedevs.com/2.2.0/launch/upcoming/"},
    refresh_interval=60,
    export_params={"file_path": "launches.parquet"},
    export_interval=300,
):
    if wave.failed_sources: print(wave)
```
→ [Streaming & pagination](./docs/streaming_and_pagination.md)

### `fjord()` — a multi-source data pipeline

Fans out across N concurrent sources, fuses them through a user-defined `outflow(state)`, exports the combined output.

```python
async for wave in Incorporator.fjord(
    stream_params=[
        {"cls": Coin,  "incorp_params": {"inc_url": "..."}, "refresh_interval": 30},
        {"cls": Order, "incorp_params": {"inc_url": "..."}, "refresh_interval": 5},
    ],
    outflow="outflow.py",
    export_params={"file_path": "fusion.parquet"},
):
    if wave.failed_sources: print(wave)
```
→ [Tutorial 6 — Multi-Source Fjord](./docs/6_multi_source_fjord.md)

### `Tideweaver` — orchestrate multiple feeds on independent intervals

When you need several sources at different cadences inside a single time window, with dependency edges gating downstream work until upstreams produce fresh data, build a `Watershed` and hand it to `Tideweaver`:

```python
from incorporator import Tideweaver, Watershed, Stream, Fjord

watershed = Watershed.diamond(
    window=(start, end),
    head=Stream(name="binance", cls=BinanceBook, interval=15, incorp_params={...}),
    middle=[Stream(name="coinbase", cls=CoinbaseTicker, interval=30, incorp_params={...}),
            Stream(name="kraken",   cls=KrakenTicker,   interval=30, incorp_params={...})],
    tail=Fjord(name="best_market", cls=BestMarket, interval=30,
               export_params={"file_path": "arb_signals.ndjson"}),
    outflow="arb_outflow.py",
)
async for tide in Tideweaver(watershed).run():
    print(tide.tide_number, tide.fired, tide.skipped)
```
Four shape helpers (`parallel`, `chain`, `fanout`, `diamond`) plus `custom` with explicit `edges`. Declarative `watershed.json` config + `incorporator tideweaver run / validate` CLI mirror the `stream` / `fjord` workflow.

→ [Tutorial 7 — Tideweaver](./docs/7_tideweaver.md)

### `display()` — REPL debug print: `launches[0].display()`

---

## 🚀 From Code to Production — CLI & Docker

The CLI runs the same engines from declarative config. No Python required.

| Command | What it does |
|---------|--------------|
| `incorporator init --type {stream,fjord,tideweaver}` | Scaffold a starter `pipeline.json` or `watershed.json` (+ `outflow.py` for fjord). |
| `incorporator validate <config>.json` | Structural check before you ship — no network calls. Auto-detects type. |
| `incorporator stream pipeline.json` | Run a single-source stream pipeline. |
| `incorporator fjord pipeline.json` | Run a multi-source fjord pipeline. |
| `incorporator tideweaver run watershed.json` | Run a windowed orchestration graph. |

```bash
incorporator init --type stream --output-dir .
incorporator validate pipeline.json && incorporator stream pipeline.json   # ...or: docker compose up -d
```

Secrets stay out of config — `${API_KEY}` for env vars, `${file:/run/secrets/api_key}` for Docker / Kubernetes Secrets mounts. Set `INCORPORATOR_SECRETS_ROOT=/run/secrets` to sandbox `${file:...}` against directory-traversal.

→ [CLI reference](./docs/cli_and_configuration.md) · [Deployment & secrets](./docs/deployment.md)

---

## 🛠 Resilience & Batteries Included

* **GIL-free hyperthreading** via the `[speedups]` extra. → [Installation](./docs/installation.md)
* **Invisible decompression** for `.gz`, `.bz2`, `.lzma`, `.zip`, `.tar` — ZIP/TAR paths validated against directory-traversal and a 1 GB bomb cap. → [Formats](./docs/formats_and_compression.md)
* **Connection pooling + retries + DLQ** — HTTP/2-multiplexed `httpx.AsyncClient`, Tenacity backoff, failed URLs on `wave.failed_sources`. Opt-in `block_internal_redirects=True` rejects 3xx Locations to RFC1918 / loopback / cloud-metadata IPs.
* **Atomic writes** — Parquet, Feather, ORC, JSON, XML, XLSX build to a tempfile and `os.replace()` on success; crash mid-write never leaves a corrupt-footer file.
* **Spreadsheet-injection guard** — CSV / XLSX cells starting with `=` / `@` / `+` / `-` are quoted on export (OWASP default).
* **Non-blocking observability** — subclass `LoggedIncorporator`; logs flow through a `QueueHandler` so disk I/O never blocks the event loop.
* **Cross-format round-tripping** — JSON ↔ Parquet ↔ SQLite ↔ Avro ↔ CSV ↔ XML. → [Tutorial 2](./docs/2_universal_formats.md)

---

## 📚 Tutorials (in order)

A focused 1–7 curriculum. Each slot introduces one new verb or technique. Runnable code under [`/examples`](./examples).

1. [🌱 **First Steps + DX Inspector**](./docs/1_first_steps.md) — discovery-first flow: `test()` profiles a CoinGecko endpoint, then `incorp()` applies its recommendations.
2. [📦 **Snapshot Warehouse — Universal Formats**](./docs/2_universal_formats.md) — fan CoinGecko top-100 snapshots into NDJSON / CSV / SQLite / Parquet, then round-trip every artifact.
3. [🚀 **Parent → Child Drilling**](./docs/3_parent_child_drilling.md) — CoinGecko `/coins/markets` → `/coins/{id}` fan-out — the canonical backtest-data-prep pattern.
4. [🔄 **Stateful Refresh**](./docs/4_stateful_refresh.md) — `refresh()` three ways against Binance's live ticker.
5. [🌊 **Streaming Daemons — Both Polling Modes**](./docs/5_streaming_daemon.md) — `stateful_polling=True` for mark-to-market dashboards; `stateful_polling=False` for paginated bulk drains.
6. [🌊 **Multi-Source Fjord**](./docs/6_multi_source_fjord.md) — `fjord()` fusing CoinGecko + Binance into a live cross-venue spread metric.
7. [🪡 **Tideweaver — Multi-Exchange Arb Scanner** *(capstone)*](./docs/7_tideweaver.md) — declarative windowed orchestration: three exchanges → one composite best-market record with arb-opportunity flag.

## 📑 Reference

* [📖 **Library Reference**](./docs/library_reference.md) — every public class, rendered from source docstrings.
* [🩺 **Production Debugging with `get_error()`**](./docs/debugging.md) — `LoggedIncorporator` + structured error logs + DLQ retry.
* [📦 **Formats & Compression**](./docs/formats_and_compression.md) — every format kwarg, compression rules.
* [🌊 **Streaming & Pagination**](./docs/streaming_and_pagination.md) — paginator family for endpoints / files too big for RAM.
* [🐳 **CLI & Configuration**](./docs/cli_and_configuration.md) — running pipelines from `pipeline.json` / `watershed.json`.
* [⚡ **Performance**](./docs/performance.md) — measured throughput per format, memory profile, tuning knobs.

## 📎 Appendices — same patterns, different domains

* [🚀 **SpaceX Launches**](./docs/appendix/spacex_launches.md) — T3 + T5 patterns against SpaceX v4 (launches → rockets → launchpads; slow-cadence daemon).
* [🏁 **NASCAR Tideweaver**](./docs/appendix/nascar_tideweaver.md) — Tutorial 7's diamond shape against race telemetry (laps + pits + flags → driver state).
* [🧬 **Pokémon ETL**](./docs/appendix/pokeapi_etl.md) — paginated HATEOAS drill + array reductions with `calc` / `sum_attributes`.
* [🚨 **Shady Jimmy's XML Audit**](./docs/appendix/xml_post_audit.md) — XML ingestion + declarative bulk POST + fraud audit.
* [🕸️ **Crypto Graph Mapping** (static)](./docs/appendix/crypto_graph_mapping.md) — `link_to`-based in-memory join; Tutorial 6's pattern as a one-shot.
* [🏁 **NASCAR Fantasy Fjord** *(advanced)*](./docs/appendix/nascar_fantasy_fjord.md) — six-source fjord with state-aware `inflow(state)` and multi-output `outflow(state)`.
* [🪡 **Parquet Snapshots in a Tideweaver Window**](./docs/appendix/tideweaver_parquet_snapshots.md) — landing columnar artifacts at window close.
* [🪡 **Tideweaver vs. Prefect**](./docs/appendix/tideweaver_vs_prefect.md) — picking between in-process and cloud orchestration, plus the recommended hybrid pattern.
* [🐘 **Data Lake Pivot** (legacy)](./docs/appendix/data_lake_pivot.md) — JSON ↔ Avro/SQLite walkthrough; T2 covers the headline pattern.

---

## 🤝 Philosophy & Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the dev install, quality bar, and architecture conventions. Security disclosures: [`SECURITY.md`](./SECURITY.md). Release notes: [`CHANGELOG.md`](./CHANGELOG.md).
