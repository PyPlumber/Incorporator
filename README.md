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
* **Works with unpredictable JSON APIs** — and digests XML, CSV, NDJSON, SQLite, Parquet without a line of schema. Native Python objects instantly, no manual model definitions.
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
→ [Tutorial 1 — First Steps](./examples/01-first-steps/README.md)

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

### `stream()` — paginated bulk export, O(1) memory

For paginated APIs or local files too big for RAM, `stream()` fetches one page at a time, exports it, releases the page, and moves on — peak memory stays at roughly one chunk regardless of dataset size. A `Wave` per chunk is the built-in observability stream.

```python
from incorporator.io.pagination import PageNumberPaginator

async for wave in Launch.stream(
    incorp_params={
        "inc_url": "https://api.example.com/launches",
        "inc_page": PageNumberPaginator(page_param="page"),
    },
    export_params={"file_path": "launches.parquet"},
):
    if wave.failed_sources: print(wave)
```

For live dashboards keeping a registry hot across many sources, reach for `fjord()` instead.

→ [Streaming & pagination](./docs/streaming_and_pagination.md) · [Tutorial 8](./examples/08-streaming-daemon/README.md)

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
→ [Tutorial 10 — Multi-Source Fjord](./examples/10-multi-source-fjord/README.md)

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

→ [Tutorial 11 — Tideweaver](./examples/11-tideweaver/README.md)

### When to reach for which long-running verb

| Verb | Sources | Shape | Reach for it when… |
|---|---|---|---|
| `stream()` | one | paginated chunks, O(1) memory | bulk drain a paginated API or massive local file into a warehouse / archive |
| `fjord()` | many | stateful in-memory registry, live refresh | keep a hot multi-source object graph synchronised and snapshot it on a cadence |
| `Tideweaver` | many | windowed graph of streams + fjords with dependency edges | run several feeds at independent intervals inside a single time window, with downstream work gated on fresh upstream data |

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
* **Atomic writes + spreadsheet-injection guard** — Parquet / Feather / ORC / JSON / XML / XLSX build via tempfile + `os.replace()` (no half-written files); CSV / XLSX cells starting with `=` / `@` / `+` / `-` are quoted on export (OWASP).
* **Non-blocking observability** — subclass `LoggedIncorporator`; logs flow through a `QueueHandler` so disk I/O never blocks the event loop.
* **Cross-format round-tripping** — JSON ↔ Parquet ↔ SQLite ↔ Avro ↔ CSV ↔ XML. → [Tutorial 3](./examples/03-universal-formats/README.md)

---

## 📚 Tutorials (in order)

The eleven-tutorial curriculum.  Each slot introduces one new verb or technique, alternating CoinGecko-heavy steps with non-CG domain examples so per-minute rate-limit windows refresh between CG calls and each Incorporator pattern lands across multiple real-world verticals.  Runnable code under [`/examples`](./examples).

1. [🌱 **First Steps + DX Inspector**](./examples/01-first-steps/README.md) — discovery-first flow: `test()` profiles a CoinGecko endpoint, then `incorp()` applies its recommendations.
2. [🔁 **Data Lake Pivot**](./examples/02-data-lake-pivot/README.md) — SaaS roster → BI-ready columnar; pivot a `/users` endpoint into Avro + SQLite.
3. [📦 **Snapshot Warehouse — Universal Formats**](./examples/03-universal-formats/README.md) — fan CoinGecko top-100 snapshots into NDJSON / CSV / SQLite / Parquet, then round-trip every artifact.
4. [🛡️ **XML Post Audit**](./examples/04-xml-post-audit/README.md) — federal-VIN fraud audit: XML invoice ledger enriched via one batched POST.
5. [🚀 **Parent → Child Drilling**](./examples/05-parent-child-drilling/README.md) — CoinGecko `/coins/markets` → `/coins/{id}` fan-out — the canonical backtest-data-prep pattern.
6. [🚀 **SpaceX Launches**](./examples/06-spacex-launches/README.md) — ops-dashboard feed: upcoming launches drilled for rocket + launchpad detail.
7. [🔄 **Stateful Refresh**](./examples/07-stateful-refresh/README.md) — `refresh()` three ways against Binance's live ticker.
8. [🌊 **Streaming Daemons — Both Polling Modes**](./examples/08-streaming-daemon/README.md) — stateful for live dashboards; chunking for paginated bulk drains.
9. [🏁 **NASCAR Fantasy Fjord**](./examples/09-nascar-fantasy-fjord/README.md) — fantasy-sports scoring fjord across Cup, Xfinity, Truck series; previews T10's abstraction.
10. [🌊 **Multi-Source Fjord**](./examples/10-multi-source-fjord/README.md) — `fjord()` fusing CoinGecko + Binance into a live cross-venue spread metric.
11. [🧵 **Tideweaver — Multi-Exchange Arb Scanner** *(capstone)*](./examples/11-tideweaver/README.md) — declarative windowed orchestration: three exchanges → one best-market record.

## 📑 Reference

* [📖 **Library Reference**](./docs/library_reference.md) — every public class, rendered from source docstrings.
* [📑 **API Atlas**](./docs/api_atlas.md) — paste-ready map of every public callable: signature, pseudocode, "when to reach for it", common kwargs, tutorial cross-links.
* [🩺 **Production Debugging with `get_error()`**](./docs/debugging.md) — `LoggedIncorporator` + structured error logs + DLQ retry.
* [📦 **Formats & Compression**](./docs/formats_and_compression.md) + [🌊 **Streaming & Pagination**](./docs/streaming_and_pagination.md) — every format kwarg, compression rules, and the paginator family for endpoints / files too big for RAM.
* [🐳 **CLI & Configuration**](./docs/cli_and_configuration.md) — running pipelines from `pipeline.json` / `watershed.json`.
* [⚡ **Performance**](./docs/performance.md) — measured throughput per format, memory profile, tuning knobs.

## 📎 Appendices — optional side-quests

* [🧬 **Pokémon ETL**](./examples/appendix/pokeapi-etl/README.md) — paginated HATEOAS drill + array reductions with `calc` / `sum_attributes`.  Mirrors T5.
* [🕸️ **Crypto Graph Mapping** (static)](./examples/appendix/crypto-graph-mapping/README.md) — `link_to`-based in-memory join; T10's fjord pattern as a one-shot.
* [🏁 **NASCAR Tideweaver**](./examples/appendix/nascar-tideweaver/README.md) — T11's diamond shape against race telemetry (laps + pits + flags → driver state).
* [🧵 **Tideweaver Deep Dives**](./examples/appendix/tideweaver-parquet-snapshots/README.md) — [Parquet at window close](./examples/appendix/tideweaver-parquet-snapshots/README.md) and [Tideweaver vs. Prefect](./examples/appendix/tideweaver-vs-prefect/README.md) — columnar artifacts plus the in-process-vs-cloud orchestration decision.

---

## 🤝 Philosophy & Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the dev install, quality bar, and architecture conventions. Security disclosures: [`SECURITY.md`](./SECURITY.md). Release notes: [`CHANGELOG.md`](./CHANGELOG.md).

---

**Have a suggestion or hitting a snag?** [Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/README.md) · [Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) · [Browse open issues](https://github.com/PyPlumber/incorporator/issues)
