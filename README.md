***

# 🚀 Incorporator

**Schema-free ingestion for APIs you don't control — and an orchestrator that tells you how to tune it.**

Both halves share the same primitives — Penstock throttling at the HTTP and edge layers, Wave / Tide / RejectEntry outcome records, FlowControl — so the parser and the orchestrator compose, but you can adopt either without the other. Local paginators (`SQLitePaginator`, `CSVPaginator`, `AvroPaginator`) accept the same `penstock=` kwarg as the HTTP layer — one rate-limit primitive across both.

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
* **Works with unpredictable JSON APIs** — digests XML, CSV, NDJSON, SQLite, Parquet, Avro without a line of schema; missing keys and mutating types absorbed without validation errors.
* **Joins that don't depend on fetch order** — `link_to()` / `link_to_list()` re-read the target's registry on every lookup instead of snapshotting it once, so a join built before its target populates starts resolving the moment it does; `calc()` / `calc_all()` skip coercion outright on `None`, killing spurious per-row warnings. *Both ship in the next release — current PyPI is v1.4.0.*
* **The pipeline tells you what to tune** — after a Tideweaver run, `architect.tune()` consumes the accumulated rejects, tides, and waves and emits a `TuningReport` of severity-sorted hints.
* **Disk-backed observability for orchestration** — `LoggedTideweaver` routes every `Tide` and `RejectEntry` through a `QueueHandler` pipeline, replayable with `get_tides()` / `get_rejects()`.

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

Built on Pydantic V2 metaprogramming, HTTPX, and Tenacity. No system dependencies. Requires Python 3.10+. CI runs against 3.10 / 3.11 / 3.13 on Ubuntu and Windows.

```bash
pip install incorporator                  # core
pip install incorporator[speedups]        # orjson + lxml + cramjam
pip install incorporator[parquet]         # pyarrow — Parquet, Feather, ORC
pip install incorporator[avro]            # fastavro
pip install incorporator[xlsx]            # openpyxl — XLSX read/write
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
    refresh_params=None,                                 # chunking: opt out of per-chunk refresh
    export_params={"file_path": "launches.ndjson", "if_exists": "append"},
):
    if wave.failed_sources: print(wave)
```

Use an append-friendly format (`.ndjson` / `.csv` / `.sqlite` / `.avro`) — Parquet, Feather, ORC, and Excel rebuild the whole file every wave. For multi-source live registries reach for `fjord()`. Pass `adapt_chunk_size=True` to resize `paginator.chunk_size` via AIMD — bounded by `chunk_size_min` / `chunk_size_max` and a target latency window of `target_min_sec` / `target_max_sec`.

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
    outflow="outflow.py",
)
async for tide in Tideweaver(watershed).run():
    print(tide.tide_number, tide.fired, tide.skipped)
```
Four shape helpers (`parallel`, `chain`, `fanout`, `diamond`) plus `custom` with explicit `edges`. Declarative `watershed.json` config + `incorporator tideweaver run / validate` CLI mirror the `stream` / `fjord` workflow. After the run, `Tideweaver.summary(tides=tides)` returns a `TuningReport` (see Resilience).
**Probe → plan → run, no disk round-trip.** When you have N unknown endpoints, `architect()` profiles each one and emits a runnable plan you can tune in-memory before handing it to `Tideweaver`:

```python
plan = await Coin.architect(
    sources={"binance": book_url, "coinbase": cb_url, "kraken": kr_url},
    output="plan",
)
plan.currents[0].interval_hint = 10                  # tune
watershed = plan.to_watershed(window=(start, end))   # materialise
async for tide in Tideweaver(watershed).run():
    ...
```

`architect(output=...)` also emits `"report"` (pretty-printed), `"python"` (paste-ready module), or `"json"` (paste-ready `watershed.json`). After a run, `architect.tune()` consumes the accumulated rejects + tides + waves and emits a `TuningReport` of structured recommendations — see Resilience below.
**Per-edge flow control.** Each edge composes six orthogonal primitives — `Gate` (HardLock / SoftPass / Weir), `SurgeBarrier`, `Penstock` (Sustained / Burst / Window / Backpressure / Signal), `Reservoir`, `Spillway` (DropOldest / RaiseOverflow / ExportToArchive), and a declarative `FlowObserver` (Null / Logging / Signal) for telemetry. The shape constructors accept a top-level `flow=` or `gate_mode=` shorthand; explicit `Edge(...)` carries its own:

```python
from incorporator.tideweaver import (
    FlowControl, Weir, BurstPenstock, Reservoir, LoggingObserver,
)

edge_flow = FlowControl(
    gate=Weir(),                                          # fire on freshness
    penstock=BurstPenstock(rate_per_sec=2.0, burst=5),    # token bucket
    reservoir=Reservoir(depth=3),                         # buffer 3 waves
    observer=LoggingObserver(fire_level="info"),          # declarative telemetry
)
```

The same shapes deserialize from `watershed.json` via Pydantic discriminated unions (`{"gate": {"type": "weir"}, ...}`) — see [Appendix — NASCAR Tideweaver](./examples/appendix/nascar-tideweaver/README.md) for the JSON form on a working diamond. For non-verb tick logic (cron-style cleanups, custom side-effects), subclass `CustomCurrent` and override `async tick(scheduler)`.

`Stream(parent_current=...)` declares a parent-child row fan without a `CustomCurrent` wrapper — one child row per parent snapshot entry at tick time:

```python
child = Stream(
    name="coin_detail",
    cls=CoinDetail,
    interval=60,
    parent_current="markets",          # row-fans from markets._tideweaver_snapshot
    incorp_params={"inc_code": "id"},  # inc_parent injected per row at tick time
)
```
→ [Tutorial 11 — Tideweaver](./examples/11-tideweaver/README.md) · [Canal toolkit primitives in API Atlas](./docs/api_atlas.md#canal-toolkit-primitives)
### When to reach for which long-running verb

| Verb | Sources | Shape | Reach for it when… |
|---|---|---|---|
| `stream()` | one | paginated chunks, O(1) memory | bulk drain a paginated API or massive local file into a warehouse / archive |
| `fjord()` | many | stateful in-memory registry, live refresh | keep a hot multi-source object graph synchronised and snapshot it on a cadence |
| `Tideweaver` | many | windowed graph of streams + fjords with dependency edges | run several feeds at independent intervals inside a single time window, with downstream work gated on fresh upstream data |

### `display()` — REPL debug print: `launches[0].display()`

### Typed directive wrappers (optional)
`excl_lst`, `name_chg`, `code_attr`, `name_attr` accept typed frozen wrappers alongside bare shapes. Old call sites keep working; mixed sequences are accepted.

```python
from incorporator.schema.directives import Ex, Nm
await Users.incorp(
    inc_url="https://api.example.com/users",
    excl_lst=["legacy_flag", Ex("profile.internal.ssn")],   # nested drop
    name_chg=[("external_id", "id"), Nm("user_name", "name")],
    code_attr="id",
)
```

`Ex("a.b.c")` drops nested leaves (previously only top-level keys could
be dropped). PK binding now runs after rename, so `inc_code` resolves
correctly whether `name_chg` removes the source field or creates it.

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
| `incorporator deps [--missing] [--category CAT] [--json]` | List installed optional extras and what each one unlocks; `--json` for CI. |

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
* **Connection pooling + phase-aware retries + structured rejects** — HTTP/2-multiplexed `httpx.AsyncClient`, phase-aware retry classification (connect-phase errors capped at ~3 attempts; server-responded 5xx/429 up to 8 attempts honoring `Retry-After`; non-idempotent POST is not retried; HTTP 408 and 425 are retryable), and `IncorporatorList.rejects: list[RejectEntry]` carrying `source` / `error_kind` / `is_url_traffic_error` / `message` / `retry_after` / `wave_index` for every failed source. `RejectEntry.__str__` includes the HTTP reason phrase (`[HTTP 429 Too Many Requests]`; `[HTTP 522]` for non-standard codes). The legacy flat `failed_sources: list[str]` is preserved as a derived view. Opt-in `block_internal_redirects=True` rejects 3xx Locations to RFC1918 / loopback / cloud-metadata IPs.
* **Friendly rate limiting** — the framework ships with **no implicit per-host throttling**.  Opt in once at startup with `register_host_penstock` (one source of truth across every `incorp()` call) or pass `requests_per_second=X` per call.  The same `Penstock` primitive serves both the HTTP layer and Tideweaver edges:

  ```python
  from incorporator import register_host_penstock
  register_host_penstock("api.coingecko.com", rate_per_sec=0.2)   # ~12 r/min
  register_host_penstock("pokeapi.co",        rate_per_sec=1.5)   # ~90 r/min
  ```

  `architect()` surfaces 429 / `Retry-After` hints during probing and recommends a `Penstock` on the matching edge.  See [`register_host_penstock` in the API Atlas](./docs/api_atlas.md#register_host_penstock).
* **Lambda-free `conv_dict`** — `inc`, `calc`, `calc_all`, `pluck`, `link_to`, `link_to_list`, `split_and_get` all short-circuit silently on garbage input (`None`, `""`, `"N/A"`, `"null"`, `"unknown"`, `"nan"`, `"undefined"`) before the user callable runs.  Defensive null guards inside lambdas are no longer needed; use stdlib callables directly:

  ```python
  conv_dict = {
      "id":     inc(int),                                              # type coerce
      "title":  calc(str.lower, "title", default="", target_type=str), # transform
      "status": calc("Alive".__eq__, "status", default=False),         # enum-to-bool
      "region": pluck("meta.region.name"),                             # lift nested
  }
  ```

  See `incorporator.io.SourceRef` for the opt-in typed source value (URL / file / parent / payload / kwargs) when you need explicit source dispatch.
* **Atomic writes + spreadsheet-injection guard** — Parquet / Feather / ORC / JSON / XML / XLSX build via tempfile + `os.replace()` (no half-written files); CSV / XLSX cells starting with `=` / `@` / `+` / `-` are quoted on export (OWASP).
* **Non-blocking observability with a routing split** — subclass `LoggedIncorporator`; logs flow through a `QueueHandler` so disk I/O never blocks the event loop. URL/internet-traffic errors (HTTP 4xx/5xx, network failures) route to `_api.log`; parse and codebase errors route to `_error.log`; `_debug.log` is the superset of both. The file location tells you whether the fault is the API's or your code's. `get_rejects()` unions both files so every reject is covered regardless of routing. For orchestration runs, `LoggedTideweaver` (from `incorporator.tideweaver`) is the parallel drop-in for `Tideweaver` — routes every yielded `Tide` and every accumulated `RejectEntry` to disk via the same `QueueHandler` pipeline; replay with `get_tides()` / `get_rejects()` / `get_scheduler_events()`.
* **The pipeline tells you what to tune.** `architect.tune()` reads accumulated rejects, tides, and pass interval and returns a `TuningReport` of severity-sorted hints — per-edge `Penstock` rates, byte-rate-aware penstock recommendations, evidence-based timeout hints via `tune(timeout=...)`, surge thresholds. `tw.summary(tides=tides)` returns the same report from an existing instance.

  ```python
  from incorporator.tideweaver import LoggedTideweaver, tune
  tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="PriceSession")
  tides = [tide async for tide in tw.run()]
  report = tune(rejects=tw.rejects, tides=tides, pass_interval=tw.pass_interval)
  print(report.render())   # hint blocks, sorted by severity
  ```
* **Keyed reject audit + backlog short-circuit.** `Tideweaver.rejects` returns a `list[RejectEntry]` whose `error_kind` is one of `"PenstockLimited"`, `"SurgeHalted"`, `"SkipAhead"`, `"GateBlocked"` — every canal-layer skip that never reached a tick body lands here, with `from_name` / `to_name` / `cooldown_sec` populated for keyed analysis. Set `backlog_backoff_factor=2.0` on the `Tideweaver` constructor to extend the next-pass wait when the scheduler is consistently saturated; the default `1.0` is disabled.
* **Optional-dependency introspection** — `list_deps()` / `install_hint()` / `Category` / `DepInfo` Python API; CLI surface via `incorporator deps` (see CLI table). → [`docs/cli_and_configuration.md`](./docs/cli_and_configuration.md)
* **Cross-format round-tripping** — JSON ↔ Parquet ↔ SQLite ↔ Avro ↔ CSV ↔ XML. → [Tutorial 3](./examples/03-universal-formats/README.md)

---

## 📚 Tutorials (in order)

The eleven-tutorial curriculum — rewritten this cycle to one converged style (each class defined exactly once, no incidental `getattr`) and revalidated against live API output along the way.  Each slot introduces one new verb or technique, alternating CoinGecko-heavy steps with non-CG domain examples so per-minute rate-limit windows refresh between CG calls and each Incorporator pattern lands across multiple real-world verticals.  Runnable code under [`/examples`](./examples).

1. [🌱 **First Steps + DX Inspector**](./examples/01-first-steps/README.md) — discovery-first flow: `test()` profiles a CoinGecko endpoint, then `incorp()` applies its recommendations.
2. [🔁 **Data Lake Pivot**](./examples/02-data-lake-pivot/README.md) — SaaS roster → BI-ready columnar; pivot a `/users` endpoint into Avro + SQLite.
3. [📦 **Snapshot Warehouse — Universal Formats**](./examples/03-universal-formats/README.md) — fan CoinGecko top-100 snapshots into NDJSON / CSV / SQLite / Parquet, then round-trip every artifact.
4. [🛡️ **XML Post Audit**](./examples/04-xml-post-audit/README.md) — federal-VIN fraud audit: XML invoice ledger enriched via one batched POST.
5. [🚀 **Parent → Child Drilling**](./examples/05-parent-child-drilling/README.md) — CoinGecko `/coins/markets` → `/coins/{id}` fan-out — the canonical backtest-data-prep pattern.
6. [🗺️ **State Sports**](./examples/06-state-sports/README.md) — state/province → teams → rosters across NFL/NBA/MLB/NHL: a live CountriesNow reference-data fetch, two chained per-parent `inc_parent` drills (league → team, team → roster), and a `conv_dict` showcasing `inc`/`calc`/`pluck` — a pure one-shot script, no Watershed.
7. [🔄 **Stateful Refresh**](./examples/07-stateful-refresh/README.md) — `refresh()` three ways against Binance's live ticker.
8. [🌊 **Streaming Daemon — Paginated Bulk Export at O(1) Memory**](./examples/08-streaming-daemon/README.md) — `stream()`'s canonical job: chunking-mode drain of a paginated source. Plus a single-source `stateful_polling=True` compatibility shim with a pointer to T10 for the multi-source live-daemon path.
9. [🏁 **NASCAR Fantasy Fjord**](./examples/09-nascar-fantasy-fjord/README.md) — fantasy-sports scoring fjord across Cup, Xfinity, Truck series; previews T10's abstraction.
10. [🌊 **Multi-Source Fjord**](./examples/10-multi-source-fjord/README.md) — `fjord()` fusing CoinGecko + Binance into a live cross-venue spread metric.
11. [🧵 **Tideweaver — Multi-Exchange Arb Scanner** *(capstone)*](./examples/11-tideweaver/README.md) — declarative windowed orchestration: three exchanges → one best-market record.

## 📑 Reference

* [📖 **Library Reference**](./docs/library_reference.md) — every public class, rendered from source docstrings.
* [📑 **API Atlas**](./docs/api_atlas.md) — paste-ready map of every public callable: signature, pseudocode, "when to reach for it", common kwargs, tutorial cross-links.
* [🩺 **Production Debugging**](./docs/debugging.md) — `LoggedIncorporator` + the api/error routing split + reader API (`get_rejects`, `get_api`, `get_error`, `get_current`, `get_scheduler_events`) + retry loop via `RejectEntry`.
* [📦 **Formats & Compression**](./docs/formats_and_compression.md) + [🌊 **Streaming & Pagination**](./docs/streaming_and_pagination.md) — every format kwarg, compression rules, and the paginator family for endpoints / files too big for RAM.
* [🐳 **CLI & Configuration**](./docs/cli_and_configuration.md) — running pipelines from `pipeline.json` / `watershed.json`.
* [⚡ **Performance**](./docs/performance.md) — measured throughput per format, memory profile, tuning knobs. Per-chunk validation uses `TypeAdapter(list[Cls]).validate_python(rows)` and is 1.3-2.0× faster than per-row `model_validate` (`tests/benchmarks/test_validate_batch_vs_per_row.py`). Trade-off: within a single `incorp()` call, peak memory is O(N); `stream()` still keeps RSS flat by releasing each chunk. Outcome records — `Wave`, `Tide`, `RejectEntry`, and slotted dataclass `CurrentOutcome` (per-current outcomes inside `Tide`) — carry structured fields (HTTP retry counts, schema-cache hits, source URLs, per-edge identity, status codes, cooldown hints) for keyed audit.

## 📎 Appendices — optional side-quests

* [🧬 **Pokémon ETL**](./examples/appendix/pokeapi-etl/README.md) — paginated HATEOAS drill + array reductions with `calc` / `sum_attributes`.  Mirrors T5.
* [🕸️ **Crypto Graph Mapping**](./examples/appendix/crypto-graph-mapping/README.md) — `link_to`-based live in-memory join; T10's fjord pattern as a one-shot.
* [🏁 **NASCAR Tideweaver**](./examples/appendix/nascar-tideweaver/README.md) — T11's diamond shape against race telemetry (laps + pits + flags → driver state).
* [⚾ **MLB Pulse**](./examples/appendix/mlb-pulse/README.md) — four MLB Stats API endpoints fused inside a Tideweaver window; mirrors T11's diamond shape and T5's drill pattern.
* [🧵 **Tideweaver Deep Dives**](./examples/appendix/tideweaver-parquet-snapshots/README.md) — [Parquet at window close](./examples/appendix/tideweaver-parquet-snapshots/README.md) and [Tideweaver vs. Prefect](./examples/appendix/tideweaver-vs-prefect/README.md) — columnar artifacts plus the in-process-vs-cloud orchestration decision.

---

## 🤝 Philosophy & Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the dev install, quality bar, and architecture conventions. Security disclosures: [`SECURITY.md`](./SECURITY.md). Release notes: [`CHANGELOG.md`](./CHANGELOG.md).

---

**Have a suggestion or hitting a snag?** [Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/README.md) · [Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) · [Browse open issues](https://github.com/PyPlumber/incorporator/issues)
