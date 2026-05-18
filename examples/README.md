# Incorporator Examples

Every tutorial is its own self-contained directory.  The `README.md` inside is the canonical write-up; the `.py` next to it is the runnable code; sidecars / fixtures / `pipeline.json` / `watershed.json` ship in the same folder.

## 🛣️ Recommended speed-run sequence

If you're going through the curriculum in one sitting, alternate the four appendix detours between the CoinGecko-heavy tutorials.  Each detour is a 2–8 s real-world side-quest (no CoinGecko) that gives CoinGecko's per-minute rate-limit window time to refresh between tutorials.

| # | Step | API | Wall-clock | Real-world thread |
|---|---|---|---|---|
| 1 | **T1 — First Steps** | CoinGecko (2) | ~10 s | core: `incorp()` / `test()` / `inc_dict` |
| 2 | 🛣️ [Data Lake Pivot](./appendix/data-lake-pivot/) | jsonplaceholder (1) | ~2 s | **SaaS roster → BI-ready columnar** |
| 3 | **T2 — Universal Formats** | CoinGecko (1) | ~5 s | warehouse round-trip |
| 4 | 🛣️ [XML Post Audit](./appendix/xml-post-audit/) | NHTSA POST (1) | ~3 s | **Used-car fraud audit** vs federal VIN database |
| 5 | **T3 — Parent-Child Drilling** | CoinGecko (11) | ~55 s | biggest CG hit; backtest data prep |
| 6 | 🛣️ [SpaceX Launches](./appendix/spacex-launches/) | SpaceX (~7) | ~5 s | **Ops dashboard feed** — upcoming-launches enrichment |
| 7 | **T4 — Stateful Refresh** | Binance.us (4) | ~10 s | natural non-CG cooldown |
| 8 | **T5 — Streaming Daemon** | Binance + CoinGecko (3) | ~30 s | live ticker + paginated drain |
| 9 | 🛣️ [NASCAR Fantasy Fjord](./appendix/nascar-fantasy-fjord/) | NASCAR (6) | ~8 s | **Fantasy sports scoring** — multi-series concurrent extract |
| 10 | **T6 — Multi-Source Fjord** | CoinGecko + Binance | continuous | terminal CG phase; one cycle |
| 11 | **T7 — Tideweaver** | local fixtures only | ~5 s | orchestration capstone |

> **Why detour?** CoinGecko's free public tier is 5–15 calls per minute, server-side.  Even with Incorporator's host-aware throttle (12 req/min on `api.coingecko.com` — see [the rate-limit registry](../incorporator/io/fetch.py)), back-to-back tutorial runs can stack calls inside the same 60-s window and trip the limiter.  The detours above are quick non-CG appendices that give CG's window time to refresh **while teaching complementary patterns** — XML + POST ingestion, ops-dashboard fan-out, multi-source sports analytics.

The tables below remain the canonical index of the curriculum and appendices.  Use the speed-run sequence when learning end-to-end; use the tables when you need a specific concept.

## The seven-tutorial curriculum

| # | Folder | What you learn |
|---|---|---|
| 1 | [`01-first-steps/`](./01-first-steps/) | 🌱 Discovery-first flow: `test()` profiles an endpoint, then `incorp()` applies the recommendations. |
| 2 | [`02-universal-formats/`](./02-universal-formats/) | 📦 Fan a CoinGecko snapshot into NDJSON / CSV / SQLite / Parquet and round-trip each. |
| 3 | [`03-parent-child-drilling/`](./03-parent-child-drilling/) | 🚀 `inc_parent` / `inc_child` — parent → child fan-out with O(1) dedup. |
| 4 | [`04-stateful-refresh/`](./04-stateful-refresh/) | 🔄 `refresh()` three ways against a live ticker. |
| 5 | [`05-streaming-daemon/`](./05-streaming-daemon/) | 🌊 Two polling modes: stateful dashboards + chunking bulk drains. |
| 6 | [`06-multi-source-fjord/`](./06-multi-source-fjord/) | 🌊 `fjord()` fuses two live sources into a derived spread. |
| 7 | [`07-tideweaver/`](./07-tideweaver/) | 🧵 Capstone — declarative diamond orchestration across three exchanges. |

## Appendix — same patterns, different domains

| Folder | Domain |
|---|---|
| [`appendix/spacex-launches/`](./appendix/spacex-launches/) | Parent → child + streaming daemon against SpaceX v4. |
| [`appendix/nascar-tideweaver/`](./appendix/nascar-tideweaver/) | Tideweaver diamond on race telemetry (laps + pits + flags). |
| [`appendix/pokeapi-etl/`](./appendix/pokeapi-etl/) | Paginated HATEOAS drill + array reductions via `calc()`. |
| [`appendix/xml-post-audit/`](./appendix/xml-post-audit/) | XML ingestion + declarative bulk POST + federal-DB audit. |
| [`appendix/crypto-graph-mapping/`](./appendix/crypto-graph-mapping/) | `link_to`-based in-memory join — Tutorial 6's pattern as a one-shot. |
| [`appendix/nascar-fantasy-fjord/`](./appendix/nascar-fantasy-fjord/) | Six-source fjord with state-aware `inflow()` and multi-output `outflow()`. |
| [`appendix/tideweaver-parquet-snapshots/`](./appendix/tideweaver-parquet-snapshots/) | Safe Parquet writes inside a Tideweaver window. |
| [`appendix/tideweaver-vs-prefect/`](./appendix/tideweaver-vs-prefect/) | In-process vs cloud orchestration decision matrix. |
| [`appendix/data-lake-pivot/`](./appendix/data-lake-pivot/) | JSON ↔ Avro/SQLite walkthrough (legacy). |

## CLI templates

[`cli-templates/`](./cli-templates/) holds the generic `pipeline.json` shapes referenced by [the CLI guide](../docs/cli_and_configuration.md) and [the deployment guide](../docs/deployment.md):

- `stream-basic.json` — minimal one-shot CSV export.
- `fjord-basic.json` — two-source fjord skeleton with `outflow_example.py`.
- `daemon-mode.json` — stateful polling daemon with decoupled refresh/export.
- `with-auth.json` — bearer-token via `${API_KEY}` env expansion.

## Running a tutorial

Each tutorial directory is runnable from anywhere in the repo:

```bash
python examples/06-multi-source-fjord/fjord.py
```

Sidecars (`outflow.py`, fixtures, `pipeline.json`) live next to the script that uses them — Python sibling imports work because Python adds the script's directory to `sys.path[0]` automatically.

For tutorials with a `pipeline.json` or `watershed.json`, the CLI form is:

```bash
incorporator validate examples/07-tideweaver/watershed.json
incorporator tideweaver run examples/07-tideweaver/watershed.json
```

Run from the repo root so relative `inc_file` paths inside the JSON resolve correctly.
