# Incorporator Examples

Every tutorial is its own self-contained directory.  The `README.md` inside is the canonical write-up; the `.py` next to it is the runnable code; sidecars / fixtures / `pipeline.json` / `watershed.json` ship in the same folder.

## The eleven-tutorial curriculum

The curriculum alternates CoinGecko-heavy steps with non-CoinGecko domain examples.  That rhythm gives CoinGecko's per-minute rate-limit window time to refresh between CG-touching tutorials, **and** broadens each Incorporator pattern across multiple real-world verticals (compliance, aerospace, sports analytics, fantasy leagues) so you see the same primitives apply everywhere.

| # | Folder | API | Wall-clock | What you learn |
|---|---|---|---|---|
| **T1** | [`01-first-steps/`](./01-first-steps/) | CoinGecko (2) | ~10 s | 🌱 Discovery-first flow: `test()` profiles an endpoint, then `incorp()` applies the recommendations. |
| **T2** | [`02-data-lake-pivot/`](./02-data-lake-pivot/) | jsonplaceholder (1) | ~2 s | 🔁 SaaS roster → BI-ready columnar: pivot a `/users` endpoint into Avro + SQLite. |
| **T3** | [`03-universal-formats/`](./03-universal-formats/) | CoinGecko (1) | ~5 s | 📦 Fan a CoinGecko snapshot into NDJSON / CSV / SQLite / Parquet and round-trip each. |
| **T4** | [`04-xml-post-audit/`](./04-xml-post-audit/) | NHTSA POST (1) | ~3 s | 🛡️ Federal-VIN fraud audit: XML invoice ledger enriched via one batched POST. |
| **T5** | [`05-parent-child-drilling/`](./05-parent-child-drilling/) | CoinGecko (11) | ~55 s | 🚀 `inc_parent` / `inc_child` — parent → child fan-out with O(1) dedup. |
| **T6** | [`06-spacex-launches/`](./06-spacex-launches/) | SpaceX (~7) | ~5 s | 🚀 Ops-dashboard feed: upcoming launches drilled for rocket + launchpad detail. |
| **T7** | [`07-stateful-refresh/`](./07-stateful-refresh/) | Binance.us (4) | ~10 s | 🔄 `refresh()` three ways against a live ticker. |
| **T8** | [`08-streaming-daemon/`](./08-streaming-daemon/) | Binance + CG (3) | ~30 s | 🌊 Two polling modes: stateful dashboards + chunking bulk drains. |
| **T9** | [`09-nascar-fantasy-fjord/`](./09-nascar-fantasy-fjord/) | NASCAR (6) | ~8 s | 🏁 Fantasy-sports scoring fjord across Cup, Xfinity, Truck series. |
| **T10** | [`10-multi-source-fjord/`](./10-multi-source-fjord/) | CG + Binance | continuous | 🌊 `fjord()` fuses two live sources into a derived spread. |
| **T11** | [`11-tideweaver/`](./11-tideweaver/) | local fixtures | ~5 s | 🧵 Capstone — declarative diamond orchestration across three exchanges. |

> **Why the alternating rhythm?**  CoinGecko's free public tier is 5–15 calls per minute, server-side.  Incorporator's host-aware throttle (12 req/min on `api.coingecko.com` — see [the rate-limit registry](../incorporator/io/fetch.py)) paces each script, but the per-minute window persists across scripts.  Interleaving non-CG steps between CG-heavy ones lets the window refresh while you learn a complementary pattern.

## Appendix — optional side-quests, different domains

| Folder | Mirrors | Domain |
|---|---|---|
| [`appendix/pokeapi-etl/`](./appendix/pokeapi-etl/) | T5 | Paginated HATEOAS drill + array reductions via `calc()`. |
| [`appendix/crypto-graph-mapping/`](./appendix/crypto-graph-mapping/) | T10 | `link_to`-based in-memory join — T10's pattern as a one-shot. |
| [`appendix/nascar-tideweaver/`](./appendix/nascar-tideweaver/) | T11 | Tideweaver diamond on race telemetry (laps + pits + flags). |
| [`appendix/tideweaver-parquet-snapshots/`](./appendix/tideweaver-parquet-snapshots/) | T11 | Safe Parquet writes inside a Tideweaver window. |
| [`appendix/tideweaver-vs-prefect/`](./appendix/tideweaver-vs-prefect/) | T11 | In-process vs cloud orchestration decision matrix. |

## CLI templates

[`cli-templates/`](./cli-templates/) holds the generic `pipeline.json` shapes referenced by [the CLI guide](../docs/cli_and_configuration.md) and [the deployment guide](../docs/deployment.md):

- `stream-basic.json` — minimal one-shot CSV export.
- `fjord-basic.json` — two-source fjord skeleton with `outflow_example.py`.
- `daemon-mode.json` — stateful polling daemon with decoupled refresh/export.
- `with-auth.json` — bearer-token via `${API_KEY}` env expansion.

## Running a tutorial

Each tutorial directory is runnable from anywhere in the repo:

```bash
python examples/10-multi-source-fjord/fjord.py
```

Sidecars (`outflow.py`, fixtures, `pipeline.json`) live next to the script that uses them — Python sibling imports work because Python adds the script's directory to `sys.path[0]` automatically.

For tutorials with a `pipeline.json` or `watershed.json`, the CLI form is:

```bash
incorporator validate examples/11-tideweaver/watershed.json
incorporator tideweaver run examples/11-tideweaver/watershed.json
```

Run from the repo root so relative `inc_file` paths inside the JSON resolve correctly.