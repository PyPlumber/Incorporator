***

# 🌊 Streaming Daemon: Two Polling Modes for Two Real Use Cases

`incorp()` fetches once.  `refresh()` refreshes the live registry once.  `stream()` is
the long-running pipeline — periodic fetch + optional periodic export, all running as a
daemon under your event loop.  The kwargs *are* the pipeline definition.

`stream()` runs the chunking engine by default.  Setting `stateful_polling=True`
routes you through the fjord engine via a thin single-source shim — the user-facing
contract is unchanged (the same `incorp` / `refresh` / `export` waves with the same
fields), the underlying engine is fjord with an identity outflow.  Pick the right
mode for the job and the rest is plumbing.  This tutorial walks both modes
back-to-back, against two real crypto-research use cases.

**Prerequisites:** [Tutorial 4](../04-stateful-refresh/README.md) (`refresh()` semantics),
[Tutorial 1](../01-first-steps/README.md) (`incorp()`, `inc_dict`), [Tutorial 2](../02-universal-formats/README.md)
(append-friendly format selection for warehouses).

> **REST polling vs WebSockets — pick the right layer for your latency budget.**
> Sub-second tick data wants WebSockets — reach for `python-binance`, `websockets`,
> or CCXT's async streams for that.  REST polling at ≥30 s intervals is the right
> tool for dashboards, mark-to-market valuations, slow-cadence indicators, portfolio
> NAV snapshots, and time-series warehouses.  That's what `stream()` is built for.

---

## Part 1 — `stateful_polling=True`: The Live Mark-to-Market Dashboard

**Scenario:** you need a live in-memory registry of every USDT pair on Binance.us, so
your dashboard can read `BinancePair.inc_dict["BTCUSDT"].last_price` at any moment and
get a reading no more than 30 seconds stale.  Periodically you also snapshot the whole
registry to an NDJSON log for downstream consumers.

```python
import asyncio

from incorporator import LoggedIncorporator


class BinancePair(LoggedIncorporator):
    """Live ticker registry — auto-keyed by trading symbol."""


async def main() -> None:
    async for wave in BinancePair.stream(
        incorp_params={
            "inc_url": "https://api.binance.us/api/v3/ticker/24hr",
            "inc_code": "symbol",
            "inc_name": "symbol",
        },
        stateful_polling=True,                    # live registry, in-place refresh
        refresh_interval=30,                      # poll every 30 s
        export_params={"file_path": "data/binance_ticker.ndjson"},
        export_interval=300,                      # snapshot every 5 min
        enable_logging=True,                      # JSON-line logs to disk
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} pairs")


if __name__ == "__main__":
    asyncio.run(main())
```

**What runs:**

1. **Seed.** One `BinancePair.incorp(...)` builds the initial registry (~1,900 pairs).
2. **Two daemon tasks spawn.**
   * **Refresh daemon** re-fetches every 30 s, merges updated records into
     `BinancePair.inc_dict` under a shared lock.
   * **Export daemon** wakes every 5 min, snapshots the registry under the same lock,
     writes an NDJSON file (atomic rewrite of the *current* view; not an append log).
3. **Wave stream.** Each daemon yields one Wave per cycle into a shared queue.  Your
   `async for` loop observes the pipeline without polling it.
4. **Shutdown.** Ctrl+C / SIGTERM sets a shutdown event; daemons drain in flight, the
   queue closes, the loop exits.

**The registry is *live*.**  Your dashboard renderer can read
`BinancePair.inc_dict["BTCUSDT"]` at any moment between waves and get the most recent
refresh.  This is the mark-to-market pattern.

> **Snapshot vs append log.**  In `stateful_polling=True` mode the engine **rewrites
> the destination file each export wave** with the latest snapshot.  Downstream
> consumers can `head` / `read_ndjson()` the file at any moment and see the live
> state.  If you need an append-on-every-wave forensic ledger instead, opt in:
>
> ```python
> export_params={
>     "file_path": "data/binance_history.ndjson",
>     "if_exists": "append",                     # accumulate, not overwrite
> },
> ```

---

## Part 2 — `stateful_polling=False`: The Bulk-Ingestion Chunking Pipeline

**Scenario:** you're building a historical warehouse and need to walk every coin
CoinGecko knows about, not just the top-100.  At `per_page=250` that's 40+ pages.
You don't want to hold the whole list in memory; you want to write each page to disk
and move on.

```python
from incorporator import LoggedIncorporator
from incorporator.io.pagination import PageNumberPaginator


class CoinPage(LoggedIncorporator):
    """One transient chunk of CoinGecko market rows per wave."""


async def main() -> None:
    paginator = PageNumberPaginator(page_param="page", start_page=1)

    async for wave in CoinPage.stream(
        incorp_params={
            "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
            "params": {"vs_currency": "usd", "per_page": 250},
            "inc_code": "id",
            "inc_name": "name",
            "inc_page": paginator,                # makes the chunking engine fire
            "excl_lst": ["image"],
        },
        refresh_params=None,                      # chunking: opt out of per-chunk refresh
        export_params={
            "file_path": "data/coins_full.ndjson",
            "if_exists": "append",                # accumulate every page
        },
        # stateful_polling defaults to False → chunking engine.
        # No refresh_interval / export_interval — chunking is event-driven by the paginator.
        enable_logging=True,
    ):
        print(f"📦 page {wave.chunk_index}: {wave.rows_processed} coins")
```

**What runs:**

1. **Each wave is a fresh `incorp()` for the next page.**  The paginator yields page
   1, 2, 3 … until the response is empty.
2. **Per-page export.**  Every wave appends its chunk to the NDJSON file.  Peak
   memory is one page's worth of records (~250 coins, ~50 KB).
3. **Clean exit.**  When the paginator exhausts, the stream completes — the daemon
   exits on its own.  No Ctrl+C needed; this is a *one-shot bulk drain*, not a
   long-running watcher.

**The registry is *transient*.**  Each wave's records belong to that wave only;
`CoinPage.inc_dict` is for in-wave processing, not for cross-wave reads.  Use chunking
mode whenever the answer to "do I need this registry to outlive the chunk?" is no.

> **Why `refresh_params=None`?**  The default refresh policy assumes a stable
> per-instance origin URL — true for stateful registries, false for paginated
> chunks (each wave is a different page).  Passing `None` opts out of per-chunk
> refresh entirely; the paginator is the source of newness.

---

## Decision Matrix

| You want… | Engine | `stream()` flags |
|---|---|---|
| Live registry that mutates over time (dashboard, mark-to-market, slow indicators) | **Stateful** | `stateful_polling=True`, `refresh_interval=N`, `export_interval=M` |
| One-shot bulk ingestion of a paginated source (historical backfill, warehouse seed) | **Chunking** | `inc_page=<Paginator>`, `export_params={"if_exists": "append"}` |
| Both at once across multiple sources on independent cadences | Tideweaver | [Tutorial 7](../07-tideweaver/README.md) — declarative graph of currents |

---

## What `stream()` is doing under the hood

Same shape regardless of engine — the dispatch differs only in *how* each wave is
populated:

1. **Pipeline routing.**  `run_pipeline()` picks the stateful or chunking engine based
   on `stateful_polling`.
2. **Wave queue.**  Both engines write Waves into one `asyncio.Queue`.  Your
   `async for` loop drains it.
3. **Shared HTTP/2 client.**  All requests share one connection pool; Tenacity
   retries transient HTTP errors transparently.
4. **Atomic export writes.**  Every export path goes through the same crash-safe
   tempfile + `os.replace()` machinery as one-shot `export()`.
5. **Graceful drain.**  Ctrl+C / SIGTERM triggers ordered shutdown — in-flight refresh
   completes, export drains, file is sealed.

For the full engine breakdown see the [streaming & pagination
guide](../../docs/streaming_and_pagination.md) and the
[CLI configuration guide](../../docs/cli_and_configuration.md).

---

## `LoggedIncorporator` → structured logs on disk

Both modes above subclass `LoggedIncorporator` (instead of `Incorporator`) and pass
`enable_logging=True`.  Every wave is routed through a `QueueHandler` background
thread into rotating JSON-line log files:

```
logs/api.log      # successful chunks
logs/error.log    # failed_sources entries (URLs redacted)
logs/debug.log    # internal lifecycle events
```

Post-process with `jq`, ship to a log aggregator, or `tail -f` — disk I/O never
blocks the event loop.

---

## 🐳 Run it from the CLI

Both engines have CLI equivalents driven by `pipeline.json`.

**Stateful (Binance dashboard pattern):**

```json
{
  "incorp_params": {
    "inc_url": "https://api.binance.us/api/v3/ticker/24hr",
    "inc_code": "symbol",
    "inc_name": "symbol"
  },
  "export_params": {"file_path": "data/binance_ticker.ndjson"},
  "stateful_polling": true,
  "refresh_interval": 30.0,
  "export_interval": 300.0
}
```

**Chunking (CoinGecko full-catalogue drain):**

```json
{
  "incorp_params": {
    "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
    "params": {"vs_currency": "usd", "per_page": 250},
    "inc_code": "id",
    "inc_name": "name",
    "inc_page": "PageNumberPaginator(page_param='page')",
    "excl_lst": ["image"]
  },
  "export_params": {
    "file_path": "data/coins_full.ndjson",
    "if_exists": "append"
  },
  "stateful_polling": false
}
```

```bash
incorporator validate pipeline.json
incorporator stream pipeline.json --logs
```

The `--logs` flag swaps in `LoggedIncorporator` automatically.  Add
`--heartbeat-file /tmp/inc.beat` and your Docker `HEALTHCHECK` (already baked into
the ship-with-the-repo `Dockerfile`) will restart the container if the daemon
hangs.  See the [deployment guide](../../docs/deployment.md) for the full Compose / secrets /
healthcheck walkthrough.

---

## When to use `stream()` vs `incorp()`

| You want… | Reach for |
|---|---|
| One-shot fetch into Python objects | `incorp()` |
| Live dashboard / mark-to-market registry on a cadence | `stream(stateful_polling=True, ...)` |
| Bulk drain of a paginated source into an append-friendly warehouse | `stream(stateful_polling=False, inc_page=...)` |
| Multi-source fusion with a custom `outflow()` join | [`fjord()`](../06-multi-source-fjord/README.md) |
| Multi-source orchestration on independent cadences within a window | [Tideweaver](../07-tideweaver/README.md) |

---

## Where to Go Next

| Goal | Read |
|---|---|
| Pick the right refresh mode before wrapping in a daemon | [Tutorial 4 — Stateful Refresh](../04-stateful-refresh/README.md) |
| Stream multiple sources concurrently with a fused outflow | [Tutorial 6 — Multi-Source Fjord](../06-multi-source-fjord/README.md) |
| Orchestrate multiple sources on independent cadences in one window | [Tutorial 7 — Tideweaver](../07-tideweaver/README.md) |
| Run the same daemon shape against a non-crypto domain | [Appendix — SpaceX Launches](../appendix/spacex-launches/README.md) |
| Land per-window columnar artifacts (Parquet) | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Master the paginator family for the chunking engine | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |
| Ship as a Docker daemon with health checks | [Deployment Guide](../../docs/deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/05-streaming-daemon/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
