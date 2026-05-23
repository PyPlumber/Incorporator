***

# 🌊 Tutorial 8 — Streaming Daemon: Paginated Bulk Export at O(1) Memory

You're building a historical crypto warehouse and need to walk every coin CoinGecko
knows about, not just the top-100.  At `per_page=250` that's 40+ pages.  You don't
want to hold the whole list in memory; you want to write each page to disk and move
on.  That's the canonical `stream()` job: **paginated bulk-export chunking** under
your event loop, with peak memory pinned at one page.

`incorp()` fetches once.  `refresh()` refreshes a live registry once.  `stream()` is
the long-running pipeline — a paginator drives waves through the chunking engine
until the source exhausts, every wave written through the same crash-safe export
machinery as one-shot `export()`.  The kwargs *are* the pipeline definition.

**The Goal:**

* Drain CoinGecko's full `/coins/markets` catalogue to a single NDJSON file.
* Hold one page (~250 rows, ~50 KB) in memory at a time — not the whole catalogue.
* Append-on-every-wave so the warehouse grows monotonically; resumable on crash.
* Structured per-wave logs on disk via `LoggedIncorporator`.

**Prerequisites:** [Tutorial 7](../07-stateful-refresh/README.md) (`refresh()` semantics),
[Tutorial 1](../01-first-steps/README.md) (`incorp()`, `inc_dict`), [Tutorial 3](../03-universal-formats/README.md)
(append-friendly format selection for warehouses).

> **REST polling vs WebSockets — pick the right layer for your latency budget.**
> Sub-second tick data wants WebSockets — reach for `python-binance`, `websockets`,
> or CCXT's async streams for that.  REST polling at ≥30 s intervals is the right
> tool for paginated bulk drains, dashboards, mark-to-market valuations,
> slow-cadence indicators, portfolio NAV snapshots, and time-series warehouses.
> That's what `stream()` is built for.

> **Vocabulary anchor for the daemon family.**  Three long-running shapes
> share the same wave queue + LoggedIncorporator plumbing under the hood;
> the curriculum names them consistently:
> - **chunking-mode `stream()`** (Part 1 below) — paginated O(1) bulk drain.
> - **stateful single-source shim** (`stream(stateful_polling=True)`, Part 2) —
>   one source, live registry, compatibility path over fjord's engine.
> - **multi-source stateful daemon** ([`fjord()`](../10-multi-source-fjord/README.md), T10) —
>   N sources, fused `outflow(state)`, the canonical live-registry pattern.
> - **fjord flush** (Tideweaver `Fjord` current, T11) — the per-tick flush
>   variant of the daemon, scheduled inside a windowed graph.

---

## Part 1 — Chunking-mode bulk drain (the canonical use)

**Scenario:** historical warehouse seed.  Forty pages of CoinGecko market rows,
streamed one page at a time, each page appended to NDJSON, then released.

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
`CoinPage.inc_dict` is for in-wave processing, not for cross-wave reads.  Use
chunking mode whenever the answer to "do I need this registry to outlive the chunk?"
is **no**.

> **Why `refresh_params=None`?**  The default refresh policy assumes a stable
> per-instance origin URL — true for stateful registries, false for paginated
> chunks (each wave is a different page).  Passing `None` opts out of per-chunk
> refresh entirely; the paginator is the source of newness.

### Adaptive chunk sizing

`adapt_chunk_size=True` lets `stream()` resize `paginator.chunk_size` between
chunks via AIMD (additive-increase / multiplicative-decrease), bounded by
`chunk_size_min` / `chunk_size_max` and the latency window
`[target_min_sec, target_max_sec]`:

```python
async for wave in CoinPage.stream(
    incorp_params={...},
    adapt_chunk_size=True,
    chunk_size_min=100, chunk_size_max=100_000,
    target_min_sec=0.030, target_max_sec=0.100,
):
    ...
```

---

## What `stream()` is doing under the hood

1. **Pipeline routing.**  `run_pipeline()` picks the chunking engine when
   `stateful_polling` is left at its default of `False` (and the fjord engine
   otherwise — see Part 2).
2. **Wave queue.**  The engine writes Waves into one `asyncio.Queue`.  Your
   `async for` loop drains it.
3. **Shared HTTP/2 client.**  All page requests share one connection pool; Tenacity
   retries transient HTTP errors transparently.
4. **Atomic export writes.**  Every export path goes through the same crash-safe
   tempfile + `os.replace()` machinery as one-shot `export()`.  Appends are
   line-anchored so a kill mid-write never produces a torn record.
5. **Graceful drain.**  Ctrl+C / SIGTERM triggers ordered shutdown — in-flight
   page completes, export drains, file is sealed.

For the full engine breakdown see the [streaming & pagination
guide](../../docs/streaming_and_pagination.md) and the
[CLI configuration guide](../../docs/cli_and_configuration.md).

---

## `LoggedIncorporator` → structured logs on disk

The example above subclasses `LoggedIncorporator` (instead of `Incorporator`) and
passes `enable_logging=True`.  Every wave is routed through a `QueueHandler`
background thread into rotating JSON-line log files:

```
logs/api.log      # successful chunks
logs/error.log    # failed_sources entries (URLs redacted)
logs/debug.log    # internal lifecycle events
```

Post-process with `jq`, ship to a log aggregator, or `tail -f` — disk I/O never
blocks the event loop.

---

## Part 2 — Single-source live registries via `stateful_polling=True` (shim)

If you have a **single** source whose live registry you want to keep in memory and
snapshot to disk on a cadence — say a Binance.us mark-to-market dashboard — you
*can* reach for `stream(stateful_polling=True)`.  It's a thin compatibility shim
that routes through fjord's engine with an identity outflow.  The user-facing
contract is unchanged: the same `incorp` / `refresh` / `export` waves with the same
fields.

> **For multi-source live registries, jump to [Tutorial 10](../10-multi-source-fjord/README.md).**
> `fjord()` is the canonical multi-source stateful daemon; the shim below is
> documented for single-source compatibility, not for net-new daemons.

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
        stateful_polling=True,                    # single-source shim over fjord
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

The registry is *live*: your dashboard renderer can read
`BinancePair.inc_dict["BTCUSDT"]` at any moment between waves and get the most
recent refresh.  Exports rewrite the destination file each cycle by default
(snapshot semantics); pass `"if_exists": "append"` if you want a forensic ledger
instead.

> **Reach for `fjord()` directly for anything more than this.**  As soon as you
> have **two or more sources**, or need **cross-source join logic** in an
> `outflow()`, the shim's identity-outflow shape stops earning its keep.
> [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) is the
> canonical stateful-daemon pattern.  For windowed orchestration of multiple feeds
> on independent cadences reach for
> [Tideweaver (Tutorial 11)](../11-tideweaver/README.md).  T8 itself stays focused
> on chunking — that's `stream()`'s real job.

---

## Decision Matrix

| You want… | Reach for |
|---|---|
| One-shot bulk ingestion of a paginated source (historical backfill, warehouse seed) | **`stream(inc_page=<Paginator>, if_exists="append")`** — chunking engine, this tutorial |
| Single-source live registry on a cadence (mark-to-market, slow-indicator dashboard) | `stream(stateful_polling=True, refresh_interval=..., export_interval=...)` — compatibility shim, fine for the simple case |
| Multi-source live registry with a fused `outflow()` join | [`fjord()`](../10-multi-source-fjord/README.md) — Tutorial 10, the canonical stateful daemon |
| Multi-source orchestration on independent cadences within a window | [Tideweaver](../11-tideweaver/README.md) — Tutorial 11, declarative graph of currents |
| One-shot fetch into Python objects | `incorp()` ([Tutorial 1](../01-first-steps/README.md)) |
| Manual one-shot refresh of an existing registry | `refresh()` ([Tutorial 7](../07-stateful-refresh/README.md)) |

---

## 🐳 Run it from the CLI

Both engines have CLI equivalents driven by `pipeline.json`.

**Chunking (CoinGecko full-catalogue drain — the canonical T8 use):**

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

**Stateful shim (single-source Binance dashboard pattern):**

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

```bash
incorporator validate pipeline.json
incorporator stream pipeline.json --logs
```

The `--logs` flag swaps in `LoggedIncorporator` automatically.  Add
`--heartbeat-file /tmp/inc.beat` and your Docker `HEALTHCHECK` (already baked into
the ship-with-the-repo `Dockerfile`) will restart the container if the daemon
hangs.  See the [deployment guide](../../docs/deployment.md) for the full Compose
/ secrets / healthcheck walkthrough.

---

## Where to Go Next

> 👉 **Up next: [Tutorial 9 — NASCAR Fantasy Fjord](../09-nascar-fantasy-fjord/README.md).**  T10 is about to introduce `fjord()` for coordinated multi-source refresh — the canonical stateful-daemon pattern that supersedes the `stateful_polling=True` shim shown above.  T9 gives you the shape first on a real fantasy-sports scoring problem: pull driver standings concurrently from Cup, Xfinity, and Truck series; join with track + driver master data; produce a weekly fantasy-points table.  Its state-aware `inflow()` even previews T10's `depends_on` graph.  Runs in ~8 s.

| Goal | Read |
|---|---|
| Pick the right refresh mode before wrapping in a daemon | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| Preview the multi-source fjord shape on a fantasy-sports problem | [Tutorial 9 — NASCAR Fantasy Fjord](../09-nascar-fantasy-fjord/README.md) |
| Stream multiple sources concurrently with a fused outflow | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Orchestrate multiple sources on independent cadences in one window | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| Land per-window columnar artifacts (Parquet) | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Master the paginator family for the chunking engine | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |
| Ship as a Docker daemon with health checks | [Deployment Guide](../../docs/deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/08-streaming-daemon/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
