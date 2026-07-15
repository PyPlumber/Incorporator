***

# Tutorial 8 — Streaming Daemon: Paginated Bulk Export at O(1) Memory

You're building a historical crypto warehouse and need to walk every coin CoinGecko
knows about, not just the top-100.  At `per_page=250` that's 40+ pages.  You don't
want to hold the whole list in memory; you want to write each page to disk and move
on.  That's the canonical `stream()` job: **paginated bulk-export chunking** under
your event loop, with peak memory pinned at one page.

`incorp()` fetches once.  `refresh()` refreshes a live registry once.  `stream()` is
the long-running pipeline — a paginator drives waves through the chunking engine
until the source exhausts, every wave written through the same export machinery as
one-shot `export()`.  The kwargs *are* the pipeline definition, and they're the same
kwargs you already know from Tutorial 1: schema-free, no class definitions required.

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
> share the same Wave telemetry + LoggedIncorporator plumbing under the hood;
> the curriculum names them consistently:
> - **chunking-mode `stream()`** (Part 1 below) — paginated O(1) bulk drain.
> - **stateful single-source shim** (`stream(stateful_polling=True)`, Part 2) —
>   one source, live registry, compatibility path over fjord's engine.
> - **multi-source stateful daemon** ([`fjord()`](../10-multi-source-fjord/README.md), T10) —
>   N sources, fused `outflow(state)`, the canonical live-registry pattern.
> - **fjord flush** (Tideweaver `Fjord` current — full vocabulary at
>   [T11](../11-tideweaver/README.md)) — the per-tick flush variant of the
>   daemon, scheduled inside a windowed graph.

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
        print(f"page {wave.chunk_index}: {wave.rows_processed} coins")
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
chunks via an AIMD policy — additive-increase / multiplicative-decrease (grow
by 20% of the current size, shrink by half), bounded by `chunk_size_min` /
`chunk_size_max` and the latency window `[target_min_sec, target_max_sec]`:

```python
async for wave in CoinPage.stream(
    incorp_params={...},
    adapt_chunk_size=True,
    chunk_size_min=100, chunk_size_max=100_000,
    target_min_sec=0.030, target_max_sec=0.100,
):
    ...
```

> **Local paginators only.** `adapt_chunk_size` resizes `paginator.chunk_size` in
> place. `SQLitePaginator`, `CSVPaginator`, and `AvroPaginator` expose this
> attribute — web paginators (`PageNumberPaginator`, `NextUrlPaginator`, etc.) do
> not. Enabling the flag with a web paginator is a silent no-op: the engine logs a
> DEBUG message and continues without adaptation.

---

## What `stream()` is doing under the hood

1. **Pipeline routing.**  `run_pipeline()` picks the chunking engine when
   `stateful_polling` is left at its default of `False` (and the fjord engine
   otherwise — see Part 2).
2. **Wave generator.**  The chunking engine is a plain `AsyncGenerator` that
   yields one `Wave` per chunk directly to your `async for` loop.  No internal
   queue — `asyncio.Queue` is used only in the fjord engine.
3. **Shared HTTP/2 client.**  All page requests share one connection pool; Tenacity
   retries transient HTTP errors transparently.
4. **NDJSON append writes.**  In append mode, `NDJSONHandler` opens the target file
   in binary append mode (`ab`) and writes each record as a JSON line followed by
   `\n`.  A kill mid-write may leave a partial trailing line; prior records remain
   intact.  Non-append formats (JSON, XML) go through `atomic_write_path` —
   a sibling tempfile renamed on success.
5. **Graceful drain.**  Ctrl+C / SIGTERM triggers ordered shutdown — in-flight
   page completes, export drains, file is sealed.  When running via the CLI runner
   the signal handler is wired automatically; for bare `asyncio.run(main())`
   you need to install your own SIGTERM handler.

For the full engine breakdown see the [streaming & pagination
guide](../../docs/streaming_and_pagination.md) and the
[CLI configuration guide](../../docs/cli_and_configuration.md).

---

## `LoggedIncorporator` → structured logs on disk

The example above subclasses `LoggedIncorporator` (instead of `Incorporator`) and
passes `enable_logging=True`.  Every wave is routed through a `QueueHandler`
background thread into rotating JSON-line log files:

```
logs/CoinPage_api.log    # URL/internet-traffic errors (is_url_traffic_error=True):
                         #   HTTP 4xx/5xx, network timeouts, connection failures
logs/CoinPage_error.log  # all non-API records at INFO+: successful waves,
                         #   parse failures, schema errors — URLs redacted
logs/CoinPage_debug.log  # superset of both files above + DEBUG lifecycle events
```

Each wave record carries the full `Wave` payload as a structured `wave` JSON key,
so you can post-process with `jq` by any field:

```bash
jq 'select(.wave.rows_processed > 0) | .wave | {chunk_index, rows_processed,
    bytes_processed, bytes_downloaded, http_fetch_time_sec,
    http_retry_count, schema_cache_hit, conv_dict_time_sec}' \
    logs/CoinPage_error.log
```

The fields available per wave are:

| Field | Description |
|---|---|
| `rows_processed` | Records ingested this chunk |
| `bytes_processed` | Decoded response body size (`len(response.content)`) — post-decompression |
| `bytes_downloaded` | Wire byte count (`response.num_bytes_downloaded`) — compressed transfer size. `None` for non-HTTP sources |
| `http_fetch_time_sec` | HTTP round-trip latency in seconds (`response.elapsed`). `None` for non-HTTP sources |
| `http_retry_count` | Tenacity retries beyond the first |
| `schema_cache_hit` | `True` when schema compiled class was reused |
| `validation_error_count` | Pydantic rows rejected this chunk |
| `conv_dict_time_sec` | Seconds in the converter/ETL pass |
| `processing_time_sec` | Total wall-clock seconds for the chunk |
| `failed_sources` | Source URIs that errored (empty on success) |
| `rejects` | `list[RejectEntry]` from this chunk's incorp call — structured failure records with `is_url_traffic_error`, `error_kind`, `retry_after` |

The `bytes_downloaded` / `bytes_processed` pair is useful for diagnosing
compression efficiency: when `bytes_downloaded << bytes_processed`, the
server is sending compressed data and the client is expanding it.
`get_rejects()` returns the union of `_api.log` + `_error.log` reject
records; use `get_api()` to filter to URL-traffic errors only.

Disk I/O never blocks the event loop — the `QueueHandler` flushes in a background
thread.  Ship the log files to any aggregator or `tail -f` during a drain.

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
    """Live ticker registry — auto-keyed by trading symbol.

    Real fields, not a bare `pass` body: `stateful_polling=True` exports
    the same already-built row instances `incorp`/`refresh` produced, and
    that pass-through only round-trips when the receiver class declares
    its own fields (see the CLI addendum at the bottom of this page for
    the framework mechanics).
    """

    symbol: str
    lastPrice: float
    priceChangePercent: float
    highPrice: float
    lowPrice: float
    volume: float


async def main() -> None:
    async for wave in BinancePair.stream(
        incorp_params={
            "inc_url": "https://api.binance.us/api/v3/ticker/24hr",
            "inc_code": "symbol",
            "inc_name": "symbol",
        },
        stateful_polling=True,                    # single-source shim over fjord
        refresh_interval=30,                      # poll every 30 s
        export_params={"file_path": "out/binance_ticker.ndjson"},
        export_interval=300,                      # snapshot every 5 min
        enable_logging=True,                      # JSON-line logs to disk
    ):
        if wave.failed_sources:
            print(f"  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"  {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} pairs")


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
> [Tideweaver (Tutorial 11)](../11-tideweaver/README.md), the capstone that walks
> the full vocabulary (diamonds, penstocks, spillways).  T8 itself stays focused
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

## Run it from the CLI

Both engines have CLI equivalents driven by config files that ship next to
`streaming_daemon.py`: see [`pipeline.json`](pipeline.json) (chunking — the
canonical T8 use, matches the script's default) and
[`pipeline_stateful.json`](pipeline_stateful.json) (the stateful shim). Full
run instructions are in the addendum at the bottom of this page.

The `--logs` flag enables disk logging inside `LoggedIncorporator`.  Add
`--heartbeat-file /tmp/inc.beat` and your Docker `HEALTHCHECK` (already baked into
the ship-with-the-repo `Dockerfile`) will restart the container if the daemon
hangs.  See the [deployment guide](../../docs/deployment.md) for the full Compose
/ secrets / healthcheck walkthrough.

---

## Where to Go Next

> **Up next: [Tutorial 9 — NASCAR Fantasy Fjord](../09-nascar-fantasy-fjord/README.md).**  T9 previews the multi-source `fjord()` shape — the canonical stateful-daemon pattern that supersedes the `stateful_polling=True` shim shown above — on a real fantasy-sports scoring problem before T10 introduces `fjord()` formally.  T9 has you pull driver standings concurrently from Cup, Xfinity, and Truck series; join with track + driver master data; and produce a weekly fantasy-points table.  Its state-aware `inflow()` even previews T10's `depends_on` graph.  Runs in ~8 s.

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

## 🐳 Run It From the CLI (+ Docker)

Reference material — three ways to run each engine, in order. **Only
`pipeline.json` (chunking) matches `streaming_daemon.py`'s actual default
run** — `main()` calls `chunking_demo()` unconditionally and
`stateful_demo()` is commented out (`streaming_daemon.py:137-141`), so
picking a demo in the Python form means editing the source. The CLI form
has no such asymmetry: either config runs directly, by name.

**1. Python entry** (what every section above walked through):

```bash
cd examples/08-streaming-daemon
python streaming_daemon.py          # runs chunking_demo() by default
```

**2. CLI form** — [`pipeline.json`](pipeline.json) (chunking) and
[`pipeline_stateful.json`](pipeline_stateful.json) (stateful shim) ship
next to the entry script; no inline JSON duplicates here (see them drift
once, trust them forever). `pipeline.json` is pure JSON, no sidecar
needed. `pipeline_stateful.json` references [`outflow.py`](outflow.py) —
`streaming_daemon.py`'s `stateful_demo()` imports the same `BinancePair`
class from that same file, so the Python and CLI forms share one
receiver-class definition (see the caveat below for why a fielded class
is required here).

```bash
cd examples/08-streaming-daemon      # see caveat below
incorporator validate pipeline.json
incorporator stream pipeline.json --logs

incorporator validate pipeline_stateful.json
incorporator stream pipeline_stateful.json --logs
```

> **Run from inside this directory.** Both configs' `export_params.file_path`
> (`"out/coins_full.ndjson"`, `"out/binance_ticker.ndjson"`) are CWD-relative.
> Running `incorporator stream examples/08-streaming-daemon/pipeline.json`
> from the repo root silently writes to `<repo-root>/out/` instead.
>
> **Caveat — `pipeline.json`'s chunking drain has no page cap.**
> `chunking_demo()`'s `max_pages=3` cap is a `break` in the Python
> *consumer loop*, not a paginator setting — `PageNumberPaginator` has no
> `end_page`/`max_pages` kwarg, and `stream()`'s chunking engine
> unconditionally forces `paginator.call_lim=1` per wave regardless (see
> the comment at `streaming_daemon.py:92-96`). A bare CLI run has no
> equivalent consumer-loop hook, so `incorporator stream pipeline.json`
> walks CoinGecko's **entire** catalogue (40-70+ pages at `per_page=250`) —
> several minutes wall-clock. Interrupt it (`Ctrl+C`) after a page or two
> rather than waiting for full completion; the framework's documented
> graceful-drain guarantee seals the file cleanly.
>
> **Caveat — no host throttle on the CLI path.**
> `register_host_penstock("api.coingecko.com", rate_per_sec=0.2)` lives at
> the top of `streaming_daemon.py`; the CLI path never imports that file,
> so a plain `incorporator stream pipeline.json` run has no CoinGecko
> throttle registered (same gap as [Tutorial 10](../10-multi-source-fjord/README.md)).
> There's currently no declarative `pipeline.json` field for registering a
> host throttle.
>
> **Why `pipeline_stateful.json` needs a fielded receiver class.**
> `stream(stateful_polling=True)` synthesises an *identity* outflow: export
> receives the same already-built row instances `incorp()`/`refresh()`
> produced, not freshly-shaped dicts. That pass-through only round-trips
> cleanly when the receiver class declares real fields — a bare
> `class BinancePair(LoggedIncorporator): pass` crashes at the export tick
> with `Outflow Error: 1 validation error for BinancePair ...
> input_type=DynamicModel` (confirmed live: `incorp`/`refresh` succeed,
> `export` fails). Root cause is a framework bug in the bare-class fallback
> of `flush()`'s schema re-inference, not this config — see the framework
> gap tracked in this repo's context notes. The workaround shown here (a
> fielded class in [`outflow.py`](outflow.py), wired via `"outflow":
> "outflow.py"` and imported by `stateful_demo()` too) sidesteps the buggy
> branch entirely and is fully verified end to end, same as `pipeline.json`
> (chunking).

**3. Docker** — reasoned from the `Dockerfile`/`docker-compose.yml`, **NOT
run or verified** (no Docker available in this pass — confirm before
relying on it):

```bash
# Reasoned, unverified.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/examples/08-streaming-daemon:/app/config:ro" \
  -v "$(pwd)/examples/08-streaming-daemon/out:/app/out" \
  incorporator:latest \
  stream /app/config/pipeline.json --logs
```

The image's `WORKDIR` is `/app`, and `export_params.file_path` is
CWD-relative (never rebased against the config's directory) — so
either config's `"out/..."` target resolves to `/app/out/...` inside the
container. The mount target must therefore be `/app/out`, not one of the
three paths the `Dockerfile` prepares (`/app/config`, `/app/data`,
`/app/logs`). Because `/app/out` is not one of the pre-`chown`'d
directories, `--user` overrides to the invoking host user so the non-root
`appuser` can still write. Swap `pipeline.json` for
`pipeline_stateful.json` in the command above to run the stateful shim
instead.

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/08-streaming-daemon/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
