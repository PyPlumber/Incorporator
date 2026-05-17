***

# 🌊 Streaming Daemon: A Live SpaceX Launch Watcher

So far you've used `incorp()` to fetch a snapshot. But what if you need to
**continuously watch an API**, pick up new records as they appear, and
flush them to a data file every few minutes?

That's what `stream()` is for. It's a long-running pipeline: periodic
fetch + optional stateful refresh + optional periodic export, all running
as a daemon under your event loop. The kwargs *are* the pipeline
definition.

By the end of this tutorial you'll have a daemon that polls the SpaceX
"latest launch" endpoint every 60 seconds, accumulates updates in an
in-memory registry, and flushes a Parquet snapshot to disk every 5
minutes — restart-safe, signal-aware, and runnable from either Python or
the CLI.

---

## The Goal

* **Source:** `https://api.spacexdata.com/v4/launches/upcoming`
* **Refresh cadence:** every 60 seconds
* **Export cadence:** every 5 minutes, into `data/spacex_upcoming.ndjson`
* **Failure handling:** transient errors logged via the wave stream, not
  fatal
* **Shutdown:** Ctrl+C / SIGTERM drains in-flight work and exits cleanly

---

## Step 1: The Python Pipeline

```python
import asyncio
from incorporator import LoggedIncorporator


class Launch(LoggedIncorporator):
    """SpaceX latest-launch tracker — pass enable_logging=True to stream() so waves land in JSON logs."""


async def main():
    async for wave in Launch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
            "inc_code": "id",
            "inc_name": "name",
        },
        stateful_polling=True,                                  # live registry, not one-shot
        refresh_interval=30.0,                                  # re-fetch every 30 s
        export_params={"file_path": "data/spacex_upcoming.ndjson"},
        export_interval=90.0,                                   # flush every 90 s
        enable_logging=True,                                    # JSON-line logs to disk
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
```

That's it. **No `while True` loop. No sleep. No try/except.** The engine
handles cadence, retries, draining, and shutdown.

> **Two modes — pick one explicitly:**
>
> `stateful_polling` controls what happens on each wave. Use
> `stateful_polling=True` when you want to build one in-memory registry
> and keep it live — useful for APIs where re-fetching the full dataset
> on every wave would waste quota or incur per-call charges. Use the
> default (`False`) when each wave should fetch the next chunk of data
> from a paginated source and then discard it from memory.
>
> * **`stateful_polling=True`** *(used above — the production watcher
>   shape)*: seed the registry once, keep it live in memory, refresh
>   and export on independent cadences, run until Ctrl+C / SIGTERM.
> * **`stateful_polling=False`** *(the default — chunking mode)*: each
>   wave is a fresh `incorp()` for the next chunk. State is released
>   between chunks (O(1) memory). The daemon **exits** when the source
>   has no more chunks. Use this to drain a paginated catalogue once
>   and walk away.
>
> On a single-record endpoint with `stateful_polling=False` the
> daemon emits one Wave and exits because the source has no more
> chunks — confirm that's the intent before reaching for `stream()`.

> **Format constraint:** `stream()` writes incrementally on every
> export wave, so the export target must be an **append-friendly**
> format: `.ndjson` / `.csv` / `.sqlite` / `.avro`. Parquet / Feather /
> ORC / Excel / XML / JSON all reject append mode (footer-indexed or
> monolithic encodings) — use those only for one-shot `incorp()` →
> `export()` round-trips, not stream destinations. Pick NDJSON if
> you're unsure — it's the streaming-native default.

> **What the file contains across waves:**  in `stateful_polling=True`
> mode the engine RE-EXPORTS THE SAME REGISTRY on every wave — every
> wave rewrites the destination file with the latest snapshot
> (~18 launches in the example above).  The file holds the *current*
> view, not an accumulation across waves.  This is the right default
> for "watcher" pipelines: a downstream consumer can `head` /
> `read_ndjson()` the file at any moment and see the live state.
>
> If you need an **append-on-every-wave** ledger (forensic archive,
> debugging trace, change-data-capture log), opt in explicitly:
>
> ```python
> export_params={
>     "file_path": "data/spacex_history.ndjson",
>     "if_exists": "append",                       # forensic accumulation
> },
> ```
>
> The chunked mode (`stateful_polling=False`, the default) behaves
> differently: each chunk is NEW data so it *does* accumulate by
> design.  See the [CLI configuration guide](./cli_and_configuration.md#parameter-breakdown)
> for the mode-aware `export_params` defaults summary.

---

## What `stream()` is doing under the hood

1. **Seed.** Runs one `Launch.incorp(...)` with your `incorp_params` to
   build the initial in-memory registry (`Launch.inc_dict`). Emits one
   Wave with `rows_processed` = the number of records the source
   returned. For `/launches/upcoming` that's ~18; for the singular
   `/launches/latest` it would be exactly 1 — the row count reflects
   the **source's shape**, not the daemon's health.
2. **Two daemon tasks spawn.**
   * A **refresh daemon** re-fetches every `refresh_interval` seconds and
     merges new/updated records into `Launch.inc_dict` under a shared lock.
     Emits one Wave per refresh cycle.
   * An **export daemon** wakes every `export_interval` seconds, snapshots
     the registry under the same lock, and calls `Launch.export(...)` to
     write the file. Emits one Wave per export cycle.
3. **Wave stream.** Each daemon yields one Wave per cycle into a
   shared queue. Your `async for` loop consumes them — that's how you
   observe the pipeline without polling it yourself.
4. **Shutdown.** Ctrl+C / SIGTERM sets a shutdown event; daemons drain,
   the queue closes, the `async for` loop exits.

---

## Step 2: Configuring Real-World Resilience

The pipeline above is already production-shaped, but two flags make it
operator-friendly:

### `LoggedIncorporator` → structured logs on disk

By subclassing `LoggedIncorporator` (instead of `Incorporator`) and
passing `enable_logging=True` to the verb call, every wave is routed
through a `QueueHandler` background thread into rotating JSON-line log
files:

```
logs/api.log      # successful chunks
logs/error.log    # failed_sources entries (URLs redacted)
logs/debug.log    # internal lifecycle events
```

You can post-process these with `jq`, ship them to a log aggregator, or
just `tail -f` them — disk I/O never blocks the event loop.

### `inc_dict` survives across refresh waves

Because `LoggedIncorporator` (and `Incorporator`) back `inc_dict` with a
`WeakValueDictionary`, the registry stays O(1) and never accumulates
unreachable objects. You can query it from anywhere:

```python
latest = Launch.inc_dict["5eb87d47ffd86e000604b38a"]  # by inc_code
print(f"Last mission: {latest.inc_name}")
```

---

## 🐳 Run it from the CLI

The same pipeline expressed as `pipeline.json` — no Python wrapper
required:

```json
{
  "incorp_params": {
    "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
    "inc_code": "id",
    "inc_name": "name"
  },
  "refresh_params": {},
  "export_params": {"file_path": "data/spacex_upcoming.ndjson"},
  "stateful_polling": true,
  "refresh_interval": 30.0,
  "export_interval": 90.0
}
```

```bash
incorporator validate pipeline.json
incorporator stream pipeline.json --logs
```

The `--logs` flag swaps in `LoggedIncorporator` automatically. Add
`--heartbeat-file /tmp/inc.beat` and your Docker `HEALTHCHECK` (already
baked into the ship-with-the-repo `Dockerfile`) will restart the
container if the daemon hangs.

For the full production-Docker walkthrough (compose, secrets, healthchecks,
graceful shutdown), see [the deployment guide](./deployment.md).

---

## When to use `stream()` vs `incorp()`

| You want… | Reach for |
|---|---|
| One-shot fetch into Python objects | `incorp()` |
| Periodic fetch + export of a single source as a daemon | `stream()` |
| Multi-source fusion with a custom `outflow()` join | [`fjord()`](./6_multi_source_fjord.md) |

---

## See Also

* **[Tutorial 4 — Stateful Refresh](./4_stateful_refresh.md)** — the
  one-shot `refresh()` that `stream()` wraps in a daemon.
* **[Tutorial 6 — Multi-Source Fjord](./6_multi_source_fjord.md)** —
  `fjord()` for fusing N concurrent sources.
* **[Tutorial 7 — Tideweaver](./7_tideweaver.md)** — coordinate multiple
  `stream()` pipelines on independent cadences with dependency gating
  inside a bounded time window.
* **[Streaming & Pagination Deep Dive](./streaming_and_pagination.md)** —
  paginator integration (handing `stream()` an `inc_page=` paginator for
  O(1) chunked ingestion).
* **[Library reference](./library_reference.md)** — full `stream()`
  kwarg signature.
