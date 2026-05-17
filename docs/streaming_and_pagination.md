# Streaming & Pagination (O(1) Memory)

When dealing with massive datasets (10M+ rows) or heavily paginated REST APIs, loading everything into RAM at once will cause your server to crash with an Out-Of-Memory (OOM) error.

Incorporator solves this natively using **Stateful Paginators**. By passing a paginator to the `inc_page` parameter, the framework shifts into a strict **O(1) Memory Chunking** mode. It fetches a chunk, processes it, saves it to disk, and triggers Python's Garbage Collector before moving to the next chunk.

---

## 1. Local Data Streaming (Databases & Massive Files)

Incorporator treats massive local files exactly like paginated web APIs. These paginators maintain persistent file pointers or database cursors, yielding byte-encoded arrays directly into Incorporator's C-speed instantiation engine.

### Available Local Paginators:
*   **`SQLitePaginator(db_path, sql_query, chunk_size)`**: Maintains a live database cursor, yielding rows incrementally via `fetchmany()`.
*   **`CSVPaginator(file_path, chunk_size, delimiter)`**: Maintains a persistent file pointer, slicing the CSV block by block.
*   **`AvroPaginator(file_path, chunk_size)`**: Utilizes `fastavro` to read binary blocks sequentially.

### Example: Streaming 10 Million Rows from SQLite to CSV

```python
import asyncio
from incorporator import LoggedIncorporator, FormatType
from incorporator.io.pagination import SQLitePaginator

class User(LoggedIncorporator): pass

async def run_massive_export():
    # 1. Initialize the Stateful Paginator
    db_streamer = SQLitePaginator(
        db_path="massive_database.db",
        sql_query="SELECT * FROM users_table",
        chunk_size=10000,
    )

    # 2. Start the Autonomous O(1) Stream
    async for wave in User.stream(
        incorp_params={
            "inc_url": "local_database_stream", # Satisfies origin tracking
            "inc_page": db_streamer,            # Hands control to the Paginator
            "format_type": FormatType.JSON,     # Local paginators yield JSON bytes
        },
        export_params={
            "file_path": "output/users_export.csv"
            # Note: stream() automatically forces if_exists="append"
        },
    ):
        print(f"Exported chunk {wave.chunk_index}: {wave.rows_processed} rows")

asyncio.run(run_massive_export())
```
*Because the dataset decays at the end of each loop iteration, memory consumption remains perfectly flat.*

---

## 2. Web API Pagination (REST APIs)

Web APIs use wildly different pagination strategies. Incorporator provides out-of-the-box support for the 5 most common patterns. They feature built-in infinite loop protection and seamlessly integrate with the framework's concurrency engine.

### Available Web Paginators:
*   **`CursorPaginator(cursor_param="cursor")`**: Extracts the next token from the payload and appends it to the query string (e.g., Twitter/X API).
*   **`NextUrlPaginator(*path_keys)`**: Drills into the JSON body to find the fully qualified "next" URL (e.g., PokéAPI).
*   **`OffsetPaginator(limit, offset_param, limit_param)`**: Increments a skip counter (e.g., Open Library API).
*   **`PageNumberPaginator(page_param="page", start_page=1)`**: Increments a page query parameter (e.g., CoinGecko).
*   **`LinkHeaderPaginator()`**: Parses the HTTP `Link` header looking for `rel="next"` (e.g., GitHub API).

### Example: Scraping a Paginated API

You can use paginators with the standard `incorp()` method for simple array accumulation, or with `.stream()` for continuous daemon polling.

```python
import asyncio
from incorporator import Incorporator
from incorporator.io.pagination import PageNumberPaginator

class Item(Incorporator): pass

async def scrape_api():
    # Setup the paginator to increment '?page='
    paginator = PageNumberPaginator(page_param="page", start_page=1)

    # incorp() will automatically loop until the API returns no more data
    dataset = await Item.incorp(
        inc_url="https://api.example.com/items",
        inc_page=paginator
    )
    
    print(f"Successfully scraped {len(dataset)} total items across all pages.")

asyncio.run(scrape_api())
```

---

## 3. How the Stateful Engine Works

Whether you use a Web Paginator or a Local Paginator, the internal mechanics are identical:

1.  **State Retention:** The `AsyncPaginator` class holds variables like `self.offset`, `self.current_cursor`, or `self._reader` in its `__init__` method.
2.  **O(1) Orchestration:** `.stream()` drives the paginator one chunk at a time — exactly one page is fetched, materialised, exported, and released before the next iteration begins.
3.  **Daemon Reset:** If you are running an infinite stream with `--poll 3600` (1 hour), the orchestrator automatically calls `paginator.reset()` when it wakes up, starting the extraction loop back at row/page 1 to check for new data.

### Example: A Self-Resetting Background Daemon

If you want to run a continuous data scraper in the background (e.g., pulling live event logs every 10 minutes), the `stream()` engine handles the paginator resets for you automatically.

```python
import asyncio
from incorporator import LoggedIncorporator
from incorporator.io.pagination import NextUrlPaginator

class LiveEvent(LoggedIncorporator): pass

async def run_infinite_scraper():
    # 1. STATE RETENTION: Paginator holds the URLs and cursors safely.
    paginator = NextUrlPaginator("meta", "next_page_link")

    # 2. O(1) ORCHESTRATION: stream() drives the paginator one page at a time.
    # Each iteration fetches one page, exports it, releases the RAM, and repeats
    # until the API is exhausted.
    async for wave in LiveEvent.stream(
        incorp_params={
            "inc_url": "https://api.example.com/live-events",
            "inc_page": paginator,
        },
        export_params={"file_path": "output/live_events.csv"},
        poll_interval=600.0,  # Sleep for 10 minutes when the API runs out of pages
        enable_logging=True,
    ):
        print(f"Processed chunk {wave.chunk_index}: {wave.rows_processed} events.")

        # 3. DAEMON RESET: After exhaustion, it sleeps for 600s.
        # When it wakes up, stream() calls paginator.reset() behind the scenes
        # and starts pulling from page 1 all over again!

if __name__ == "__main__":
    asyncio.run(run_infinite_scraper())
```

---

## 4. Skip the Code: Run it via CLI

Don't want to write boilerplate Python scripts to run your streams? You don't have to.

Incorporator includes a built-in CLI that can execute this exact same stateful, infinite-looping daemon using a simple JSON file.

Instead of writing the Python loop above, simply define your pipeline in **`pipeline.json`**:
```json
{
  "stateful_polling": false,
  "incorp_params": {
    "inc_url": "https://api.example.com/live-events"
  },
  "export_params": {
    "file_path": "output/live_events.csv",
    "if_exists": "append"
  }
}
```
*(Note: While advanced custom Paginator classes require the Python API to instantiate, standard API parameters can be mapped natively here).*

Then, trigger the infinite daemon directly from your terminal:
```bash
incorporator stream pipeline.json --poll 600.0 --logs
```

**Ready to automate your pipelines without writing code?**  
👉 **[Read the CLI & Configuration Guide](cli_and_configuration.md)**

---

## 5. When to use `fjord()` instead of `stream()`

`stream()` operates on **one** source per pipeline. When you need to keep a
live, joined object map synchronised across **multiple** APIs and combine
them into a brand-new entity (e.g. CoinGecko spot price + Binance futures
price → CoinMarket with computed spread), reach for `fjord()`.

`fjord()` runs each source's refresh daemon concurrently under a shared
lock, then calls a user-supplied `outflow(state)` function on each export
wave. The output class is built dynamically from the rows `outflow()`
returns — named after the `outflow` filename (`coin_market.py` →
`CoinMarket`). No output class to declare. Stateful-polling only — no
chunking mode.

```python
# coin_market.py defines Coin, BinanceFutures, and outflow(state).
# The fjord output class is built dynamically from the outflow file's
# stem ("coin_market.py" -> "CoinMarket"), so the receiver class below
# is just the orchestrator entry point — not the output type.
from coin_market import Coin, BinanceFutures

async for wave in Incorporator.fjord(
    stream_params=[
        {"cls": Coin,           "incorp_params": {...}, "refresh_params": {}},
        {"cls": BinanceFutures, "incorp_params": {...}, "refresh_params": {}},
    ],
    outflow="coin_market.py",
    export_params={"file_path": "markets.ndjson"},
    refresh_interval=60.0,
    export_interval=300.0,
):
    print(wave)
```

The CLI ships an equivalent subcommand:
```bash
incorporator fjord pipeline.json --logs
```

👉 See the [fjord section](./cli_and_configuration.md#6-the-fjord-subcommand--multi-source-stateful-pipelines)
of the CLI guide for the JSON schema and a worked example, or the
[Library reference](./library_reference.md) for the full method signature.

---

## 6. Performance Characteristics

Streaming pipelines benefit from several recent engine optimisations — they
apply automatically, no code changes required:

* **HTTP/2 multiplexing** in the shared `httpx.AsyncClient` — one TCP/TLS
  connection carries every concurrent request, eliminating per-batch
  handshake overhead.
* **LRU `SCHEMA_REGISTRY`** — compiled Pydantic classes are cached and
  evicted by least-recently-used; long-running daemons that see many
  distinct shapes don't thrash the cache.
* **Batched `model_validate`** — Pydantic instantiation runs in 1000-row
  batches so the Rust core can amortise schema lookups across the batch.
* **In-place columnar parse** — Parquet/Feather/ORC parse uses
  `pyarrow.compute` for vectorised JSON-prefix detection, skipping the
  per-cell Python check entirely when string columns contain no JSON.
* **`asyncio.to_thread` for `outflow_fn`** — CPU-heavy user joins in
  `fjord()` no longer block refresh / export daemons running on other
  sources.

Measured throughput on commodity hardware: 200k+ rows/sec for Parquet
parse, 140k–250k rows/sec for delimited and columnar writes. See
[`tests/benchmarks/`](../tests/benchmarks/) for the full per-format
matrix.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Pick the right polling mode for your pipeline | [Tutorial 5 — Streaming Daemons](./5_streaming_daemon.md) |
| Snapshot millions of rows into a warehouse without OOM | [Tutorial 2 — Universal Formats](./2_universal_formats.md) |
| Tune chunk size against memory + throughput | [Performance Guide](./performance.md) |
| Land columnar Parquet at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](./appendix/tideweaver_parquet_snapshots.md) |
| Get structured error logs from a chunked daemon | [Production Debugging](./debugging.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/streaming_and_pagination.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)