# CLI & Pipeline Configuration

The Incorporator CLI transforms the framework from a lightweight micro-client into an **Autonomous Pipeline Daemon**. 

By defining your extraction, enrichment, and loading rules in a simple JSON file, you can run infinite streams in the background—perfect for Docker containers, systemd services, or live data scraping.

## 1. Prerequisites
To use the CLI, you must install Incorporator with the Orchestration upgrades:
```bash
pip install "incorporator[orchestrate]"
```

---

## 2. The Configuration File (`pipeline.json`)

The CLI is driven entirely by a JSON configuration file. This file maps directly to the "Holy Trinity" API parameters (`incorp`, `refresh`, and `export`) and allows you to toggle the execution engine.

Here is a standard `pipeline.json` designed to scrape an API, enrich it, and append it to a local CSV file:

```json
{
  "stateful_polling": false,
  "incorp_params": {
    "inc_url": "https://jsonplaceholder.typicode.com/posts",
    "inc_code": "id",
    "excl_lst": ["userId"]
  },
  "refresh_params": {
    "http_method": "GET",
    "delay_between_batches": 1.0
  },
  "export_params": {
    "file_path": "data/output.csv",
    "if_exists": "append"
  }
}
```

### Parameter Breakdown:
*   **`stateful_polling` (Optional, defaults to `false`):** Toggles the core execution engine (see *Dual-Engine Architecture* below).
*   **`incorp_params` (Required):** Dictates the initial extraction. Must contain an origin (e.g., `inc_url`, `inc_file`, or `inc_page`).
*   **`refresh_params` (Optional):** If provided, the daemon will use the objects mapped during extraction to execute stateful updates (e.g., fetching deep relational data).
*   **`export_params` (Optional):** Dictates the load phase. To stream continuously without overwriting previous chunks, use `"if_exists": "append"`.

---

## 3. The Dual-Engine Architecture

Incorporator v2.0 features two distinct execution pipelines, controlled entirely by the `"stateful_polling"` JSON flag.

### Engine 1: Big Data Chunking (`"stateful_polling": false`)
*   **Best for:** 10M+ row databases, massive CSV files, or heavily paginated APIs.
*   **Behavior:** It strictly enforces O(1) Memory limits. It fetches a single chunk, enriches it, saves it to disk, and **completely flushes the RAM** before fetching the next chunk. 

### Engine 2: Live Stateful Polling (`"stateful_polling": true`)
*   **Best for:** Live Dashboards, 100 Stock Tickers, or 50 IoT Sensors.
*   **Behavior:** It runs extraction (`incorp`) **exactly once** to build the object graph in RAM. On subsequent polling intervals, it skips extraction and simply `refresh()`es the existing objects in memory, making it incredibly fast and network-efficient for small datasets.

---

## 4. Running the CLI

Once your `pipeline.json` is ready, execute it from the terminal using the `incorporator stream` command.

### Standard Execution (Single Pass)
To run the pipeline exactly once and exit:
```bash
incorporator stream pipeline.json
```
**Output:**
```text
🚀 Starting Incorporator Stream...
Chunk 1 | 10000 rows | 1.84s
Chunk 2 | 10000 rows | 1.91s
🛑 Stream process completed gracefully.
```

### Daemon Execution (Infinite Polling)
To keep the pipeline alive in the background, use the `--poll` flag. This tells the orchestrator to wait `X` seconds after a successful run before automatically restarting the extraction/hydration cycle.
```bash
incorporator stream pipeline.json --poll 60.0
```

---

## 5. Observability & Telemetry (`--logs`)

When running Incorporator as a background daemon (especially inside a Docker container), you need robust observability without blocking the async event loop.

By appending the `--logs` flag, you activate Incorporator's **Multiplex Disk Logging**:
```bash
incorporator stream pipeline.json --poll 3600.0 --logs
```

### What happens when `--logs` is enabled?
Terminal output is suppressed, and telemetry is routed to non-blocking background OS threads. Incorporator will automatically create a `logs/` directory and generate three rotating JSON Lines files (max 15MB each):

1.  **`logs/{Class}_api.log`**: Tracks all successful HTTP traffic, rate limits, and chunk throughput.
2.  **`logs/{Class}_error.log`**: The Dead Letter Queue (DLQ). Catches network timeouts, 400/500 status codes, and malformed data schemas.
3.  **`logs/{Class}_debug.log`**: Deep framework execution traces for local troubleshooting.

---

## 6. The `fjord` Subcommand — Multi-Source Stateful Pipelines

While `incorporator stream` operates on **one** source per pipeline, the
`incorporator fjord` subcommand drives **multiple sources concurrently** and
joins them into a brand-new combined output class via a Python `combine()`
function you supply.

Use fjord when you need to keep a live, joined object map synchronised
across N APIs (e.g. crypto spot price + futures price → combined market
spread).

### Configuration File (`fjord.json`)

Unlike `stream`, fjord needs Python references that can't live in JSON. So
the config points to a `code_file` — a single `.py` containing your
`Incorporator` subclasses **and** a top-level `combine(state)` function.
The CLI imports that file at startup, resolves the class names by
`getattr`, and validates each resolved object is an `Incorporator`
subclass.

```json
{
  "code_file": "my_pipeline.py",
  "output_class": "CoinMarket",
  "stream_params": [
    {
      "cls_name": "Coin",
      "incorp_params": {
        "inc_url": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
        "inc_code": "id"
      },
      "refresh_params": {}
    },
    {
      "cls_name": "BinanceFutures",
      "incorp_params": {
        "inc_url": "https://fapi.binance.com/fapi/v1/ticker/price",
        "inc_code": "symbol"
      },
      "refresh_params": {}
    }
  ],
  "export_params": {"file_path": "markets.ndjson"},
  "refresh_interval": 60,
  "export_interval": 300
}
```

### Parameter Breakdown

| Field | Required | Description |
| :--- | :--- | :--- |
| `code_file` | ✅ | Path to a `.py` file containing Incorporator subclasses and a `combine(state)` function. Resolved relative to the JSON config's directory. |
| `output_class` | ✅ | Name of the **combined output** Incorporator subclass declared in `code_file`. Instances built from `combine()` rows land in this class's `inc_dict`. |
| `stream_params` | ✅ | List of per-source dicts. Each must declare `cls_name` (string matching a subclass in `code_file`) and `incorp_params`. Optional: `refresh_params`, `export_params` (per-source export). |
| `export_params` | ✅ | Destination for the combined output graph. |
| `refresh_interval` | ⬜ | Cadence (seconds) for per-source refresh daemons. Each entry can override. |
| `export_interval` | ⬜ | Cadence (seconds) for the combine-and-export tick. |

### The `combine()` Function

Lives in the same `code_file` as the classes. Receives `state` — a dict
keyed by source-class name, valued with that source's `IncorporatorList`.
Returns a list of dicts; fjord builds `OutputClass(**row)` instances and
exports them.

```python
# my_pipeline.py
from incorporator import Incorporator

class Coin(Incorporator): pass
class BinanceFutures(Incorporator): pass
class CoinMarket(Incorporator):
    coin_name: str = ""
    spot_price: float = 0.0
    futures_price: float = 0.0
    spread: float = 0.0

def combine(state):
    coins = state["Coin"]
    futures = state["BinanceFutures"]
    rows = []
    for c in coins:
        f = futures.inc_dict.get(c.inc_code)
        if not f:
            continue
        rows.append({
            "inc_code":      c.inc_code,
            "coin_name":     getattr(c, "name", ""),
            "spot_price":    getattr(c, "current_price", 0.0),
            "futures_price": getattr(f, "price", 0.0),
            "spread":        getattr(f, "price", 0.0) - getattr(c, "current_price", 0.0),
        })
    return rows
```

### Running the Daemon

```bash
incorporator fjord fjord.json --logs
```

There is no `--poll` flag — fjord is **stateful-polling only** by design.
Cadence is driven by `refresh_interval` and `export_interval` inside the
JSON config.

### When to Reach For `fjord` vs `stream`

| Need | Use |
| :--- | :--- |
| Stream **one** source through chunked/stateful polling | `incorporator stream` |
| Concurrently poll **N sources** and join them into a new entity | `incorporator fjord` |
| Sequencing / DAG dependencies between pipelines | Wrap either in a Prefect flow (see `deployment.md`) |

For the full method-level signature of `fjord()`, see the pdoc-built
[API reference](./api_reference.md).