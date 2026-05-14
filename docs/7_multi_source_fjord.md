***

# 🌊 Multi-Source Fjord: Live Crypto Spread (Capstone)

`stream()` watches **one** source. `fjord()` watches **N** sources
concurrently and lets you fuse them through a user-defined
`outflow(state)` function — the engine handles every concurrent
refresh, every export tick, the shared lock, the wave queue, and the
dynamic output class.

This is the capstone of the curriculum: you've already loaded a
CoinGecko coin catalogue (tutorial 1), kept a Binance ticker registry
live (tutorial 5), and seen the pattern of refreshing two registries
in parallel. Now you'll **fuse them** — compute a basis-point spread
between CoinGecko's USD price and Binance's USDT price for every
overlapping symbol, on a 60-second cadence, with each source refreshing
independently every 30 seconds.

---

## The Goal

* **Source A:** `https://api.coingecko.com/api/v3/coins/markets`
  (USD prices, top 100 by market cap)
* **Source B:** `https://api.binance.com/api/v3/ticker/price`
  (USDT prices for every trading pair)
* **Fusion:** for each CoinGecko coin where a matching `{SYMBOL}USDT`
  exists in Binance, emit a row with both prices + the basis-point spread
* **Cadence:** sources refresh every 30 s; fused output writes every 60 s
* **Output:** `data/crypto_spread.ndjson` — append-friendly columnar format

Notice: no output class is declared. `fjord()` builds it dynamically
from the rows your `outflow()` returns, named after the code-file
stem (`crypto_spread.py` → `CryptoSpread`).

---

## Step 1: `crypto_spread.py` — The Outflow Sidecar

`fjord()` needs Python code (class definitions + the join logic), so
it lives in a sidecar file:

```python
# examples/fjord_code/crypto_spread.py
from datetime import datetime, timezone
from typing import Any, Dict, List

from incorporator import Incorporator


class CoinGecko(Incorporator):
    """Source A — CoinGecko USD market prices."""


class BinancePair(Incorporator):
    """Source B — Binance USDT-quoted prices."""


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Join CoinGecko USD vs Binance USDT for overlapping symbols."""
    coins = state["CoinGecko"] or []
    pairs = state["BinancePair"]
    if pairs is None:
        return []

    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for coin in coins:
        symbol = getattr(coin, "symbol", "").upper()
        if not symbol:
            continue

        pair = pairs.inc_dict.get(f"{symbol}USDT")
        if pair is None:
            continue                                      # not traded on Binance

        gecko_usd = float(getattr(coin, "current_price", 0) or 0)
        binance_usdt = float(getattr(pair, "price", 0) or 0)
        if gecko_usd <= 0 or binance_usdt <= 0:
            continue

        spread_bps = round(((binance_usdt - gecko_usd) / gecko_usd) * 10_000, 2)

        rows.append({
            "symbol": symbol,
            "coingecko_usd": gecko_usd,
            "binance_usdt": binance_usdt,
            "spread_bps": spread_bps,
            "fused_at": now,
        })

    return rows
```

Two source classes + one function. No daemon plumbing, no lock
acquisition, no wave emission — `fjord()` handles all of it.

---

## Step 2: The Pipeline

```python
import asyncio
from incorporator import Incorporator

# Bring the classes into scope so fjord() can register them.
from examples.fjord_code.crypto_spread import BinancePair, CoinGecko


async def main():
    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": CoinGecko,
                "incorp_params": {
                    "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
                    "params": {"vs_currency": "usd", "per_page": 100, "page": 1},
                    "inc_code": "id",
                },
            },
            {
                "cls": BinancePair,
                "incorp_params": {
                    "inc_url": "https://api.binance.com/api/v3/ticker/price",
                    "inc_code": "symbol",
                },
            },
        ],
        outflow="examples/fjord_code/crypto_spread.py",
        export_params={"file_path": "data/crypto_spread.ndjson"},
        refresh_interval=30.0,                              # each source re-fetches every 30 s
        export_interval=60.0,                               # fused output writes every 60 s
    ):
        op = wave.operation
        print(f"{op:40s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
```

---

> **Format constraint** *(same as `stream()`)*: fjord writes
> incrementally on every export tick, so the export target must be an
> **append-friendly** format: `.ndjson` / `.csv` / `.sqlite` / `.avro`.
> Parquet / Feather / ORC / Excel / XML / JSON all reject append mode.
> Pick NDJSON if unsure.

---

## What `fjord()` is Doing Under the Hood

1. **Concurrent seed.** All `stream_params[*].cls.incorp(...)` calls
   run in parallel via `asyncio.gather`. One wave per source.
2. **Per-source refresh daemons.** One daemon per entry. Each
   independently re-fetches on its own `refresh_interval` (override
   per entry — CoinGecko's free tier is rate-limited while Binance is
   not, so you may want different cadences).
3. **One outflow daemon.** Every `export_interval`, it snapshots every
   source under the shared lock, releases the lock, then calls your
   `outflow(state)` *in a worker thread* (via `asyncio.to_thread`) so a
   heavy CPU join doesn't block the refresh daemons.
4. **Dynamic output class.** From the rows `outflow()` returns, the
   engine uses `infer_dynamic_schema()` to build a Pydantic class
   named after the `crypto_spread.py` stem — `CryptoSpread`. The
   instances auto-register in `CryptoSpread.inc_dict` for downstream
   `link_to(...)` use if you want to keep fused history in memory.
5. **Export.** Same handler dispatch as `stream()` — file extension
   picks the format (Parquet here, but switch to `.ndjson`, `.csv`,
   `.sqlite`, `.avro`, etc., for free). Parquet appends per row group;
   NDJSON is the streaming-native choice if you'd rather not deal with
   columnar batching.
6. **Shutdown.** SIGTERM / Ctrl+C cancels every task; the wave queue
   drains; the `async for` loop exits.

---

## 🐳 Run It From the CLI

The same pipeline as a `pipeline.json`:

```json
{
  "outflow": "examples/fjord_code/crypto_spread.py",
  "stream_params": [
    {
      "cls_name": "CoinGecko",
      "incorp_params": {
        "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
        "params": {"vs_currency": "usd", "per_page": 100, "page": 1},
        "inc_code": "id"
      },
      "refresh_params": {}
    },
    {
      "cls_name": "BinancePair",
      "incorp_params": {
        "inc_url": "https://api.binance.com/api/v3/ticker/price",
        "inc_code": "symbol"
      },
      "refresh_params": {}
    }
  ],
  "export_params": {"file_path": "data/crypto_spread.ndjson"},
  "refresh_interval": 30.0,
  "export_interval": 60.0
}
```

```bash
incorporator validate pipeline.json
incorporator fjord pipeline.json --logs
```

The JSON uses `cls_name` (string) while the Python uses `cls` (class
reference). The CLI loader resolves `cls_name` by importing the
outflow file and looking up the class by name — that's how the JSON
stays serialisable.

---

## When Fjord Shines

| Scenario | Why fjord wins |
|---|---|
| Joining two REST APIs that update at different rates | Independent per-source refresh cadences |
| Computing a derived dataset live (price spreads, latency joins, etc.) | `outflow()` runs CPU-heavy joins off the event loop |
| Needing a strong-typed output class without declaring one | `infer_dynamic_schema()` builds it from the rows |
| Production observability across a fan-out pipeline | One `Wave` per source per tick + per outflow tick — pipe to disk via `enable_logging=True` |

---

## Secondary Example — Non-Financial Domain

If you'd rather see the fjord pattern applied to a different domain,
the SpaceX launch + rocket fusion is available as a reference:

* **`examples/fjord_code/launch_with_rocket.py`** — joins
  `/v4/launches/latest` with `/v4/rockets` so the latest launch row
  carries the matching rocket's name, height, mass, and success-rate
  percentage.

The pattern is identical: two source classes, one `outflow(state)`
function, the dynamic output class comes from the filename stem.

---

## See Also

* **[Tutorial 5 — Stateful Refresh](./5_stateful_refresh.md)** — the
  refresh contract that fjord wraps in a daemon, one source at a time.
* **[Tutorial 6 — Streaming Daemons](./6_streaming_daemon.md)** —
  single-source equivalent of fjord; reach for it when you only need
  one feed.
* **[CLI & Configuration Guide](./cli_and_configuration.md)** — the
  full `pipeline.json` schema for fjord pipelines.
* **[Library reference](./library_reference.md)** — full method
  signature for `fjord()`.
