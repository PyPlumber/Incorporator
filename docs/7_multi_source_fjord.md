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
* **Source B:** `https://api.binance.us/api/v3/ticker/price`
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
                    "inc_url": "https://api.binance.us/api/v3/ticker/price",
                    "inc_code": "symbol",
                },
            },
        ],
        outflow="examples/fjord_code/crypto_spread.py",
        export_params={"file_path": "data/crypto_spread.ndjson"},
        refresh_interval={"CoinGecko": 60, "BinancePair": 30},   # per-source cadences
        export_interval=60.0,                                    # fused output every 60 s
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

> **Seed-empty abort:** if *any* source yields zero records on the
> initial seed, the engine aborts the whole pipeline with a
> `fjord_incorp:<ClassName>` wave whose `failed_sources` explains
> why.  No daemons spawn, the `async for` loop exits cleanly with
> code 0.  Always print `wave.failed_sources` so geo-blocks
> (`api.binance.com` is blocked in the US — use `api.binance.us`),
> rate-limit responses, and transient API outages surface visibly
> instead of looking like a successful run with empty data.

> **Refresh is on by default.**  Every fjord source automatically
> spawns a refresh daemon — you don't need `"refresh_params": {}`
> boilerplate on each entry.  To **opt OUT** of refresh on a specific
> source (e.g. a static catalogue that never changes), set
> `"refresh_params": None` on that entry.
>
> **Per-source intervals — two equivalent shapes:**
>
> ```python
> # Top-level dict by class name (one place, easy to scan):
> refresh_interval={"CoinGecko": 60, "BinancePair": 30}
>
> # OR inline per-entry (overrides the dict if both are set):
> {"cls": CoinGecko, "incorp_params": {...}, "refresh_interval": 60}
> ```
>
> The dict shape is JSON-friendly (works in `pipeline.json` too) and
> reads at a glance.  Inline overrides take priority when both are
> set on the same source.  Defaults: 60 s refresh, 300 s export, when
> nothing is specified.

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
   picks the format.  Use any append-friendly format: `.ndjson` (the
   example), `.csv`, `.sqlite`, or `.avro`.  Parquet / Feather / ORC /
   Excel / XML / JSON reject append mode and would crash a streaming
   daemon — see the format-constraint note above.  As with `stream()`,
   each tick replaces the destination file with the latest fused
   snapshot; opt into accumulation with
   `export_params={"if_exists": "append"}` when you want a forensic
   ledger.
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
      }
    },
    {
      "cls_name": "BinancePair",
      "incorp_params": {
        "inc_url": "https://api.binance.us/api/v3/ticker/price",
        "inc_code": "symbol"
      }
    }
  ],
  "export_params": {"file_path": "data/crypto_spread.ndjson"},
  "refresh_interval": {"CoinGecko": 60, "BinancePair": 30},
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

## Two Powers You'll Grow Into

The crypto-spread example above uses the simplest fjord shape:
N independent sources, one outflow function, one output file.  Two
extensions handle relational + multi-view cases.

### Power 1 — State-aware `inflow(state)`: live `link_to(...)` across sources

When one source's `conv_dict` needs a reference to another source's
already-loaded registry (e.g. resolving a foreign-key URL to the
actual Pydantic object), define a top-level `inflow(state)` callable
in `inflow.py`.  `fjord()` switches from parallel-seed to
declaration-order sequential seed, and calls `inflow(state)` before
each source's `incorp()` with the snapshots loaded so far:

```python
# swapi_inflow.py
from incorporator import link_to, link_to_list, split_and_get

get_id = split_and_get('/', -1, int)

def inflow(state):
    # On the Planet + Film seeds, state is empty / partial — be defensive.
    overrides = {}
    if "Planet" in state and "Film" in state:
        overrides["Person"] = {
            "conv_dict": {
                "homeworld": link_to(state["Planet"], extractor=get_id),
                "films":     link_to_list(state["Film"], extractor=get_id),
            }
        }
    return overrides
```

```python
async for wave in Incorporator.fjord(
    stream_params=[
        {"cls": Planet, "incorp_params": {"inc_url": ".../planets/", "inc_code": "id"}},
        {"cls": Film,   "incorp_params": {"inc_url": ".../films/",   "inc_code": "id"}},
        {"cls": Person, "incorp_params": {"inc_url": ".../people/",  "inc_code": "id"}},
    ],
    inflow="swapi_inflow.py",           # ← state-aware overrides
    outflow="swapi_outflow.py",
    export_params={"file_path": "data/people.ndjson"},
):
    print(wave)
```

`Person.homeworld` arrives as a fully-typed `Planet` object instead
of a URL string — so an outflow function can `getattr(person.homeworld,
"inc_name")` directly.

If `inflow.py` exists but defines *no* `inflow` function, fjord keeps
the legacy parallel-seed path (zero overhead) — the sidecar simply
extends the token resolver's allow-list as it always has.

### Power 2 — Multi-output: N derived classes from one outflow

Return a `dict[ClassName, list[dict]]` from `outflow(state)` and
fjord builds one derived class **per dict key** and exports each to
its own file.  One join, N analytical views:

```python
# swapi_outflow.py
def outflow(state):
    people = list(state["Person"])
    by_planet = {}
    for p in people:
        hw = getattr(p, "homeworld", None)
        hw_name = getattr(hw, "inc_name", "Unknown") if hw else "Unknown"
        by_planet.setdefault(hw_name, []).append(p.inc_name)

    return {
        "JediArchive":  [{"name": p.inc_name, "height": p.height} for p in people],
        "Demographics": [{"planet": hw, "citizens": len(c)}
                         for hw, c in by_planet.items()],
        "Filmography":  [{"name": p.inc_name, "films_count": len(p.films)}
                         for p in people],
    }
```

```python
async for wave in Incorporator.fjord(
    stream_params=[...],
    inflow="swapi_inflow.py",
    outflow="swapi_outflow.py",
    export_params={                               # one entry per output key
        "JediArchive":  {"file_path": "data/jedi.parquet"},
        "Demographics": {"file_path": "data/demographics.csv"},
        "Filmography":  {"file_path": "data/films.ndjson"},
    },
):
    print(wave)                                   # one wave per derived class per tick
```

Each derived class gets its own `_daemon_tick` wrap so a failure
building `Demographics` doesn't block `JediArchive` from exporting.
The single-output `list[dict]` return remains the legacy path — list
return = one file.

> **Power-user note:** if `outflow.py` already declares a real
> `Incorporator` subclass with a matching name, fjord uses that class
> instead of the inferred-dynamic one — full type control on derived
> classes when you want it.

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
