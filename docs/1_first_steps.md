***

# 🌱 First Steps with Incorporator: Your First `incorp()`

Welcome. This is the smallest meaningful Incorporator program — a
single API call that turns 100 coins from CoinGecko into a typed
Python object graph, indexed for O(1) lookups, with zero schema
defined by you.

By the end of this tutorial you'll know the canonical call shape, the
two kwargs every call uses (`inc_url`, `inc_code`), and the
`inc_dict` registry that makes everything else in this framework
work.

---

## The Goal

* **Endpoint:** `https://api.coingecko.com/api/v3/coins/markets`
  — CoinGecko's top-100 coins ranked by market cap. Public, no
  auth, JSON array of dicts.
* **Result:** a Python object per coin, accessible via dot-notation
  (`coin.name`, `coin.current_price`) and via the class registry
  (`Coin.inc_dict["bitcoin"]`).
* **Lines of code:** under twenty.

---

## Step 1: Define a Subclass

```python
from incorporator import Incorporator


class Coin(Incorporator):
    pass
```

That's the whole class definition. No field declarations, no type
annotations, no validators. The framework will infer the schema from
the first response payload and attach the fields to your class
dynamically.

---

## Step 2: Make the Call

```python
import asyncio


async def main():
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 100, "page": 1},
        inc_code="id",                              # primary key for inc_dict
        inc_name="name",                            # human-readable label
    )
    print(f"Loaded {len(coins)} coins.")


asyncio.run(main())
```

Two kwargs to know:

* **`inc_url`** — the endpoint. Pass a single URL, a list, or a file
  path (the framework auto-detects formats by extension).
* **`inc_code`** — which field on each record becomes the **primary
  key** in `Coin.inc_dict`. Pick something unique. CoinGecko returns
  `"id"` slugs like `"bitcoin"`, `"ethereum"` — those become the keys.

Optional but useful:

* **`inc_name`** — a human-readable label. Used by `display()` and
  some converters. CoinGecko's `"name"` field ("Bitcoin", "Ethereum")
  is the natural choice.

Everything else (`conv_dict`, `excl_lst`, `name_chg`, `inc_parent`,
`inc_page`, ...) is for the more advanced tutorials. If you're facing an
unfamiliar endpoint and don't know which kwargs you need, `test()` will
profile it and print the exact snippet to paste — that's Tutorial 3.

---

## Step 3: Use the Object Graph

```python
# Dot-notation works everywhere.
btc = coins.inc_dict["bitcoin"]
print(f"{btc.name}: ${btc.current_price:,.2f}")
print(f"Market cap rank: {btc.market_cap_rank}")
print(f"24h change: {btc.price_change_percentage_24h:.2f}%")

# Iteration works because IncorporatorList IS a list.
for coin in coins[:5]:
    print(f"#{coin.market_cap_rank}: {coin.name} ({coin.symbol.upper()})")
```

Output looks like:

```text
Bitcoin: $67,234.51
Market cap rank: 1
24h change: 2.13%
#1: Bitcoin (BTC)
#2: Ethereum (ETH)
#3: Tether (USDT)
...
```

The framework **inferred every field** from the API response — there
was no `current_price: float`, no `market_cap_rank: int` declaration.
Pydantic v2 absorbed the JSON shape, attached every key as a typed
attribute, and registered each instance in `Coin.inc_dict` under its
`inc_code`.

---

## What Just Happened

When you called `Coin.incorp(...)`:

1. **Fetch.** The framework opened an `httpx.AsyncClient` (HTTP/2
   multiplexed, connection-pooled, Tenacity-backed retries) and hit
   the URL.
2. **Parse.** The response body was decoded as JSON via the fastest
   parser available (`orjson` if you installed the `[speedups]`
   extra, stdlib `json` otherwise).
3. **Schema inference.** The framework sampled the records to merge
   every key into a unified Pydantic schema. Optional fields handle
   the case where some records omit keys.
4. **Build.** Each record became a Pydantic instance — validated,
   typed, dot-accessible. Each instance auto-registered into
   `Coin.inc_dict` under its `inc_code` (a `WeakValueDictionary` so
   the registry never leaks memory).
5. **Return.** You got back an `IncorporatorList` — a normal Python
   list with the registry, failed-source list, and class metadata
   attached.

---

## Where to Go Next

| Goal | Tutorial |
|---|---|
| Same call against `.csv`, `.parquet`, `.xlsx`, `.sqlite` | [Tutorial 2 — Universal Formats](./2_universal_formats.md) |
| Don't know what an API returns? Let the framework write your kwargs | [Tutorial 3 — DX Inspector](./3_dx_inspector.md) |
| Drill into nested API graphs (parent → child) | [Tutorial 4 — Parent-Child Drilling](./4_parent_child_drilling.md) |
| Keep this registry live (refresh prices every minute) | [Tutorial 5 — Stateful Refresh](./5_stateful_refresh.md) |
| Run as a long-lived daemon | [Tutorial 6 — Streaming Daemons](./6_streaming_daemon.md) |
| Fuse CoinGecko + Binance into a live spread (capstone) | [Tutorial 7 — Multi-Source Fjord](./7_multi_source_fjord.md) |

**Common follow-ups you might be wondering about:**

* **Pagination** — CoinGecko's `/coins/markets` accepts `page=N` for
  the next batch. To fetch every page automatically, pass a paginator:
  see [Streaming & Pagination](./streaming_and_pagination.md).
* **Type casting** — strings that look like dates / numbers can be
  coerced via `conv_dict={"last_updated": inc(datetime)}`. The DX
  Inspector (Tutorial 3) prints the exact snippet to paste.
* **Errors** — failed sources surface on `coins.failed_sources`. For
  durable error logs and DLQ retry, see
  [Production Debugging](./debugging.md).
