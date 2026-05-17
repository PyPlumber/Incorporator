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
profile it and print the exact snippet to paste — see the
*Profiling Unknown APIs with `test()`* section below.

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

## Profiling Unknown APIs with `test()`

The walkthrough above assumes you already know `inc_code="id"` and
`inc_name="name"`. For an *unfamiliar* endpoint — third-party docs are
sparse, you've never seen the payload — Incorporator ships a JIT API
profiler: **`test()`**. Same call shape as `incorp()`, but it fetches a
single safe page and prints a five-section structured report with the
exact kwargs you should paste.

### Run the inspector

```python
import asyncio
from incorporator import Incorporator


class Coin(Incorporator):
    pass


asyncio.run(Coin.test(inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"))
```

That's the whole invocation. **Swap `.incorp()` for `.test()`** to
trigger the inspector.

### Read the report

```text
======================================================================
🕵️‍♂️  INCORPORATOR DX INSPECTOR
======================================================================

📦 1. PAYLOAD STRUCTURE:
   ├── id: str = bitcoin
   ├── name: str = Bitcoin
   ├── current_price: float = 67234.51
   └── last_updated: str = 2026-05-14T12:00:00.000Z

🔑 2. IDENTITY MAPPING:
   ✅ inc_code='id'
   ✅ inc_name='name'

🛠️  3. ETL / TYPE CASTING SUGGESTIONS:
   conv_dict={'last_updated': inc(datetime), 'ath_date': inc(datetime)}

📑 4. PAGINATION HINTS:
   (Skipped — no pagination metadata.)

🗑️  5. HEAVY-FIELD HINTS:
   excl_lst=['image']
======================================================================
```

Each section is actionable:

1. **Payload structure** — tree-view of every key with types and sample values; warns when the
   root shape suggests a `rec_path=` wrapper.
2. **Identity mapping** — regex-scored candidates for `inc_code` (UUIDs, integer IDs, slugs) and
   `inc_name` (display strings).
3. **Type casting** — routes detection through the framework's own `parses_as_datetime` /
   `parses_as_int` / `parses_as_float` predicates; every suggestion is structurally what `inc()`
   accepts at runtime.
4. **Pagination hints** — detects `next` / `cursor` / `offset+limit` shapes.
5. **Heavy-field hints** — flags asset URLs, base64 blobs, oversized strings → `excl_lst`.

### Turn the report into a real call

Copy the suggestions verbatim. Same class, swap `.test()` for `.incorp()`, paste the kwargs:

```python
from datetime import datetime
from incorporator.schema.converters import inc

coins = await Coin.incorp(
    inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
    inc_code="id",
    inc_name="name",
    conv_dict={"last_updated": inc(datetime), "ath_date": inc(datetime)},
    excl_lst=["image"],
)
```

You went from *"what does this API look like?"* to a fully-typed, indexed, datetime-aware object
graph **without writing or reading a schema**.

### Safety guarantees

* **Single page only.** When you pass a paginator, `test()` forces `call_lim=1`.
* **Short timeout.** Defaults to `timeout=5.0` so an unresponsive endpoint fails fast.
* **Result preview cap.** Returns at most 3 records; the return value is a real
  `IncorporatorList` so `sample[0].whatever` works for poking at the shape.
* **Error analysis on failure.** If the fetch raises, `test()` routes the exception through the
  same inspector module to suggest diagnostics (auth headers missing, wrong content type, etc.).

> **Drill-down:** when the inspector finds nested list-of-dicts inside the top-level record (e.g.
> SpaceX `/launches/latest` has `cores: [...]`), it surfaces a copy-pasteable
> `rec_path='cores'` hint for re-running `test()` against that nested level.

---

## Where to Go Next

| Goal | Tutorial |
|---|---|
| Same call against `.csv`, `.parquet`, `.xlsx`, `.sqlite` | [Tutorial 2 — Universal Formats](./2_universal_formats.md) |
| Drill into nested API graphs (parent → child) | [Tutorial 3 — Parent-Child Drilling](./3_parent_child_drilling.md) |
| Keep this registry live (refresh prices every minute) | [Tutorial 4 — Stateful Refresh](./4_stateful_refresh.md) |
| Run as a long-lived daemon | [Tutorial 5 — Streaming Daemons](./5_streaming_daemon.md) |
| Fuse CoinGecko + Binance into a live spread | [Tutorial 6 — Multi-Source Fjord](./6_multi_source_fjord.md) |
| Orchestrate multi-source pipelines in a windowed graph (capstone) | [Tutorial 7 — Tideweaver](./7_tideweaver.md) |

**Common follow-ups you might be wondering about:**

* **Pagination** — CoinGecko's `/coins/markets` accepts `page=N` for the next batch. To fetch
  every page automatically, pass a paginator: see [Streaming & Pagination](./streaming_and_pagination.md).
* **Type casting** — strings that look like dates / numbers can be coerced via
  `conv_dict={"last_updated": inc(datetime)}`. The inspector above prints the exact snippet to paste.
* **Errors** — failed sources surface on `coins.failed_sources`. For durable error logs and DLQ
  retry, see [Production Debugging](./debugging.md).
