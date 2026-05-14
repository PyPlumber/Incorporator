***

# 🔄 Stateful Refresh: Keeping Binance Tickers Live

`incorp()` builds an object graph.  `refresh()` keeps it **synchronised
with the source** without you having to re-pass `inc_code`, `inc_url`,
or any of the other identity-mapping kwargs.

The contract: `refresh()` re-fetches and rebuilds the registry — the
same `Class.inc_dict` you read from before now points at fresh
Pydantic instances under the same primary keys. The canonical view is
always `Class.inc_dict[<key>]` — read from there after every refresh
to get the latest values. (Pydantic v2 models are validated at
construction, so the framework replaces rather than mutates; cached
local references will read stale data.)

This tutorial uses Binance's public `/api/v3/ticker/24hr` endpoint
(no auth, ~1,900 pairs in one HTTP call) — a real live-data feed
where the values move every few seconds. By the end you'll know the
three resolution modes, the identity-mapping memory that makes
`refresh()` ergonomic, and the patterns that distinguish `refresh()`
from `stream()` / `fjord()`.

---

## The Three Resolution Modes

`refresh()` chooses what to re-fetch from the shape of the `instance`
argument:

### 1. In-state — `refresh()` (no args)

Re-fetches the URL the class was loaded from. Identity mapping is
remembered from the original `incorp()` call — no need to re-pass
`inc_code` / `inc_name`. The most common mode by far.

```python
pairs = await Pair.incorp(
    inc_url="https://api.binance.com/api/v3/ticker/24hr",
    inc_code="symbol",
)
btc_before = Pair.inc_dict["BTCUSDT"].lastPrice

await asyncio.sleep(2)
await Pair.refresh()                              # no args — uses cls.inc_url

# Read the latest value via inc_dict (refresh replaces instances, so
# any local var you captured pre-refresh now points at a stale model).
btc_after = Pair.inc_dict["BTCUSDT"].lastPrice
assert btc_before != btc_after                    # Binance moved on us
```

### 2. Re-source — `refresh(new_url)`

Re-fetches the registry from a brand-new source. If the string starts
with `http` it's a URL; otherwise it's a local file path. Useful when
migrating from a v1 endpoint to v2, or swapping a heavy `24hr` endpoint
for the lighter `price` endpoint when you only need the latest price.

```python
# Repoint at the lighter "current price only" endpoint:
await Pair.refresh("https://api.binance.com/api/v3/ticker/price")
```

### 3. Targeted — `refresh(instance=[obj, obj, ...])`

Refresh a specific list of instances. Useful when your business logic
has flagged a subset stale (e.g. pairs your portfolio actually holds)
and you'd rather not refresh all 1,900.

```python
my_holdings = [Pair.inc_dict[s] for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT")]
await Pair.refresh(instance=my_holdings)
```

> **Note on targeted mode**: when a class was loaded from a *single* URL,
> the framework currently dedups the request set down to that one URL
> and re-applies the response across the full registry — the "subset"
> intent is honored at the API boundary but the actual fetch still
> covers all records. Multi-URL per-instance origin tracking is a
> framework limitation to be aware of.

---

## Identity-Mapping Memory

Every `Incorporator` subclass remembers the `inc_code` and `inc_name`
field names from its first `incorp()` call. That means `refresh()`
just works:

```python
class Pair(Incorporator):
    pass

await Pair.incorp(inc_url="...", inc_code="symbol")
await Pair.refresh()                              # still keyed by 'symbol' — no need to re-pass
```

If you want to change the key on a specific refresh tick (rare), pass
`inc_code="..."` explicitly to override.

---

## HTTP Deduplication

When a multi-URL registry is refreshed via `refresh()`, origin URLs
are deduplicated across the resolved instance set. 1,000 instances
sharing 20 source URLs trigger 20 fetches, not 1,000. That makes
in-state refresh cheap enough for nightly cron jobs, manual triggers,
and "user clicked refresh" UI flows even on six-figure registries.

---

## Refresh vs. Incorp vs. Stream

| Need | Reach for |
|---|---|
| First-time load of an API or file | `incorp()` |
| One-shot "pull the latest" on a loaded graph | `refresh()` |
| Continuous polling on a fixed cadence | `stream()` |
| Multi-source fan-out + fused outflow | `fjord()` |

`refresh()` is **stateless on cadence** — runs once when you call it.
`stream()` wraps `incorp()` + `refresh()` in a daemon with refresh
and export intervals. If you find yourself writing
`while True: await Pair.refresh(); await asyncio.sleep(60)` — switch
to `stream()` (next tutorial).

---

## Step 1: Minimal Live-Refresh Loop

```python
import asyncio
from incorporator import Incorporator


class Pair(Incorporator):
    pass


async def main():
    # 1. Initial load — fills Pair.inc_dict with ~1,900 trading pairs.
    pairs = await Pair.incorp(
        inc_url="https://api.binance.com/api/v3/ticker/24hr",
        inc_code="symbol",
    )
    btc = Pair.inc_dict["BTCUSDT"]
    print(f"BTCUSDT lastPrice before:  {btc.lastPrice}")

    # 2. Wait for the market to move.
    await asyncio.sleep(2)

    # 3. In-state refresh — same instances, latest values.
    await Pair.refresh()

    # 4. `btc` was mutated in place — your local reference reflects the new price.
    print(f"BTCUSDT lastPrice after:   {btc.lastPrice}")


if __name__ == "__main__":
    asyncio.run(main())
```

Two verbs, one shared registry, zero stale references.

---

## When `refresh()` raises

* **No instances loaded.** Calling `refresh()` before any `incorp()`
  (and without a new URL/file) returns an empty list with a warning —
  there's nothing to refresh.
* **Origin missing on a targeted instance.** Pass `instance=[obj]` to
  an obj whose `inc_url` / `inc_file` is `None` and the framework logs
  a warning and skips it rather than crashing the batch.

Transient HTTP errors are handled by the same Tenacity retry policy
`incorp()` uses; permanent failures surface via
`refreshed.failed_sources` for DLQ-style retry workflows (see the
[Production Debugging](./debugging.md) reference).

---

## See Also

* **[Tutorial 4 — Parent-Child Drilling](./4_parent_child_drilling.md)** —
  the registry-building patterns refresh keeps live.
* **[Tutorial 6 — Streaming Daemons](./6_streaming_daemon.md)** — the
  daemon form of `refresh()` on a cadence.
* **[Tutorial 7 — Multi-Source Fjord](./7_multi_source_fjord.md)** —
  combine refreshes from CoinGecko + Binance into a live spread metric.
* **[Production Debugging](./debugging.md)** — what to do when
  `failed_sources` is non-empty after a refresh.
* **[Library reference](./library_reference.md)** — full method
  signature, every kwarg.
