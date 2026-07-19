***

# Tutorial 7 — Stateful Refresh: Keeping Binance Tickers Live

**Prerequisites:** [Tutorial 1](../01-first-steps/README.md) (`incorp()`, `test()`, `inc_dict`),
[Tutorial 5](../05-parent-child-drilling/README.md) (two-registries mental model),
[Tutorial 6](../06-state-sports/README.md) (per-parent `inc_parent` drilling).

Your dashboard reads `Pair.inc_dict['BTCUSDT'].lastPrice` every render. You need that
value to be no more than 30 seconds stale, without rebuilding the whole registry from
scratch every refresh. That's the `refresh()` verb's job: one call, re-fetches the
source, and replaces the registry entries under the same primary keys.

`Pair(Incorporator): pass` — no field declarations, no schema file. The class absorbs
whatever Binance returns: 80-column tickers today, 82-column tickers next quarter.
That schema-free property is why a single `refresh()` keeps working across API shape
changes without a migration step.

Three resolution modes — in-state, re-source, targeted — cover every refresh shape
you'll need. By the end of this tutorial you'll know which to reach for, plus the
identity-mapping memory that makes `refresh()` ergonomic and the HTTP-dedup behaviour
that makes it cheap.

---

> **Runtime contract — bind every `incorp()` return.** Covered in depth
> in [Tutorial 1](../01-first-steps/README.md#step-3-apply-the-recommendations-with-incorp).
> For refresh, the same binding is what keeps the registry alive between
> waves: drop the strong reference and the next `refresh()` has an empty
> `inc_dict` to work with.

---

## How `refresh()` Updates the Registry

`refresh()` re-fetches the source and calls `build_instances`, which creates
**new** objects via Pydantic's `adapter.validate_python()`. Those new objects
replace the old entries in `Class.inc_dict`.

This means: **a local variable captured before the refresh still points to
the old object.** After calling `refresh()`, re-read from `Class.inc_dict`
to get the latest values.

```python
pairs = await Pair.incorp(
    inc_url="https://api.binance.us/api/v3/ticker/24hr",
    inc_code="symbol",
)

# Capturing a local variable here — this holds the OLD object.
old_ref = Pair.inc_dict["BTCUSDT"]

await asyncio.sleep(2)
await Pair.refresh()   # replaces inc_dict["BTCUSDT"] with a new instance

# old_ref.lastPrice is STALE — it still points to the pre-refresh object.
# Re-read from inc_dict to get the new value:
btc_latest = Pair.inc_dict["BTCUSDT"].lastPrice
```

The safe pattern after any `refresh()` call: **read via `Class.inc_dict[key]`**,
not via a reference you captured before the call.

---

## The Three Resolution Modes

`refresh()` chooses what to re-fetch from the shape of the `instance`
argument:

### 1. In-state — `refresh()` (no args)

Re-fetches the URL the class was loaded from. Identity mapping is
remembered from the original `incorp()` call — no need to re-pass
`inc_code` / `inc_name`. The most common mode by far.

> **Geo-block note.** `api.binance.com` returns 451 in the US, UK, and
> Singapore. The examples target the `api.binance.us` mirror (same v3
> endpoint shape, ~600 listed pairs vs ~1,900). Swap back to `.com` if
> you're outside those regions.

```python
pairs = await Pair.incorp(
    inc_url="https://api.binance.us/api/v3/ticker/24hr",
    inc_code="symbol",
)
btc_before = Pair.inc_dict["BTCUSDT"].lastPrice

await asyncio.sleep(2)
await Pair.refresh()                              # no args — uses cls.inc_url

# Re-read via inc_dict to get the refreshed instance:
btc_after = Pair.inc_dict["BTCUSDT"].lastPrice
assert btc_before != btc_after                    # Binance moved on us
```

### 2. Re-source — `refresh(instance="new_url")`

Re-fetches the registry from a brand-new source. If the string starts
with `http` it's a URL; otherwise it's a local file path. Useful when
migrating from a v1 endpoint to v2, or swapping a heavy `24hr` endpoint
for the lighter `price` endpoint when you only need the latest price.

```python
# Repoint at the lighter "current price only" endpoint:
await Pair.refresh("https://api.binance.us/api/v3/ticker/price")
```

Subsequent `refresh()` calls with no args will use the new URL — the
class's stored `inc_url` is updated on re-source.

### 3. Targeted — `refresh(instance=[obj, obj, ...])`

Refresh a specific list of instances. Useful when your business logic
has flagged a subset stale (e.g. pairs your portfolio actually holds)
and you'd rather not refresh all 1,900.

```python
my_holdings = [Pair.inc_dict[s] for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT")]
await Pair.refresh(instance=my_holdings)
# After refresh, re-read from inc_dict:
btc = Pair.inc_dict["BTCUSDT"].lastPrice
```

> **Note on targeted mode**: when a class was loaded from a *single* URL,
> the framework currently dedups the request set down to that one URL
> and re-applies the response across the full registry — the "subset"
> intent is honored at the API boundary but the actual fetch still
> covers all records. Multi-URL per-instance origin tracking is a
> framework limitation to be aware of.

---

## Identity-Mapping Memory

**Call `refresh()` with no arguments and the framework re-fetches
with the exact same URL, query params, headers, and converters you
declared on `incorp()`** — no boilerplate, no re-passing. The class
silently remembers its first call-context (`inc_code`, `inc_name`,
`params`, `headers`, `rec_path`, `conv_dict`, `excl_lst`, `name_chg`,
`payload_list`, `sql_query`, `parquet_decimal_columns`, …) and merges
it under whatever you supply to `refresh()`. Concretely:

```python
class Pair(Incorporator):
    pass

await Pair.incorp(
    inc_url="https://api.coingecko.com/api/v3/coins/markets",
    params={"vs_currency": "usd", "per_page": 100, "page": 1},   # required!
    headers={"X-Custom": "..."},
    rec_path="results",
    conv_dict={"price": inc(float)},
    inc_code="id",
)

await Pair.refresh()    # replays params + headers + rec_path + conv_dict
```

Without this auto-replay, the refresh would hit the bare
`/coins/markets` URL with no `?vs_currency=usd` and CoinGecko would
return a 422. The framework persists the context as
`Pair._incorp_kwargs` and merges it under your explicit refresh
kwargs.

**Caller-supplied kwargs always win on key conflict.** Pass any kwarg
explicitly to `refresh()` and it overrides the stored value for that
call only:

```python
await Pair.refresh(params={"vs_currency": "eur"})   # one-off override
```

This includes `inc_page=` — useful when you want to re-source with
pagination in a single refresh call without restructuring the original
`incorp()`:

```python
await Pair.refresh(inc_page=PageNumberPaginator(page_param="page"))
```

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

The same `refresh()` verb that runs here in a single-source loop
becomes one current inside a Tideweaver window when you need N sources
on independent cadences — the primitives do not change.

---

## Step 1: Minimal Live-Refresh Loop

```python
import asyncio
from incorporator import Incorporator


class Pair(Incorporator):
    pass


async def main():
    # 1. Initial load — fills Pair.inc_dict (~600 pairs on .us, ~1,900 on .com).
    pairs = await Pair.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr",
        inc_code="symbol",
    )
    price_before = Pair.inc_dict["BTCUSDT"].lastPrice
    print(f"BTCUSDT lastPrice before:  {price_before}")

    # 2. Wait for the market to move.
    await asyncio.sleep(2)

    # 3. In-state refresh — replays the original incorp()'s URL,
    #    inc_code, conv_dict (none here), and any headers/params.
    await Pair.refresh()

    # 4. Re-read via inc_dict — refresh() replaces registry entries with
    #    new instances. Re-reading here gives you the current values.
    price_after = Pair.inc_dict["BTCUSDT"].lastPrice
    print(f"BTCUSDT lastPrice after:   {price_after}")


if __name__ == "__main__":
    asyncio.run(main())
```

Two verbs, one shared registry, one read pattern: always re-read via `Class.inc_dict[key]` after refresh.

---

## When `refresh()` raises

* **No instances loaded.** Calling `refresh()` before any `incorp()`
  (and without a new URL/file) returns an empty list with a warning —
  there's nothing to refresh.
* **Origin missing on a targeted instance.** Pass `instance=[obj]` to
  an obj whose `inc_url` / `inc_file` is `None` and the framework logs
  a warning and skips it rather than crashing the batch.

Transient HTTP errors are handled by the same phase-aware retry policy
`incorp()` uses; permanent failures surface via
`refreshed.failed_sources` (a flat list of URL strings) or the richer
`refreshed.rejects` (a `list[RejectEntry]`). Each `RejectEntry` carries
`error_kind`, `is_url_traffic_error` (bool — `True` for HTTP/network
failures, `False` for parse/schema failures), and `retry_after`:

```python
refreshed = await Pair.refresh()
for entry in refreshed.rejects:
    origin = "API" if entry.is_url_traffic_error else "parse"
    print(f"[{origin}] {entry.error_kind}: {entry.source}")
```

See the [Production Debugging](../../docs/debugging.md) reference for
structured retry orchestration via `RejectEntry`.

---

## Run it

```bash
python examples/07-stateful-refresh/stateful_refresh.py
```

There's no CLI form for `refresh()` — the tutorial's verb has no `incorporator`
subcommand. For the long-running polling case specifically, Tutorial 8's
`stream` daemon and `cli-templates/daemon-mode.json` cover it from the CLI;
the three `refresh()` shapes shown here stay in Python.

---

## Where to Go Next

> **Up next: [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md).** T7 ran `refresh()` manually three ways; T8 wraps it in a long-lived daemon and introduces the `stateful_polling` choice: `True` keeps doing what this tutorial did — one live registry, refreshed every N seconds (mark-to-market dashboard / portfolio NAV / slow-indicator pattern); `False` (the default) turns `stream()` into a paginator-driven ingestion loop for bulk data that doesn't fit in memory (historical backfills, warehouse seeds, multi-page pulls). Same verb, two engines — T8 walks both back-to-back with a decision matrix at the close.

| Goal | Read |
|---|---|
| Discover an unfamiliar endpoint first | [Tutorial 1 — First Steps + DX Inspector](../01-first-steps/README.md) |
| Drill parent records before refreshing | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Wrap `refresh()` in a daemon with periodic export | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Refresh multiple sources concurrently and fuse the results | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Orchestrate N sources on independent cadences in one window | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| Diagnose refresh failures with structured logs | [Production Debugging](../../docs/debugging.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/07-stateful-refresh/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
