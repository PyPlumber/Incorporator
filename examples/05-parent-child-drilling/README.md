***

# 🚀 Tutorial 5 — Parent-Child Drilling: Backtest Data Prep

You're prepping a backtest.  Top-10 coins by market cap is a `/coins/markets` call.  The actual research signal — developer activity, sentiment, genesis date, full description — lives behind `/coins/{id}` for each of those ten.  A `for` loop would be sequential, fragile, and hit the rate limit.  `inc_parent` + `inc_child` makes it one concurrent fan-out with retry and dedup baked in.

Most REST APIs are **graphs**, not tables.  Loading one endpoint gets you the nodes; you still need a second round-trip to load each edge.  This tutorial uses the canonical crypto research pattern — load CoinGecko's top-N coins as parents, then drill each coin's `/coins/{id}` endpoint for the rich per-coin detail (full description, homepage links, developer activity, full market data, sentiment).  All in two `incorp()` calls.

**Prerequisites:** [Tutorial 1](../01-first-steps/README.md) (`incorp()`, `test()`, `inc_dict`),
[Tutorial 3](../03-universal-formats/README.md) (knows what `export()` looks like; you'll often
land child-drilled detail in a warehouse), [Tutorial 4](../04-xml-post-audit/README.md)
(POST + form payload shapes for non-GET enrichment paths).

> ## ⚠️ CoinGecko rate-limit warning — READ THIS FIRST
>
> The free public API allows roughly **5–15 calls per *minute*** (not per second).
> Incorporator's default rate limiter is **15 req/*sec* — 60× too fast**.  Without a
> safeguard you will be throttled or banned within seconds.  Two ways to opt in:
>
> 1. **Per-call kwarg (this tutorial).** The script passes
>    `requests_per_second=0.2` directly to `incorp()` so the kwarg is
>    visible to readers.  Local to each call; easiest to discover.
> 2. **Process-wide registration (production).** Call
>    `register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))`
>    once at process start; every subsequent `incorp()` / `stream()` /
>    `fjord()` against that host inherits the cap with no per-call kwarg.
>    See [Penstock surface in the API Atlas](../../docs/api_atlas.md#shared-kwargs-glossary).
>
> Total wall-clock without a key: ~50 s.  (v1.2.0 removed the older
> implicit per-host registry that paced CoinGecko / pokeapi / NHTSA
> automatically — both options above are opt-in.)
>
> **Bumping the rate with a free Demo key.**  CoinGecko's [Demo plan](https://www.coingecko.com/en/developers/dashboard)
> gives 30 req/min stable (requires email signup, no card).  Set
> `COINGECKO_DEMO_API_KEY=your_key` in your environment; the tutorial reads it
> via `os.environ.get(...)`, passes it as `headers={"x-cg-demo-api-key": ...}`,
> and bumps the throttle to 0.5 req/sec (30 req/min).  Wall-clock drops to ~20 s.

---

## The Pattern

```python
# 1. Load the parents — each market row has an `id` field (e.g. "bitcoin").
coins = await Coin.incorp(
    inc_url="https://api.coingecko.com/api/v3/coins/markets",
    params={"vs_currency": "usd", "per_page": 10, "page": 1},
    inc_code="id",
    inc_name="name",
)

# 2. Drill the per-coin detail endpoint for every parent, concurrently.
details = await CoinDetail.incorp(
    inc_url="https://api.coingecko.com/api/v3/coins/{}",   # `{}` is the ID slot
    inc_parent=coins,                                       # parent list to walk
    inc_child="id",                                         # field name on parent
    inc_code="id",
    excl_lst=["image", "tickers"],                          # heavy fields
)
```

For each `inc_parent` / `inc_child` pair, the framework:

1. Walks `coins`, extracts the child field (`id`) from every record.
2. **Deduplicates the IDs** — N parents that reference K unique children fire K requests,
   not N.  Top-10 coins by market cap are all unique, so 10 parents → 10 child requests,
   but the dedup logic is there for any parent shape with overlap (see *When dedup
   matters*, below).
3. Substitutes each unique ID into the `{}` slot of `inc_url`.
4. Fires every request **concurrently** through the same shared `httpx.AsyncClient`
   (HTTP/2 multiplexed).
5. Builds a typed instance per response and registers it under `<Cls>.inc_dict[<id>]`.

Two registries, fully populated, ready for an O(1) two-way join.

> **Two registries, manual join.**  `Coin.inc_dict` and `CoinDetail.inc_dict` are
> independent.  The join lives in your application code — `CoinDetail.inc_dict[coin.id]`
> — not inside the framework.  Each `Incorporator` subclass keeps its own registry; the
> framework gives you O(1) lookups on both sides and gets out of the way.

---

## Step 1: The Pipeline

```python
import asyncio

from incorporator import Incorporator


class Coin(Incorporator):
    """Lightweight market row from /coins/markets."""


class CoinDetail(Incorporator):
    """Full per-coin record from /coins/{id}."""


async def main():
    # Parents: top 10 by market cap.
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 10, "page": 1},
        inc_code="id",
        inc_name="name",
    )
    print(f"📥 Loaded {len(coins)} parent market rows.")

    # Concurrent child drill — one /coins/{id} per parent.
    details = await CoinDetail.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/{}",
        inc_parent=coins,
        inc_child="id",
        inc_code="id",
        excl_lst=["image", "tickers", "community_data", "developer_data"],
    )
    print(f"🔗 Drilled {len(details)} per-coin detail records.\n")

    # Application-side O(1) join over the two registries.
    print(f"{'COIN':<14} {'PRICE':>14} {'GENESIS':<12} {'HOMEPAGE'}")
    print("=" * 80)
    for coin in coins:
        detail = CoinDetail.inc_dict.get(coin.id)
        if detail is None:
            continue
        homepage = (detail.links.homepage or [""])[0] if detail.links else ""
        genesis = detail.genesis_date or "—"
        print(
            f"{coin.name:<14} "
            f"${coin.current_price:>12,.2f} "
            f"{genesis:<12} "
            f"{homepage[:38]}"
        )


if __name__ == "__main__":
    asyncio.run(main())
```

Output (real CoinGecko data, top 10):

```text
COIN                    PRICE GENESIS      HOMEPAGE
================================================================================
Bitcoin         $    67,234.51 2009-01-03   http://www.bitcoin.org
Ethereum        $     3,210.88 2015-07-30   https://www.ethereum.org/
Tether          $         1.00 —            https://tether.to/
BNB             $       582.34 —            http://www.binance.com
Solana          $       148.21 2020-03-16   https://solana.com/
...
```

**11 HTTP requests** total — one markets call, 10 concurrent child drills.  A naive
loop would have fired 10 sequential drills *after* the parent, multiplying total
latency.

---

## Why This Beats a `for` Loop

| Naive `for` loop | `inc_parent` + `inc_child` |
|---|---|
| Sequential requests; latency = N × RTT | Concurrent via `httpx.AsyncClient`; latency ≈ max RTT |
| Re-requests the same child if multiple parents share it | Auto-deduplicates parent IDs before fan-out |
| One bad endpoint crashes the whole batch | Failed sources surface in `details.failed_sources`; rest succeed |
| You write retry / backoff yourself | Tenacity-backed exponential retry baked in |
| You write the join `for` loop manually | `inc_dict` lookup is O(1), no loop needed |

---

## When Dedup Matters

Top-10 parents → 10 unique children is the easy case.  Dedup shines when parents
*overlap* — every research workflow has this pattern eventually:

```python
# Parent shape: ticker pairs from a live feed.  Many pairs share a quote asset.
class Pair(Incorporator):
    pass


pairs = await Pair.incorp(inc_url="https://api.binance.us/api/v3/ticker/price")
# pairs has ~2,000 records like {"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}, ...
# Pretend each pair record carries a `quote_id` field — e.g. "usdt", "usdc", "busd".

# Drill the canonical CoinGecko record for each unique quote asset.
quotes = await Coin.incorp(
    inc_url="https://api.coingecko.com/api/v3/coins/{}",
    inc_parent=pairs,
    inc_child="quote_id",                          # ~2,000 parents, ~5 unique quote IDs
    inc_code="id",
)
# Framework fires 5 requests, not 2,000.
```

The same dedup story applies when `inc_child` points to a **list field** — e.g. each
parent has `categories: list[str]`.  The framework flattens, dedups across all parents,
fires one request per unique child.

---

## URL Templates and the `{}` Slot

`inc_url` accepts a single `{}` placeholder that gets format-substituted with each
extracted parent value:

```python
# Single ID per parent
inc_url="https://api.example.com/users/{}/profile"

# When the parent field is already a full URL (HATEOAS pattern),
# leave inc_url empty and the framework uses the URL as-is.
inc_url=""  # implicit when inc_child="self_url"
```

For URL fragments stored as `_links.detail.href` deep in the parent schema, use a
dotted path: `inc_child="_links.detail.href"`.  The framework walks the path on each
parent and extracts the leaf string.

---

## Going Further: Historical Price Drill

The natural next backtest step is `/coins/{id}/market_chart?vs_currency=usd&days=30` —
30 days of `[timestamp, price]` pairs per coin.  That response shape (three parallel
arrays under `prices`, `market_caps`, `total_volumes`) is *one record per child URL*,
which doesn't fit the standard `inc_code` model directly.  The
[Pokédex ETL appendix](../appendix/pokeapi-etl/README.md) shows the `calc()` + nested-array
reduction pattern that solves this — and the same trick handles `market_chart` row
flattening.

---

## Where to Go Next

> 👉 **Up next: [Tutorial 6 — SpaceX Launches](../06-spacex-launches/README.md).**  The parent-child pattern you just learned on CoinGecko powers operational dashboards too.  T6 pulls SpaceX's upcoming-launches list and drills each launch concurrently for full rocket specs and launchpad coordinates — same `inc_parent` / `inc_child` shape, different vertical, unlimited API.  Lands in ~5 s and gives CoinGecko's window a long-overdue breather after T5's 11-drill burst.

| Goal | Read |
|---|---|
| Profile the parent endpoint before drilling | [Tutorial 1 — First Steps + DX Inspector](../01-first-steps/README.md) |
| Land the drilled results in a warehouse | [Tutorial 3 — Universal Formats](../03-universal-formats/README.md) |
| Apply parent-child to an ops dashboard (different vertical) | [Tutorial 6 — SpaceX Launches](../06-spacex-launches/README.md) |
| Keep both registries live with `refresh()` | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| Fuse parent + child into one composite record | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Paginated HATEOAS drill with `calc()` reductions | [Appendix — PokéAPI ETL](../appendix/pokeapi-etl/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/05-parent-child-drilling/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
