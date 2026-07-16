***

> 📎 **Appendix — Static graph join.** `link_to`-based one-shot
> in-memory join across CoinGecko + Binance. See
> [Tutorial 10 — Multi-Source Fjord](../../10-multi-source-fjord/README.md) for
> the same fusion as a live daemon; reach for this appendix when
> you want the static pattern without the daemon scaffolding.

***

# 🕸️ Multi-Graph Mapping: The Unified Trading Dashboard

You don't need a daemon. You want T10's cross-venue join done **once**, the results printed (or exported), then the process exits. `link_to(...)` builds the in-memory join across two CoinGecko + Binance endpoints in one async-for-free call — the same `link_to` you'd use inside a fjord's `outflow(state)`, but consumed at the call site instead of by the engine.

In the real world, no single API has all the data you need:

* **CoinGecko** is fantastic for global market caps and circulating supply, but lacks deep, real-time exchange liquidity metrics.
* **Binance** has real-time order book bids and asks, but their API is fragmented across hundreds of individual trading pairs (USDT, USDC, etc.).

Try to merge these with standard Python `for` loops and you'll hit **429 Too Many Requests** instantly. This appendix pulls data from **three different endpoints**, merges them into a single Python object graph, and maps 100 assets to 400 exchange markets using exactly **3 API calls**.

---

## 🎯 The Goal

Build a Unified Stablecoin Liquidity Dashboard. For the top 100 cryptocurrencies, see their Global Price, their Binance **USDT** volume/bids, and their Binance **USDC** volume/bids side-by-side.

## 💻 The Complete Code

```python
import asyncio
from incorporator import Incorporator, link_to, register_host_penstock
from incorporator.schema.converters import calc

# Pace api.coingecko.com at 0.2 req/sec (12/min — under the 5-15/min
# free-tier ceiling).
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# ==========================================
# 1. DECLARATIVE ETL FACTORY
# ==========================================
def make_linker(quote_currency: str):
    """
    A factory function that returns a custom linker for a specific stablecoin.
    e.g., passing "USDC" returns a function that synthesizes "BTCUSDC".
    """
    def linker(symbol_str: str) -> str:
        if symbol_str:
            return f"{symbol_str.upper()}{quote_currency}"
        return None
    return linker

# ==========================================
# 2. DEFINE OUR THREE API CLASSES
# ==========================================
class BinanceStat(Incorporator): pass
class BinanceBook(Incorporator): pass
class CryptoAsset(Incorporator): pass

async def main() -> None:
    # 1. Fetch the Target Registries (Execute BEFORE linking!)
    binance_stats = await BinanceStat.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr", 
        inc_code="symbol",
        excl_lst=["priceChangePercent", "weightedAvgPrice", "openPrice", "prevClosePrice"]
    )
    
    binance_books = await BinanceBook.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/bookTicker", 
        inc_code="symbol"
    )

    # 2. Fetch CoinGecko and Fuse the Graph!
    assets = await CryptoAsset.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1",
        inc_code="id",
        inc_name="name",
        conv_dict={
            # We use our Factory to generate 4 parallel mapping routes simultaneously!
            "stats_usdt": calc(link_to(binance_stats, extractor=make_linker("USDT")), "symbol"),
            "book_usdt":  calc(link_to(binance_books, extractor=make_linker("USDT")), "symbol"),
            
            "stats_usdc": calc(link_to(binance_stats, extractor=make_linker("USDC")), "symbol"),
            "book_usdc":  calc(link_to(binance_books, extractor=make_linker("USDC")), "symbol"),
        }
    )

    # 3. Sort by Market Cap Rank Ascending
    assets.sort(key=lambda a: getattr(a, "market_cap_rank", 0))

    # 4. Traverse the Unified Graph
    def extract_market_data(stats_obj, book_obj):
        """Helper to safely extract data if the Binance link was successful."""
        if stats_obj and book_obj:
            vol_str = getattr(stats_obj, "quoteVolume", "0")
            bid_str = getattr(book_obj, "bidPrice", "0")
            return f"${float(vol_str):,.0f}", f"${float(bid_str):,.2f}"
        return "N/A", "N/A"

    for asset in assets[:15]: # Print Top 15 for brevity
        symbol = str(getattr(asset, "symbol", "")).upper()
        global_price = f"${getattr(asset, 'current_price', 0):,.2f}"

        # Safely traverse the 4 linked Binance objects!
        vol_usdt, bid_usdt = extract_market_data(getattr(asset, "stats_usdt", None), getattr(asset, "book_usdt", None))
        vol_usdc, bid_usdc = extract_market_data(getattr(asset, "stats_usdc", None), getattr(asset, "book_usdc", None))

        print(f"{symbol:<8} | {global_price:<12} | USDT Vol: {vol_usdt:<15} | USDC Vol: {vol_usdc}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🧠 Architecture Deep Dive: How it Works

### 1. Zero-Network Graph Mapping (Immunity to 429 Errors)
A naïve `httpx` port — 100 CoinGecko assets × 4 Binance endpoints — needs **400 API requests** and earns an IP ban in seconds.

Look at the execution order. **Incorporator makes 3 API calls total:**
1. The entire global Binance Stats registry (1 call).
2. The entire global Binance Order Book registry (1 call).
3. The 100 CoinGecko assets (1 call).

When it hits the `link_to` configuration, **it disconnects from the network.** It synthesizes the target string (e.g., `"BTCUSDT"`) and searches Incorporator's internal RAM registry (`inc_dict`). All 400 mappings execute as `O(1)` dict lookups, completely bypassing server rate limits.

The single live source that *does* hit the network — CoinGecko — is paced by a `register_host_penstock("api.coingecko.com", rate_per_sec=0.2)` registered up top, keeping it under the 5-15/min free-tier ceiling. The penstock guards the one real call; `link_to` makes the other 400 mappings free.

> **Strong-ref note.** `inc_dict` is a `WeakValueDictionary` ([T1's runtime contract](../../01-first-steps/README.md#step-3-apply-the-recommendations-with-incorp) has the canonical lifecycle treatment). As long as `binance_stats` and `binance_books` are held in `main()`'s local scope (they are — by the `await` returns), every record stays resident and `link_to` resolves cleanly. Drop those references and the registries can be garbage-collected mid-traversal.

### 2. The Factory Closure Pattern
Instead of four separate `lambda`s, one Factory function:
```python
def make_linker(quote_currency: str):
```
CoinGecko gives us `"btc"`; Binance expects `"BTCUSDT"`. The factory generates a closure that uppercases and appends the specific stablecoin suffix. The `conv_dict` stays perfectly declarative.

### 3. Native Null-Safety (Sparse Data Handling)
In crypto, highly liquid coins trade against everything — but newer tokens might only have a `USDT` book and no `USDC` book yet.

`link_to` is natively null-safe: if it searches the registry for `NEWCOINUSDC` and fails, it attaches `None` to `asset.stats_usdc`. It **never** raises `AttributeError`. You use `getattr(..., None)` in your print loop and display `"N/A"`.

### 4. Database-Like Querying (`.sort()`)
Because Incorporator infers the schema and transforms raw JSON into Python objects *during* ingestion, the final `assets` list behaves like a clean database result:
```python
assets.sort(key=lambda a: getattr(a, "market_cap_rank", 0))
```
No nightmare dict lookups — standard Python `.sort()`, `filter()`, and comprehensions across your dynamically mapped graph using dot-notation.

### 5. Float typing for `current_price`

CoinGecko's `current_price` field arrives with an inconsistent JSON
number type: whole-dollar coins (`64524`) come back as an `int`, everything
else (`0.999`) as a `float`. **Keep `"current_price": inc(float,
default=0.0)`** in the `conv_dict` — pinning the type here is what lets
sub-dollar prices (stablecoins, small-caps) keep their cents; drop it and
the column re-reads as whatever type showed up first, rounding fractional
prices to whole dollars. This is an example-side typing decision, not a
framework bug — Binance's numeric fields arrive as strings, so they never
need it.

---

## Run it

```bash
# Python entry
python examples/appendix/crypto-graph-mapping/crypto_graph_mapping.py

# Same three-source join, from the CLI
incorporator tideweaver run watershed.json
```

Also runs in Docker via the [central mount pattern](../../README.md#running-a-tutorial-in-docker) (not run or verified). This is a bounded Tideweaver `parallel` shape that exits when the window ends, not `cls.fjord()`'s unbounded daemon. Verified live: both forms sort the same top assets by `market_cap_rank`, with `usdt_*`/`usdc_*` legitimately `None` for assets missing that quote on binance.us (see [`watershed.json`](watershed.json) + [`outflow.py`](outflow.py)).

---

## Where to Go Next

| Goal | Read |
|---|---|
| Drill parent records into per-record children | [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Run the same multi-source join as a live daemon | [Tutorial 10 — Multi-Source Fjord](../../10-multi-source-fjord/README.md) |
| Coordinate the joined sources in a windowed graph | [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) |
| Configure the join from JSON for the CLI | [CLI & Configuration Guide](../../../docs/cli_and_configuration.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/crypto-graph-mapping/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
