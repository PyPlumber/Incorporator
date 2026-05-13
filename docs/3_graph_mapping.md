***

# 🕸️ Multi-Graph Mapping: The Unified Trading Dashboard

In the real world, no single API has all the data you need. 
* **CoinGecko** is fantastic for global market caps and circulating supply, but it lacks deep, real-time exchange liquidity metrics. 
* **Binance** has lightning-fast order book bids and asks, but their API is fragmented across hundreds of individual trading pairs (USDT, USDC, etc.).

If you try to merge these APIs using standard Python `for` loops, you will hit **429 Too Many Requests** rate-limit bans instantly. 

**Incorporator’s Graph Mapping** solves this. In this tutorial, we will pull data from **three different endpoints**, merge them into a single Python object graph, and map 100 assets to 400 exchange markets using exactly **3 API calls**.

---

## 🎯 The Goal
We are going to build a Unified Stablecoin Liquidity Dashboard. 
For the top 100 cryptocurrencies, we want to see their Global Price, their Binance **USDT** volume/bids, and their Binance **USDC** volume/bids side-by-side.

## 💻 The Complete Code

```python
import asyncio
from incorporator import Incorporator, link_to
from incorporator.methods.converters import calc

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

    # 3. Sort by Global Price Descending
    assets.sort(key=lambda a: getattr(a, "current_price", 0), reverse=True)

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
If you wrote this using standard `httpx` loops, mapping 100 CoinGecko assets to 4 different Binance endpoints would require **400 API requests**. The exchange would IP-ban you within 3 seconds.

Look closely at the execution order. **Incorporator only makes 3 API calls:**
1. It downloads the entire global Binance Stats registry (1 call).
2. It downloads the entire global Binance Order Book registry (1 call).
3. It downloads the 100 CoinGecko assets (1 call).

When it hits the `link_to` configuration, **it disconnects from the network.** It synthesizes the target string (e.g., `"BTCUSDT"`) and searches Incorporator's internal lightning-fast RAM registry (`inc_dict`). 
It executes all 400 data mappings locally in `O(1)` memory lookup time, completely bypassing server rate limits.

### 2. The Factory Closure Pattern
Instead of writing 4 separate `lambda` functions, we wrote a single Factory function:
```python
def make_linker(quote_currency: str):
```
Because the CoinGecko API gives us `"btc"`, but Binance expects `"BTCUSDT"`, the factory dynamically generates a pure Python closure that uppercases the string and appends the specific stablecoin suffix. This keeps the `conv_dict` perfectly declarative and highly readable.

### 3. Native Null-Safety (Sparse Data Handling)
In the cryptocurrency market, highly liquid coins (like Bitcoin) trade against everything. But newer tokens might only trade against `USDT` and have no `USDC` order book yet.

Because `link_to` is natively null-safe, if it searches the memory registry for `NEWCOINUSDC` and fails to find it, it gracefully attaches `None` to `asset.stats_usdc`. 

It will **never** throw an `AttributeError` or crash your pipeline. You simply use `getattr(..., None)` in your print loop to safely display `"N/A"`.

### 4. Database-Like Querying (`.sort()`)
Because Incorporator transforms raw JSON into strict, flat Python objects *during* ingestion, your final `assets` array behaves exactly like a clean database result. 
```python
assets.sort(key=lambda a: getattr(a, "current_price", 0), reverse=True)
```
Instead of writing nightmare dictionary lookups (`x.get("current_price", 0)`), you can run standard Python `.sort()`, `filter()`, or list comprehensions across your dynamically mapped graph using beautiful dot-notation (`a.current_price`).

---

## 🐳 Run it from the CLI

Multi-source fusion is the canonical fjord shape. Each source is its own entry under `stream_params`; the `outflow()` function in the `outflow.py` file performs the `link_to` join and returns the unified rows:

```json
{
  "outflow": "outflow.py",
  "stream_params": [
    {
      "cls_name": "BinanceBook",
      "incorp_params": {
        "inc_url": "https://api.binance.us/api/v3/ticker/bookTicker",
        "inc_code": "symbol"
      },
      "refresh_params": {}
    },
    {
      "cls_name": "CryptoAsset",
      "incorp_params": {
        "inc_url": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=100",
        "inc_code": "id",
        "inc_name": "name"
      },
      "refresh_params": {}
    }
  ],
  "export_params": {"file_path": "data/crypto_fusion.ndjson"},
  "refresh_interval": 60,
  "export_interval": 120
}
```

```bash
incorporator validate pipeline.json
incorporator fjord pipeline.json
```

The `outflow.py` defines `BinanceBook(Incorporator)`, `CryptoAsset(Incorporator)`, and the `outflow(state)` function that runs the `link_to` lookups across the two in-memory registries. With the intervals above, every 60 s the sources refresh, and every 120 s the fused dataset is flushed to disk. See [`examples/fjord_code/outflow_example.py`](../examples/fjord_code/outflow_example.py) for the pattern and [the CLI guide](./cli_and_configuration.md) for the full schema.