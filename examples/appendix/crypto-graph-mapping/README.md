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

---

## Run it from the CLI

`cls.fjord()` is a genuine unbounded daemon — `incorporator fjord pipeline.json`
loops forever until Ctrl+C/SIGTERM, which contradicts this appendix's own
point ("you don't need a daemon... the results printed, then the process
exits"). The CLI form instead ships a short, bounded [Tideweaver](../../11-tideweaver/README.md)
`watershed.json` (`shape: "parallel"`) — this appendix is outside the
numbered tutorial path, so it's free to reach for Tideweaver even though
Tutorial 11 introduces it later. `incorporator tideweaver run` exits on its
own once the window's `end` timestamp passes; no daemon, no Ctrl+C needed.

Four currents: three independent Streams (`binance_stats`, `binance_books`,
`crypto_assets` — no dependency among them, matching `shape: "parallel"`'s
"N independent pipelines, none waiting on each other") feeding one `Fjord`
tail (`liquidity`) whose `outflow(state)` performs the exact same `link_to` +
`make_linker` factory-closure join as `main()` above — just read-time, once
all three parent snapshots exist, instead of build-time in local scope.

See [`outflow.py`](outflow.py) and [`watershed.json`](watershed.json), which
ship next to the entry script.

```bash
cd examples/appendix/crypto-graph-mapping
incorporator validate watershed.json
incorporator tideweaver run watershed.json
```

Produces `out/crypto_liquidity.ndjson` with up to 100 rows, sorted by
`market_cap_rank` ascending. `usdt_*`/`usdc_*` are legitimately `None` for
assets not listed on binance.us under that quote currency — real sparse
data, not a bug.

> **Suspected framework gap — schema inference locks a numeric field's type
> from its first sampled row, with no int→float promotion across the rest of
> the sample.** `crypto_assets`'s `current_price` conv_dict entry
> (`inc(float, default=0.0)`) is a defensive fix, not decoration. CoinGecko's
> `current_price` is a raw JSON number; `market_cap_desc` ordering always
> puts bitcoin first. When bitcoin's live price happens to be a whole-dollar
> amount at fetch time, the JSON parser hands back a Python `int`, the schema
> builder locks the whole column to `int` from that first row, and every
> other asset's price — most of which genuinely have cents (`$0.999342`,
> `$0.0731`, ...) — truncates to `0`. Observed live, intermittently (present
> on some fetches, absent on others, purely dependent on whether BTC quoted a
> round number at that instant). Forcing `inc(float, ...)` in the conv_dict
> sidesteps it. Binance's `quoteVolume`/`bidPrice` fields never hit this
> because Binance always returns them as JSON strings, not numbers — no
> int/float ambiguity to lock onto.

---

## 🐳 Run It From the CLI (+ Docker)

Reference material — three ways to run the exact same three-source join, in order.

**1. Python entry** (what every section above walked through — the
build-time `link_to` join, ~3 API calls, one-shot):

```bash
cd examples/appendix/crypto-graph-mapping
python crypto_graph_mapping.py
```

**2. CLI form** — [`outflow.py`](outflow.py) + [`watershed.json`](watershed.json)
ship next to the entry script; no inline JSON duplicate here (see it drift
once, trust it forever).

```bash
cd examples/appendix/crypto-graph-mapping
incorporator validate watershed.json
incorporator tideweaver run watershed.json
```

> **Run from inside this directory.** `export_params.file_path`
> (`"out/crypto_liquidity.ndjson"`) is CWD-relative, and `"outflow":
> "outflow.py"` is config-dir-relative — running `incorporator tideweaver
> run examples/appendix/crypto-graph-mapping/watershed.json` from the repo
> root writes output to `<repo-root>/out/` and would break the sidecar
> resolution if it were repo-root-relative instead.
>
> **The window is dateless.** `watershed.json`'s `window` references
> `@window_start` / `@window_end`, two public `datetime` names defined in
> `outflow.py` and evaluated fresh at sidecar-import time (a 70-second span
> from "now") — no env vars, no editing timestamps before a re-run.

**3. Docker** — reasoned from the `Dockerfile`/`docker-compose.yml`, **NOT
run or verified** (no Docker available in this pass — confirm before
relying on it):

```bash
# Reasoned, unverified.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/examples/appendix/crypto-graph-mapping:/app/config:ro" \
  -v "$(pwd)/examples/appendix/crypto-graph-mapping/out:/app/out" \
  incorporator:latest \
  tideweaver run /app/config/watershed.json
```

The image's `WORKDIR` is `/app`, and `export_params.file_path` is
CWD-relative (never rebased against the config's directory) — so
`watershed.json`'s `"out/crypto_liquidity.ndjson"` resolves to
`/app/out/...` inside the container. The mount target must therefore be
`/app/out`, not one of the three paths the `Dockerfile` prepares
(`/app/config`, `/app/data`, `/app/logs`). Because `/app/out` is not one of
the pre-`chown`'d directories, `--user` overrides to the invoking host user
so the non-root `appuser` can still write.

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
