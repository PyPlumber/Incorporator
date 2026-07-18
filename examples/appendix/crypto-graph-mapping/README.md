***

> 📎 **Appendix — Static graph join.** `link_to`-based one-shot
> in-memory join across CoinGecko + Binance. See
> [Tutorial 10 — Multi-Source Fjord](../../10-multi-source-fjord/README.md) for
> the same fusion as a live daemon; reach for this appendix when
> you want the static pattern without the daemon scaffolding.

***

# 🕸️ Multi-Graph Mapping: The Unified Trading Dashboard

You don't need a daemon. You want T10's cross-venue join done **once**, the results printed (or exported), then the process exits. `link_to(...)` builds the in-memory join across two CoinGecko + Binance endpoints in one async-for-free call. `crypto_graph_mapping.py`'s `main()` resolves that join BUILD-time, inside `CryptoAsset`'s own `conv_dict`; the CLI form (`watershed.json`) resolves the *same* join READ-time instead, inside a `CryptoLiquidity` fjord's `outflow(state)` — see [Section 6](#6-the-declarative-form-watershedjson--a-read-time-fjord) for why the two entry points deliberately diverge.

In the real world, no single API has all the data you need:

* **CoinGecko** is fantastic for global market caps and circulating supply, but lacks deep, real-time exchange liquidity metrics.
* **Binance** has real-time order book bids and asks, but their API is fragmented across hundreds of individual trading pairs (USDT, USDC, etc.).

Try to merge these with standard Python `for` loops and you'll hit **429 Too Many Requests** instantly. This appendix pulls data from **three different endpoints**, merges them into a single Python object graph, and maps 100 assets to 400 exchange markets using exactly **3 API calls**.

---

## 🎯 The Goal

Build a Unified Stablecoin Liquidity Dashboard. For the top 100 cryptocurrencies, see their Global Price, their Binance **USDT** volume/bids, and their Binance **USDC** volume/bids side-by-side.

## 💻 The Complete Code

`BinanceStat`/`BinanceBook`/`CryptoAsset`/`CryptoLiquidity` and every conv_dict
helper live in `crypto_graph_mapping.py`, defined exactly once — `outflow.py`
only re-exports them (see [Section 6](#6-the-declarative-form-watershedjson--a-read-time-fjord)
below). `main()` reads linearly — fetch both Binance registries, then fetch
CoinGecko with the join `conv_dict` written inline in the `incorp()` call
itself, then print the dashboard:

```python
from incorporator import Incorporator, inc, link_to, register_host_penstock
from incorporator.schema.converters import calc

register_host_penstock("api.coingecko.com", rate_per_sec=0.2)


def make_linker(quote_currency: str):
    """A factory returning a linker for one stablecoin, e.g. "USDC" -> "BTCUSDC"."""

    def linker(symbol_str: str) -> str | None:
        if symbol_str:
            return f"{symbol_str.upper()}{quote_currency}"
        return None

    return linker


def upper_symbol(value: str) -> str:
    return value.upper()


class BinanceStat(Incorporator): ...
class BinanceBook(Incorporator): ...
class CryptoAsset(Incorporator): ...


def quote_volume(stat: BinanceStat) -> str:
    return stat.quoteVolume


def bid_price(book: BinanceBook) -> str:
    return book.bidPrice


async def main() -> None:
    binance_stats = await BinanceStat.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr", inc_code="symbol", ...
    )
    binance_books = await BinanceBook.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/bookTicker", inc_code="symbol"
    )
    assets = await CryptoAsset.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1",
        inc_code="id",
        inc_name="name",
        conv_dict={
            "current_price": inc(float, default=0.0),
            "symbol": calc(upper_symbol, "symbol", default="", target_type=str),
            "market_cap_rank": inc(int, default=0),
            # We use our Factory to generate 4 parallel mapping routes simultaneously!
            "stats_usdt": calc(link_to(binance_stats, extractor=make_linker("USDT")), "symbol"),
            "book_usdt": calc(link_to(binance_books, extractor=make_linker("USDT")), "symbol"),
            "stats_usdc": calc(link_to(binance_stats, extractor=make_linker("USDC")), "symbol"),
            "book_usdc": calc(link_to(binance_books, extractor=make_linker("USDC")), "symbol"),
            # Extraction entries read the just-linked object — no getattr, no guard
            # (calc's own garbage short-circuit skips the func on a missed link).
            "usdt_volume": calc(quote_volume, "stats_usdt", target_type=float),
            "usdt_bid": calc(bid_price, "book_usdt", target_type=float),
            "usdc_volume": calc(quote_volume, "stats_usdc", target_type=float),
            "usdc_bid": calc(bid_price, "book_usdc", target_type=float),
        },
    )
    print_dashboard(assets)
```

`binance_stats`/`binance_books` stay bound as `main()`'s own locals for the
duration of the `CryptoAsset.incorp()` call — the `link_to(...)` entries
above traverse them live while `conv_dict` resolves. `print_dashboard()`
sorts with `assets.sort(key=operator.attrgetter("market_cap_rank"))` and
reads every field with plain dots (`asset.usdt_volume`, `asset.current_price`,
...) — no `getattr` anywhere in the file. See
[`crypto_graph_mapping.py`](crypto_graph_mapping.py) for the full file,
including `print_dashboard()`'s cp1252-safe formatting.

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

> **Strong-ref note.** `inc_dict` is a `WeakValueDictionary` ([T1's runtime contract](../../01-first-steps/README.md#step-3-apply-the-recommendations-with-incorp) has the canonical lifecycle treatment). As long as `binance_stats` and `binance_books` are held as `main()`'s own locals for the duration of the `CryptoAsset.incorp()` call (they are — both are awaited a few lines above and stay in scope), every record stays resident and `link_to` resolves cleanly. Drop those references and the registries can be garbage-collected mid-traversal.

### 2. The Factory Closure Pattern
Instead of four separate `lambda`s, one Factory function:
```python
def make_linker(quote_currency: str):
```
CoinGecko gives us `"btc"`; Binance expects `"BTCUSDT"`. The factory generates a closure that uppercases and appends the specific stablecoin suffix. The `conv_dict` stays perfectly declarative.

### 3. Native Null-Safety (Sparse Data Handling)
In crypto, highly liquid coins trade against everything — but newer tokens might only have a `USDT` book and no `USDC` book yet.

`link_to` is natively null-safe: if it searches the registry for `NEWCOINUSDC` and fails, it attaches `None` to `asset.stats_usdc`. It **never** raises `AttributeError`. The `conv_dict`'s `usdt_volume`/`usdt_bid`/`usdc_volume`/`usdc_bid` entries then resolve to their `calc` default (`None`) rather than crashing, since `calc`'s garbage short-circuit skips `quote_volume`/`bid_price` entirely on a missed link. `print_dashboard()`'s `fmt_usd()` helper checks `if value is None` and displays `"N/A"` — a plain `None`-check, not a defensive `getattr`.

### 4. Database-Like Querying (`.sort()`)
Because Incorporator infers the schema and transforms raw JSON into Python objects *during* ingestion, the final `assets` list behaves like a clean database result:
```python
assets.sort(key=operator.attrgetter("market_cap_rank"))
```
No nightmare dict lookups, no `getattr` default — `market_cap_rank` is always present (`inc(int, default=0)` guarantees it), so a plain `operator.attrgetter` is enough. Standard Python `.sort()`, `filter()`, and comprehensions work across your dynamically mapped graph using dot-notation.

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

### 6. The declarative form (`watershed.json`) — a read-time fjord

The CLI form does **not** mirror `main()`'s build-time join. It joins the
same three sources on the *other* side of Incorporator's verb split: "a
stream is meant for continuous chunking ingestion; fjords are meant for
multi-source, in-state graph maps, and manipulating the export." This
appendix's CLI form is exactly that fjord use-case — three independent-source
`Stream`s (`binance_stats`, `binance_books`, `crypto_assets`) each fetch their
own registry once (`interval` ≥ the window length), and a fourth current,
`liquidity`, is a `Fjord` that maps the graph across all three and shapes the
export in one step.

`crypto_assets`' own `conv_dict` in `watershed.json` carries only its *own*
three static coercions (`current_price`, `symbol`, `market_cap_rank`) — no
join fields. The join moved entirely into `liquidity`'s `parent_currents:
["binance_stats", "binance_books", "crypto_assets"]`, which hard-gates the
Fjord's first wave behind all three upstreams having produced at least one
wave, plus `outflow.py`'s `outflow(state)` function.

**`outflow(state)` is the return-twin of `print_dashboard()`'s loop.**
`outflow.py` exists only because the CLI needs an importable module to
point `watershed.json` at — otherwise `outflow(state)` would sit at the
bottom of `crypto_graph_mapping.py` itself, right after `print_dashboard()`.
It reads `state["CryptoAsset"]` for the ready-built assets, then reads the
Binance graph maps directly off the **classes**, not off `state`:

```python
stat_usdt = BinanceStat.inc_dict.get(f"{asset.symbol}USDT")
book_usdt = BinanceBook.inc_dict.get(f"{asset.symbol}USDT")
```

`link_to` is a `conv_dict` primitive — it belongs inside a build-time join
(as in `main()`, [Section 1](#1-zero-network-graph-mapping-immunity-to-429-errors)
above), where `calc(link_to(...), "symbol")` routes through the framework's
own garbage short-circuit. `outflow(state)` has no `conv_dict` machinery to
route through, so it reads the same live graph map `link_to` itself reads
from (`BinanceStat.inc_dict`) directly, with a plain `.get(...)` and an
explicit `is not None` check per field. `symbol` arrives pre-uppercased
(`crypto_assets`' own `conv_dict` runs `calc(upper_symbol, ...)`), so the
join keys are plain f-strings — no `make_linker` needed on this path.

> A related fix (`link_to`'s target dataset is now read live on every call
> instead of snapshotted once at construction) also lets a JSON `conv_dict`
> reference a class token directly in a `link_to(...)` entry, the same way
> `main()`'s Python `conv_dict` does. This appendix's CLI form doesn't need
> that — see [`docs/api_atlas.md`](../../../docs/api_atlas.md) for the
> general case.

**GC-safety is automatic.** `BinanceStat.inc_dict` / `BinanceBook.inc_dict`
are each a `WeakValueDictionary` — reading them depends on *something*
holding a strong reference to each instance between that Stream's own tick
and any later `liquidity` wave that looks it up. That something is the same
`_tideweaver_snapshot` strong-ref list every `Stream` current auto-parks on
each tick; nothing extra is required in `outflow.py` for the lookups to
stay live.

**`CryptoLiquidity`'s implicit projection.** `outflow(state)` returns plain
dicts keyed `inc_code`, `symbol`, `current_price`, `market_cap_rank`,
`usdt_volume`, `usdt_bid`, `usdc_volume`, `usdc_bid`. Because `outflow.py`
re-exports a pre-declared `CryptoLiquidity` class (defined once, in
`crypto_graph_mapping.py`) whose 7 business fields match those keys, the Fjord
instantiates `CryptoLiquidity` directly from each returned row — the returned
dict *is* the export shape; no separate `transform()` hook is needed.

`BinanceStat`/`BinanceBook`/`CryptoAsset`/`CryptoLiquidity` and the
join-token helper the `conv_dict`/`outflow(state)` reference (`upper_symbol`)
are defined exactly once, in `crypto_graph_mapping.py`. `outflow.py` does not
redefine them — it `import`s them and re-exports the names, so the CLI's
class/token resolvers see the same class objects the Python entry point
uses. This matters because `outflow.py` gets loaded (`exec_module`'d)
multiple times per run under distinct `sys.modules` keys; a class *defined
inside* `outflow.py` would become a different class object on each load,
while a plain `import` of `crypto_graph_mapping` always resolves through
Python's own module cache to the same object. `outflow.py`'s docstring
spells this out in full.

Because `outflow.py` needs to `import crypto_graph_mapping` — the mirror
image of every other tutorial sidecar, which gets imported *from* the
main script — `outflow.py` inserts its own directory onto `sys.path`
before that import (guarded against a double insert). See
[Tutorial 11](../../11-tideweaver/README.md)'s `arb_scanner.py` for the
standard direction of this idiom, which this appendix deliberately flips.

**Why the two entry points join differently, on purpose.** `main()` in
`crypto_graph_mapping.py` uses `link_to` inside a build-time `conv_dict` — it
has no waves, so the join happens exactly once. `watershed.json`'s CLI form
is a genuine multi-wave fjord: it joins read-time, once per wave, straight
off the live graph map, since there's no `conv_dict` step in `outflow(state)`
to route a `link_to` through. Read both files side by side as a deliberate
teaching pair: the same three sources, joined two different ways, each
idiomatic for its own entry shape.

### 7. Dot-notation, not `getattr` — where each guard actually lives

Neither `.py` file in this appendix contains a `getattr(`. In `main()`'s
build-time join, three separate framework guarantees make that possible,
each at a different stage:

* **`calc`'s input-key dot-path drills raw JSON only.** A `conv_dict`
  input key like `"stats_usdt.quoteVolume"` would walk dicts and lists —
  it cannot reach *into* an already-linked `Incorporator` instance. That's
  why `usdt_volume`'s extraction step is a two-hop `conv_dict` sequence
  (`stats_usdt` link, *then* `usdt_volume: calc(quote_volume, "stats_usdt", ...)`)
  rather than a single dotted input key.
* **The extraction helpers are plain one-liners with no guard**
  (`def quote_volume(stat): return stat.quoteVolume`) because `calc`'s own
  garbage short-circuit never calls the function on a `None`/garbage
  input — a missed `link_to` resolves straight to the entry's `default`
  and the helper is skipped entirely. The null path is guarded by the
  framework, once, not by every helper that reads a linked object.
* **Built instances read with plain dots** (`asset.usdt_volume`,
  `asset.market_cap_rank`, `asset.symbol`) because `conv_dict` writes
  every declared field — value or default — onto every row at build time.
  There's nothing left to guard against by the time `print_dashboard()`
  runs; the one remaining "might be missing" case (`usdt_volume` etc. for
  a sparse asset) is a real `None`, checked once in `fmt_usd()`'s
  `if value is None`, not scattered `getattr(..., None)` calls.

`outflow.py`'s read-time join has no `calc` step to lean on — it reads
`BinanceStat.inc_dict` / `BinanceBook.inc_dict` directly with `.get(...)`,
not dispatched through the `conv_dict` machinery, so there's no garbage
short-circuit to skip a step on a missed lookup. `outflow(state)` guards
explicitly instead, with an inline `x.field if x is not None else None`
conditional per lookup — still zero `getattr(`, just a different guard
mechanism for a different call site.

---

## Run it

```bash
# Python entry (build-time join)
python examples/appendix/crypto-graph-mapping/crypto_graph_mapping.py

# Same three sources, joined read-time by a fjord instead
incorporator tideweaver run watershed.json
```

Also runs in Docker via the [central mount pattern](../../README.md#running-a-tutorial-in-docker) (not run or verified). The CLI form is a bounded Tideweaver `parallel` shape (3 independent-source Streams, one-shot) with a `CryptoLiquidity` `Fjord` tail that exits when the window ends, not `cls.fjord()`'s unbounded daemon — designed to sort the same top assets by `market_cap_rank` as the Python entry, with `usdt_*`/`usdc_*` legitimately `None` for assets missing that quote on binance.us (see [`watershed.json`](watershed.json) + [`outflow.py`](outflow.py)).

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
