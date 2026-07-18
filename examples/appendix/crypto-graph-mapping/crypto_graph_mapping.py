"""
Advanced Graph Mapping: The Stablecoin Liquidity Dashboard
----------------------------------------------------------
This example demonstrates how to fuse THREE completely different REST API
endpoints into a single Python object, while dynamically generating FOUR
distinct mapping paths (USDT and USDC sub-markets).

It showcases the `link_to` function, the `calc` interceptor, and Factory Closures
to create a deeply interconnected, null-safe data graph.

``BinanceStat``/``BinanceBook``/``CryptoAsset``/``CryptoLiquidity`` are
defined ONCE, here. ``outflow.py`` re-exports them (rather than redefining
them) so the CLI's class/token resolvers see the same canonical objects
this file's own ``main()`` uses -- see ``outflow.py``'s docstring for why
that matters. ``CryptoLiquidity`` is the CLI form's derived fjord row --
this file's own ``main()`` never builds one; it writes the same 7 fields
straight onto ``CryptoAsset`` via its build-time join instead. ``main()``
reads linearly, top to bottom: fetch both Binance registries, then fetch
CoinGecko with the 11-entry join ``conv_dict`` written inline in the
``incorp()`` call itself, then print the dashboard -- no intermediate
fetch/build wrapper functions.
That makes this file's ``sys.path`` relationship with ``outflow.py`` the
mirror image of T11's idiom (``examples/11-tideweaver/arb_scanner.py``
inserts the MAIN script's dir so it can import FROM ``outflow.py``); here
``outflow.py`` inserts ITS OWN dir so it can import FROM this file instead.

Run with:
    python examples/appendix/crypto-graph-mapping/crypto_graph_mapping.py
"""

import asyncio
import operator

from incorporator import Incorporator, IncorporatorList, inc, link_to, register_host_penstock
from incorporator.schema.converters import calc

# Pace api.coingecko.com at 0.2 req/sec (12/min — under the 5-15/min
# free-tier ceiling).
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)


def make_linker(quote_currency: str):
    """
    A factory function that returns a custom linker for a specific stablecoin.
    e.g., passing "USDC" returns a function that synthesizes "BTCUSDC".
    """

    def linker(symbol_str: str) -> str | None:
        if symbol_str:
            return f"{symbol_str.upper()}{quote_currency}"
        return None

    return linker


def upper_symbol(value: str) -> str:
    """Named wrapper for ``str.upper``. ``str.upper`` is attribute access,
    rejected by the JSON token grammar's safe-eval walker (see
    ``incorporator/cli/tokens.py``); a named module-level function resolves
    as a bare ``ast.Name`` instead. Shared by both entry forms so the same
    token name works in ``watershed.json`` and here.
    """
    return value.upper()


class BinanceStat(Incorporator):
    """Registry 1: 24hr volume and price statistics from api.binance.us."""


class BinanceBook(Incorporator):
    """Registry 2: real-time order book bids and asks from api.binance.us."""


class CryptoAsset(Incorporator):
    """Global market data from api.coingecko.com, joined against
    BinanceStat/BinanceBook build-time via its own conv_dict."""


class CryptoLiquidity(Incorporator):
    """The CLI form's derived fjord row (see ``watershed.json``'s ``liquidity``
    current + ``outflow.py``'s ``outflow(state)``). ``main()`` below doesn't
    build any of these directly -- the build-time join in this file writes
    the same 7 fields straight onto ``CryptoAsset`` instead. Defined here,
    not in ``outflow.py``, per this appendix's identity-safety contract
    (see ``outflow.py``'s module docstring)."""

    symbol: str
    current_price: float
    market_cap_rank: int
    usdt_volume: float | None
    usdt_bid: float | None
    usdc_volume: float | None
    usdc_bid: float | None


def quote_volume(stat: BinanceStat) -> str:
    """24hr quote volume off a linked ``BinanceStat``.

    No None-guard: ``calc``'s garbage short-circuit never invokes this on a
    missed link (a failed ``link_to`` resolves straight to the entry's
    ``default``, skipping the func entirely).
    """
    return stat.quoteVolume


def bid_price(book: BinanceBook) -> str:
    """Best bid price off a linked ``BinanceBook``. Same null-safety
    contract as ``quote_volume``."""
    return book.bidPrice


def fmt_usd(value: float | None, decimals: int = 0) -> str:
    """Format a possibly-``None`` float as a dollar string, ``"N/A"`` if
    missing. ``value`` is ``None`` only for assets with no matching
    binance.us pair under that quote currency — real sparse data."""
    if value is None:
        return "N/A"
    return f"${value:,.{decimals}f}"


def print_dashboard(assets: IncorporatorList) -> None:
    """Plain-dot readout. `conv_dict` already wrote every declared field
    (or its default) onto each row at build time, so no `getattr`/
    defensive access is needed here."""
    assets.sort(key=operator.attrgetter("market_cap_rank"))

    print("=" * 115)
    print(
        f"{'ASSET':<18} | {'GLOBAL PRICE':<14} | {'USDT VOLUME':<16} | "
        f"{'USDT BEST BID':<14} | {'USDC VOLUME':<16} | {'USDC BEST BID'}"
    )
    print("=" * 115)

    for asset in assets:
        # CoinGecko asset names/symbols occasionally carry non-ASCII glyphs
        # (zero-width spaces on spam tokens, accented Latin, etc); a Windows
        # cp1252 console raises UnicodeEncodeError on those, so ASCII-replace
        # before printing.
        name = str(asset.inc_name).encode("ascii", errors="replace").decode("ascii")
        symbol = asset.symbol.encode("ascii", errors="replace").decode("ascii")
        global_price = fmt_usd(asset.current_price, decimals=2)
        vol_usdt = fmt_usd(asset.usdt_volume)
        bid_usdt = fmt_usd(asset.usdt_bid, decimals=2)
        vol_usdc = fmt_usd(asset.usdc_volume)
        bid_usdc = fmt_usd(asset.usdc_bid, decimals=2)

        asset_label = f"{name} ({symbol})"
        print(f"{asset_label:<18} | {global_price:<14} | {vol_usdt:<16} | {bid_usdt:<14} | {vol_usdc:<16} | {bid_usdc}")

    print("=" * 115)


async def main() -> None:
    print("Initiating Multi-Graph Data Fusion...")

    print("Fetching 24H Exchange Statistics...")
    binance_stats = await BinanceStat.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr",
        inc_code="symbol",
        excl_lst=["priceChangePercent", "weightedAvgPrice", "openPrice", "prevClosePrice"],
    )

    print("Fetching Live Order Book Bids/Asks...")
    binance_books = await BinanceBook.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/bookTicker", inc_code="symbol"
    )
    print(f"Loaded {len(binance_stats)} Stats and {len(binance_books)} Order Books into memory.")

    print("Fetching CoinGecko assets and building 4 sub-market links per asset...")
    assets = await CryptoAsset.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1",
        inc_code="id",
        inc_name="name",
        # binance_stats/binance_books must stay bound as main()'s locals for the
        # duration of this call -- inc_dict is a WeakValueDictionary, and the
        # link_to(...) entries below traverse it live while conv_dict resolves.
        conv_dict={
            "current_price": inc(float, default=0.0),
            "symbol": calc(upper_symbol, "symbol", default="", target_type=str),
            "market_cap_rank": inc(int, default=0),
            # MAGIC HAPPENS HERE: We use our Factory to generate 4 parallel mapping routes!
            # It maps the USDT and USDC pairings to both the Stats AND the Order Books.
            "stats_usdt": calc(link_to(binance_stats, extractor=make_linker("USDT")), "symbol"),
            "book_usdt": calc(link_to(binance_books, extractor=make_linker("USDT")), "symbol"),
            "stats_usdc": calc(link_to(binance_stats, extractor=make_linker("USDC")), "symbol"),
            "book_usdc": calc(link_to(binance_books, extractor=make_linker("USDC")), "symbol"),
            # Extraction entries MUST follow their matching link entries above --
            # conv_dict applies in dict-literal order; reading a link field before
            # it's resolved reads None every time.
            "usdt_volume": calc(quote_volume, "stats_usdt", target_type=float),
            "usdt_bid": calc(bid_price, "book_usdt", target_type=float),
            "usdc_volume": calc(quote_volume, "stats_usdc", target_type=float),
            "usdc_bid": calc(bid_price, "book_usdc", target_type=float),
        },
    )
    print(f"Fused {len(assets)} assets. Commencing Unified Readout...\n")

    print_dashboard(assets)


if __name__ == "__main__":
    asyncio.run(main())
