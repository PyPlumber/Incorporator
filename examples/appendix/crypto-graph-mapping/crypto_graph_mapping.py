"""
Advanced Graph Mapping: The Stablecoin Liquidity Dashboard
----------------------------------------------------------
This example demonstrates how to fuse THREE completely different REST API
endpoints into a single Python object, while dynamically generating FOUR
distinct mapping paths (USDT and USDC sub-markets).

It showcases the `link_to` function, the `calc` interceptor, and Factory Closures
to create a deeply interconnected, null-safe data graph.
"""

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
class BinanceStat(Incorporator):
    """Registry 1: Holds 24hr volume and price statistics."""

    pass


class BinanceBook(Incorporator):
    """Registry 2: Holds real-time order book bids and asks."""

    pass


class CryptoAsset(Incorporator):
    """The Base Object: Holds global CoinGecko data."""

    pass


async def main() -> None:
    print("🌐 Initiating Multi-Graph Data Fusion...")

    # ==========================================
    # PHASE 1: Fetch the Two Target Registries
    # ==========================================
    print("⏳ Fetching 24H Exchange Statistics...")
    binance_stats = await BinanceStat.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr",
        inc_code="symbol",
        excl_lst=["priceChangePercent", "weightedAvgPrice", "openPrice", "prevClosePrice"],
    )

    print("⏳ Fetching Live Order Book Bids/Asks...")
    binance_books = await BinanceBook.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/bookTicker", inc_code="symbol"
    )

    print(f"✅ Loaded {len(binance_stats)} Stats and {len(binance_books)} Order Books into memory.")

    # ==========================================
    # PHASE 2: Fetch CoinGecko and Fuse!
    # ==========================================
    print("⏳ Fetching CoinGecko assets and building 4 sub-market links per asset...")

    # ``binance_stats`` and ``binance_books`` must stay bound in this
    # scope — ``inc_dict`` is a ``WeakValueDictionary``, so dropping
    # either reference would let the registries get GC'd before the
    # ``link_to`` resolvers below traverse them.  See T1's runtime
    # contract for the canonical lifecycle treatment.
    assets = await CryptoAsset.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1",
        inc_code="id",
        inc_name="name",
        conv_dict={
            # MAGIC HAPPENS HERE: We use our Factory to generate 4 parallel mapping routes!
            # It maps the USDT and USDC pairings to both the Stats AND the Order Books.
            "stats_usdt": calc(link_to(binance_stats, extractor=make_linker("USDT")), "symbol"),
            "book_usdt": calc(link_to(binance_books, extractor=make_linker("USDT")), "symbol"),
            "stats_usdc": calc(link_to(binance_stats, extractor=make_linker("USDC")), "symbol"),
            "book_usdc": calc(link_to(binance_books, extractor=make_linker("USDC")), "symbol"),
        },
    )

    print(f"✅ Fused {len(assets)} assets. Commencing Unified Readout...\n")

    # ==========================================
    # PHASE 3: Traverse the Unified Graph
    # ==========================================
    print("=" * 115)
    print(
        f"{'ASSET':<18} | {'GLOBAL PRICE':<14} | {'USDT VOLUME':<16} | {'USDT BEST BID':<14} | {'USDC VOLUME':<16} | {'USDC BEST BID'}"
    )
    print("=" * 115)

    def extract_market_data(stats_obj, book_obj):
        """Helper to safely extract data if the Binance link was successful."""
        if stats_obj and book_obj:
            vol_str = getattr(stats_obj, "quoteVolume", "0")
            bid_str = getattr(book_obj, "bidPrice", "0")
            return f"${float(vol_str):,.0f}", f"${float(bid_str):,.2f}"
        return "N/A", "N/A"

    # We sort descending (reverse=True) based on the 'current_price' attribute.
    # getattr ensures it defaults to 0 if the API is missing the price.
    assets.sort(key=lambda a: getattr(a, "market_cap_rank", 0))

    for asset in assets:
        name = str(getattr(asset, "inc_name", "Unknown"))
        symbol = str(getattr(asset, "symbol", "")).upper()
        global_price = f"${getattr(asset, 'current_price', 0):,.2f}"

        # Safely traverse the 4 linked Binance objects!
        vol_usdt, bid_usdt = extract_market_data(getattr(asset, "stats_usdt", None), getattr(asset, "book_usdt", None))
        vol_usdc, bid_usdc = extract_market_data(getattr(asset, "stats_usdc", None), getattr(asset, "book_usdc", None))

        asset_label = f"{name} ({symbol})"
        print(f"{asset_label:<18} | {global_price:<14} | {vol_usdt:<16} | {bid_usdt:<14} | {vol_usdc:<16} | {bid_usdc}")

    print("=" * 115)


if __name__ == "__main__":
    asyncio.run(main())
