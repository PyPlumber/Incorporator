"""Outflow logic for the crypto-graph-mapping Tideweaver.

Mirrors ``crypto_graph_mapping.py``'s three source classes plus the derived
``CryptoLiquidity`` row. Unlike ``main()``'s BUILD-time ``conv_dict`` join,
``outflow(state)`` here joins READ-TIME once all three parallel Streams
have parked a snapshot -- host-throttle registration lives here too, since
the CLI path imports this module directly.
"""

from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.extractors import link_to

# Pace api.coingecko.com at 0.2 req/sec (12/min -- under the 5-15/min
# free-tier ceiling). MUST live here, not just in the Python entry -- this
# module is what the CLI path actually imports.
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time, a few seconds before the config's window is parsed --
# see incorporator/tideweaver/config.py's build_watershed ordering.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=70)


def make_linker(quote_currency: str):
    """Factory: returns a linker synthesizing e.g. "BTC" -> "BTCUSDC".

    Same factory-closure pattern as ``crypto_graph_mapping.py`` -- a
    ``def``-based closure, not a ``lambda``, so it stays lambda-free-legal.
    ``symbol_str`` arrives pre-upper (CryptoAsset's own conv_dict coerces
    ``symbol`` at build time), so the ``.upper()`` here is a no-op on an
    already-upper string -- kept for safety since this factory is also
    reused verbatim against Binance's own already-upper ``symbol`` keys.
    """

    def linker(symbol_str: str) -> str | None:
        if symbol_str:
            return f"{symbol_str.upper()}{quote_currency}"
        return None

    return linker


def upper_symbol(value: str) -> str:
    """Named wrapper for str.upper -- str.upper is attribute access,
    rejected by the JSON token grammar's safe-eval walker (see
    incorporator/cli/tokens.py); a named module-level function resolves
    as a bare ast.Name instead. Public (no leading underscore) so both
    entry forms' token resolvers see it.
    """
    return value.upper()


class BinanceStat(Incorporator):
    """Registry 1: 24hr volume and price statistics from api.binance.us."""


class BinanceBook(Incorporator):
    """Registry 2: real-time order book bids and asks from api.binance.us."""


class CryptoAsset(Incorporator):
    """The base registry: global market data from api.coingecko.com."""


class CryptoLiquidity(Incorporator):
    """Derived liquidity dashboard row -- one per CoinGecko asset, produced by outflow(state)."""

    symbol: str
    current_price: float
    market_cap_rank: int
    usdt_volume: float | None
    usdt_bid: float | None
    usdc_volume: float | None
    usdc_bid: float | None


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join CoinGecko assets against Binance USDT/USDC stats + order books, read-time.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a list
            of that class's parked ``_tideweaver_snapshot`` rows.

    Returns:
        Up to 100 rows sorted by ``market_cap_rank`` ascending, or an empty
        list when any of the three parent Streams hasn't fired yet.
        ``usdt_*``/``usdc_*`` are legitimately ``None`` for assets not listed
        on binance.us under that quote currency -- real sparse data, not a
        bug (newer tokens may only have a USDT book, no USDC book yet).
    """
    stats = state.get("BinanceStat", [])
    books = state.get("BinanceBook", [])
    assets = state.get("CryptoAsset", [])
    if not (stats and books and assets):
        return []

    link_stats_usdt = link_to(stats, extractor=make_linker("USDT"))
    link_books_usdt = link_to(books, extractor=make_linker("USDT"))
    link_stats_usdc = link_to(stats, extractor=make_linker("USDC"))
    link_books_usdc = link_to(books, extractor=make_linker("USDC"))

    rows: list[dict[str, Any]] = []
    for asset in assets:
        symbol = asset.symbol  # already upper-cased via CryptoAsset's own conv_dict
        stats_usdt = link_stats_usdt(symbol)
        book_usdt = link_books_usdt(symbol)
        stats_usdc = link_stats_usdc(symbol)
        book_usdc = link_books_usdc(symbol)
        rows.append(
            {
                "inc_code": asset.inc_code,
                "symbol": asset.symbol,
                "current_price": asset.current_price,
                "market_cap_rank": asset.market_cap_rank,
                "usdt_volume": getattr(stats_usdt, "quoteVolume", None),
                "usdt_bid": getattr(book_usdt, "bidPrice", None),
                "usdc_volume": getattr(stats_usdc, "quoteVolume", None),
                "usdc_bid": getattr(book_usdc, "bidPrice", None),
            }
        )
    rows.sort(key=operator.itemgetter("market_cap_rank"))
    return rows
