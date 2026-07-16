"""Outflow logic and class definitions for the crypto-graph-mapping Tideweaver.

Defines the three source ``Incorporator`` subclasses referenced from
``watershed.json`` (mirroring ``crypto_graph_mapping.py``'s ``BinanceStat`` /
``BinanceBook`` / ``CryptoAsset``), the derived ``CryptoLiquidity`` output
class, and the ``outflow(state)`` function the tail Fjord calls each tick.

``crypto_graph_mapping.py``'s ``main()`` does this exact join at BUILD time
via ``conv_dict={"stats_usdt": calc(link_to(binance_stats, ...), "symbol"), ...}``
because it has all three registries in local scope before it constructs
``CryptoAsset``. A Tideweaver ``parallel`` shape has no such ordering
guarantee across its three independent source Streams (and no cross-current
``inflow(state)`` seed hook — that's specific to the legacy ``fjord()``
daemon's tiered seeding), so this join happens READ-TIME instead, in the
tail Fjord's ``outflow(state)``, once all three parent snapshots exist. Same
``link_to`` + ``make_linker`` factory-closure pattern, just invoked directly
as a plain callable (``Op.__call__``) rather than wired into a ``conv_dict``.

Imported by ``incorporator tideweaver run watershed.json``, so host-throttle
registration lives here (the Python entry registers it independently too).
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
    """

    def linker(symbol_str: str) -> str | None:
        if symbol_str:
            return f"{symbol_str.upper()}{quote_currency}"
        return None

    return linker


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
        symbol = getattr(asset, "symbol", "")
        stats_usdt = link_stats_usdt(symbol)
        book_usdt = link_books_usdt(symbol)
        stats_usdc = link_stats_usdc(symbol)
        book_usdc = link_books_usdc(symbol)
        rows.append(
            {
                "inc_code": asset.inc_code,
                "symbol": str(symbol).upper(),
                "current_price": getattr(asset, "current_price", 0.0),
                "market_cap_rank": getattr(asset, "market_cap_rank", 0),
                "usdt_volume": getattr(stats_usdt, "quoteVolume", None),
                "usdt_bid": getattr(book_usdt, "bidPrice", None),
                "usdc_volume": getattr(stats_usdc, "quoteVolume", None),
                "usdc_bid": getattr(book_usdc, "bidPrice", None),
            }
        )
    rows.sort(key=operator.itemgetter("market_cap_rank"))
    return rows
