"""Sidecar for the crypto-graph-mapping Tideweaver -- a pure name-bag.

This file exists only because the CLI needs an importable module to point
``watershed.json``'s ``"outflow"`` key at; otherwise ``outflow(state)``
below would just sit at the bottom of ``crypto_graph_mapping.py``, as the
return-twin of ``print_dashboard()``'s loop -- same fields, same join keys,
returned as dicts instead of printed as table rows.

``BinanceStat``/``BinanceBook``/``CryptoAsset``/``CryptoLiquidity`` and the
join helper (``upper_symbol``) are defined ONCE, in
``crypto_graph_mapping.py``. This module only re-exports them (via a plain
``import``) plus the CLI-only tokens the JSON config needs
(``window_start``/``window_end``) and the fjord's ``outflow(state)`` fusion
hook -- the join happens READ-TIME, once per wave, directly against the
live class-level graph map (``BinanceStat.inc_dict`` /
``BinanceBook.inc_dict``), no intermediate link ops.
"""

from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any

from crypto_graph_mapping import (
    BinanceBook,
    BinanceStat,
    CryptoAsset,
    CryptoLiquidity,
    upper_symbol,
)

__all__ = [
    "BinanceStat",
    "BinanceBook",
    "CryptoAsset",
    "CryptoLiquidity",
    "upper_symbol",
    "window_start",
    "window_end",
    "outflow",
]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time, a few seconds before the config's window is parsed --
# see incorporator/tideweaver/config.py's build_watershed ordering.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=70)


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join CoinGecko assets against Binance USDT/USDC stats + order books, read-time.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a list
            of that class's parked ``_tideweaver_snapshot`` rows.

    Returns:
        Up to 100 rows, one per CoinGecko asset, sorted by ``market_cap_rank``
        ascending, or an empty list before ``crypto_assets`` has fired.
        ``usdt_*``/``usdc_*`` are legitimately ``None`` for assets not listed
        on binance.us under that quote currency -- real sparse data, not a
        bug (newer tokens may only have a USDT book, no USDC book yet).

    The readiness guard only checks ``assets`` -- ``liquidity``'s
    ``parent_currents`` already hard-gates this current's first wave behind
    all three upstreams having ticked at least once, so ``binance_stats``/
    ``binance_books`` have always fired by the time this function runs at
    all. Any residual per-symbol miss is a real sparse-data case, resolved
    per-row by the ``.get(...)`` lookups below returning ``None``, not
    something a broader readiness check would catch.

    ``symbol`` arrives pre-uppercased (``crypto_assets``' own static
    ``conv_dict`` in ``watershed.json`` runs ``calc(upper_symbol, ...)``),
    so the join keys below are plain f-strings, no factory closure needed
    here. ``BinanceStat.inc_dict`` / ``BinanceBook.inc_dict`` are the SAME
    live graph maps ``main()``'s own build-time join traverses -- read
    directly here since there's no ``conv_dict`` machinery on this
    read-time path to route through. GC-safety is automatic: each Stream
    current auto-parks a strong-ref ``_tideweaver_snapshot`` on every tick,
    which is what keeps each ``BinanceStat``/``BinanceBook`` instance alive
    (and its weak-ref ``inc_dict`` entry resolvable) between that Stream's
    own tick and any later ``liquidity`` wave that looks it up here.

    ``quoteVolume``/``bidPrice`` resolve to real ``float`` values below (not
    Binance's raw numeric strings) because ``BinanceStat``/``BinanceBook``
    each coerce their own field via a one-entry ``conv_dict`` at their own
    ``incorp()`` time -- see ``crypto_graph_mapping.py``'s ``main()`` and
    ``watershed.json``'s ``binance_stats``/``binance_books`` currents.
    ``CryptoLiquidity`` is a bare class (no field declarations); the dict
    keys returned below ARE its export shape.
    """
    assets = state["CryptoAsset"]
    if not assets:
        return []

    rows: list[dict[str, Any]] = []
    for asset in assets:
        stat_usdt = BinanceStat.inc_dict.get(f"{asset.symbol}USDT")
        book_usdt = BinanceBook.inc_dict.get(f"{asset.symbol}USDT")
        stat_usdc = BinanceStat.inc_dict.get(f"{asset.symbol}USDC")
        book_usdc = BinanceBook.inc_dict.get(f"{asset.symbol}USDC")
        rows.append(
            {
                "inc_code": asset.inc_code,
                "symbol": asset.symbol,
                "current_price": asset.current_price,
                "market_cap_rank": asset.market_cap_rank,
                "usdt_volume": stat_usdt.quoteVolume if stat_usdt is not None else None,
                "usdt_bid": book_usdt.bidPrice if book_usdt is not None else None,
                "usdc_volume": stat_usdc.quoteVolume if stat_usdc is not None else None,
                "usdc_bid": book_usdc.bidPrice if book_usdc is not None else None,
            }
        )
    rows.sort(key=operator.itemgetter("market_cap_rank"))
    return rows
