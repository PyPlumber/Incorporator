"""Sidecar for the arb-scanner Tideweaver diamond -- a pure name-bag.

This file exists only because the CLI needs an importable module to point
``watershed.json``'s ``"outflow"`` key at; otherwise ``outflow(state)`` below
would just sit at the bottom of ``arb_scanner.py``, as the return-twin of
``main()``'s tide-print loop -- same fields, same join keys, returned as
dicts instead of printed as tide records.

``BinanceBook``/``CoinbaseTicker``/``KrakenTicker``/``BestMarket`` and the
``normalize_asset`` helper are defined ONCE, in ``arb_scanner.py``. This
module only re-exports them (via a plain ``import``) plus the CLI-only
tokens the JSON config needs (``window_start``/``window_end``) and the
fjord's ``outflow(state)`` fusion hook -- the cross-venue best-bid/best-ask
join happens READ-TIME, once per flush, directly against the plain lists
Tideweaver's Fjord current hands in ``state``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from arb_scanner import (
    BestMarket,
    BinanceBook,
    CoinbaseTicker,
    KrakenTicker,
    _venue_quotes,
    normalize_asset,
)

__all__ = [
    "BinanceBook",
    "CoinbaseTicker",
    "KrakenTicker",
    "BestMarket",
    "normalize_asset",
    "window_start",
    "window_end",
    "outflow",
]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil. Fixtures are offline, so a
# 90-second window gives the 15/30/30/30s intervals a handful of ticks --
# enough for the tail Fjord to flush its append-mode export more than once.
# Mirrors arb_scanner.py's own main() window duration.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=90)


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Snapshot the three exchange registries -> per-asset best-market record.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a
            list of that class's parked ``_tideweaver_snapshot`` rows. This
            is a Tideweaver Fjord-current wave (a ``Watershed``/diamond run,
            not a ``cls.fjord()`` daemon), so ``state`` values are PLAIN
            lists.

    Returns:
        One row per canonical asset present on at least one venue, carrying
        the best bid, best ask, the originating venues, the cross-venue
        spread in basis points, and an ``arb_opportunity`` flag for
        ``spread_bps > 5``.
    """
    quotes: list[tuple[str, float, float, str]] = []
    quotes += _venue_quotes(state.get("BinanceBook", []), "binance")
    quotes += _venue_quotes(state.get("CoinbaseTicker", []), "coinbase")
    quotes += _venue_quotes(state.get("KrakenTicker", []), "kraken")

    by_asset: dict[str, list[tuple[float, float, str]]] = {}
    for asset, bid, ask, venue in quotes:
        by_asset.setdefault(asset, []).append((bid, ask, venue))

    rows: list[dict[str, Any]] = []
    for asset, venues in by_asset.items():
        best_bid_price, best_bid_venue = max(((b, v) for b, _, v in venues), default=(0.0, ""))
        best_ask_price, best_ask_venue = min(((a, v) for _, a, v in venues), default=(0.0, ""))
        if not (best_bid_price and best_ask_price):
            continue
        mid = (best_bid_price + best_ask_price) / 2
        spread_bps = (best_bid_price - best_ask_price) / mid * 10_000
        rows.append(
            {
                "asset": asset,
                "best_bid": best_bid_price,
                "best_bid_venue": best_bid_venue,
                "best_ask": best_ask_price,
                "best_ask_venue": best_ask_venue,
                "spread_bps": round(spread_bps, 2),
                "arb_opportunity": spread_bps > 5,
            }
        )
    return rows
