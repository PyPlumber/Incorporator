"""Outflow logic for the multi-exchange arb-scanner Tideweaver diamond.

Defines the four ``Incorporator`` subclasses referenced from
``examples/11-tideweaver/watershed.json`` (one per exchange head/middle + the
tail's derived output class), the shared ``normalize_asset`` helper each
venue Stream's build-time ``conv_dict`` calls, and the ``outflow(state)``
function the ``Fjord`` tail current calls each tick.

Symbol normalization is hard-coded for the demo (2 assets x 3 exchanges).
Real scanners would build the normalization table from each exchange's
``/exchangeInfo`` (or equivalent) endpoint — add a 4th ``Stream`` current
for that and let ``normalize_asset`` consume it.

Each venue Stream's build-time ``conv_dict`` (declared inline in
``arb_scanner.py``, next to each ``Stream(...)`` call, and mirrored as JSON
string tokens in ``watershed.json``) normalizes that venue's raw
symbol/bid/ask fields to uniform ``asset`` / ``bid`` / ``ask`` attributes via
``normalize_asset`` below. ``outflow()`` therefore reads plain, pre-coerced
attributes across all three venues with no per-venue field-name plumbing and
no try/except. ``normalize_asset`` has no leading underscore so it resolves
both as a direct Python import and as a ``watershed.json`` conv_dict token
(``calc(normalize_asset, ...)``).

Relative ``inc_file`` / ``outflow`` paths in ``watershed.json`` resolve
against the config file's directory, not the caller's working directory.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from incorporator import Incorporator

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil. Fixtures are offline, so a
# 2-minute window gives each interval (15/30/30/30s) 4-8 ticks -- enough for
# the tail Fjord to flush its append-mode export more than once.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=2)

# ---------------------------------------------------------------------------
# Source classes (one per exchange feed)
# ---------------------------------------------------------------------------


class BinanceBook(Incorporator):
    """Binance.us /api/v3/ticker/bookTicker — top of book per symbol."""


class CoinbaseTicker(Incorporator):
    """Coinbase Advanced Trade /products/{id}/ticker — top of book per product."""


class KrakenTicker(Incorporator):
    """Kraken /0/public/Ticker — top of book for the requested pairs."""


class BestMarket(Incorporator):
    """Derived per-asset arb snapshot — built by the fjord flush each tick."""


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------


# Map exchange-native symbol → canonical asset code.  Real production
# scanners build this dynamically from each exchange's /exchangeInfo feed.
NORMALIZATION: dict[str, str] = {
    # Binance.us
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "BTCUSD": "BTC",
    "ETHUSD": "ETH",
    # Coinbase Advanced Trade
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
    # Kraken (uses X/Z prefixes for "fiat-quoted crypto")
    "XXBTZUSD": "BTC",
    "XETHZUSD": "ETH",
    "XBTUSD": "BTC",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_asset(raw: Any) -> str | None:
    """Map one venue's raw symbol/pair key to a canonical asset code.

    Referenced by each venue Stream's build-time conv_dict in arb_scanner.py
    and by watershed.json's matching conv_dict token.
    """
    return NORMALIZATION.get(str(raw))


def _venue_quotes(rows: list[Any], venue: str) -> list[tuple[str, float, float, str]]:
    """Extract canonical (asset, bid, ask, venue) tuples -- rows already carry
    uniform pre-coerced asset/bid/ask attrs via each venue Stream's build-time
    conv_dict (see arb_scanner.py Stream definitions and watershed.json)."""
    out: list[tuple[str, float, float, str]] = []
    for row in rows:
        asset = row.asset
        if asset is None:
            continue
        if row.bid > 0 and row.ask > 0:
            out.append((asset, row.bid, row.ask, venue))
    return out


# ---------------------------------------------------------------------------
# Outflow
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Snapshot the three exchange registries → per-asset best-market record.

    Args:
        state: keyed by upstream ``Incorporator`` subclass name; maps to a
            list of the current registry instances per class (populated by
            each upstream ``Stream`` current's chunking drain).

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
