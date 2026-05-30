"""Outflow logic for the multi-exchange arb-scanner Tideweaver diamond.

Defines the four ``Incorporator`` subclasses referenced from
``examples/11-tideweaver/watershed.json`` (one per exchange head/middle + the
tail's derived output class) plus the ``outflow(state)`` function the
``Fjord`` tail current calls each tick.

Symbol normalization is hard-coded for the demo (2 assets × 3 exchanges).
Real scanners would build the normalization table from each exchange's
``/exchangeInfo`` (or equivalent) endpoint — add a 4th ``Stream`` current
for that and let ``outflow(state)`` consume it.

Run from the repo root so the relative inc_file / outflow paths resolve:

    incorporator validate examples/11-tideweaver/watershed.json
    incorporator tideweaver run examples/11-tideweaver/watershed.json --json-output
"""

from typing import Any


# ---------------------------------------------------------------------------
# Source classes (one per exchange feed)
# ---------------------------------------------------------------------------


from incorporator import Incorporator


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


def _venue_quotes(
    rows: list[Any],
    symbol_attr: str,
    bid_attr: str,
    ask_attr: str,
    venue: str,
) -> list[tuple[str, float, float, str]]:
    """Extract canonical (asset, bid, ask, venue) tuples from one exchange's registry."""
    out: list[tuple[str, float, float, str]] = []
    for row in rows:
        raw = getattr(row, symbol_attr, None)
        if raw is None:
            continue
        asset = NORMALIZATION.get(str(raw))
        if asset is None:
            continue
        try:
            bid = float(getattr(row, bid_attr, 0) or 0)
            ask = float(getattr(row, ask_attr, 0) or 0)
        except (TypeError, ValueError):
            continue
        if bid > 0 and ask > 0:
            out.append((asset, bid, ask, venue))
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
    quotes += _venue_quotes(state.get("BinanceBook", []), "symbol", "bidPrice", "askPrice", "binance")
    quotes += _venue_quotes(state.get("CoinbaseTicker", []), "product_id", "bid", "ask", "coinbase")
    quotes += _venue_quotes(state.get("KrakenTicker", []), "_key", "b", "a", "kraken")

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
