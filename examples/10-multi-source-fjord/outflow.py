"""
Outflow sidecar for `examples/10-multi-source-fjord/crypto_spread.py`.

The fjord engine imports this file at runtime, registers the two source
classes (CoinGecko + BinancePair), and calls `outflow(state)` on each
export wave to fuse them into a single row stream: the basis-point
spread between CoinGecko USD price and Binance USDT price for every
overlapping symbol.

The cross-source join (CoinGecko symbol -> Binance pair) and both
sources' numeric coercion happen at BUILD time, in the `conv_dict`s
declared in `crypto_spread.py` and the sibling `inflow.py` — see those
files' docstrings for the wiring. By the time a row reaches `outflow()`
below, `coin.binance_pair` is either a resolved `BinancePair` instance
or `None` (never a raw string to parse), and `coin.current_price` /
`pair.price` are already floats. Reads here are plain attribute access;
no `getattr(..., default) or fallback`, no `float(x or 0)`, no
`.inc_dict.get(...)` registry lookup.

Dynamic output class is built from this file's stem —
`outflow.py` -> `Outflow`.
"""

from datetime import datetime, timezone
from typing import Any

from incorporator import Incorporator


class CoinGecko(Incorporator):
    """Source A — CoinGecko USD market prices (top N by market cap)."""


class BinancePair(Incorporator):
    """Source B — Binance current USDT-quoted prices for every pair."""


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join CoinGecko USD vs Binance USDT for overlapping symbols.

    For each CoinGecko coin whose ``binance_pair`` resolved (build-time
    join via `inflow.py`'s `link_to`), emit a row containing the symbol,
    both prices, and the basis-point spread.

    ``state`` is a snapshot of each source by class name, taken under
    the engine's shared lock. Return ``list[dict]``; fjord handles the
    export.
    """
    coins = state["CoinGecko"] or []

    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for coin in coins:
        pair = coin.binance_pair  # plain attribute — None if unmatched on Binance
        if pair is None:
            continue

        gecko_usd = coin.current_price  # already coerced float
        binance_usdt = pair.price  # already coerced float (BinancePair's own conv_dict)

        # Cross-field validity check on the JOINED pair (both legs must be
        # positive to compute a spread) — this is output-shaping business
        # logic, not a null-safety workaround, so it stays in outflow().
        if gecko_usd <= 0 or binance_usdt <= 0:
            continue

        # Basis points: 1 bp = 0.01%.  Positive = Binance higher than CoinGecko.
        spread_bps = round(((binance_usdt - gecko_usd) / gecko_usd) * 10_000, 2)

        rows.append(
            {
                "symbol": pair.inc_code.removesuffix("USDT"),
                "coingecko_usd": gecko_usd,
                "binance_usdt": binance_usdt,
                "spread_bps": spread_bps,
                "fused_at": now,
            }
        )

    return rows
