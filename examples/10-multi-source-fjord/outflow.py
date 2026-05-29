"""
Outflow sidecar for `examples/10-multi-source-fjord/crypto_spread.py`.

The fjord engine imports this file at runtime, registers the two source
classes (CoinGecko + BinancePair), and calls `outflow(state)` on each
export wave to fuse them into a single row stream: the basis-point
spread between CoinGecko USD price and Binance USDT price for every
overlapping symbol.

Dynamic output class is built from this file's stem —
`outflow.py` → `Outflow`.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from incorporator import Incorporator


class CoinGecko(Incorporator):
    """Source A — CoinGecko USD market prices (top N by market cap)."""


class BinancePair(Incorporator):
    """Source B — Binance current USDT-quoted prices for every pair."""


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Join CoinGecko USD vs Binance USDT for overlapping symbols.

    For each CoinGecko coin where a matching ``{SYMBOL}USDT`` pair
    exists in Binance, emit a row containing the symbol, both prices,
    and the basis-point spread.

    ``state`` is a snapshot of each source by class name, taken under
    the engine's shared lock. Return ``List[dict]``; fjord handles the
    export.
    """
    coins = state["CoinGecko"] or []
    pairs = state["BinancePair"]
    if pairs is None:
        return []

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for coin in coins:
        symbol = getattr(coin, "symbol", "").upper()
        if not symbol:
            continue

        binance_key = f"{symbol}USDT"
        pair = pairs.inc_dict.get(binance_key)
        if pair is None:
            continue  # CoinGecko coin not traded on Binance

        try:
            gecko_usd = float(getattr(coin, "current_price", 0) or 0)
            binance_usdt = float(getattr(pair, "price", 0) or 0)
        except (TypeError, ValueError):
            continue

        if gecko_usd <= 0 or binance_usdt <= 0:
            continue

        # Basis points: 1 bp = 0.01%.  Positive = Binance higher than CoinGecko.
        spread_bps = round(((binance_usdt - gecko_usd) / gecko_usd) * 10_000, 2)

        rows.append(
            {
                "symbol": symbol,
                "coingecko_usd": gecko_usd,
                "binance_usdt": binance_usdt,
                "spread_bps": spread_bps,
                "fused_at": now,
            }
        )

    return rows
