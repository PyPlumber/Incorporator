"""
Outflow sidecar for `examples/10-multi-source-fjord/crypto_spread.py`.

The fjord engine imports this file at runtime, registers the two source
classes (CoinGecko + BinancePair), and calls `outflow(state)` on each
export wave to fuse them into a single row stream: the basis-point
spread between CoinGecko USD price and Binance USDT price for every
overlapping symbol.

Both sources' numeric coercion happens at BUILD time, in each source's
own static `conv_dict` declared in `crypto_spread.py`. The cross-source
join (CoinGecko symbol -> Binance pair) happens here, at READ time:
`state["BinancePair"]` is the live `IncorporatorList` snapshot the fjord
engine hands `outflow()` on every wave, taken under its own shared lock,
so `.inc_dict.get(...)` is a safe, cheap lookup against already-coerced
`BinancePair` instances.

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


def _to_binance_symbol(sym: str) -> str:
    """CoinGecko ticker symbol -> Binance USDT pair key: 'btc' -> 'BTCUSDT'."""
    return f"{sym.upper()}USDT"


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join CoinGecko USD vs Binance USDT for overlapping symbols.

    For each CoinGecko coin, look up the matching `BinancePair` by
    computed key (`SYMBOLUSDT`) against the live `state["BinancePair"]`
    snapshot; unmatched coins are skipped.

    ``state`` is a snapshot of each source by class name, taken under
    the engine's shared lock. Return ``list[dict]``; fjord handles the
    export.
    """
    coins = state["CoinGecko"] or []
    pairs = state["BinancePair"]

    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for coin in coins:
        pair = pairs.inc_dict.get(_to_binance_symbol(coin.symbol))
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
