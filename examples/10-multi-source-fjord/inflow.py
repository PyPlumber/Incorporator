"""Inflow sidecar for `examples/10-multi-source-fjord/crypto_spread.py`.

Provides the state-aware ``inflow(state)`` hook that wires CoinGecko's
build-time ``conv_dict`` against the already-seeded Binance registry.

CoinGecko declares ``depends_on=["BinancePair"]`` in `crypto_spread.py`,
which switches fjord from parallel to tiered seeding: BinancePair seeds
in tier 0, then this module's ``inflow(state)`` fires before CoinGecko's
own ``incorp()`` in tier 1, with ``state["BinancePair"]`` already a live
``IncorporatorList``.

``link_to``'s conv_dict key must match the SOURCE field it reads (the
dispatcher feeds a bare ``Op`` with ``d.get(key)`` — see
``incorporator/schema/builder.py``'s conv_dict pass) — so the override
below targets ``"symbol"`` (CoinGecko's raw ticker field), not a new
``"binance_pair"`` key.  ``crypto_spread.py``'s ``name_chg`` then renames
the resolved-object field to ``binance_pair`` post-conv_dict, freeing
``coin.binance_pair`` as a plain, unambiguous attribute for ``outflow()``.
"""

from typing import Any

from incorporator import inc, link_to


def _to_binance_symbol(sym: str) -> str:
    """CoinGecko ticker symbol -> Binance USDT pair key: 'btc' -> 'BTCUSDT'."""
    return f"{sym.upper()}USDT"


def inflow(state: dict[str, Any]) -> dict[str, Any]:
    """Emit CoinGecko's conv_dict override once BinancePair has seeded.

    CoinGecko is tier 1 (``depends_on=["BinancePair"]``); BinancePair is
    tier 0.  By the time this fires for CoinGecko, ``state["BinancePair"]``
    is a live ``IncorporatorList`` — ``link_to()`` with a computed-key
    extractor resolves the cross-source join at build time, once per row,
    instead of a per-row registry lookup inside ``outflow()`` on every
    export wave.
    """
    overrides: dict[str, Any] = {}
    if "BinancePair" in state:
        overrides["CoinGecko"] = {
            "conv_dict": {
                "current_price": inc(float, default=0.0),
                "symbol": link_to(state["BinancePair"], extractor=_to_binance_symbol),
            }
        }
    return overrides
