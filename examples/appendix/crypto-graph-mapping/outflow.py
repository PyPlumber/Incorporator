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

**Identity safety, and why this arrangement is required, not cosmetic.**
This file gets ``exec_module``'d 2-3x under distinct ``sys.modules`` keys
(``usercode.py``'s ``load_user_module``, invoked once each for the class/
token resolver, and any fjord-outflow path). A class DEFINED here would
become a distinct class object on every such exec -- an ``issubclass``/
identity check spanning two of those execs could then silently disagree.
Because this file only IMPORTS ``crypto_graph_mapping``, Python's own
module cache (``sys.modules['crypto_graph_mapping']``, set on first
import) guarantees every re-exec of this sidecar binds the SAME canonical
class objects.

**The one gap this file works around.** ``load_user_module`` does not add
this file's own parent directory to ``sys.path`` before running it (unlike
``python <script>.py``, which auto-prepends the script's directory). Every
other shipped sidecar avoids needing this because the MAIN script imports
FROM the sidecar (T9/T10/T11's direction); this example deliberately flips
it, so the ``sys.path.insert`` below is required, guarded against a double
insert.
"""

from __future__ import annotations

import operator
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from crypto_graph_mapping import (  # noqa: E402
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
