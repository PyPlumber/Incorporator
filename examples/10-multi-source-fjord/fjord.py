"""
Tutorial 6 — Multi-Source Fjord: Live Crypto Spread
---------------------------------------------------
Companion script for `examples/10-multi-source-fjord/README.md`.

`stream()` watches one source. `fjord()` watches N sources concurrently
and lets you fuse them through a user-defined `outflow(state)` function.

This pipeline fuses two live feeds — CoinGecko USD prices and Binance
USDT prices — and emits a basis-point spread row per overlapping
symbol. Each source refreshes on its own cadence; the outflow daemon
snapshots every source under a shared lock and calls `outflow()` in a
worker thread so a heavy join can't block the refresh daemons.

The dynamic output class is built from the outflow filename stem —
`crypto_spread.py` → `CryptoSpread`. No output class to declare.

Run with:
    python examples/10-multi-source-fjord/fjord.py
"""

import asyncio
import sys
from pathlib import Path

from incorporator import Incorporator

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via ``python -m`` /
# pytest / any path other than ``python examples/10-multi-source-fjord/fjord.py``
# (Python only adds the script's directory to sys.path automatically in the
# bare-script case).
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# Bring the source classes into scope so fjord() can register them.
from crypto_spread import BinancePair, CoinGecko  # noqa: E402


async def main() -> None:
    # Refresh + export daemons are ON by default — no need for boilerplate
    # `refresh_params={}` on each source.  Pass `refresh_params=None` to
    # opt OUT of refresh on a specific source.  Top-level intervals can be
    # a scalar (applies to every source) OR a dict keyed by class name for
    # different cadences per source.
    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": CoinGecko,
                "incorp_params": {
                    "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
                    "params": {"vs_currency": "usd", "per_page": 100, "page": 1},
                    "inc_code": "id",
                },
            },
            {
                "cls": BinancePair,
                "incorp_params": {
                    # api.binance.com is geo-blocked in many regions (US, UK, Singapore).
                    # api.binance.us is the US-licensed mirror with the same v3 endpoint
                    # shape; swap back to api.binance.com if you're outside those regions.
                    "inc_url": "https://api.binance.us/api/v3/ticker/price",
                    "inc_code": "symbol",
                },
            },
        ],
        outflow=str(HERE / "crypto_spread.py"),
        export_params={"file_path": str(OUT / "crypto_spread.ndjson")},
        refresh_interval={                                  # per-source cadences
            "CoinGecko": 60,                                # CoinGecko's free tier is rate-limited
            "BinancePair": 30,                              # Binance is faster
        },
        export_interval=60.0,                               # fused output: every 60 s
    ):
        op = wave.operation                                 # e.g. "fjord_refresh:CoinGecko"
        if wave.failed_sources:
            # Surface the reason fjord might abort — empty seeds (geo-blocks,
            # rate limits, transient outages) cause the engine to bail early.
            print(f"⚠️  {op:40s} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {op:40s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
