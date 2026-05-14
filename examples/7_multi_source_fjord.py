"""
Tutorial 7 — Multi-Source Fjord: Live Crypto Spread (capstone)
--------------------------------------------------------------
Companion script for `docs/7_multi_source_fjord.md`.

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
    python examples/7_multi_source_fjord.py
"""

import asyncio

from incorporator import Incorporator

# Bring the source classes into scope so fjord() can register them.
from examples.fjord_code.crypto_spread import BinancePair, CoinGecko


async def main() -> None:
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
                    "inc_url": "https://api.binance.com/api/v3/ticker/price",
                    "inc_code": "symbol",
                },
            },
        ],
        outflow="examples/fjord_code/crypto_spread.py",
        # fjord writes incrementally per export tick — same append constraint
        # as stream().  NDJSON / CSV / SQLite / Avro accept appends; Parquet /
        # Feather / ORC / Excel / XML / JSON do not.
        export_params={"file_path": "data/crypto_spread.ndjson"},
        refresh_interval=30.0,                              # each source re-fetches every 30 s
        export_interval=60.0,                               # fused spread rows write every 60 s
    ):
        op = wave.operation                                 # e.g. "fjord_refresh:CoinGecko"
        print(f"{op:40s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
