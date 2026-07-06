"""
Tutorial 11 — Tideweaver: Multi-Exchange Arbitrage Scanner Diamond
------------------------------------------------------------------
Companion script for ``examples/11-tideweaver/README.md``.

Demonstrates a four-current diamond against three crypto exchanges:

    binance + coinbase + kraken   →   best_market (fjord flush)

Three head/middle Stream currents pull each exchange's top-of-book feed
on independent intervals; the tail Fjord current snapshots all three
registries, normalizes symbols to a canonical asset code, computes the
cross-venue best bid / best ask / spread, and flags arb opportunities.

To stay runnable without hitting live exchange APIs (and without
burning quota or risking geo-blocks), all three sources read from the
local JSON snapshot files in ``fixtures/`` — hand-crafted slices that
mimic each exchange's response shape.  Real pipelines swap
``inc_file`` for ``inc_url`` against the live endpoints; the rest of
the watershed stays identical.

Run with:
    python examples/11-tideweaver/arb_scanner.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Fjord, Stream, Tideweaver, Watershed
from incorporator.schema.converters import calc

HERE = Path(__file__).resolve().parent
SNAPSHOT_DIR = HERE / "fixtures"
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via ``python -m`` or
# from a working directory other than HERE.  Python only auto-adds the
# script's directory to sys.path for ``python <script>`` invocations.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# Reuse the same class definitions + outflow() that the CLI watershed.json
# loads, so the Python and JSON entry points stay in lockstep.
from outflow import (  # noqa: E402, F401
    BestMarket,
    BinanceBook,
    CoinbaseTicker,
    KrakenTicker,
    normalize_asset,
)


async def main() -> None:
    # Output lives next to the script so you can inspect each run's arb
    # signals after the watershed drains.  Append mode preserves prior
    # runs — delete OUT/arb_signals.ndjson before re-running for a clean
    # log.  ``examples/**/out/`` is gitignored, so nothing leaks into git.
    out_file = OUT / "arb_signals.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="binance",
            cls=BinanceBook,
            interval=3.0,
            incorp_params={
                "inc_file": str(SNAPSHOT_DIR / "binance_book.json"),
                "inc_code": "symbol",
                "conv_dict": {
                    "asset": calc(normalize_asset, "symbol", default=None),
                    "bid": calc(float, "bidPrice", default=0.0, target_type=float),
                    "ask": calc(float, "askPrice", default=0.0, target_type=float),
                },
            },
        ),
        middle=[
            Stream(
                name="coinbase",
                cls=CoinbaseTicker,
                interval=3.0,
                incorp_params={
                    "inc_file": str(SNAPSHOT_DIR / "coinbase_ticker.json"),
                    "inc_code": "trade_id",
                    "conv_dict": {
                        "asset": calc(normalize_asset, "product_id", default=None),
                        "bid": calc(float, "bid", default=0.0, target_type=float),
                        "ask": calc(float, "ask", default=0.0, target_type=float),
                    },
                },
            ),
            Stream(
                name="kraken",
                cls=KrakenTicker,
                interval=3.0,
                incorp_params={
                    "inc_file": str(SNAPSHOT_DIR / "kraken_ticker.json"),
                    # Kraken's raw pair key is "_key"; Pk-bind (pass 4) runs
                    # AFTER name_chg (pass 3), so inc_code targets the
                    # renamed field, not the raw leading-underscore one.
                    "inc_code": "pair",
                    "name_chg": [("_key", "pair")],
                    "conv_dict": {
                        # conv_dict (pass 2) runs BEFORE name_chg (pass 3), so
                        # this must reference the RAW pre-rename key "_key".
                        "asset": calc(normalize_asset, "_key", default=None),
                        # "b"/"a" are 3-element [price, wholeLotVolume,
                        # lotVolume] string lists -- index 0 drills the price.
                        "bid": calc(float, "b.0", default=0.0, target_type=float),
                        "ask": calc(float, "a.0", default=0.0, target_type=float),
                    },
                },
            ),
        ],
        tail=Fjord(
            name="best_market",
            cls=BestMarket,
            interval=3.0,
            export_params={
                "file_path": str(out_file),
                "format": "ndjson",
                "if_exists": "append",
            },
        ),
        outflow=str(OUTFLOW_PATH),
        drain_timeout=10.0,
    )

    print("Running 3-exchange arb-scanner diamond for 15 s...\n")
    async for tide in Tideweaver(watershed).run():
        print(
            f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<32} "
            f"| skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )

    if out_file.exists():
        rows = out_file.read_text(encoding="utf-8").splitlines()
        print(f"\nWrote {len(rows)} best-market rows to {out_file}")
        for line in rows[-4:]:
            print(f"  {line}")


if __name__ == "__main__":
    asyncio.run(main())
