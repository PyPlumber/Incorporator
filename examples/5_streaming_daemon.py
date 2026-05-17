"""
Tutorial 5 — Streaming Daemon: Two Polling Modes for Two Real Use Cases
-----------------------------------------------------------------------
Companion script for `docs/5_streaming_daemon.md`.

Demonstrates the two engines that `stream()` exposes via the
`stateful_polling` flag:

1. ``stateful_demo`` — ``stateful_polling=True`` against Binance's
   live ticker.  Live registry, refresh in place every 30 s, snapshot
   to NDJSON every 5 min.  This is the long-running dashboard pattern;
   Ctrl+C / SIGTERM drains gracefully.
2. ``chunking_demo`` — ``stateful_polling=False`` (default) against
   paginated CoinGecko ``/coins/markets``.  Each wave is one page;
   chunks appended to NDJSON; daemon exits cleanly when the paginator
   exhausts.  This is a one-shot bulk-drain pattern.

``main()`` runs only the chunking demo by default (it terminates).
Uncomment the ``await stateful_demo()`` call to run the long-running
Binance ticker daemon instead.

Run with:
    python examples/5_streaming_daemon.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from incorporator import LoggedIncorporator
from incorporator.io.pagination import PageNumberPaginator


DATA = Path("data")
DATA.mkdir(exist_ok=True)


# ----------------------------------------------------------------------
# Part 1 — Stateful daemon: live Binance ticker dashboard
# ----------------------------------------------------------------------


class BinancePair(LoggedIncorporator):
    """Live ticker registry — auto-keyed by trading symbol."""


async def stateful_demo() -> None:
    """Long-running daemon: refresh every 30 s, snapshot every 5 min."""
    print("📈 Starting Binance ticker daemon (Ctrl+C to drain)...\n")
    async for wave in BinancePair.stream(
        incorp_params={
            "inc_url": "https://api.binance.us/api/v3/ticker/24hr",
            "inc_code": "symbol",
            "inc_name": "symbol",
        },
        stateful_polling=True,
        refresh_interval=30,
        export_params={"file_path": DATA / "binance_ticker.ndjson"},
        export_interval=300,
        enable_logging=True,
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(
                f"✅ {wave.operation} chunk {wave.chunk_index}: "
                f"{wave.rows_processed} pairs"
            )


# ----------------------------------------------------------------------
# Part 2 — Chunking daemon: one-shot bulk drain of CoinGecko
# ----------------------------------------------------------------------


class CoinPage(LoggedIncorporator):
    """One transient chunk of CoinGecko market rows per wave."""


async def chunking_demo(max_pages: int = 3) -> None:
    """One-shot drain of paginated CoinGecko markets.

    `max_pages` caps the run for demo purposes so we don't burn quota
    walking the full catalogue (40+ pages at per_page=250).  In a real
    backfill, drop the cap and let the paginator run to exhaustion.
    """
    print("📦 Starting CoinGecko paginated drain (chunking mode)...\n")
    paginator = PageNumberPaginator(page_param="page", start_page=1)
    paginator.call_lim = max_pages                  # safety cap for the demo

    out = DATA / "coins_full.ndjson"
    if out.exists():
        out.unlink()                                # start fresh

    async for wave in CoinPage.stream(
        incorp_params={
            "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
            "params": {"vs_currency": "usd", "per_page": 250},
            "inc_code": "id",
            "inc_name": "name",
            "inc_page": paginator,
            "excl_lst": ["image"],
        },
        export_params={
            "file_path": out,
            "if_exists": "append",
        },
        enable_logging=True,
    ):
        if wave.failed_sources:
            print(f"⚠️  page {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"📦 page {wave.chunk_index}: {wave.rows_processed} coins")

    print(f"\n✅ Drain complete. Output: {out} ({out.stat().st_size:,} bytes)")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------


async def main() -> None:
    # Default: run the chunking demo (terminates).
    await chunking_demo()
    # Uncomment to run the long-running stateful daemon (Ctrl+C to drain):
    # await stateful_demo()


if __name__ == "__main__":
    asyncio.run(main())
