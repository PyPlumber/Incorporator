"""
Tutorial 8 — Streaming Daemon: Paginated Bulk Export at O(1) Memory
-------------------------------------------------------------------
Companion script for `examples/08-streaming-daemon/README.md`.

``stream()``'s canonical job is paginated bulk-export chunking: drain
a multi-page source one page at a time, append each chunk to disk,
release the chunk, repeat — peak memory pinned at one page regardless
of dataset size.  ``chunking_demo`` below shows that pattern against
CoinGecko's ``/coins/markets`` catalogue.

For live single-source registries that need to stay hot in memory and
snapshot to disk on a cadence, ``stream(stateful_polling=True)``
survives as a compatibility shim over the fjord engine.
``stateful_demo`` shows that path against Binance's live ticker.
For multi-source live registries or any cross-source join, reach
for ``fjord()`` directly (see Tutorial 10) — the shim is single-
source only.

``main()`` toggles between the two demos so the file is runnable
either way — flip which call is commented in / out.

Run with:
    python examples/08-streaming-daemon/streaming_daemon.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from incorporator import LoggedIncorporator, register_host_penstock
from incorporator.io.pagination import PageNumberPaginator
from incorporator.io.penstock import SustainedPenstock

# Pace api.coingecko.com at 0.2 req/sec (12/min) — the free-tier ceiling
# is 5-15/min documented.
register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))


HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


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
        export_params={"file_path": OUT / "binance_ticker.ndjson"},
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
    # NOTE: ``paginator.call_lim`` is clobbered every wave by stream()'s
    # chunked engine (it forces call_lim=1 per wave for O(1) memory).
    # The demo cap lives in the consumer loop instead — break after
    # ``max_pages`` waves have come through.  In a real backfill, drop
    # the break and let the paginator run to exhaustion naturally.
    paginator = PageNumberPaginator(page_param="page", start_page=1)

    out = OUT / "coins_full.ndjson"
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
        refresh_params=None,                # chunking mode: opt out of per-chunk refresh
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
        if wave.chunk_index >= max_pages:
            break                            # demo cap; remove for full drain

    if out.exists():
        print(f"\n✅ Drain complete. Output: {out} ({out.stat().st_size:,} bytes)")
    else:
        print("\n⚠️  Drain produced no output — likely rate-limited or empty source.")


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
