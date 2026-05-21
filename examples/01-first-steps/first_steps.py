"""
Tutorial 1 — First Steps with Incorporator: CoinGecko Market Data
-----------------------------------------------------------------
Companion script for `examples/01-first-steps/README.md`.

Two demos in one file, in the order the tutorial teaches them:

1. ``inspector_demo`` — the JIT API Profiler runs FIRST.  Swap ``.incorp()``
   for ``.test()`` to fetch one safe page and print the exact
   ``incorp()`` kwargs the framework recommends for the endpoint.
2. ``incorp_demo`` — apply those recommendations: top-100 coins by
   market cap, no schema declared, full dot-notation + O(1) registry.

Run with:
    python examples/01-first-steps/first_steps.py
"""

import asyncio

from incorporator import Incorporator, register_host_throttle
from incorporator.io.throttle import FixedIntervalThrottle

# Respect CoinGecko's anon-tier rate ceiling (5-15 req/min documented):
# 0.2 req/sec = 12 req/min, comfortably under.  The framework ships
# no implicit per-host throttling — register hosts you care about
# explicitly at startup (or pass `requests_per_second=` per call).
register_host_throttle("api.coingecko.com", lambda: FixedIntervalThrottle(0.2))


class Coin(Incorporator):
    pass


async def incorp_demo() -> None:
    # One call.  Zero schema.  Two kwargs that matter.
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 100, "page": 1},
        inc_code="id",                              # primary key for the registry
        inc_name="name",                            # display label
    )
    print(f"✅ Loaded {len(coins)} coins.\n")

    # Dot-notation access.
    btc = Coin.inc_dict["bitcoin"]
    print(f"  Name:      {btc.name}")
    print(f"  Symbol:    {btc.symbol.upper()}")
    print(f"  Price:     ${btc.current_price:,.2f}")
    print(f"  Rank:      #{btc.market_cap_rank}")
    print(f"  24h Δ:     {btc.price_change_percentage_24h:+.2f}%")

    # Iteration — IncorporatorList IS a list.
    print("\n  Top 5 coins by market cap:")
    for coin in coins[:5]:
        print(f"    #{coin.market_cap_rank} {coin.name} ({coin.symbol.upper()})")


async def inspector_demo() -> None:
    # ------------------------------------------------------------------
    # DX Inspector — hit the unknown endpoint via test().
    # ------------------------------------------------------------------
    # Safe: forces call_lim=1, short timeout, 3-record preview.
    print("\n" + "=" * 70)
    print("DX INSPECTOR DEMO — Coin.test(...)")
    print("=" * 70)
    await Coin.test(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 10},
    )
    # The kwargs in incorp_demo() above are exactly what the inspector
    # recommends for this endpoint.  See `examples/01-first-steps/README.md` for the
    # five-section report breakdown.


async def main() -> None:
    # Discovery first: profile the endpoint with test() and let the inspector
    # print the recommended kwargs.
    await inspector_demo()
    # Application second: paste those recommendations into a real incorp() call.
    await incorp_demo()


if __name__ == "__main__":
    asyncio.run(main())
