"""
Tutorial 1 — First Steps with Incorporator: CoinGecko Market Data
-----------------------------------------------------------------
Companion script for `docs/1_first_steps.md`.

Two demos in one file:

1. ``incorp_demo`` — smallest meaningful Incorporator program: one API
   call against CoinGecko's top-100-by-market-cap endpoint, no schema
   declared, full dot-notation + O(1) registry.
2. ``test_demo`` — the JIT API Profiler. Swap ``.incorp()`` for
   ``.test()`` to fetch one safe page and print the exact ``incorp()``
   kwargs the framework recommends.

Run with:
    python examples/1_first_steps.py
"""

import asyncio

from incorporator import Incorporator


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


async def test_demo() -> None:
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
    # recommends for this endpoint.  See `docs/1_first_steps.md` for the
    # five-section report breakdown.


async def main() -> None:
    await incorp_demo()
    await test_demo()


if __name__ == "__main__":
    asyncio.run(main())
