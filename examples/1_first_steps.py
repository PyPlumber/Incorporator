"""
Tutorial 1 — First Steps with Incorporator: CoinGecko Market Data
-----------------------------------------------------------------
Companion script for `docs/1_first_steps.md`.

Smallest meaningful Incorporator program: one API call against
CoinGecko's top-100-by-market-cap endpoint, no schema declared, full
dot-notation + O(1) registry.

Run with:
    python examples/1_first_steps.py
"""

import asyncio

from incorporator import Incorporator


class Coin(Incorporator):
    pass


async def main() -> None:
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


if __name__ == "__main__":
    asyncio.run(main())
