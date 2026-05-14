"""
Tutorial 3 — DX Inspector: Let the Framework Write Your Kwargs
--------------------------------------------------------------
Companion script for `docs/3_dx_inspector.md`.

`test()` is the JIT API Profiler. Hand it the URL of any endpoint and
it fetches one safe page, walks the payload tree, runs value scoring
to detect identity-shaped fields, and prints the exact `incorp()`
kwargs you'd write yourself — minus the trial and error.

Run with:
    python examples/3_dx_inspector.py
"""

import asyncio

from incorporator import Incorporator


class Coin(Incorporator):
    """Placeholder subclass — test() doesn't need a real schema declared."""


async def main() -> None:
    # ------------------------------------------------------------------
    # STEP 1 — Hit the unknown endpoint via test()
    # ------------------------------------------------------------------
    # Safe: forces call_lim=1, short timeout, 3-record preview.
    await Coin.test(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 10},
    )

    # ------------------------------------------------------------------
    # STEP 2 — Paste the inspector's kwargs into a real incorp() call.
    # ------------------------------------------------------------------
    # Uncomment to run the round-trip yourself.  The kwargs below are
    # exactly what the inspector recommended in Step 1's printout.
    #
    # from datetime import datetime
    # from incorporator.schema.converters import inc
    #
    # coins = await Coin.incorp(
    #     inc_url="https://api.coingecko.com/api/v3/coins/markets",
    #     params={"vs_currency": "usd", "per_page": 10},
    #     inc_code="id",                                # from identity mapping
    #     inc_name="name",
    #     conv_dict={                                   # from type casting
    #         "last_updated": inc(datetime),
    #         "ath_date": inc(datetime),
    #     },
    #     excl_lst=["image"],                           # from heavy-field hints
    # )
    # btc = coins.inc_dict["bitcoin"]
    # print(f"BTC: ${btc.current_price:,.2f} (updated {btc.last_updated:%Y-%m-%d %H:%M})")


if __name__ == "__main__":
    asyncio.run(main())
