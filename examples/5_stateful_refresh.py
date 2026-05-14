"""
Tutorial 5 — Stateful Refresh: Live Binance Ticker
--------------------------------------------------
Companion script for `docs/5_stateful_refresh.md`.

Demonstrates the three `refresh()` resolution modes against Binance's
public ticker feed (~1,900 pairs, no auth required). Each call
mutates the existing Pydantic instances in place — local Python
references survive across refreshes without reassignment.

Run with:
    python examples/5_stateful_refresh.py
"""

import asyncio

from incorporator import Incorporator


class Pair(Incorporator):
    """Binance trading pair — auto-keyed by ``symbol``."""


async def main() -> None:
    # ------------------------------------------------------------------
    # 1. INITIAL LOAD — ~1,900 pairs in one HTTP call.
    # ------------------------------------------------------------------
    pairs = await Pair.incorp(
        inc_url="https://api.binance.com/api/v3/ticker/24hr",
        inc_code="symbol",
    )
    print(f"✅ Loaded {len(pairs)} trading pairs from Binance.")
    btc = Pair.inc_dict["BTCUSDT"]
    eth = Pair.inc_dict["ETHUSDT"]
    print(f"   BTCUSDT lastPrice: {btc.lastPrice}")
    print(f"   ETHUSDT lastPrice: {eth.lastPrice}")

    # ------------------------------------------------------------------
    # 2. IN-STATE REFRESH — no args.
    # ------------------------------------------------------------------
    # Identity-mapping memory: the framework remembers inc_code='symbol'
    # from the initial incorp call, so refresh() doesn't need it re-passed.
    # `btc` and `eth` are the same Python objects — their fields get
    # mutated in place.
    print("\n⏳ Waiting 2 seconds for the market to move...")
    await asyncio.sleep(2)

    await Pair.refresh()

    print("🔄 In-state refresh complete.")
    print(f"   BTCUSDT lastPrice: {btc.lastPrice}      (same object, new value)")
    print(f"   ETHUSDT lastPrice: {eth.lastPrice}")

    # ------------------------------------------------------------------
    # 3. RE-SOURCE REFRESH — repoint at a different endpoint.
    # ------------------------------------------------------------------
    # /ticker/price returns the same symbols but only the latest price —
    # lighter payload when you don't need 24-hour volume / high / low.
    await Pair.refresh("https://api.binance.com/api/v3/ticker/price")
    print("\n🔁 Re-sourced from /ticker/price (lighter endpoint).")
    print(f"   BTCUSDT current price: {Pair.inc_dict['BTCUSDT'].price}")

    # ------------------------------------------------------------------
    # 4. TARGETED REFRESH — refresh a chosen subset.
    # ------------------------------------------------------------------
    # The framework dedups to the single class URL on single-URL
    # registries, but the API form is honored — useful when you've
    # flagged specific pairs stale and want clear intent in the code.
    my_pairs = [Pair.inc_dict[s] for s in ("BTCUSDT", "ETHUSDT")]
    await Pair.refresh(instance=my_pairs)
    print(f"\n🎯 Targeted refresh of {len(my_pairs)} pairs.")
    print(f"   BTCUSDT current price: {Pair.inc_dict['BTCUSDT'].price}")

    # Any failed sources surface on the result list for DLQ retry.
    # (See docs/debugging.md for the LoggedIncorporator + get_error pattern.)
    if pairs.failed_sources:
        print(f"⚠️  Failed sources during initial load: {pairs.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
