"""
Tutorial 4 — Stateful Refresh: Live Binance Ticker
--------------------------------------------------
Companion script for `docs/4_stateful_refresh.md`.

Demonstrates the three `refresh()` resolution modes against Binance's
public ticker feed (~1,900 pairs, no auth required). Each call
mutates the existing Pydantic instances in place — local Python
references survive across refreshes without reassignment.

Run with:
    python examples/04-stateful-refresh/stateful_refresh.py
"""

import asyncio

from incorporator import Incorporator


class Pair(Incorporator):
    """Binance trading pair — auto-keyed by ``symbol``."""


async def main() -> None:
    # ------------------------------------------------------------------
    # 1. INITIAL LOAD — ~600 pairs in one HTTP call.
    # ------------------------------------------------------------------
    # api.binance.com is geo-blocked in many regions (US, UK, Singapore)
    # — it returns 451 "Unavailable For Legal Reasons" rather than data.
    # api.binance.us is the US-licensed mirror with the same v3 endpoint
    # shape (fewer listed pairs — ~600 vs ~1,900 — but identical refresh
    # semantics).  Swap back to api.binance.com if you're outside those
    # regions and want the full pair universe.
    pairs = await Pair.incorp(
        inc_url="https://api.binance.us/api/v3/ticker/24hr",
        inc_code="symbol",
    )
    print(f"✅ Loaded {len(pairs)} trading pairs from Binance.")
    btc_before = Pair.inc_dict["BTCUSDT"].lastPrice
    print(f"   BTCUSDT lastPrice (before): {btc_before}")

    # ------------------------------------------------------------------
    # 2. IN-STATE REFRESH — no args.
    # ------------------------------------------------------------------
    # Identity-mapping memory: the framework remembers inc_code='symbol'
    # from the initial incorp call, so refresh() doesn't need it re-passed.
    # The canonical "what's the current value" lookup is Pair.inc_dict[...] —
    # refresh replaces the instances under the same keys, so a local var
    # captured before the refresh would now point at a stale model.
    print("\n⏳ Waiting 2 seconds for the market to move...")
    await asyncio.sleep(2)
    await Pair.refresh()

    btc_after = Pair.inc_dict["BTCUSDT"].lastPrice
    moved = "moved!" if btc_after != btc_before else "no change (Binance quiet)"
    print(f"🔄 In-state refresh complete.  BTCUSDT lastPrice: {btc_after}  ({moved})")

    # ------------------------------------------------------------------
    # 3. RE-SOURCE REFRESH — repoint at a different endpoint.
    # ------------------------------------------------------------------
    # /ticker/price returns the same symbols but only the latest price —
    # lighter payload when you don't need 24-hour volume / high / low.
    # The framework rebuilds every instance with the new endpoint's schema,
    # so the registry now exposes `.price` instead of `.lastPrice`.
    await Pair.refresh("https://api.binance.us/api/v3/ticker/price")
    print(f"\n🔁 Re-sourced from /ticker/price (lighter endpoint).")
    print(f"   BTCUSDT current price: {Pair.inc_dict['BTCUSDT'].price}")
    print(f"   cls.inc_url updated to: {Pair.inc_url}")

    # ------------------------------------------------------------------
    # 4. TARGETED REFRESH — refresh a chosen subset.
    # ------------------------------------------------------------------
    # The framework dedups to the single class URL on single-URL
    # registries (no per-instance origin tracking yet), but the API form
    # is honored — useful when you've flagged specific pairs stale and
    # want explicit intent in the code.
    my_pairs = [Pair.inc_dict[s] for s in ("BTCUSDT", "ETHUSDT")]
    await Pair.refresh(instance=my_pairs)
    print(f"\n🎯 Targeted refresh of {len(my_pairs)} pairs.")
    print(f"   BTCUSDT current price: {Pair.inc_dict['BTCUSDT'].price}")

    # Any failed sources surface on the result list for DLQ retry.
    # (See docs/debugging.md for the LoggedIncorporator + get_error pattern.)
    if pairs.failed_sources:
        print(f"\n⚠️  Failed sources during initial load: {pairs.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
