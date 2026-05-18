"""
Tutorial 3 — Parent → Child Drilling: CoinGecko Top-N → /coins/{id}
-------------------------------------------------------------------
Companion script for `docs/3_parent_child_drilling.md`.

Two `incorp()` calls build two registries — lightweight market rows
(parent) and full per-coin detail records (child) — then join them by
ID in O(1).  The framework dedups parent IDs, fans out the children
concurrently through one shared HTTP/2 client, retries on transient
failure, and surfaces any DLQ entries on `failed_sources`.

**Rate-limit note.**  CoinGecko's free public tier is 5–15 requests
per *minute* (not per second).  Incorporator's host-aware registry
auto-paces calls against `api.coingecko.com` at 0.2 req/sec (12/min) —
the explicit ``requests_per_second`` kwarg below documents the throttle
so readers see the knob and can crank it up with an API key.

Set ``COINGECKO_DEMO_API_KEY`` in your environment to use CoinGecko's
free Demo plan (30 req/min stable, requires email signup at
https://www.coingecko.com/en/developers/dashboard).  The script reads
the env var and bumps the throttle automatically when present.

Run with:
    python examples/03-parent-child-drilling/parent_child_drilling.py
"""

import asyncio
import os

from incorporator import Incorporator


class Coin(Incorporator):
    """Lightweight market row from /coins/markets."""


class CoinDetail(Incorporator):
    """Full per-coin detail record from /coins/{id}."""


async def main() -> None:
    # ------------------------------------------------------------------
    # API-key opt-in: free Demo plan = 30 req/min stable.
    # ------------------------------------------------------------------
    demo_key = os.environ.get("COINGECKO_DEMO_API_KEY")
    headers = {"x-cg-demo-api-key": demo_key} if demo_key else None
    # 30/min with the key, 12/min without — both safely under CoinGecko's
    # respective ceilings.  Reader can override either way.
    rps = 0.5 if demo_key else 0.2
    if demo_key:
        print("🔑 Using CoinGecko Demo API key (30 req/min).")
    else:
        print("ℹ️  No COINGECKO_DEMO_API_KEY set — running anonymous (12 req/min).")

    # ------------------------------------------------------------------
    # PHASE 1 — Load the parent list (top 10 by market cap).
    # ------------------------------------------------------------------
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 10, "page": 1},
        inc_code="id",
        inc_name="name",
        excl_lst=["image"],
        headers=headers,
        requests_per_second=rps,
    )
    print(f"✅ Loaded {len(coins)} parent market rows.")

    # ------------------------------------------------------------------
    # PHASE 2 — Drill /coins/{id} for every parent, concurrently.
    # ------------------------------------------------------------------
    # The framework extracts each coin's `id`, dedups (10 unique → 10
    # requests), substitutes into the `{}` slot, and fans out through
    # the shared HTTP/2 client.  Heavy fields excluded to keep the
    # response footprint tight.
    #
    # ``requests_per_second`` paces the 10 child drills so they all
    # land inside CoinGecko's per-minute budget — without it the burst
    # would 429 the last few requests.
    details = await CoinDetail.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/{}",
        inc_parent=coins,
        inc_child="id",
        inc_code="id",
        excl_lst=["image", "tickers", "community_data", "developer_data"],
        headers=headers,
        requests_per_second=rps,
    )
    print(f"✅ Drilled {len(details)} per-coin detail records.\n")

    # ------------------------------------------------------------------
    # PHASE 3 — Application-side O(1) two-way join.
    # ------------------------------------------------------------------
    # Each Incorporator subclass keeps its own inc_dict.  The join lives
    # in this loop; the framework gives you O(1) lookups on both sides.
    header = f"{'COIN':<14} {'PRICE':>14} {'GENESIS':<12} HOMEPAGE"
    print("=" * 80)
    print(header)
    print("=" * 80)
    for coin in coins:
        detail = CoinDetail.inc_dict.get(coin.id)
        if detail is None:
            continue
        # Defensive guards: CoinGecko's /coins/{id} sometimes omits ``links``
        # (memecoins / new listings); ``genesis_date`` is null for many.  Use
        # ``getattr`` with defaults so the demo doesn't blow up on the first
        # incomplete record.
        links_obj = getattr(detail, "links", None)
        homepage_list = getattr(links_obj, "homepage", []) if links_obj else []
        homepage = (homepage_list[0] if homepage_list else "")[:38]
        genesis = getattr(detail, "genesis_date", None) or "—"
        print(
            f"{coin.name:<14} "
            f"${coin.current_price:>12,.2f} "
            f"{genesis:<12} "
            f"{homepage}"
        )

    # Failed sources surface on each result list for DLQ retry.
    if details.failed_sources:
        print(f"\n⚠️  Failed detail drills: {details.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
