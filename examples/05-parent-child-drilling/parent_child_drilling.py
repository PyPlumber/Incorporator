"""
Tutorial 5 — Parent → Child Drilling: CoinGecko Top-N → /coins/{id}
-------------------------------------------------------------------
Companion script for `examples/05-parent-child-drilling/README.md`.

Two `incorp()` calls build two registries — lightweight market rows
(parent) and full per-coin detail records (child) — then join them by
ID in O(1).  The framework dedups parent IDs, fans out the children
concurrently through one shared HTTP/2 client, retries on transient
failure, and surfaces any RejectEntry records on `.rejects`.

**Rate-limit note.**  CoinGecko's free public tier is 5–15 requests
per *minute* (not per second).  The framework ships no implicit
per-host throttling; this script calls ``register_host_penstock`` at
startup to pace ``api.coingecko.com`` at 0.2 req/sec (12/min).  The
explicit ``requests_per_second`` kwarg further down documents the same
knob for per-call override scenarios.

Set ``COINGECKO_DEMO_API_KEY`` in your environment to use CoinGecko's
free Demo plan (30 req/min stable, requires email signup at
https://www.coingecko.com/en/developers/dashboard).  The script reads
the env var and bumps the throttle automatically when present.

Run with:
    python examples/05-parent-child-drilling/parent_child_drilling.py
"""

import asyncio
import os

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import inc
from incorporator.schema.extractors import pluck

# Pace api.coingecko.com at 0.2 req/sec (12/min — comfortably under
# the 5-15/min free-tier ceiling).
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# Build-time lift of the nested `links.homepage` path and an ASCII default for
# `genesis_date` — collapses the read-time null-guard pyramid to plain attrs.
# CoinGecko can omit `links` entirely (memecoins / new listings); pluck()
# resolves missing path segments to None rather than raising, so the read-time
# loop still needs one `or []` guard for that None-vs-[] case (honest
# boundary, not a design flaw).
COINDETAIL_CONV_DICT = {
    "links_homepage": pluck("links.homepage"),
    "genesis_date": inc(str, default="-"),
}


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
        print("OK: Using CoinGecko Demo API key (30 req/min).")
    else:
        print("INFO: No COINGECKO_DEMO_API_KEY set - running anonymous (12 req/min).")

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
    print(f"OK: Loaded {len(coins)} parent market rows.")

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
        conv_dict=COINDETAIL_CONV_DICT,
    )
    print(f"OK: Drilled {len(details)} per-coin detail records.\n")

    # ------------------------------------------------------------------
    # PHASE 3 — Application-side O(1) two-way join.
    # ------------------------------------------------------------------
    # Each Incorporator subclass keeps its own inc_dict.  The join lives
    # in this loop; the framework gives you O(1) lookups on both sides.
    # Honest read-time boundary: CoinDetail is drilled per-coin via
    # inc_parent/inc_child (T5's core pattern) — the two-registry join is
    # deliberately read-time; see "Two registries, manual join" in the README.
    header = f"{'COIN':<14} {'PRICE':>14} {'GENESIS':<12} HOMEPAGE"
    print("=" * 80)
    print(header)
    print("=" * 80)
    for coin in coins:
        detail = CoinDetail.inc_dict.get(coin.id)
        if detail is None:
            continue
        homepage_list = detail.links_homepage or []
        homepage = (homepage_list[0] if homepage_list else "")[:38]
        print(f"{coin.name:<14} ${coin.current_price:>12,.2f} {detail.genesis_date:<12} {homepage}")

    # Failed sources surface on each result list for reject-retry workflows.
    # Structured view (preferred): ``details.rejects`` carries per-source
    # ``error_kind`` / ``retry_after`` / ``wave_index`` (one ``RejectEntry``
    # per failed source).
    if details.failed_sources:
        print(f"\nWARN: Failed detail drills: {details.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
