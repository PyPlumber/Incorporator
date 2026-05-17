"""
Tutorial 3 — Parent → Child Drilling: SpaceX Launches + Rockets + Launchpads
----------------------------------------------------------------------------
Companion script for `docs/3_parent_child_drilling.md`.

Three `incorp()` calls build three registries — launches, rockets,
and launchpads — then join them by ID in O(1).  The framework fans
out each parent → child drill concurrently, dedupes IDs (only a
handful of pads / rocket types serve hundreds of upcoming launches),
and retries on transient failure.

Run with:
    python examples/3_parent_child_drilling.py
"""

import asyncio

from incorporator import Incorporator


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
    pass


class Pad(Incorporator):
    pass


async def main() -> None:
    # ------------------------------------------------------------------
    # PHASE 1 — Load the parent list (upcoming launches).
    # ------------------------------------------------------------------
    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    print(f"✅ Loaded {len(launches)} upcoming launches.")

    # ------------------------------------------------------------------
    # PHASE 2 — Drill BOTH `rocket` and `launchpad` IDs in parallel.
    # ------------------------------------------------------------------
    # Each drill is an independent incorp() call against the same parent
    # registry.  The framework extracts each launch's child ID, dedups
    # the set (5 unique pads cover 18 launches; only 2 rocket types), and
    # fans out concurrent requests through the shared HTTP/2 client.
    rockets, pads = await asyncio.gather(
        Rocket.incorp(
            inc_url="https://api.spacexdata.com/v4/rockets/{}",
            inc_parent=launches,
            inc_child="rocket",
            inc_code="id",
        ),
        Pad.incorp(
            inc_url="https://api.spacexdata.com/v4/launchpads/{}",
            inc_parent=launches,
            inc_child="launchpad",
            inc_code="id",
        ),
    )
    print(f"✅ Loaded {len(rockets)} unique rockets, {len(pads)} unique launchpads.")

    # ------------------------------------------------------------------
    # PHASE 3 — O(1) three-way join.
    # ------------------------------------------------------------------
    header = f"{'LAUNCH':<32} {'ROCKET':<14} {'PAD':<20} {'REGION':<18} {'SUCCESS':>8}"
    print("\n" + "=" * len(header))
    print(header)
    print("=" * len(header))
    for launch in launches[:15]:
        rocket = Rocket.inc_dict.get(launch.rocket)
        pad = Pad.inc_dict.get(launch.launchpad)

        rocket_name = rocket.name if rocket else "?"
        pad_name = (pad.name if pad else "?")[:20]
        region = (pad.region if pad else "?")[:18]
        success = (
            f"{pad.launch_successes}/{pad.launch_attempts}"
            if pad and pad.launch_attempts
            else "—"
        )
        name = (launch.name or "Unknown")[:32]
        print(f"{name:<32} {rocket_name:<14} {pad_name:<20} {region:<18} {success:>8}")

    # Failed sources surface on each result list for DLQ retry.
    if rockets.failed_sources or pads.failed_sources:
        print(f"\n⚠️  Failed: rockets={rockets.failed_sources}, pads={pads.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
