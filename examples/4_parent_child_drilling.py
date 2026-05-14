"""
Tutorial 4 — Parent → Child Drilling: SpaceX Launches + Rockets
---------------------------------------------------------------
Companion script for `docs/4_parent_child_drilling.md`.

Two `incorp()` calls build two registries — launches and rockets —
then join them by rocket ID in O(1).  The framework handles the
parent→child fan-out concurrently, dedupes the rocket IDs (most
upcoming launches share a handful of rocket types), and retries on
transient failure.

Run with:
    python examples/4_parent_child_drilling.py
"""

import asyncio

from incorporator import Incorporator


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
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
    # PHASE 2 — Drill each launch's `rocket` field into the rockets endpoint.
    # ------------------------------------------------------------------
    # `inc_url` carries a single `{}` slot.  The framework extracts each
    # launch's `rocket` ID, dedups the set, and fans out concurrent
    # requests to /v4/rockets/{rocket_id}.
    rockets = await Rocket.incorp(
        inc_url="https://api.spacexdata.com/v4/rockets/{}",
        inc_parent=launches,
        inc_child="rocket",
        inc_code="id",
    )
    print(f"✅ Loaded {len(rockets)} unique rockets.")

    # ------------------------------------------------------------------
    # PHASE 3 — O(1) in-memory join.
    # ------------------------------------------------------------------
    print("\n" + "=" * 78)
    print(f"{'LAUNCH':<40} {'ROCKET':<22} {'HEIGHT':>12}")
    print("=" * 78)
    for launch in launches[:15]:
        rocket = Rocket.inc_dict.get(launch.rocket)
        rocket_name = rocket.name if rocket else "?"
        try:
            height = f"{rocket.height.meters:.1f} m" if rocket else "?"
        except AttributeError:
            height = "?"
        name = (launch.name or "Unknown")[:40]
        print(f"{name:<40} {rocket_name:<22} {height:>12}")

    # Any failed sources surface on the result list for DLQ retry.
    if rockets.failed_sources:
        print(f"\n⚠️  Failed rocket lookups: {rockets.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
