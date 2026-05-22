"""
Tutorial 6 — SpaceX Launches: Parent-Child + Streaming
------------------------------------------------------
Companion script for `examples/06-spacex-launches/README.md`.

Re-runs the patterns from Tutorial 5 (parent-child drilling) and the
single-source stateful_polling=True shim from Tutorial 8 (streaming
daemon) against the SpaceX v4 public API.  No new framework concepts
here — read T5 / T8 first; this script demonstrates that the verbs
are domain-agnostic.

Two demos in one file:

1. ``parent_child_demo`` — launches → rockets + launchpads, concurrent
   two-way drill, O(1) three-way join.
2. ``streaming_demo`` — periodic refresh of the upcoming-launch feed,
   with NDJSON snapshot exports every 5 minutes.  This one is a
   long-running daemon; ``main()`` runs only the parent-child demo by
   default.  Uncomment the ``await streaming_demo()`` call to run the
   daemon.

Run with:
    python examples/06-spacex-launches/spacex_launches.py
"""

import asyncio
from pathlib import Path

from incorporator import Incorporator, LoggedIncorporator

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


# ----------------------------------------------------------------------
# Demo 1: Parent-Child Drilling
# ----------------------------------------------------------------------


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
    pass


class Pad(Incorporator):
    pass


async def parent_child_demo() -> None:
    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    print(f"✅ Loaded {len(launches)} upcoming launches.")

    # Concurrent fan-out — dedup collapses ~36 child refs into ~5 unique IDs.
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
    print(f"✅ Loaded {len(rockets)} unique rockets, {len(pads)} unique launchpads.\n")

    # O(1) three-way join.
    header = f"{'LAUNCH':<32} {'ROCKET':<14} {'PAD':<20} {'REGION':<12} {'SUCCESS':>8}"
    print("=" * len(header))
    print(header)
    print("=" * len(header))
    for launch in launches[:15]:
        rocket = Rocket.inc_dict.get(launch.rocket)
        pad = Pad.inc_dict.get(launch.launchpad)
        rocket_name = rocket.name if rocket else "?"
        pad_name = (pad.name if pad else "?")[:20]
        region = (pad.region if pad else "?")[:12]
        success = (
            f"{pad.launch_successes}/{pad.launch_attempts}"
            if pad and pad.launch_attempts
            else "—"
        )
        name = (launch.name or "Unknown")[:32]
        print(f"{name:<32} {rocket_name:<14} {pad_name:<20} {region:<12} {success:>8}")

    # Structured view (preferred): ``rockets.rejects`` / ``pads.rejects``
    # carry per-source ``error_kind`` / ``retry_after`` / ``wave_index``
    # (one ``RejectEntry`` per failed source) — see Tutorial 6's "Reading
    # the structured reject list" section.
    if rockets.failed_sources or pads.failed_sources:
        print(f"\n⚠️  Failed: rockets={rockets.failed_sources}, pads={pads.failed_sources}")


# ----------------------------------------------------------------------
# Demo 2: Streaming Daemon (long-running; opt-in)
# ----------------------------------------------------------------------


class StreamedLaunch(LoggedIncorporator):
    """Same Launch entity, but with LoggedIncorporator for non-blocking log shipping."""


async def streaming_demo() -> None:
    """Long-running daemon — Ctrl+C / SIGTERM to drain gracefully."""
    async for wave in StreamedLaunch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
            "inc_code": "id",
            "inc_name": "name",
        },
        stateful_polling=True,
        refresh_interval=120,        # poll every 2 min
        export_params={"file_path": str(OUT / "launches.ndjson")},
        export_interval=300,         # flush every 5 min
        enable_logging=True,
    ):
        if wave.failed_sources:
            print(f"⚠️  Failures in chunk {wave.chunk_index}: {wave.failed_sources}")


async def main() -> None:
    await parent_child_demo()
    # Uncomment to run the long-running daemon (Ctrl+C to drain):
    # await streaming_demo()


if __name__ == "__main__":
    asyncio.run(main())


# ----------------------------------------------------------------------
# Tideweaver integration sketch (illustrative, not executed)
# ----------------------------------------------------------------------
#
# This script demonstrates parent-child drilling and the single-source
# stateful_polling shim.  When you outgrow that — e.g. you want the
# launch feed gated behind a Weir on a downstream Export current that
# snapshots the joined launch/rocket/pad object graph every 5 minutes —
# wrap the feed in a Tideweaver edge:
#
#   from datetime import datetime, timedelta, timezone
#   from incorporator import Tideweaver, Watershed, Stream, Export
#   from incorporator.observability.tideweaver import (
#       Edge, FlowControl, Weir, BurstPenstock, LoggingObserver,
#   )
#
#   feed = Stream(
#       name="upcoming",
#       cls=StreamedLaunch,
#       interval=120,
#       incorp_params={
#           "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
#           "inc_code": "id",
#       },
#   )
#   snapshot = Export(
#       name="snapshot",
#       cls=StreamedLaunch,
#       interval=300,
#       export_params={"file_path": "out/launches.ndjson"},
#       depends_on=["upcoming"],
#   )
#   edge_flow = FlowControl(
#       gate=Weir(),                                       # fire on fresh wave
#       penstock=BurstPenstock(rate_per_sec=1.0, burst=2), # smooth bursts
#       observer=LoggingObserver(fire_level="info"),       # per-edge telemetry
#   )
#   window = (datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(hours=4))
#   watershed = Watershed(
#       window=window,
#       currents=[feed, snapshot],
#       edges=[Edge(from_name="upcoming", to_name="snapshot", flow=edge_flow)],
#   )
#   # async for tide in Tideweaver(watershed).run():
#   #     ...
#
# For a runnable diamond pattern see examples/11-tideweaver/ or
# examples/appendix/nascar-tideweaver/.
