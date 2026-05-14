"""
Multi-Source Fjord Tutorial: SpaceX Launches + Rocket Specs
-----------------------------------------------------------
Companion script for `docs/7_multi_source_fjord.md`.

`stream()` watches one source. `fjord()` watches N sources concurrently
and lets you fuse them through a user-defined `outflow(state)` function.

Each source refreshes on its own cadence; the outflow daemon snapshots
every source under a shared lock and calls `outflow()` in a worker
thread so a heavy join can't block the refresh daemons.

The dynamic output class is built from the outflow filename stem —
`launch_with_rocket.py` → `LaunchWithRocket`. No output class to declare.

Run with:
    python examples/7_multi_source_fjord.py
"""

import asyncio

from incorporator import Incorporator

# Bring the source classes into scope so fjord() can register them.
from examples.fjord_code.launch_with_rocket import SpaceXLaunch, SpaceXRocket


async def main() -> None:
    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": SpaceXLaunch,
                "incorp_params": {
                    "inc_url": "https://api.spacexdata.com/v4/launches/latest",
                    "inc_code": "id",
                },
            },
            {
                "cls": SpaceXRocket,
                "incorp_params": {
                    "inc_url": "https://api.spacexdata.com/v4/rockets",
                    "inc_code": "id",
                },
            },
        ],
        outflow="examples/fjord_code/launch_with_rocket.py",
        export_params={"file_path": "data/launch_with_rocket.parquet"},
        refresh_interval=60.0,                              # each source re-fetches every minute
        export_interval=120.0,                              # fused output writes every 2 minutes
    ):
        op = wave.operation                                 # e.g. "fjord_refresh:SpaceXLaunch"
        print(f"{op:40s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
