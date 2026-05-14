"""
Streaming Daemon Tutorial: Live SpaceX Launch Watcher
-----------------------------------------------------
Companion script for `docs/6_streaming_daemon.md`.

Demonstrates `stream()` — a long-running daemon that periodically refreshes
a single source and flushes snapshots to disk on its own cadence. Subclass
`LoggedIncorporator` so every wave is captured in rotating JSON-line log
files (logs/api.log, logs/error.log, logs/debug.log) — disk I/O never
blocks the event loop.

Run with:
    python examples/6_streaming_daemon.py

Ctrl+C / SIGTERM triggers a graceful drain.
"""

import asyncio

from incorporator import LoggedIncorporator


class Launch(LoggedIncorporator):
    """SpaceX latest-launch tracker. enable_logging=True routes every wave
    through the background QueueHandler so the event loop stays unblocked."""

    enable_logging = True


async def main() -> None:
    async for wave in Launch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/latest",
            "inc_code": "id",
            "inc_name": "name",
        },
        refresh_interval=60.0,                                  # re-fetch every minute
        export_params={"file_path": "data/spacex_latest.parquet"},
        export_interval=300.0,                                  # flush every 5 minutes
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
