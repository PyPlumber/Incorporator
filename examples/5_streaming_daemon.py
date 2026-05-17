"""
Streaming Daemon Tutorial: Live SpaceX Launch Watcher
-----------------------------------------------------
Companion script for `docs/5_streaming_daemon.md`.

Demonstrates `stream()` — a long-running daemon that periodically refreshes
a single source and flushes snapshots to disk on its own cadence.

Source: SpaceX `/v4/launches/upcoming` — returns ~18 upcoming launches
as a list, so each refresh wave processes many records instead of the
single record that `/v4/launches/latest` returns.

`LoggedIncorporator` + `enable_logging=True` routes every wave through
the background QueueHandler into rotating JSON-line log files
(logs/api.log, logs/error.log, logs/debug.log) — disk I/O never blocks
the event loop.

Run with:
    python examples/5_streaming_daemon.py

Ctrl+C / SIGTERM triggers a graceful drain.
"""

import asyncio
from pathlib import Path

from incorporator import LoggedIncorporator

HERE = Path(__file__).parent


class Launch(LoggedIncorporator):
    """SpaceX latest-launch tracker.

    Passing ``enable_logging=True`` to ``stream()`` below routes every
    wave through the background QueueHandler so the event loop stays
    unblocked.
    """


async def main() -> None:
    # stream() has TWO modes (selected by ``stateful_polling``):
    #   * False (default) = "chunking mode": every wave is a fresh incorp,
    #     state is released between chunks.  Exits when the source has no
    #     more chunks — handy for paginated catalogues you want to drain
    #     once.
    #   * True            = "stateful daemon": seed once, keep the registry
    #     live, refresh + export on independent cadences until Ctrl+C.
    #     This is the production-watcher shape — what we want here.
    #
    # Export format note: stream() writes incrementally on every export
    # wave, so the target must accept append mode: NDJSON / CSV / SQLite /
    # Avro.  Parquet / Feather / ORC / Excel / XML / JSON reject appends
    # (footer-indexed or monolithic encodings).
    async for wave in Launch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
            "inc_code": "id",
            "inc_name": "name",
        },
        stateful_polling=True,                                  # live registry, not one-shot
        refresh_interval=30.0,                                  # re-fetch every 30 seconds
        export_params={"file_path": str(HERE.parent / "data/spacex_upcoming.ndjson")},
        export_interval=90.0,                                   # flush every 90 seconds
        enable_logging=True,                                    # opt into JSON-line logs
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
