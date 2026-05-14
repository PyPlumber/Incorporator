"""
Streaming Daemon Tutorial: Live SpaceX Launch Watcher
-----------------------------------------------------
Companion script for `docs/6_streaming_daemon.md`.

Demonstrates `stream()` — a long-running daemon that periodically refreshes
a single source and flushes snapshots to disk on its own cadence. Subclass
`LoggedIncorporator` and pass `enable_logging=True` on the verb call so
every wave is captured in rotating JSON-line log files (logs/api.log,
logs/error.log, logs/debug.log) — disk I/O never blocks the event loop.

Run with:
    python examples/6_streaming_daemon.py

Ctrl+C / SIGTERM triggers a graceful drain.
"""

import asyncio

from incorporator import LoggedIncorporator


class Launch(LoggedIncorporator):
    """SpaceX latest-launch tracker.

    Passing ``enable_logging=True`` to ``stream()`` below routes every
    wave through the background QueueHandler so the event loop stays
    unblocked.
    """


async def main() -> None:
    # stream() has TWO modes (selected by ``stateful_polling``):
    #   * False (default) = "chunking mode": every tick is a fresh incorp,
    #     state is released between chunks.  Exits when the source has no
    #     more chunks — handy for paginated catalogues you want to drain
    #     once.
    #   * True            = "stateful daemon": seed once, keep the registry
    #     live, refresh + export on independent cadences until Ctrl+C.
    #     This is the production-watcher shape — what we want here.
    #
    # Export format note: stream() writes incrementally on every export
    # tick, so the target must accept append mode: NDJSON / CSV / SQLite /
    # Avro.  Parquet / Feather / ORC / Excel / XML / JSON reject appends
    # (footer-indexed or monolithic encodings).
    async for wave in Launch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/latest",
            "inc_code": "id",
            "inc_name": "name",
        },
        stateful_polling=True,                                  # live registry, not one-shot
        refresh_interval=60.0,                                  # re-fetch every minute
        export_params={"file_path": "data/spacex_latest.ndjson"},
        export_interval=300.0,                                  # flush every 5 minutes
        enable_logging=True,                                    # opt into JSON-line logs
    ):
        if wave.failed_sources:
            print(f"⚠️  {wave.operation} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {wave.operation} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
