***

# 🌊 Multi-Source Fjord: Fusing SpaceX Launches with Rocket Specs

`stream()` watches **one** source. `fjord()` watches **N** sources
concurrently and lets you fuse them through a user-defined
`outflow(state)` function — the engine handles every concurrent refresh,
every export tick, the shared lock, the audit queue, and the dynamic
output class.

This tutorial builds a live fusion: every minute we refresh both the
SpaceX launches feed AND the rocket-specs feed; every two minutes we
join them by `rocket_id` and write the combined dataset to disk.

---

## The Goal

* **Source A:** `https://api.spacexdata.com/v4/launches/latest` — current launch
* **Source B:** `https://api.spacexdata.com/v4/rockets` — full rocket catalogue
* **Fusion:** for the current launch, attach the matching rocket's specs
  (name, height, mass, success_rate_pct)
* **Cadence:** sources refresh every 60 s; fused output writes every 120 s

Notice: no output class is declared. `fjord()` builds it dynamically from
the rows your `outflow()` returns, named after the code-file stem.

---

## Step 1: `outflow.py` — The Code File

`fjord()` needs Python code (class definitions + the join logic), so it
lives in an `outflow.py`:

```python
# outflow.py
from typing import Any, Dict, List
from incorporator import Incorporator


class SpaceXLaunch(Incorporator):
    """One source — the current launch."""


class SpaceXRocket(Incorporator):
    """Other source — the full rocket catalogue, registered by id."""


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fuse the two sources into one row per current launch.

    `state` is a snapshot of each source by class name, taken under
    the engine's shared lock. Read it, return List[dict], and let the
    engine handle the export.
    """
    launches = state["SpaceXLaunch"] or []
    rockets = state["SpaceXRocket"]

    rows = []
    for launch in launches:
        rocket = rockets.inc_dict.get(launch.rocket) if rockets else None
        rows.append({
            "id": launch.id,
            "name": launch.name,
            "rocket_name": rocket.name if rocket else None,
            "rocket_height_m": rocket.height.meters if rocket else None,
            "rocket_mass_kg": rocket.mass.kg if rocket else None,
            "rocket_success_pct": rocket.success_rate_pct if rocket else None,
        })
    return rows
```

That's the entire `outflow.py`. Two classes + one function. No daemon
plumbing, no lock acquisition, no audit emission — `fjord()` handles all
of it.

---

## Step 2: The Python Pipeline

```python
import asyncio
from incorporator import Incorporator

# Bring the classes into scope so fjord() can register them.
from outflow import SpaceXLaunch, SpaceXRocket


async def main():
    async for audit in Incorporator.fjord(
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
        outflow="outflow.py",
        export_params={"file_path": "data/launch_with_rocket.parquet"},
        refresh_interval=60.0,
        export_interval=120.0,
    ):
        op = audit.operation         # e.g. "fjord_refresh:SpaceXLaunch" or "outflow:Outflow"
        print(f"{op:30s} chunk {audit.chunk_index}: {audit.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## What `fjord()` is doing under the hood

1. **Concurrent seed.** All `stream_params[*].cls.incorp(...)` calls run
   in parallel via `asyncio.gather`. One audit per source.
2. **Per-source refresh daemons.** One daemon per entry. Each independently
   re-fetches on its own `refresh_interval` (you can override per entry —
   sources refresh at different cadences naturally).
3. **One outflow daemon.** Every `export_interval`, it snapshots every
   source under the shared lock, releases the lock, then calls your
   `outflow(state)` *in a worker thread* (via `asyncio.to_thread`) so a
   heavy CPU join doesn't block the refresh daemons.
4. **Dynamic output class.** From the rows `outflow()` returns, the engine
   uses `infer_dynamic_schema()` to build a Pydantic class named after
   the `outflow.py` stem (`outflow.py` → `Outflow`). The instances
   auto-register in `Outflow.inc_dict` for downstream `link_to(...)` use.
5. **Export.** Same handler dispatch as `stream()` — file extension picks
   the format (Parquet here, but switch to `.ndjson`, `.csv`, `.sqlite`,
   `.avro`, etc., for free).
6. **Shutdown.** SIGTERM / Ctrl+C cancels every task; the audit queue
   drains; the `async for` loop exits.

---

## 🐳 Run it from the CLI

The same pipeline as a `pipeline.json`:

```json
{
  "outflow": "outflow.py",
  "stream_params": [
    {
      "cls_name": "SpaceXLaunch",
      "incorp_params": {
        "inc_url": "https://api.spacexdata.com/v4/launches/latest",
        "inc_code": "id"
      },
      "refresh_params": {}
    },
    {
      "cls_name": "SpaceXRocket",
      "incorp_params": {
        "inc_url": "https://api.spacexdata.com/v4/rockets",
        "inc_code": "id"
      },
      "refresh_params": {}
    }
  ],
  "export_params": {"file_path": "data/launch_with_rocket.parquet"},
  "refresh_interval": 60.0,
  "export_interval": 120.0
}
```

```bash
incorporator validate pipeline.json
incorporator fjord pipeline.json --logs
```

Notice the JSON uses `cls_name` (string) while the Python uses `cls`
(class reference). The CLI loader resolves `cls_name` by importing the
`outflow.py` file and looking up the class by name — that's how the JSON
stays serialisable.

---

## When fjord shines

| Scenario | Why fjord wins |
|---|---|
| Joining two REST APIs that update at different rates | Independent per-source refresh cadences |
| Computing a derived dataset live (e.g. price spreads, latency joins) | `outflow()` runs CPU-heavy joins off the event loop |
| Needing a strong-typed output class without declaring one | `infer_dynamic_schema()` builds it from the rows |
| Production observability across a fan-out pipeline | One `AuditResult` per source per tick + per outflow tick |

For the single-source pipeline equivalent, see
[Streaming Daemon](./6_streaming_daemon.md). For the full method signature,
see the [Library reference](./library_reference.md).
