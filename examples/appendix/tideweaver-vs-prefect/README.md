***

> 📎 **Appendix — Picking the right orchestrator.** Tideweaver is
> an in-process, window-bounded scheduler. Prefect (and Dagster,
> Airflow, Argo) is an out-of-process, cluster-aware scheduler.
> They solve adjacent problems and compose well; this appendix
> walks through when to reach for each — and how to run them
> together. Read [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md)
> first.

***

# 🧵 Tideweaver vs. Prefect (or Dagster, Airflow, Argo)

Both tools schedule work over time. Tideweaver runs in-process with one event loop; Prefect runs as a cluster with a server, workers, and a UI. Reach for Tideweaver when your orchestration fits inside one Python process and your window is bounded. Reach for Prefect when you need cross-machine scheduling, persistent state across restarts, or a team UI for monitoring.

In other words: Tideweaver is a **graph of named currents** running on independent intervals **inside one Python process**, for the duration of one bounded time window. Prefect / Dagster / Airflow / Argo are **DAGs of tasks** running on a fleet of workers, scheduled by a control plane, for the lifetime of your data platform.

The two are not competitors. Tideweaver fits inside a single Prefect task. Prefect fits around a single Tideweaver window.

---

## Decision table

| Requirement                                      | Tideweaver           | Prefect / Dagster / Airflow |
|--------------------------------------------------|----------------------|------------------------------|
| Sub-minute cadence per source                    | ✅ native            | ⚠️ heavy (scheduler overhead) |
| Multiple sources, independent intervals          | ✅ native            | ⚠️ N flows or N scheduled tasks |
| In-process registries (`cls.inc_dict` reused)    | ✅ native            | ❌ workers don't share memory |
| Bounded time window (e.g. a 4-hour NASCAR race)  | ✅ native            | ⚠️ requires external stop logic |
| Per-current fault isolation (`on_error`)         | ✅ native            | ⚠️ task-level retries        |
| Graceful drain at window close                   | ✅ native            | ⚠️ requires custom hook      |
| Cron-style daily / weekly scheduling             | ⚠️ wrap in cron/systemd | ✅ native                 |
| Multi-machine fan-out                            | ❌ (single process)  | ✅ native                    |
| UI / audit log / observability dashboard         | ❌ (log records only) | ✅ native                    |
| Multi-team coordination across services          | ❌                   | ✅ native                    |

A useful summary: **Tideweaver is what runs during a window. Prefect is what decides which window to run next.** Sub-minute cadence and in-process registries are the dividing line — at hour/day cadence with cross-machine fan-out, Prefect's overhead pays for itself.

---

## Only Prefect

Pick standalone Prefect when:

* The workload is *task-level concurrent* but not *source-level recurring* — e.g. "transform 50 files, then load the results".
* Sources tick on hour / day boundaries, not seconds / minutes.
* You need durable scheduling across machine reboots, fleet scaling, or per-task retries with backoff visible in a UI.

Wrap your `incorp()` calls inside `@task`-decorated functions and let Prefect's executor drive concurrency. No Tideweaver involved.

## Only Tideweaver

Pick standalone Tideweaver when:

* The workload is *bounded*: a race weekend, a market session, a measurement campaign.
* Sources tick on sub-minute cadences.
* In-process registries matter — downstream currents read live upstream `cls.inc_dict` snapshots rather than disk artifacts.
* You don't want to operate a control-plane server.

Run `incorporator tideweaver run watershed.json` from cron, systemd, or a Docker `CMD`; let the process exit at window close.

## Both — recommended for production

The strongest pattern wraps Tideweaver inside a Prefect `@flow`:

```python
import asyncio
from datetime import datetime, timedelta, timezone

from prefect import flow, task

from incorporator import Tideweaver, Watershed


@task(retries=3, retry_delay_seconds=60)
async def run_race_window(start: datetime, end: datetime) -> None:
    watershed = Watershed.diamond(
        window=(start, end),
        head=...,
        middle=[...],
        tail=...,
        outflow="race_outflow.py",
    )
    async for tide in Tideweaver(watershed).run():
        # per-tick observability (Prefect logger picks this up)
        if tide.skipped:
            print(tide.tide_number, tide.skipped)


@flow(name="nascar-race-day")
async def race_day_flow() -> None:
    start = datetime.now(timezone.utc)
    end = start + timedelta(hours=4)
    await run_race_window(start, end)


if __name__ == "__main__":
    asyncio.run(race_day_flow())
```

* **Prefect** handles the calendar (cron deployment), infra-level retries (whole-task restarts after worker failure), the UI, the audit log, and the multi-flow dependency graph.
* **Tideweaver** handles everything inside the four-hour window: sub-minute scheduling, per-source `on_error` policy, dependency gating between currents, graceful drain at window close.

The seam between them is the `@task` boundary. Tideweaver returns when the window closes; Prefect logs the result and schedules the next deployment.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Master the Tideweaver orchestrator first | [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) |
| Ship pipelines with Docker + secrets | [Deployment Guide](../../../docs/deployment.md) |
| Land columnar artifacts in the hybrid pattern | [Appendix — Parquet Snapshots in a Tideweaver Window](../tideweaver-parquet-snapshots/README.md) |
| See the same diamond shape against a non-crypto domain | [Appendix — NASCAR Tideweaver](../nascar-tideweaver/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/tideweaver-vs-prefect/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
