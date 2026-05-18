***

# 🚀 Tutorial 6 — SpaceX Launches: Ops Dashboard Feed

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

T5 introduced parent-child fan-out on crypto (top-N coins + per-coin detail drills).  **The same pattern powers operational dashboards in every vertical** — aerospace launch trackers, vehicle telemetry, e-commerce order pipelines, healthcare claims.  This tutorial re-runs the parent-child shape on SpaceX's v4 API to prove Incorporator's verbs are domain-agnostic, **and** gives you the second half of T5's pattern (the streaming daemon variant) on an unlimited-rate API so you can iterate without CoinGecko's per-minute window pressure.

The SpaceX v4 public API (`api.spacexdata.com/v4`) is a clean HATEOAS graph: every launch references a rocket, a launchpad, and a list of payload IDs.  Plenty of overlap (a handful of rocket variants and launchpads serve hundreds of historical launches), so dedup actually *matters* here — which makes it a great cross-check for T5's patent-child mechanics.

We'll cover two patterns in one tutorial:

1. **Parent → Child drilling** — load upcoming launches, drill rockets
   + launchpads concurrently, three-way O(1) join.  Mirrors
   [T5](../05-parent-child-drilling/README.md) on a different vertical.
2. **Streaming daemon** — periodic refresh + export of the launch
   feed, log shipping via `LoggedIncorporator`.  Previews
   [T8](../08-streaming-daemon/README.md) (Part 1, stateful mode).

---

## Pattern 1: Parent → Child Drilling (Launches → Rockets + Launchpads)

```python
import asyncio

from incorporator import Incorporator


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
    pass


class Pad(Incorporator):
    pass


async def main():
    # 1. Parent — upcoming launches (~18 records).
    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    print(f"✅ Loaded {len(launches)} upcoming launches.")

    # 2. Concurrent two-way drill.  Dedup before fan-out collapses
    # ~36 child references (rocket + launchpad per launch) into ~5
    # unique IDs.  Five HTTP requests, not 36.
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
    print(f"✅ {len(rockets)} unique rockets, {len(pads)} unique launchpads.\n")

    # 3. O(1) three-way join in application code.
    for launch in launches[:5]:
        rocket = Rocket.inc_dict.get(launch.rocket)
        pad = Pad.inc_dict.get(launch.launchpad)
        print(
            f"{launch.name:<32} "
            f"{rocket.name:<14} "
            f"{pad.name:<20} "
            f"{pad.region:<12} "
            f"{pad.launch_successes}/{pad.launch_attempts}"
        )


if __name__ == "__main__":
    asyncio.run(main())
```

Output (real SpaceX data):

```text
USSF-44                          Falcon Heavy   KSC LC 39A           Florida       55/55
Starlink 4-36 (v1.5)             Falcon 9       CCSFS SLC 40         Florida       97/99
CRS-26                           Falcon 9       KSC LC 39A           Florida       55/55
SWOT                             Falcon 9       VAFB SLC 4E          California    27/28
TTL-1                            Falcon 9       VAFB SLC 4E          California    27/28
```

The dedup story: **18 launches, 36 child references, 5 HTTP requests.**  Compare to a
naive `for` loop firing 36 sequential requests.

### When parent fields are lists

If `inc_child` points to a list field (e.g. each launch has `payloads: List[str]`), the
framework **flattens** the lists and dedups across all parents before fan-out.  One
launch with three payloads becomes three requests, not one.

```python
payloads = await Payload.incorp(
    inc_url="https://api.spacexdata.com/v4/payloads/{}",
    inc_parent=launches,
    inc_child="payloads",     # field on Launch that holds a list of IDs
    inc_code="id",
)
```

---

## Pattern 2: Streaming Daemon (Periodic Launch Refresh)

The SpaceX upcoming-launch feed updates infrequently — perfect for a slow-cadence
daemon (every few minutes) that snapshots changes to disk as the schedule slips and
shuffles.  This is the **stateful_polling=True** mode covered in T8 Part 1:
one live registry, refreshed in place.

```python
from incorporator import LoggedIncorporator


class Launch(LoggedIncorporator):
    pass


async def daemon():
    async for wave in Launch.stream(
        incorp_params={
            "inc_url": "https://api.spacexdata.com/v4/launches/upcoming",
            "inc_code": "id",
            "inc_name": "name",
        },
        stateful_polling=True,
        refresh_interval=120,                              # poll every 2 min
        export_params={"file_path": "data/launches.ndjson"},
        export_interval=300,                               # flush every 5 min
        enable_logging=True,
    ):
        if wave.failed_sources:
            print(f"⚠️  Failures in chunk {wave.chunk_index}: {wave.failed_sources}")
```

Ctrl+C / SIGTERM triggers the graceful drain — in-flight requests finish, final export
fires, the daemon exits cleanly.  Same `LoggedIncorporator` + `enable_logging=True`
pattern T8 walks through; nothing SpaceX-specific.

### Why this domain works well for daemons

* **Stable schedule changes** — launch slips happen on the order of hours/days, not
  seconds, so a 2-minute refresh captures every change with low API pressure.
* **Bounded registry** — the upcoming feed is always ~18-30 records; the live
  registry is tiny, and the per-tick export is a snapshot of the full current state.
* **No auth, no quota** — public, free, ideal for tutorial code.

---

## Where to Go Next

> 👉 **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**  T6 introduced the streaming-daemon shape briefly; T7 takes the registry from "loaded once" to "kept live" — `refresh()` three ways against a Binance.us ticker.

| Goal | Read |
|---|---|
| Keep a registry live with `refresh()` | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| See the crypto-spine version of parent-child drilling | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| See the full streaming-daemon coverage | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Fuse SpaceX launches + rockets into one composite | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| See another non-crypto domain in the curriculum | [Appendix — NASCAR Tideweaver](../appendix/nascar-tideweaver/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/06-spacex-launches/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
