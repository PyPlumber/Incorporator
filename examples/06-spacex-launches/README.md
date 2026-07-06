***

# 🚀 Tutorial 6 — SpaceX Launches: Ops Dashboard Feed

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Your ops dashboard needs the next SpaceX launches and their full rocket + launchpad context — joined, deduped, and ready to render.  SpaceX's v4 API is a clean HATEOAS graph; `inc_parent` + `inc_child` against 18 launches dedups 36 child references into 5 unique IDs and 5 HTTP requests.

T5 introduced parent-child fan-out on crypto (top-N coins + per-coin detail drills).  **The same pattern powers operational dashboards in every vertical** — aerospace launch trackers, vehicle telemetry, e-commerce order pipelines, healthcare claims.  This tutorial re-runs the parent-child shape on SpaceX's v4 API to prove Incorporator's verbs are domain-agnostic, **and** gives you the streaming-daemon variant on an unlimited-rate API so you can iterate without CoinGecko's per-minute window pressure.

Plenty of overlap in this graph (a handful of rocket variants and launchpads serve hundreds of historical launches), so dedup actually *matters* here — which makes it a great cross-check for T5's parent-child mechanics.

We'll cover two patterns in one tutorial:

1. **Parent → Child drilling** — load upcoming launches, drill rockets
   + launchpads concurrently, three-way O(1) join.  Mirrors
   [T5](../05-parent-child-drilling/README.md) on a different vertical.
2. **Streaming daemon** — periodic refresh + export of the launch
   feed via the `stateful_polling=True` shim, log shipping via
   `LoggedIncorporator`.  Previews
   [T8](../08-streaming-daemon/README.md) (the demoted Part 2 shim
   path; for the canonical multi-source live-daemon pattern reach
   for [T10's `fjord()`](../10-multi-source-fjord/README.md)).

---

## Pattern 1: Parent → Child Drilling (Launches → Rockets + Launchpads)

```python
import asyncio

from incorporator import Incorporator, link_to


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
    pass


class Pad(Incorporator):
    pass


async def main():
    # 1. Parent — upcoming launches (~18 records).  Raw FK strings for now —
    # the drill below needs them as-is, before any join runs.
    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    print(f"OK: Loaded {len(launches)} upcoming launches.")

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
    print(f"OK: {len(rockets)} unique rockets, {len(pads)} unique launchpads.\n")

    # 3. Build-time join, applied once: link_to() returns a plain callable —
    # invoking it directly re-coerces the already-built `launches` list in
    # place, no second incorp() round-trip needed.  After this, launch.rocket
    # / launch.launchpad ARE the resolved Rocket / Pad instances.
    resolve_rocket = link_to(rockets)
    resolve_pad = link_to(pads)
    for launch in launches:
        launch.rocket = resolve_rocket(launch.rocket)
        launch.launchpad = resolve_pad(launch.launchpad)

    # 4. Read the join as plain attributes — a launch may legitimately
    # reference a rocket/pad outside the fetched set, so the None-guard
    # here is an honest display-time boundary, not a re-lookup.
    for launch in launches[:5]:
        rocket, pad = launch.rocket, launch.launchpad
        print(
            f"{launch.name:<32} "
            f"{rocket.name if rocket else '?':<14} "
            f"{pad.name if pad else '?':<20} "
            f"{pad.region if pad else '?':<12} "
            f"{f'{pad.launch_successes}/{pad.launch_attempts}' if pad else '-'}"
        )


if __name__ == "__main__":
    asyncio.run(main())
```

> **Why the join can't sit in `Launch`'s own `conv_dict`.** `link_to(rockets)`
> needs `rockets` to already exist — but `rockets` is drilled FROM `launches`
> via `inc_parent=launches, inc_child="rocket"`, and that drill reads the raw
> FK string off the already-built `Launch` instances. If `Launch`'s own
> `conv_dict` resolved `rocket` to a `Rocket` instance first, the drill would
> try to URL-template that instance into `.../rockets/{}` instead of its id.
> The fix: build `launches` once (raw FKs), drill `rockets` / `pads` from it
> (unchanged T5 mechanics), then apply `link_to` as a single build-time pass
> over the already-built `launches` list — still one join, done once, before
> any reader touches `launch.rocket`, just not inside the `incorp()` call
> itself. See `docs/api_atlas.md`'s "Build-time vs read-time: where coercion
> + joins belong" section for the general rule.

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

### Reading the structured reject list

Both `rockets` and `pads` come back as `IncorporatorList` instances, each carrying a
structured `rejects` list (of `RejectEntry`) alongside the legacy bare-string
`failed_sources` view. ETL practice calls failed-load rows *rejects* — Incorporator
uses the same idiom. When a child drill fails (rate limit, 5xx, timeout) the entry
records the URI, error class, `is_url_traffic_error` flag, parsed `Retry-After`
header, and the wave index it belonged to:

```python
for entry in rockets.rejects:
    retry = f" (retry after {entry.retry_after:.1f}s)" if entry.retry_after else ""
    origin = "API" if entry.is_url_traffic_error else "parse"
    print(f"{entry.source} [{origin}] {entry.error_kind}{retry}")
# https://api.spacexdata.com/v4/rockets/5e9... [API] HTTPStatusError (retry after 30.0s)
# https://api.spacexdata.com/v4/rockets/5e9... [API] ReadTimeout
```

`entry.is_url_traffic_error` is `True` for HTTP 4xx/5xx, network timeouts, and
connection failures — the same classification that determines whether a logged
failure lands in `_api.log` or `_error.log`. `str(entry)` now includes the HTTP
reason phrase when available: `[HTTP 429 Too Many Requests]`.

`rockets.failed_sources` is the derived view (`[entry.source for entry in
rockets.rejects]`) — kept for back-compat. Reach for `rejects` whenever production
retry logic needs per-source error classification or backoff timing.

### When parent fields are lists

If `inc_child` points to a list field (e.g. each launch has `payloads: list[str]`), the
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
shuffles.  Single-source live registry, so the `stateful_polling=True` shim is fine
here.  For multi-source live registries reach for `fjord()` directly (T10).

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
            print(f"WARN: Failures in chunk {wave.chunk_index}: {wave.failed_sources}")
```

Ctrl+C / SIGTERM triggers the graceful drain — in-flight requests finish, final export
fires, the daemon exits cleanly.  Same `LoggedIncorporator` + `enable_logging=True`
pattern T8 walks through; nothing SpaceX-specific.

### Why this domain works well for daemons

* **Stable schedule changes** — launch slips happen on the order of hours/days, not
  seconds, so a 2-minute refresh captures every change with low API pressure.
* **Bounded registry** — the upcoming feed is always ~18-30 records; the live
  registry is tiny, and the per-refresh export is a snapshot of the full current state.
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
