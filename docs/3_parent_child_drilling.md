***

# 🚀 Drilling API Graphs: Parent → Child Composition

Most REST APIs are **graphs**, not tables. A SpaceX launch references a
rocket by ID. A GitHub repository references commits, issues, and
contributors via URL fragments. Loading one endpoint gets you the
nodes; you still need a second round-trip to load each edge.

The naive solution is a `for` loop that fires N requests sequentially,
dies on the first rate-limit hit, and crashes if any single endpoint
returns malformed JSON. The Incorporator solution is **declarative**:
tell the framework which parent field carries the child ID and let it
fan out the requests concurrently with retry + dedup baked in.

This tutorial uses SpaceX's public v4 API to load upcoming launches,
then drill into each launch's `rocket` field to fetch the matching
rocket specs — all in two `incorp()` calls.

---

## The Pattern

```python
# 1. Load the parents — each launch has `rocket` and `launchpad` ID fields.
launches = await Launch.incorp(
    inc_url="https://api.spacexdata.com/v4/launches/upcoming",
    inc_code="id",
    inc_name="name",
)

# 2. Drill BOTH child relationships in parallel.
rockets, pads = await asyncio.gather(
    Rocket.incorp(
        inc_url="https://api.spacexdata.com/v4/rockets/{}",  # `{}` is the ID slot
        inc_parent=launches,                                 # parent list to walk
        inc_child="rocket",                                  # field name on parent
        inc_code="id",
    ),
    Pad.incorp(
        inc_url="https://api.spacexdata.com/v4/launchpads/{}",
        inc_parent=launches,
        inc_child="launchpad",
        inc_code="id",
    ),
)
```

For each `inc_parent` / `inc_child` pair, the framework:

1. Walks `launches`, extracts the child field from every record.
2. **Deduplicates the IDs** — 18 upcoming launches share just 2
   rocket types and 3 launchpads, so the framework fires 5 HTTP
   requests, not 36.
3. Substitutes each unique ID into the `{}` slot of `inc_url`.
4. Fires every request **concurrently** through the same shared
   `httpx.AsyncClient` (HTTP/2 multiplexed). Both drills overlap in
   time because `asyncio.gather` runs them in parallel.
5. Builds a typed instance per response and registers it under
   `<Cls>.inc_dict[<id>]`.

Three registries, fully populated, ready for an O(1) three-way join.

---

## Step 1: The Pipeline

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
    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    print(f"Loaded {len(launches)} upcoming launches.")

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
    print(f"Loaded {len(rockets)} rockets, {len(pads)} pads.")

    for launch in launches:
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

Output (real SpaceX data, today):

```text
LAUNCH                           ROCKET         PAD                  REGION       SUCCESS
========================================================================================
USSF-44                          Falcon Heavy   KSC LC 39A           Florida       55/55
Starlink 4-36 (v1.5)             Falcon 9       CCSFS SLC 40         Florida       97/99
CRS-26                           Falcon 9       KSC LC 39A           Florida       55/55
SWOT                             Falcon 9       VAFB SLC 4E          California    27/28
TTL-1                            Falcon 9       VAFB SLC 4E          California    27/28
```

The dedup story: **18 launches, 36 child references, 5 HTTP requests.**
The framework collapsed the duplicates before fan-out. Compare to a
naive `for` loop that would have fired 36 sequential requests.

---

## Why This Beats a `for` Loop

| Naive `for` loop | `inc_parent` + `inc_child` |
|---|---|
| Sequential requests; latency = N × RTT | Concurrent via `httpx.AsyncClient`; latency ≈ max RTT |
| Re-requests the same rocket if multiple launches share it | Auto-deduplicates parent IDs before fan-out |
| One bad endpoint crashes the whole batch | Failed sources surface in `rockets.failed_sources`; rest succeed |
| You write retry / backoff yourself | Tenacity-backed exponential retry baked in |
| You write the join `for` loop manually | `inc_dict` lookup is O(1), no loop needed |

---

## URL Templates and the `{}` Slot

`inc_url` accepts a single `{}` placeholder that gets format-substituted
with each extracted parent value. Examples:

```python
# Single ID per parent → drill /endpoint/{ID}/details
inc_url="https://api.example.com/users/{}/profile"

# When the parent's field is already a full URL (HATEOAS pattern),
# leave inc_url empty and the framework uses the URL as-is.
inc_url=""  # implicit when inc_child="self_url"
```

For URL fragments stored as `_links.detail.href` deep in the parent
schema, use a dotted path: `inc_child="_links.detail.href"`. The
framework walks the path on each parent and extracts the leaf string.

---

## When Parent Fields Are Lists

If `inc_child` points to a list field (e.g. each launch has
`payloads: List[str]` — a list of payload IDs), the framework
**flattens** the lists and dedups across all parents before fan-out.
One launch with three payloads becomes three requests, not one.

```python
payloads = await Payload.incorp(
    inc_url="https://api.spacexdata.com/v4/payloads/{}",
    inc_parent=launches,
    inc_child="payloads",     # field on Launch that holds a list of IDs
    inc_code="id",
)
```

This pattern scales to thousands of parents with five-figure unique
child IDs without any extra code on your part.

---

## Joining the Two Registries

After both `incorp()` calls return, both `Launch.inc_dict` and
`Rocket.inc_dict` are populated. The O(1) join lives in your application
code:

```python
for launch in launches:
    rocket = Rocket.inc_dict.get(launch.rocket)
    if rocket is None:
        continue
    print(f"{launch.name} → {rocket.name} ({rocket.success_rate_pct}% success rate)")
```

If you want the join to live in the **schema** itself (so
`launch.rocket` returns a `Rocket` instance instead of an ID string),
use the `link_to` converter — see the next tutorial's introduction to
ETL transformations.

---

## See Also

* **[Tutorial 4 — Stateful Refresh](./4_stateful_refresh.md)** — keep
  both registries live as launches and rockets change over time.
* **[Tutorial 6 — Multi-Source Fjord](./6_multi_source_fjord.md)** —
  fuses parent + child + outflow into a single daemon pipeline.
* **[XML POST Auditing](./appendix/xml_post_audit.md)** *(appendix)* —
  a related pattern that combines parent extraction with declarative
  bulk-POST batching for fraud-audit workflows.
* **[Library reference](./library_reference.md)** — full kwarg list
  for `inc_parent` / `inc_child`.
