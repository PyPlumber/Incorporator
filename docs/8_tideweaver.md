***

# 🪡 Tideweaver: Orchestrating Streams, Flushes, and Exports

`stream()` watches one source.  `fjord()` watches N sources concurrently.
**Tideweaver** orchestrates them — a graph of named currents over a single
time window, each ticking on its own interval, with dependency edges that
gate dependents until their upstream produces new data.

Five names cover the whole layer:

| Name | Role |
|---|---|
| `Tideweaver` | The orchestrator — runs a `Watershed`. |
| `Watershed` | The plan: window + currents + edges. Serialisable as `watershed.json`. |
| `Current` | One node — typed via `Stream`, `Fjord`, or `Export`. |
| `Tide` | One scheduler pass. Emitted as a log record per pass. |
| `Wave` | Already exists. One emit from a stream or fjord flush. |

A **fjord flush** is the tick unit of a `Fjord` current inside Tideweaver:
snapshot the upstream currents' registries, run the user-supplied
`outflow(state)`, build the dynamic output class, export.  It is *not* a
call to `cls.fjord()` (which is a long-running daemon).

---

## The Goal

We'll walk all four shape helpers in order of complexity:

* `Watershed.parallel(...)` — N unrelated currents sharing only the window.
* `Watershed.chain(...)` — A → B → C with strict ordering.
* `Watershed.fanout(...)` — one source feeding N independent sinks.
* `Watershed.diamond(...)` — the capstone: laps + pits + flags → driver state.

---

## Step 1: `parallel()` — the warm-up

The simplest shape: two currents, no edges, each ticking on its own interval.
Tideweaver runs them concurrently for the window duration.

```python
import asyncio
from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, Stream, Tideweaver, Watershed


class Coins(Incorporator):
    """Live CoinGecko coin list."""


class News(Incorporator):
    """Headlines from a public news feed."""


async def main() -> None:
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(minutes=5))

    watershed = Watershed.parallel(
        window=window,
        currents=[
            Stream(
                name="coins",
                cls=Coins,
                interval=60,
                incorp_params={"inc_url": "https://api.coingecko.com/api/v3/coins/list"},
            ),
            Stream(
                name="news",
                cls=News,
                interval=30,
                incorp_params={"inc_url": "https://api.example.com/headlines"},
            ),
        ],
    )

    async for tide in Tideweaver(watershed).run():
        print(f"Tide {tide.tide_number}: fired={tide.fired} duration={tide.duration_sec:.3f}s")


if __name__ == "__main__":
    asyncio.run(main())
```

Each Tide record tells you which currents fired this pass and which got
skipped (`"not_due"`, `"awaiting_upstream"`, `"skip_ahead"`, ...).

---

## Step 2: `chain()` — strict A → B → C

Add an ordering constraint: B may not tick until A has produced a wave.
That's the **hard** mode (the default).  Use **soft** when you only want
the in-pass ordering (B runs after A in topo order but doesn't wait for A's
data).

```python
watershed = Watershed.chain(
    window=window,
    currents=[a, b, c],
    dependency_mode="hard",  # or "soft"
)
```

Skip-ahead: if A's tick has been running longer than
`skip_threshold * b.interval` (default 2.0×), B skips this pass with
reason `"skip_ahead"` so it doesn't queue up behind a stuck upstream.

---

## Step 3: `fanout()` — one source, N sinks

```python
watershed = Watershed.fanout(
    window=window,
    source=upstream_stream,
    sinks=[sink_a, sink_b, sink_c],
)
```

Every sink has a single hard dependency on `source`; the sinks are
independent of each other.

---

## Step 4: `diamond()` — the NASCAR capstone

```python
watershed = Watershed.diamond(
    window=window,
    head=laps_stream,
    middle=[pits_stream, flags_stream],
    tail=state_fjord,
    outflow="race_outflow.py",
)
```

`state_fjord` is a `Fjord` current — its tick is a **fjord flush**:

1. Snapshot the upstream classes' registries (`Lap`, `Pit`, `Flag`).
2. Hand them to `outflow(state)`, defined in `race_outflow.py`.
3. Materialise the returned rows into the dynamic output class.
4. Export them to the configured destination (e.g. `state.ndjson`).

See `examples/8_nascar_tideweaver.py` for the full runnable example using
local JSON files as sources — no credentials, no network.

---

## Restart policy

Each `Current` carries an `on_error` policy:

* `"restart"` (default) — tenacity-backed exp-backoff retry on the failing tick.
* `"isolate"` — log + continue siblings; the parked current resumes next tick.
* `"fail_watershed"` — propagate; the whole graph cancels.

---

## A note on `stateful_polling`

`stream()` accepts `stateful_polling=True` to switch into a long-running
daemon mode with its own internal `refresh_interval` / `export_interval`.
That conflicts with Tideweaver's per-interval tick model, so `Stream`
inside a `Watershed` rejects the flag at construction time with a clear
error.  Use a `Fjord` current instead when you need a stateful fan-in.

---

## Run it from the CLI

`watershed.json` is the declarative form.  Every Python knob has a JSON
equivalent; env-var interpolation (`${VAR}`, `${VAR:-default}`,
`${file:/run/secrets/key}`) is applied at load time.

```json
{
  "window": {"start": "${RACE_START}", "end": "${RACE_END}"},
  "shape": "diamond",
  "outflow": "race_outflow.py",
  "drain_timeout": 30,
  "head":   {"name": "laps",  "class": "LapData",     "verb": "stream", "interval": 30},
  "middle": [
    {"name": "pits",  "class": "PitStops",   "verb": "stream", "interval": 30},
    {"name": "flags", "class": "FlagEvents", "verb": "stream", "interval": 30}
  ],
  "tail":   {"name": "state", "class": "DriverState", "verb": "fjord",  "interval": 30,
             "export_params": {"file_path": "state.ndjson", "format": "ndjson"}},
  "dependency_mode": "hard"
}
```

Supported `shape` values:

* `"chain"` — top-level `currents: [...]`.
* `"diamond"` — `head` / `middle` / `tail`.
* `"fanout"` — `source` + `sinks: [...]`.
* `"parallel"` — `currents: [...]`, no `dependency_mode`.
* `"custom"` — `currents: [...]` + `edges: [{"from": "a", "to": "b", "mode": "hard"}]`.

Each current entry's `"class"` is resolved against the outflow sidecar
(same convention as `fjord()`).  Run it:

```bash
incorporator tideweaver run watershed.json --json-output
```

One NDJSON `Tide` record per scheduler pass lands on stdout; status banners
go to stderr so log shippers can ingest stdout directly.
