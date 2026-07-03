***

# 🧵 Tutorial 11 — Tideweaver: Multi-Exchange Arbitrage Scanner Diamond (Capstone)

**Prerequisites:** [Tutorial 7](../07-stateful-refresh/README.md) (`refresh()` mechanics),
[Tutorial 8](../08-streaming-daemon/README.md) (`stream()`, both polling modes),
[Tutorial 10](../10-multi-source-fjord/README.md) (`fjord()` + `outflow(state)`).

Multi-exchange arb scanners monitor 5–20 exchanges concurrently for cross-venue
spreads. Three exchanges — Binance.us, Coinbase Advanced Trade, Kraken — each
ticking at its own cadence, converging on one fused "best bid / best ask /
spread / opportunity flag" tail.

Each exchange current runs the same schema-free `incorp()` call from Tutorial 1
— no Pydantic models, no exchange-specific parsers. The `BinanceBook`,
`CoinbaseTicker`, and `KrakenTicker` classes are all empty subclasses, and the
orchestrator inherits every primitive you already know: the same `Wave`,
`RejectEntry`, and `FlowControl` that appear in a single-source fetch.

**Tideweaver** lets you declare it instead of writing the async scheduler
yourself: one `Watershed.diamond()`, three exchange currents, one `Fjord`
current that snapshots them all and writes the consolidated arb signal. No
hand-rolled `asyncio` glue, no per-current restart logic, no manual snapshot
locking — the orchestrator runs a graph of named currents over a single time
window, each ticking on its own interval, with dependency edges that gate
downstream work until upstream produces fresh data.

> **CCXT comparison.** Real arb bots are usually built on
> [CCXT](https://github.com/ccxt/ccxt) with hand-rolled `asyncio` glue.  If you
> need 100+ exchange support available immediately, CCXT is the standard library.
> Incorporator targets the case where you want a typed object graph +
> declarative orchestration + the same verbs your single-source pipelines
> already use.

This tutorial builds the data layer for the 3-exchange scanner above —
Binance.us + Coinbase Advanced Trade + Kraken, all free, all no-auth — using
`Watershed.diamond()`.

---

## Five names cover the whole layer

| Name | Role |
|---|---|
| `Tideweaver` | The orchestrator — runs a `Watershed`. |
| `Watershed` | The plan: window + currents + edges.  Serialisable as `watershed.json`. |
| `Current` | One node — typed via `Stream`, `Fjord`, or `Export`. |
| `Tide` | One scheduler pass.  Emitted as a log record per pass. |
| `Wave` | Already exists.  One emit from a stream or fjord flush. |

A **fjord flush** is the scheduling primitive of a `Fjord` current inside Tideweaver:
snapshot the upstream currents' registries, run the user-supplied `outflow(state)`,
build the dynamic output class, export.  It is *not* a call to `cls.fjord()` (which is
a long-running daemon).

> Three more names for advanced cases (covered in
> [`docs/api_atlas.md`](../../docs/api_atlas.md)): `FlowObserver` for
> declarative per-edge telemetry, `CustomCurrent` for tick logic outside
> the verb taxonomy, and `architect()` for scaffolding a `Watershed`
> from probe data (see the next section).

---

## Skip the boilerplate: `architect(output="plan")`

When you already know the source URLs but not how to wire them together,
`architect()` profiles them in parallel and emits a runnable `Watershed`
scaffold.  Setting `output="plan"` returns an in-memory
`OrchestrationPlan` you can pass straight into `.to_watershed()` —
no `watershed.json` disk round-trip:

```python
from incorporator import Incorporator, Tideweaver

# Probe N sources; architect picks intervals and shape from response signals.
plan = await Incorporator.architect(
    sources={
        "binance":  "https://api.binance.us/api/v3/depth?symbol=BTCUSDT",
        "coinbase": "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1",
        "kraken":   "https://api.kraken.com/0/public/Depth?pair=XBTUSD",
    },
    output="plan",
)

watershed = plan.to_watershed(
    window=(start, end),
    classes={"binance": BinanceBook, "coinbase": CoinbaseTicker, "kraken": KrakenTicker},
)
async for tide in Tideweaver(watershed).run():
    print(tide)
```

Reach for `architect()` when discovering — hand-construct
(`Watershed.diamond(...)` below) when you already know the shape and want
full control over per-current intervals, `on_error` policies, or custom
`FlowControl` literals.  The four output formats trade off
inspect-ability for round-trip-ability: `"report"` (terminal review),
`"python"` (paste-ready module body), `"json"` (round-trippable
`watershed.json`), `"plan"` (the in-memory handoff shown above).  After a
run, `tune()` closes the loop — see [Post-run tuning](#post-run-tuning)
below.

---

## The four shape helpers

We'll walk all four in order of complexity, then build the arb-scanner diamond.

* `Watershed.parallel(...)` — N unrelated currents sharing only the window.
* `Watershed.chain(...)` — A → B → C with strict ordering.
* `Watershed.fanout(...)` — one source feeding N independent sinks.
* `Watershed.diamond(...)` — N inputs into one fused sink — the arb scanner shape.

---

## Step 1: `parallel()` — the warm-up

The simplest shape: two currents, no edges, each ticking on its own interval.
Tideweaver runs them concurrently for the window duration.

```python
import asyncio
from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, Stream, Tideweaver, Watershed


class CoinList(Incorporator):
    """Live CoinGecko coin list."""


class TopMarkets(Incorporator):
    """Live CoinGecko top-100 markets."""


async def main() -> None:
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(minutes=5))

    watershed = Watershed.parallel(
        window=window,
        currents=[
            Stream(
                name="coins",
                cls=CoinList,
                interval=60,
                incorp_params={"inc_url": "https://api.coingecko.com/api/v3/coins/list"},
            ),
            Stream(
                name="markets",
                cls=TopMarkets,
                interval=30,
                incorp_params={
                    "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
                    "params": {"vs_currency": "usd", "per_page": 100},
                    "inc_code": "id",
                },
            ),
        ],
    )

    async for tide in Tideweaver(watershed).run():
        print(f"Tide {tide.tide_number}: fired={tide.fired} duration={tide.duration_sec:.3f}s")


if __name__ == "__main__":
    asyncio.run(main())
```

Each Tide record tells you which currents fired this pass and which got skipped
(`"not_due"`, `"awaiting_upstream"`, `"skip_ahead"`, …).

---

## Step 2: `chain()` — strict A → B → C

Add an ordering constraint: B may not tick until A has produced a wave.  That's
**hard** mode (the default).  Use **soft** when you only want in-pass ordering (B runs
after A in topo order but doesn't wait for A's data).

```python
watershed = Watershed.chain(
    window=window,
    currents=[a, b, c],
    gate_mode="hard",  # or "soft" or "weir"
)
```

`gate_mode=` also accepts the `GateMode` enum (`from
incorporator.tideweaver import GateMode`;
`GateMode.HARD` / `GateMode.SOFT` / `GateMode.WEIR`) — both forms
produce identical `FlowControl` because `GateMode` is a `str`-subclass.
The JSON form below always uses the lowercase string.

Skip-ahead: when `gate_mode="hard"`, every edge gets a default
`SurgeBarrier(threshold_multiple=2.0, action="skip")` — if A's tick has been
running longer than `2.0 × b.interval`, B skips this pass with reason
`"skip_ahead"` so it doesn't queue up behind a stuck upstream.  Override per
edge with `flow=FlowControl(surge_barrier=SurgeBarrier(threshold_multiple=3.0, action="bypass"))`.

---

## Step 3: `fanout()` — one source, N sinks

```python
watershed = Watershed.fanout(
    window=window,
    source=upstream_stream,
    sinks=[sink_a, sink_b, sink_c],
)
```

Every sink has a single hard dependency on `source`; the sinks are independent of each
other.

---

## Step 4: `diamond()` — the multi-exchange arb-scanner capstone

Three exchange currents — Binance.us, Coinbase Advanced Trade, Kraken — each ticking
on their own cadence.  One tail `Fjord` current snapshots all three exchange registries,
finds the best bid and best ask per symbol across venues, computes the cross-venue
spread, and flags any opportunity where the spread crosses a threshold (basis points).

> **`stateful_polling=True` is rejected inside a Watershed.**
> Tideweaver does its own per-tick scheduling, so a `Stream` current with
> `stateful_polling=True` would conflict with the orchestrator's interval
> clock and is **rejected at construction time**.
> Inside a Watershed, every `Stream` current is a chunking-mode `incorp()`
> call per tick.  If you want stateful behaviour across ticks (live
> `inc_dict` accumulating between Tides), put a `Fjord` current
> downstream — its outflow snapshots the upstream registries on each
> flush.  The diamond below does exactly that.

```python
import asyncio
from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, Fjord, Stream, Tideweaver, Watershed


class BinanceBook(Incorporator):
    """Binance.us /api/v3/ticker/bookTicker — top of book per symbol."""


class CoinbaseTicker(Incorporator):
    """Coinbase Advanced Trade /products/{id}/ticker — top of book per product."""


class KrakenTicker(Incorporator):
    """Kraken /0/public/Ticker — top of book for the requested pairs."""


class BestMarket(Incorporator):
    """Derived per-symbol arb snapshot — built dynamically by the fjord flush."""


async def main() -> None:
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(minutes=10))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="binance",
            cls=BinanceBook,
            interval=15,
            incorp_params={
                "inc_url": "https://api.binance.us/api/v3/ticker/bookTicker",
                "inc_code": "symbol",
            },
        ),
        middle=[
            Stream(
                name="coinbase",
                cls=CoinbaseTicker,
                interval=30,
                incorp_params={
                    "inc_url": "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                    "inc_code": "trade_id",
                },
            ),
            Stream(
                name="kraken",
                cls=KrakenTicker,
                interval=30,
                incorp_params={
                    "inc_url": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD",
                    "rec_path": "result",
                    "inc_code": "_key",
                },
            ),
        ],
        tail=Fjord(
            name="best_market",
            cls=BestMarket,
            interval=30,
            export_params={
                "file_path": "data/arb_signals.ndjson",
                "format": "ndjson",
                "if_exists": "append",
            },
        ),
        outflow="outflow.py",
        drain_timeout=10.0,
    )

    async for tide in Tideweaver(watershed).run():
        print(
            f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<32} "
            f"| skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )


if __name__ == "__main__":
    asyncio.run(main())
```

The `BestMarket` Fjord current's tick is a **fjord flush**:

1. Snapshot the three upstream registries — held alive between flushes via
   the strong-ref `_tideweaver_snapshot` classvar the scheduler parks on
   each upstream Stream class (so the snapshot survives the chunking
   engine's weak-ref `inc_dict`).
2. Hand them to `outflow(state)`, defined in `outflow.py`.
3. Materialise the returned rows into the dynamic output class.
4. Export them to `data/arb_signals.ndjson` (append-friendly — every flush adds rows).

### `outflow.py` — symbol normalization + best-market join

The three exchanges return the same logical asset under different symbol shapes —
Binance "BTCUSDT", Coinbase "BTC-USD", Kraken "XXBTZUSD" / "XBTUSD".  The outflow
function normalizes to a canonical key (e.g. `"BTC"`), then computes best bid / best
ask across venues.

> **Sidecar naming convention.**  All fjord/Tideweaver tutorials use the bare
> semantic name `outflow.py` — matching the `incorporator init --type fjord`
> scaffold and the DX convention established in T9/T10.
>
> **Output class is inferred here.**  `outflow(state)` returns a list of
> dicts; `flush()` infers the output class fields from the dict keys.  The
> inferred class is named after the class declared in the watershed.json
> `"class"` field (here, `BestMarket`) — the declaration supplies the name,
> but the schema comes from the rows.  `BestMarket` must stay declared so
> the watershed.json `"class"` field (and the `arb_scanner.py` import)
> can resolve it by name; declaring it bare is fine because `flush()`
> infers the fields from the rows and, if a bare class would drop
> undeclared keys, emits a one-time WARNING and falls through to inference
> so no fields are lost.  Declare the fields explicitly to silence the WARNING.

```python
# outflow.py
from typing import Any


# Map exchange-native symbols to a canonical asset code.  Real scanners
# load this from /exchangeInfo equivalents; we hard-code 2 pairs for the demo.
NORMALIZATION = {
    # Binance.us
    "BTCUSDT": "BTC", "ETHUSDT": "ETH",
    "BTCUSD":  "BTC", "ETHUSD":  "ETH",
    # Coinbase
    "BTC-USD": "BTC", "ETH-USD": "ETH",
    # Kraken
    "XXBTZUSD": "BTC", "XETHZUSD": "ETH", "XBTUSD": "BTC",
}


def _venue_quotes(rows, symbol_attr: str, bid_attr: str, ask_attr: str, venue: str):
    """Extract canonical (asset, bid, ask, venue) tuples from one exchange's registry."""
    out = []
    for row in rows:
        raw = getattr(row, symbol_attr, None)
        if raw is None:
            continue
        asset = NORMALIZATION.get(str(raw))
        if asset is None:
            continue
        try:
            bid = float(getattr(row, bid_attr, 0) or 0)
            ask = float(getattr(row, ask_attr, 0) or 0)
        except (TypeError, ValueError):
            continue
        if bid > 0 and ask > 0:
            out.append((asset, bid, ask, venue))
    return out


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    quotes = []
    quotes += _venue_quotes(state.get("BinanceBook", []),    "symbol",   "bidPrice", "askPrice", "binance")
    quotes += _venue_quotes(state.get("CoinbaseTicker", []), "product_id", "bid",    "ask",      "coinbase")
    quotes += _venue_quotes(state.get("KrakenTicker", []),   "_key",     "b",        "a",        "kraken")

    # Group by canonical asset.
    by_asset: dict[str, list[tuple]] = {}
    for asset, bid, ask, venue in quotes:
        by_asset.setdefault(asset, []).append((bid, ask, venue))

    # Best bid (highest) wins for sellers; best ask (lowest) wins for buyers.
    rows = []
    for asset, venues in by_asset.items():
        best_bid_price, best_bid_venue = max(((b, v) for b, _, v in venues), default=(0.0, ""))
        best_ask_price, best_ask_venue = min(((a, v) for _, a, v in venues), default=(0.0, ""))
        if not (best_bid_price and best_ask_price):
            continue
        mid = (best_bid_price + best_ask_price) / 2
        spread_bps = (best_bid_price - best_ask_price) / mid * 10_000
        rows.append({
            "asset": asset,
            "best_bid": best_bid_price,
            "best_bid_venue": best_bid_venue,
            "best_ask": best_ask_price,
            "best_ask_venue": best_ask_venue,
            "spread_bps": round(spread_bps, 2),
            "arb_opportunity": spread_bps > 5,           # 5 bps threshold
        })
    return rows
```

A positive `spread_bps` with `arb_opportunity=True` means *the best bid on one venue is
higher than the best ask on another* — classic cross-venue arb signal.

> **Missing-peer `KeyError` in `outflow(state)`?**  Same as the fjord verbs (T9, T10):
> fjord's seed-error formatter rewrites the failed-sources entry to a copy-pasteable
> diagnostic suggesting `state.get('X')` for soft access.  In Tideweaver, the `Fjord`
> current's flush respects the same contract — every `state.get(...)` defaults to
> `[]`, no manual guard needed.

> **Production:** real arb scanners drive symbol normalization off each exchange's
> `/exchangeInfo` (or equivalent) feed instead of hard-coded dicts.  CCXT does this
> implicitly via its `markets` cache; with Incorporator, you'd add a 4th Stream
> current pulling `/exchangeInfo` and let `outflow(state)` build the normalization
> table dynamically per tick.

---

## Restart policy

Each `Current` carries an `on_error` policy:

* `"restart"` (default) — Tenacity-backed exp-backoff retry on the failing tick.
* `"isolate"` — log + continue siblings; the parked current resumes next tick.
* `"fail_watershed"` — propagate; the whole graph cancels.

For an arb scanner, `"isolate"` is usually right: one exchange going down shouldn't
crash the whole orchestrator; the tail Fjord just emits a best-market record from
whichever exchanges are still up. A failed tick never advertises a wave — hard-
gated dependents keep gating with `awaiting_upstream` until the isolated current
produces a successful tick again, so a downstream never fires on stale, re-stamped
data from before the failure began.

---

## Run it from the CLI

`watershed.json` is the declarative form.  Every Python knob has a JSON equivalent;
env-var interpolation (`${VAR}`, `${VAR:-default}`, `${file:/run/secrets/key}`) is
applied at load time.

```json
{
  "window": {"start": "${WINDOW_START}", "end": "${WINDOW_END}"},
  "shape": "diamond",
  "outflow": "outflow.py",
  "drain_timeout": 30,
  "gate_mode": "hard",
  "head":   {"name": "binance", "class": "BinanceBook",    "verb": "stream", "interval": 15,
             "incorp_params": {"inc_url": "https://api.binance.us/api/v3/ticker/bookTicker",
                               "inc_code": "symbol"}},
  "middle": [
    {"name": "coinbase", "class": "CoinbaseTicker", "verb": "stream", "interval": 30,
     "incorp_params": {"inc_url": "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                       "inc_code": "trade_id"}},
    {"name": "kraken",   "class": "KrakenTicker",   "verb": "stream", "interval": 30,
     "incorp_params": {"inc_url": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD",
                       "rec_path": "result", "inc_code": "_key"}}
  ],
  "tail":   {"name": "best_market", "class": "BestMarket", "verb": "fjord", "interval": 30,
             "export_params": {"file_path": "data/arb_signals.ndjson",
                               "format": "ndjson", "if_exists": "append"}}
}
```

Supported `shape` values:

* `"chain"` — top-level `currents: [...]`.
* `"diamond"` — `head` / `middle` / `tail`.
* `"fanout"` — `source` + `sinks: [...]`.
* `"parallel"` — `currents: [...]`, no `gate_mode`.
* `"custom"` — `currents: [...]` + `edges: [{"from": "a", "to": "b", "gate_mode": "hard"}]` (or `"flow": {...}` for a per-edge `FlowControl`).

Each current entry's `"class"` is resolved against the outflow sidecar (same
convention as `fjord()`).  Run it:

```bash
incorporator tideweaver run watershed.json --json-output
```

The CLI resolves `outflow`, `inflow`, and `inc_file` paths relative to `watershed.json`'s directory, so the command works from any working directory. `export_params.file_path` (`"data/arb_signals.ndjson"`) is CWD-relative — the output file lands in `<your working directory>/data/`, not alongside the config.

One NDJSON `Tide` record per scheduler pass lands on stdout; status banners go to
stderr so log shippers can ingest stdout directly.

---

## Post-run tuning

`LoggedTideweaver` is the drop-in that routes every `Tide` + `RejectEntry` to
disk via the `QueueHandler` pipeline; `tune()` reads those records and returns
a `TuningReport` of severity-sorted hints:

```python
from incorporator.tideweaver import LoggedTideweaver, tune

tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="ArbSession")
tides = [tide async for tide in tw.run()]
report = tune(rejects=tw.rejects, tides=tides, pass_interval=tw.pass_interval)
print(report.render())                       # hint blocks, sorted by severity
```

`tw.summary(tides=tides)` returns the same report via instance method.  Each
`tide.current_outcomes` is a `list[CurrentOutcome]` with per-current `status`
/ `reason` / `in_flight_sec` — which currents fired, which skipped, per pass.

**`logger_name` resolution.** The `logger_name` kwarg can be omitted when the
`Watershed` carries a `name` field — `LoggedTideweaver` resolves it in order:
explicit `logger_name` → `watershed.name` → `"Tideweaver"`. Setting
`watershed.name="ArbSession"` above produces the same file prefix without
repeating the string at the constructor.

`tune()` runs rule functions and skips rules whose required inputs are absent
— pass only what you have:

| Rule | Requires | What it checks |
|---|---|---|
| `_tune_chunk_size` | `waves=` | p50 / p99 chunk latency; suggests smaller chunks under backpressure |
| `_tune_penstock_rate` | `rejects=` | per-edge and per-host rate-limit patterns |
| `_tune_surge_threshold` | `rejects=` + `tides=` | surge-barrier threshold relative to actual skip volume |
| `_tune_pass_interval` | `tides=` + `pass_interval=` | saturation (all passes == pass_interval) or heap-empty waste |
| `_tune_retry_policy` | `rejects=` | retry budget exhaustion and compound-retry cost |
| `_tune_compound_budget` | `pass_interval=` | aggregate retry ceiling vs window duration |
| `_tune_parent_child` | `tides=` + `waves=` | silent-skip rate on parent→child current pairs |

### Replaying a session from disk

`LoggedTideweaver.get_tides()`, `get_rejects()`, and `get_scheduler_events()`
read the `QueueHandler` log files produced during a previous run — useful when
you want to analyse a completed overnight window without rerunning it:

```python
from incorporator.tideweaver import LoggedTideweaver, tune

# Read records written during a previous run.
tides   = await LoggedTideweaver.get_tides("ArbSession")
rejects = await LoggedTideweaver.get_rejects("ArbSession")
events  = await LoggedTideweaver.get_scheduler_events("ArbSession")

# Each element is a raw dict; the Tide data is under rec["tide"].
for rec in tides:
    t = rec["tide"]
    print(t["tide_number"], t["fired"], t["duration_sec"])

# Watershed lifecycle events land in get_scheduler_events():
for rec in events:
    evt = rec["scheduler_event"]
    if evt["event_type"] in ("watershed_started", "watershed_completed"):
        print(evt["event_type"], evt["detail"])
```

`get_tides()` reads the dedicated `logs/<logger_name>_tide.log` file
and returns records sorted by `tide_number` — single-file read, no merge
needed.

`get_rejects()` unions `_error.log` + `_api.log`, returning records with
a top-level `"reject"` key. Canal-layer rejects land in `_error.log`;
verb-layer HTTP failures may land in `_api.log` depending on
`is_url_traffic_error`. The union covers both:

```python
for rec in rejects:
    entry = rec["reject"]
    origin = "API" if entry["is_url_traffic_error"] else "codebase"
    print(f"[{origin}] {entry['error_kind']}: {entry['source']}")
```

### Green-wave coordination with `phase_offset_sec`

When three exchange currents all have the same `interval`, they compete for the
first scheduler pass and most of them emit `"awaiting_upstream"` skips on
pass 1.  `phase_offset_sec` on `Current` staggers the first tick of each
current to land just after the upstream it depends on:

```python
Stream(
    name="coinbase",
    cls=CoinbaseTicker,
    interval=30,
    phase_offset_sec=5.0,          # first tick fires 5 s after run start
    incorp_params={"inc_url": "..."},
)
```

The default is `0.0` — first tick fires immediately.  A stagger of
`~1 × expected_upstream_latency` typically eliminates most warm-up skips
without adding meaningful wall-clock latency to the window.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Master the fjord pattern that `Fjord` currents reuse | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Run the same diamond shape against a different domain | [Appendix — NASCAR Tideweaver](../appendix/nascar-tideweaver/README.md) |
| Land columnar Parquet artifacts at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Pick between in-process and cloud orchestration | [Appendix — Tideweaver vs. Prefect](../appendix/tideweaver-vs-prefect/README.md) |
| Configure Tideweaver from `watershed.json` for the CLI | [CLI & Configuration §9](../../docs/cli_and_configuration.md#9-the-tideweaver-subcommand--windowed-orchestration) |
| Ship as a Docker container with environment-driven config | [Deployment Guide](../../docs/deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/11-tideweaver/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
