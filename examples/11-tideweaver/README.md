***

# 🧵 Tutorial 11 — Tideweaver: Multi-Exchange Arbitrage Scanner Diamond (Capstone)

**Prerequisites:** [Tutorial 7](../07-stateful-refresh/README.md) (`refresh()` mechanics),
[Tutorial 8](../08-streaming-daemon/README.md) (`stream()`, both polling modes),
[Tutorial 10](../10-multi-source-fjord/README.md) (`fjord()` + `outflow(state)`).

Multi-exchange arb scanners monitor 5–20 exchanges concurrently for cross-venue
spreads. Three exchanges — Binance.us, Coinbase Advanced Trade, Kraken — each
ticking at its own cadence, converging on one fused "best bid / best ask /
spread / opportunity flag" tail.

**Tideweaver** lets you declare it instead of writing the async scheduler
yourself: one `Watershed.diamond()`, three exchange currents, one `Fjord`
current that snapshots them all and writes the consolidated arb signal. No
hand-rolled `asyncio` glue, no per-current restart logic, no manual snapshot
locking — the orchestrator runs a graph of named currents over a single time
window, each ticking on its own interval, with dependency edges that gate
downstream work until upstream produces fresh data.

> **CCXT comparison.** Real arb bots are usually built on
> [CCXT](https://github.com/ccxt/ccxt) with hand-rolled `asyncio` glue.  If you
> need 100+ exchange support out of the box, CCXT is the standard library.
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
    inc_url=[
        "https://api.binance.us/api/v3/depth?symbol=BTCUSDT",
        "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1",
        "https://api.kraken.com/0/public/Depth?pair=XBTUSD",
    ],
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
`watershed.json`), `"plan"` (the in-memory handoff shown above).

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
> clock and is **rejected at construction time** (AGENTS.md GOTCHA #9).
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
        outflow="arb_outflow.py",
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
2. Hand them to `outflow(state)`, defined in `arb_outflow.py`.
3. Materialise the returned rows into the dynamic output class.
4. Export them to `data/arb_signals.ndjson` (append-friendly — every flush adds rows).

### `arb_outflow.py` — symbol normalization + best-market join

The three exchanges return the same logical asset under different symbol shapes —
Binance "BTCUSDT", Coinbase "BTC-USD", Kraken "XXBTZUSD" / "XBTUSD".  The outflow
function normalizes to a canonical key (e.g. `"BTC"`), then computes best bid / best
ask across venues.

> **Sidecar naming is project convention, not a framework rule.**  The three
> fjord/Tideweaver tutorials each pick differently — T9 and T10 both use
> `outflow.py` (matches the `incorporator init --type fjord` scaffold);
> T11 (here) names it after the entry verb (`arb_outflow.py`).  Both
> are valid — pick whichever fits your deployment shape.
>
> **Output class is dynamic here.**  `outflow(state)` returns a list of
> dicts, so fjord builds the output class from the row keys (named after
> the sidecar stem).  Don't pre-declare a bare `class ArbOutflow(Incorporator):
> pass` — Pydantic V2's `extra='ignore'` would silently drop every field.
> The framework emits a one-time WARNING per bare-class trap.

```python
# arb_outflow.py
from typing import Any, Dict, List


# Map exchange-native symbols to a canonical asset code.  Real scanners
# load this from /exchangeInfo equivalents; we hard-code 2 pairs for the demo.
NORMALIZATION = {
    # Binance.us
    "BTCUSDT": "BTC", "ETHUSDT": "ETH",
    "BTCUSD":  "BTC", "ETHUSD":  "ETH",
    # Coinbase
    "BTC-USD": "BTC", "ETH-USD": "ETH",
    # Kraken
    "XXBTZUSD": "BTC", "XETHZUSD": "ETH",
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
        bid = float(getattr(row, bid_attr, 0) or 0)
        ask = float(getattr(row, ask_attr, 0) or 0)
        if bid and ask:
            out.append((asset, bid, ask, venue))
    return out


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    quotes = []
    quotes += _venue_quotes(state.get("BinanceBook", []),    "symbol",   "bidPrice", "askPrice", "binance")
    quotes += _venue_quotes(state.get("CoinbaseTicker", []), "product_id", "bid",    "ask",      "coinbase")
    quotes += _venue_quotes(state.get("KrakenTicker", []),   "_key",     "b",        "a",        "kraken")

    # Group by canonical asset.
    by_asset: Dict[str, List[tuple]] = {}
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
whichever exchanges are still up.

---

## Run it from the CLI

`watershed.json` is the declarative form.  Every Python knob has a JSON equivalent;
env-var interpolation (`${VAR}`, `${VAR:-default}`, `${file:/run/secrets/key}`) is
applied at load time.

```json
{
  "window": {"start": "${WINDOW_START}", "end": "${WINDOW_END}"},
  "shape": "diamond",
  "outflow": "arb_outflow.py",
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

One NDJSON `Tide` record per scheduler pass lands on stdout; status banners go to
stderr so log shippers can ingest stdout directly.

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
