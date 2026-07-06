***

# 🌊 Tutorial 10 — Multi-Source Fjord: Live Crypto Spread

**Prerequisites:** [Tutorial 1](../01-first-steps/README.md) (`incorp()`, `inc_dict`),
[Tutorial 7](../07-stateful-refresh/README.md) (`refresh()`), [Tutorial 8](../08-streaming-daemon/README.md)
(`stream()`, `Wave`, both polling modes), [Tutorial 9](../09-nascar-fantasy-fjord/README.md)
(the production-shape fjord preview).

You're scanning for cross-venue crypto arb. CoinGecko prices USD; Binance prices
USDT. For every symbol where both venues quote, you want one row carrying both
prices and the basis-point spread, updated every 60 seconds.

`fjord()` watches both sources concurrently with independent refresh cadences,
calls your `outflow(state)` join function on each export wave, and emits the
fused rows to NDJSON. The engine handles every concurrent refresh, every export
wave, the shared lock, the wave queue, and the dynamic output class around that
one function. You write the join. Everything else is declared.

You've already loaded a CoinGecko coin catalogue (T1), kept a Binance ticker
registry live (T8), and seen the full production fjord shape with seven sources
(T9). Now you'll learn the formal abstraction by fusing two sources end-to-end:
60-second fused output cadence, 30-second per-source refresh cadence, single
NDJSON tail. T11 generalises this same shape to N exchanges in a windowed graph.

> **Polling-mode policy.**  `fjord()` is the *multi-source equivalent
> of `stream(stateful_polling=True)`* — every per-source daemon refreshes
> its registry in place so the shared `outflow(state)` snapshot is always
> reading from live data (Wave records are introduced in T8).
> You don't pass `stateful_polling=` to `fjord()`; it's implicit in the
> contract.  If you need bulk chunked drains across multiple sources,
> reach for parallel `stream(stateful_polling=False, inc_page=...)` calls
> or T11's Tideweaver currents instead.
>
> **`cls.fjord()` (here) is a long-running daemon.**  Tideweaver's `Fjord`
> current (T11) is a per-tick *flush* that runs the same `outflow(state)`
> contract on a window scheduler.  Same shape; different scheduling context.

---

## The Goal

* **Source A:** `https://api.coingecko.com/api/v3/coins/markets`
  (USD prices, top 100 by market cap)
* **Source B:** `https://api.binance.us/api/v3/ticker/price`
  (USDT prices for every trading pair)
* **Fusion:** for each CoinGecko coin where a matching `{SYMBOL}USDT`
  exists in Binance, emit a row with both prices + the basis-point spread
* **Cadence:** sources refresh every 30 s; fused output writes every 60 s
* **Output:** `out/crypto_spread.ndjson` — append-friendly columnar format

Notice: no output class is declared. `fjord()` builds it dynamically
from the rows your `outflow()` returns, named after the code-file
stem (`outflow.py` → `Outflow`).

---

## Step 1: `inflow.py` + `outflow.py` — Build-Time Join, Read-Time Fuse

`fjord()` needs Python code (class definitions + the join logic), so
it lives in two sidecar files split by direction of data flow:
`inflow.py` wires the cross-source join into CoinGecko's own
`conv_dict` at build time; `outflow.py` declares the source classes and
reads the already-joined, already-coerced fields as plain attributes.

> **Read-time DX rule: coerce + join at build time; outflow reads plain
> attributes.**  The pre-rewrite version of this tutorial hand-rolled
> the join and the coercion inside `outflow()` itself —
> `pairs.inc_dict.get(f"{symbol}USDT")`, `float(getattr(pair, "price",
> 0) or 0)`.  Every one of those guards exists only because the raw
> field hadn't been resolved/coerced yet.  Move the resolution earlier
> (into the `conv_dict` at each source's own build time, via
> `link_to()` and `inc()`/`calc()`) and the guards disappear — not
> relocated, *eliminated* — because the framework's `is_garbage_value`
> null contract already did the defensive work once, at construction.
> See `docs/api_atlas.md`'s "Build-time vs read-time: where coercion +
> joins belong" section for the general rule.

CoinGecko needs the Binance registry to exist before its own
`conv_dict` runs, so it declares `depends_on=["BinancePair"]` —
this switches `fjord()`'s seed from all-parallel to **tiered**:
BinancePair seeds in tier 0, then `inflow(state)` fires for CoinGecko
in tier 1 with `state["BinancePair"]` already a live `IncorporatorList`.

```python
# examples/10-multi-source-fjord/inflow.py
from incorporator import inc, link_to


def _to_binance_symbol(sym: str) -> str:
    """CoinGecko ticker symbol -> Binance USDT pair key: 'btc' -> 'BTCUSDT'."""
    return f"{sym.upper()}USDT"


def inflow(state):
    overrides = {}
    if "BinancePair" in state:
        overrides["CoinGecko"] = {
            "conv_dict": {
                "current_price": inc(float, default=0.0),
                # link_to()'s conv_dict key must match the SOURCE field it
                # reads ("symbol") -- the dispatcher feeds it d.get(key).
                # name_chg (below, in crypto_spread.py) frees a clean,
                # distinctly-named attribute for outflow.py.
                "symbol": link_to(state["BinancePair"], extractor=_to_binance_symbol),
            }
        }
    return overrides
```

```python
# examples/10-multi-source-fjord/outflow.py
from datetime import datetime, timezone
from typing import Any

from incorporator import Incorporator


class CoinGecko(Incorporator):
    """Source A — CoinGecko USD market prices."""


class BinancePair(Incorporator):
    """Source B — Binance USDT-quoted prices."""


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join CoinGecko USD vs Binance USDT for overlapping symbols."""
    coins = state["CoinGecko"] or []
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    for coin in coins:
        pair = coin.binance_pair            # plain attribute -- None if unmatched
        if pair is None:
            continue

        gecko_usd = coin.current_price      # already a float
        binance_usdt = pair.price           # already a float (BinancePair's own conv_dict)

        # Cross-field validity check on the JOINED pair -- output-shaping
        # business logic, not a null guard, so it stays in outflow().
        if gecko_usd <= 0 or binance_usdt <= 0:
            continue

        spread_bps = round(((binance_usdt - gecko_usd) / gecko_usd) * 10_000, 2)

        rows.append({
            "symbol": pair.inc_code.removesuffix("USDT"),
            "coingecko_usd": gecko_usd,
            "binance_usdt": binance_usdt,
            "spread_bps": spread_bps,
            "fused_at": now,
        })

    return rows
```

Two source classes + one function. No daemon plumbing, no lock
acquisition, no wave emission, and — after this rewrite — no
`getattr(..., default) or fallback`, no `float(x or 0)`, no
`.inc_dict.get(...)` registry lookup: `fjord()` handles the plumbing,
the build-time `conv_dict` handles the defensive work, `outflow()`
reads plain attributes.

> **Don't pre-declare the output class.**  For multi-output
> `outflow(state) -> dict[ClassName, list[dict]]`, the framework builds
> one dynamic Pydantic class per dict key.  For single-output, it builds
> one named after the outflow file's stem (PascalCase).  Declaring a
> bare `class Outflow(Incorporator): pass` would suppress field
> inference and silently drop every row column.  T9 walks the
> multi-output version of this contract; T10's single-output shape works
> the same way under the hood.
>
> If you *do* pre-declare (e.g. to type the output for a downstream
> consumer), the subclass must declare every field you intend to keep —
> Pydantic V2's default `extra='ignore'` silently drops unknown fields.
> The framework emits a one-time WARNING per bare-class trap so you'll
> spot it in logs the first time it fires.

---

## Step 2: The Pipeline

```python
import asyncio
from incorporator import Incorporator

# Bring the classes into scope so fjord() can register them.
from outflow import BinancePair, CoinGecko


async def main():
    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": CoinGecko,
                "incorp_params": {
                    "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
                    "params": {"vs_currency": "usd", "per_page": 100, "page": 1},
                    "inc_code": "id",
                    "name_chg": [("symbol", "binance_pair")],
                },
                "depends_on": ["BinancePair"],     # waits for tier 0 -- see inflow.py
            },
            {
                "cls": BinancePair,
                "incorp_params": {
                    "inc_url": "https://api.binance.us/api/v3/ticker/price",
                    "inc_code": "symbol",
                    "conv_dict": {"price": inc(float, default=0.0)},
                },
            },
        ],
        inflow="examples/10-multi-source-fjord/inflow.py",
        outflow="examples/10-multi-source-fjord/outflow.py",
        export_params={"file_path": "out/crypto_spread.ndjson"},
        refresh_interval={"CoinGecko": 60, "BinancePair": 30},   # per-source cadences
        export_interval=60.0,                                    # fused output every 60 s
    ):
        op = wave.operation
        print(f"{op:40s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
```

---

> **Format constraint** *(same as `stream()`)*: fjord writes
> incrementally on every export wave, so the export target must be an
> **append-friendly** format: `.ndjson` / `.csv` / `.sqlite` / `.avro`.
> Parquet / Feather / ORC / Excel / XML / JSON all reject append mode.
> Pick NDJSON if unsure.

> **Seed-empty abort — print `wave.failed_sources` on every wave.**
> If *any* source yields zero records on the initial seed, the engine
> aborts the whole pipeline with a `fjord_incorp:<ClassName>` wave whose
> `failed_sources` explains why.  No daemons spawn, the `async for` loop
> exits cleanly with code 0 — which looks identical to a successful run
> with empty data unless you log `failed_sources`.  Always print it so
> geo-blocks (`api.binance.com` is blocked in the US — use
> `api.binance.us`), rate-limit responses, and transient API outages
> surface visibly.  For structured per-source errors with HTTP retry
> hints, reach for the returned ``IncorporatorList``'s ``.rejects``
> attribute (a ``list[RejectEntry]``) — each entry carries
> ``error_kind``, ``is_url_traffic_error`` (bool: ``True`` for
> HTTP/network failures, ``False`` for parse failures),
> ``retry_after`` (parsed from the HTTP header), and the parent
> ``wave_index``.  ``str(entry)`` includes the HTTP reason phrase
> when available, e.g. ``[HTTP 429 Too Many Requests]``.
>
> **`KeyError` on a missing peer?**  When `inflow(state)` raises
> `KeyError` because a peer source hasn't seeded yet, the seed-error
> formatter rewrites the wave's `failed_sources` to a copy-pasteable
> diagnostic — `"inflow(state) for source 'Race' raised KeyError on
> missing peer 'Track' — guard inflow(state) against missing keys
> (e.g. state.get('Track') or add depends_on=['Track'] to enforce
> ordering)"`.  Either guard the access defensively or declare the
> ordering on the dependent source's entry.

> **Refresh is on by default.**  Every fjord source automatically
> spawns a refresh daemon — you don't need `"refresh_params": {}`
> boilerplate on each entry.  To **opt OUT** of refresh on a specific
> source (e.g. a static catalogue that never changes), set
> `"refresh_params": None` on that entry.
>
> **Per-source intervals — two equivalent shapes:**
>
> ```python
> # Top-level dict by class name (one place, easy to scan):
> refresh_interval={"CoinGecko": 60, "BinancePair": 30}
>
> # OR inline per-entry (overrides the dict if both are set):
> {"cls": CoinGecko, "incorp_params": {...}, "refresh_interval": 60}
> ```
>
> The dict shape is JSON-friendly (works in `pipeline.json` too) and
> reads at a glance.  Inline overrides take priority when both are
> set on the same source.  Defaults: 60 s refresh, 300 s export, when
> nothing is specified.

> **CoinGecko is rate-limited — register the host throttle at startup.**
> The free tier allows roughly 5–15 calls per *minute*; v1.2.0 removed
> the implicit per-host registry that used to auto-pace it.  One line
> at process start re-engages the cap:
>
> ```python
> from incorporator import register_host_penstock
> from incorporator.io.penstock import SustainedPenstock
>
> register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
> ```
>
> Every subsequent `incorp()` / `stream()` / `fjord()` against
> `api.coingecko.com` inherits the cap.  Binance is unmetered on the
> public price endpoints used here; only register hosts you actually
> need to throttle.

> **Production observability — `LoggedIncorporator` for disk-readable
> logs.** Subclass the source classes from `LoggedIncorporator` and
> pass `enable_logging=True` on the fjord call; every wave and
> every `RejectEntry` lands in `logs/<ClassName>_{api,error,debug}.log`
> via a non-blocking `QueueHandler`.  URL/internet-traffic errors route
> to `_api.log`; parse/codebase errors route to `_error.log`.  Replay
> with `await ClassName.get_rejects()` (unions both files) from any
> other process — see [docs/debugging.md](../../docs/debugging.md) for
> the reader API and retry loop.

---

## What `fjord()` is Doing Under the Hood

1. **Tiered seed.** Because CoinGecko declares `depends_on=["BinancePair"]`,
   `fjord()` seeds in topological tiers instead of one flat parallel batch:
   BinancePair (tier 0) seeds first, then CoinGecko (tier 1) seeds with
   `inflow(state)`'s build-time `link_to()` override applied. Sources with
   no `depends_on` at all fall back to the fully-parallel `asyncio.gather`
   path — one wave per source either way.
2. **Per-source refresh daemons.** One daemon per entry. Each
   independently re-fetches on its own `refresh_interval` (override
   per entry — CoinGecko's free tier is rate-limited while Binance is
   not, so you may want different cadences).
3. **One outflow daemon.** Every `export_interval`, it snapshots every
   source under the shared lock, releases the lock, then calls your
   `outflow(state)` *in a worker thread* (via `asyncio.to_thread`) so a
   heavy CPU join doesn't block the refresh daemons.
4. **Dynamic output class.** From the rows `outflow()` returns, the
   engine uses `infer_dynamic_schema()` to build a Pydantic class
   named after the `outflow.py` stem — `Outflow`. The
   instances auto-register in `Outflow.inc_dict` for downstream
   `link_to(...)` use if you want to keep fused history in memory.
5. **Export.** Same handler dispatch as `stream()` — file extension
   picks the format.  Use any append-friendly format: `.ndjson` (the
   example), `.csv`, `.sqlite`, or `.avro`.  Parquet / Feather / ORC /
   Excel / XML / JSON reject append mode and would crash a streaming
   daemon — see the format-constraint note above.  As with `stream()`,
   each wave replaces the destination file with the latest fused
   snapshot; opt into accumulation with
   `export_params={"if_exists": "append"}` when you want a forensic
   ledger.
6. **Shutdown.** SIGTERM / Ctrl+C cancels every task; the wave queue
   drains; the `async for` loop exits.

---

## 🐳 Run It From the CLI

The same pipeline as a `pipeline.json`:

```json
{
  "inflow": "examples/10-multi-source-fjord/inflow.py",
  "outflow": "examples/10-multi-source-fjord/outflow.py",
  "stream_params": [
    {
      "cls_name": "CoinGecko",
      "incorp_params": {
        "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
        "params": {"vs_currency": "usd", "per_page": 100, "page": 1},
        "inc_code": "id",
        "name_chg": [["symbol", "binance_pair"]]
      },
      "depends_on": ["BinancePair"]
    },
    {
      "cls_name": "BinancePair",
      "incorp_params": {
        "inc_url": "https://api.binance.us/api/v3/ticker/price",
        "inc_code": "symbol",
        "conv_dict": {"price": "inc(float, default=0.0)"}
      }
    }
  ],
  "export_params": {"file_path": "out/crypto_spread.ndjson"},
  "refresh_interval": {"CoinGecko": 60, "BinancePair": 30},
  "export_interval": 60.0
}
```

> `inc()` sigils (`"inc(float, default=0.0)"`) resolve straight out of
> plain JSON — no sidecar needed for coercion alone. `link_to()` is
> different: it needs the *peer dataset* (`state["BinancePair"]`), which
> only exists inside a Python `inflow(state)` callable, so the join
> itself stays in `inflow.py` and `pipeline.json` only needs to point
> `"inflow"` at it plus declare `depends_on`.

```bash
incorporator validate pipeline.json
incorporator fjord pipeline.json --logs
```

The JSON uses `cls_name` (string) while the Python uses `cls` (class
reference). The CLI loader resolves `cls_name` by importing the
outflow file and looking up the class by name — that's how the JSON
stays serialisable.

---

## Two Advanced Patterns

The crypto-spread example above uses the simplest fjord shape:
N independent sources, one outflow function, one output file.  Two
extensions handle relational + multi-view cases.

### Pattern 1 — State-aware `inflow(state)`: live `link_to(...)` across sources

When one source's `conv_dict` needs a reference to another source's
already-loaded registry (e.g. resolving a foreign-key URL to the
actual Pydantic object), define a top-level `inflow(state)` callable
in `inflow.py`.  `fjord()` switches from parallel-seed to
declaration-order sequential seed, and calls `inflow(state)` before
each source's `incorp()` with the snapshots loaded so far:

```python
# swapi_inflow.py
from incorporator import link_to, link_to_list, split_and_get

get_id = split_and_get('/', -1, int)

def inflow(state):
    # On the Planet + Film seeds, state is empty / partial — be defensive.
    overrides = {}
    if "Planet" in state and "Film" in state:
        overrides["Person"] = {
            "conv_dict": {
                "homeworld": link_to(state["Planet"], extractor=get_id),
                "films":     link_to_list(state["Film"], extractor=get_id),
            }
        }
    return overrides
```

```python
async for wave in Incorporator.fjord(
    stream_params=[
        {"cls": Planet, "incorp_params": {"inc_url": ".../planets/", "inc_code": "id"}},
        {"cls": Film,   "incorp_params": {"inc_url": ".../films/",   "inc_code": "id"}},
        {"cls": Person, "incorp_params": {"inc_url": ".../people/",  "inc_code": "id"}},
    ],
    inflow="swapi_inflow.py",           # ← state-aware overrides
    outflow="swapi_outflow.py",
    export_params={"file_path": "data/people.ndjson"},
):
    print(wave)
```

`Person.homeworld` arrives as a fully-typed `Planet` object instead
of a URL string — so an outflow function can `getattr(person.homeworld,
"inc_name")` directly.

If `inflow.py` exists but defines *no* `inflow` function, fjord keeps
the legacy parallel-seed path (zero overhead) — the sidecar simply
extends the token resolver's allow-list as it always has.

### Pattern 2 — Multi-output: N derived classes from one outflow

When a single outflow run should write to more than one destination
file — e.g., one normalized entity table and a separate aggregation view
— return a `dict[ClassName, list[dict]]` from `outflow(state)` instead
of a plain list. Fjord builds one derived class per dict key and exports
each to its own file. One join, N analytical views:

```python
# swapi_outflow.py
def outflow(state):
    people = list(state["Person"])
    by_planet = {}
    for p in people:
        hw = getattr(p, "homeworld", None)
        hw_name = getattr(hw, "inc_name", "Unknown") if hw else "Unknown"
        by_planet.setdefault(hw_name, []).append(p.inc_name)

    return {
        "JediArchive":  [{"name": p.inc_name, "height": p.height} for p in people],
        "Demographics": [{"planet": hw, "citizens": len(c)}
                         for hw, c in by_planet.items()],
        "Filmography":  [{"name": p.inc_name, "films_count": len(p.films)}
                         for p in people],
    }
```

```python
async for wave in Incorporator.fjord(
    stream_params=[...],
    inflow="swapi_inflow.py",
    outflow="swapi_outflow.py",
    export_params={                               # one entry per output key
        "JediArchive":  {"file_path": "data/jedi.parquet"},
        "Demographics": {"file_path": "data/demographics.csv"},
        "Filmography":  {"file_path": "data/films.ndjson"},
    },
):
    print(wave)                                   # one wave per derived class
```

Each derived class gets its own `_daemon_tick` wrap so a failure
building `Demographics` doesn't block `JediArchive` from exporting.
The single-output `list[dict]` return remains the legacy path — list
return = one file.

> **Power-user note:** if `outflow.py` already declares a real
> `Incorporator` subclass with a matching name, fjord uses that class
> instead of the inferred-dynamic one — full type control on derived
> classes when you want it.

---

## When Fjord Shines

| Scenario | Why fjord wins |
|---|---|
| Joining two REST APIs that update at different rates | Independent per-source refresh cadences |
| Computing a derived dataset live (price spreads, latency joins, etc.) | `outflow()` runs CPU-heavy joins off the event loop |
| Needing a strong-typed output class without declaring one | `infer_dynamic_schema()` builds it from the rows |
| Production observability across a fan-out pipeline | One `Wave` per source per wave + per outflow wave — pipe to disk via `enable_logging=True` |

---

## Where to Go Next

> 👉 **Up next: [Tutorial 11 — Tideweaver](../11-tideweaver/README.md).**  T11 is the capstone — a declarative diamond orchestration across three exchanges in a windowed graph.  Runs entirely against local JSON fixtures, no APIs touched.

| Goal | Read |
|---|---|
| Capstone: orchestrate a diamond graph in a window | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| Master single-source stateful polling first | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| Master the single-source `stream()` daemon | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| See the 7-source production fjord with state-aware `inflow()` | [Tutorial 9 — NASCAR Fantasy Fjord](../09-nascar-fantasy-fjord/README.md) |
| Run the static (non-daemon) join variant | [Appendix — Crypto Graph Mapping](../appendix/crypto-graph-mapping/README.md) |
| Configure fjord from JSON for the CLI | [CLI & Configuration Guide](../../docs/cli_and_configuration.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/10-multi-source-fjord/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
