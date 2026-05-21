# API Atlas

The map you open when you know which verb you want but forget its kwargs.
`library_reference.md` is the auto-generated pdoc HTML for exhaustive
parameter tables; the `examples/NN-*/README.md` tutorials are narrative
and runnable. This atlas sits between them: paste-ready signatures,
3-7 step pseudocode, and one-line "when to reach for it" narrative for
every public callable.

> Every code block here is copy-paste runnable assuming
> `from incorporator import Incorporator, LoggedIncorporator` and
> `import asyncio` are already at the top of the file.

---

## Table of Contents

- [Discovery & ingestion](#discovery--ingestion)
  - [`test`](#test)
  - [`incorp`](#incorp)
- [Live updates](#live-updates)
  - [`refresh`](#refresh)
- [Persistence](#persistence)
  - [`export`](#export)
- [Daemons](#daemons)
  - [`stream`](#stream)
  - [`fjord`](#fjord)
- [REPL](#repl)
  - [`display`](#display)
- [Orchestration](#orchestration)
  - [`Tideweaver orchestration surface`](#tideweaver-orchestration-surface)
- [Telemetry](#telemetry)
  - [`Wave.log_meta`](#wavelog_meta)
- [Observability layer (`LoggedIncorporator`)](#observability-layer-loggedincorporator)
  - [`LoggedIncorporator` — shared `enable_logging=` note](#loggedincorporator--shared-enable_logging-note)
  - [`get_error`](#loggedincorporator-get_error)
  - [`log_debug` / `log_info` / `log_error`](#loggedincorporator-log_debug--log_info--log_error)
  - [`log_api`](#loggedincorporator-log_api)
  - [`log_meta`](#loggedincorporator-log_meta)
  - [`log_cls_info` / `log_cls_error`](#loggedincorporator-log_cls_info--log_cls_error)
- [Class-attribute reference](#class-attribute-reference)
- [Shared kwargs glossary](#shared-kwargs-glossary)
- [Where to Go Next](#where-to-go-next)

---

## Discovery & ingestion

### test

**Signature**
```python
@classmethod
async def test(
    cls: Type[TIncorporator],
    **kwargs: Any,
) -> Union[TIncorporator, "IncorporatorList[TIncorporator]", List[Any]]:
```

**What it does (pseudocode)**
1. Force `__inspect=True` so the network engine returns the raw payload tree alongside the parsed instances.
2. Cap `call_lim=1` when a paginator is set; default `timeout=5.0` so unresponsive endpoints fail fast.
3. Delegate to `incorp(**kwargs)` inside a try/except that traps every exception and routes to `analyze_error()`.
4. The inspector walks the payload, prints a tree view plus suggested `inc_code` / `inc_name` / `rec_path` / `conv_dict`.
5. Slice the resulting list down to at most 3 records so a giant endpoint can't flood your terminal.
6. Return the truncated list (or an empty `IncorporatorList` on failure) — diagnostics have already printed by then.

**When to reach for it**
The 30-second Shady Jimmy probe — point it at an endpoint you've never seen, read the printed suggestions, paste them into a real `incorp()` call. Use it whenever you're about to hand-write `rec_path` and `conv_dict` from a tab full of raw JSON.

**Common kwargs**
- Everything `incorp()` accepts — `test()` forwards `**kwargs` unchanged.
- `timeout` — overrides the 5-second safety default if your endpoint is genuinely slow.
- `call_lim` — explicitly override the 1-page paginator cap.
- `inc_page` — pass a paginator to inspect how pagination shapes the payload.

**Yields / returns**
An `IncorporatorList` of at most 3 records on success, an empty `IncorporatorList` on fetch failure. Inspector output is the real product — the return value is for poking at structure in the REPL afterward.

**See also**
[Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md) ·
[Debugging Guide](./debugging.md)

---

### architect

**Signature**
```python
@classmethod
async def architect(
    cls: Type[TIncorporator],
    sources: Mapping[str, Union[str, Path, Mapping[str, Any]]],
    *,
    output: Literal["report", "python", "json", "plan"] = "report",
    shared_kwargs: Optional[Mapping[str, Any]] = None,
) -> Optional[Union[str, OrchestrationPlan]]:
```

**What it does (pseudocode)**
1. Resolve every `sources` value: URL string → `inc_url=`, Path-like or existing file → `inc_file=`, dict → spread verbatim as `incorp()` kwargs.  `shared_kwargs` propagates to every probe; per-source kwargs win on conflict.
2. For each source, create a throwaway subclass `type(f"_ArchitectProbe_{name}", (cls,), {})` and call its `test(**probe_kwargs, __capture_into=[...])`.  The throwaway shields the user's class from probe-driven mutation (`cls.inc_url` / `cls.inc_file` / `cls._incorp_kwargs` / `cls.inc_dict`); the `__capture_into` sidechannel suppresses the per-source print and routes the inspector's structured `SourceProfile` into a list.
3. Run cross-source analysis on the captured profiles: detect `fanout` (one source's PK appears as a non-PK field in all others), `diamond` (multiple sources share a PK field name — needs a tail Fjord), `parallel` (disjoint field sets), or `custom` (some overlap but no clear pattern).
4. For each downstream edge, pick a `Penstock` via a three-tier confidence ladder: known-strict host registry → 429 observed during probe → no penstock.
5. Dispatch on `output=`: `"report"` prints inspector output + cross-source hints; `"python"` emits a paste-ready Python module (class defs + `Stream(...)` constructors + `Watershed.<shape>(...)` + a `Tideweaver` runner); `"json"` emits a paste-ready `watershed.json` that round-trips through `load_watershed()`; `"plan"` returns the structured `OrchestrationPlan` dataclass directly so callers can probe → tune → run in one expression via `plan.to_watershed()`.

**When to reach for it**
The multi-source counterpart of `test()`.  Reach for it when you have several unknown endpoints / fixtures and you want a Tideweaver scaffold rather than per-call `incorp()` advice.  Especially useful for: cross-exchange arb diamonds, NASCAR race-day fusion graphs, anywhere you'd otherwise sketch the topology on a napkin first.

**Common kwargs**
- `sources` — mapping of `name` → URL string, file path / `Path`, or dict of `incorp()` kwargs.  Pass `{"verb": "fjord", ...}` in the dict form to nominate a tail Fjord on diamond shapes.
- `output` — `"report"` (default, prints only), `"python"` (returns + prints a module), `"json"` (returns + prints `watershed.json`), `"plan"` (returns the structured `OrchestrationPlan` — no print, no rendering).
- `shared_kwargs` — common `timeout` / `headers` / `requests_per_second` applied to every probe.

**Yields / returns**
- `output="report"` → `None` (prints only).
- `output="python"` / `"json"` → the rendered string (also printed to stdout for human eyeballs).
- `output="plan"` → the `OrchestrationPlan` dataclass directly.  Pair with `plan.to_watershed(window=...)` for the in-memory probe → tune → run handoff (no disk round-trip).

**Worked example**
```python
class Coin(Incorporator):
    pass

await Coin.architect(
    sources={
        "binance":  "examples/11-tideweaver/fixtures/binance_book.json",
        "coinbase": "examples/11-tideweaver/fixtures/coinbase_ticker.json",
        "kraken":   "examples/11-tideweaver/fixtures/kraken_ticker.json",
    },
    output="json",
)
# → prints + returns a watershed.json that loads cleanly:
#   incorporator tideweaver run watershed.json

# In-memory probe → tune → run handoff (no disk round-trip):
plan = await Coin.architect(sources={...}, output="plan")
plan.currents[0].interval_hint = 10  # tune
watershed = plan.to_watershed()
async for tide in Tideweaver(watershed).run():
    ...
```

**Confidence honesty** — what `architect()` will NOT decide for you (left as `_TODO_` placeholders or commented suggestions in the scaffold):
- `interval` — a freshness SLO call.
- `gate_mode` — defaults to `"hard"`; alternatives noted.
- `Reservoir.depth` — defaults to `1`; scaling rule noted.
- `Spillway` — defaults to `DropOldest`.
- `phase_offset_sec` — needs full-graph timing.
- `Fjord` tail placement + `outflow.py` sidecar — user wires fan-in if desired.

**See also**
[Canal toolkit primitives](#canal-toolkit-primitives) ·
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) ·
[`docs/cli_and_configuration.md §9`](./cli_and_configuration.md)

---

### incorp

**Signature**
```python
@classmethod
async def incorp(
    cls: Type[TIncorporator],
    inc_url: Optional[Union[str, List[str]]] = None,
    inc_file: Optional[Union[str, "os.PathLike[str]", List[Union[str, "os.PathLike[str]"]]]] = None,
    inc_parent: Optional[Union[TIncorporator, "IncorporatorList[TIncorporator]"]] = None,
    inc_child: Optional[str] = None,
    inc_code: Optional[str] = None,
    inc_name: Optional[str] = None,
    excl_lst: Optional[List[str]] = None,
    conv_dict: Optional[Dict[str, Any]] = None,
    name_chg: Optional[List[Tuple[str, str]]] = None,
    inc_page: Optional[AsyncPaginator] = None,
    inflow: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
```

**What it does (pseudocode)**
1. If `inflow=` is set, load the sidecar and resolve string-form tokens in `conv_dict` / `inc_page`.
2. If `inc_parent=` is given, hand off to the Parent-Child router (HATEOAS drill via `inc_child` JSONPath).
3. Normalise `inc_url` / `inc_file` into a source list; remember the seed kwargs on the class so `refresh()` can replay them.
4. Fan out the source list concurrently through the network engine (sliding window of 50, rate-limited, exponential-backoff retries).
5. Hand the parsed payload to the schema factory; build a dynamic Pydantic model and instantiate one record per row.
6. Register every instance into `cls.inc_dict`; return a single instance for one record, or an `IncorporatorList` (carrying `.failed_sources`) otherwise.

**When to reach for it**
This is the cold-start verb — the one you call when a new endpoint hits your radar and you want a working object graph in three lines. Backtest data prep, one-shot CSV-to-Pydantic conversions, the seed call before any daemon takes over.

**Common kwargs**
- `inc_url` / `inc_file` — single string or list; list triggers concurrent fan-out.
- `inc_code` — field name to use as the primary key in `inc_dict`.
- `inc_parent` + `inc_child` — drill a parent list's URLs into child fetches (HATEOAS).
- `conv_dict` — `{field_name: converter}` pre-validation coercion (`inc`, `calc`, `link_to`, `pluck`, ...).
- `inc_page` — `AsyncPaginator` subclass for paginated endpoints.
- `rec_path` — dot-notation drill into a wrapper response (e.g. `"results"`).
- `concurrency_limit`, `requests_per_second`, `timeout`, `headers` — network knobs.

**Yields / returns**
Returns a single `TIncorporator` for one-record sources, otherwise an `IncorporatorList[TIncorporator]` whose `.failed_sources` is the DLQ.

**See also**
[Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md) ·
[Tutorial 5 — Parent → Child Drilling](../examples/05-parent-child-drilling/README.md) ·
[Library Reference](./library_reference.md)

---

### register_host_throttle

**Signature**
```python
def register_host_throttle(host: str, factory: Callable[[], ThrottleStrategy]) -> None:
```

**What it does (pseudocode)**
1. Registers a per-host throttle factory keyed by lowercase hostname.
2. The factory is called once per `resolve_throttle()` invocation so each fan-out leg gets independent state.
3. Re-registering the same host replaces the previous factory.

**When to reach for it**
The framework ships with **no implicit per-host throttling**.  Use this to attach a throttle for an in-house API or any public host that imposes a documented rate ceiling.  The alternative — `incorp(..., requests_per_second=X)` per call — is fine for one-shot scripts; the registry is the right tool when you have many call sites against the same host and want one source of truth.

**Worked example**
```python
from incorporator import register_host_throttle
from incorporator.io.throttle import FixedIntervalThrottle, BurstThrottle

# Conservative rate for CoinGecko's anon tier (5-15 req/min documented).
register_host_throttle("api.coingecko.com", lambda: FixedIntervalThrottle(0.2))

# Bursty in-house API: 50 req/s sustained, 200-burst tolerance.
register_host_throttle(
    "api.internal.acme.com",
    lambda: BurstThrottle(requests_per_second=50.0, burst=200),
)
```

**Common kwargs**
- `host` — lowercase hostname; `urllib.parse` extracts this from URLs at resolve time.
- `factory` — zero-arg callable returning a fresh `ThrottleStrategy`.  Strategies are stateful (token-bucket counters, last-fire timestamps); the per-call factory keeps fan-out legs independent.

**Yields / returns**
`None`.  Side-effect-only: mutates the module-level `_HOST_FACTORIES` dict.

**Related**
- `incorporator.io.throttle.resolve_throttle(source, requests_per_second=, burst=)` — the resolver every `incorp()` call routes through.  Five-tier precedence: env-var bypass > `rps<=0` > caller rps > registered host > `DEFAULT_RPS=15` fallback.
- `incorporator.io.throttle.known_host_rates()` — diagnostic view of `host → float` rates currently registered.

**See also**
[Tutorial 1](../examples/01-first-steps/README.md) — CoinGecko example with explicit registration ·
[Library Reference](./library_reference.md)

---

## Live updates

### refresh

**Signature**
```python
@classmethod
async def refresh(
    cls: Type[TIncorporator],
    instance: Optional[Union[str, Path, TIncorporator, List[TIncorporator]]] = None,
    new_url: Optional[Union[str, List[str]]] = None,
    new_file: Optional[Union[str, List[str]]] = None,
    inc_child: Optional[str] = None,
    inc_code: Optional[str] = None,
    inc_name: Optional[str] = None,
    excl_lst: Optional[List[str]] = None,
    conv_dict: Optional[Dict[str, Any]] = None,
    name_chg: Optional[List[Tuple[str, str]]] = None,
    inc_page: Optional[AsyncPaginator] = None,
    inflow: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
```

**What it does (pseudocode)**
1. Replay the seed call's persisted kwargs (`cls._incorp_kwargs`) so `params`, `headers`, `rec_path`, `conv_dict` apply automatically.
2. Resolve instance mode: `None` → every live instance in `inc_dict`; `str | Path` → re-source against a new URL/file; `list` / `obj` → targeted partial update.
3. Deduplicate origin URLs across the resolved instance set (1000 instances sharing 20 URLs ⇒ 20 fetches).
4. Optionally drill a parent → child path via `inc_child` and dedupe the extracted child URLs.
5. Fan out the deduplicated source list concurrently through the network engine.
6. Rebuild instances in a worker thread; Pydantic field updates mutate existing Python references in-place — callers holding the old list see fresh values without reassignment.

**When to reach for it**
The one-shot re-fetch verb — call it from a REPL or wrap it in your own scheduler when you want fresh field values mutated into the existing object graph without rebuilding the world. For daemonised live mark-to-market reach for `fjord()` (Tutorial 10) instead; `refresh()` itself is manual.

**Common kwargs**
- `instance` — mode selector (`None`, new URL string, or specific instances).
- `new_url` / `new_file` — explicit source override; also updates `cls.inc_url` / `cls.inc_file` so subsequent in-state refreshes hit the new source.
- `inc_child` — drill nested child URLs for re-enrichment.
- `conv_dict`, `excl_lst`, `name_chg` — override the seed call's persisted settings on this refresh tick.
- `**kwargs` — anything `incorp()` accepts; user-supplied keys win on conflict with persisted seed kwargs.

**Yields / returns**
Same as `incorp()` — a single instance or an `IncorporatorList[TIncorporator]`. Existing references are mutated in-place.

**See also**
[Tutorial 7 — Stateful Refresh](../examples/07-stateful-refresh/README.md) ·
[Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md)

---

## Persistence

### export

**Signature**
```python
@classmethod
async def export(
    cls: Type[TIncorporator],
    *,
    instance: Union[str, Path, TIncorporator, List[TIncorporator]],
    file_path: Optional[Union[str, Path]] = None,
    format_type: Optional[FormatType] = None,
    compression: Optional[str] = None,
    sql_table: Optional[str] = None,
    if_exists: str = "replace",
    outflow: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> None:
```

**What it does (pseudocode)**
1. Resolve mode: if `file_path=None`, `instance` is treated as the output path and the data source is `cls.inc_dict.values()`; otherwise `instance` is the data and `file_path` is the destination.
2. If `outflow=` is set, run `transform(instances)` in a worker thread and peek the first row to learn the post-transform field shape.
3. Infer the writer format from the extension (or honour `format_type=`); look up the matching handler under `io/handlers/`.
4. Wrap the source in a lazy generator — `model_dump()` runs per row, not per list — so 10M-row exports stay flat on RSS.
5. JSON/NDJSON fast-path: yield Pydantic instances directly so the handler can call `model_dump_json()` (~15-25% throughput win).
6. Hand the lazy iterator to the format writer; optionally compress the output file in a background thread.

**When to reach for it**
The fan-out write verb — point `incorp()`'s result at a Parquet warehouse, a SQLite analytics DB, an NDJSON tail file. Cross-format pivots ("JSON API in, Parquet out") cost one extra `await` and zero schema declarations.

**Common kwargs**
- `instance` — in-state mode (path string) or explicit data (list / model).
- `file_path` — destination; omit to enter in-state mode.
- `format_type` — `FormatType` enum override when the extension is ambiguous.
- `compression` — `"gz"`, `"bz2"`, `"xz"`, `"zip"`, `"tar"`, `"zstd"`, `"lz4"`, `"snappy"`, `"brotli"`.
- `sql_table`, `if_exists` — SQLite knobs (`"replace"` / `"append"` / `"fail"`).
- `outflow` — sidecar `.py` defining `transform(instances) -> Iterable`.
- `delimiter` (CSV/TSV/PSV), `xml_root`, `json_indent` — handler-specific overrides.

**Yields / returns**
`None`. Side effect: the file is written; failures raise `IncorporatorFormatError`.

**See also**
[Tutorial 2 — Data Lake Pivot](../examples/02-data-lake-pivot/README.md) ·
[Tutorial 3 — Universal Formats](../examples/03-universal-formats/README.md) ·
[Formats & Compression](./formats_and_compression.md)

---

## Daemons

### stream

**Signature**
```python
@classmethod
async def stream(
    cls: Type[TIncorporator],
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]] = _UNSET,
    export_params: Optional[Dict[str, Any]] = None,
    poll_interval: Optional[float] = None,
    stateful_polling: bool = False,
    refresh_interval: Optional[float] = None,
    export_interval: Optional[float] = None,
    inflow: Optional[Union[str, Path]] = None,
    outflow: Optional[Union[str, Path]] = None,
) -> AsyncGenerator["Wave", None]:
```

**What it does (pseudocode)**
1. Front-door validation: reject chunking + paginator + monolithic output format (would silently overwrite previous chunks).
2. If `outflow=` is set, refuse chunking mode (per-chunk state has no persistent registry) and switch the receiver class to the user-defined subclass.
3. Load any `inflow.py` sidecar; capture an optional `inflow(state)` callable for the stateful path.
4. Stateful branch: delegate to the fjord engine with a synthesised identity outflow — preserves Python-object identity in `inc_dict` across waves.
5. Chunking branch: delegate to `run_pipeline` — every iteration calls `incorp()` for the next chunk, optionally `refresh()` then `export()`, and releases per-chunk state before fetching the next.
6. Yield one `Wave` per iteration (chunk in chunking, refresh / export tick in stateful) — engine completion ends the generator.

**When to reach for it**
The chunking daemon — unattended overnight drain of a paginated source, one page in memory at a time, so 10M-row pulls stay flat on RSS. Reach for `fjord()` instead when you want the live stateful daemon shape (mark-to-market dashboards, multi-source polling).

**Common kwargs**
- `incorp_params` — kwargs forwarded to `incorp()` every wave (or just once in stateful mode).
- `refresh_params` — kwargs for `refresh()`; omit to skip refresh, pass `{}` to run with defaults.
- `export_params` — kwargs for `export()`; chunking mode forces `if_exists="append"`.
- `stateful_polling` — `False` (chunking, default) vs `True` (delegates to the fjord engine for single-source stateful runs).
- `poll_interval` / `refresh_interval` / `export_interval` — interval cascade; refresh and export each fall back to `poll_interval`.
- `inflow=` — sidecar for token-resolver helpers plus an optional `inflow(state)` hook (stateful only).
- `outflow=` — user-defined subclass for the receiver; **stateful only** (raises `ValueError` in chunking mode).

**Yields / returns**
`AsyncGenerator[Wave, None]` — one `Wave` per chunk or per daemon iteration. `wave.operation` is `"chunk"`, `"incorp"`, `"refresh"`, or `"export"`.

**See also**
[Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md) ·
[Streaming & Pagination Deep Dive](./streaming_and_pagination.md)

---

### fjord

**Signature**
```python
@classmethod
async def fjord(
    cls,
    stream_params: List[Dict[str, Any]],
    outflow: Union[str, Path],
    export_params: Dict[str, Any],
    refresh_interval: Optional[float] = None,
    export_interval: Optional[float] = None,
    inflow: Optional[Union[str, Path]] = None,
) -> AsyncGenerator["Wave", None]:
```

**What it does (pseudocode)**
1. Validate `stream_params` shape — every entry must carry a `cls` (Incorporator subclass) and `incorp_params`; fail loud on missing keys.
2. Load any `inflow.py` sidecar; if it defines a top-level `inflow(state)` callable, switch from parallel gather seeding to sequential dependent seeding.
3. Derive the dynamic output class name from the `outflow=` file stem (PascalCase), and load the `outflow(state)` callable.
4. Seed every source concurrently with one `incorp()` call apiece (or sequentially when `inflow(state)` is defined).
5. Run a refresh daemon per source on its own `refresh_interval`; the registries stay independent until export time.
6. On every `export_interval`, snapshot all source registries, call `outflow(state)`, build the dynamic output class, export the combined rows.
7. Yield a `Wave` per phase: `"fjord_incorp:<Class>"`, `"fjord_refresh:<Class>"`, `"export:<Class>"`, and `"outflow:<DynamicClass>"`.

**When to reach for it**
The stateful live-daemon verb — concurrent source refresh + outflow fusion. Live mark-to-market dashboard fusing CoinGecko USD + Binance USDT, fantasy NASCAR Sunday fusing five APIs into one truth file, or a single-source live registry that keeps mutating in place (N=1 fjord is legitimate when you want the daemon shape without writing a custom loop).

**The `inflow(state)` contract**

When the `inflow.py` sidecar defines a top-level `inflow(state)` callable, fjord switches from parallel-gather seeding to sequential dependent seeding so later sources can read from earlier ones. The hook is called **once per source, just before that source is seeded**, and must return per-class kwarg overlays:

1. **Call cadence.** `inflow(state)` fires once per source in `stream_params` order — *before* that source's `incorp()` runs. With N sources, the hook is invoked N times.
2. **Progressive state.** `state` is a `dict[str, IncorporatorList]` keyed by source class name and is populated incrementally — the first call sees an empty dict; the second sees only the first source's list; the Nth sees N-1 entries.
3. **Guard for missing keys.** Because earlier calls see a partial `state`, every read must guard: `state.get("Track")` or `if "Track" in state:`. When the keys you need aren't there yet, return `{}` (no overrides for this source).
4. **Return shape.** `dict[str, dict[str, Any]]` — a per-class kwarg overlay merged into that source's `incorp_params` just before seeding. Outer key = source class name; inner dict = kwargs to overlay (e.g. `inc_url`, `conv_dict`).
5. **Failure mode.** An unguarded `KeyError` (or any exception) inside `inflow(state)` aborts the pipeline and emits a `fjord_incorp:<source>` wave whose `failed_sources` carries the exception's `str()`. The remaining sources never seed.

**Output classes are always built by the framework — don't pre-declare them in the outflow sidecar.**

* **Single-output** (`outflow(state) -> list[dict]`): one dynamic class is built, named after the **outflow file's stem** in PascalCase. Fields are inferred from the returned rows.
* **Multi-output** (`outflow(state) -> dict[ClassName, list[dict]]`): one dynamic class per dict key, named exactly that key. Fields inferred per output.

Declaring a bare `class FantasyTeam(Incorporator): pass` in the outflow file *suppresses* field inference — the framework reuses your declared class and Pydantic silently drops every row field that isn't on it. Only pre-declare an output class when you want **full type control** with explicit field declarations; otherwise let the framework build the dynamic class.

**Navigating `state` inside `outflow(state)`:**

```python
def outflow(state):
    """state is dict[str, IncorporatorList], keyed by source class name."""
    rows = []
    for inv in state["Invoice"]:            # iterate as a list
        # link_to() in inflow() already resolves inv.Vehicle.VIN to a
        # live Pydantic instance — no extra lookup needed in outflow.
        nht = state["NHTSASpec"].inc_dict.get(inv.Vehicle.VIN)
        rows.append({
            "vin": inv.Vehicle.VIN,
            "nht_make": nht.Make if nht else None,
        })
    return rows
```

Three lessons: iterate the registry as a list; look up by `inc_dict[key]`; trust foreign keys that `link_to(state["..."])` resolved during inflow (don't re-look them up).

**Common kwargs**
- `stream_params` — list of `{"cls": ..., "incorp_params": {...}, "refresh_params": {...}, "refresh_interval": ..., "export_params": {...}}` per source.
- `outflow` — required path to `outflow.py` defining `outflow(state) -> list[dict]` (or `dict[ClassName, list[dict]]` for multi-output).
- `export_params` — kwargs forwarded to the dynamic output class's `export()`; the joined output must have a destination.
- `refresh_interval` / `export_interval` — default cadences; per-entry overrides on `stream_params` win.
- `inflow` — sidecar for token-resolver helpers and the optional `inflow(state)` sequential seed hook.

**Yields / returns**
`AsyncGenerator[Wave, None]` — one per phase. The `operation` field identifies which source / class produced the wave.

**See also**
[Tutorial 9 — NASCAR Fantasy Fjord](../examples/09-nascar-fantasy-fjord/README.md) ·
[Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md) ·
[Appendix — NASCAR Tideweaver](../examples/appendix/nascar-tideweaver/README.md)

---

## REPL

### display

**Signature**
```python
def display(self) -> None:
```

**What it does (pseudocode)**
1. Read `self.__class__.__name__`, falling back to `"UnknownClass"` if absent.
2. Print one line containing `class`, `inc_code`, `inc_name`, and `last_rcd`.
3. Return `None`.

**When to reach for it**
The REPL spot-check. Use it when you're tabbing through `launches.inc_dict` interactively and want a one-liner identity dump without typing `model_dump_json(indent=2)`. For structured output in production, use `model_dump_json()` directly.

**Common kwargs**
- None — `display()` is parameter-free.

**Yields / returns**
`None`. The line is printed to stdout.

**See also**
[Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md)

---

## Orchestration

### Tideweaver orchestration surface

**Signatures**
```python
class Tideweaver:
    def __init__(
        self,
        watershed: Watershed,
        *,
        tick_factory: Optional[TickFactory] = None,
        pass_interval: Optional[float] = None,
    ) -> None: ...
    async def run(self) -> AsyncIterator[Tide]: ...

class Watershed(BaseModel):
    @classmethod
    def chain(cls, *, window, currents, gate_mode=None, flow=None, **kwargs) -> "Watershed": ...
    @classmethod
    def diamond(cls, *, window, head, middle, tail, gate_mode=None, flow=None, **kwargs) -> "Watershed": ...
    @classmethod
    def fanout(cls, *, window, source, sinks, gate_mode=None, flow=None, **kwargs) -> "Watershed": ...
    @classmethod
    def parallel(cls, *, window, currents, **kwargs) -> "Watershed": ...
```

`gate_mode=` is the shorthand (one of `"hard"` / `"soft"` / `"weir"`, default
`"hard"`).  `flow=` is the full-dict form: a :class:`FlowControl` composing
gate + surge_barrier + penstock + reservoir + spillway.  They are mutually
exclusive — pass one, neither (defaults to `gate_mode="hard"`), but not both.
`Edge(gate_mode=..., flow=...)` follows the same mutex rule for custom
explicit-edge graphs.  See [Canal toolkit primitives](#canal-toolkit-primitives)
below for the full per-edge FlowControl surface.

**What it does (pseudocode)**
1. Construct a `Watershed` via one of the four shape constructors (`chain` / `diamond` / `fanout` / `parallel`) — or the bare `Watershed(...)` for custom mixed-mode edges.
2. The validator folds `Current.depends_on` declarations into `Edge`s, checks unique names, validates the time window, runs a toposort to reject cycles.
3. Pass the `Watershed` to `Tideweaver(watershed)`; the scheduler computes `pass_interval` (default `min(interval)/2`, clamped `[0.05, 1.0]`).
4. `async for tide in Tideweaver(...).run()` — on every scheduler pass, walk the topological order; for each `Current`, gate on interval + upstream wave freshness, then fire the per-tick body.
5. Verb-typed `Current` subclasses dispatch differently: `Stream` runs chunking `cls.stream(...)` and parks a strong-ref snapshot on `_tideweaver_snapshot`; `Fjord` is a per-tick flush (`outflow(state)` → build → export); `Export` runs `cls.export(...)`.
6. When the window closes, the scheduler drains in-flight ticks (`drain_timeout` seconds), then exits.

**When to reach for it**
The windowed orchestration verb — when one source's `stream()` isn't enough, when N sources need independent cadences, when downstream work must gate on upstream freshness. Multi-exchange arb scanning across a market-open window, race-day telemetry fusion (laps + pits + flags → driver state), any "run these feeds together for the next four hours" workload.

**Common kwargs**
- `window=(start, end)` — inclusive start, exclusive end; the run exits at `end`.
- `currents=[...]` — list of `Stream` / `Fjord` / `Export` (or bare `Current` for tests).
- `edges=[...]` — explicit edges; each `Edge(from_name=..., to_name=..., gate_mode="hard"/"soft"/"weir")` shorthand or `flow=FlowControl(...)` full-dict form.
- `inflow=` / `outflow=` — graph-level sidecar defaults; per-current values win.
- `gate_mode` (shape constructors) — `"hard"` (default), `"soft"`, or `"weir"`. Mutually exclusive with `flow=`.
- `flow` (shape constructors) — full `FlowControl(...)` shared across every edge produced by the shape. Mutually exclusive with `gate_mode=`.
- `drain_timeout` — seconds the scheduler waits for in-flight ticks at window close.
- `pass_interval` (`Tideweaver`) — override the auto-derived scheduler tick.

**Yields / returns**
`Tideweaver.run()` yields one `Tide` per scheduler pass, carrying `tide_number`, `fired`, `skipped: List[(name, reason)]`, `duration_sec`.

**See also**
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) ·
[Appendix — NASCAR Tideweaver](../examples/appendix/nascar-tideweaver/README.md) ·
[Library Reference](./library_reference.md)

---

### Canal toolkit primitives

Per-edge flow control.  Every `Edge` carries a `FlowControl` composing
five orthogonal primitives — each is a Pydantic strategy hierarchy and
serialises into `watershed.json` via discriminated unions.

**Signatures**
```python
class FlowControl(BaseModel):
    gate: Gate                                    # default HardLock()
    surge_barrier: Optional[SurgeBarrier] = None
    penstock: Optional[Penstock] = None
    reservoir: Reservoir                          # default Reservoir(depth=1)
    spillway: Spillway                            # default DropOldest()


# Gate — pass / hold decision per upstream
class HardLock(Gate): ...    # block until a fresh upstream wave arrived
class SoftPass(Gate): ...    # fire on own cadence regardless of upstream
class Weir(Gate): ...        # gate on wave freshness, no skip-ahead

# SurgeBarrier — conditional override when upstream runs long
class SurgeBarrier(BaseModel):
    threshold_multiple: float = 2.0
    action: Literal["skip", "halt", "bypass"] = "skip"

# Penstock — edge-level rate limit
class SustainedPenstock(Penstock):    # rate_per_sec: float
class BurstPenstock(Penstock):        # rate_per_sec: float, burst: int
class WindowPenstock(Penstock):       # window_sec: float, cap: int
class BackpressurePenstock(Penstock): # min_rate < max_rate, scales with reservoir
class SignalPenstock(Penstock):       # rate_fn(scheduler, edge_state, now) -> float

# Reservoir — per-edge FIFO buffer of recent waves
class Reservoir(BaseModel):
    depth: int = 1   # 1..1024

# Spillway — overflow handler when reservoir is full
class DropOldest(Spillway): ...                       # silent default
class RaiseOverflow(Spillway): ...                    # WARNING log per displacement
class ExportToArchive(Spillway):                       # strong-ref backlog list
    archive_cls: Type[Incorporator]
```

**What each does (pseudocode)**
- **`Gate`** — `HardLock` blocks until upstream has a wave newer than the dependent's last consumption; `SoftPass` ignores upstream entirely (sequence-only); `Weir` gates on freshness without triggering surge logic — fire-on-own-cadence once upstream emitted at least one wave.
- **`SurgeBarrier`** — when an upstream's currently-running tick exceeds `threshold_multiple × upstream.interval`, fires `action`: `"skip"` (skip this dependent pass), `"halt"` (skip until upstream finishes), `"bypass"` (fire ignoring this edge's gate AND penstock).
- **`Penstock`** — per-edge rate-limit strategy.  `SustainedPenstock` is a flat rate (1/rate_per_sec min gap); `BurstPenstock` token bucket with burst capacity; `WindowPenstock` sliding-window cap; `BackpressurePenstock` interpolates `max_rate → min_rate` as the reservoir fills; `SignalPenstock` calls a user callable for the live rate.
- **`Reservoir`** — buffers the last N wave-snapshots on each edge.  Default `depth=1` keeps the most recent.  Read by `BackpressurePenstock` for fullness; surfaced to user code via `edge_state.waves`.
- **`Spillway`** — fires when a wave is displaced from a full reservoir.  `DropOldest` silently evicts; `RaiseOverflow` emits a WARNING log; `ExportToArchive` extends `archive_cls._spillway_backlog` (strong-ref) with the displaced instances.

**Worked example**
```python
from incorporator.observability.tideweaver import (
    Edge, FlowControl, Watershed,
    HardLock, SurgeBarrier, BurstPenstock, Reservoir, ExportToArchive,
)

flow = FlowControl(
    gate=HardLock(),
    surge_barrier=SurgeBarrier(threshold_multiple=3.0, action="bypass"),
    penstock=BurstPenstock(rate_per_sec=5.0, burst=10),
    reservoir=Reservoir(depth=8),
    spillway=ExportToArchive(archive_cls=AuditArchive),
)
watershed = Watershed(
    window=(start, end),
    currents=[upstream, downstream],
    edges=[Edge(from_name="upstream", to_name="downstream", flow=flow)],
)
```

**JSON form** — every primitive uses a `type` discriminator tag:
```json
{
  "flow": {
    "gate":         {"type": "hard"},
    "surge_barrier":{"threshold_multiple": 3.0, "action": "bypass"},
    "penstock":     {"type": "burst", "rate_per_sec": 5.0, "burst": 10},
    "reservoir":    {"depth": 8},
    "spillway":     {"type": "export_to_archive", "archive_cls": "audit:AuditArchive"}
  }
}
```

**When to reach for it**
- Lab default (no kwargs) — bare `Watershed.chain(currents=[...])` is `HardLock` + `Reservoir(depth=1)` + `DropOldest` + a default `SurgeBarrier(threshold_multiple=2.0, action="skip")`.  Good enough for most pipelines.
- Production needs (slow downstream behind a fast upstream) — add a `Penstock` to throttle and a deeper `Reservoir` + an `ExportToArchive` `Spillway` to audit what didn't get processed.
- Multi-source fusion where one feed can lag — `SurgeBarrier(action="bypass")` keeps the fjord ticking on the others.
- Green-wave coordination — pair a deeper `Reservoir` with `BackpressurePenstock` to smooth consumption rate against upstream burstiness.

**See also**
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) ·
[`docs/cli_and_configuration.md §9`](./cli_and_configuration.md) ·
[Library Reference](./library_reference.md)

---

## Telemetry

### Wave.log_meta

**Signature**
```python
def log_meta(self) -> str:
```

**What it does (pseudocode)**
1. Format `operation`, `chunk_index`, `rows_processed`, `processing_time_sec`, and `len(failed_sources)` into a single `key:"value", key:value, ...` line.
2. Return — used by `_route_wave_to_log()` so `Wave` records share the flat `meta` shape with instance-level log records.

**When to reach for it**
Rarely called directly — the routing adapter calls it on every `Wave` written to disk. Read it when you want to know what shows up under `record["meta"]` in `get_error()` output for chunk / refresh / export waves (vs. per-instance records, which use `LoggingMixin.log_meta`).

**Common kwargs**
- None — bound method on the immutable `Wave` model.

**Yields / returns**
`str` — one-line `Wave` summary.

**See also**
[Production Debugging with `get_error()`](./debugging.md) ·
[Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md)

---

## Observability layer (`LoggedIncorporator`)

### LoggedIncorporator — shared `enable_logging=` note

Every verb on `LoggedIncorporator` (`incorp`, `refresh`, `export`, `stream`, `fjord`)
accepts every kwarg its `Incorporator` counterpart accepts, plus one extra:
`enable_logging: bool = False`. When set to `True`, the call wires up a
per-class `QueueHandler`-backed logger that writes rotating JSON-line records
to `logs/<ClassName>_{api,error,debug}.log`. Disk I/O runs on a background
thread — the event loop never blocks on log writes. Logging is **opt-in per
call**, so the same class can run unobserved one moment and fully-traced the
next. Failures, fatal pipeline errors, and per-`Wave` throughput are all
routed through `_route_wave_to_log()` and queryable later via `get_error()`.

---

<a id="loggedincorporator-get_error"></a>
### LoggedIncorporator.get_error

**Signature**
```python
@classmethod
async def get_error(cls) -> List[Dict[str, Any]]:
```

**What it does (pseudocode)**
1. Resolve `logs/<ClassName>_error.log`; return `[]` if the file does not exist (safe to call before any error has been logged).
2. In a worker thread (`asyncio.to_thread`), walk the file line-by-line and parse each JSON line into a dict.
3. Silently skip malformed lines; treat `OSError` as "no errors yet" — never propagate disk-read failures.
4. Return the list of parsed records (level, msg, meta, wave dump, timestamp, optional exc_info).

**When to reach for it**
The post-run forensics verb. After an overnight stream daemon, call `await Class.get_error()` to walk every failure the pipeline saw — feed `.failed_sources` into a retry orchestrator, assert on logged failure shape in tests, or generate a Slack digest of what broke.

**Common kwargs**
- None — `get_error()` is parameter-free.

**Yields / returns**
`List[Dict[str, Any]]` — each dict has `level`, `msg`, `meta`, optional `wave` (full Pydantic dump), `time`, optional `exc_info`.

**See also**
[Production Debugging with `get_error()`](./debugging.md) ·
[Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md)

---

<a id="loggedincorporator-log_debug--log_info--log_error"></a>
### LoggedIncorporator.log_debug / log_info / log_error

**Signature**
```python
def log_debug(self, msg: str) -> None: ...
def log_info(self, msg: str) -> None: ...
def log_error(self, msg: str, exc_info: bool = False) -> None: ...
```

**What it does (pseudocode)**
1. Grab the class-scoped logger via `_get_logger()`; cheap `isEnabledFor` check noops when the level is off — free to sprinkle through unlogged code paths.
2. Build a flat `meta` string via `self.log_meta()` (class, `inc_code`, `inc_name`, origin URL/file).
3. Dispatch to `logger.<level>()` with `extra={"meta": ..., "is_api": False}`; `log_error` additionally honours `exc_info=True` for traceback attach inside `except` blocks.
4. The `QueueHandler` enqueues the record on a background thread; the caller returns immediately.

**When to reach for it**
The per-instance trace verbs — use `log_debug` for verbose noise you want grep-able later, `log_info` for "this happened to this instance" milestones, `log_error` (with `exc_info=True`) inside `except` blocks to capture the traceback alongside instance identity for later forensics.

**Common kwargs**
- `msg` — the human-readable message; `meta` is attached automatically.
- `exc_info` (`log_error` only) — `True` inside `except` to attach the active traceback.

**Yields / returns**
`None`. The record is enqueued for the background log thread.

**See also**
[Production Debugging with `get_error()`](./debugging.md)

---

<a id="loggedincorporator-log_api"></a>
### LoggedIncorporator.log_api

**Signature**
```python
def log_api(self, msg: str) -> None:
```

**What it does (pseudocode)**
1. Cheap level check on the class logger; build the `meta` string from `self.log_meta()`.
2. Emit an INFO record with `extra={"meta": ..., "is_api": True}`.
3. The `APIFilter` on `api.log` lets the record through; `StandardFilter` on `error.log` drops it — outbound HTTP traces accumulate cleanly in `logs/<ClassName>_api.log`, separated from instance lifecycle noise.

**When to reach for it**
The audit-trail verb for outbound HTTP. Use it to record "I called endpoint X with payload Y at time T" without polluting your generic info channel — handy when you want a clean record of every request a long-running daemon made overnight.

**Common kwargs**
- `msg` — the human-readable trace line; identity meta is attached automatically.

**Yields / returns**
`None`. The record is routed to `api.log` by the `is_api=True` filter flag.

**See also**
[Production Debugging with `get_error()`](./debugging.md)

---

<a id="loggedincorporator-log_meta"></a>
### LoggedIncorporator.log_meta

**Signature**
```python
def log_meta(self) -> str:
```

**What it does (pseudocode)**
1. Read `self.__class__.__name__` (fallback `"UnknownClass"`), plus `self.inc_code`, `self.inc_name`, `cls.inc_file`, `cls.inc_url`.
2. Format as a flat `key:"value", key:"value", ...` string.
3. Return — used by every instance log call as the `extra["meta"]` payload.

**When to reach for it**
You rarely call it directly — every `log_info` / `log_error` / `log_api` call invokes it for you. Override it on a subclass when you want extra identity fields in the meta string; keep the `key:"value"` shape so existing `get_error()` consumers still parse the records.

**Common kwargs**
- None — bound method on the instance.

**Yields / returns**
`str` — one-line identity summary.

**See also**
[Production Debugging with `get_error()`](./debugging.md)

---

<a id="loggedincorporator-log_cls_info--log_cls_error"></a>
### LoggedIncorporator.log_cls_info / log_cls_error

**Signature**
```python
@classmethod
def log_cls_info(cls, msg: str) -> None: ...
@classmethod
def log_cls_error(cls, msg: str, exc_info: bool = False) -> None: ...
```

**What it does (pseudocode)**
1. Look up the class logger via `_get_cls_logger()`; cheap level check noops when the level is off.
2. Build a class-only meta string (`class:"<Name>"`), since there is no `self` to inspect.
3. Dispatch to `logger.info()` / `logger.error()` with `extra={"meta": ..., "is_api": False}`; `log_cls_error` honours `exc_info=True` to ride the active traceback along.
4. Factory / `@classmethod` lifecycle events land in the same `api.log` / `error.log` files as instance-level events.

**When to reach for it**
The class-level counterpart to `log_info` / `log_error` — use these inside `@classmethod` factory paths where no `self` exists. They're how `LoggedIncorporator.stream()` brackets daemon runs with "Initiating ..." / "Stream process completed gracefully." entries.

**Common kwargs**
- `msg` — human-readable message.
- `exc_info` (`log_cls_error` only) — `True` to attach the active traceback.

**Yields / returns**
`None`. The record lands in `api.log` (info) or `error.log` (error).

**See also**
[Production Debugging with `get_error()`](./debugging.md)

---

### Shared kwargs glossary

- `inflow=` — sidecar `.py` exposing public symbols for `conv_dict` token resolution; in fjord, may also define `inflow(state)` for sequential dependent seeding (see [the `inflow(state)` contract](#fjord) under the fjord entry for call cadence, guard requirements, and return shape).
- `outflow=` — sidecar `.py` whose stem becomes the dynamic output class name; must define `outflow(state) -> list[dict]` (or `dict[ClassName, list[dict]]` for multi-output fjord).
- `inc_page=` — `AsyncPaginator` subclass (`PageNumberPaginator`, `CursorPaginator`, `OffsetPaginator`, `NextUrlPaginator`, `LinkHeaderPaginator`) that drives chunking-mode `stream()` or paginated `incorp()`.
- `format_type=` — `FormatType` enum forcing a writer when the file extension is ambiguous; otherwise auto-detected from extension.
- `enable_logging=` — on `LoggedIncorporator` only; wires the call into per-class rotating JSONL handlers (`logs/<ClassName>_{api,error,debug}.log`).
- `inc_code=` — field name on each record that becomes the primary key in `inc_dict`. Pass the field name (e.g. `"id"`); the framework reads each record's value at that key.

---

## Class-attribute reference

| Symbol | Owner | Kind | Purpose |
|---|---|---|---|
| `inc_dict` | `Incorporator` (ClassVar) | `WeakValueDictionary[Any, Incorporator]` | per-class O(1) registry — `inc_code → instance`. Auto-populated by `model_post_init()`. |
| `inc_url` / `inc_file` | `Incorporator` (ClassVar) | `Optional[str]` | origin tracking. `refresh()` falls back to these when called without explicit new sources. |
| `inc_code` / `inc_name` / `last_rcd` | instance | universal Pydantic fields | identity (auto-counter fallback) + display label + UTC construction timestamp. |
| `failed_sources` | `IncorporatorList` | `List[str]` | DLQ surface — every URL/file that hit a permanent failure. Read by retry orchestrators. |
| `Wave.{chunk_index, operation, rows_processed, failed_sources, processing_time_sec, timestamp}` | `Wave` (frozen Pydantic) | model fields | one record per pipeline tick. Yielded by `stream()` and `fjord()`. |
| `IncorporatorList.inc_dict` | property on the list | shared view of class registry | what `incorp()`'s return value exposes; mutations write through to `cls.inc_dict`. |

---

## Where to Go Next

| Goal | Read |
|---|---|
| See a verb run end-to-end against a live API | [Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md) |
| Drain 10M rows without OOM (chunking mode) | [Streaming & Pagination Deep Dive](./streaming_and_pagination.md) |
| Orchestrate multiple verbs on a windowed schedule | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) and [Appendix — NASCAR Tideweaver](./appendix/nascar_tideweaver.md) |
| Survive overnight runs with healthchecks + logs | [Deployment Guide](./deployment.md) |
| Generate the full pdoc HTML reference | [Library Reference](./library_reference.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/api_atlas.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
