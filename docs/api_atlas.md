# API Atlas

The map you open when you know which verb you want but forget its kwargs.
`library_reference.md` is the auto-generated pdoc HTML for exhaustive
parameter tables; the `examples/NN-*/README.md` tutorials are narrative
and runnable. This atlas sits between them: paste-ready signatures,
3-7 step pseudocode, and one-line "when to reach for it" narrative for
every public callable.

The same eight verbs — `incorp / test / architect / refresh / export / stream / fjord / display` — work on any source (JSON, XML, CSV, NDJSON, SQLite, Parquet, Avro, and more) without class declarations or validation schemas.  The same `Penstock / Wave / RejectEntry` primitives that govern a single `incorp()` call govern a multi-source `Tideweaver` window: you are not learning two tools.

> Every code block here is copy-paste runnable assuming
> `from incorporator import Incorporator, LoggedIncorporator` and
> `import asyncio` are already at the top of the file.

---

## Table of Contents

- [Discovery & ingestion](#discovery--ingestion)
  - [`test`](#test)
  - [`architect`](#architect)
  - [`incorp`](#incorp)
  - [`register_host_penstock`](#register_host_penstock)
- [Live updates](#live-updates)
  - [`refresh`](#refresh)
- [Persistence](#persistence)
  - [`export`](#export)
- [Daemons](#daemons)
  - [`stream`](#stream)
  - [`fjord`](#fjord)
  - [Build-time vs read-time: where coercion + joins belong](#build-time-vs-read-time-where-coercion--joins-belong)
- [REPL](#repl)
  - [`display`](#display)
- [Orchestration](#orchestration)
  - [Tideweaver orchestration surface](#tideweaver-orchestration-surface)
  - [`Tideweaver.summary` / `tune` / `TuningReport`](#tideweaversummary--tune--tuningreport)
  - [Scheduler-event enums — `SkipReason` / `WakeReason` / `GateMode`](#scheduler-event-enums--skipreason--wakereason--gatemode)
  - [Canal toolkit primitives](#canal-toolkit-primitives)
  - [`CustomCurrent`](#customcurrent)
- [Row filtering: pick the right primitive](#row-filtering-pick-the-right-primitive)
- [Telemetry](#telemetry)
  - [`Wave.log_meta`](#wavelog_meta)
- [Observability layer (`LoggedIncorporator` + `LoggedTideweaver`)](#observability-layer-loggedincorporator--loggedtideweaver)
  - [`LoggedIncorporator` — shared `enable_logging=` note](#loggedincorporator--shared-enable_logging-note)
  - [`get_error`](#loggedincorporator-get_error)
  - [`get_api`](#loggedincorporator-get_api)
  - [`get_rejects`](#loggedincorporator-get_rejects)
  - [`get_current`](#loggedincorporator-get_current)
  - [`log_debug` / `log_info` / `log_error`](#loggedincorporator-log_debug--log_info--log_error)
  - [`log_api`](#loggedincorporator-log_api)
  - [`log_meta`](#loggedincorporator-log_meta)
  - [`log_cls_info` / `log_cls_error`](#loggedincorporator-log_cls_info--log_cls_error)
  - [`LoggingMixin`](#loggingmixin)
  - [`setup_class_logger`](#setup_class_logger)
  - [`LoggedTideweaver`](#loggedtideweaver)
- [Schema utilities](#schema-utilities)
- [Shared kwargs glossary](#shared-kwargs-glossary)
- [DATA-SHAPE directives](#data-shape-directives)
- [Class-attribute reference](#class-attribute-reference)
- [FormatType](#formattype)
- [CompressionType](#compressiontype)
- [Exception hierarchy](#exception-hierarchy)
- [Optional-dependency introspection](#optional-dependency-introspection)
  - [`list_deps`](#list_deps---listdepinfo)
  - [`install_hint`](#install_hintdep_name-str---str)
  - [`Category` enum](#category-enum)
  - [`DepInfo` fields](#depinfo-fields)
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
) -> TIncorporator | "IncorporatorList[TIncorporator]" | list[Any]:
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
    sources: Mapping[str, str | Path | Mapping[str, Any]],
    *,
    output: Literal["report", "python", "json", "plan"] = "report",
    shared_kwargs: Mapping[str, Any] | None = None,
) -> str | OrchestrationPlan | None:
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
    inc_url: str | list[str] | None = None,
    inc_file: str | "os.PathLike[str]" | list[str | "os.PathLike[str]"] | None = None,
    inc_parent: TIncorporator | "IncorporatorList[TIncorporator]" | None = None,
    inc_child: str | None = None,
    inc_code: str | None = None,
    inc_name: str | None = None,
    excl_lst: list[str] | None = None,
    conv_dict: dict[str, Any] | None = None,
    name_chg: list[tuple[str, str]] | None = None,
    inc_page: AsyncPaginator | None = None,
    inflow: str | Path | None = None,
    **kwargs: Any,
) -> TIncorporator | "IncorporatorList[TIncorporator]":
```

**What it does (pseudocode)**
1. If `inflow=` is set, load the sidecar and resolve string-form tokens in `conv_dict` / `inc_page`.
2. If `inc_parent=` is given, hand off to the Parent-Child router (HATEOAS drill via `inc_child` JSONPath).
3. Normalise `inc_url` / `inc_file` into a source list; remember the seed kwargs on the class so `refresh()` can replay them.
4. Fan out the source list concurrently through the network engine (sliding window of 50, rate-limited, exponential-backoff retries).
5. Hand the parsed payload to the schema factory; build a dynamic Pydantic model and instantiate one record per row.
6. Register every instance into `cls.inc_dict`; always return an `IncorporatorList` (carrying `.failed_sources`) — even a single-record result is wrapped in a length-1 list, never a bare instance.

**When to reach for it**
This is the cold-start verb — the one you call when a new endpoint hits your radar and you want a working object graph in three lines. Backtest data prep, one-shot CSV-to-Pydantic conversions, the seed call before any daemon takes over. **Memory note (v1.2.1+):** `incorp()` validates each chunk as a whole via `TypeAdapter(list[Cls]).validate_python(rows)` — peak memory scales with the source row count, not streaming row-by-row. For large pulls reach for `stream()` (chunking mode) instead so each chunk releases before the next is fetched.

**Common kwargs**
- `inc_url` / `inc_file` — single string or list; list triggers concurrent fan-out.
- `inc_code` — field name to use as the primary key in `inc_dict`.
- `inc_parent` + `inc_child` — drill a parent list's URLs into child fetches (HATEOAS).
- `conv_dict` — `{field_name: converter}` pre-validation coercion (`inc`, `calc`, `calc_all`, `pluck`, `link_to`, `link_to_list`, `split_and_get`).  **Null-handling contract:** every converter short-circuits on garbage input (`None`, `""`, `"N/A"`, `"null"`, `"unknown"`, `"nan"`, `"undefined"`) before invoking the user callable — defensive null guards in lambdas are unnecessary.  Idioms: `calc(str.lower, "title", default="", target_type=str)` · `calc(str.upper, "code", default="", target_type=str)` · `calc(str.strip, "name", default="", target_type=str)` · `calc(len, "body", default=0, target_type=int)` · `calc("Alive".__eq__, "status", default=False)`.  **`inc(TYPE)`** coerces the field's existing value to a standard Python type (`int` / `float` / `bool` / `str` / `datetime` / a Pydantic type / `new` for pass-through) — the argument is always a TYPE, never a function; passing a callable (e.g. `inc(str.upper)`) is a build-time misuse: `inc()` coerces to a TYPE (int/float/bool/str/datetime or a Pydantic type) and will pass values through UNCHANGED here, and the `incorporator.schema.converters` logger emits a WARNING. **`calc(fn, *keys)`** builds the field by applying a FUNCTION to one or more source values — this is correct even when the output key equals the source key: an in-place transform like `calc(str.upper, "Make")` writing back to `"Make"` is a valid, common idiom, not an exception case. **`pluck(key, chain=fn)`** lifts a nested value out via a dot-notation path (e.g. `"data.attributes.price"`), with an optional `chain=` callable applied to the extracted value before assignment.  **v1.2.3 purity default flip:** `calc()` and `calc_all()` now default `pure=True`. For `calc()`/`CalcOp` the wrapped callable is memoised via `functools.lru_cache(maxsize=10_000)` at Op construction, so identical input tuples are computed once per process — pass `pure=False` explicitly for side-effecting callables (`datetime.now()`, `uuid.uuid4()`, logging, DB writes, mutable counters) so the side effects fire on every row rather than only on cache miss. `calc_all()` accepts the same `pure` flag for API symmetry but does **not** wrap the callable at construction: it runs as a single whole-column pass, so there is no per-input cache to populate.
- `params` — dict merged **onto** the URL's existing query string, not substituted for it. Prior to 1.3.5, request-level `params` replaced the URL's query outright, silently dropping any embedded or paginator-carried query keys (e.g. a paginator's cursor token) — the practical symptom was a page-1 refetch loop on paginated endpoints. `params` now merges via `httpx.URL.copy_merge_params()`; on key collision, your `params` value wins, matching the existing base/request-params precedence.
- `payload_list` — list of per-request POST bodies, one per resolved source (`inc_url` entry or `each()`-expanded slot). Length must equal the resolved source list exactly — no silent truncation, no auto-expansion. A length mismatch now raises `ValueError` naming both counts and the three valid idioms: (1) pass `inc_url` as a list of N URLs, one per `payload_list` entry; (2) use the declarative `each()` token via `inc_parent` routing, which auto-expands `inc_url` to match; or (3) omit `inc_url` (`source=None`) for payload-only mode, which auto-matches placeholder length to `payload_list`. **Payload-only passthrough (idiom 3):** each `payload_list[i]` IS the source payload — it is parsed/`conv_dict`-transformed/validated exactly like a fetched body, but with no network call, no HTTP client, no SSRF check, and no rate limiting; `http_method` / `payload_type` are read but inert. **Declarative guard:** the `each()` router branch (reached via `inc_parent` + declarative POST tokens) requires a real target URL — `each()` with no `inc_url` and no parent-extracted URLs raises the same `ValueError("Missing Target URL...")` the bulk POST branch already raises. This guard is scoped to the declarative seam only; direct `incorp(source=None, payload_list=[...])` remains legal.
- `inc_page` — `AsyncPaginator` subclass for paginated endpoints.
- `rec_path` — dot-notation drill into a wrapper response; supports integer indices for list segments (e.g. `"results"` or `"dates.0.games"`).
- **Dot-notation coverage (Bundle G).** All six path-string surfaces accept `"a.b.0.c"` form (dict keys and integer list indices): `rec_path`, `pluck()`, `calc()` input keys, `calc_all()` input keys, `inc_code=`, `inc_name=`, and `inc_child=`. The authoritative implementation is `DataPath` (`incorporator/schema/path.py`) — behaviour is identical across all surfaces.
- `concurrency_limit`, `requests_per_second`, `timeout`, `headers` — network knobs.

**Yields / returns**
Always returns an `IncorporatorList[TIncorporator]` — a one-record source yields a length-1 list, never a bare instance (`is_single` removed). `.failed_sources: list[str]` is the legacy flat reject-list view.  For structured access — exception type, `is_url_traffic_error` flag, `Retry-After` hints, wave index — read `.rejects: list[RejectEntry]` (fields: `source`, `error_kind`, `is_url_traffic_error`, `message`, `retry_after`, `wave_index`).

**Parent-child short-circuit (v1.3.3 correctness).** When `inc_parent` is supplied and the parent snapshot is empty, `incorp()` returns an empty `IncorporatorList` without making a network request. This prevents malformed ``.../{}`` requests on endpoints that interpolate parent IDs into the URL. Existing code that checks `len(result) == 0` or `result.failed_sources` is unaffected.

#### Build rows from memory — the payload-only passthrough

**Reach for `incorp(payload_list=[...])` with no `inc_url`/`inc_file` whenever the rows you want already sit in memory** — a nested array inside an already-fetched row (e.g. a roster row's `athletes` list), a reshape between two calls, or fixture data. One dict entry = one row; every entry flows through the FULL build pipeline (`conv_dict` converters, `excl_lst`, `name_chg`, PK-binding, schema inference) with zero network, no HTTP client, no rate limiting:

```python
payload = [
    {**athlete.model_dump(), "team_name": team.inc_name}   # stamp parent context
    for team in rosters for athlete in team.athletes
    if athlete.active                                       # row filter = plain comprehension
]
players = await Player.incorp(
    payload_list=payload,
    inc_code="id", inc_name="fullName",
    conv_dict={
        "salary": pluck("contract.salary"),
        "salary_per_year": calc(salary_per_year, "contract.salary", "experience.years"),
    },
)
```

Prefer this passthrough over a `calc()` helper that walks a nested array and emits a list of per-element dicts inside `conv_dict`.

**Ordering:** the build pipeline runs `excl_lst` before `conv_dict` (`Ex -> conv_dict -> Nm -> Pk`) — a field cannot be both read by a converter and excluded in the same call. To consume-and-rename, use an in-place `calc` (output key == source key) followed by `name_chg`.

**See also**
[Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md) ·
[Tutorial 5 — Parent → Child Drilling](../examples/05-parent-child-drilling/README.md) ·
[Library Reference](./library_reference.md)

---

### register_host_penstock

**Signature**
```python
def register_host_penstock(
    host: str,
    penstock: Penstock | Callable[[], Penstock] | None = None,
    *,
    rate_per_sec: float | None = None,
    burst: int | None = None,
) -> None:
```

**What it does (pseudocode)**
1. Registers a per-host `Penstock` keyed by lowercase hostname.  Accepts a `Penstock` instance, a zero-arg factory callable (legacy back-compat), or the `rate_per_sec=`/`burst=` shorthand — a bare `rate_per_sec` builds a `SustainedPenstock`; adding `burst` builds a `BurstPenstock` instead.
2. Each `resolve_penstock()` invocation builds a fresh `BoundPenstock` (sharing the registered config, with its own `FlowState` + `asyncio.Lock`) so fan-out legs run independently.
3. Re-registering the same host replaces the previous penstock.

**When to reach for it**
The framework ships with **no implicit per-host throttling**.  Use this to attach a penstock for an in-house API or any public host that imposes a documented rate ceiling.  The alternative — `incorp(..., requests_per_second=X)` per call — is fine for one-shot scripts; the registry is the right tool when you have many call sites against the same host and want one source of truth.

**Worked example**
```python
from incorporator import register_host_penstock

# Conservative rate for CoinGecko's anon tier (5-15 req/min documented).
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# Bursty in-house API: 50 req/s sustained, 200-burst tolerance.
register_host_penstock("api.internal.acme.com", rate_per_sec=50.0, burst=200)
```

**Common kwargs**
- `host` — lowercase hostname; `urllib.parse` extracts this from URLs at resolve time.
- `penstock` — a `Penstock` instance (preferred for non-Sustained/Burst policies) or a zero-arg callable returning one.  The `Penstock` config is frozen Pydantic; the per-call binding owns the mutable state + lock.  Mutually exclusive with `rate_per_sec`/`burst`.
- `rate_per_sec` / `burst` — keyword-only shorthand that builds a `SustainedPenstock` (bare `rate_per_sec`) or `BurstPenstock` (`rate_per_sec` + `burst`) inline, skipping the explicit import + instantiation.

**Yields / returns**
`None`.  Side-effect-only: mutates the module-level `_HOST_PENSTOCKS` dict.

**Related**
- `incorporator.io.penstock.resolve_penstock(source, requests_per_second=, burst=)` — the resolver every `incorp()` call routes through.  Five-tier precedence: env-var bypass > `rps<=0` > caller rps > registered host > `DEFAULT_RPS=15` fallback.
- `incorporator.io.penstock.known_host_rates()` — diagnostic view of `host → float` rates currently registered.
- `incorporator.io.penstock.Penstock` — the unified rate-control primitive shared by both the HTTP host registry and the Tideweaver edge layer.  Subclasses: `NullPenstock`, `SustainedPenstock`, `BurstPenstock`, `WindowPenstock`, `SignalPenstock` (and `BackpressurePenstock` at the edge layer only).

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
    instance: str | Path | TIncorporator | list[TIncorporator] | None = None,
    new_url: str | list[str] | None = None,
    new_file: str | list[str] | None = None,
    inc_child: str | None = None,
    inc_code: str | None = None,
    inc_name: str | None = None,
    excl_lst: list[str] | None = None,
    conv_dict: dict[str, Any] | None = None,
    name_chg: list[tuple[str, str]] | None = None,
    inc_page: AsyncPaginator | None = None,
    inflow: str | Path | None = None,
    **kwargs: Any,
) -> TIncorporator | "IncorporatorList[TIncorporator]":
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
Same as `incorp()` — always an `IncorporatorList[TIncorporator]`, even for a single-record refresh. Existing references are mutated in-place.

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
    instance: str | Path | TIncorporator | list[TIncorporator],
    file_path: str | Path | None = None,
    format_type: FormatType | None = None,
    compression: str | None = None,
    sql_table: str | None = None,
    if_exists: str = "replace",
    outflow: str | Path | None = None,
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
    incorp_params: dict[str, Any],
    refresh_params: dict[str, Any] | None = _UNSET,
    export_params: dict[str, Any] | None = None,
    poll_interval: float | None = None,
    stateful_polling: bool = False,
    refresh_interval: float | None = None,
    export_interval: float | None = None,
    inflow: str | Path | None = None,
    outflow: str | Path | None = None,
    # Adaptive chunk sizing (v1.2.1+):
    adapt_chunk_size: bool = False,
    chunk_size_min: int = 100,
    chunk_size_max: int = 100_000,
    target_min_sec: float = 0.030,
    target_max_sec: float = 0.100,
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
- `adapt_chunk_size=` (v1.2.1+) — `True` to let the engine resize `paginator.chunk_size` between chunks via AIMD.  Companions: `chunk_size_min` / `chunk_size_max` clamp the range, `target_min_sec` / `target_max_sec` define the latency window the engine tries to settle inside.  See [Streaming & Pagination — Adaptive chunk sizing](./streaming_and_pagination.md#adaptive-chunk-sizing-v121).

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
    stream_params: list[dict[str, Any]],
    outflow: str | Path,
    export_params: dict[str, Any],
    refresh_interval: float | None = None,
    export_interval: float | None = None,
    inflow: str | Path | None = None,
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

> The `nht` lookup above is a read-time `inc_dict.get(...)` — one join that stayed read-time in this worked example, alongside `inv.Vehicle.VIN` which resolved at build time via `inflow()`.  See "Build-time vs read-time: where coercion + joins belong" below for the general rule and why both patterns coexist honestly rather than one replacing the other everywhere.

### Build-time vs read-time: where coercion + joins belong

**The rule:** coerce and join at **build time**, in the `conv_dict` passed to `incorp()` — via `inc()`/`calc()` for coercion and `link_to()`/`link_to_list()` for joins — so that `outflow(state)` (or any code reading the resulting instances) touches **plain attributes**. The framework's `is_garbage_value` null contract already does the defensive work once, at construction; a second `getattr(x, "field", default) or fallback` at read time is pure duplication of a guarantee the framework already gave you.

```python
# Read-time (avoid): defensive guards on every read, every export wave
def outflow(state):
    rows = []
    for coin in state["CoinGecko"]:
        symbol = getattr(coin, "symbol", "").upper()
        pair = state["BinancePair"].inc_dict.get(f"{symbol}USDT")
        price = float(getattr(pair, "price", 0) or 0) if pair else 0
        ...

# Build-time (prefer): join + coerce once, at each source's own incorp()
def inflow(state):
    return {
        "CoinGecko": {
            "conv_dict": {
                "symbol": link_to(state["BinancePair"], extractor=to_binance_symbol),
            }
        }
    }
# BinancePair's own conv_dict: {"price": inc(float, default=0.0)}

def outflow(state):
    rows = []
    for coin in state["CoinGecko"]:
        pair = coin.binance_pair          # plain attribute, None if unmatched
        if pair is None:
            continue
        price = pair.price                # already a float
        ...
```

**Why build-time joins survive refresh.** `link_to(dataset, ...)` builds its own private `WeakValueDictionary` registry by walking `dataset` once, at the moment the `conv_dict` entry is constructed — it does not re-read the target class's `inc_dict` on every access. `Incorporator.refresh()` mutates existing instances' *fields* in place rather than replacing the objects, so a resolved reference from an earlier build-time join keeps seeing current data across refresh waves for free, with zero re-lookup cost. This is *not* the same guarantee as reading `Cls.inc_dict` directly inside `outflow()` — that dict is a `ClassVar[WeakValueDictionary]` and can be momentarily empty between a garbage-collection pass and the next tick; a `link_to()`-built registry sidesteps that race entirely because it isn't `inc_dict`.

**`link_to()`'s conv_dict key must match the SOURCE field it reads.** The dispatcher feeds every non-`calc` conv_dict `Op` with `d.get(key)` — the same key it writes back — so `link_to()` (like `inc()`) only works when the conv_dict key equals the raw source field name (e.g. `"track_id": link_to(state["Track"])` reads and overwrites `track_id`). If you want the resolved object under a *different* name than the raw source field, pass `conv_dict={"symbol": link_to(...)}` together with `name_chg=[("symbol", "new_name")]` in the same `incorp()`/`incorp_params` call — the ETL pipeline runs `conv_dict` (pass 2) before `name_chg` (pass 3), so the rename applies to the already-resolved object, not the raw value. A bare new conv_dict key with no matching source field (e.g. `conv_dict={"binance_pair": link_to(...)}` when the raw row has no `binance_pair` field) silently resolves to `None` on every row — the dispatcher feeds it `d.get("binance_pair")`, which is garbage, so the join extractor never fires.

**Two joins that genuinely can't move to build time — the honest boundary.** Not every read-time lookup is a shortcut; some are the correct design:

1. **List-of-dicts fan-out.** When the FK lives inside a nested list of dicts (e.g. a roster `[{"series_id": 1, "driver_id": 3989}, ...]`) rather than a flat scalar field, `link_to()` can't fan out — it resolves exactly one scalar per conv_dict entry. Keep this read-time.
2. **Runtime-chosen target dataset.** When *which* dataset to join against is itself decided per-row by another field's value (e.g. route to `CupStanding` vs `CupOwnerStanding` depending on a driver-status flag), `link_to()` binds to one dataset per conv_dict entry and can't branch. This is dynamic dispatch, not a static FK — keep it read-time.

Both patterns are worked through in [Tutorial 9 — NASCAR Fantasy Fjord](../examples/09-nascar-fantasy-fjord/README.md)'s `FantasyTeam` view, alongside the build-time join in the same tutorial's `Race` source and in [Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md)'s `CoinGecko` ↔ `BinancePair` spread.

**No read-time registry accessor is being added.** A tempting "fix" for read-time joins is a convenience method like `inc_get(key)` or `inc_lookup(key)` that wraps `Cls.inc_dict.get(key)` with friendlier syntax. This is deliberately **not** a framework primitive: `inc_dict` is a `ClassVar[WeakValueDictionary]`, and a fjord `outflow(state)` wave runs against a `state` snapshot taken under a lock — reading the *class-level* registry instead of the snapshot re-introduces exactly the GC-race and stale-read risk the snapshot contract exists to avoid. The two honest boundaries above (list-fan-out, runtime dataset choice) are genuinely better served by keeping the lookup inline in `outflow(state)`, reading from the `state` snapshot's own `IncorporatorList.inc_dict` (a `@property` proxy onto the class-level registry, safe because the whole snapshot was captured together) — not by adding a new primitive that would encourage bypassing `state` entirely.

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
        tick_factory: TickFactory | None = None,
        pass_interval: float | None = None,
        backlog_backoff_factor: float = 1.0,        # v1.2.1+, opt-in 2.0
    ) -> None: ...
    async def run(self) -> AsyncIterator[Tide]: ...
    def summary(
        self,
        *,
        tides: list[Tide] | None = None,
        waves: list[Wave] | None = None,
    ) -> "TuningReport": ...                         # v1.2.1+
    rejects: list[RejectEntry]                       # canal-layer only (verb-layer rejects live on IncorporatorList.rejects)

class Watershed(BaseModel):
    name: str | None = None                          # v1.3.3: drives LoggedTideweaver logger_name default
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
`"hard"`).  `flow=` is the full-dict form: a `FlowControl` composing
gate + surge_barrier + penstock + reservoir + spillway + observer.  They are mutually
exclusive — pass one, neither (defaults to `gate_mode="hard"`), but not both.
`Edge(gate_mode=..., flow=...)` follows the same mutex rule for custom
explicit-edge graphs.  See [Canal toolkit primitives](#canal-toolkit-primitives)
below for the full per-edge FlowControl surface.

**What it does (pseudocode)**
1. Construct a `Watershed` via one of the four shape constructors (`chain` / `diamond` / `fanout` / `parallel`) — or the bare `Watershed(...)` for custom mixed-mode edges.
2. The validator folds `Current.depends_on` declarations into `Edge`s, checks unique names, validates the time window, runs a toposort to reject cycles.
3. Pass the `Watershed` to `Tideweaver(watershed)`; the scheduler computes `pass_interval` (default `min(interval)/2`, clamped `[0.05, 1.0]`).
4. `async for tide in Tideweaver(...).run()` — on every scheduler pass, walk the topological order; for each `Current`, gate on interval + upstream wave freshness, then fire the per-tick body.
5. Verb-typed `Current` subclasses dispatch differently: `Stream` runs chunking `cls.stream(...)` and parks a strong-ref snapshot on `_tideweaver_snapshot`; when `Stream.parent_current` is set, it instead reads the parent's snapshot and calls `cls.incorp(inc_parent=snapshot, ...)` directly (parent-child drill mode — see [Row filtering: pick the right primitive](#row-filtering-pick-the-right-primitive) for how to scope the parent's rows at the source); `Fjord` is a per-tick flush (`outflow(state)` → build → export); `Export` runs `cls.export(...)`.
6. When the window closes, the scheduler drains in-flight ticks (`drain_timeout` seconds), then exits.

**When to reach for it**
The windowed orchestration verb — when one source's `stream()` isn't enough, when N sources need independent cadences, when downstream work must gate on upstream freshness. Multi-exchange arb scanning across a market-open window, race-day telemetry fusion (laps + pits + flags → driver state), any "run these feeds together for the next four hours" workload.

**Common kwargs**
- `window=(start, end)` — inclusive start, exclusive end; the run exits at `end`.
- `currents=[...]` — list of `Stream` / `Fjord` / `Export` (or bare `Current` for tests).
- `edges=[...]` — explicit edges; each `Edge(from_name=..., to_name=..., gate_mode="hard"/"soft"/"weir")` shorthand or `flow=FlowControl(...)` full-dict form.
- `inflow=` / `outflow=` — graph-level sidecar defaults; per-current values win.
- `gate_mode` (shape constructors) — `"hard"` (default), `"soft"`, or `"weir"`.  Accepts both plain strings and the `GateMode` enum (`from incorporator.tideweaver import GateMode; GateMode.HARD`); both forms produce identical `FlowControl` because `GateMode` is a `str`-subclass.  Mutually exclusive with `flow=`.
- `flow` (shape constructors) — full `FlowControl(...)` shared across every edge produced by the shape. Mutually exclusive with `gate_mode=`.
- `drain_timeout` — seconds the scheduler waits for in-flight ticks at window close.
- `pass_interval` (`Tideweaver`) — override the auto-derived scheduler tick.
- `backlog_backoff_factor` (`Tideweaver`, v1.2.1+) — multiplicatively extend the next-pass wait when the scheduler is consistently saturated.  Default `1.0` is disabled; set to `2.0` (or larger) to opt in.  See [Post-run tuning](#tideweaversummary--tune--tuningreport) for the diagnostic side.

**Yields / returns**
`Tideweaver.run()` yields one `Tide` per scheduler pass, carrying `tide_number`, `fired`, `skipped: list[(name, reason)]`, `duration_sec`, plus the v1.2.1 outcome-record fields: `wake_reason`, `heap_depth`, `current_outcomes: list[CurrentOutcome]`, `in_flight_count_at_start`, `canal_rejects_added`, `next_due_in_sec`.

**See also**
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) ·
[Appendix — NASCAR Tideweaver](../examples/appendix/nascar-tideweaver/README.md) ·
[Library Reference](./library_reference.md)

---

### Tideweaver.summary / tune / TuningReport

**Signatures** *(v1.2.1+, extended in v1.3.3)*
```python
# Functional form — module-level callable.
def tune(
    *,
    rejects: list[RejectEntry] | None = None,
    tides: list[Tide] | None = None,
    waves: list[Wave] | None = None,
    pass_interval: float | None = None,
    timeout: float | None = None,               # v1.3.3: triggers _tune_http_timeout rule
) -> TuningReport: ...

# Instance-method convenience — same return.
class Tideweaver:
    def summary(
        self,
        *,
        tides: list[Tide] | None = None,
        waves: list[Wave] | None = None,
    ) -> TuningReport: ...

class TuningReport(BaseModel):                 # frozen
    hints: list[TuningHint]
    summary: dict[str, Any]
    analyzed_at: datetime
    def render(self) -> str: ...               # human-readable, severity-sorted

class TuningHint(BaseModel):
    severity: Literal["high", "med", "low", "info"]
    knob: str                                  # which Tideweaver knob to move
    scope: dict[str, str]                      # current name / edge / host etc.
    current_value: Any
    recommended_value: Any
    signal: str                                # which metric triggered the hint
    rationale: str                             # one-sentence explanation
    sample_size: int
```

All call paths are **keyword-only**.  All inputs default to `None` — pass whatever subset of `rejects` / `tides` / `waves` you have on hand; the heuristics scale down gracefully.

**Import path** *(load-bearing — not top-level)*
```python
from incorporator.tideweaver import tune, TuningReport, TuningHint
```

**What it does (pseudocode)**
1. Aggregate the supplied records by current, edge, and host.
2. Run rule functions across the supplied data — each targets one knob:
   - `chunk_size` — p50 and p99 of `wave.processing_time_sec` minus `http_fetch_time_sec` (parse-only time, v1.3.3); recommends chunk resize to settle inside `[target_min_sec, target_max_sec]`.
   - `penstock_rate` — per-edge and per-host `PenstockLimited` reject frequency; augments rationale with per-host byte/sec computed from `wave.bytes_downloaded` / `wave.http_fetch_time_sec` (v1.3.3).
   - `surge_threshold` — `SurgeHalted` / `"skip"` fraction against pass count; recommends raising `threshold_multiple` or switching action.
   - `pass_interval` — `wake_reason=="pass_interval"` saturation fraction and heap-empty fraction; recommends pass_interval adjustment.
   - `retry_policy` — `HTTPStatusError`, `PenstockLimited`, and `GateBlocked` reject shapes; recommends retry budget or cooldown changes.
   - `compound_retry_budget` — checks whether the worst-case retry budget exceeds `pass_interval` and recommends lengthening the interval.
   - `parent_child` — `parent_snapshot_size == 0` in waves or child tides firing when no parent fired; recommends investigating `parent_current` configuration.
   - `http_timeout` (v1.3.3) — when `timeout=` is supplied, compares observed p99 `http_fetch_time_sec` against the configured timeout; recommends adjustment when the gap is too narrow (< 85% headroom) or too large (> 3× p99).
3. Emit a `TuningHint` per recommended adjustment with severity, current value, recommended value, sample size, and rationale.
4. Return the structured `TuningReport`; `.render()` formats severity-sorted hint blocks for console review.

**When to reach for it**
The post-window feedback loop.  After a Tideweaver run, feed the accumulated `tw.rejects` (canal + verb layer) and the per-pass `Tide` records back in — the report tells you what knob to move next window, with the signal that drove the recommendation.  Pair with `LoggedTideweaver.get_tides()` / `get_rejects()` for cross-process replay.

**Common kwargs**
- `rejects` — the `Tideweaver.rejects` list at run end.  Drives Penstock-rate and SurgeBarrier recommendations.
- `tides` — the list collected from `async for tide in tw.run()`.  Drives `pass_interval` and `chunk_size` recommendations.
- `waves` — optional per-source `Wave` records; enables row-throughput hints and feeds the `penstock_rate` byte-rate calculation and the `http_timeout` rule.
- `pass_interval` — the value used at runtime; lets the analyzer compare against the recommendation.
- `timeout` (v1.3.3) — the `httpx` timeout in seconds used at runtime; triggers the `_tune_http_timeout` rule. Supply when you want evidence-based timeout recommendations from observed p99 latency.

**Yields / returns**
`TuningReport` — frozen Pydantic model; iterate `.hints` for programmatic use, `print(report.render())` for human review.

**See also**
[Tutorial 11 — Post-run tuning](../examples/11-tideweaver/README.md#post-run-tuning) ·
[Production Debugging — Orchestration debugging](./debugging.md#orchestration-debugging--loggedtideweaver--architecttune)

---

### Scheduler-event enums — `SkipReason` / `WakeReason` / `GateMode`

**Import**
```python
from incorporator.tideweaver import SkipReason, WakeReason, GateMode
```

All three are `str`-subclass enums — equality against plain string literals keeps working, and Pydantic v2 serialises the value (not the name), so wire format is unchanged.

| Enum | Members | Where it surfaces |
|---|---|---|
| `SkipReason` | `STILL_RUNNING`, `NOT_DUE`, `PHASE_OFFSET`, `AWAITING_UPSTREAM`, `SKIP_AHEAD`, `SURGE_HALTED`, `PENSTOCK_LIMITED` | `tide.skipped: list[(name, reason)]` and `RejectEntry.error_kind` for canal-layer skips |
| `WakeReason` | `STARTUP`, `TIMER`, `WAKE_EVENT`, `PASS_INTERVAL`, `SHUTDOWN` | `tide.wake_reason` |
| `GateMode` | `HARD`, `SOFT`, `WEIR` | Shape constructors (`Watershed.chain` / `diamond` / `fanout`) and `Edge(gate_mode=...)`; accepts either the enum or a plain string |

The source-of-truth module is `incorporator/tideweaver/reasons.py` (`SkipReason`, `WakeReason`) and `flow.py` (`GateMode`).

**Gate hierarchy note**: `HardLock`, `SoftPass`, and `Weir` are thin shells over `Gate` — they inherit a single `gate_reason(ctx)` body and override three ClassVar check flags (`_check_in_flight`, `_check_freshness`, `_check_consumed`).  Authors of custom `Gate` subclasses can do the same, or override `gate_reason()` directly.

---

### Canal toolkit primitives

Per-edge flow control.  Every `Edge` carries a `FlowControl` composing
six orthogonal primitives — each is a Pydantic strategy hierarchy and
serialises into `watershed.json` via discriminated unions.

**Signatures**
```python
class FlowControl(BaseModel):
    gate: Gate                                    # default HardLock()
    surge_barrier: SurgeBarrier | None = None
    penstock: Penstock | None = None
    reservoir: Reservoir                          # default Reservoir(depth=1)
    spillway: Spillway                            # default DropOldest()
    observer: FlowObserver                        # default NullObserver()


# Gate — pass / hold decision per upstream
# All three subclasses inherit gate_reason() from Gate and override ClassVar
# check flags (_check_in_flight, _check_freshness, _check_consumed: bool).
class HardLock(Gate): ...    # all checks True (inherits base defaults)
class SoftPass(Gate): ...    # all checks False — always returns None
class Weir(Gate): ...        # _check_in_flight=False; freshness + consumed checks True

# SurgeBarrier — conditional override when upstream runs long
class SurgeBarrier(BaseModel):
    threshold_multiple: float = 2.0
    action: Literal["skip", "halt", "bypass"] = "skip"

# Penstock — edge-level rate limit (same primitive as HTTP `register_host_penstock`)
class SustainedPenstock(Penstock):    # rate_per_sec: float
class BurstPenstock(Penstock):        # rate_per_sec: float, burst: int
class WindowPenstock(Penstock):       # window_sec: float, cap: int
class BackpressurePenstock(Penstock): # min_rate < max_rate, scales with reservoir
class SignalPenstock(Penstock):       # rate_fn(state, now) -> float

# Reservoir — per-edge FIFO buffer of recent waves
class Reservoir(BaseModel):
    depth: int = 1   # 1..1024

# Spillway — overflow handler when reservoir is full
class DropOldest(Spillway): ...                       # silent default
class RaiseOverflow(Spillway): ...                    # WARNING log per displacement, routed to session error.log (never raises despite the name)
class ExportToArchive(Spillway):                      # strong-ref backlog list
    archive_cls: Type[Incorporator]

# FlowObserver — declarative per-edge telemetry (synchronous, cheap)
class NullObserver(FlowObserver): ...                   # no-op default
class LoggingObserver(FlowObserver):                    # per-event Python logging
    fire_level: Literal["debug","info","warning"]      = "debug"
    skip_level: Literal["debug","info","warning"]      = "debug"
    spillway_level: Literal["debug","info","warning"]  = "warning"
    reservoir_level_level: Literal[...]                = "debug"
    reservoir_threshold: float = 0.0                  # 0.0..1.0; only emit when used/cap >= threshold
class SignalObserver(FlowObserver):                     # forward to user callable
    callback: Callable[[str, tuple[str, str], dict], None]
```

**What each does (pseudocode)**
- **`Gate`** — `HardLock` blocks until upstream has a wave newer than the dependent's last consumption; `SoftPass` ignores upstream entirely (sequence-only); `Weir` gates on freshness without triggering surge logic — fire-on-own-cadence once upstream emitted at least one wave.
- **`SurgeBarrier`** — when an upstream's currently-running tick exceeds `threshold_multiple × upstream.interval`, fires `action`: `"skip"` (skip this dependent pass), `"halt"` (skip until upstream finishes), `"bypass"` (fire ignoring this edge's gate AND penstock).
- **`Penstock`** — per-edge rate-limit strategy.  `SustainedPenstock` is a flat rate (1/rate_per_sec min gap); `BurstPenstock` token bucket with burst capacity; `WindowPenstock` sliding-window cap; `BackpressurePenstock` interpolates `max_rate → min_rate` as the reservoir fills; `SignalPenstock` calls a user callable for the live rate.  The same `Penstock` class hierarchy serves the HTTP host registry via [`register_host_penstock`](#register_host_penstock).
- **`Reservoir`** — buffers the last N wave-snapshots on each edge.  Default `depth=1` keeps the most recent.  Read by `BackpressurePenstock` for fullness; surfaced to user code via `edge_state.waves`.
- **`Spillway`** — fires when a wave is displaced from a full reservoir.  `DropOldest` silently evicts; `ExportToArchive` extends `archive_cls._spillway_backlog` (strong-ref) with the displaced instances.  `RaiseOverflow` logs a WARNING — routed into the active session's `error.log` (retrievable via `LoggedTideweaver.get_scheduler_events`) when running under a `LoggedTideweaver` session, falling back to the bare module logger otherwise. Retrieved records carry `event_type="spillway_overflow"`, the displaced `edge` pair, and a `detail` string.
- **`FlowObserver`** — synchronous lifecycle hooks called by the scheduler on every per-edge event.  Four hooks: `on_fire` (dependent tick fired), `on_skip(reason)` (gate/penstock/surge blocked), `on_spillway(displaced_wave, overflow_count)`, `on_reservoir_level(used, capacity)`.  Ships with `NullObserver` (no-op default), `LoggingObserver` (configurable Python-`logging` emission per event), and `SignalObserver` (forwards to a user callable for metric pipelines like statsd / Prometheus).  Hooks must not `await` — queue slow work off-thread.

**Worked example**
```python
from incorporator.tideweaver import (
    Edge, FlowControl, Watershed,
    HardLock, SurgeBarrier, BurstPenstock, Reservoir, ExportToArchive,
)

from incorporator.tideweaver import LoggingObserver

flow = FlowControl(
    gate=HardLock(),
    surge_barrier=SurgeBarrier(threshold_multiple=3.0, action="bypass"),
    penstock=BurstPenstock(rate_per_sec=5.0, burst=10),
    reservoir=Reservoir(depth=8),
    spillway=ExportToArchive(archive_cls=AuditArchive),
    observer=LoggingObserver(fire_level="info", spillway_level="warning"),
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
    "spillway":     {"type": "export_to_archive", "archive_cls": "audit:AuditArchive"},
    "observer":     {"type": "logging", "fire_level": "info", "spillway_level": "warning"}
  }
}
```

**Edge asymmetry — bare Edge() vs Edge(gate_mode="hard")**

These two look equivalent but differ on `SurgeBarrier`:

- `Edge(from_name=..., to_name=...)` — bare constructor uses `FlowControl()` defaults: `HardLock` gate, **no SurgeBarrier**, `Reservoir(depth=1)`, `DropOldest`.
- `Edge(from_name=..., to_name=..., gate_mode="hard")` — invokes `flow_from_mode("hard")` which attaches a `SurgeBarrier(threshold_multiple=2.0, action="skip")` in addition to `HardLock`.  `"soft"` and `"weir"` do **not** add a SurgeBarrier.

Pass `flow=FlowControl(...)` explicitly to control the SurgeBarrier independently of the gate shorthand.

**When to reach for it**
- Lab default (no kwargs) — bare `Watershed.chain(currents=[...])` applies `gate_mode="hard"` across every derived edge, which **does** include a default `SurgeBarrier(threshold_multiple=2.0, action="skip")`.  Good enough for most pipelines.
- Production needs (slow downstream behind a fast upstream) — add a `Penstock` to throttle and a deeper `Reservoir` + an `ExportToArchive` `Spillway` to audit what didn't get processed.
- Multi-source fusion where one feed can lag — `SurgeBarrier(action="bypass")` keeps the fjord ticking on the others.
- Green-wave coordination — pair a deeper `Reservoir` with `BackpressurePenstock` to smooth consumption rate against upstream burstiness.

**See also**
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) ·
[`docs/cli_and_configuration.md §9`](./cli_and_configuration.md) ·
[Library Reference](./library_reference.md)

---

### CustomCurrent

**Import path** *(load-bearing — not top-level)*
```python
from incorporator.tideweaver import CustomCurrent
```

**Signature**
```python
class CustomCurrent(Current):
    auto_park_snapshot: ClassVar[bool] = True

    async def tick(self, scheduler: Tideweaver) -> None: ...
```

`CustomCurrent` is the escape hatch for tick logic that does not fit the three verb-typed Currents (`Stream` / `Fjord` / `Export`).  Subclass it, override `async tick(self, scheduler)`, and place the instance in the `Watershed.currents` list like any other Current.  The scheduler calls `current._run_tick(scheduler)` — which wraps `tick()` with the auto-park logic described below — in place of the normal verb dispatch.

**What it does (pseudocode)**
1. On each scheduler pass that satisfies the interval + upstream gate checks, the scheduler calls `await current._run_tick(scheduler)`.
2. `_run_tick` records the pre-tick value of `cls._tideweaver_snapshot` (the identity sentinel).
3. `await self.tick(scheduler)` runs the user-supplied body.
4. If `auto_park_snapshot` is `True` and the tick body did NOT assign a new list to `cls._tideweaver_snapshot` (identity check — a new list object means the user wired their own snapshot), the scheduler parks `list(cls.inc_dict.values())` as `cls._tideweaver_snapshot` so downstream `HardLock` edges see fresh upstream waves without manual wiring.
5. After the tick returns, if the resulting `_tideweaver_snapshot` is empty while at least one upstream current's snapshot was non-empty, the scheduler emits a per-tick WARNING — surfaces silent predicate or `conv_dict` mismatches without needing a debugger (fires each tick while the condition persists).

**`auto_park_snapshot` opt-out contract**

Set `auto_park_snapshot: ClassVar[bool] = False` on the subclass to disable the automatic snapshot park.  Only correct for ticks that are pure side-effects (health-check pings, external metric pushes) that should never gate downstream currents.  When `False`, downstream `HardLock` edges will stay permanently unblocked unless the tick body manually assigns `cls._tideweaver_snapshot`.

```python
from incorporator.tideweaver import CustomCurrent

class HealthcheckPing(CustomCurrent):
    auto_park_snapshot: ClassVar[bool] = False   # pure side-effect — don't gate downstream

    async def tick(self, scheduler):
        response = await httpx.get("https://internal.acme/health")
        if response.status_code != 200:
            raise RuntimeError(f"health check failed: {response.status_code}")
```

**Immutability contract**

`tick()` MUST NOT register new `Current`s or `Edge`s, nor mutate `scheduler.watershed.currents` / `scheduler.watershed.edges`, after `Tideweaver.run()` has started.  The scheduler memoises transitive-upstream lookups once per instance for O(1) gate evaluation; runtime topology mutations would silently invalidate that cache and produce incorrect gating decisions.  To add a current mid-run, stop the current watershed and start a new `Tideweaver` instance.

**When to reach for it**
Use `CustomCurrent` only when the standard verb-typed Currents genuinely cannot express the tick logic: health-check pings, sentinel-row insertions, externally-driven publishers, computed-field filters that depend on a derived attribute only available post-seeding.  For all standard `incorp()` / `refresh()` / `export()` shapes reach for `Stream`, `Fjord`, or `Export` first.

**See also**
[Row filtering: pick the right primitive](#row-filtering-pick-the-right-primitive) (escape-hatch entry #5) ·
[Class-attribute reference](#class-attribute-reference) (`CustomCurrent` row) ·
[Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md)

---

## Row filtering: pick the right primitive

The framework has **no post-fetch row-filter primitive** — there is no
`Stream.parent_filter`, no `Fjord.parent_filters`. Row scoping always
happens at the source. Pick the right primitive from this decision tree:

1. **SQL source** → `SQLitePaginator(sql_query="... WHERE ...")`.
   Database-side `WHERE` is the cheapest filter the framework can express
   — the rows you don't want never leave SQLite. See
   `incorporator/io/pagination/local.py`.

2. **HTTP source with a filter-capable API** → `inc_url` carrying the
   filter in the URL string (`?divisionId=201`, `?status=active`,
   `?since=2024-01-01`). Probe the live API if the filter parameter is
   undocumented. This is the established framework idiom:
   - `examples/appendix/mlb-pulse/` → `?leagueId=103` for AL teams.
   - `examples/11-tideweaver/` → `?pair=XBTUSD,ETHUSD` for a symbol set.
   - `examples/appendix/crypto-graph-mapping/` →
     `?vs_currency=usd&per_page=100`.
   - `examples/appendix/pokeapi-etl/` → `?limit=50&offset=0`.

3. **Aggregating multiple upstreams where the filter belongs with the
   join logic** → filter inside the `outflow(state)` return list.
   See T9 (NASCAR fjord) and T10 (multi-source fjord) for the pattern —
   the filter and the row-shaping live together where the join is
   declared.

4. **Multi-child with different filters** → declare a separate
   URL-filtered parent `Stream` per filter, one child per parent.
   Cheaper than fetch-all-and-post-filter, and the dependency graph
   stays explicit.

5. **Computed-field filter (rare, escape hatch)** → subclass
   `CustomCurrent` and override `async tick(...)` to filter and call
   `cls.incorp(inc_parent=filtered)` yourself. Use this only when the
   URL / SQL / outflow primitives genuinely can't express the predicate
   (e.g. the filter depends on a derived attribute that's only available
   after seeding the parent).

**See also** the `Stream.parent_current` / `Fjord.parent_currents`
entries in the class-attribute reference at the end of this document —
the declarative parent-child dependency primitives that pair with this
decision tree.

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

## Observability layer (`LoggedIncorporator` + `LoggedTideweaver`)

### LoggedIncorporator — shared `enable_logging=` note

The five verbs `LoggedIncorporator` overrides (`incorp`, `refresh`, `export`, `stream`, `fjord`)
accept every kwarg their `Incorporator` counterpart accepts, plus one extra:
`enable_logging: bool = False`. (`test`, `architect`, and `display` are inherited unchanged —
they don't take `enable_logging=` and don't emit log records.) When set to `True`, the call wires up a
per-class `QueueHandler`-backed logger that writes rotating JSON-line records
to `logs/<ClassName>_{api,error,debug}.log`. Disk I/O runs on a background
thread — the event loop never blocks on log writes. Logging is **opt-in per
call**, so the same class can run unobserved one moment and fully-traced the
next.

**Routing model (v1.3.3).** Records are classified at write time before reaching any file handler:

| File | Receives |
|---|---|
| `<ClassName>_api.log` | URL/internet-traffic errors (`is_url_traffic_error=True`): HTTP 4xx/5xx, network timeouts, connection failures |
| `<ClassName>_error.log` | All non-API-routed records at INFO and above: successful waves, parse failures, schema errors |
| `<ClassName>_debug.log` | Superset — every record in both files above, plus DEBUG-floor lifecycle events |

The routing decision is a single read of `record.is_url_traffic_error` (`logger.py:199`). Per-wave throughput and structured `RejectEntry` records are both queryable via the reader API described in the entries below.

---

<a id="loggedincorporator-get_error"></a>
### LoggedIncorporator.get_error

**Signature**
```python
@classmethod
async def get_error(cls) -> list[dict[str, Any]]:
```

**What it does (pseudocode)**
1. Resolve `logs/<ClassName>_error.log`; return `[]` if the file does not exist.
2. In a worker thread (`asyncio.to_thread`), walk the file line-by-line and parse each JSON line into a dict.
3. Silently skip malformed lines; treat `OSError` as "no errors yet".
4. Return the list of parsed records.

**When to reach for it**
Codebase/parse errors only — schema failures, converter errors, canal skips where `is_url_traffic_error=False`. Use `get_rejects()` when you want all failures across both routing files.

**Yields / returns**
`list[dict[str, Any]]` — each dict has `level`, `msg`, `meta`, optional `wave`, `time`, optional `exc_info`.

**See also**
[Production Debugging](./debugging.md) ·
[Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md)

---

<a id="loggedincorporator-get_api"></a>
### LoggedIncorporator.get_api

**Signature**
```python
@classmethod
async def get_api(cls) -> list[dict[str, Any]]:
```

**What it does (pseudocode)**
1. Resolve `logs/<ClassName>_api.log`; return `[]` if absent.
2. Read line-by-line in a worker thread; parse each JSON line.
3. Return all records — these are exclusively URL/internet-traffic records (`is_url_traffic_error=True`): HTTP 4xx/5xx responses, network timeouts, and connection failures.

**When to reach for it**
When you want to inspect only the API/network failure side — for example, to count 429s, check `Retry-After` patterns, or build a host-level failure summary without mixing in parse errors.

**Yields / returns**
`list[dict[str, Any]]` — same shape as `get_error()`.

**See also**
[Production Debugging](./debugging.md)

---

<a id="loggedincorporator-get_rejects"></a>
### LoggedIncorporator.get_rejects

**Signature**
```python
@classmethod
async def get_rejects(cls) -> list[dict[str, Any]]:
```

**What it does (pseudocode)**
1. Call `read_log(cls.__name__, ["error", "api"], key="reject")`.
2. `read_log` reads both `_error.log` and `_api.log`, parses each JSON line, and keeps only records that contain a top-level `"reject"` key. `key="reject"` is a JSON-key presence check, not a log-level filter.
3. Return the union — every `RejectEntry` the class has written, regardless of which routing file it landed in.

**When to reach for it**
The default reject reader — the one to call when you want all failures for retry orchestration, without caring whether each one was a URL/internet error or a codebase error. The `entry["reject"]["is_url_traffic_error"]` field lets you classify them after the fact if needed.

**Yields / returns**
`list[dict[str, Any]]` — each dict has a top-level `"reject"` key whose value is the `RejectEntry` model dump.

**See also**
[Production Debugging](./debugging.md) ·
[`get_error`](#loggedincorporator-get_error) · [`get_api`](#loggedincorporator-get_api)

---

<a id="loggedincorporator-get_current"></a>
### LoggedIncorporator.get_current

**Signature**
```python
@classmethod
async def get_current(cls, code: str) -> list[dict[str, Any]]:
```

**What it does (pseudocode)**
1. Call `read_log(cls.__name__, ["debug"], meta_contains=code)`.
2. Reads only `<ClassName>_debug.log` because the debug file carries no filter and a DEBUG floor — every record that lands in `_api.log` or `_error.log` also lands in `_debug.log`. Reading the debug superset exclusively avoids the double-counting that would occur when unioning all three files.
3. Filter to records whose `meta` field contains the string `code`.
4. Return the matching records.

**When to reach for it**
Retrieve all log records for a specific instance identity within the current session (e.g. a specific `inc_code` value). Avoids reading api + error + debug separately and deduplicating — the debug superset gives the complete picture in one read.

**Common kwargs**
- `code` — the `inc_code` value (or any `meta` substring) that identifies the target instance or current.

**Yields / returns**
`list[dict[str, Any]]`

**See also**
[Production Debugging](./debugging.md)

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

<a id="loggingmixin"></a>
### LoggingMixin

**Signature**
```python
class LoggingMixin():
    # Instance-level verbs
    def log_debug(self, msg: str) -> None: ...
    def log_info(self, msg: str) -> None: ...
    def log_error(self, msg: str, exc_info: bool = False) -> None: ...
    def log_api(self, msg: str) -> None: ...
    def log_meta(self) -> str: ...
    # Class-level verbs
    @classmethod
    def log_cls_info(cls, msg: str) -> None: ...
    @classmethod
    def log_cls_error(cls, msg: str, exc_info: bool = False) -> None: ...
    @classmethod
    async def get_error(cls) -> list[dict[str, Any]]: ...
```

**Import**
```python
from incorporator import LoggingMixin
# also available from:
from incorporator.observability.logger import LoggingMixin, setup_class_logger
```

**What it does**
Escape-hatch mixin that adds all structured logging methods to any `Incorporator` subclass without the full `LoggedIncorporator` verb-wrapper machinery.  The mixin provides `log_debug` / `log_info` / `log_error` / `log_api` / `log_cls_info` / `log_cls_error` / `get_error` on a custom subclass.  All methods silently noop when the class has not been wired through `setup_class_logger()` — safe to leave in code paths that may run before logging is enabled.  Records land in rotating JSONL files via a `QueueHandler`-backed background thread.

**When to reach for it**
Subclass `LoggingMixin` directly when you want the structured logging surface on a custom `Incorporator` subclass but don't want the verb overrides that `LoggedIncorporator` adds (`incorp` / `refresh` / `export` / `stream` / `fjord` with `enable_logging=` wiring).  Typical pattern: a custom subclass that manages its own fetch logic but still needs `get_error()` and per-instance log calls for post-mortem forensics.

```python
from incorporator import Incorporator
from incorporator.observability.logger import LoggingMixin, setup_class_logger

class Audited(LoggingMixin, Incorporator):
    pass

setup_class_logger(Audited)
# instance.log_info("backtest prep started")
# failures = await Audited.get_error()
```

**See also**
[`setup_class_logger`](#setup_class_logger) ·
[`LoggedIncorporator` — shared `enable_logging=` note](#loggedincorporator--shared-enable_logging-note)

---

<a id="setup_class_logger"></a>
### setup_class_logger

**Signature**
```python
def setup_class_logger(cls: str | type[Any]) -> None:
```

**Import**
```python
from incorporator import setup_class_logger
# also available from:
from incorporator.observability.logger import setup_class_logger
```

**What it does (pseudocode)**
1. Resolve the logger name: if `cls` is a class, use `cls.__name__`; if `cls` is a plain string, use it directly (the string form is used by `LoggedTideweaver` to set up a named logger without a class reference).
2. Return early (idempotent) if the name is already in `_ACTIVE_LISTENERS` or the logger already has handlers — prevents duplicate background threads on repeated calls or dynamic class rebuilds.
3. Wire four rotating JSONL handlers (`debug.log`, `error.log`, `api.log`, `tide.log`) at `logs/<name>_*`, each capped at 5 MB × 3 backups.
4. Attach a `QueueHandler` + `QueueListener` backed by `queue.SimpleQueue` so all disk I/O runs on a dedicated background thread — the event loop never blocks on log writes.
5. Register the listener in `_ACTIVE_LISTENERS`; register a `_cleanup_listeners` atexit hook that gracefully stops all listeners on process exit.

**When to reach for it**
Call it directly when you use `LoggingMixin` on a custom subclass rather than `LoggedIncorporator`, or when you want structured logging on a class identified by a string name (e.g. a dynamically constructed logger name in a Tideweaver pipeline).  `LoggedIncorporator` calls it automatically when `enable_logging=True` — you don't need to call it manually for standard verb usage.

```python
from incorporator.observability.logger import LoggingMixin, setup_class_logger

class MyPipeline(LoggingMixin, Incorporator):
    pass

setup_class_logger(MyPipeline)          # wire once at startup
instance.log_info("pipeline started")   # now live
```

**Common kwargs**
- `cls` — a class (uses `cls.__name__` as the logger key) or a plain string name (used when a caller manages a custom logger name).

**Yields / returns**
`None`.  Idempotent — safe to call multiple times; subsequent calls for the same name are no-ops.

**See also**
[`LoggingMixin`](#loggingmixin) ·
[`LoggedIncorporator` — shared `enable_logging=` note](#loggedincorporator--shared-enable_logging-note)

---

### LoggedTideweaver

**Signatures** *(v1.2.1+, extended in v1.3.3)*
```python
class LoggedTideweaver(Tideweaver):
    def __init__(
        self,
        watershed: Watershed,
        *,
        enable_logging: bool = False,
        logger_name: str | None = None,  # resolves: logger_name > watershed.name > "Tideweaver"
        log_currents: bool = True,       # v1.3.3: route per-current waves/rejects to session log
        pass_interval: float | None = None,
        backlog_backoff_factor: float = 1.0,
        # ...plus every Tideweaver kwarg.
    ) -> None: ...

    @classmethod
    async def get_tides(cls, logger_name: str) -> list[dict[str, Any]]: ...
    @classmethod
    async def get_rejects(cls, logger_name: str) -> list[dict[str, Any]]: ...
    @classmethod
    async def get_current(cls, logger_name: str, code: str) -> list[dict[str, Any]]: ...
    @classmethod
    async def get_scheduler_events(cls, logger_name: str) -> list[dict[str, Any]]: ...
```

**`logger_name` resolution (v1.3.3).** The resolved name drives log file prefixes. Precedence: explicit `logger_name` kwarg > `watershed.name` > `"Tideweaver"`.

```python
ws = Watershed(name="NightlyPrices", window=(...), currents=[...])
tw = LoggedTideweaver(ws, enable_logging=True)
# logger_name resolves to "NightlyPrices"
# → logs/NightlyPrices_tide.log, NightlyPrices_error.log, NightlyPrices_api.log, NightlyPrices_debug.log
```

**`log_currents` (v1.3.3).** When `True` (default), each `Stream` current's yielded waves and their `wave.rejects` are routed to the session log tagged with `current`, `class`, and `code` meta fields — no separate per-class `<Class>_*.log` files are created during a watershed run. Set `log_currents=False` to suppress all per-current routing.

**Import path** *(load-bearing — not top-level)*
```python
from incorporator.tideweaver import LoggedTideweaver
```

**What it does (pseudocode)**
1. Construct exactly like `Tideweaver(...)`; disk I/O routes through the same `QueueHandler`-backed background thread as `LoggedIncorporator` — the event loop never blocks on log writes.
2. On every yielded `Tide`, route via `_route_tide_to_log()`: error-class passes (canal rejects added, or `surge_halted` / `skip_ahead` skip reasons) → ERROR; fired passes → INFO; pure no-op passes → DEBUG.  The handler set then sorts records by level — `debug.log` is the superset (every tide), `error.log` accepts INFO and above (fired + error-class), `tide.log` collects every record tagged `is_tide=True` for `get_tides()` readback.
3. On every accumulated `RejectEntry` (swept in a `finally` block so records land on disk even under cancellation), emit a JSON-line — routed to `_api.log` when `is_url_traffic_error=True`, otherwise to `_error.log`.
4. `get_tides(logger_name)` reads the dedicated `logs/<logger_name>_tide.log` file and returns records sorted by `tide_number` — single-file read, no merge needed.
5. `get_rejects(logger_name)` unions `_error.log` + `_api.log`, returning records with a `"reject"` key.
6. `get_current(logger_name, code)` reads `_debug.log` filtered by `meta_contains=code` — the debug superset avoids double-counting.
7. `get_scheduler_events(logger_name)` reads `_error.log` filtered to records with a `"scheduler_event"` key. Includes `watershed_started`, `watershed_completed`, and six diagnostic event types.

**When to reach for it**
The orchestration-side `LoggedIncorporator` — for Tideweaver pipelines that need disk-readable Tide + RejectEntry capture without inline `print(tide)`.  Pair with `tune()` for the post-run feedback loop; pair with `LoggedTideweaver.get_tides()` / `get_rejects()` for cross-process replay (a separate analysis worker reading the log files).

**Common kwargs**
- `watershed` — same as `Tideweaver`.
- `enable_logging=` — `True` to wire up the `QueueHandler` pipeline.
- `logger_name=` — namespace for the log files; required when `enable_logging=True`.
- `backlog_backoff_factor=` — same v1.2.1 opt-in as `Tideweaver`.

**Yields / returns**
Inherits `run()` from `Tideweaver` — `AsyncIterator[Tide]`.  `get_tides(logger_name)` returns `list[dict[str, Any]]` — each dict has a top-level `"tide"` key whose value is the Tide model dump.  `get_rejects(logger_name)` returns `list[dict[str, Any]]` — each dict has a top-level `"reject"` key.  Both return `[]` when no log files exist yet.

**Log-file layout (v1.3.3)**

When `enable_logging=True`, the runner writes four rotating JSONL files under `logs/<logger_name>_`:

| File | Contents | Reader |
|---|---|---|
| `<logger_name>_tide.log` | Every yielded `Tide` (fired + no-op), single source of truth | `LoggedTideweaver.get_tides()` |
| `<logger_name>_error.log` | Non-API records at INFO+: canal-layer rejects, codebase errors, scheduler events, lifecycle events, successful tides | `LoggedTideweaver.get_rejects()` / `get_scheduler_events()` |
| `<logger_name>_api.log` | URL/internet-traffic errors (`is_url_traffic_error=True`) from verb-layer rejects | `LoggedTideweaver.get_rejects()` (unioned) |
| `<logger_name>_debug.log` | Superset — all records from both files above, plus DEBUG lifecycle events | `LoggedTideweaver.get_current()` |

**`get_scheduler_events()` event types:** `watershed_started`, `watershed_completed`, `isolated_tick_failure`, `tick_parked`, `empty_output`, `empty_parent_snapshot`, `fjord_flush_failure`, `spillway_overflow` (v1.3.5). Each record includes `event_type`, `current_name`, `cls_name`, `tide_number`, `session`, and `detail`. `spillway_overflow` is edge-scoped rather than current-scoped or tide-scoped: `current_name`, `cls_name`, and `tide_number` are `None`; `edge` carries the `[from_name, to_name]` pair and `detail` describes the displacement (e.g. `"Tideweaver: spillway overflow on edge upstream → downstream (count=3)"`).

**See also**
[Tutorial 11 — Post-run tuning](../examples/11-tideweaver/README.md#post-run-tuning) ·
[Production Debugging — Orchestration debugging](./debugging.md#orchestration-debugging--loggedtideweaver--architecttune) ·
[Deployment — Production logging for Tideweaver](./deployment.md#production-logging-for-tideweaver--loggedtideweaver)

---

## Schema utilities

Helpers for `conv_dict`, `json_payload`, and `form_payload` that don't fit neatly inside the `incorp` kwarg descriptions.  All are importable from the top-level `incorporator` package.

**Import**
```python
from incorporator import new, sum_attributes, each, join_all, as_list
```

---

### `new` — generated-field sentinel

**Signature**
```python
new  # module-level value, not a callable
```

`new` is a module-level sentinel value (instance of the private `_NewSentinel` class).  Assign it as the value for a `conv_dict` key to signal "this field should exist on the dynamic class but has no source key to map from — generate it from scratch."  The schema factory accepts any valid Python type for a `new`-valued key; the value defaults to `None` unless a converter on the same key sets it.

```python
from incorporator import Incorporator, new, calc

class Enriched(Incorporator):
    pass

await Enriched.incorp(
    inc_url="https://api.example.com/items",
    conv_dict={
        "computed_rank": new,       # field exists on the class; populated by a downstream calc
        "upper_name":   calc(str.upper, "name", target_type=str),
    },
)
```

**When to reach for it**
When you need a field to exist on the dynamic class (e.g. for a downstream `calc()` to write into, or for Pydantic to include in the schema) but no source key maps to it directly.  Rare in typical ETL — most fields come from the raw payload.

---

### `sum_attributes` — safe field-sum reducer

**Signature**
```python
def sum_attributes(*args: Any) -> float:
```

`sum_attributes` is a ready-made `calc()` reducer that safely sums N fields, treating `None` and non-numeric values as zero.  Numeric strings (`"42"`), floats, ints, and `None` all mix without raising.  Use it as the `func` argument to `calc()`:

```python
from incorporator import calc, sum_attributes

await Pokemon.incorp(
    inc_url="https://pokeapi.co/api/v2/pokemon?limit=151",
    conv_dict={
        "total_stats": calc(sum_attributes, "hp", "attack", "defense", "speed"),
    },
)
# pokemon.total_stats == float(hp + attack + defense + speed)
```

**When to reach for it**
Any time you'd write a 3-line try/except to total a handful of row fields: Pokémon base-stat totals, revenue sums across line items, point totals in a fantasy scoring sheet.

---

### `each` — POST fan-out sentinel

**Signature**
```python
def each() -> _EachSentinel:
```

`each` is a POST fan-out sentinel.  Place `each()` as the value in `json_payload` or `form_payload` to tell the router "make one POST per parent ID rather than a single bulk request."  Produces N concurrent requests — one per row in the parent snapshot.  Returns an `_EachSentinel` instance (not an `Op`).

```python
from incorporator import Incorporator, each

results = await Decoded.incorp(
    inc_url="https://vpic.nhtsa.dot.gov/.../DecodeVin/",
    inc_parent=invoices,
    inc_child="Vehicle.VIN",
    http_method="POST",
    json_payload={"vin": each(), "format": "json"},
)
```

**When to reach for it**
When the target endpoint takes exactly one ID per request and will not accept a bulk body.  Contrast with `join_all()` (one POST, delimited string) and `as_list()` (one POST, JSON array) for endpoints that accept batch shapes.

---

### `join_all` — bulk-POST delimited string

**Signature**
```python
def join_all(delimiter: str = ",") -> Op:
```

`join_all` collapses all parent IDs into one delimited string for a single bulk POST.  Returns an `Op` instance.  Place it in `form_payload` when the endpoint accepts a delimited-batch body (e.g. NHTSA `DecodeVINValuesBatch/` takes `vin1;vin2;vin3`).

```python
from incorporator import Incorporator, join_all

specs = await NHTSASpec.incorp(
    inc_url="https://vpic.nhtsa.dot.gov/.../DecodeVINValuesBatch/",
    inc_parent=invoices,
    inc_child="Vehicle.VIN",
    http_method="POST",
    payload_type="form",
    form_payload={"data": join_all(";"), "format": "json"},
)
```

**Args**
- `delimiter` — separator between IDs.  Default `","` ; common alternatives are `";"` and `"|"`.

**When to reach for it**
When the endpoint supports a delimited-batch shape and `each()` (N requests) would be wasteful or rate-limited.

---

### `as_list` — bulk-POST JSON array

**Signature**
```python
def as_list() -> Op:
```

`as_list` wraps all parent IDs in one JSON array for a single bulk POST.  Returns an `Op` instance.  Place it in `json_payload` when the endpoint expects `{"ids": [1, 2, 3]}` or any other JSON-array-bodied bulk shape.

```python
from incorporator import Incorporator, as_list

results = await Audit.incorp(
    inc_url="https://api.example.com/bulk-audit",
    inc_parent=invoices,
    inc_child="id",
    http_method="POST",
    json_payload={"ids": as_list()},   # → {"ids": [1, 2, 3, ...]}
)
```

**When to reach for it**
When the endpoint expects a JSON array body and a single round-trip.  Scalar inputs are wrapped in a single-element list.  Contrast with `each()` (N requests) and `join_all()` (one request, delimited string).

---

## Shared kwargs glossary

- `inflow=` — sidecar `.py` exposing public symbols for `conv_dict` token resolution; in fjord, may also define `inflow(state)` for sequential dependent seeding (see [the `inflow(state)` contract](#fjord) under the fjord entry for call cadence, guard requirements, and return shape).
- `outflow=` — sidecar `.py` whose stem becomes the dynamic output class name; must define `outflow(state) -> list[dict]` (or `dict[ClassName, list[dict]]` for multi-output fjord).
- `inc_page=` — `AsyncPaginator` subclass (`PageNumberPaginator`, `CursorPaginator`, `OffsetPaginator`, `NextUrlPaginator`, `LinkHeaderPaginator` for web; `SQLitePaginator`, `CSVPaginator`, `AvroPaginator` for local) that drives chunking-mode `stream()` or paginated `incorp()`. Every paginator subclass accepts a keyword-only `penstock=` argument (defaults to `NullPenstock()`); pass a `SustainedPenstock(rate_per_sec=...)` / `BurstPenstock` / `WindowPenstock` / `SignalPenstock` to throttle the yield rate at the paginator layer. Web paginators compose additively with host-level throttles registered via `register_host_penstock`; local paginators have no other throttle path, so the per-paginator penstock is the only way to bound their disk-speed iteration. See [Streaming & Pagination §6](./streaming_and_pagination.md#6-throttling-paginators) for worked examples.
- `format_type=` — `FormatType` enum forcing a writer when the file extension is ambiguous; otherwise auto-detected from extension.
- `enable_logging=` — on `LoggedIncorporator` only; wires the call into per-class rotating JSONL handlers (`logs/<ClassName>_{api,error,debug}.log`).
- `inc_code=` — field name on each record that becomes the primary key in `inc_dict`. Pass the field name (e.g. `"id"`); the framework reads each record's value at that key.

---

## DATA-SHAPE directives

The four data-shape pipeline parameters (`excl_lst`, `name_chg`,
`code_attr`, `name_attr`) travel through a single normalizer
(`_normalize_etl_kwargs`) into typed frozen-dataclass directives before
the dispatcher runs.  Bare strings and 2-tuples keep working — the
normalizer accepts mixed sequences of bare shapes and directive
instances, and emits an identical `NormalizedKwargs` container either
way.

**Import**
```python
from incorporator.schema.directives import Ex, Nm
```

`Pk` is also importable from the same module but is synthesised
internally — users pass `code_attr="field"` / `name_attr="field"` (bare
strings) and the framework constructs the `Pk` instances at normalize
time.

**The three directives**

| Directive | Shape | Where it goes | Purpose |
|---|---|---|---|
| `Ex(field: str)` | frozen dataclass | `excl_lst` | Drop a field.  Bare `Ex("foo")` drops the top-level key `"foo"`; dotted-path `Ex("a.b.c")` drops the nested leaf via `DataPath.pop`. |
| `Nm(old: str, new: str)` | frozen dataclass | `name_chg` | Rename a top-level key.  Same semantics as the bare 2-tuple `("old", "new")`; both forms produce identical normalised output. |
| `Pk(source: str, target: Literal["code", "name"])` | frozen dataclass | synthesised internally | Bind the value at `source` to `inc_code` or `inc_name`.  Built by the normalizer from `code_attr` / `name_attr` bare strings; `Pk.source` is rewritten through the `name_chg` rename map (first-hit) so renames don't desync the bind. |

**Four-pass dispatch order** — `incorporator/schema/builder.py:185-315`

1. **Ex (drop)** — every directive applied per row via `Ex.apply_drop(record)`.
2. **`conv_dict` ops** — converter operations apply per row, op-outer / row-inner.
3. **Nm (rename)** — every directive applied per row via `Nm.apply_rename(record)`.
4. **Pk (PK-bind)** — runs last so renamed source fields resolve cleanly.  `Pk.apply_bind(record)` reads `_path.resolve(record)` and writes `inc_code` or `inc_name`.

PK binding running after rename closes two silent failure modes the
prior order let through — Case A (rename moves the source away from
where `code_attr` pointed) and Case B (rename *creates* the field
`code_attr` targets, but the bind ran too early and the auto-counter
fallback silently wrote synthetic IDs).

**Worked example**

```python
from incorporator import Incorporator
from incorporator.schema.directives import Ex, Nm

class Invoice(Incorporator): pass

# Bare-string form (always worked).
await Invoice.incorp(
    inc_file="invoices.json",
    excl_lst=["internal_id"],
    name_chg=[("ext_id", "id")],
    code_attr="id",
)

# Directive form (post-normalizer).
await Invoice.incorp(
    inc_file="invoices.json",
    excl_lst=[Ex("internal_id"), Ex("audit.legacy_flag")],   # nested drop
    name_chg=[Nm("ext_id", "id"), Nm("vendor_code", "code")],
    code_attr="id",
)

# Mixed sequences are accepted in the same list.
await Invoice.incorp(
    inc_file="invoices.json",
    excl_lst=["internal_id", Ex("audit.legacy_flag")],
    name_chg=[("ext_id", "id"), Nm("vendor_code", "code")],
    code_attr="id",
)
```

**JSON form** — the same directives resolve as text tokens through `resolve_tokens()`:

```json
{
  "incorp_params": {
    "excl_lst": ["internal_id", "Ex('audit.legacy_flag')"],
    "name_chg": [["ext_id", "id"], "Nm('vendor_code', 'code')"],
    "code_attr": "id"
  }
}
```

`Pk` is allow-listed at `incorporator/cli/tokens.py:125-127` for
forward-compat — a token string like `"Pk('id', target='code')"`
resolves to a `Pk` instance — but JSON pipelines today have no
canonical destination slot for it.  Continue to use `code_attr` /
`name_attr` bare strings in JSON.

**When to reach for the directive form**

- Type-safe, IDE-friendly drop/rename declarations in Python code.
- Hashable frozen containers for cache/replay across many `incorp()` calls.
- Nested-leaf drops via `Ex("a.b.c")` that bare-string `excl_lst` cannot express.

**See also**
[Library Reference](./library_reference.md) ·
`incorporator/schema/directives.py` ·
`incorporator/schema/builder.py`

---

## Class-attribute reference

| Symbol | Owner | Kind | Purpose |
|---|---|---|---|
| `inc_dict` | `Incorporator` (ClassVar) | `WeakValueDictionary[Any, Incorporator]` | per-class O(1) registry — `inc_code → instance`. Auto-populated by `model_post_init()`. |
| `inc_url` / `inc_file` | `Incorporator` (ClassVar) | `str | None` | origin tracking. `refresh()` falls back to these when called without explicit new sources. |
| `inc_code` / `inc_name` / `last_rcd` | instance | universal Pydantic fields | identity (auto-counter fallback) + display label + UTC construction timestamp. |
| `failed_sources` | `IncorporatorList` | `list[str]` | legacy flat reject-list surface — every URL/file that hit a permanent failure.  Derived view of `rejects` (`[entry.source for entry in rejects]`). |
| `Wave.{chunk_index, operation, rows_processed, failed_sources, processing_time_sec, timestamp}` | `Wave` (frozen Pydantic) | core model fields | one record per pipeline tick. Yielded by `stream()` and `fjord()`. |
| `Wave.{source_url, bytes_processed, http_retry_count, validation_error_count, schema_cache_hit, conv_dict_time_sec, parent_snapshot_size}` | `Wave` (frozen Pydantic) | v1.2.1 outcome-record fields | per-wave telemetry surface: source URL, byte volume (`bytes_processed` = decoded/decompressed `len(response.content)`), HTTP retry count, validation error count, schema-cache hit flag, per-chunk ETL wall-clock, and upstream snapshot row count. |
| `Wave.bytes_downloaded` | `Wave` (frozen Pydantic) | v1.3.3 telemetry field | Wire byte count transferred (`response.num_bytes_downloaded`). Distinct from `bytes_processed` (decoded size). `None` for non-HTTP chunks (file-mode, local paginator, error path). Use the ratio `bytes_processed / bytes_downloaded` as a compression-efficiency signal. |
| `Wave.http_fetch_time_sec` | `Wave` (frozen Pydantic) | v1.3.3 telemetry field | HTTP round-trip latency in seconds (`response.elapsed.total_seconds()`). `None` for non-HTTP chunks. Feeds `_tune_http_timeout` and `_tune_penstock_rate` in `tune()`. |
| `Wave.rejects` | `Wave` (frozen Pydantic) | v1.3.3 telemetry field | `list[RejectEntry]` from the chunk's incorp/seed call. Per-chunk — not accumulated across the stream session. Empty on exception-path waves where no `IncorporatorList` was available. |
| `Tide.{tide_number, fired, skipped, duration_sec, timestamp}` | `Tide` (frozen Pydantic) | core model fields | one record per `Tideweaver` scheduler pass. Yielded by `Tideweaver.run()`. |
| `Tide.{current_outcomes, wake_reason, heap_depth, in_flight_count_at_start, canal_rejects_added, next_due_in_sec}` | `Tide` (frozen Pydantic) | v1.2.1 outcome-record fields | per-pass scheduler telemetry: list of per-current outcomes, wake reason (Literal), heap depth, in-flight tick count at start, new canal rejects this pass, seconds until next due tick. |
| `CurrentOutcome` (`incorporator.tideweaver.current_outcome`) | `@dataclass(frozen=True, slots=True)` | per-current outcome record | Fields: `name: str`, `status: str` (`"fired"` / `"skipped"` / `"still_running"`), `reason: str | None`, `bypassed_edges: tuple[str, ...]`, `in_flight_sec: float | None`, `last_wave_at: datetime | None`, `parent_snapshot_size: int | None` (upstream snapshot row count consumed by a parent-child tick; `None` for non-parent-child currents — used by `tune()` to detect empty-upstream misconfiguration). Surfaced via `tide.current_outcomes`. |
| `IncorporatorList.inc_dict` | property on the list | shared view of class registry | what `incorp()`'s return value exposes; mutations write through to `cls.inc_dict`. |
| `IncorporatorList.rejects` | property on the list | `list[RejectEntry]` | structured reject list — entry fields: `source`, `error_kind`, `message`, `retry_after`, `wave_index`.  Read by retry orchestrators that want the exception type or `Retry-After` hint without parsing strings. |
| `Tideweaver.rejects` | attribute on the instance | `list[RejectEntry]` | structured canal-layer reject list — same `RejectEntry` type, but `error_kind` can be one of four canal-layer literals (`"PenstockLimited"` / `"SurgeHalted"` / `"SkipAhead"` / `"GateBlocked"`) for scheduler-level skips that never reached a tick body.  `from_name` / `to_name` / `cooldown_sec` populated for per-edge attribution. |
| `RejectEntry` (top-level export) | frozen Pydantic | failure record | `from incorporator import RejectEntry`.  Populated by HTTP error sites in `io/fetch.py`, fjord seed errors, and the `Tideweaver` scheduler (canal-layer skips). v1.2.1 added `from_name`, `to_name`, `host`, `status_code`, `attempt_number`, `duration_sec`, `cooldown_sec`. v1.3.3 added `is_url_traffic_error: bool` (always present, `True` when the underlying exception is an httpx `HTTPStatusError` or `RequestError`, or an `IncorporatorNetworkError` wrapping one via `__cause__`; `False` for parse/schema/canal errors) and `session: str | None` (set from `logger_name` in Tideweaver sessions). `__str__` now includes HTTP reason phrases: `[HTTP 429 Too Many Requests]`; `[HTTP 522]` for non-standard codes (httpx `codes.get_reason_phrase()` lookup, graceful fallback for unknown codes). |
| `SourceRef` (`incorporator.io.SourceRef`) | frozen dataclass | source value type | Five factories (`from_url` / `from_file` / `from_parent` / `from_payload` / `from_kwargs`) plus an auto-detect `parse()` classmethod.  Internal scaffolding for `incorp()` / `architect()` source dispatch; opt-in public API for callers wanting explicit source typing. |
| `Stream.parent_current` | `Stream` field | declarative parent-child dependency | `parent_current: str` names an upstream `Stream` current in the same watershed. The framework auto-derives a `HardLock` Watershed edge from the parent, drives the snapshot read on every dependent tick, and injects the parent's `_tideweaver_snapshot` as `inc_parent` into the child's `cls.incorp(...)` call. **The parent declares its row scope at the URL / SQL / outflow level — the framework does not post-filter at the child.** See [Row filtering: pick the right primitive](#row-filtering-pick-the-right-primitive) for how to scope the parent. |
| `Fjord.parent_currents` | `Fjord` field | declarative multi-parent dependency | `parent_currents: list[str]` names one or more upstream `Stream` (or `Fjord`) currents. Same semantics as `Stream.parent_current` — auto-derived `HardLock` edges, snapshot reads on every tick — broadcast across all named parents into the fjord's `state` dict before `outflow(state)` runs. Each parent declares its own row scope at the URL / SQL / outflow level. See [Row filtering: pick the right primitive](#row-filtering-pick-the-right-primitive). |
| `CustomCurrent` (`incorporator.tideweaver.CustomCurrent`) | abstract `Current` subclass | escape hatch | Subclass and override `async tick(self, scheduler: Tideweaver) -> None` for non-verb tick logic (cron-style cleanups, custom side-effects, externally-driven publishers). The scheduler auto-parks `list(cls.inc_dict.values())` as `cls._tideweaver_snapshot` after every tick when the body didn't assign one (v1.2.3+) — downstream `HardLock` edges see fresh upstream waves without manual snapshot wiring. Set `auto_park_snapshot: ClassVar[bool] = False` to opt out (rare — only when the tick is a pure side-effect that shouldn't gate downstream). Also v1.2.3+: the scheduler emits a one-line WARNING per pass when a CustomCurrent tick succeeds but produces an empty `_tideweaver_snapshot` while upstream snapshots are non-empty — surfaces silent predicate / conv_dict mismatches without needing a debugger. |
| `GateContext` / `SurgeContext` / `FlowState` | frozen dataclasses | narrow value types | What custom `Gate.gate_reason(ctx)` / `SurgeBarrier.is_tripped(ctx)` / `Penstock.consume_reason(state, flow, now)` overrides read.  Authoring a custom strategy?  Subclass against these — never the scheduler. |

---

## FormatType

`FormatType` is the enum that names every supported format.  It surfaces in three programmatic contexts: as a parameter gate (`format_type=` on `export()` / `stream()` / `fjord()`), as a routing key inside the streaming and snapshot engines, and as a utility for callers writing custom outflow sidecars.

**Import**
```python
from incorporator import FormatType
```

**Three utility members**

| Member | Kind | What it does |
|---|---|---|
| `FormatType.is_append_safe` | `@property → bool` | `True` for formats whose write handler accepts `if_exists="append"` without producing a corrupt or unreadable file.  The chunked-stream, stateful-poll, and fjord engines consult this property to decide whether to inject append semantics on subsequent ticks or fall back to `"replace"`.  Append-safe: `NDJSON`, `CSV`, `TSV`, `PSV`, `SQLite`, `Avro`.  Monolithic (not append-safe): `JSON`, `XML`, `XLSX`, `Parquet`, `Feather`, `ORC`, `HTML`. |
| `infer_format(path_or_url)` | module-level function | Auto-detect `FormatType` from a file extension or URL string.  Strips one compression suffix (`.gz`, `.zst`, `.lz4`, etc.) before matching.  LRU-cached — safe to call on every paginator yield or fan-out source.  Import: `from incorporator.io.formats import infer_format`. |
| `convert_type(type_str, from_fmt, to_fmt, default="string")` | module-level function | Translate a type-string between two format type systems via the Python type bridge (e.g. `convert_type("integer", FormatType.JSON, FormatType.AVRO)` → `"long"`).  Used by `architect()` to emit cross-format conv_dict templates.  Import: `from incorporator.io.formats import convert_type`. |

**Worked example — Parquet snapshot guard**
```python
from incorporator import FormatType

fmt = FormatType.PARQUET
if not fmt.is_append_safe:
    # Parquet is monolithic — use a new filename per tick or accept full overwrite.
    export_params = {"file_path": f"snapshot_{tick}.parquet", "if_exists": "replace"}
```

**See also**
[Formats & Compression](./formats_and_compression.md) ·
[Appendix — Tideweaver Parquet Snapshots](../examples/appendix/tideweaver-parquet-snapshots/README.md)

---

## CompressionType

`CompressionType` is the enum that names every compression and archive format Incorporator can transparently decompress (and re-compress on `export()`).  The enum value is the canonical file-extension suffix (without the dot) used by `infer_compression()` to detect compression from a path or URL.

**Import**
```python
from incorporator import CompressionType
# companion:
from incorporator.io.compression import infer_compression
```

**Members by family**

| Family | Members | Enum values | Notes |
|---|---|---|---|
| Native streams (always available) | `GZIP`, `BZ2`, `XZ`, `LZMA` | `"gz"`, `"bz2"`, `"xz"`, `"lzma"` | stdlib `gzip` / `bz2` / `lzma`; single-file decompression |
| Native archives (always available) | `ZIP`, `TAR`, `TGZ` | `"zip"`, `"tar"`, `"tgz"` | multi-file archives; use `archive_target=` on `incorp()` to select a specific member |
| Cramjam plugins (requires `[speedups]`) | `ZSTD`, `LZ4`, `SNAPPY`, `BROTLI` | `"zst"`, `"lz4"`, `"snappy"`, `"br"` | Rust-backed via the `cramjam` package; install with `pip install incorporator[speedups]` |

**`infer_compression(path_or_url: str) -> CompressionType | None`**

Auto-detect the compression type from a file path or URL by its extension.  Case-insensitive — `data.JSON.GZ` resolves to `GZIP`.  Returns `None` when no recognised compression suffix is found.  LRU-cached for the same cardinality story as `infer_format`.

```python
from incorporator.io.compression import infer_compression, CompressionType

assert infer_compression("data.csv.gz") == CompressionType.GZIP
assert infer_compression("archive.tar.zst") == CompressionType.ZSTD
assert infer_compression("plain.json") is None
```

**Decompression bomb protection**

Every decompression path enforces a ceiling on the decompressed payload size (default 1 GB).  Override via the `INCORPORATOR_MAX_DECOMPRESSED_BYTES` environment variable when the workload legitimately exceeds 1 GB.

**When to reach for it**
Pass the string value directly to `export(compression=...)` (e.g. `compression="gz"`), or use the enum directly when you need to branch on format in a custom outflow sidecar.  The framework handles transparent decompression automatically during `incorp()` and `stream()` — you only need `CompressionType` explicitly when writing export or sidecar code.

**See also**
[Formats & Compression](./formats_and_compression.md) ·
[`FormatType`](#formattype)

---

## Exception hierarchy

All Incorporator exceptions inherit from `IncorporatorError`, which inherits from the built-in `Exception`.  Import from the top-level package:

```python
from incorporator import (
    IncorporatorError,
    IncorporatorFormatError,
    IncorporatorNetworkError,
    IncorporatorSchemaError,
)
```

| Class | Inherits from | When it is raised |
|---|---|---|
| `IncorporatorError` | `Exception` | Base class for all Incorporator exceptions; catch this to handle any framework error in one `except` block. |
| `IncorporatorFormatError` | `IncorporatorError` | Data cannot be parsed into a dict — malformed JSON, unreadable CSV/XML, archive extraction failure, decompression bomb exceeded. Raised by `export()` on write failures too. |
| `IncorporatorNetworkError` | `IncorporatorError` | The internal HTTP client exhausted all retries or encountered a non-retryable I/O error. |
| `IncorporatorSchemaError` | `IncorporatorError` | Dynamic Pydantic model compilation failed — typically a type conflict in the inferred schema during Dynamic Class Building. |

**When to reach for each**

- **`IncorporatorError`** — catch-all in a top-level error handler; use when any framework failure should funnel to the same recovery path.
- **`IncorporatorFormatError`** — catch in export sidecars, archive-reading code, or any path that reads user-supplied file data.  The message always includes the format, the source, and (for archives) the member name.
- **`IncorporatorNetworkError`** — catch in retry orchestrators that need to distinguish network failures from parse failures.  `IncorporatorList.rejects` carries structured `RejectEntry` records for per-source HTTP failures; `IncorporatorNetworkError` is raised only when the whole fetch leg fails (not just individual URLs).  v1.3.3: when `IncorporatorNetworkError` wraps an httpx exception via `.__cause__`, the corresponding `RejectEntry.is_url_traffic_error` is `True`.

**Retry behavior (v1.3.3).** The fetch layer applies a phase-aware classifier:
- Connect-phase errors (connection refused, DNS failure) are capped at ~3 attempts with short waits.
- Server-responded errors (5xx, 429) use up to 8 attempts with exponential backoff; `Retry-After` is honored during the in-flight retry loop itself (bounded to a 120s ceiling), not only surfaced on the final-failure reject path.
- Non-idempotent POST is not retried after a response is received.
- HTTP 408 (Request Timeout) and 425 (Too Early) are retryable.
- All other 4xx responses are permanent failures (single attempt).
- **`IncorporatorSchemaError`** — catch during development when iterating on `conv_dict` / `rec_path` kwargs; in production these should not occur unless the source schema changes unexpectedly.

---

## Optional-dependency introspection

**When to reach for it:** inspect which optional packages are available at runtime, generate install hints, or surface a machine-readable dependency manifest for CI health checks.

**Import**
```python
from incorporator import list_deps, install_hint, Category, DepInfo
```

### `list_deps() -> list[DepInfo]`

Returns one `DepInfo` record for every registered optional dependency, in declaration order. Modules are imported lazily inside the function — no circular-import risk.

### `install_hint(dep_name: str) -> str`

Returns a `pip install incorporator[<extra>]` string for the named package, or `pip install <dep_name>` when the package is not registered.

### `Category` enum

| Value | Extra | Purpose |
|---|---|---|
| `SPEEDUP` | `speedups` | Faster JSON / compression (orjson, lxml, cramjam) |
| `FORMAT` | `avro / parquet / xlsx` | Additional file-format support (fastavro, pyarrow, openpyxl) |
| `ORCHESTRATE` | `orchestrate` | CLI + Prefect integration (typer, prefect) |
| `PLATFORM_FIX` | `parquet` | Windows-only compat shims (tzdata) |

### `DepInfo` fields

| Field | Type | Description |
|---|---|---|
| `name` | `str` | PyPI package name |
| `extra` | `str` | `[project.optional-dependencies]` key |
| `category` | `Category` | Functional grouping |
| `description` | `str` | One-line human summary |
| `version_spec` | `str` | Minimum version constraint |
| `is_available` | `bool` | `True` when importable at runtime |
| `module` | `Any` | The imported module, or `None` |
| `platform_marker` | `str \| None` | PEP 508 marker when platform-gated |
| `include_in_all` | `bool` | Whether this dep is in the `[all]` extra |

**Runnable example — iterate missing deps with install hints**
```python
from incorporator import list_deps, install_hint

for dep in list_deps():
    if not dep.is_available:
        print(f"Missing: {dep.name}  →  {install_hint(dep.name)}")
```

---

## Where to Go Next

| Goal | Read |
|---|---|
| See a verb run end-to-end against a live API | [Tutorial 1 — First Steps + DX Inspector](../examples/01-first-steps/README.md) |
| Drain 10M rows without OOM (chunking mode) | [Streaming & Pagination Deep Dive](./streaming_and_pagination.md) |
| Orchestrate multiple verbs on a windowed schedule | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) and [Appendix — NASCAR Tideweaver](../examples/appendix/nascar-tideweaver/README.md) |
| Survive overnight runs with healthchecks + logs | [Deployment Guide](./deployment.md) |
| Generate the full pdoc HTML reference | [Library Reference](./library_reference.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/api_atlas.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
