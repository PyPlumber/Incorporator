# Changelog

All notable changes to Incorporator are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **Removed implicit per-host throttling** for `api.coingecko.com`,
  `pokeapi.co`, and `vpic.nhtsa.dot.gov`.  The framework now ships
  throttle-agnostic — calls to these hosts that previously auto-paced
  at 0.2 / 1.5 / 1.5 req/sec respectively will hit the
  `DEFAULT_RPS=15` fallback unless you explicitly opt in.

  Migration — pick one of:

  ```python
  # Option A: per-call kwarg (most local, easiest to discover).
  await Coin.incorp(
      inc_url="https://api.coingecko.com/api/v3/coins/markets",
      requests_per_second=0.2,
  )

  # Option B: register once at startup; every subsequent call against
  # the host respects the rate.
  from incorporator import register_host_throttle
  from incorporator.io.throttle import FixedIntervalThrottle

  register_host_throttle("api.coingecko.com", lambda: FixedIntervalThrottle(0.2))
  register_host_throttle("pokeapi.co", lambda: FixedIntervalThrottle(1.5))
  register_host_throttle("vpic.nhtsa.dot.gov", lambda: FixedIntervalThrottle(1.5))
  ```

  Per-host rationale (rates from the previous registry — re-verify
  against the provider's published docs when you copy):

  - `api.coingecko.com` → 0.2 r/s (12/min, comfortably under the 5-15/min anon ceiling).
  - `pokeapi.co` → 1.5 r/s (90/min, under the 100/min documented ceiling).
  - `vpic.nhtsa.dot.gov` → 1.5 r/s (under the 100-200/min documented ceiling).

- **Dropped `incorporator.io.fetch._KNOWN_API_RATE_LIMITS`** and
  **`_resolve_host_safe_rate`** — the backward-compat shims have no
  remaining callers.  Use `incorporator.io.throttle.known_host_rates()`
  for the live registry view.

- **Removed `watershed.json` legacy aliases** — the v1.2.0
  `dependency_mode` (top-level) and `"mode"` (per-edge) aliases for
  `gate_mode` are gone.  Passing them now raises ``ValueError`` with
  inline migration guidance instead of silently warning.

  Migration:

  ```json
  // Before v1.3.0:
  {"shape": "chain", "dependency_mode": "hard", "currents": [...]}

  // After v1.3.0:
  {"shape": "chain", "gate_mode": "hard", "currents": [...]}

  // Per-edge:
  {"shape": "custom", "edges": [{"from": "a", "to": "b", "mode": "hard"}]}
  // becomes:
  {"shape": "custom", "edges": [{"from": "a", "to": "b", "gate_mode": "hard"}]}
  ```

  The valid values (``"hard"`` / ``"soft"`` / ``"weir"``) are unchanged.

- **Narrowed `Gate` / `SurgeBarrier` / `Penstock` / `Spillway` method
  signatures.**  Strategies no longer accept the full ``Tideweaver``
  scheduler as their first argument — they read narrow ``GateContext``
  /  ``SurgeContext`` value types instead.  Tightens the FlowControl ↔
  scheduler boundary so subclasses can be unit-tested without a real
  scheduler.  Affected:

  - ``Gate.gate_reason(scheduler, dependent, up_name, now)`` →
    ``Gate.gate_reason(ctx: GateContext)``
  - ``SurgeBarrier.is_tripped(scheduler, dependent, up_name, now)`` →
    ``SurgeBarrier.is_tripped(ctx: SurgeContext)``
  - ``Penstock.consume_reason(scheduler, edge_state, flow, now)`` →
    ``Penstock.consume_reason(edge_state, flow, now)``
  - ``Spillway.overflow(scheduler, edge, displaced_wave)`` →
    ``Spillway.overflow(edge, displaced_wave, overflow_count)``
  - ``SignalPenstock.rate_fn(scheduler, edge_state, now) -> float`` →
    ``rate_fn(edge_state, now) -> float``

  Most users never override these; the change is invisible.  Users
  with custom Gate/Penstock/Spillway subclasses or ``rate_fn``
  callables update their signatures (drop the first scheduler arg).

### Added

- **`register_host_throttle` promoted to package top-level.**
  `from incorporator import register_host_throttle` works; the
  submodule path `incorporator.io.throttle.register_host_throttle`
  continues to work and is the same callable.  New entry in
  [`docs/api_atlas.md`](docs/api_atlas.md) walks the registration API
  side-by-side with the existing `resolve_throttle` resolver.

### Internal

- **`incorporator/observability/tideweaver/architect.py`** routes
  Penstock tier-1 (host-aware) recommendations through the live
  `known_host_rates()` view rather than the import-time
  `_KNOWN_API_RATE_LIMITS` shim.  Behavior unchanged for users who
  register hosts; tier-1 falls silent for users who don't.

## [1.2.0] - 2026-05-21

### Added — Canal toolkit (per-edge `FlowControl` primitives)

Five orthogonal flow-control primitives composable per edge via
`FlowControl`.  Each is a Pydantic strategy hierarchy and serialises
into `watershed.json` via discriminated unions.

- **`Gate` hierarchy** — `HardLock` (default), `SoftPass`, `Weir`.
  Pass/hold decision per upstream edge.  `Watershed.chain/diamond/fanout(gate_mode=...)`
  shorthand maps `"hard"`/`"soft"`/`"weir"` to the right `Gate`; `Edge(gate_mode=)`
  same on the explicit-edge path.  Mutually exclusive with the `flow=`
  full-dict form.  `Weir` is the new third mode: gates on wave freshness
  without blocking on in-flight upstream and without triggering skip-ahead.
- **`SurgeBarrier`** — conditional override when an upstream is running
  long.  Three actions: `"skip"` (skip reason `skip_ahead`), `"halt"`
  (skip reason `surge_halted`), `"bypass"` (fire ignoring this edge's
  gate **and** penstock).  Houses `threshold_multiple` (was
  `skip_threshold` — see **Breaking** below).
- **`Penstock` hierarchy** — edge-level rate limiting.  Five strategies:
  `SustainedPenstock` (fixed rate), `BurstPenstock` (token bucket),
  `WindowPenstock` (sliding-window cap), `BackpressurePenstock` (rate
  interpolates with reservoir fullness — `max_rate` when empty →
  `min_rate` when full), `SignalPenstock` (user callable
  `rate_fn(scheduler, edge_state, now) -> float`).  Limited consumers
  surface skip reason `penstock_limited`.
- **`Reservoir(depth=N)`** — per-edge FIFO buffer of recent waves.
  Default `depth=1` keeps only the most recent.  Read by
  `BackpressurePenstock` for fullness and exposed to user code for
  N-deep history.
- **`Spillway` hierarchy** — overflow handler when the reservoir is
  full.  Three: `DropOldest` (default, silent), `RaiseOverflow` (one
  WARNING log per displacement), `ExportToArchive(archive_cls=...)`
  (each displaced wave's instances append to
  `archive_cls._spillway_backlog`).

### Added — Other

- **`Current.phase_offset_sec`** — green-wave coordination.  Skips the
  first N seconds of a run with skip reason `phase_offset`.  Stages
  offsets across parallel currents to spread work without changing
  their intervals.
- **`watershed.json` loader** for the full canal toolkit —
  discriminated unions for `gate` / `penstock` / `reservoir` /
  `spillway` / `surge_barrier`; `gate_mode` shorthand on the JSON
  `Edge` form too; string-reference resolution for
  `SignalPenstock.rate_fn` and `ExportToArchive.archive_cls` via
  `module:attr` syntax.
- **14 new public names** exported from
  `incorporator.observability.tideweaver`: `FlowControl`, `Gate`,
  `HardLock`, `SoftPass`, `Weir`, `SurgeBarrier`, `Penstock`,
  `SustainedPenstock`, `BurstPenstock`, `WindowPenstock`,
  `BackpressurePenstock`, `SignalPenstock`, `Reservoir`, `Spillway`,
  `DropOldest`, `RaiseOverflow`, `ExportToArchive`.

### Changed

- **One `httpx.AsyncClient` pooled per HTTP-config signature across
  drains.**  Previously each drain (and each chunk within a drain in
  `chunked.py`) built its own client.  The pool keys by `(timeout,
  verify, http2, follow_redirects, max_connections,
  max_keepalive_connections, read_timeout)` — significant
  connection-reuse improvement for multi-stream Tideweaver pipelines
  hitting the same backend.  `chunked.py` also skips client
  construction entirely for file-mode + pooled drains.
- **`_outflow.flush()` snapshot attribute renamed** from
  `_fjord_snapshot` to `_tideweaver_snapshot` on output classes.  The
  snapshot serves both the legacy fjord daemon and the new Tideweaver
  `Fjord` flush — the old name was misleading.
- **Routing tests** (`tests/test_tideweaver_routing_*.py`) converted
  to use the new `Weir` gate where they previously workarounded the
  old `dependency_mode="soft"` via `interval` tweaks or
  file-rereads.

### Fixed

- **`SurgeBarrier(action="bypass")` no longer charges the Penstock.**
  The scheduler's `_tick_wrapper.finally` block previously debited the
  `BurstPenstock` bucket and appended to the `WindowPenstock`
  `window_log` for every upstream edge unconditionally — including
  bypassed ones, violating the documented "bypass ignores gate AND
  penstock" contract.  `_gate_reason` now returns the set of bypassed
  upstreams and threads it through to the wrapper, which skips
  penstock post-consumption for those edges.
- **`BackpressurePenstock(min_rate=10, max_rate=2)` now raises
  `ValidationError` at construction.**  Previously each field was
  validated `gt=0` individually but no cross-field validator enforced
  the ordering, so swapped values silently inverted the curve (a full
  reservoir got a *higher* effective rate than an empty one).  Equal
  values are also rejected as degenerate (no backpressure curve).
- **`Fjord` output classes now carry `_tideweaver_snapshot` properly**
  (was: snapshot parking missed the Fjord case, causing downstream
  reservoir pushes to read from the live `inc_dict` instead of the
  parked strong-ref list).

### Breaking

- **`skip_threshold` moved from `Current` to per-edge `SurgeBarrier`.**
  Previously `Stream/Fjord/Export(..., skip_threshold=N)` (and the
  matching `watershed.json` per-current key) configured the surge
  threshold multiplier.  It is now `SurgeBarrier.threshold_multiple`,
  composed into `FlowControl.surge_barrier`, scoped per edge (one
  dependent can declare different surge tolerances per upstream).
  The JSON loader raises an explicit `ValueError` with migration
  instructions when the old key is present at the current level.
  Python migration: replace
  ```python
  Stream(name="b", cls=B, interval=0.1, skip_threshold=2.0)
  ```
  with the edge-scoped form
  ```python
  Edge(
      from_name="a",
      to_name="b",
      flow=FlowControl(
          surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="skip"),
      ),
  )
  ```

### Internal

- **CI now triggers on the `workflow` branch** (was: `main` + dead
  `refactor-ai` only).  The 14 commits of canal-toolkit work landed
  on `workflow` and slipped past CI during development.
- **`CONTRIBUTING.md`** drops `tests/` from the ruff + black quickstart
  commands.  Running ruff against `tests/` overrides
  `[tool.ruff].exclude` and produces a ~1000-line `S101` (assert)
  false-positive storm.

## [1.1.3] - 2026-05-16

### Added
- **Tideweaver orchestration layer** — graph-based orchestration over
  `stream()` / fjord-flush / `export()`. Build a `Watershed` (time window +
  named `Current` nodes + dependency edges), hand it to `Tideweaver`, run.
  New names: `Tideweaver`, `Watershed`, `Current` (subclasses `Stream` /
  `Fjord` / `Export`), `Tide` (per-pass log record); existing `Wave`
  unchanged. Shape constructors `parallel` / `chain` / `fanout` / `diamond`
  plus `custom` with explicit `edges`. Hard/soft dep gating, skip-ahead,
  graceful drain at window close, per-current `on_error` (`restart` /
  `isolate` / `fail_watershed`). See `examples/11-tideweaver/README.md`.
- **`incorporator tideweaver run|validate` CLI** with declarative
  `watershed.json` config (same env-var interpolation + token resolution
  as `stream` / `fjord`). `run` pre-flights via the same validator.
- **`incorporator init --type tideweaver`** scaffold — generates a
  `watershed.json` (diamond shape) + paired `outflow.py`.
- **`examples/nascar_watershed.json`** + **`examples/tideweaver_code/race_outflow.py`**
  — on-disk sample for the CLI smoke-test path.
- **Public exports** for the seven new names (`Tideweaver`, `Watershed`,
  `Current`, `Stream`, `Fjord`, `Export`, `Tide`).

### Changed
- **Curriculum renumbered to T1–T11** — four appendix tutorials promoted
  into the main path. Folder renames:
  - `examples/appendix/data-lake-pivot/` → `examples/02-data-lake-pivot/`
  - `examples/02-universal-formats/` → `examples/03-universal-formats/`
  - `examples/appendix/xml-post-audit/` → `examples/04-xml-post-audit/`
  - `examples/03-parent-child-drilling/` → `examples/05-parent-child-drilling/`
  - `examples/appendix/spacex-launches/` → `examples/06-spacex-launches/`
  - `examples/04-stateful-refresh/` → `examples/07-stateful-refresh/`
  - `examples/05-streaming-daemon/` → `examples/08-streaming-daemon/`
  - `examples/appendix/nascar-fantasy-fjord/` → `examples/09-nascar-fantasy-fjord/`
  - `examples/06-multi-source-fjord/` → `examples/10-multi-source-fjord/`
  - `examples/07-tideweaver/` → `examples/11-tideweaver/`
  Matching `docs/N_*.md` redirect stubs renumbered. Remaining appendices
  (`pokeapi-etl`, `crypto-graph-mapping`, `nascar-tideweaver`,
  `tideweaver-parquet-snapshots`, `tideweaver-vs-prefect`) stay in
  `examples/appendix/` as optional side-quests.
- **`cli/validate.py`** auto-detects watershed configs (top-level `window`
  + `shape` keys); `tideweaver` added to `ConfigType` and `--type`. No
  change to `stream` / `fjord` validation.
- **`observability/pipeline/_outflow.py`** factors a shared async `flush()`
  generator yielding `(derived_name, row_count, error)` per output class.
  Used by both `_outflow_daemon` and `Tideweaver._tick_fjord`; removes ~50
  lines of duplication. Legacy wave-emission shape preserved.
- **CLI help text** updated across `init`, `validate`, and the new
  `tideweaver` sub-app for consistency across all three pipeline types.
- **Docs pass** — `docs/cli_and_configuration.md` gains §9 for `tideweaver`
  and a "When to Reach For" table row; `docs/library_reference.md` adds an
  `incorporator.observability.tideweaver` bullet; README adds a Tideweaver
  subsection under "The Verbs"; `docs/installation.md` and
  `docs/deployment.md` mention the new sub-command.
- **Tick → wave prose drift** from the earlier rename cleaned up in
  `examples/07-stateful-refresh/README.md` and
  `examples/08-streaming-daemon/README.md`.
- **`stream(stateful_polling=True)` collapsed into a thin shim** over
  `fjord()` (`observability/pipeline/_stateful_shim.py`). Two engines
  (chunking + stateful) become one (chunking) plus a single-source-fjord
  shim. Wave-contract preserved: same `operation` strings, same
  `chunk_index` cadence, same instance identity across refreshes.
  `stateful_polling=True` continues to work as documented.
- **Typeless-format reads auto-coerce via `_schema_union`.** When a class
  has been incorp'd from a typed source (JSON / NDJSON / Parquet / SQLite
  / Avro) and is then read from a typeless format (CSV / TSV / PSV),
  `build_instances()` synthesises `inc()` converters for fields not in
  `conv_dict`. User-supplied `conv_dict` entries still win on conflict;
  asymmetry is one-way (coerce towards richer types, never towards `str`).
  See `incorporator.schema.factory._expand_conv_dict_with_schema_union`.
- **Examples folder reorganised** into per-tutorial directories with
  co-located docs and isolated `out/` dirs
  (`examples/02-universal-formats/{universal_formats.py, README.md, out/}`
  etc.), replacing the flat-script-root layout.

### Fixed
- **`fetch_concurrent_payloads` no longer cancels siblings on non-429
  errors.** Both gather sites (Path A batched-with-delay, Path B
  sliding-window workers) now use `return_exceptions=True` and route
  failures through `failed_sources` the same way the 429 path always did.
  (Behavior change: non-429 HTTP errors now produce a warning + DLQ entry
  per failed source, never a batch abort.) Matches existing 429 / 5xx
  semantics and the `LoggedIncorporator` + `get_error` DLQ pattern.
- **`incorp(inc_file=Path(...))` silently returned `[]`.**
  `_normalize_source_list` only handled `str` / `list`; a single
  `pathlib.Path` (or any `os.PathLike`) fell through to the `payload_list`
  branch. Now coerces via `os.fspath` at every entry point. Affected
  tutorial 2 (CSV round-trip) and the XML-post-audit appendix.
- **T5 chunking demo errored on default `refresh_params`.** Paginated
  transient instances have no stable origin URL; tutorial code now opts
  out with `refresh_params=None`. Parameter documented in the T5 chunking
  snippet.
- **T3 defensive `getattr` guards** for variable-shape CoinGecko
  `/coins/{id}` responses (missing `links`, `null` `genesis_date`).
  Pre-existing pathology; no framework change.
- **T4 swapped to `api.binance.us`** to bypass `api.binance.com`'s 451
  geo-block in US / UK / Singapore. Same v3 endpoint shape, ~600 listed
  pairs vs ~1,900 on `.com`. Swap back outside those regions for full
  coverage.

### Fixed (diagnostic ergonomics)
- **Clearer Seed Error wave from `fjord()`.** When `_seed_one_source`
  raises, the emitted `Wave.failed_sources` entry now names the source
  class and exception type, and — for the common `KeyError` raised
  inside `inflow(state)` — points directly at the missing peer with a
  concrete fix suggestion (`state.get(...)` or `depends_on=[...]`).
  Previously a bare `KeyError('Track')` stringified to just `'Track'`
  in the failure message, leaving the user no signal about which
  source raised or what stage failed.  Helper:
  `incorporator.observability.pipeline.fjord._format_seed_error`.
- **Bare-class data-loss warning at outflow flush.** `flush()` prefers
  a user-pre-declared subclass when the outflow module exposes one
  with the matching `__name__`.  A "bare" declaration like
  `class Race(Incorporator): pass` adds no fields beyond the base
  three; under Pydantic V2's default `extra='ignore'`, every row
  field is silently dropped on `model_validate` (silent data loss).
  A one-time WARNING per class identity now surfaces the issue with
  a fix suggestion (declare the fields explicitly or delete the
  class so `infer_dynamic_schema` takes over).  Helper:
  `incorporator.observability.pipeline._outflow._warn_on_bare_user_class`.
- **`analyze_error()` inspector survives cp1252 stdout.** The error
  inspector's emoji prefixes (`🚨` / `💡` / `👉`) used to crash mid-
  diagnosis on Windows cp1252 console with `UnicodeEncodeError`,
  hiding the actual error message under a secondary traceback.
  A local `p()` helper in `analyze_error()` now catches the encode
  error and falls back to ASCII (emojis become `?`) so the
  diagnosis still lands.  Set `PYTHONIOENCODING=utf-8` for the
  prettier rendering.

### Added (defaults change for three specific hosts)
- **Host-aware rate-limit registry.** The HTTP engine consults an internal
  `_KNOWN_API_RATE_LIMITS` table when the caller does not pass
  `requests_per_second`:
  - `api.coingecko.com` → 0.2 req/sec (12/min, under the 5–15/min free-tier
    ceiling).
  - `pokeapi.co` → 1.5 req/sec (90/min, under the 100/min ceiling).
  - `vpic.nhtsa.dot.gov` → 1.5 req/sec (90/min, under NHTSA's 100–200/min
    ceiling). Method-agnostic — applies to GET and POST (the xml-post-audit
    appendix's `DecodeVINValuesBatch` POST shares the same bucket).
  Caller-supplied `requests_per_second` always wins; unknown hosts keep the
  15 req/sec default. INFO log line names the applied rate when the
  registry fires. (Behavior change for callers hitting CoinGecko / PokeAPI
  / NHTSA vPIC without explicit throttle: e.g. CoinGecko 10-source drill
  goes from ~700 ms to ~50 s.)
- **`depends_on: List[str]` on fjord source entries** — declares which peer
  classes a source's `inflow(state)` reads. When any entry declares it,
  the seed runs in topological tiers (parallel within tier via
  `asyncio.gather`, later tiers wait on earlier `state[...]`). Unknown
  names raise `ValueError` at engine entry. No `depends_on` anywhere
  falls through to the existing sequential declaration-order seed
  (bit-identical to pre-feature behaviour). Mixed 6-source watershed
  with 2 ordered no longer pays serial cost for the other 4.

## [1.1.2] - 2026-05-15

### Changed
- **Docstring polish pass** — all public docstrings now carry Google-style
  `Args:` / `Returns:` / `Yields:` sections. Covers converter predicates
  (`is_garbage_value`, `parses_as_datetime`, `parses_as_int`,
  `parses_as_float`), extractor helpers (`link_to_list`, `sum_attributes`,
  `as_list`), `LoggedIncorporator` verbs (`refresh`, `export`, `stream`,
  `fjord`), and all 8 `paginate()` async-generator methods. `display()`
  and `refresh()` return-type descriptions corrected.
- **pyproject.toml classifiers** — dropped Python 3.10 / 3.12; CI tests
  3.9 / 3.11 / 3.13.
- **Project description** rewritten ("Schema-free ETL mapper…").
- **Docs / examples** — stale "v2.0" version refs, dead legacy filename
  refs, and unexplained advanced-pattern lead-ins resolved.
- **`SECURITY.md`** supported-versions table updated to v1.1.x; stale
  parameter name (`code_file=` → `outflow=`) corrected.
- **`CONTRIBUTING.md`** test count (521+), mypy file count (47), and
  Python version list brought up to date.

## [1.1.1] - 2026-05-14

### Fixed
- **Logger `atexit` AttributeError on Python 3.11+** — `_cleanup_listeners()`
  no longer raises when a registered `QueueListener` was already stopped on
  another thread. Python 3.11's stdlib clears `_thread` to `None` after the
  first `.stop()`, and the second call would raise
  `AttributeError: 'NoneType' object has no attribute 'join'` in the atexit
  hook. Guarded all three stop() sites (atexit hook, eviction path, test fixture).
- **ISO datetimes with compact `+0000` offset on Python 3.9/3.10** —
  `parses_as_datetime` / `_fallback_date` now accept the no-colon timezone
  form. Was silently falling back to the user's default on 3.9/3.10 because
  `datetime.fromisoformat()` only learned the compact form in 3.11.
- **pyarrow ORC reader on Windows** — `[parquet]` extra now installs
  `tzdata>=2024.1` on Windows, where pyarrow's hardcoded
  `/usr/share/zoneinfo` lookup would otherwise fail with
  `ORC Read Error: Time zone file /usr/share/zoneinfo/UTC does not exist`.
  Linux/macOS installs are unchanged.

### Changed
- Bumped CI actions to `actions/checkout@v6` and `actions/setup-python@v6`
  for Node.js 24 compatibility (June 2026 deadline). No user-facing impact.

## [1.1.0] - 2026-05-14

### Added — Continuous Integration
- **`.github/workflows/ci.yml`** — three-job GitHub Actions workflow (lint / typecheck / test) running on every PR and push to `main` / `refactor-ai`. The test matrix is 3 Pythons (3.9 / 3.11 / 3.13) × 2 OSes (Ubuntu + Windows) = 6 parallel cells. Total wall-clock ~2-3 minutes. CI badge surfaced in the README's CODE QUALITY block.
- **Branch-protection convention** documented in `CONTRIBUTING.md` — maintainer click-through to require `lint`, `typecheck`, and the 6 `test` cells before merging to `main`.

### Changed — `AuditResult` renamed to `Wave`
- **`AuditResult` → `Wave`** on the public surface. The per-tick value
  yielded by `stream()` and `fjord()` is now named `Wave`, matching the
  framework's fjord / inflow / outflow vocabulary. Imported as
  `from incorporator import Wave`. No deprecation alias; the old name
  is gone.
- **Log record key `"audit"` → `"wave"`** in the JSON-line log format.
  When `LoggedIncorporator` is enabled, the structured Pydantic dump
  appears under `record["wave"]` on disk. Downstream `jq` / log
  aggregator scripts that read `.audit` need to switch to `.wave`.
- Internal renames for consistency: `_route_audit_to_log` →
  `_route_wave_to_log`, `_emit_audit` → `_emit_wave`,
  `audit_queue` → `wave_queue`.

### Added — `inflow` / `outflow` sidecar files & `@name` references
- **`inflow=` kwarg** on `incorp()` / `refresh()` / `stream()`. Points at
  a Python sidecar (`inflow.py`) holding user-defined helpers — `calc`
  reducers, custom converters, paginator instances, anything the
  trinity's `conv_dict` / `inc_page` kwargs need but JSON can't carry
  directly. Imports happen **once** (cached via `sys.modules`); the
  CLI's token resolver extends its allow-list with the module's public
  symbols so JSON tokens can reference user functions by bare name.
- **`outflow=` kwarg** on `fjord()`, `stream()`, and `export()`. The
  canonical sidecar-file parameter (replaces the never-shipped
  `code_file=` working name). On `stream()`, `outflow=` requires
  `stateful_polling=True` — chunking mode releases per-chunk state and
  has no persistent registry for a user-defined subclass to attach to.
- **`@name` sigil syntax** in `pipeline.json`. Bare-name references to
  inflow symbols (`"inc_page": "@launches_pager"`) eliminate JSON-escape
  ugliness entirely. Coexists with call-grammar tokens
  (`"inc_page": "NextUrlPaginator('next')"`) — mix-and-match.
- **`calc`, `calc_all`, `link_to`, `link_to_list`** added to the token
  resolver's allow-list. They now resolve when `inflow.py` provides the
  user callable / registry referenced in the first arg.
- **`incorporator init --with-inflow`** flag — scaffolds an `inflow.py`
  stub alongside `pipeline.json`. Off by default for `--type stream`
  (keeps minimal cases minimal).

### Added — CLI & Production Deployment
- **`incorporator init / validate / stream / fjord`** CLI subcommands. Drives the same engines from a `pipeline.json` — no Python wrapper required for single- or multi-source ETLs.
- **Env-var + Secrets-file interpolation in `pipeline.json`**: `${API_KEY}`, `${VAR:-default}`, `${VAR:?required}`, and `${file:/run/secrets/api_key}` for Docker / Kubernetes Secrets mounts.
- **`--json-output` flag** on `stream` / `fjord` for machine-readable NDJSON Wave lines (one per chunk).
- **`--heartbeat-file PATH` flag** + Docker `HEALTHCHECK` so orchestrators can detect a hung daemon and restart automatically.
- **SIGTERM graceful shutdown** — `docker stop` / `kubectl delete pod` drain in-flight daemons cleanly instead of falling through to KeyboardInterrupt.
- **`docker-compose.yml` + `.env.example`** shipped with the repo for a 5-minute production deployment.
- **`LoggedIncorporator.fjord` override** mirroring `stream`'s structured Wave routing into the queued JSON log files.

### Added — New Format Handlers
- **Apache Parquet** (`[parquet]` extra → `pyarrow`). Columnar format for data lakes / warehouses, with streaming row-group writes (O(1) memory).
- **Feather / Apache Arrow IPC** (`[parquet]` extra, shares the pyarrow install). Zero-copy columnar interchange.
- **Apache ORC** (`[parquet]` extra). Hadoop / Hive columnar format.
- **Excel `.xlsx`** (`[xlsx]` extra → `openpyxl`, ~250 KB).
- **HTML table parser** (`[speedups]` extra → `lxml`). Parse-only — closes the `pandas.read_html` gap.

### Added — Performance Optimisations (automatic, no code changes)
- **HTTP/2 multiplexing** in `httpx.AsyncClient` — one TCP/TLS connection carries every concurrent request.
- **Long-lived connection pool** decoupled from worker count (`max_keepalive_connections=10, max_connections=concurrency_limit`).
- **LRU `SCHEMA_REGISTRY`** via `collections.OrderedDict` — hot schemas stay; cold ones age off the front. No more cache thrash in long daemon runs.
- **Batched `Pydantic.model_validate`** in 1000-row chunks instead of per-row `**kwargs` unpack.
- **`asyncio.to_thread` for user `outflow_fn`** — heavy joins in `fjord()` no longer block refresh/export daemons on other sources.
- **In-place columnar parse** with `pyarrow.compute` vectorised JSON-prefix scan for Parquet / Feather / ORC. Parquet parse: 159k → 200k rows/sec (+26%); Feather: 165k → 214k (+30%).
- **Per-row key sanitisation hoisted** out of Avro and XML write loops. Avro write: 43k → 62k rows/sec (+43%).
- **ETL loop inversion** (rows-outer, keys-inner) for CPU cache locality on large rename/exclusion passes.
- **Stratified schema sampling** (up to 100 evenly-spaced records) instead of `data[:50]` so rare field types are more likely to be discovered.
- Removed redundant `gc.collect()` from the chunked engine and unconditional `dict.copy()` from the single-shot path.

### Added — Test & Benchmark Coverage
- **28 benchmark tests** covering write + parse throughput for every registered format handler (JSON, NDJSON, CSV/TSV/PSV, XML, HTML, SQLite, Parquet, Feather, ORC, Avro, XLSX).
- **331 standard tests passing** under mypy strict, ruff, and black.

### Added — Architecture
- **`fjord()` method** on `Incorporator` for multi-source stateful streaming. Fans out N concurrent sources, fuses through a user-defined `outflow(state)` function, exports the combined output. Output class derived from the `outflow` file's stem — no class to declare.
- **`incorporator/cli/` subpackage** (was a single `cli.py`). Cleaner split between `validate`, `scaffold`, `envexpand`.
- **`schema/factory.py`** module extracted from `base.py` — `child_incorp` and `build_instances` now testable in isolation.

### Changed
- **README rewritten** to a verb-forward structure. All 7 verbs (`incorp / test / refresh / export / stream / fjord / display`) have idiomatic examples. CLI/Docker positioned as the natural production extension of `stream()` / `fjord()`.
- **`pdoc`-generated reference renamed** from "API Reference" to "Library Reference" — Incorporator consumes HTTP APIs, so "API Reference" was semantically misleading.
- **All 5 tutorials** got a "Run it from the CLI" addendum showing the equivalent `pipeline.json`.

### Fixed
- **Broken `pip install incorporator[cramjam]` references** in `formats_and_compression.md` and `installation.md`. The `[cramjam]` extra does not exist — `cramjam` is bundled inside `[speedups]` alongside `orjson` and `lxml`. Anyone copy-pasting from the old docs hit a pip install error.
- **README `[all]` description** corrected — `[all]` deliberately excludes `[parquet]` (pyarrow is ~30 MB) and `[docs]`. Old copy claimed it installed "the complete Enterprise Big Data suite", which was false.

## [1.0.8] and earlier

See the git history for changes prior to the production-readiness release. Highlights from earlier versions:

- Apache Avro support via `fastavro` (`[avro]` extra)
- Cramjam ≥2.x compatibility for compression
- `_inspector` JIT API profiler (the `test()` verb)
- Coverage uplift from 77% → 85%
- Black formatting cross-check alongside ruff
