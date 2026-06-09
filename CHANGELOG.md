# Changelog

All notable changes to Incorporator are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.3] - 2026-06-08

### Fixed

- **Phase-aware retry classifier — real async-path fix** (`incorporator/io/fetch.py`,
  `incorporator/observability/tideweaver/_retry_defaults.py`): the network-retry
  cap introduced in the prior commit was broken in the real async path.  A
  dead host ran all 8 attempts (~74 s measured) because `retry_if_exception`
  passes only the exception to its predicate — any attempt-count closure over
  `retrying.statistics` is unreliable at predicate-fire time.  Root fix: the
  attempt cap now lives in `_make_http_stop(method)`, a stop callable that reads
  `retry_state.attempt_number` and `retry_state.outcome.exception()` directly.
  A companion `_make_http_wait(method)` dispatches short bounded backoffs
  (`min=0.25 s`, `max=3 s`) for network-class errors vs. the existing slow
  exponential for server-responded (5xx/429) errors, eliminating ~58 s of
  excess sleeping on a dead host.  The unusable `attempt_num` parameter is
  dropped from `_is_retryable_error` (type + idempotency classification only;
  attempt bounding moved to stop).  Measured result: ConnectError / ConnectTimeout
  / ReadTimeout-GET exhaust in exactly `_HTTP_NETWORK_RETRY_STOP=3` total
  attempts with total sleep ≤ `3 × 3 = 9 s`; 5xx=8 attempts, 429=8 attempts,
  404=1 attempt, POST-ReadTimeout=1 attempt are all unchanged.
- **HTTP 408 / 425 now retry like transient server errors** (`incorporator/io/fetch.py`):
  `_is_retryable_status` now includes `408 Request Timeout` and `425 Too Early`
  alongside existing 5xx and 429 codes.  Both are capped at `_HTTP_INNER_STOP = 8`
  attempts via the same stop callable used by 5xx; exhaustion raises as
  `IncorporatorNetworkError` (same path as exhausted 5xx, no change to the
  soft-skip vs raise policy).
- **Empty-parent child-drill short-circuit** (`incorporator/schema/factory.py`):
  `child_incorp` now returns an empty `IncorporatorList` immediately when the
  parent dataset yields zero child IDs, without issuing any HTTP request.
  Previously the unsubstituted `{}` template URL was dispatched, producing bogus
  requests, retry storms, `{}` output rows, and misattributed warnings.
- **File-mode and paginator telemetry guard** (`incorporator/io/fetch.py`): the
  `is_file_mode` and `inc_page` branches in `_process_single_source` now reset
  `_last_bytes_downloaded`, `_last_http_fetch_time_sec`, and
  `_last_bytes_processed` to `None`, preventing stale HTTP telemetry from
  bleeding into subsequent non-HTTP chunks on the same class.
- **AIMD parse-only steering** (`incorporator/observability/pipeline/chunked.py`): online AIMD ring now uses the parse-only remainder (`processing_time_sec - http_fetch_time_sec`) for HTTP sources, matching the offline `_tune_chunk_size` signal; file/SQLite sources fall back to end-to-end correctly; target window re-derived from `_PARSE_TOO_FAST_P50=0.001s` / `_PARSE_MEMORY_P99=0.100s`.
- **`_tune_chunk_size` edge hardening** (`incorporator/observability/tideweaver/architect.py`): negative parse-time remainders are clamped to `max(0.0, ...)` before percentile computation (mirrors the online AIMD clamp in `chunked.py`); mixed HTTP/file source groups where HTTP waves are the majority (> `_HTTP_MAJORITY_FRACTION=0.5`) now steer on the parse-only signal instead of falling back to coarse end-to-end thresholds.

### Added

- **Architect wire-bytes / HTTP-latency telemetry** (`incorporator/observability/tideweaver/architect.py`,
  `incorporator/tools/inspector.py`): three enhancements that close the feedback loop between the
  E′ per-Wave telemetry and the offline architect tuner.

  - **(a) Probe seeding**: `ResponseMeta` gains two optional fields — `wire_bytes: int | None` and
    `http_latency_sec: float | None`.  `_probe_one` in `architect.py` backfills them after
    `test()` returns by reading the probe class's `_last_bytes_downloaded` and
    `_last_http_fetch_time_sec` ClassVars (populated by `fetch.py` for HTTP sources; reset to
    `None` for file-mode so file-probe fields stay `None` correctly).

  - **(b) `_tune_penstock_rate` byte-rate awareness**: the function gains an optional `waves`
    parameter.  When supplied, it builds a host → bytes/sec map by grouping waves on the
    hostname extracted from `Wave.source_url` and appends the measured throughput to the rationale
    string for HTTP-reject host groups.  The existing req/s logic is unchanged when
    `bytes_downloaded` is `None` on all matching waves (file-mode / pre-telemetry fallback).
    Reject-to-wave linkage is keyed on hostname rather than `wave_index`, because HTTP-layer
    rejects carry `wave_index=None` and canal-layer rejects set it to a tide counter (not a
    `Wave.chunk_index`); the host-keyed join is the only reliable correlation.

  - **(c) New `_tune_http_timeout` rule**: groups waves by `source_url`, computes p99 HTTP
    latency for each group (skips non-HTTP sources where all `http_fetch_time_sec` are `None`),
    and emits **HIGH** "raise timeout" / **LOW** "lower timeout" / **INFO** "well-sized" hints
    relative to the configured timeout (passed via `tune(timeout=...)`) or the
    `_DEFAULT_TIMEOUT_PROXY_SEC = 5.0` module constant when unspecified.
    Thresholds: `_TIMEOUT_PROXIMITY_FACTOR = 0.85` (p99 ≥ 85% of timeout = SRE headroom
    breach) and `_TIMEOUT_HEADROOM_FACTOR = 3.0` (timeout > 3× p99 = fail-fast budget wasted),
    both module-level constants with derivation citations.  `tune()` accepts a new `timeout`
    keyword argument forwarded to `_tune_http_timeout`; when omitted, `_DEFAULT_TIMEOUT_PROXY_SEC`
    is used and `current_value` on timeout hints is `None`.  Registered in `tune()` alongside
    `_tune_chunk_size` (both consume waves).

### Changed

Observability / log-surface changes only — rendered diagnostic strings, warning
attribution, and additive structured fields. None of these change execution,
data flow, or the result of any verb; existing pipelines run unchanged.

- **`RejectEntry.__str__`** now renders a fully-decorated form:
  `"{error_kind}: {source}"` + ` ({from_name}->{to_name})` when `from_name`
  is set + ` [HTTP {status_code}]` when `status_code` is set + ` — {message[:120]}`
  when message is present and distinct from source.  All output is cp1252-safe.
  Anything that scraped the old `"error_kind: message"` string (log parsers,
  custom formatters) will see the richer form.
- **Partial-data `UserWarning`** now fires from the `await incorp()` /
  `await refresh()` call site (via `warnings.warn` in `base.py` after the
  `asyncio.to_thread` join) instead of from inside the worker thread
  (`schema/factory.py`).  The attributed source frame is now the user's
  `incorp()`/`refresh()` call site — no longer `thread.py` internals.
- **`_route_reject_to_log`** (`observability/logger.py`) now uses `str(reject)`
  as its sole message renderer; the prior three-rendering hand-assembly is removed.
- **`cli/tideweaver.py` source-failure summary** now renders each failure via
  `str(reject)` (structured detail) rather than listing only the source name.
- **`incorporator/io/fetch.py`** all five error log sites now call
  `logger.warning(str(reject))` after building the `RejectEntry`; the 429 tip
  is a separate `logger.info` call with no emoji.
- **`incorporator/schema/factory.py`** no longer emits a `warnings.warn` inside
  the `asyncio.to_thread` worker (the old call that resolved to `thread.py`).
- **`Tide` model shape** gained the `session: str | None` field (default
  `None`), so `model_dump()` output now includes a `session` key.
- **`RejectEntry` model shape** gained the `session: str | None` field (default
  `None`); same `model_dump()` note as `Tide`.
- **`Watershed` model shape** gained the `name: str | None` field (default
  `None`).  `extra="forbid"` still rejects unregistered keys; the field
  round-trips through `watershed.json`.
- **`Tideweaver.__init__`** gained the `logger_name: str | None = None`
  keyword-only argument (additive; existing constructions are unaffected).

### Added

- `_format_reject_warning(rejects, cap=5)` module-level helper in
  `incorporator/rejects.py` — count headline + up to `cap` rendered entries +
  overflow line.  Used by `base.py`'s warning emission.
- `_build_canal_reject(...)` module-level helper in
  `incorporator/observability/tideweaver/scheduler.py` — single
  `model_construct` site for all five canal skip kinds.
- `_route_scheduler_event_to_log(logger_name, event_type, current_name, detail, ...)` in
  `incorporator/observability/logger.py` — routes Tideweaver scheduler
  diagnostics (isolated tick failures, parked ticks, empty output, empty parent
  snapshots, fjord flush failures) to the session's structured error log under
  a top-level `"scheduler_event"` key.
- `LoggedTideweaver.get_scheduler_events(logger_name)` — async classmethod
  that reads the session `error.log`, filters for `"scheduler_event"` records,
  and returns them sorted ascending by `tide_number`.  Returns `[]` when no
  matching records exist.  Completes the three-reader surface alongside
  `get_tides(logger_name)` and `get_rejects(logger_name)`.
- `Tideweaver.logger_name: str | None` — new keyword argument on
  `Tideweaver.__init__` (default `None`).  When set, the scheduler diagnostic
  sites route through `_route_scheduler_event_to_log` instead of the bare
  module logger; when `None` the existing fallback is retained unchanged.
- `LoggedTideweaver` now passes `self._logger_name` to the base
  `Tideweaver.__init__(logger_name=...)` so structured scheduler-event routing
  activates automatically for all `enable_logging=True` runs.
- **`Watershed.name: str | None`** — optional human-readable label on
  `Watershed`; declared field so `extra="forbid"` still rejects truly unknown
  keys and the field round-trips through `watershed.json` config.
- **`LoggedTideweaver` default `logger_name`** now resolves to
  `watershed.name or "Tideweaver"` when no explicit `logger_name` is passed;
  an explicit non-`None` value always wins.  Named watersheds automatically
  name their session log files.
- **`Tide.session: str | None`** — new field (default `None`); populated from
  `self.logger_name` in `_run_pass` via `Tide.model_construct` so every
  structured tide record is queryable by session.
- **`RejectEntry.session: str | None`** — new field (default `None`);
  populated at all five canal skip sites (`_build_canal_reject`) from
  `self.logger_name`; HTTP-layer rejects retain `session=None`.
- **`scheduler_event` payload** includes a `session` key equal to
  `logger_name`, making concurrent-run records distinguishable inside the file.
- **`_SCHEDULER_ERROR_EVENTS`** promoted from a per-call local `set` to a
  module-level `frozenset` in `observability/logger.py`; no behaviour change.
- **`empty_parent_snapshot` detail strings** in `scheduler.py` module-logger
  fallback paths now use `--` (ASCII) consistently; previously used the
  em-dash `—`, which differs from the structured-path strings.
- **URL internet-traffic errors now route to `<Class>_api.log`** —
  `RejectEntry` gains an additive `is_url_traffic_error: bool = False` field
  (in-memory only, default `False`).  `_build_reject_entry` in
  `incorporator/io/fetch.py` sets it `True` when the originating exception is
  `httpx.HTTPStatusError` (4xx/5xx) or `httpx.RequestError` (transport /
  network layer), and `False` for `IncorporatorFormatError` (parse errors),
  file-mode errors, fjord seed errors, and canal-layer skips.
  `_route_reject_to_log` in `observability/logger.py` now passes
  `is_api=reject.is_url_traffic_error` to `_emit_payload` so
  `APIFilter` routes URL-traffic rejects to `api.log` and all other rejects
  remain in `error.log`, unchanged.
  `LoggedIncorporator.incorp` and `refresh` now route each `RejectEntry` from
  the returned `IncorporatorList` through `_route_reject_to_log` after
  `super()` returns (previously only `LoggedTideweaver` called this path; the
  `LoggedIncorporator` verbs never wrote rejects to any log file).
  `setup_class_logger(cls)` is now called before `super().refresh()` when
  `enable_logging=True` (was called only after — a latent bug surfaced by the
  reach requirement).
  New `LoggingMixin.get_api()` classmethod reads all records from
  `<Class>_api.log` (full file, no key filter) — returns the union of
  hand-called `log_api()` records and URL-traffic reject records.
  `LoggingMixin.get_rejects()` now reads both `error.log` and `api.log`
  (filtered on `"reject"` key) and returns the combined list, so callers need
  not know which file a particular reject landed in.
  `get_error()` docstring now notes that URL-traffic rejects live in
  `api.log` / `get_api()` and will not appear in `get_error()` results.

## [1.3.2] - 2026-06-07

### Added

- `incorporator/io/config_paths.py`: a shared helper that resolves the file paths
  declared in a `pipeline.json` / `watershed.json`, used uniformly by the run,
  validate, and log code paths.

### Changed

- Config path resolution: relative **input** paths in a CLI config (`inc_file`,
  `inc_files`, `inflow`, `outflow`, and `refresh`'s `new_file`) now resolve against
  the **config file's own directory**, so a pipeline/watershed JSON runs from any
  working directory (and reads alongside a read-only Docker config mount).
  **Output** paths (`export_params.file_path`) and URLs stay relative to the current
  working directory. The in-process `Incorporator.incorp(...)` API is unchanged.
- `incorporator tideweaver run` now exits non-zero with a summary when a current
  produced zero rows because every source failed to load; a legitimately empty run
  still exits 0.
- CLI and log output is now ASCII-only, so commands no longer raise
  `UnicodeEncodeError` on Windows (cp1252) consoles when piped or redirected.
- The Tideweaver examples (09, 10, 11, nascar-tideweaver, mlb-pulse) use the bare
  `outflow.py` / `inflow.py` sidecar naming shared by both the Python runner and the
  `watershed.json` CLI form, with `conv_dict` declared inline in `incorp_params`.

### Fixed

- A Fjord tail whose declared output class has no fields now infers the output
  schema from the `outflow(state)` rows (emitting a one-time warning) instead of
  risking silently dropped fields.
- `incorporator validate` resolves config paths the same way a run does, so it
  catches a relative `inc_file` that would otherwise fail only at run time.
- Docker `stop_grace_period` is set above `INCORPORATOR_DRAIN_TIMEOUT` so SIGTERM
  drains complete before SIGKILL.
- Example 09 (NASCAR fantasy): the manufacturer make is parsed from the driver
  logo URL, and console output is ASCII-safe.

### Docs

- Refreshed the Tideweaver example READMEs and code comments to describe current
  runtime behavior (output-class inference; input-vs-output path resolution) and
  removed stale "run from the repo root" guidance.
- Example 09 split into `inflow.py` / `outflow.py` sidecars with owner-seat scoring;
  READMEs synced to the split-sidecar layout.

## [1.3.1] - 2026-06-05

### Fixed

- CLI now correctly forwards `inflow` and `outflow` arguments to `stream()`;
  custom-verb errors produce a clearer diagnostic message instead of a bare
  `AttributeError`.
- `--logs` flag in the CLI is properly wired to `LoggedTideweaver`, so
  structured log output is captured when running Tideweaver sub-roles from
  the command line.

### Changed

- Test suite is tiered into `fast`, `slow`, and `benchmark` markers and the
  CI matrix is parallelised accordingly, cutting wall-clock CI time.

### Docs

- API Atlas expanded with entries for 11 previously undocumented public
  symbols plus `CustomCurrent`.
- Marketing-review positioning pass across the adopt-all docs; corrected a
  `calc_all` example that produced wrong output.
- Seven user guides and 15 tutorial READMEs reconciled with the v1.2.0..HEAD
  API surface (factual corrections and completeness fixes).
- CONTRIBUTING updated to reflect current CI facts; SECURITY updated with
  accurate archive-handling behaviour.
- Removed stale historical benchmark snapshot and associated review-audit docs.

## [1.3.0] - 2026-06-02

First PyPI publication since v1.2.0 (2026-05-21).  The intervening tags
`v1.2.1`, `v1.2.2`, and `v1.2.3` were local / GitHub-only — no PyPI
upload — so this release is what PyPI users will see as the cumulative
delta on top of v1.2.0.

Content shipped under those tag-only releases is documented in the
`[1.2.3]`, `[1.2.2]`, and `[1.2.1]` sections below; this `[1.3.0]`
entry exists for the version-bump rationale and to mark the
PyPI-resumption point.  No additional API surface in this release
beyond what is already documented in those sections.

## [1.2.3] - 2026-06-01

### 2026-05-31 — post-audit cleanup: deleted unmeasured perf machinery

Audit of the 32 commits since 2026-05-29 surfaced two perf mechanisms
that shipped without measurement and existed past their justification.
Both deleted; their constants and dispatch branches go with them.  Net
-147 LOC of internal machinery; no API surface change.

#### Removed

- **`_SMALL_TABLE_THRESHOLD = 64` and its fast-path branch** in
  `incorporator/io/handlers/columnar.py:_table_to_dicts`.  The constant
  shipped in commit `63d6f2d` (perf-batch "Items 1/6/Adjacent C/Item
  9/Adjacent A") with no benchmark.  The companion pinning bench added
  22 hours later (`bba81c3`) measured the premise as false on
  contemporary hardware: pyarrow.compute vectorisation wins at
  row_count=30, well below the 64 threshold the fast path gated.  Arrow
  vectorisation is now unconditional on Parquet / Feather / ORC parse
  paths.
- **Cardinality-sample-and-decide cache machinery** in
  `incorporator/schema/builder.py` — `_maybe_cache_bare()`,
  `_CACHE_SKIP` sentinel, the W3/W4 cache-decision blocks across the
  three dispatcher branches (CalcOp / whole_row / generic Op), the
  `_cache` slots on `Op` and `CalcOp`, and five of the six hardcoded
  constants the mechanism depended on (`500` × 3 sample sizes, `0.5`
  cardinality crossover, plus the previously-deleted `64`).  The
  mechanism existed because the agent talked the user out of "just
  cache the results" with invented justifications about high-cardinality
  workloads suffering from cache-miss overhead.  The replacement is one
  line at Op construction.

#### Changed

- **`Op` and `CalcOp` now wrap pure callables in
  `functools.lru_cache(maxsize=10_000)` at construction time** when
  `is_pure=True` (and, for `Op`, `whole_row=False` — pluck's `_pluck`
  operates on unhashable dicts).  Replaces the cardinality-sample
  decision logic with unconditional caching.  Calls on unhashable args
  fall through `Op.__call__`'s `__wrapped__` recovery path (covers
  `join_all` on lists, `inc(new)` on dicts, `calc(len, "list_field")`).
- **`Op.is_pure=True` documented as a caller-asserted claim** in the
  class docstring — side-effecting closures with `is_pure=True` only
  fire side effects on cache miss.  Parity with the existing warning
  on `calc()`.

#### Notes

- Benchmark floors hold under the new always-cache design:
  low-cardinality 567k rows/sec (≥150k floor), continuous-data 100k
  rows/sec (≥80k floor — worst case: every row is a cache miss),
  calc(pure=True) 461k at 1.03× pure-vs-impure ratio, CalcOp persistent
  cache 490-517k rows/sec across 5 sequential batches.
- The `10_000` `lru_cache` maxsize is documented as a memory bound, not
  a tuning knob.

### 2026-05-31 — typed wrapper-handler unification

DATA-SHAPE pipeline parameters now travel as typed frozen-dataclass
directives (`Ex`, `Nm`, `Pk`) through a four-pass dispatcher: drop →
conv_dict → rename → PK-bind. PK binding moved to the final pass and
its source path is rewritten through the rename map at config time,
which closes two silent failure modes — Case A (rename moves the PK
source away) and Case B (rename creates the PK source). Both are now
pinned by regression tests. Bare strings and tuples continue to work
in every existing call site.

#### Added

- **`Ex(field: str)` directive** at `incorporator/schema/directives.py` —
  frozen-dataclass drop wrapper.  `excl_lst` accepts bare strings
  (top-level key drop, as always) and `Ex(...)` instances (nested-leaf
  drop via `DataPath.pop`).  Mixed sequences are accepted; the
  normalizer splits and merges them in one pass.
- **`Nm(old: str, new: str)` directive** at
  `incorporator/schema/directives.py` — frozen-dataclass rename
  wrapper.  `name_chg` accepts bare 2-tuples and `Nm(...)` instances
  interchangeably; the normalised result is identical.
- **`NormalizedKwargs` container + `_normalize_etl_kwargs(...)`**
  at `incorporator/schema/directives.py` — single normalizer that
  splits `excl_lst` / `name_chg` mixed sequences, synthesises `Pk`
  from `code_attr` / `name_attr` bare strings, and rewrites
  `Pk.source` through the rename map at normalize time (first-hit).
- **`DataPath.pop(record)` and `DataPath.set(record, value, *, create_parents=False)`** at
  `incorporator/schema/path.py` — nested-path mutation primitives
  backing `Ex.apply_drop`, `Pk.apply_bind`, and (later in this cycle)
  nested `Nm` renames.  With `create_parents=False` (default) missing
  intermediates are a silent no-op; with `create_parents=True` missing
  intermediate str-keyed dicts are auto-created on the way down.
  `DataPath.has(record)` added alongside — distinguishes absent key
  from explicit-`None` without a try/except.
- **CLI token allow-list entries for `Ex` / `Nm` / `Pk`** at
  `incorporator/cli/tokens.py:126-128`.  String forms like
  `"Ex('field')"` and `"Nm('old', 'new')"` resolve through
  `resolve_tokens()` in `pipeline.json` / `watershed.json`.

#### Fixed

- **Silent PK-bind regression introduced by commit `2fb46d0` (Phase C2
  dispatcher reorder).**  Case A — `name_chg` renames the field
  `code_attr` points at, so the PK bind resolved against the wrong
  key.  Case B — `name_chg` creates the field `code_attr` targets,
  but the PK bind ran before rename and resolved to `None`, after
  which Pydantic's auto-counter fallback silently wrote `"1"`,
  `"2"`, `"3"` instead of the real value.  Both failure modes were
  silent (no error, no warning, no existing test exercised them).
  The four-pass dispatcher with `Pk.source` rewritten through the
  rename map at normalize time closes both cases; pinned by 20 new
  regression tests.

#### Changed

- **Dispatcher order restored to Ex → Op → Nm → Pk** at
  `incorporator/schema/builder.py:154-274`.  PK binding (pass 4)
  runs after rename (pass 3) so renamed source fields resolve
  cleanly.  Each pass iterates rows-outer / directives-inner to
  keep each row dict warm in CPU cache.
- **`Nm` supports nested and cross-parent renames** (commit `6cd1754`).
  `Nm("user.email", "contact.email")` drills via `DataPath`,
  auto-creating intermediate parent dicts on the target side.  The
  `_old_path` / `_new_path` slots on `Nm` back the multi-segment
  path resolution; `Nm.apply_rename` routes single-segment renames
  through a fast path and multi-segment through `DataPath.has` /
  `DataPath.pop` / `DataPath.set(create_parents=True)`.
- **`_PkBindOp` and its `_splice_pk_binding` virtual-splice helper
  deleted.**  Pass 4 dispatches directly on `normalized.pk_tuple`.

### 2026-05-31 — columnar conv_dict reorientation + parse/write perf recovery

A session of architectural reorientation: `conv_dict` is now uniformly
columnar at the dispatcher level (op-outer / row-inner), with
unconditional `lru_cache` wrapping for `is_pure=True` ops at `Op`
construction time (no per-batch cardinality sampling — see the
post-audit cleanup entry above for the deletion of the original
adaptive-sample machinery).  All 7 closure-returning converters
collapse to a single generic `Op` class; the existing `CalcOp` /
`CalcAllOp` stay dedicated for their richer state.  Plus surgical
parse-side and write-side perf recovery.

#### Default change — `calc()` / `calc_all()` `pure` defaults to `True`

`calc()` and `calc_all()` default `pure=True`.  `is_pure=True` ops are
wrapped in `functools.lru_cache(maxsize=10_000)` at `Op` construction —
each unique input tuple is computed once and the result is reused for
repeated identical inputs.

Pass `pure=False` explicitly when your `func` must run on every row
(side effects: `datetime.now()`, `uuid.uuid4()`, logging, DB writes,
network calls, mutable counters).

Rationale: `conv_dict` is a data-transform layer.  Defaulting to
`pure=True` matches the common case; explicit `pure=False` covers
the side-effect path.

#### Added

- **`Op` class** at `incorporator/schema/converters.py`.  Generic
  conv_dict marker carrying `_func`, `input_keys`, `is_pure`,
  `whole_row` slots — replaces the 7 dedicated marker classes
  (`PluckOp`, `LinkToOp`, `LinkToListOp`, `SplitAndGetOp`,
  `JoinAllOp`, `AsListOp`, `IncOp`).  When `is_pure=True and not
  whole_row`, the `Op` constructor replaces `_func` with
  `functools.lru_cache(maxsize=10_000)(func)` — unconditional at
  construction time, no per-Op `_cache` slot, no runtime sampling.
  `Op.__call__` has an `__wrapped__` fallback for unhashable args
  (`join_all` on lists, `inc(new)` on dicts).
- **`Op.whole_row` flag** signals dispatcher to pass the whole row
  dict (replaces the former `isinstance(op, PluckOp)` branch).

#### Changed

- **`conv_dict` dispatcher** at `apply_etl_transformations` is now
  op-outer / row-inner uniformly (was nested per-row / per-op).  See
  the typed wrapper-handler unification entry above for the final
  PK-binding pass order (Ex → Op → Nm → Pk; Pk runs LAST so renames
  from pass 3 are visible to it).
- **`pluck()` dispatch correction.**  PluckOp's `__call__` expects
  the whole row dict to navigate paths from root, but the prior
  dispatcher passed `d.get(key)`.  Latent bug — no test exercised it
  in `apply_etl_transformations`; PluckOp's own unit tests test it
  directly.  Now `op(d)` correctly per its documented contract.
- **`serialize_nested()`** at `incorporator/io/formats.py` routes
  through `_orjson_mod.dumps_str` instead of stdlib `json.dumps`.
  All 7 callers gain orjson speedup when `[speedups]` is installed.
- **`_batched_columns` inline scalar fast-path** at `columnar.py`
  bypasses `serialize_nested` for `str/int/float/bool` values via
  `_SCALAR_TYPES` frozenset + `type(v) in` C-level membership check
  (no MRO walk).  Recovers ~41% of a previously-measured Arrow-write
  throughput regression; ORC writes now exceed v1.1.3 docs claim.
- **`DataPath.resolve()` single-segment fast-path** at
  `incorporator/schema/path.py` skips the multi-segment walk for the
  common single-key case.  Helps every caller of `resolve()`, not
  just PK binding.
- **`apply_etl_transformations` PK-binding dotless fast-path**:
  config-time branch on `code_attr` / `name_attr` complexity skips
  DataPath construction entirely when the path contains no `.`.

#### Performance

Bench (stagger+alternate methodology, 5 runs/format):

| Format | pre | post | Δ |
|---|---:|---:|---:|
| ORC write | 212k | 293k | +38 % |
| Feather write | 208k | 285k | +37 % |
| Parquet write | 189k | 248k | +31 % |
| CSV parse | 178k | 210k | +18 % |
| SQLite parse | 201k | 228k | +13 % |

Arrow write recovery brings Feather to +18 % over the v1.1.3 docs
claim; Parquet/ORC come back within ~11–12 % of docs under the
stricter stagger+alternate methodology.

### 2026-05-30 — internal grammar gets typed

This session's 11 commits (Chains α through ζ plus docs / docstring
sweeps) are mostly framework-internal refactors that make the
scheduler's vocabulary legible at the type level.  No user-facing
breaking changes; every existing string comparison and JSON shape
keeps working.

#### Added

- **`SkipReason`, `WakeReason` enums** (`incorporator.observability.tideweaver`).
  `str`-subclass enums so `SkipReason.SURGE_HALTED == "surge_halted"`
  stays `True` — existing code that compares `tide.skipped` entries
  against plain string literals keeps working, and IDEs / mypy now
  narrow on the typed surface.  Pydantic v2 serialises the value
  (not the name).  Source: `observability/tideweaver/reasons.py`.
- **`GateMode` enum** (`incorporator.observability.tideweaver`).
  `str`-subclass; members `HARD` / `SOFT` / `WEIR`.  Shape constructors
  (`Watershed.chain` / `diamond` / `fanout`) and `Edge(gate_mode=...)`
  accept either form — `gate_mode="hard"` and `gate_mode=GateMode.HARD`
  produce identical `FlowControl`.  Source: `observability/tideweaver/flow.py`.
- **Per-class `tide.log` file** for `LoggedTideweaver`.  Every yielded
  `Tide` now lands in a dedicated `logs/<logger_name>_tide.log` in
  addition to the existing `_api` / `_error` / `_debug` files.
  `LoggedTideweaver.get_tides()` reads this single file (sorted by
  `tide_number`) instead of merging `_error.log` + `_debug.log`.
- **`incorporator deps` CLI + `list_deps()` / `install_hint()` / `Category`
  / `DepInfo` public API** for runtime optional-dependency introspection.
  Tabular or JSON output; filterable by category or installed-status.
  See `docs/cli_and_configuration.md §10` and `docs/api_atlas.md`
  Optional-dependency introspection section.
- **`CustomCurrent.auto_park_snapshot` ClassVar** (default `True`) —
  the scheduler's `_run_tick` wrapper now automatically parks
  `list(cls.inc_dict.values())` as `cls._tideweaver_snapshot` after
  the tick if the tick body didn't manually assign one (identity check
  on the pre-tick value).  Subclasses opt out with
  `auto_park_snapshot = False`.  Source:
  `observability/tideweaver/current.py:318`.
- **Scheduler empty-output WARNING** — CustomCurrent ticks that
  succeed but produce empty output despite non-empty upstream
  snapshot(s) emit a one-line WARNING per pass naming the current and
  its upstreams.  Helps catch silent predicate / conv_dict
  mismatches in user tick bodies.  Source:
  `observability/tideweaver/scheduler.py:818`.

#### Changed (internal)

- **`Gate` hierarchy collapsed** — `HardLock` / `SoftPass` / `Weir`
  no longer carry their own `gate_reason()` bodies.  The base
  `Gate.gate_reason(ctx)` does the work; subclasses override three
  ClassVar check flags.  Behaviour unchanged.
- **`DataPath` + `DataKind` value types** (`schema/path.py`, `schema/kind.py`)
  + **`classify()`** (`schema/converters.py`).  Internal type-ladder
  consolidation that replaces four ad-hoc predicates with one walk.
  The dotted-path surfaces (`rec_path`, `pluck`, `calc` / `calc_all`
  keys, `inc_code`, `inc_name`, `inc_child`) all route through
  `DataPath` for identical behaviour.
- **Optional-dep probes migrated to the `_deps` registry**;
  `orjson` fast-path now also covers the logger pipeline.
- **`_emit_payload` helper** in the observability sweep — reduces
  duplication across the three routing functions
  (`_route_wave_to_log` / `_route_tide_to_log` / `_route_reject_to_log`).

#### Internal

- `IncorporatorList.failed_sources` cached on first read (perf);
  baseline micro-benchmarks added under `tests/benchmarks/`.
- PEP 585 lowercase sweep continued through T9 / T11 outflow
  sidecars; `DLQ → rejects` rename swept across docstrings.
- `parses_as_datetime` / `parses_as_int` / `parses_as_float`
  `Returns:` sections aligned with the `classify`-based
  implementation.

#### Examples

- **`examples/09-nascar-fantasy-fjord/nascar_fantasy.py`** adopted
  the **`inc(int, default=0)` (OUTPUT key == SOURCE key)** DX-first
  migration over `calc(int, "key", default=0, target_type=int)` for
  flat-typed integer fields in the FantasyTeam `conv_dict`.  Shorter,
  reads as "coerce-with-fallback" rather than "compute-from-input".

## [1.2.2] - 2026-05-26

Docs polish for the v1.2.1 surface.  **Tag-only release** — no PyPI
publish, no GitHub Release object.  No runtime / API changes.

### Docs

- **README refreshed to v1.2.1 surface** — `tune()` / `TuningReport`
  / `LoggedTideweaver` / `register_host_penstock` callouts.
- **`docs/api_atlas.md`** — new entries for `tune()`, `TuningReport`,
  `TuningHint`, `LoggedTideweaver` (with `get_tides()` /
  `get_rejects()` disk readers).
- **CLI reference (`docs/cli_and_configuration.md`)** — v1.2.1 Tide
  schema fields, canal-layer `error_kind` values,
  `backlog_backoff_factor` constructor arg.
- **Streaming guide (`docs/streaming_and_pagination.md`)** —
  `adapt_chunk_size=True` AIMD subsection + voice cleanup.
- **Performance guide (`docs/performance.md`)** — v1.2.1 typo fix,
  `adapt_chunk_size` notes, `backlog_backoff_factor` notes.
- **Deployment guide (`docs/deployment.md`)** — `LoggedTideweaver`
  and `backlog_backoff_factor` for production Tideweaver runs.
- **Debugging guide (`docs/debugging.md`)** — orchestration
  debugging recipe using `LoggedTideweaver` + `tune()`.
- **Installation guide (`docs/installation.md`)** — Python 3.10
  floor (3.9 dropped in v1.2.1) + voice cleanup.
- **Formats guide (`docs/formats_and_compression.md`)** — voice
  cleanup (dropped "modern", "lightning-fast").
- **Historical benchmarks** — `docs/benchmark_results_v1.1.3.md`
  renamed to `_historical.md` with a header note pointing at
  `docs/performance.md` for current numbers.
- **Tutorials refreshed for v1.2.1**: T2 (data-lake-pivot) voice
  cleanup; T3 (universal-formats) surface refresh; T5
  (parent-child-drilling) corrected stale host-registry claim
  and added `register_host_penstock` alternative; T7
  (stateful-refresh) `RejectEntry` alongside `failed_sources`;
  T8 (streaming-daemon) surface refresh; T9 (nascar-fantasy-fjord)
  surfaces `RejectEntry` for production retries; T10
  (multi-source-fjord) `rejects` + `register_host_penstock` +
  `LoggedIncorporator` for production fjord; T11 (tideweaver)
  surface refresh.
- **Appendices**: `crypto-graph-mapping` voice cleanup;
  `nascar-tideweaver` surface refresh; `tideweaver-parquet-snapshots`
  picked up `LoggedTideweaver` + `tune()` + canal rejects;
  `tideweaver-vs-prefect` gained v1.2.1 capability rows.
- **THANK_YOU.md** voice cleanup.
- **`architect.py` module docstring** advertises `tune()` +
  `TuningReport` as the post-runtime feedback loop.

## [1.2.1] - 2026-05-23

This release tags the canal-followup work (the v1.2.0-era A-F items
that landed on `workflow` post-v1.2.0) together with the
TypeAdapter refactor and the outcome-record telemetry buildout.
**Tag-only release** — no PyPI publish, no GitHub Release object.

### Added

- **Structured canal-layer rejects (A-F-1)**.  `Tideweaver.rejects`
  now surfaces canal-layer skips (`PenstockLimited`, `SurgeHalted`,
  `SkipAhead`, `GateBlocked`) as `RejectEntry` records, parallel to
  the verb-layer `IncorporatorList.rejects`.  Closes the canal-audit
  F-1 gap.
- **Per-paginator `Penstock` composition (A-F-9)**.  Paginators now
  accept a `penstock=` kwarg that composes with host-level
  throttles.  Local paginators (`SQLitePaginator`, `CSVPaginator`,
  `AvroPaginator`) can finally be rate-limited.
- **Scheduler / Reservoir / Penstock micro-benchmarks (F-1)**.
  `tests/benchmarks/test_scheduler_pass_overhead.py`,
  `test_reservoir_throughput.py`, `test_penstock_overhead.py` cover
  the canal toolkit's hot paths.  All hold their throughput floors.
- **TypeAdapter-vs-per-row validation benchmark (A-F-3)**.
  `tests/benchmarks/test_validate_batch_vs_per_row.py` quantifies
  the batch-validation speedup that motivated A-F-4.
- **Canal routing test coverage (A-F-2 + E-F-3)**.
  `tests/test_tideweaver_routing_*.py` exercises chain / diamond /
  fanout / parallel / custom shapes with realistic intervals.
- **Outcome-record telemetry**: `Wave`, `Tide`, `RejectEntry`
  schemas gained 6 / 5 / 7 new fields covering HTTP retry counts,
  schema cache hits, source URLs, per-edge identity, status codes,
  cooldown hints.  New `CurrentOutcome` slotted dataclass captures
  per-current outcomes inside Tide.
- **`LoggedTideweaver`** — drop-in for `Tideweaver` with structured
  JSON-line logs.  Routes every yielded Tide + every accumulated
  `RejectEntry` to disk via the existing JSONFormatter +
  QueueHandler.  Companion disk readers:
  `LoggedTideweaver.get_tides()`, `LoggedTideweaver.get_rejects()`,
  `LoggingMixin.get_rejects()`.
- **`architect.tune()`** — post-runtime feedback loop.  Consumes
  accumulated rejects + tides + waves and emits a `TuningReport` of
  structured recommendations across `chunk_size`, penstock rate,
  surge threshold, `pass_interval`, retry policy.  Companion
  `Tideweaver.summary()` convenience method.
- **Adaptive `chunk_size` in `stream()`** — opt-in via
  `adapt_chunk_size=True` keyword.  AIMD policy (additive-increase
  / multiplicative-decrease) on `paginator.chunk_size` between
  chunks based on recent processing times.  Bounded by
  `chunk_size_min` / `chunk_size_max` / target window.
- **Backlog short-circuit on `Tideweaver`** — opt-in via
  `backlog_backoff_factor=2.0` constructor arg.  Extends the
  next-pass wait when the scheduler is consistently saturated;
  default 1.0 = disabled = identical behaviour to v1.2.0.
- **CLI `tideweaver` test coverage** — new
  `tests/test_cli_tideweaver.py` covers `tideweaver run --json-output`
  NDJSON shape, `--heartbeat-file` touch behaviour, and
  `--drain-timeout` precedence.

### Changed

- **Batch-validate rows via cached `TypeAdapter` (A-F-4)**.
  `build_instances` now calls `TypeAdapter(list[Cls]).validate_python(rows)`
  once per chunk instead of per-row `Cls.model_validate(row)`.  The
  `TypeAdapter` is cached per dynamic class.  Measured 1.3-2.0×
  faster validation on realistic workloads (per
  `tests/benchmarks/test_validate_batch_vs_per_row.py`).  Trade-off:
  `incorp()` peak memory is now O(N) instead of streaming row-by-
  row.  Documented in `docs/performance.md`.
- **Python 3.9 support dropped.**  `requires-python` raised to
  `>=3.10` to accommodate `@dataclass(slots=True)` on
  `CurrentOutcome` and PEP 604 union syntax.  CI matrix now tests
  3.10 / 3.11 / 3.13.
- **`execute_request` refactored** from `@retry` decorator to
  explicit `AsyncRetrying` loop.  Tenacity parameters (same
  `stop_after_attempt(8)`, `wait_random_exponential`, retry
  predicate, `reraise=True`) are byte-identical to v1.2.0.
  Captures `attempt_number` for `RejectEntry.attempt_number`.
- **Bulk `inc_dict` insertion** — new `_BATCH_INSERT_MODE` ClassVar
  gates the per-instance write in `model_post_init`; the
  `build_instances` call site does one `WeakValueDictionary.update()`
  after `TypeAdapter.validate_python()`.  Saves ~100-200 ns/row.
- **`RejectEntry.model_construct()` at all framework-internal
  sites** — skip Pydantic validation on trusted input.  Companion
  `Wave.model_construct()` and `Tide.model_construct()` everywhere
  the framework builds these records.
- **CLI heartbeat-touch hardened** — `_emit_wave` / `_emit_tide`
  wrap the serialise/print in try/finally so the heartbeat file
  always touches even if `model_dump_json` raises.  Prevents a
  serialisation glitch from killing the Docker HEALTHCHECK.

### Fixed

- **Gate dedup direction (bug fix)** — `_last_consumed` is now keyed
  on `(from_name, to_name)` consistently; previously a direction
  inversion could cause a gate to under-block.  Pinned by
  `tests/test_tideweaver_dedup.py`.
- **Examples T-10 rename completion** — `fjord.py` →
  `crypto_spread.py` → `outflow.py` rename had stale references;
  fixed.
- **`Tide.wake_reason` Literal narrowing** — was `str`; now
  `Literal["startup", "timer", "wake_event", "pass_interval", "shutdown"]`.
  No runtime change; better mypy / IDE narrowing.
- **`Tide.next_due_in_sec` accuracy** — computed from post-walk
  monotonic timestamp instead of pass-start; eliminates a
  microsecond-scale overstatement.

### Internal

- PEP 585 builtin-generics across the source tree
  (`typing.List/Dict/Tuple/Set` → builtins).
- PEP 604 union syntax (`Optional[X]` → `X | None`).
- `from __future__ import annotations` rollout to all 63 source
  files.
- ruff `UP` (pyupgrade) ruleset enabled; locks in the
  modernisation.
- `itertools.pairwise` + `isinstance(x, T1 | T2)` union syntax at
  the relevant sites.
- Comment-sweep: stripped ~94 lines of historical / planned-
  refactor prose from inline comments and docstrings; current
  behaviour described instead.
- Obsolete planning docs `docs/canal_evaluation.md` and
  `docs/canal_integration_audit.md` removed — their recommendations
  are now implemented in code.

## [1.2.0] - 2026-05-22

### Changed

- **Unified null-handling across `calc` / `calc_all` / `pluck` /
  `link_to` / `link_to_list` / `split_and_get`.**  Aligned with the
  null contract `inc()` has always provided: when input values are
  garbage (``None``, ``""``, ``"N/A"``, ``"null"``, ``"unknown"``,
  ``"nan"``, ``"undefined"``), the user-supplied callable
  (``func`` / ``chain`` / ``extractor`` / ``cast_type``) is no
  longer invoked.  ``calc`` returns ``default`` (or ``None`` for the
  extractors) silently — no warning emitted.  Warnings still fire
  when the callable raises on **real** data, separating the
  "missing data" case from the "function exploded" case.

  Migration: explicit null guards in user lambdas are no longer
  necessary.  Use stdlib callables directly:

  ```python
  # before — defensive null guard inside the lambda
  calc(lambda v: v.lower() if v else "", "title", default="", target_type=str)
  pluck("data.title", chain=lambda v: v.lower() if v else "")
  link_to(books, extractor=lambda v: v.upper() if v else None)

  # after — same behaviour, no log noise, no lambda
  calc(str.lower, "title", default="", target_type=str)
  pluck("data.title", chain=str.lower)
  link_to(books, extractor=str.upper)
  ```

  **Performance: net win.**  ``is_garbage_value`` pre-checks cost
  ~50 ns per row but eliminate the Python exception raise (~30 µs)
  + ``logger.warning`` call (~10 µs) that previously fired on every
  garbage row.  On garbage-heavy datasets the dispatch path is now
  ~95% faster; on garbage-free datasets the overhead is <0.5%.
  ``split_and_get``'s narrow null check (``None``/``""``) is
  widened to the full garbage set for consistency.

### Added

- **`RejectEntry` structured reject list.**  `IncorporatorList`
  now carries a `rejects: List[RejectEntry]` property with structured
  failure records (`source`, `error_kind`, `message`, `retry_after`,
  `wave_index`).  HTTP error sites in `incorporator/io/fetch.py`
  build entries with `error_kind` from the exception type and
  `retry_after` parsed from any `Retry-After` header.  ETL practice
  calls failed-load rows *rejects* rather than the messaging-system
  *dead-letter queue* term — the rename follows that convention.

  The legacy `failed_sources: List[str]` attribute remains as a
  derived view (`[entry.source for entry in rejects]`) so existing
  user code, tests, and tutorials continue to work unchanged.
  Reach for `rejects` when you need structured access to the
  exception type or retry hint:

  ```python
  result = await Coin.incorp(inc_url=["...", "https://broken/"])
  for entry in result.rejects:
      if entry.error_kind == "HTTPStatusError" and entry.retry_after:
          schedule_retry(entry.source, after=entry.retry_after)
  ```

  Sidecar pipeline write sites (`chunked.py`, `_outflow.py`,
  `_stateful_shim.py`) still route through the back-compat
  `failed_sources=[...]` constructor kwarg and are auto-wrapped into
  entries with `error_kind="Unknown"`.

- **`SourceRef` value type for source dispatch.**  A new
  `incorporator.io.SourceRef` frozen dataclass consolidates the
  "what kind of source is this?" classification used by
  `incorp()`, `architect()`, and other source-consuming verbs.  Five
  factories (`from_url`, `from_file`, `from_parent`, `from_payload`,
  `from_kwargs`) plus an auto-detect `parse()` classmethod.  Public
  verb signatures unchanged; `SourceRef` is internal scaffolding plus
  an opt-in public type for callers that want explicit source typing.

### Internal subclass API change

- **`_EdgeState` now composes a `FlowState` field.**  The Tideweaver
  scheduler's per-edge bookkeeping (`_EdgeState`) used to declare four
  fields — `last_consumed_at`, `bucket_tokens`, `bucket_last_refill_at`,
  `window_log` — directly.  They're now grouped under
  ``_EdgeState.flow_state: FlowState`` (the canal-toolkit type from
  ``incorporator.io.penstock``).  Built-in Penstocks were updated; only
  third-party Penstock subclasses that override
  ``consume_reason(edge_state, flow, now)`` and read the old top-level
  fields are affected.

  Migration:

  ```python
  # Before:
  if edge_state.last_consumed_at is None:
      ...
  edge_state.bucket_tokens = float(self.burst)
  edge_state.window_log.append(now)

  # After:
  if edge_state.flow_state.last_consumed_at is None:
      ...
  edge_state.flow_state.bucket_tokens = float(self.burst)
  edge_state.flow_state.window_log.append(now)
  ```

  Finishes Phase A2's intent (i14): the dead Penstock-specific fields
  no longer live at the top of ``_EdgeState`` for subclasses that don't
  use them — they're behind the ``flow_state`` namespace owned by the
  edge's Penstock.

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
  from incorporator import register_host_penstock
  from incorporator.io.penstock import SustainedPenstock

  register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
  register_host_penstock("pokeapi.co", SustainedPenstock(rate_per_sec=1.5))
  register_host_penstock("vpic.nhtsa.dot.gov", SustainedPenstock(rate_per_sec=1.5))
  ```

  Per-host rationale (rates from the previous registry — re-verify
  against the provider's published docs when you copy):

  - `api.coingecko.com` → 0.2 r/s (12/min, comfortably under the 5-15/min anon ceiling).
  - `pokeapi.co` → 1.5 r/s (90/min, under the 100/min documented ceiling).
  - `vpic.nhtsa.dot.gov` → 1.5 r/s (under the 100-200/min documented ceiling).

- **Dropped `incorporator.io.fetch._KNOWN_API_RATE_LIMITS`** and
  **`_resolve_host_safe_rate`** — the backward-compat shims have no
  remaining callers.  Use `incorporator.io.penstock.known_host_rates()`
  for the live registry view.

- **Unified rate-limit primitive: `Penstock` replaces `ThrottleStrategy`.**
  The HTTP throttle layer and the Tideweaver edge layer now share one
  canal-toolkit primitive — `Penstock` is the structural gate, the
  throttle settings (`rate_per_sec`, `burst`, `window_sec`) configure
  it, and the rate is the computed output.  `io/throttle.py` is gone;
  the new home is `io/penstock.py`.  JSON config shapes (`watershed.json`)
  are unchanged — the same `{"type": "burst", "rate_per_sec": ..., "burst": ...}`
  payload works at both layers.

  Migration:

  | Before | After |
  | --- | --- |
  | `from incorporator.io.throttle import FixedIntervalThrottle` | `from incorporator.io.penstock import SustainedPenstock` |
  | `FixedIntervalThrottle(0.2)` | `SustainedPenstock(rate_per_sec=0.2)` |
  | `BurstThrottle(2.0, 10)` | `BurstPenstock(rate_per_sec=2.0, burst=10)` |
  | `NullThrottle()` | `NullPenstock()` |
  | `register_host_throttle("h", lambda: FixedIntervalThrottle(0.2))` | `register_host_penstock("h", SustainedPenstock(rate_per_sec=0.2))` |
  | `from incorporator import register_host_throttle` | `from incorporator import register_host_penstock` |
  | `ThrottleStrategy` (Protocol) | `Penstock` (Pydantic BaseModel) |
  | `resolve_throttle(...)` | `resolve_penstock(...)` |

  The legacy factory-callable form still works on
  `register_host_penstock` (it accepts either a `Penstock` instance or
  a zero-arg callable returning one), so `lambda: FixedIntervalThrottle(...)`
  ports cleanly by changing the inner class name.

  Bug fix included: `BurstPenstock`'s refill logic now does an explicit
  `None` check on `bucket_last_refill_at` instead of `or now`, so a
  legitimate `0.0` watermark no longer silently erases the refill
  window (latent in the previous `BurstThrottle` since v1.0).

- **Removed `watershed.json` legacy aliases** — the v1.2.0
  `dependency_mode` (top-level) and `"mode"` (per-edge) aliases for
  `gate_mode` are gone.  Passing them now raises ``ValueError`` with
  inline migration guidance instead of silently warning.

  Migration:

  ```json
  // Before v1.2.0:
  {"shape": "chain", "dependency_mode": "hard", "currents": [...]}

  // v1.2.0+:
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

  Callers that do not subclass Gate / Penstock / Spillway or supply
  custom ``rate_fn`` callables see no observable change.  Subclasses
  and custom ``rate_fn`` callables update their signatures (drop the
  first scheduler arg).

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
