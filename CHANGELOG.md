# Changelog

All notable changes to Incorporator are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.3] - 2026-05-16

### Added
- **Tideweaver orchestration layer** — graph-based orchestration over the
  existing `stream()` / fjord-flush / `export()` primitives.  Build a
  `Watershed` (one time window + named `Current` nodes + dependency edges),
  hand it to `Tideweaver`, run.  Five names cover the whole layer:
  `Tideweaver`, `Watershed`, `Current` (with verb-typed subclasses
  `Stream` / `Fjord` / `Export`), `Tide` (per-pass log record), and the
  existing `Wave`.  Four shape constructors (`parallel`, `chain`, `fanout`,
  `diamond`) cover the common topologies; a `custom` shape with explicit
  `edges` covers everything else.  Hard / soft dependency gating,
  skip-ahead, graceful drain at window close, and per-current
  `on_error` policy (`restart` / `isolate` / `fail_watershed`).  See
  [Tutorial 7](./docs/7_tideweaver.md).
- **`incorporator tideweaver run|validate` CLI sub-commands** plus
  declarative `watershed.json` config with the same env-var interpolation
  and token-resolution pipeline that `stream` / `fjord` configs use.
  `run` pre-flights with the same validator `validate` uses (parity with
  `_run_stream` / `_run_fjord`).
- **`incorporator init --type tideweaver`** scaffold — third scaffold
  type next to `stream` / `fjord`.  Generates a `watershed.json` (diamond
  shape) + paired `outflow.py` ready to edit.
- **`examples/nascar_watershed.json`** + **`examples/tideweaver_code/race_outflow.py`**
  — on-disk sample for the CLI smoke-test path; mirrors the
  `examples/fjord_code/` convention used by Tutorial 7.
- **Public exports on `incorporator`** for the seven new names
  (`Tideweaver`, `Watershed`, `Current`, `Stream`, `Fjord`, `Export`,
  `Tide`).

### Changed
- **`cli/validate.py`** auto-detects watershed configs (top-level
  `window` + `shape` keys) and now exposes `tideweaver` in the
  `ConfigType` literal and the `--type` flag.  No behaviour change for
  `stream` / `fjord` validation paths.
- **`observability/pipeline/_outflow.py`** factors a shared async
  `flush()` generator that yields `(derived_name, row_count, error)` per
  output class.  Used by both the legacy `_outflow_daemon` and the new
  `Tideweaver._tick_fjord` — eliminates ~50 lines of duplicated
  outflow-normalize / dynamic-class-build / per-class export logic.
  Wave-emission shape preserved exactly for the legacy daemon.
- **CLI help text** updated across `init`, `validate`, and the new
  `tideweaver` sub-app so the auto-generated `--help` output covers all
  three pipeline types consistently.
- **Documentation pass** — `docs/cli_and_configuration.md` gains a §9
  for the `tideweaver` sub-command and a row in the
  "When to Reach For" decision table; `docs/library_reference.md` adds
  a bullet for `incorporator.observability.tideweaver`; README adds a
  brief Tideweaver subsection under "The Verbs"; `docs/installation.md`
  and `docs/deployment.md` mention the new sub-command alongside
  `stream` / `fjord`.
- **Tick → wave prose drift** from the earlier user-visible rename
  cleaned up in `docs/5_stateful_refresh.md` and
  `docs/6_streaming_daemon.md`.
- **`stream(stateful_polling=True)` collapsed into a thin shim** over
  `fjord()`.  Two engines (chunking + stateful) became one engine
  (chunking) plus a single-source-fjord shim — eliminates a parallel
  code path with subtly drifting semantics.  Wave-contract preserved:
  same `operation` strings, same `chunk_index` cadence, same instance
  identity across refreshes.  Shim lives at
  `observability/pipeline/_stateful_shim.py`; regex-anchored op-string
  remap and explicit inflow wire-through keep the user-visible surface
  unchanged.  `stateful_polling=True` continues to work as documented.
- **Typeless-format reads now auto-coerce via `_schema_union`.**  When
  a class has already been incorp'd from a typed source (JSON / NDJSON /
  Parquet / SQLite / Avro) and is then read from a typeless format
  (CSV / TSV / PSV), `build_instances()` synthesises `inc()` converters
  for every field the user didn't name in `conv_dict`.  Round-tripping
  an `int` field through CSV preserves the `int` automatically — the
  30-line manual `conv_dict` friction disappears.  User-supplied
  `conv_dict` entries still win on key conflict; the asymmetry is
  one-way (coerce towards richer types, never towards `str`).
  See `incorporator.schema.factory._expand_conv_dict_with_schema_union`.
- **Examples folder reorganised** into per-tutorial directories with
  co-located docs (`examples/02-universal-formats/{universal_formats.py,
  README.md, out/}` etc.).  Replaces the previous flat-script root +
  scattered subdir layout; each tutorial is now self-contained with
  output isolated to its own `out/` dir.

### Fixed
- **`incorp(inc_file=Path(...))` silently returned empty list.**
  `_normalize_source_list` only handled `str` and `list`; a single
  `pathlib.Path` (or any `os.PathLike`) fell through to the
  `payload_list` branch and was dropped.  Now coerces via `os.fspath`
  at every entry point.  Affected tutorial 2 (CSV round-trip) and
  the XML-post-audit appendix in real-world testing.
- **T5 chunking demo errored on default `refresh_params`.**  Paginated
  transient instances have no stable origin URL, so the default
  per-chunk refresh attempt raised.  Tutorial code now opts out
  explicitly with `refresh_params=None`; the parameter is documented
  in the T5 chunking-mode snippet.
- **T3 defensive `getattr` guards** for variable-shape CoinGecko
  `/coins/{id}` responses (missing `links` on memecoins / new
  listings, `null` `genesis_date`).  Pre-existing pathology; no
  framework change.
- **T4 swapped to `api.binance.us`** to bypass `api.binance.com`'s
  451 geo-block in the US / UK / Singapore.  Same v3 endpoint shape,
  ~600 listed pairs vs ~1,900 on `.com`.  Swap back if you're outside
  those regions and want the full pair universe.

## [1.1.2] - 2026-05-15

### Changed
- **Documentation polish pass** — all public docstrings now have formal
  Google-style `Args:` / `Returns:` / `Yields:` sections. Covers
  converter predicates (`is_garbage_value`, `parses_as_datetime`,
  `parses_as_int`, `parses_as_float`), extractor helpers (`link_to_list`,
  `sum_attributes`, `as_list`), `LoggedIncorporator` verbs (`refresh`,
  `export`, `stream`, `fjord`), and all 8 `paginate()` async-generator
  methods. `display()` and `refresh()` return-type descriptions corrected.
- **pyproject.toml classifiers** — removed Python 3.10 and 3.12 entries;
  CI only tests 3.9 / 3.11 / 3.13.
- **Project description** rewritten to accurately describe the library
  ("Schema-free ETL mapper…").
- Docs and example files updated: stale "v2.0" version references,
  dead legacy filename references, and unexplained advanced-pattern
  lead-ins resolved.
- `SECURITY.md` supported-versions table updated to v1.1.x; stale
  parameter name (`code_file=` → `outflow=`) corrected.
- `CONTRIBUTING.md` test count (521+), mypy file count (47), and
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
