# Changelog

All notable changes to Incorporator are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
