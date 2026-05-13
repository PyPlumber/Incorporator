# Changelog

All notable changes to Incorporator are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — `inflow` / `outflow` sidecar files & `@name` references
- **`inflow=` kwarg** on `incorp()` / `refresh()` / `stream()`. Points at
  a Python sidecar (`inflow.py`) holding user-defined helpers — `calc`
  reducers, custom converters, paginator instances, anything the
  trinity's `conv_dict` / `inc_page` kwargs need but JSON can't carry
  directly. Imports happen **once** (cached via `sys.modules`); the
  CLI's token resolver extends its allow-list with the module's public
  symbols so JSON tokens can reference user functions by bare name.
- **`outflow=` kwarg** on `fjord()`, `stream()`, and `export()`. Canonical
  replacement for `code_file=`, which becomes a deprecated alias
  (emits `DeprecationWarning`). On `stream()`, `outflow=` requires
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

### Changed — Naming
- **`code_file=` → `outflow=`** on `Incorporator.fjord()` and
  `Incorporator.export()`. Old name preserved as a deprecated alias
  (`DeprecationWarning`); will be removed in a future major.
- **`"code_file"` → `"outflow"`** as the canonical top-level key in
  `pipeline.json` for fjord configs. Old key still accepted by the
  validator with a yellow deprecation hint in the CLI output.

### Added — CLI & Production Deployment
- **`incorporator init / validate / stream / fjord`** CLI subcommands. Drives the same engines from a `pipeline.json` — no Python wrapper required for single- or multi-source ETLs.
- **Env-var + Secrets-file interpolation in `pipeline.json`**: `${API_KEY}`, `${VAR:-default}`, `${VAR:?required}`, and `${file:/run/secrets/api_key}` for Docker / Kubernetes Secrets mounts.
- **`--json-output` flag** on `stream` / `fjord` for machine-readable NDJSON audit lines (one per chunk).
- **`--heartbeat-file PATH` flag** + Docker `HEALTHCHECK` so orchestrators can detect a hung daemon and restart automatically.
- **SIGTERM graceful shutdown** — `docker stop` / `kubectl delete pod` drain in-flight daemons cleanly instead of falling through to KeyboardInterrupt.
- **`docker-compose.yml` + `.env.example`** shipped with the repo for a 5-minute production deployment.
- **`LoggedIncorporator.fjord` override** mirroring `stream`'s structured audit routing into the queued JSON log files.

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
- **`fjord()` method** on `Incorporator` for multi-source stateful streaming. Fans out N concurrent sources, fuses through a user-defined `outflow(state)` function, exports the combined output. Output class derived from the `code_file` stem — no class to declare.
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
