1. PROJECT IDENTITY
Package: incorporator (v1.0.8, installed as editable via pip install -e .)
Purpose: A zero-boilerplate async micro-client for ETL. Users subclass Incorporator, call incorp(), and get typed dot-notation objects back — all network, parsing, retry, and schema logic is invisible to them.
Philosophy: Dynamic Class Building + Zero-Boilerplate. The end-user should feel like they have superpowers with a single function call.
Stack: Python, Pydantic V2, httpx, tenacity. No other external dependencies are permitted in the core install.
Microclient identity: core `pip install incorporator` installs exactly 3 deps (pydantic, httpx, tenacity). Every heavy dep is opt-in via an extras flag. Lazy-import pattern (try/except ImportError inside parse()/write()) guarantees no optional dep is ever pulled at framework import time.
Environment: Windows, PyCharm, GitHub repo, pytest test suite.

Optional extras:
  [speedups]    → orjson>=3.9, lxml>=4.9, cramjam>=2.7   (fast JSON/XML/compression)
  [avro]        → fastavro>=1.8                            (Avro binary format)
  [xlsx]        → openpyxl>=3.1                            (Excel .xlsx read/write — pure Python, ~250 KB)
  [parquet]     → pyarrow>=14.0                            (Parquet + Feather/Arrow IPC + ORC; ~27 MB, NOT in [all])
  [orchestrate] → typer>=0.9.0, prefect>=2.10.0           (CLI + Prefect workflow nodes)
  [docs]        → pdoc>=14.0                                (auto-build API reference site; contributor-only)
  [all]         → orjson, lxml, cramjam, fastavro, openpyxl, typer, prefect
                  ⚠️ [all] deliberately excludes pyarrow (~27 MB → use [parquet]) AND
                  pdoc (contributor-only → use [docs]). Users opt in explicitly.

2. LOCKED FILE TREE (Source of Truth)
Do not invent files. Only the following source files exist:

incorporator/
  __init__.py              # Public API surface — exports only
  base.py                  # Incorporator — thin orchestrator; public API classmethods +
                           #   Pydantic lifecycle only; delegates to schema/, list.py, io/,
                           #   observability/, tools/, usercode.py
                           # Public API: incorp(), refresh(), export(), stream(), fjord(), test()
  cli/                     # CLI subpackage (was four cli_*.py files at root).
    __init__.py            # Typer entry point. Commands: stream, fjord, validate, init.
                           # Flags: --logs, --json-output, --heartbeat-file, --poll.
                           # _load_pipeline_config parses JSON then runs env expansion.
                           # SIGTERM handler installed inside _run_stream / _run_fjord —
                           # triggers shutdown_event so the engine drains gracefully.
                           # pyproject.toml entry point resolves `incorporator.cli:main`
                           # here.
    envexpand.py           # expand_env() — recursive walker + regex substituter.
                           # ${VAR}, ${VAR:-default}, ${VAR:?msg}, ${file:/path}, and
                           # $${LITERAL} escape. EnvExpansionError on missing var/file.
    validate.py            # validate_stream_config(), validate_fjord_config(),
                           # autodetect_type(). Returns list[str] of human-readable
                           # error strings (empty == valid). Imports the user's
                           # code_file and reuses usercode.load_outflow_function for
                           # arity checks.
    scaffold.py            # init templates (stream / fjord). write_scaffold() refuses
                           # to overwrite existing files; raises FileExistsError.
  exceptions.py            # IncorporatorError, IncorporatorFormatError,
                           #   IncorporatorNetworkError, IncorporatorSchemaError
  list.py                  # IncorporatorList, _deduplicate_extracted — public collection
                           #   wrapper for Incorporator instances; no dependency on base.py
                           #   or schema/. Lives at root because it is a runtime collection,
                           #   not a schema artifact.
  usercode.py              # apply_code_transform(), load_outflow_function(),
                           #   pascal_case_from_stem() — filesystem loaders for user-supplied
                           #   Python (transform() and outflow() hooks). Extracted from base.py
                           #   so base.py is exclusively Incorporator class identity.
  integrations/
    __init__.py
    prefect.py             # Prefect integration (run_incorporator_stream, run_incorporator_flow)
                           # Optional — shielded behind try/except ImportError.
  tools/
    __init__.py            # Subpackage docstring describes the DX-tooling category.
    inspector.py           # _print_tree(), analyze_data(), analyze_error()
                           # ⚠️ Imports from ..exceptions (parent package).
  io/
    __init__.py
    compression.py         # CompressionType enum, decompress_data, compress_file
                           # _CRAMJAM_MODULE_MAP translates enum values to cramjam module names
                           # ⚠️ cramjam ≥2.x: decompress() returns Buffer not bytes — wrap in bytes()
                           #   Compressor.compress() returns int (bytes consumed) — drain via finish()
    fetch.py               # RateLimiter, HTTPClientBuilder, execute_request(),
                           #   resolve_source_payload(), _process_single_source(),
                           #   fetch_concurrent_payloads()
    formats.py             # FormatType enum, FORMAT_TO_PYTHON, PYTHON_TO_FORMAT,
                           #   to_python_type(), to_format_type(), convert_type(),
                           #   infer_format(), ensure_string(), serialize_nested(),
                           #   deserialize_nested(), xml_to_dict(), check_xml_security()
                           # Supported FormatType values:
                           #   JSON, NDJSON, CSV, TSV, PSV, XML, SQLITE, AVRO,
                           #   XLSX, PARQUET, FEATHER, ORC, HTML
    handlers/
      __init__.py          # _HANDLERS registry, parse_source_data(), write_destination_data()
                           #   canonical imports: FormatType from io.formats, not from here
                           #   _peek_iterable() — centralized empty-iterable guard applied before
                           #   every handler's write(); handlers themselves do not re-check for empty
      _base.py             # BaseFormatHandler (ABC), _raise_if_append_unsupported()
      binary.py            # SQLiteHandler, AvroHandler, coerce_avro_value()
      columnar.py          # ParquetHandler, FeatherHandler, OrcHandler
                           #   _arrow_type_for() — Pydantic JSON-schema property → pa.DataType
                           #   _materialize_table() — shared one-shot materializer for Feather + ORC
                           #   (Parquet streams via ParquetWriter with 1024-row row-group batches)
                           #   All three require pyarrow ([parquet] extra); append rejected for all three
      delimited.py         # CSVHandler (handles CSV, TSV, PSV via delimiter param)
      markup.py            # HTMLHandler (parse-only; requires lxml via [speedups])
                           #   _extract_rows_from_table() — XPath-based, skips blank rows
                           #   kwargs: table_index=0 (default), N for Nth table, -1 for all flattened
      spreadsheet.py       # ExcelHandler (.xlsx read/write; requires openpyxl via [xlsx])
                           #   parse(): load_workbook(read_only=True, data_only=True), first sheet,
                           #     row 1 as headers; uses deserialize_nested on read
                           #   write(): Workbook(write_only=True), streams via ws.append()
                           #   append rejected (_raise_if_append_unsupported)
      text.py              # JSONHandler, NDJSONHandler, XMLHandler, _build_xml_root()
                           #   ⚠️ XMLHandler.parse() calls check_xml_security() BEFORE both lxml
                           #   and stdlib parser paths — defense-in-depth; lxml resolve_entities=False
                           #   silently drops XXE rather than raising, so the check must come first
    pagination/
      __init__.py          # Re-exports all 8 paginators
      base.py              # AsyncPaginator (ABC), _deserialize_row()
      local.py             # SQLitePaginator, CSVPaginator, AvroPaginator
      web.py               # LinkHeaderPaginator, CursorPaginator, OffsetPaginator,
                           #   PageNumberPaginator, NextUrlPaginator
  observability/
    __init__.py
    logger.py              # AuditResult (now with .log_meta()), JSONFormatter (now also
                           #   serialises an `audit` extra), APIFilter, StandardFilter,
                           #   setup_class_logger(), LoggingMixin, LoggedIncorporator
                           #   (now overrides fjord too). _route_audit_to_log() routes
                           #   per-tick audits to error/info; _redact() scrubs query-string
                           #   auth in failed_sources before logging.
    pipeline/              # Package (was monolithic pipeline.py, split for readability).
      __init__.py          # run_pipeline() public dispatcher + re-exports of every
                           #   sibling symbol so existing imports continue to work.
      _shared.py           # _interruptible_sleep(), _enrich_and_load(), _row_count()
      _daemons.py          # _refresh_daemon(), _export_daemon() — shared by stateful + fjord
      _outflow.py          # _outflow_daemon() — fjord-only; builds dynamic output class
                           #   via infer_dynamic_schema on every non-empty tick
      chunked.py           # _run_chunking_engine() — Engine 1: O(1)-memory paginator loop
      stateful.py          # _run_stateful_engine() — Engine 2: decoupled refresh/export
      fjord.py             # _run_fjord_engine()   — Engine 3: multi-source + outflow
                           # _refresh_daemon / _export_daemon accept optional
                           # operation_label kwarg so fjord can tag audits per-class
                           # (e.g. "fjord_refresh:Coin"). Default preserves existing behaviour.
  schema/
    __init__.py
    builder.py             # SCHEMA_REGISTRY, sanitize_json_key(),
                           #   apply_etl_transformations(), infer_dynamic_schema()
    converters.py          # inc(), calc(), calc_all(), new(), _NewSentinel,
                           #   _EachSentinel, CalcOp, CalcAllOp, RANKED_CONVERTERS
                           #   Note: flt alias removed — use float directly
    extractors.py          # link_to(), link_to_list(), pluck(), each(), join_all(),
                           #   as_list(), sum_attributes(), split_and_get()
    factory.py             # build_instances(), child_incorp() — schema-driven instance
                           #   assembly. Module-level (receives cls explicitly) so it has
                           #   no runtime dep on base.py. Lives in schema/ because both
                           #   functions read SCHEMA_REGISTRY and call infer_dynamic_schema.
    router.py              # extract_parent_data(), resolve_declarative_routing(), _get_attr()
examples/
  1_space_devs_quick_setup.py
  2_pokeapi_etl_calc.py
  3_crypto_graph_mapping.py
  4_nhtsa_post_audit.py
  5_data_lake_pivot.py
scripts/
  build_docs.py             # Wrapper around pdoc — writes static HTML site to ./site/
                            #   (git-ignored). Live dev server: `pdoc incorporator`.
docs/
  api_reference.md          # Landing page pointing at the pdoc-built reference. Replaced
                            #   the three duplicated prose refs (incorp/refresh/export).
  1_quick_setup.md ... 5_data_lake_pivot.md   # Narrative tutorials (kept hand-written)
  cli_and_configuration.md  # CLI guide — now includes the §6 fjord subcommand section
  formats_and_compression.md  # Includes Excel, Parquet, Feather/Arrow, ORC, HTML rows
  installation.md           # All extras documented including [xlsx], [parquet],
                            #   [orchestrate], [docs]
  streaming_and_pagination.md / deployment.md / THANK_YOU.md
tests/
  conftest.py
  test_binary_handlers.py / test_cli.py / test_compression.py / test_converters.py
  test_format_boundaries.py / test_format_parsers.py / test_local_json.py / test_logger.py
  test_paginators.py / test_post_tokens.py / test_prefect_nodes.py
  test_refresh_etl.py / test_stream.py
  test_security.py      # XXE payloads, TAR path traversal, safe XML/TAR pass-through
  test_validation.py    # export() isinstance guard, transform arity, schema drift,
                        #   _schema_union concurrent safety, sibling isolation, in-state export
  test_handlers_xlsx.py     # ExcelHandler round-trip, header inference, mixed types,
                             #   missing-dep error path (13 tests; importorskip openpyxl)
  test_handlers_parquet.py  # ParquetHandler round-trip, schema-hint write, missing-dep
                             #   error path (12 tests; importorskip pyarrow)
  test_handlers_feather.py  # FeatherHandler round-trip, compression kwarg, missing-dep
                             #   error path (10 tests; importorskip pyarrow)
  test_handlers_orc.py      # OrcHandler round-trip, missing-dep error path
                             #   (9 tests; importorskip pyarrow + importorskip pyarrow.orc)
  test_handlers_html.py     # HTMLHandler table extraction, header detection, multi-table
                             #   selection, missing-dep error path (12 tests; importorskip lxml)
  test_fjord.py             # fjord() multi-source streaming: 2-source combine, per-stream
                             #   export, combine-error path, code_file validation, stream_params
                             #   validation (7 tests)
  public/api/
    test_coingecko_etl.py / test_nascar_fantasy_etl.py / test_pokemon_etl.py
    test_rick_and_morty_etl.py / test_shady_jimmy.py / test_swapi_etl.py
  real/   ⚠️ MANUAL RUNNERS — not pytest. Do not refactor into test_ format.
  benchmarks/
    test_parquet_throughput.py    # 500k rows → assert ≥100k rows/sec; Parquet 4× smaller than NDJSON
    test_columnar_throughput.py   # Feather ≥100k rows/sec; ORC ≥100k rows/sec; Feather vs. Parquet size

3. GROUNDING PROTOCOL (Anti-Hallucination Rules)
You are FORBIDDEN from:

Inventing functions, classes, or variables not listed above
Adding external dependencies to the core install (pydantic>=2.0, httpx, tenacity)
Adding optional deps outside the established extras flags ([speedups], [avro], [xlsx], [parquet], [orchestrate])
Touching tests/public/api/real/ — these are manual exploration scripts, not test files
Renaming any public API method (incorp, refresh, export, getError)
Changing Incorporator or IncorporatorList public interface without explicit instruction
Deleting or modifying examples/ files (they are documentation by example)
Before proposing any change you MUST:

State which file(s) are affected
Confirm the function/class already exists in the locked file tree
Use NotImplementedError stubs when mapping boundaries before writing logic
Import layer rules (one-way dependency chain):

list.py         may import from nothing in incorporator — pure stdlib only
usercode.py     may import from nothing in incorporator — pure stdlib only
schema/factory.py may import from schema/ siblings and ..list — never from base.py at runtime
                  (Incorporator referenced only under TYPE_CHECKING)
io/             may import from schema/ and exceptions.py — never from observability/
schema/         may import from exceptions.py and list.py — never from io/ or observability/
observability/  may import from base.py — never from io/ or schema/ directly
                (the outflow daemon imports schema.builder inside the function body to keep
                 the module-level dep graph clean)
tools/          may import from anywhere — it is leaf-level DX feedback
integrations/   may import from anywhere — leaf-level orchestrator shims
base.py         imports from all layers + schema/factory + list.py + usercode + tools;
                it is the only permitted orchestrator
Circular imports are impossible by design — if you need to cross a boundary upward, you have the wrong file

4. ARCHITECTURAL PILLARS (Enforced Invariants)
(unchanged — Pillars A–E as before)

5. AGENT OPERATING MODES
MODE A: REFACTOR
(unchanged)

MODE B: REVIEW / AUDIT
(unchanged)

MODE C: TEST WRITING
Rules:

Always use monkeypatch.chdir(tmp_path) to prevent log file pollution
Network mocks MUST use signature async def mock_fn(url: str, *args, **kwargs) — never rigid signatures
Mock target is incorporator.io.fetch.execute_request (not _execute_get, not the old methods.network path)
⚠️ Use monkeypatch.setattr("incorporator.io.fetch.execute_request", mock_fn) — string path must match the new layout
Unit tests go in tests/test_*.py; integration tests in tests/public/api/test_*.py
Each test must have a docstring stating what behavior it proves
Tests touching _ACTIVE_LISTENERS, inc_dict, or _auto_counter MUST evict stale state at the start
Optional-dep handler tests MUST open with pytest.importorskip("<dep>") — the test suite must pass on a bare install
MODE D: FEATURE ADDITION
Rules:

New format handlers → incorporator/io/handlers/
  - Text/markup formats → text.py or markup.py
  - Columnar/binary formats → columnar.py or binary.py
  - Spreadsheet formats → spreadsheet.py
  - New handler must be registered in handlers/__init__.py _HANDLERS dict
  - New FormatType value + infer_format() extension → io/formats.py
  - New optional dep → pyproject.toml [project.optional-dependencies]
  - If dep is heavy (>5 MB wheel), do NOT add to [all] — make it a standalone extra only
New ETL converters (type coercion, math) → incorporator/schema/converters.py
New extraction/graph tokens → incorporator/schema/extractors.py
New compression algorithms → incorporator/io/compression.py
New paginators → incorporator/io/pagination/web.py or local.py
New observability → incorporator/observability/
New Incorporator-coupled factory logic (uses cls + IncorporatorList) → incorporator/schema/factory.py
New collection utilities (no incorporator imports) → incorporator/list.py
New filesystem loaders for user-supplied .py hooks → incorporator/usercode.py
New DX tooling (analyzers, diff'ers, scaffolders) → incorporator/tools/
New optional orchestrator integrations (Dagster, Airflow, …) → incorporator/integrations/
Export any new public symbols via incorporator/__init__.py and add to __all__

6. FORMAT HANDLER REFERENCE

| Format     | Handler         | File            | Extras needed | Append | Streaming write |
|------------|-----------------|-----------------|---------------|--------|-----------------|
| JSON       | JSONHandler     | text.py         | none (orjson optional) | ✗  | ✓ (one row at a time) |
| NDJSON     | NDJSONHandler   | text.py         | none          | ✓      | ✓               |
| CSV/TSV/PSV| CSVHandler      | delimited.py    | none          | ✓      | ✓               |
| XML        | XMLHandler      | text.py         | lxml optional | ✗      | ✗ (DOM)         |
| SQLite     | SQLiteHandler   | binary.py       | none          | ✓      | ✓               |
| Avro       | AvroHandler     | binary.py       | [avro]        | ✗      | ✓ (generator)   |
| Excel      | ExcelHandler    | spreadsheet.py  | [xlsx]        | ✗      | ✓ (ws.append)   |
| Parquet    | ParquetHandler  | columnar.py     | [parquet]     | ✗      | ✓ (row groups)  |
| Feather    | FeatherHandler  | columnar.py     | [parquet]     | ✗      | ✗ (one-shot)    |
| ORC        | OrcHandler      | columnar.py     | [parquet]     | ✗      | ✗ (one-shot)    |
| HTML       | HTMLHandler     | markup.py       | [speedups]    | n/a    | parse-only      |

Columnar format notes:
- ParquetHandler: streams via pq.ParquetWriter with 1024-row row-group batches — O(1) memory regardless of dataset size.
- FeatherHandler / OrcHandler: no streaming writer API in pyarrow; uses _materialize_table() helper — entire dataset in RAM. For large datasets (>available RAM), use Parquet instead.
- Two-mode schema strategy (Parquet + Feather + ORC):
    1. pydantic_schema kwarg present → build explicit pa.schema() from Pydantic JSON-schema type bridge (fastest, deterministic types)
    2. No hint → native pyarrow inference from first batch (slightly slower, always type-correct)
- Nested types (list/dict): flattened to JSON strings via serialize_nested() on write; restored via deserialize_nested() on read.
- ORC on Windows: pyarrow.orc support may require pyarrow built from source. The handler reports a clear ImportError if pyarrow.orc fails even though pyarrow itself loaded.

HTML handler notes:
- Parse only — HTML write is intentionally out of scope.
- Default: extracts first <table> on the page (table_index=0).
- table_index=N: selects the Nth table (0-indexed).
- table_index=-1: flattens ALL tables on the page into one stream.
- Header detection: first <tr> with <th> cells; falls back to row-1 <td> cells if no <th>.
- Skips fully blank rows (formatting noise).

7. SESSION STATE
Current version: 1.0.8
Last worked on: May 2026 — CLI + Docker production-readiness for the big
  release. New commands: `incorporator validate` (config schema check, no
  execution) and `incorporator init` (writes pipeline.json + outflow.py
  scaffolds). New flags: `--json-output` (NDJSON audit on stdout, status to
  stderr) and `--heartbeat-file` (touch a file each audit; paired with the
  Docker HEALTHCHECK). Env-var interpolation in pipeline.json: `${VAR}`,
  `${VAR:-default}`, `${VAR:?msg}`, `${file:/run/secrets/...}` (for k8s /
  Docker Swarm Secret mounts). LoggedIncorporator now overrides fjord;
  every audit's full Pydantic dump rides on every log record under an
  `audit` JSON key so `get_error()` consumers get structured data. Audit
  query-string credentials redacted before logging. SIGTERM handler in the
  CLI for deterministic graceful shutdown across runtimes. docker-compose.yml
  + .env.example added; Dockerfile gets a HEALTHCHECK and heartbeat-file
  default. Five new CLI-first examples in examples/ + a fjord outflow demo.
  Prior session: package architecture cleanup — factory.py → schema/factory.py
  (schema-driven assembly belongs alongside builder/router). Three filesystem loaders
  (apply_code_transform, load_outflow_function, pascal_case_from_stem) extracted from
  base.py → usercode.py. observability/pipeline.py (646 LOC) split into a package with
  3 engine modules (chunked/stateful/fjord), shared helpers (_shared.py), and the
  refresh/export/outflow daemons (_daemons.py + _outflow.py). prefect_nodes.py →
  integrations/prefect.py (new home for optional orchestrator shims).
  inspector.py → tools/inspector.py with a new tools/ subpackage describing future
  DX-tooling additions (profiler, schema_diff, exporter). formats.to_format_type →
  _to_format_type (zero external callers).  Public import surface unchanged. mypy
  strict clean (41 files). pytest: 291 passed.
  Prior session: fjord() refactored to eliminate the user-defined output class:
  outflow(state) function returns list[dict], fjord builds the dynamic Pydantic
  class from those dicts (same path incorp() uses), class name is derived from
  code_file stem (snake_case → PascalCase). Empty outflow() emits zero-row audit and
  skips export. Earlier: pdoc-generated API reference replaces three duplicated
  prose refs (incorp/refresh/export). Earlier: fjord() multi-source streaming + format
  expansion + cramjam 2.x + XMLHandler XXE hardening.

Test suite: 276 passed, 5 deselected (benchmark/slow markers), 87% coverage.

Benchmark results (all 10 pass on current hardware):
  JSON write:     ~663k rows/sec
  NDJSON write:   ~133k rows/sec
  Feather write:  ~202k rows/sec  (floor: 100k rows/sec)
  ORC write:      ~267k rows/sec  (floor: 100k rows/sec)
  Parquet write:  ~220k rows/sec  (floor: 100k rows/sec)
  Parquet file size: ~4× smaller than equivalent NDJSON

Known issues to address (do not fix without being asked):

base.py → export() first arg overloaded as file path and instance — confusing when passing a list
io/pagination/web.py → web paginators swallow all errors silently unless strict_mode=True
integrations/prefect.py → autouse=True session fixture pollutes the test suite
integrations/prefect.py → now under integrations/ (was prefect_nodes.py at root).
  Future orchestrator shims (Dagster, Airflow, Temporal) drop in next to it.
__init__.py → sum_attributes not exported from public API
Test fragility → string-based monkeypatches silently break on rename — consider patch.object instead
fetch.py → _safe_execute and _sliding_worker are still closures capturing outer state;
  same refactor opportunity now applied to pipeline/ (lift to module-level with explicit params)
ORC on Windows → pyarrow.orc import may fail even with pyarrow installed; OrcHandler
  reports a clear message but there is no fallback format

Fixed (do not re-open):
✅ parse_source_data now raises IncorporatorFormatError; fetch.py _safe_execute catches it → failed_sources populated
✅ CSVHandler.write uses all_field_names kwarg + full-scan fallback; extrasaction="ignore"
✅ orjson parse errors wrapped in IncorporatorFormatError (text.py JSONHandler)
✅ AvroHandler.write uses _schema_union superset via pydantic_schema kwarg — all fields from all records
✅ _auto_counter wrapped in threading.Lock — safe across asyncio.to_thread workers
✅ base.py docstring updated (no longer references methods/ directory)
✅ flt alias removed from schema/converters.py — use float directly
✅ FormatType/infer_format removed from handlers/__init__.__all__ — import from io.formats
✅ Dockerfile ENTRYPOINT/CMD spacing fixed; CLI incorp + export commands added (Typer)
✅ __init__.py conditionally exports run_incorporator_flow/stream (try/except ImportError)
✅ factory.py _schema_union population wrapped in per-class threading.Lock (double-checked locking)
✅ IncorporatorList made generic: List[T] base — IncorporatorList[MyModel] is now valid
✅ mypy --strict clean: 0 errors across 29 source files
✅ export() isinstance guard: raises TypeError when instance is not list/BaseModel and file_path provided
✅ _apply_code_transform() arity check: raises ValueError when transform() has != 1 parameter
✅ export() code_file schema drift: peeks first transformed row to rebuild all_field_names
✅ refresh() empty inst_list: emits logger.warning instead of silently returning
✅ check_xml_security() regex: catches parameter entities (%xxe;) with re.DOTALL flag
✅ _validate_tar_members() added to compression.py: blocks dotdot and absolute path traversal
✅ _assert_router_coverage() raises RuntimeError at import if any CompressionType missing from routers
✅ RateLimiter.wait() burst fix: last_call set before sleep, not after
✅ HTTP client creation moved inside try block in fetch_concurrent_payloads (no leak on early exception)
✅ logger.py eviction: warning emitted before old_listener.stop() when MAX_LOG_THREADS reached
✅ extractors.py link_to: logs every strong-ref fallback miss (removed if not fallback_registry guard)
✅ converters.py inc(datetime): Z-suffix fix uses endswith("Z") — mid-string Z no longer corrupts
✅ schema/router.py: itertools.repeat() replaces source_urls * N list multiplication
✅ pipeline.py run_pipeline decomposed: _row_count, _refresh_daemon, _export_daemon,
     _run_stateful_engine, _run_chunking_engine are module-level; run_pipeline is ~20-line dispatcher
✅ git history rewritten: all commits use PyPlumber <noreply@github.com>
✅ Git identity set locally: git config user.name "PyPlumber" / user.email "noreply@github.com"
✅ cramjam ≥2.x compatibility: decompress() returns Buffer, not bytes — all call sites wrap in bytes().
     Compressor.compress() returns int (bytes consumed), not bytes — return value is discarded;
     output drained via bytes(compressor.finish()) or bytes(compressor.flush()) as available.
✅ XMLHandler XXE security hardened: check_xml_security() now called BEFORE both lxml and stdlib
     parser paths. lxml resolve_entities=False silently drops XXE entities instead of raising —
     without the pre-check, attacks were silently swallowed and test_security.py would fail.
✅ ExcelHandler added (spreadsheet.py): .xlsx read/write via openpyxl; [xlsx] extra (~250 KB);
     included in [all]; parse-only first sheet; row 1 as headers; streaming write via ws.append();
     serialize_nested/deserialize_nested for list/dict round-trip; append rejected.
✅ ParquetHandler added (columnar.py): .parquet/.pq read/write via pyarrow; [parquet] extra (~27 MB);
     deliberately excluded from [all]; streaming write via ParquetWriter with 1024-row row groups
     (O(1) memory); two-mode schema (Pydantic hint → explicit Arrow schema, no hint → native inference);
     serialize_nested/deserialize_nested for nested type round-trip; append rejected.
✅ FeatherHandler added (columnar.py): .feather/.arrow/.ipc read/write via pyarrow; same [parquet]
     extra; one-shot write via _materialize_table() + feather.write_feather(); LZ4 compression
     default (feather_compression kwarg); append rejected.
✅ OrcHandler added (columnar.py): .orc read/write via pyarrow.orc; same [parquet] extra; one-shot
     write via _materialize_table() + orc.write_table(); Windows platform note documented; append
     rejected.
✅ HTMLHandler added (markup.py): parse-only; .html/.htm; requires lxml ([speedups]); extracts
     <table> elements; table_index kwarg (0=first, N=Nth, -1=all flattened); <th>-based header
     detection with <td> fallback; blank-row skipping; write raises IncorporatorFormatError.
✅ All 5 new format handlers registered in handlers/__init__.py _HANDLERS dispatch dict.
✅ FormatType enum extended: XLSX, PARQUET, FEATHER, ORC, HTML values added to formats.py.
✅ infer_format() extended: .xlsx/.xlsm → XLSX; .parquet/.pq → PARQUET; .feather/.arrow/.ipc →
     FEATHER; .orc → ORC; .html/.htm → HTML.
✅ FORMAT_TO_PYTHON / PYTHON_TO_FORMAT type bridge tables extended with Parquet/Feather/ORC
     Arrow logical types (bool, int32, int64, float, double, string, binary, null).
✅ pyproject.toml updated: xlsx and parquet extras added; openpyxl added to [all]; pyarrow
     deliberately excluded from [all] with inline comment.
✅ Benchmark suite extended: test_parquet_throughput.py (Parquet 500k rows, size vs. NDJSON);
     test_columnar_throughput.py (Feather 500k rows, ORC 500k rows, Feather vs. Parquet size).
     All 10 benchmark assertions pass (floors: columnar ≥100k rows/sec, JSON ≥500k rows/sec,
     NDJSON ≥50k rows/sec).
✅ fjord() multi-source streaming added (base.py): classmethod that ingests from N
     Incorporator subclasses concurrently, exposes their inc_dict registries to a
     user combine(state) function, builds new instances of the calling class from
     the combined rows, and exports via the existing export() pipeline. Stateful
     polling only. Per-source refresh and optional per-source export daemons. Single
     combine-and-export daemon on its own clock. Audit operations tagged per-class:
     "fjord_incorp:<Cls>", "fjord_refresh:<Cls>", "export:<Cls>", "combine".
✅ _run_fjord_engine + _combine_daemon added to pipeline.py. Reuses _refresh_daemon
     and _export_daemon (extended with optional operation_label kwarg, default
     preserves prior behaviour). Strong reference to the combined snapshot held on
     cls._fjord_snapshot so cls.inc_dict (WeakValueDictionary) stays populated
     between ticks — required by the "object map" UX contract.
✅ _load_combine_function() in base.py: mirrors _apply_code_transform's importlib
     pattern but for a top-level combine(state) function. Enforces 1-parameter arity.
     Raises FileNotFoundError / ImportError / ValueError on missing file, missing
     function, or wrong arity.
✅ CLI fjord subcommand added (cli.py): `incorporator fjord pipeline.json`. JSON config
     contains code_file (path), output_class (str), stream_params (list of dicts with
     cls_name + incorp_params), export_params, refresh_interval, export_interval. The
     CLI imports code_file once via _load_user_module() and resolves every class name
     via _resolve_incorporator_class() (validates Incorporator subclass; clear errors
     on missing class or wrong type).
✅ pdoc-generated API reference added ([docs] extra → pdoc>=14.0, contributor-only,
     deliberately excluded from [all]). scripts/build_docs.py wraps the pdoc CLI to
     write a static HTML site under ./site/ (git-ignored). Live dev server: pdoc
     incorporator. Replaces docs/incorp_reference.md, docs/refresh_reference.md, and
     docs/export_reference.md — the three duplicated prose refs whose content already
     lived in the Google-style docstrings. New docs/api_reference.md is a short
     landing page pointing at the pdoc output.
✅ Prose docs updated to current functionality (May 2026 docs sweep):
     - cli_and_configuration.md: §6 documents the fjord subcommand (JSON schema,
       combine() example, stream-vs-fjord decision table).
     - formats_and_compression.md: rows added for Excel, Parquet, Feather/Arrow,
       ORC, HTML; XXE hardening note on XML.
     - installation.md: entries added for [xlsx], [parquet], [orchestrate], [docs];
       dead "Next Steps" link fixed (now points at docs/1_quick_setup.md).
     - streaming_and_pagination.md: callout pointing single-source readers at
       fjord() for multi-source pipelines.
     - README.md: Holy Trinity bullets and Docs section consolidated to point at
       the single api_reference.md.
✅ docs/ duplicated prose refs deleted: incorp_reference.md, refresh_reference.md,
     export_reference.md. Grep confirms zero stale references in repo.
✅ fjord() refactored (May 2026 follow-up): the developer no longer declares an
     output Incorporator subclass. The user's function is now `outflow(state)` and
     returns `list[dict]` (or a single dict, auto-wrapped). fjord builds the dynamic
     Pydantic class from those dicts via `infer_dynamic_schema(name, rows, base)` —
     the same path incorp() uses after parsing — keyed by `(name, frozenset(keys),
     id(base))` so successive ticks with the same shape cache-hit and reuse the
     class. Class name is derived from `code_file` stem via
     `_pascal_case_from_stem()`: `coin_market.py` → `CoinMarket`.
     CLI: `output_class` JSON key removed; CLI now calls `Incorporator.fjord(...)`
     directly (single classmethod entry point on the base class). Audit operation
     renamed `combine` → `outflow:<ClassName>`. Empty outflow() returns emit a
     `rows_processed=0` audit and skip export (no dynamic class built that tick).
     Error tag `Combine Error` → `Outflow Error`.
     New helpers in base.py: `_pascal_case_from_stem(code_file: Path) -> str` (with
     unit tests in test_fjord.py); `_load_combine_function` renamed to
     `_load_outflow_function` (looks for top-level `outflow`, same arity check).
     Strong-ref `_fjord_snapshot` trick survives unchanged — now lives on the
     dynamic class. test_fjord.py rewritten to find the dynamic class via
     SCHEMA_REGISTRY lookup; CoinMarket(Incorporator) definition removed from the
     test module entirely.

8. QUICK REFERENCE — KEY PATTERNS
Correct mock + monkeypatch (always):

async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> dict:
    ...

monkeypatch.setattr("incorporator.io.fetch.execute_request", mock_execute_request)
(all other patterns unchanged)

**Correct mock signature (always):**
```python
async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> dict:
    ...

# Correct monkeypatch target (always use the new io.fetch path):
monkeypatch.setattr("incorporator.io.fetch.execute_request", mock_execute_request)
```

**Correct conv_dict loop (always):**
```python
for key, converter in conv_dict.items():
    value = item.get(key, None)  # never item[key]
    ...
```

**Correct test isolation (always):**
```python
def test_something(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    ...
```

**Correct codeDict type (always):**
```python
codeDict: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
```

**Correct kwargs isolation for concurrent workers (always):**
```python
return await _process_single_source(
    src, is_file_mode, _client, _rate_limiter, dynamic_payload=payload, **dict(kwargs)
)
```

**Correct Pydantic V2 dynamic field access (always):**
```python
def _get_attr(node: Any, part: str) -> Any:
    if isinstance(node, dict):
        return node.get(part)
    pydantic_extra = getattr(node, "__pydantic_extra__", None)
    if pydantic_extra and part in pydantic_extra:
        return pydantic_extra[part]
    return getattr(node, part, None)
```

**Correct schema cache key (always):**
```python
cache_key = (
    model_name,
    frozenset((k, type(v).__name__) for k, v in sample_dict.items()),
    id(base_class),
)
```

**Correct logger test isolation (always):**
```python
if "MyClassName" in _ACTIVE_LISTENERS:
    _ACTIVE_LISTENERS["MyClassName"].stop()
    del _ACTIVE_LISTENERS["MyClassName"]
setup_class_logger(MyClassName)
```

**Correct cramjam module lookup (always):**
```python
module_name = _CRAMJAM_MODULE_MAP.get(comp_type)
cj_module = getattr(cramjam, module_name, None)
```

**Correct cramjam decompress (cramjam ≥2.x returns Buffer, not bytes):**
```python
raw_bytes = bytes(cj_module.decompress(f.read()))   # Path source
raw_bytes = bytes(cj_module.decompress(data))        # bytes source
# WRONG (broke on cramjam 2.x):
raw_bytes = cj_module.decompress(data)
```

**Correct cramjam compress (cramjam ≥2.x Compressor.compress() returns int, not bytes):**
```python
compressor = cj_module.Compressor()
for chunk in _iter_chunks(source_path):
    compressor.compress(chunk)          # discard return value (it's an int, bytes consumed)
if hasattr(compressor, "finish"):
    f_out.write(bytes(compressor.finish()))
elif hasattr(compressor, "flush"):
    f_out.write(bytes(compressor.flush()))
# WRONG (broke on cramjam 2.x):
f_out.write(compressor.compress(chunk))
```

**Correct optional-dep handler pattern (lazy import, always):**
```python
class MyHandler(BaseFormatHandler):
    def parse(self, source, **kwargs):
        try:
            import some_optional_dep
        except ImportError:
            raise IncorporatorFormatError(
                "some_optional_dep not installed. Run: pip install incorporator[extra_flag]"
            ) from None
        ...
    def write(self, data, file_path, **kwargs):
        _raise_if_append_unsupported(kwargs, "FormatName")  # if append unsupported
        try:
            import some_optional_dep
        except ImportError:
            raise IncorporatorFormatError(...) from None
        ...
```

**Correct XML security check (always before parsing, both parser paths):**
```python
def parse(self, source, **kwargs):
    # Must run BEFORE lxml or stdlib — lxml resolve_entities=False silently drops XXE.
    raw_str = source.read_text(encoding="utf-8") if isinstance(source, Path) else ensure_string(source)
    check_xml_security(raw_str)
    try:
        import lxml.etree as lxml_ET
        ...
    except ImportError:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(raw_str)  # reuse raw_str — already read and security-checked
        ...
```

**Correct stateful polling shutdown (always):**
```python
shutdown_event = asyncio.Event()
# In daemons: while not shutdown_event.is_set()
# In sleep: await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
# In finally: shutdown_event.set() then cancel tasks
```

**Correct pipeline.py daemon pattern (always module-level, never closures):**
```python
# RIGHT — explicit parameters, independently testable
async def _refresh_daemon(cls, dataset_ref, refresh_params, lock, audit_queue, shutdown_event, r_interval):
    ...

# WRONG — closure capturing outer scope silently
async def _refresh_daemon():  # captures lock, dataset_ref, etc. from enclosing run_pipeline
    ...
```

**Correct security checks (never remove or weaken):**
```python
# XML — always call before parsing, before both lxml and stdlib paths
check_xml_security(raw_data)          # blocks DOCTYPE, ENTITY, %param_entity;

# TAR — always call before _find_target_in_archive
_validate_tar_members(members)        # blocks ../../ and /absolute paths

# Router — called at module import, raises RuntimeError if any CompressionType missing
_assert_router_coverage()             # in compression.py, after both router dicts
```

**Correct IncorporatorList usage (always parameterise):**
```python
# RIGHT
result: IncorporatorList[MyModel] = IncorporatorList(MyModel, items)
def foo() -> Union[MyModel, IncorporatorList[MyModel]]: ...

# WRONG — bare IncorporatorList is now a mypy error (generic type missing type arg)
result: IncorporatorList = ...
```

**Correct assert replacement (S101 enforced by ruff):**
```python
# WRONG — stripped by -O, flagged by ruff S101
assert not missing, f"..."

# RIGHT — fires unconditionally, survives optimised builds
if missing:
    raise RuntimeError(f"...")
```

**Correct fjord() multi-source streaming (always — stateful polling only):**
```python
# RIGHT — no user-defined output class. The dynamic output class is built from the
# rows outflow() returns and named after the code_file stem (snake_case → PascalCase).

async for audit in Incorporator.fjord(   # always called on the base class
    stream_params=[
        {"cls": Coin, "incorp_params": {...}, "refresh_params": {}},
        {"cls": BinanceFutures, "incorp_params": {...}, "refresh_params": {}},
    ],
    code_file="coin_market.py",          # REQUIRED — defines outflow(state); stem → "CoinMarket"
    export_params={"file_path": "out.ndjson"},  # REQUIRED — joined output destination
    refresh_interval=60.0,
    export_interval=300.0,
):
    print(audit.operation, audit.rows_processed)

# coin_market.py:
def outflow(state):
    # state: Dict[ClassName, IncorporatorList]
    coins   = state["Coin"]
    futures = state["BinanceFutures"]
    return [{"inc_code": c.inc_code, "coin_name": c.name, ...} for c in coins if ...]

# To reach the dynamic class in tests:
from incorporator.schema.builder import SCHEMA_REGISTRY
CoinMarket = next(cls for (name, *_), cls in SCHEMA_REGISTRY.items() if name == "CoinMarket")
```

**Correct fjord audit operation tags (always):**
```python
# Seed phase (one per source):           "fjord_incorp:Coin", "fjord_incorp:BinanceFutures"
# Per-source refresh:                    "fjord_refresh:Coin"
# Per-source optional export:            "export:Coin"
# Combined-output daemon:                "outflow:<DynamicClassName>"  e.g. "outflow:CoinMarket"
```

**Correct optional-dep test guard (always first line in optional-dep test files):**
```python
pytest.importorskip("openpyxl")   # test_handlers_xlsx.py
pytest.importorskip("pyarrow")    # test_handlers_parquet.py, test_handlers_feather.py
                                  # test_handlers_orc.py, test_columnar_throughput.py
pytest.importorskip("lxml")       # test_handlers_html.py
# For ORC specifically, also add:
pytest.importorskip("pyarrow.orc")
```
