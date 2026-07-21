# CLI & Pipeline Configuration

The Incorporator CLI runs the same `stream` / `fjord` / `tideweaver` verbs from a JSON config file. For `stream` pipelines, no Python sidecar is required — the JSON config alone is sufficient. `fjord` and `tideweaver` pipelines also accept a Python `outflow.py` for join logic and custom classes.

Define your extraction, enrichment, and loading rules in a JSON file and run long-running polling daemons in the background — suited for Docker containers, systemd services, or scheduled scraping jobs.

## 1. Prerequisites
To use the CLI, install Incorporator with the `[cli]` extra:
```bash
pip install "incorporator[cli]"
```

> **What `[cli]` installs.** This extra bundles `typer>=0.9.0`, the only
> dependency required for the `incorporator` CLI entry point. If you also
> want the Prefect `@flow` wrapper (`incorporator.integrations.prefect`),
> install `incorporator[orchestrate]` instead — it bundles `typer>=0.9.0`
> and `prefect>=2.10.0` together.
>
> The dedicated `[cli]` extra ships in the next release — current PyPI
> is 1.4.2.

---

## 2. The Configuration File (`pipeline.json`)

The CLI is driven entirely by a JSON configuration file. This file maps directly to the trinity verbs (`incorp`, `refresh`, `export`) and lets you toggle the execution engine.

Here is a standard `pipeline.json` designed to scrape an API, enrich it, and append it to a local CSV file:

```json
{
  "stateful_polling": false,
  "incorp_params": {
    "inc_url": "https://jsonplaceholder.typicode.com/posts",
    "inc_code": "id",
    "excl_lst": ["userId"]
  },
  "refresh_params": {
    "http_method": "GET",
    "delay_between_batches": 1.0
  },
  "export_params": {
    "file_path": "data/output.csv",
    "if_exists": "append"
  }
}
```

### Parameter Breakdown:
*   **`stateful_polling` (Optional, defaults to `false`):** Selects the execution mode (see *Stream Modes* below).
*   **`incorp_params` (Required):** Dictates the initial extraction. Must contain an origin (e.g., `inc_url`, `inc_file`, or `inc_page`).
*   **`refresh_params` (Optional):** If provided, the daemon will use the objects mapped during extraction to execute stateful updates (e.g., fetching deep relational data).
*   **`export_params` (Optional):** Dictates the load phase. Mode-aware defaults:
    *   In **chunking mode** (`stateful_polling=false`) each chunk is *new* data,
        so the engine auto-injects `if_exists="append"` after chunk 1 on
        append-friendly formats (NDJSON / CSV / SQLite / Avro).
    *   In **stateful-polling mode** (`stateful_polling=true`) every wave
        re-exports the *same* registry, so the engine always REPLACES the
        file with the latest snapshot — appending would duplicate rows.
        Set `"if_exists": "append"` explicitly to opt into a forensic ledger
        that grows on every wave.

---

## 3. Stream Modes

`stream()` has **one engine** (chunking) with a shim that adapts `stateful_polling=true`
onto the fjord engine.  The user-facing surface is the same in both modes — same
wave shape, same kwarg vocabulary — but the underlying execution differs:

### Chunking mode (`"stateful_polling": false`)
*   **Best for:** 10M+ row databases, massive CSV files, or heavily paginated APIs.
*   **Behavior:** Each chunk is fetched, enriched, written to disk, and released
    before the next chunk is fetched — keeping the live object count bounded to
    one chunk at a time. User outflow callbacks that hold external references
    break this bound; in-built export paths do not.

### Stateful mode (`"stateful_polling": true`)
*   **Best for:** Live dashboards, 100 stock tickers, or 50 IoT sensors.
*   **Behavior:** Runs extraction (`incorp`) **exactly once** to build the object graph
    in RAM. On subsequent polling intervals, it skips extraction and `refresh()`es the
    existing objects in memory — incredibly fast and network-efficient for small datasets.
*   **Under the hood:** Routes through the fjord engine as a single-source pipeline
    with a synthesised identity outflow.  The `IncorporatorList` pass-through fast
    path in `flush()` preserves Python-object identity in `cls.inc_dict` across
    waves, so object identity is preserved between refresh cycles.

---

## 4. Running the CLI

Once your `pipeline.json` is ready, execute it from the terminal using the `incorporator stream` command.

### Standard Execution (Single Pass)
To run the pipeline exactly once and exit:
```bash
incorporator stream pipeline.json
```
**Output:**
```text
Starting Incorporator Stream...
Chunk 1 | chunk | 10000 rows | 1.84s
Chunk 2 | chunk | 10000 rows | 1.91s
```
Each wave line carries the pipeline's `operation` label — `chunk` for
chunking-mode extraction here; `fjord` sessions tag their own waves
per source (`fjord_refresh:Coin`, `outflow:CoinMarket`, see §8). A
`Stream process completed gracefully.` line is written to disk (via
`log_cls_info`) whenever `--logs` is set. That same `--logs` flag also
installs the shared root INFO handler described in §6, and this
particular logger propagates to root by default — so with `--logs`
the line also lands on stderr as
`INFO:LoggedIncorporator:Stream process completed gracefully.`,
alongside the wave lines above. Without `--logs`, only the wave lines
print; there is no disk record and no `INFO:` line.

### Daemon Execution (Infinite Polling)
To keep the pipeline alive in the background, use the `--poll` flag. This tells the daemon to wait `X` seconds after a successful run before automatically restarting the extraction/hydration cycle.
```bash
incorporator stream pipeline.json --poll 60.0
```

---

### Authentication Headers

Every kwarg accepted by your subclass's `incorp()` is also accepted under
`incorp_params` in the JSON. The most common production knob is custom
headers — auth, content negotiation, user-agents:

```json
"incorp_params": {
  "inc_url": "https://api.example.com/protected",
  "inc_code": "id",
  "headers": {
    "Authorization": "Bearer ${API_KEY}",
    "Accept": "application/json"
  }
}
```

The `${API_KEY}` reference is expanded from environment at JSON-load
time (see "Environment Variables & Secrets" below). Never put a raw
token in a checked-in `pipeline.json`.

### Machine-readable Output (`--json-output`)

Both `stream` and `fjord` accept `--json-output`, which switches stdout
to NDJSON (one `Wave` per line). The startup banner and any stop/error
messages go to **stderr** so stdout stays parseable.

```bash
incorporator stream pipeline.json --json-output | jq '.rows_processed'
```

Useful for piping into GitHub Actions, Prefect, log shippers, or any
tool that wants structured progress.

### Heartbeat for Healthchecks (`--heartbeat-file`)

When you pass `--heartbeat-file PATH`, the CLI `touch`es that file
after every wave. Pair it with the Dockerfile's `HEALTHCHECK`
instruction so the container is marked unhealthy if no waves arrive
in 2 minutes:

```bash
incorporator stream pipeline.json --poll 60 --heartbeat-file /tmp/inc.beat
```

The Dockerfile (and `docker-compose.yml`) already use
`/tmp/incorporator.heartbeat` by default — see
[`deployment.md`](deployment.md).

---

## 5. Environment Variables & Secrets

Every **string** value in `pipeline.json` is scanned for `${...}`
references at load time. Three forms:

| Syntax | Meaning |
| :--- | :--- |
| `${API_KEY}` | Required env var; load fails if unset. |
| `${API_KEY:-fallback}` | Use `fallback` if `API_KEY` is unset. |
| `${API_KEY:?explanation}` | Same as `${API_KEY}` but raises with your message. |
| `${file:/run/secrets/api_key}` | Read the file's UTF-8 contents (whitespace-stripped). For Docker Swarm / Kubernetes Secrets. |
| `$${LITERAL}` | Escape — substituted with the literal `${LITERAL}`. |

Best-practice picks:

- **Local dev**: env vars via `.env` + `docker compose --env-file`. Easy.
- **Production**: file-based references with `${file:/run/secrets/...}`
  — env vars are visible to anyone with Docker daemon access
  (`docker inspect`); mounted secret files aren't.

`incorporator validate <config.json>` runs the expansion and reports
which variable is missing — so you find out before the network call
fails with a confusing 401.

### Sandboxing `${file:...}` with `INCORPORATOR_SECRETS_ROOT`

By default, `${file:/any/absolute/path}` will read any file the process
user can access. Set `INCORPORATOR_SECRETS_ROOT` to lock `${file:...}`
references to a specific directory tree:

```bash
INCORPORATOR_SECRETS_ROOT=/run/secrets incorporator stream pipeline.json
```

Any `${file:...}` path that resolves outside the configured root raises
`EnvExpansionError` at load time — before any network call is made. The
resolved path (not the file contents) appears in the error message so
the issue is diagnosable without leaking secrets.

This sandbox is relevant whenever `pipeline.json` (or any file it
references) is not fully under your control — for example, in a
multi-tenant deployment or when the config is injected at container
start-up via an external system.

```yaml
# docker-compose.yml excerpt
environment:
  INCORPORATOR_SECRETS_ROOT: /run/secrets
secrets:
  - api_key
```

Source: `incorporator/config/envexpand.py:126-184`.

---

## Text-Form Tokens (Paginators, Converters, etc.)

> **`inflow.py` — two distinct roles.** In a stream pipeline, `inflow.py`
> is a helper file whose public symbols extend the token resolver's
> allow-list (explained in this section). In a fjord pipeline it may
> *also* define a top-level `inflow(state)` callable that seeds dependent
> sources with prior sources' live data — a separate feature covered in
> [Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md). Both roles can coexist in the
> same file; neither filename is reserved.

JSON can carry strings, numbers, lists, and dicts — but not Python
callables. So how does ``pipeline.json`` express something like
``inc_page=NextUrlPaginator("next")`` or ``conv_dict={"net": inc(datetime)}``?

**Answer: as text.** The CLI loader parses any value that looks like a
Python function-call expression, resolves it against a strict allow-list
of known classes / functions, and substitutes the real Python object
before the engine sees the config.

There are **two complementary syntaxes**, both safe-eval'd at config
load time:

### Syntax 1: `@name` references (cleanest)

Pre-build the instance in `inflow.py` and reference it by bare name in
JSON. Zero escapes, zero call grammar in the JSON.

```python
# inflow.py
from incorporator.io.pagination import NextUrlPaginator
from incorporator.schema.converters import inc

next_page = NextUrlPaginator("next")
to_datetime = inc(datetime)
```

```json
{
  "inflow": "inflow.py",
  "incorp_params": {
    "inc_url": "https://api.example.com/v1/items",
    "inc_page": "@next_page",
    "rec_path": "data",
    "conv_dict": {"created_at": "@to_datetime"}
  }
}
```

### Syntax 2: Call grammar (no sidecar file)

For trivial framework cases that don't justify an `inflow.py`, use call
grammar with **single quotes inside** so no escapes are needed:

```json
{
  "incorp_params": {
    "inc_url": "https://api.example.com/v1/items",
    "inc_page": "NextUrlPaginator('next')",
    "conv_dict": {
      "created_at": "inc(datetime)",
      "price": "inc(float)",
      "tags": "as_list()"
    },
    "form_payload": {"ids": "join_all(';')"}
  }
}
```

You can mix both — `@name` for anything non-trivial, call grammar for
one-offs.

### Allow-list

These names resolve out of the box:

| Category | Names |
|---|---|
| Paginators | `NextUrlPaginator`, `CursorPaginator`, `OffsetPaginator`, `PageNumberPaginator`, `LinkHeaderPaginator`, `SQLitePaginator`, `CSVPaginator`, `AvroPaginator` |
| Converters | `inc`, `as_list`, `join_all`, `split_and_get`, `pluck`, `sum_attributes`, `calc`, `calc_all`, `link_to`, `link_to_list` |
| Directives | `Ex`, `Nm`, `Pk` |
| Types (as args) | `datetime`, `date`, `time`, `int`, `float`, `bool`, `str`, `list`, `dict`, `tuple`, `set`, `bytes`, `None`, `True`, `False`, `new` |

> **Directive forward-compat.** `Ex` and `Nm` are user-instantiable in
> JSON pipelines: `"excl_lst": ["legacy", "Ex('audit.legacy_flag')"]`
> and `"name_chg": [["ext_id", "id"], "Nm('vendor_code', 'code')"]`
> resolve through the token system and are accepted alongside bare
> strings / 2-tuples by the framework's normalizer.  `Pk` is
> allow-listed for forward compatibility, but JSON pipelines today
> have no canonical destination slot for it — PK binding stays driven
> by `code_attr` / `name_attr` bare strings, which the framework
> synthesises into `Pk` internally at normalize time.

### User Functions via `inflow`

`calc`, `calc_all`, `link_to`, and `link_to_list` take a **user-defined**
callable or registry as their first argument. JSON alone can't carry a
Python function, so these resolve **only when you supply an `inflow.py`**
whose public symbols include the named helper.

```python
# inflow.py
def calculate_bst(stats):
    return sum(s.get("base_stat", 0) for s in stats if isinstance(s, dict))
```

```json
{
  "inflow": "inflow.py",
  "incorp_params": {
    "inc_url": "https://pokeapi.co/api/v2/pokemon/?limit=50",
    "rec_path": "results",
    "inc_code": "name",
    "conv_dict": {"stats": "calc(calculate_bst, 'stats', default=0, target_type=int)"}
  },
  "export_params": {"file_path": "data/pokemon.csv"}
}
```

> **`conv_dict` is format-agnostic.** It (and every other ETL transform —
> `excl_lst`, `name_chg`, `code_attr`, `name_attr`) runs **before** format
> dispatch in `incorporator/schema/factory.py::build_instances`. So the
> reducer's output lands in *every* output format equally — CSV, NDJSON,
> Parquet, Avro, XLSX, etc. The example above with `.csv` proves this:
> the integer from `calculate_bst` ends up as a number in the CSV cell,
> not the raw list of dicts.

The CLI imports `inflow.py` **once** per pipeline run (cached by
resolved file path in the shared user-module loader); per-chunk
operations don't re-import anything.

> **`outflow` sidecar names count too.** Every verb that shares the CLI's
> config loader — `stream`, `fjord`, and `tideweaver run`/`validate` — unions
> public names from **both** `inflow` and `outflow` (when a config declares
> both, as a stateful `stream` config with `outflow=` can) into the same
> token-resolver allow-list. An inflow helper wins over an outflow helper of
> the same name.



### What still needs an outflow / fjord pattern

A user-defined Incorporator subclass (with custom methods, computed
attributes, etc.) can't live in `inflow.py` — that file is for
helper functions consumed by the token resolver. Custom classes live in
an `outflow.py` referenced by the [fjord subcommand](#8-the-fjord-subcommand--multi-source-stateful-pipelines),
or (for single-source stateful daemons) by `stream`'s `outflow=` field
when `"stateful_polling": true`.

### Safety

The resolver uses a strict safe-eval pattern based on `ast.parse`:

* Only literals (strings, numbers, bools, None), allow-listed names,
  and calls on allow-listed names are accepted.
* **Rejected with a clear error**: attribute access, subscripts,
  imports, lambdas, comprehensions, binary operators, anything not
  in the allow-list.
* Plain strings (URLs, file paths, headers, English prose) don't
  match the shape regex and pass through unchanged.
* The `@name` grammar is single-token only — `@foo.bar`, `@foo()`, and
  bare `@` all stay as literal strings.

If you write a string that *looks* like a call (matches the shape)
but uses an unknown identifier, you get a loud error at load time
with the full allow-list printed — not a confusing downstream
failure.

---

## 6. Observability & Telemetry (`--logs`)

When running Incorporator as a background daemon (especially inside a Docker container), you need structured observability without blocking the async event loop.

By appending the `--logs` flag, you activate Incorporator's **Multiplex Disk Logging**:
```bash
incorporator stream pipeline.json --poll 3600.0 --logs
```

### What happens when `--logs` is enabled?
Wave lines keep printing to the terminal exactly as they do without the
flag — `--logs` never touches stdout. It does two things instead:

- **Disk logging.** Incorporator creates a `logs/` directory and wires
  four rotating JSON Lines handlers per session (5MB each, 3 backups —
  roughly 20MB total per log type), listed below.
- **Root diagnostics on stderr.** An INFO-level root log handler is
  installed, shared by `stream`, `fjord`, and `tideweaver run`, so
  module-logger diagnostics (drain-timeout parse warnings,
  unknown-current-key typos, source-load-failure summaries) reach the
  console instead of being silently dropped by Python's default
  no-handler behavior. This unified handler ships in the next
  release — current PyPI is 1.4.2.

1.  **`logs/{Class}_api.log`**: URL/internet-traffic errors — HTTP 4xx/5xx responses, network timeouts, and connection failures where `RejectEntry.is_url_traffic_error=True`. Use `get_api()` to read these records.
2.  **`logs/{Class}_error.log`**: All non-API-routed records at INFO and above — successful waves, parse failures, schema errors. Use `get_error()` for codebase failures; `get_rejects()` to union both files.
3.  **`logs/{Class}_debug.log`**: Superset of both files above — every record that lands in `_api.log` or `_error.log` also lands here, plus DEBUG-floor lifecycle events. Used by `get_current()` to retrieve per-session records without double-counting.
4.  **`logs/{LoggerName}_tide.log`**: Every yielded `Tide` (fired and no-op), in `tide_number` order — single-file source for `LoggedTideweaver.get_tides(logger_name)`. All four files are created for every session, `stream`/`fjord` included; only a `tideweaver run --logs` session ever writes records here — for `stream`/`fjord` the file exists and stays empty. The file name uses the resolved `logger_name` (explicit arg → `watershed.name` → `"Tideweaver"`).

Every `Wave` yielded by the pipeline is also routed to these
files: the structured `wave` payload appears as a top-level JSON key
on every record, so `await MyClass.get_error()` returns rows whose
`record["wave"]` contains the full Pydantic dump (chunk index,
operation, rows processed, timing, redacted failed_sources). The same
routing applies to `fjord` waves — tagged per-source with operations
like `"fjord_refresh:Coin"` and `"outflow:CoinMarket"`.

URLs with query-string auth (`?api_key=...`, `?token=...`) are redacted
to `***REDACTED***` before being written to the log files. Headers
inside tracebacks aren't scrubbed — keep secrets out of URLs.

---

## 7. The `validate` and `init` Subcommands

`incorporator validate <config.json>` runs every structural check the
runtime does (required keys, env var expansion, inflow/outflow imports,
outflow() arity) **without executing the pipeline**. Exits 0 / 1 with a
human-readable report. Use this in CI and in pre-commit hooks.  Auto-detects
the config type from its top-level keys (`incorp_params` → stream,
`outflow` + `stream_params` → fjord, `window` + `shape` → tideweaver); pass
`--type stream|fjord|tideweaver` to force one.

`incorporator init [--type stream|fjord|tideweaver] [--output-dir .]
[--with-inflow]` writes a starter `pipeline.json` or `watershed.json`
(and, for fjord / tideweaver, an `outflow.py`). `--with-inflow` also
scaffolds an `inflow.py` for user-defined helpers (calc reducers,
custom converters). Refuses to overwrite existing files. After
running, edit the placeholders, then `validate`, then the matching run
verb (`stream`, `fjord`, or `tideweaver run`) — `init` prints this same
three-step sequence after it writes the files.

### From zero

```bash
incorporator init --type stream --output-dir config
# edit config/pipeline.json — fill in the placeholder incorp_params / export_params
incorporator validate config/pipeline.json
incorporator stream config/pipeline.json
```

`validate` runs the same config-loading path every run verb uses — JSON
parse, env expansion, sidecar imports, token resolution — so a config
that passes `validate` won't fail on load when you run it. If a
converter or export format needs an optional dependency you haven't
installed, `incorporator deps --missing` lists what's missing and the
exact `pip install` command for each (§10).

---

## 8. The `fjord` Subcommand — Multi-Source Stateful Pipelines

While `incorporator stream` operates on **one** source per pipeline, the
`incorporator fjord` subcommand drives **multiple sources concurrently** and
joins them into a brand-new output class via a Python `outflow()` function
you supply.

Use fjord when you need to keep a live, joined object map synchronised
across N APIs (e.g. crypto spot price + futures price → combined market
spread).

### No output class required

You **do not** define the output Incorporator subclass. fjord builds it
dynamically from the rows your `outflow()` function returns — same
zero-schema philosophy as `incorp()`. The class name is derived from the
`outflow` filename: **snake_case → PascalCase**.

- `coin_market.py` → `CoinMarket`
- `outflow.py` → `Outflow`
- `nascar_fantasy.py` → `NascarFantasy`

### Configuration File (`pipeline.json`)

The config points to an `outflow` file — a single `.py` containing your
source `Incorporator` subclasses **and** a top-level `outflow(state)`
function. The CLI imports that file at startup, resolves source class
names via `getattr`, and validates each resolved object is an
`Incorporator` subclass.

```json
{
  "outflow": "coin_market.py",
  "stream_params": [
    {
      "cls_name": "Coin",
      "incorp_params": {
        "inc_url": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
        "inc_code": "id"
      },
      "refresh_params": {}
    },
    {
      "cls_name": "BinanceFutures",
      "incorp_params": {
        "inc_url": "https://fapi.binance.com/fapi/v1/ticker/price",
        "inc_code": "symbol"
      },
      "refresh_params": {}
    }
  ],
  "export_params": {"file_path": "markets.ndjson"},
  "refresh_interval": 60,
  "export_interval": 300
}
```

### Parameter Breakdown

| Field | Required | Description |
| :--- | :--- | :--- |
| `outflow` | ✅ | Path to a `.py` file containing source Incorporator subclasses and a top-level `outflow(state)` function. **The filename's stem becomes the output class name** (snake_case → PascalCase). Resolved relative to the JSON config's directory. |
| `stream_params` | ✅ | List of per-source dicts. Each must declare `cls_name` (string matching a subclass in the `outflow` file) and `incorp_params`. Optional: `refresh_params`, `export_params` (per-source export). |
| `inflow` | ⬜ | Optional path to a `.py` file with two distinct roles: (1) its public symbols extend the token resolver's allow-list — reducer functions referenced from per-source `conv_dict` text tokens; (2) if it defines a top-level `inflow(state)` callable, fjord switches to sequential source seeding and calls it before each source's `incorp()` with the snapshots loaded so far (see Pattern 1 in [Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md)). Both roles can live in the same file. |
| `export_params` | ✅ | Destination for the combined output graph. |
| `refresh_interval` | ⬜ | Cadence (seconds) for per-source refresh daemons. Each entry can override. |
| `export_interval` | ⬜ | Cadence (seconds) for the outflow-and-export wave. |

### The `outflow()` Function

Lives in the same `outflow.py` file as the source classes. Receives `state` —
a dict keyed by source-class name, valued with that source's
`IncorporatorList`. Returns a `list[dict]` (or a single `dict`, auto-
wrapped). fjord feeds the rows through the same dynamic-schema-inference
path `incorp()` uses, then exports the instances.

```python
# coin_market.py
from incorporator import Incorporator

class Coin(Incorporator): pass
class BinanceFutures(Incorporator): pass

def outflow(state):
    coins = state["Coin"]
    futures = state["BinanceFutures"]
    rows = []
    for c in coins:
        f = futures.inc_dict.get(c.inc_code)
        if not f:
            continue
        rows.append({
            "inc_code":      c.inc_code,
            "coin_name":     getattr(c, "name", ""),
            "spot_price":    getattr(c, "current_price", 0.0),
            "futures_price": getattr(f, "price", 0.0),
            "spread":        getattr(f, "price", 0.0) - getattr(c, "current_price", 0.0),
        })
    return rows
```

> **Note — read-time vs build-time.** The `futures.inc_dict.get(...)` /
> `getattr(c, "name", "")` reads above are read-time joins/coercions, kept
> here for a minimal single-file CLI illustration. For production fjord
> configs, prefer resolving joins and coercions at build time (in each
> source's own `conv_dict`, via `link_to()` / `inc()`) so `outflow()` reads
> plain attributes with no defensive `getattr(..., default)`. See
> [Build-time vs read-time: where coercion + joins belong](api_atlas.md#build-time-vs-read-time-where-coercion--joins-belong)
> for the rule and the two cases where read-time is the correct design,
> and [Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md)
> for the fully worked build-time equivalent of this same CoinGecko/
> Binance-shaped join.

#### `outflow()` contract — what you do vs. what fjord does

| You write | fjord handles |
| :--- | :--- |
| The join logic (`for c in state["Coin"]…`) | Concurrent source ingestion + shared async lock |
| The dict shape per row | Building the dynamic Pydantic class from the dicts |
| Domain math (`spread = f.price - c.current_price`) | Per-source refresh daemons + per-source optional export |
| Returning `list[dict]` | Exporting the instances via `export()` (format inferred from extension) |
|  | Audit telemetry, graceful shutdown, weak-ref management |

If `outflow()` returns `[]`, fjord emits a wave with `rows_processed=0`
and skips the export for that wave. Useful for "no joined rows this
iteration" without crashing the daemon.

### Running the Daemon

```bash
incorporator fjord pipeline.json --logs
```

There is no `--poll` flag — fjord is **stateful-polling only** by design.
Cadence is driven by `refresh_interval` and `export_interval` inside the
JSON config.

### Audit operations

| Operation tag | Emitted by |
| :--- | :--- |
| `fjord_incorp:<ClassName>` | Seed phase, one per source |
| `fjord_refresh:<ClassName>` | Per-source refresh daemon wave |
| `export:<ClassName>` | Per-source export daemon wave (when `export_params` set on entry) |
| `outflow:<DynamicClassName>` | Outflow-and-export daemon wave |

### When to Reach For `fjord` vs `stream`

| Need | Use |
| :--- | :--- |
| Stream **one** source through chunked/stateful polling | `incorporator stream` |
| Concurrently poll **N sources** and join them into a new entity | `incorporator fjord` |
| Multiple sources on **independent intervals** with dependency gating in one time window | `incorporator tideweaver run` (see §9 below) |
| Sequencing / DAG dependencies between pipelines | Wrap either in a Prefect flow (see `deployment.md`) |

---

## 9. The `tideweaver` Subcommand — Windowed Orchestration

`stream` watches one source.  `fjord` joins N sources via one shared
`outflow(state)` daemon.  `tideweaver` is the layer above either: a
**graph of named currents** (each one a `stream` / fjord-flush / `export`
verb) that **tick on independent intervals** inside a single time window,
with hard or soft dependency edges gating which currents may fire when.

Use tideweaver when you have multiple feeds on different cadences (laps
update every 5s, pit reports every 30s, lap-summaries every minute) and
you need a clean way to declare *"this current depends on a fresh wave
from that one"* without writing your own `asyncio` glue.

> **`--type` auto-detection:** `incorporator validate` identifies a
> `watershed.json` automatically by the presence of top-level `"window"`
> + `"shape"` keys — the same heuristic that distinguishes it from
> `pipeline.json` (`"incorp_params"` → stream; `"outflow"` +
> `"stream_params"` → fjord).  Pass `--type tideweaver` to force the
> check if your config omits one of those sentinel keys (e.g. a
> `"shape": "custom"` watershed with only explicit `edges`).  The same
> heuristic backs `incorporator init --type ...` scaffolds.

### Two subcommands

```bash
incorporator tideweaver validate watershed.json   # structural check only; exit 0/1
incorporator tideweaver run      watershed.json   # full run; one Tide log per pass
```

The `run` verb runs the same validator before kickoff (parity with
`stream` / `fjord`), so a bad watershed fails fast with the curated
diagnostic block instead of mid-construction Pydantic errors.

The `run` verb accepts the following flags:

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--logs` | off | Enable background multiplex disk logging. |
| `--json-output` | off | Emit one NDJSON `Tide` record per line on stdout. |
| `--heartbeat-file <path>` | none | Touch this path after every tide; pairs with Docker `HEALTHCHECK`. |
| `--drain-timeout <secs>` | none | Override the watershed `drain_timeout` field — how long the scheduler waits for in-flight ticks on window close or SIGTERM. Precedence: CLI flag → `INCORPORATOR_DRAIN_TIMEOUT` env-var → `watershed.json` value → 30 s. Set to match your container orchestrator's `stop_grace_period`. |

The `validate` verb accepts no flags beyond the config path argument.

> **Container deployments.** `INCORPORATOR_DRAIN_TIMEOUT` is the canonical knob for Docker / Kubernetes: set it in `docker-compose.yml`'s `environment:` block so SIGTERM → drain → SIGKILL cycles complete cleanly.

### Configuration File (`watershed.json`)

```json
{
  "window": {"start": "${RACE_START}", "end": "${RACE_END}"},
  "shape": "diamond",
  "outflow": "outflow.py",
  "drain_timeout": 30,
  "gate_mode": "hard",
  "head":   {"name": "laps",  "class": "LapData",     "verb": "stream", "interval": 30, "incorp_params": {"inc_url": "..."}},
  "middle": [
    {"name": "pits",  "class": "PitStops",   "verb": "stream", "interval": 30, "incorp_params": {"inc_url": "..."}},
    {"name": "flags", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {"inc_url": "..."}}
  ],
  "tail":   {"name": "state", "class": "DriverState", "verb": "fjord",  "interval": 30,
             "export_params": {"file_path": "data/state.ndjson", "format": "ndjson", "if_exists": "append"}}
}
```

Five `shape` values are supported, each driving a different edge layout:
`chain`, `diamond`, `fanout`, `parallel`, and `custom` (raw `edges: [...]`
list for mixed-mode topologies).  See [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md)
for the full walk-through plus the Python-API equivalents.

> **Path resolution — inputs vs outputs.** Relative **input** paths declared in any
> config file (`inflow`, `outflow`, and `incorp_params.inc_file` / `inc_files` /
> `refresh_params.new_file`, at the top level and per-current) resolve against the
> **config file's directory** — so the same `watershed.json` /
> `pipeline.json` runs from any working directory and reads alongside a read-only
> Docker config mount. Relative **output** paths (`export_params.file_path`,
> `archive_target`) stay **CWD / `WORKDIR`-relative**, so writes land in the
> writable runtime dir (e.g. Docker's `/app/data`). Absolute paths and `inc_url` /
> `new_url` are never rewritten. The in-process Python API
> (`Incorporator.incorp(...)`) keeps its arguments CWD-relative.

> **Removed keys.** `"dependency_mode"` (top-level) and `"mode"` (per-edge)
> were removed in v1.3.0. Using either now raises `ValueError` with migration
> guidance: rename to `"gate_mode"`. There is no silent back-compat path.

> **Sidecar helpers in token fields.** Just like `pipeline.json`, any
> text-form token inside `watershed.json` — most commonly a `conv_dict`
> entry under a current's `incorp_params` — may reference a **public**
> helper defined in the top-level `inflow` and/or `outflow` sidecar, using
> the same `@name` / call-grammar syntax documented in
> [Text-Form Tokens](#text-form-tokens-paginators-converters-etc) above.
> The sidecar module(s) are loaded before token resolution, so this works
> identically to the Python `Stream(conv_dict=...)` / `incorp()` form.

### `host_penstocks` — declarative HOST-layer rate limits

A top-level `"host_penstocks"` block registers per-host outbound-request
throttling against `register_host_penstock` at config-load time — the
declarative equivalent of calling it yourself in a sidecar module (which
`architect.tune()` currently advises but has no config-file counterpart for):

```json
{
  "host_penstocks": {
    "api.example.com": {"rate_per_sec": 5.0},
    "bursty.example.com": {"rate_per_sec": 10.0, "burst": 50}
  }
}
```

This is the **HOST layer** (`incorporator.io.penstock`'s global registry,
keyed by hostname) — structurally distinct from the **per-edge**
`flow.penstock` block documented below, which throttles one edge's own
traffic. Keep the two mental models separate: `host_penstocks` says "no
matter which current talks to this host, cap it at N req/sec"; `flow.penstock`
says "this specific upstream→downstream edge fires at most N times/sec."

Only the shorthand form is supported from JSON: `rate_per_sec` alone builds
a `SustainedPenstock`; adding `burst` builds a `BurstPenstock` instead
(same precedence as `register_host_penstock`'s own keyword shorthand). Full
`Penstock` subclass declarations (`WindowPenstock`, `SignalPenstock`,
`BackpressurePenstock`) aren't expressible from JSON — call
`register_host_penstock` directly from your `inflow`/`outflow` sidecar
module (which already runs at load time) for those.

Hostnames are case-insensitive — the registry lowercases on registration,
so `"API.Example.com"` and `"api.example.com"` resolve to the same entry.
Registration is a plain dict overwrite, so loading the same `watershed.json`
more than once (e.g. `validate` then `run`) is harmless.

> **Precedence — this does NOT stack with per-call overrides.** A current's
> `incorp_params.requests_per_second` short-circuits the host registry
> entirely for that source — the two do not compose. If you set both, the
> per-call value wins and the `host_penstocks` entry for that host is
> ignored for that current.

### Per-edge flow control

Beyond `gate_mode`, watershed.json supports the full per-edge **canal
toolkit** — six orthogonal primitives composable into a `FlowControl`:
gating, surge override, rate limiting, wave buffering, overflow handling,
and declarative telemetry.  Use the shape-level `"flow": {...}` to share
one FlowControl across every shape-built edge, or the explicit `"edges"`
list with a per-edge `"flow"` for mixed topologies:

```json
{
  "shape": "custom",
  "currents": [
    {"name": "upstream",   "class": "Quote",  "verb": "stream", "interval": 1,  "incorp_params": {"inc_url": "..."}},
    {"name": "downstream", "class": "Spread", "verb": "fjord",  "interval": 5,
     "export_params": {"file_path": "data/spread.ndjson", "format": "ndjson"}}
  ],
  "edges": [
    {
      "from": "upstream",
      "to":   "downstream",
      "flow": {
        "gate":          {"type": "hard"},
        "surge_barrier": {"threshold_multiple": 3.0, "action": "bypass"},
        "penstock":      {"type": "burst", "rate_per_sec": 5.0, "burst": 10},
        "reservoir":     {"depth": 8},
        "spillway":      {"type": "export_to_archive", "archive_cls": "audit:AuditArchive"}
      }
    }
  ]
}
```

| Primitive | JSON type tags | What it does |
| :--- | :--- | :--- |
| **`gate`** | `"hard"` / `"soft"` / `"weir"` | Pass/hold decision per upstream. `hard` blocks until a fresh upstream wave; `soft` fires on own cadence; `weir` gates on freshness without skip-ahead. |
| **`surge_barrier`** | (single shape — `threshold_multiple` + `action`) | When upstream's tick runs long (>= `threshold_multiple × upstream.interval`), fire `action`: `"skip"` / `"halt"` / `"bypass"`. `bypass` ignores this edge's gate AND penstock for that pass. |
| **`penstock`** | `"sustained"` / `"burst"` / `"window"` / `"backpressure"` / `"signal"` / `"null"` | Edge-level rate limit. `sustained` flat rate; `burst` token bucket; `window` sliding-window cap; `backpressure` interpolates `max_rate → min_rate` as the reservoir fills; `signal` calls a user `rate_fn` callable; `null` is an explicit no-op (equivalent to omitting `penstock`, but names the "no throttling here" intent). Returns skip reason `"penstock_limited"`. |
| **`reservoir`** | (single shape — `depth: 1..1024`) | Per-edge FIFO buffer of recent waves. Default `depth: 1` keeps just the latest. |
| **`spillway`** | `"drop_oldest"` / `"raise_overflow"` / `"export_to_archive"` | Fires when a wave is displaced from a full reservoir. `drop_oldest` is silent; `raise_overflow` logs a WARNING — routed into the active session's `error.log` (retrievable via `LoggedTideweaver.get_scheduler_events`) when running under a `LoggedTideweaver` session, falling back to the bare module logger otherwise; `export_to_archive` extends `archive_cls._spillway_backlog` (strong refs) — unbounded by default, or capped via `max_entries` (evicts oldest, WARNING on first trip, see below). |
| **`observer`** | `"null"` / `"logging"` / `"signal"` | Declarative per-edge telemetry. Hooks: `on_fire`, `on_skip`, `on_spillway`, `on_reservoir_level`. Synchronous and cheap — the scheduler does not await them. JSON type-tags are resolved: `{"type": "logging", "fire_level": "info"}` deserialises directly. `SignalObserver.callback` accepts a bare name or `module:fn` form (same resolution as `SignalPenstock.rate_fn`). For a custom subclass, use the Python API. |

**Sidecar string resolution:** `SignalPenstock.rate_fn` accepts either a
bare name (`"peak_rate"`, looked up on the watershed-level `outflow.py`)
or a `module:attr` form (`"mymodule.signals:peak_rate"`).  Same for
`ExportToArchive.archive_cls` — `"AuditArchive"` (sidecar) or
`"audit:AuditArchive"` (module path).  Missing names raise at load time.

**`ExportToArchive.max_entries`** (optional `int`, `>= 1`, default `None`):
caps the strong-ref backlog on `archive_cls._spillway_backlog`. Without it
the backlog grows unbounded for the run's lifetime — every displaced wave's
instances are appended and never evicted, which matters for a long-window
run on a hot edge. Setting `max_entries` evicts the oldest entries once the
cap is exceeded and emits a one-time WARNING the first time eviction
happens (routed through `LoggedTideweaver.get_scheduler_events` under
`event_type="spillway_backlog_capped"`, or the bare module logger
otherwise) — so unbounded growth becomes an observable signal instead of
silent. Call `ExportToArchive.drain(archive_cls)` to pop-and-clear the
backlog yourself (e.g. flush to disk on a schedule) instead of relying
solely on the cap.

**The top-level `gate_mode` is shorthand** for `"flow": {"gate": {"type": "<mode>"}}` —
plus an implicit `SurgeBarrier(threshold_multiple=2.0, action="skip")`
when `gate_mode="hard"`.  Pass `"flow": {...}` explicitly to opt out of
the implicit surge barrier on `"hard"`.

**Python API parity — `GateMode` enum.** The shape constructors and `Edge(gate_mode=...)` accept both string form and the `GateMode` enum:

```python
from incorporator.tideweaver import GateMode, Watershed

Watershed.chain(window=(start, end), currents=[...], gate_mode=GateMode.HARD)
Watershed.chain(window=(start, end), currents=[...], gate_mode="hard")     # same Watershed
```

Identical `FlowControl` is produced — `GateMode` is a `str`-subclass so equality against `"hard"` / `"soft"` / `"weir"` keeps working in every comparison.  JSON config (`watershed.json`) continues to use the lowercase strings.

### Operation tags

| Operation tag | Emitted by |
| :--- | :--- |
| `Tide` log record (NDJSON on `--json-output`) | One per scheduler pass, regardless of which currents fired |
| `Wave` from a Stream current | Same wave shape as the stream daemon (`chunk`, `refresh`, ...) |
| `outflow:<DerivedName>` | Each fjord-flush export (same prefix as the standalone fjord engine) |

The Tide record captures which currents `fired` this pass and which
`skipped` (with reasons: `not_due`, `awaiting_upstream`, `still_running`,
`skip_ahead` / `surge_halted` (`SurgeBarrier`), `penstock_limited`
(`Penstock`), `phase_offset` (`Current.phase_offset_sec`)).  Use it for
cadence audits without parsing per-current Waves.

#### Tide record fields (v1.2.1)

In addition to `tide_number`, `fired`, `skipped`, `duration_sec`, and
`timestamp` (carried over from v1.2.0), the `Tide` record surfaced on
`--json-output` carries these fields:

| Field | Type | Meaning |
| :--- | :--- | :--- |
| `wake_reason` | `Literal["startup", "timer", "wake_event", "pass_interval", "shutdown"]` | Why the scheduler woke for this pass |
| `heap_depth` | `int` | Pending tick count on the heap at pass start |
| `in_flight_count_at_start` | `int` | Tick functions still running when the pass began |
| `current_outcomes` | `list[CurrentOutcome]` | Per-current outcome (`name`, `status`, `reason`, `bypassed_edges`, `in_flight_sec`, `last_wave_at`, `last_failed_at`, `parent_snapshot_size`) |
| `canal_rejects_added` | `int` | New `RejectEntry` records this pass appended to `tw.rejects` |
| `next_due_in_sec` | `float \| None` | Seconds until the next scheduled tick (negative means the scheduler is behind) |

#### Canal-layer rejects on `--json-output`

Beyond per-Tide records, the run accumulates a `list[RejectEntry]` on
`tw.rejects`.  Verb-layer entries carry exception-typed `error_kind`
strings (`"HTTPStatusError"`, `"RequestError"`, ...), as they have
since v1.2.0.  Canal-layer entries — every scheduler-level skip that
never reached a tick body — carry one of four literal strings:

| `error_kind` | Emitted when |
| :--- | :--- |
| `"PenstockLimited"` | The edge's `Penstock` denied consume on this pass |
| `"SurgeHalted"` | The edge's `SurgeBarrier` action fired with `"halt"` |
| `"SkipAhead"` | The edge's `SurgeBarrier` action fired with `"skip"` |
| `"GateBlocked"` | The edge's `Gate` returned hold for this pass |

Every canal-layer entry has `from_name` and `to_name` populated so
per-edge attribution is grep-able; `cooldown_sec` carries the
back-off hint when relevant.

#### Backlog short-circuit — `backlog_backoff_factor` (v1.2.1+)

Beyond the per-edge `FlowControl`, the `Tideweaver` constructor itself
accepts an opt-in `backlog_backoff_factor: float = 1.0` kwarg.  Set
it to `2.0` (or larger) to multiplicatively extend the next-pass wait
when the scheduler is consistently saturated — the heap stays full,
`tide.next_due_in_sec` keeps coming back negative.  Default `1.0` is
disabled (identical behaviour to v1.2.0).  Configure via the Python
API; no `watershed.json` key is wired up yet.

```python
Tideweaver(watershed, backlog_backoff_factor=2.0).run()
```

For the full method-level signature of `fjord()`, see the pdoc-built
[Library reference](./library_reference.md).

### Structured session logging (v1.3.3)

`LoggedTideweaver` writes three categories of structured JSONL records to disk.
The files are named after the resolved `logger_name`. From the CLI,
`tideweaver run --logs` is what builds a `LoggedTideweaver` in the
first place — without the flag, `tideweaver run` constructs a bare
`Tideweaver` and there are no log files for the reader methods below
to read.

**Session log naming via `Watershed.name`**

`Watershed` accepts an optional `name: str | None` field.  When set, a
`LoggedTideweaver` without an explicit `logger_name` argument resolves its
logger name — and therefore its log file prefix — to `watershed.name`.  An
explicit `logger_name` always wins.

```python
ws = Watershed(name="NightlyPrices", window=(...), currents=[...])
# LoggedTideweaver(ws, enable_logging=True) → logger_name resolves to "NightlyPrices"
# → logs/NightlyPrices_tide.log, logs/NightlyPrices_error.log, logs/NightlyPrices_debug.log
```

**`session` field on `Tide` and `RejectEntry`**

Every `Tide` record yielded by `Tideweaver` carries a `session: str | None`
field populated from `logger_name`; `None` when no `logger_name` is set.
Canal-layer `RejectEntry` records (`PenstockLimited`, `SurgeHalted`,
`SkipAhead`, `GateBlocked`) carry the same `session` field so concurrent-run
records are distinguishable inside a single file or aggregated view.

**Scheduler-event diagnostics**

Five scheduler-level conditions are routed as structured records to the session
`error.log` under a top-level `"scheduler_event"` key:

| `event_type` | Condition |
| :--- | :--- |
| `isolated_tick_failure` | A tick raised and `on_error="isolate"` was set |
| `tick_parked` | Retry loop exhausted (`on_error="restart"`) — current parked |
| `empty_output` | A tick completed but produced zero rows |
| `empty_parent_snapshot` | A dependent current had no parent snapshot at tick time |
| `fjord_flush_failure` | A fjord-flush step raised an unhandled exception |

Each record includes: `event_type`, `current_name`, `cls_name`, `tide_number`
(`int | null`), `session` (matching `logger_name`), and `detail`.  Records land
in `logs/<logger_name>_error.log` alongside canal-layer reject records.

**Reader methods on `LoggedTideweaver`**

Four `@classmethod` async readers provide structured access to the log files
written during a run.  All take `logger_name` as their first argument and
return a list of dicts.

| Method | Source file | Top-level key | Notes |
| :--- | :--- | :--- | :--- |
| `get_tides(logger_name)` | `<name>_tide.log` | `"tide"` | Sorted by `tide_number` ascending |
| `get_rejects(logger_name)` | `<name>_error.log` + `<name>_api.log` | `"reject"` | Union of both files — covers both URL-traffic and codebase rejects |
| `get_scheduler_events(logger_name)` | `<name>_error.log` | `"scheduler_event"` | Sorted by `tide_number`, `event_type`, `current_name`; after a failure, dependents show `awaiting_upstream` skips (DEBUG `tide` records, not scheduler events) while the failure itself stays here |
| `get_current(logger_name, code)` | `<name>_debug.log` | any | Filtered to records whose `meta` contains `code`; reads debug superset to avoid double-counting |

```python
events = await LoggedTideweaver.get_scheduler_events("NightlyPrices")
for rec in events:
    evt = rec["scheduler_event"]
    print(evt["event_type"], evt["current_name"], evt["tide_number"])

tides   = await LoggedTideweaver.get_tides("NightlyPrices")
rejects = await LoggedTideweaver.get_rejects("NightlyPrices")

# Classify rejects after the union:
for rec in rejects:
    entry = rec["reject"]
    if entry["is_url_traffic_error"]:
        print("API failure:", entry["source"], entry.get("status_code"))
    else:
        print("Parse failure:", entry["source"], entry["error_kind"])
```

Each method returns `[]` when no log file exists (run not yet started or
`enable_logging=False`).  The `jq` expression
`.[] | select(.scheduler_event.session == "NightlyPrices")` filters records by
session in a mixed-session log file.

**Watershed lifecycle events** (`watershed_started`, `watershed_completed`) are
emitted to `_error.log` at the beginning and end of every `tw.run()` call.
They appear in `get_scheduler_events()` output alongside the five diagnostic
event types. Level is `WARNING` — they are informational, not failures.

---

## 10. The `deps` Subcommand — Optional-Dependency Introspection

The `deps` subcommand lists every registered optional dependency and its install status. Use it to audit your environment, generate install commands, or pipe a machine-readable manifest into CI tooling.

```bash
incorporator deps
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--missing` | `False` | Show only deps that are **not** installed |
| `--category` | (all) | Filter to one category: `speedup`, `format`, `orchestrate`, `platform_fix` |
| `--json` | `False` | Emit a JSON array instead of a human-readable table |

### Sample output (tabular)

```
NAME      CATEGORY      EXTRA        STATUS              INSTALL
--------  ------------  -----------  ------------------  -----------------------------
orjson    speedup       speedups     ✓ 3.9.15            pip install incorporator[speedups]
lxml      speedup       speedups     ✓ 5.3.0             pip install incorporator[speedups]
fastavro  format        avro         ✗ not installed     pip install incorporator[avro]
cramjam   speedup       speedups     ✗ not installed     pip install incorporator[speedups]
tzdata    platform_fix  parquet      n/a (platform)      pip install incorporator[parquet]
```

The `STATUS` column is colour-coded: green for installed, red for missing, yellow for platform-gated deps (`n/a (platform)`) that cannot be installed on the current OS.

### Sample output (JSON)

```bash
incorporator deps --json
```

```json
[
  {
    "name": "orjson",
    "extra": "speedups",
    "category": "speedup",
    "description": "Fast JSON serialiser/deserialiser (Rust-backed, GIL-free)",
    "version_spec": ">=3.9",
    "is_available": true,
    "installed_version": "3.9.15",
    "platform_marker": null,
    "include_in_all": true
  }
]
```

> The `module` field is intentionally excluded from JSON output — it is not serialisable and exposes internal structure.

### Show only missing deps

```bash
incorporator deps --missing
incorporator deps --missing --json   # machine-readable for CI scripts
```

### Filter by category

```bash
incorporator deps --category speedup
incorporator deps --category format --json
```

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the `pipeline.json` schema in a complete tutorial | [Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md) |
| Configure a fjord pipeline with `outflow.py` | [Tutorial 10 — Multi-Source Fjord](../examples/10-multi-source-fjord/README.md) |
| Author a `watershed.json` for Tideweaver | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) |
| Ship the CLI as a Docker container with secrets | [Deployment Guide](./deployment.md) |
| Get structured error logs out of the daemons | [Production Debugging](./debugging.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/cli_and_configuration.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)