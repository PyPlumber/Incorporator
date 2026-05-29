# CLI & Pipeline Configuration

The Incorporator CLI transforms the framework from a lightweight micro-client into an **Autonomous Pipeline Daemon**. 

By defining your extraction, enrichment, and loading rules in a simple JSON file, you can run infinite streams in the background—perfect for Docker containers, systemd services, or live data scraping.

## 1. Prerequisites
To use the CLI, you must install Incorporator with the Orchestration upgrades:
```bash
pip install "incorporator[orchestrate]"
```

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
*   **Behavior:** Strictly enforces O(1) memory. Fetches a single chunk, enriches it,
    saves it to disk, and **completely flushes the RAM** before fetching the next chunk.

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
🚀 Starting Incorporator Stream...
Chunk 1 | 10000 rows | 1.84s
Chunk 2 | 10000 rows | 1.91s
🛑 Stream process completed gracefully.
```

### Daemon Execution (Infinite Polling)
To keep the pipeline alive in the background, use the `--poll` flag. This tells the orchestrator to wait `X` seconds after a successful run before automatically restarting the extraction/hydration cycle.
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
to NDJSON (one `Wave` per line). The colorized startup banner
and `🛑/❌` framing messages go to **stderr** so stdout stays parseable.

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
| Types (as args) | `datetime`, `date`, `time`, `int`, `float`, `bool`, `str`, `list`, `dict`, `tuple`, `set`, `bytes`, `None`, `True`, `False`, `new` |
| Comparison ops | `operator_eq`, `operator_ne`, `operator_lt`, `operator_le`, `operator_gt`, `operator_ge`, `operator_contains` |

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

The CLI imports `inflow.py` **once** per pipeline run (cached via
`sys.modules`); per-chunk operations don't re-import anything.

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
Terminal output is suppressed, and telemetry is routed to non-blocking background OS threads. Incorporator will automatically create a `logs/` directory and generate three rotating JSON Lines files (max 15MB each):

1.  **`logs/{Class}_api.log`**: Tracks all successful HTTP traffic, rate limits, and chunk throughput.
2.  **`logs/{Class}_error.log`**: The Dead Letter Queue (DLQ). Catches network timeouts, 400/500 status codes, and malformed data schemas.
3.  **`logs/{Class}_debug.log`**: Deep framework execution traces for local troubleshooting.

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

`incorporator init [--type stream|fjord|tideweaver] [--output-dir .]`
writes a starter `pipeline.json` or `watershed.json` (and, for fjord /
tideweaver, an `outflow.py`). Refuses to overwrite existing files. After
running, edit the placeholders, then `validate`, then the matching run
verb (`stream`, `fjord`, or `tideweaver run`).

---

## 8. The `fjord` Subcommand — Multi-Source Stateful Pipelines

While `incorporator stream` operates on **one** source per pipeline, the
`incorporator fjord` subcommand drives **multiple sources concurrently** and
joins them into a brand-new output class via a Python `outflow()` function
you supply.

Use fjord when you need to keep a live, joined object map synchronised
across N APIs (e.g. crypto spot price + futures price → combined market
spread).

### Zero output-class declaration

You **do not** define the output Incorporator subclass. fjord builds it
dynamically from the rows your `outflow()` function returns — same
zero-schema philosophy as `incorp()`. The class name is derived from the
`outflow` filename: **snake_case → PascalCase**.

- `coin_market.py` → `CoinMarket`
- `arb_outflow.py` → `ArbOutflow`
- `nascar_fantasy.py` → `NascarFantasy`

### Configuration File (`fjord.json`)

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
incorporator fjord fjord.json --logs
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

Both accept the same observability flags as `stream` / `fjord`:
`--logs`, `--json-output`, `--heartbeat-file <path>`.

### Configuration File (`watershed.json`)

```json
{
  "window": {"start": "${RACE_START}", "end": "${RACE_END}"},
  "shape": "diamond",
  "outflow": "race_outflow.py",
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

> **Legacy alias.** The JSON loader accepts `"dependency_mode"` as a
> one-release alias for `"gate_mode"` at the top level (chain / diamond /
> fanout shapes), and `"mode"` as an alias for `"gate_mode"` on per-edge
> entries.  New configs should use the canonical names; the aliases will
> be removed in a future minor release.

### Per-edge flow control

Beyond `gate_mode`, watershed.json supports the full per-edge **canal
toolkit** — five orthogonal primitives composable into a `FlowControl`:
gating, surge override, rate limiting, wave buffering, and overflow
handling.  Use the shape-level `"flow": {...}` to share one FlowControl
across every shape-built edge, or the explicit `"edges"` list with a
per-edge `"flow"` for mixed topologies:

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
| **`penstock`** | `"sustained"` / `"burst"` / `"window"` / `"backpressure"` / `"signal"` | Edge-level rate limit. `sustained` flat rate; `burst` token bucket; `window` sliding-window cap; `backpressure` interpolates `max_rate → min_rate` as the reservoir fills; `signal` calls a user `rate_fn` callable. Returns skip reason `"penstock_limited"`. |
| **`reservoir`** | (single shape — `depth: 1..1024`) | Per-edge FIFO buffer of recent waves. Default `depth: 1` keeps just the latest. |
| **`spillway`** | `"drop_oldest"` / `"raise_overflow"` / `"export_to_archive"` | Fires when a wave is displaced from a full reservoir. `drop_oldest` is silent; `raise_overflow` logs a WARNING; `export_to_archive` extends `archive_cls._spillway_backlog` (strong refs). |

**Sidecar string resolution:** `SignalPenstock.rate_fn` accepts either a
bare name (`"peak_rate"`, looked up on the watershed-level `outflow.py`)
or a `module:attr` form (`"mymodule.signals:peak_rate"`).  Same for
`ExportToArchive.archive_cls` — `"AuditArchive"` (sidecar) or
`"audit:AuditArchive"` (module path).  Missing names raise at load time.

**The top-level `gate_mode` is shorthand** for `"flow": {"gate": {"type": "<mode>"}}` —
plus an implicit `SurgeBarrier(threshold_multiple=2.0, action="skip")`
when `gate_mode="hard"`.  Pass `"flow": {...}` explicitly to opt out of
the implicit surge barrier on `"hard"`.

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
| `current_outcomes` | `list[CurrentOutcome]` | Per-current outcome (`name`, `status`, `reason`, `bypassed_edges`, `in_flight_sec`, `last_wave_at`) |
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