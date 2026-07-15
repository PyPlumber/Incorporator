# Tutorial 2 — Data Lake Pivot: From SaaS Roster to SQLite + Avro

Your SaaS roster lives in Auth0 or Okta as deeply nested JSON. BI wants it in Avro + SQLite by 7 AM. Start with `test()` to profile the endpoint and get paste-ready kwargs, call `incorp()` to absorb the nested payload into a typed object graph, then `export()` to write Avro and SQLite — with the nested `address` and `company` dicts flattened, types mapped to strict Avro (`int` → `long`) and SQLite columns, and the round-trip rehydrating the full object graph. No schema file, no ORM, no Avro schema definition.

**Prerequisites:** [Tutorial 1 — First Steps](../01-first-steps/README.md) (`incorp()`, `test()`, `inc_dict`).

**File:** [`data_lake_pivot.py`](./data_lake_pivot.py)

This tutorial walks that arc on a SaaS-style `/users` endpoint. `jsonplaceholder.typicode.com/users` stands in for any rostered REST source (Auth0, Okta, Workday, BambooHR, internal LDAP exports). The endpoint returns heavily nested `address` and `company` dictionaries — the kind of payload that usually requires manual schema definitions, type mapping, and flattening logic.

## What this tutorial proves

1. **Schema-free ingestion:** Define an empty class and `incorp()` absorbs the nested JSON payload — no Pydantic field declarations, no mapping file.
2. **Automatic type translation:** Incorporator infers types from the JSON and maps them to strict Avro types (e.g., `int` → `long`) and SQLite columns.
3. **Nested data flattening:** Nested dicts (like an `address` dictionary) are serialised for binary/SQL storage and rehydrated on read-back to dot-notation objects.
4. **One vocabulary, three storage shapes:** The same `inc_code` that keys the in-memory registry also keys SQLite columns and Avro fields — one declaration, three targets.

---

## The Code Walkthrough

### 0. Discover first with test()

For an unfamiliar endpoint, run `test()` before writing any `incorp()` call:

```python
from incorporator import Incorporator

class User(Incorporator): pass

sample = await User.test(inc_url="https://jsonplaceholder.typicode.com/users")
# Prints the payload tree, primary-key candidates, type-cast candidates,
# and the exact inc_code / inc_name / conv_dict to paste into incorp().
# Returns a 3-record preview — safe to run against live endpoints.
```

`test()` caps itself at one page and a 5-second timeout. Paste the suggested kwargs into step 1 and move on. (`base.py:1646`)

### 1. Ingest the Nested JSON API

With the suggestions from `test()` confirmed, fetch the full list. Notice that the class body is empty — Incorporator absorbs arbitrary nested JSON without field declarations.

```python
from incorporator import Incorporator, FormatType

class User(Incorporator): pass

users = await User.incorp(
    inc_url="https://jsonplaceholder.typicode.com/users",
    inc_code="id",    # keys the O(1) in-memory registry
    inc_name="name"
)
```

### 2. Export to SQLite

A single call creates the `employees` table, infers column types, serialises nested dicts, and streams rows to the C driver one by one — keeping the export memory O(1) regardless of row count. Note: the preceding `incorp()` call validates all rows in a single batch, so peak memory before export is O(N).

```python
await User.export(
    instance=users,
    file_path="users_warehouse.db",
    sql_table="employees",
    if_exists="replace"
)
```

The entire DROP + CREATE + INSERT sequence runs inside a `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK` transaction. A mid-write crash rolls back the DROP, so the old table survives intact. (`binary.py:139-184`)

### 3. Export to Apache Avro (Binary)

Avro requires a strict schema. Incorporator generates it dynamically from the inferred model and writes a binary Avro file via `fastavro`.

```python
await User.export(
    instance=users,
    file_path="users_datalake.avro",
    format_type=FormatType.AVRO
)
```

Avro field names must match `[A-Za-z_][A-Za-z0-9_]*`. When a SaaS source returns hyphenated keys (e.g., `user-id`), Incorporator sanitises them to `user_id` on write and stores the original names in the schema's `__incorporator_original_names__` attribute — so the read path restores the original key names transparently. (`binary.py:20-27`)

### 4. The Round Trip (Reading from Binary)

The ingestion vocabulary does not change across storage targets. Read back from SQLite or Avro with the same `incorp()` call:

```python
# Read from SQLite
sql_users = await User.incorp(
    inc_file="users_warehouse.db",
    sql_query="SELECT * FROM employees",
    inc_code="id",
    inc_name="name"
)

# Read from Apache Avro binary file
avro_users = await User.incorp(
    inc_file="users_datalake.avro",
    format_type=FormatType.AVRO,
    inc_code="id",
    inc_name="name"
)
```

### 5. Verifying the O(1) Object Graph

Because we passed `inc_code="id"`, no loops or array slices are needed. User ID `1` is accessible in O(1) time from all three registries.

The nested `address` dict — stored as a serialised string in SQLite and Avro — is re-inferred back into dot-notation objects on read:

```python
target_id = 1

print(f"Original JSON Name: {users.inc_dict[target_id].inc_name}")
print(f"SQLite Read Name:   {sql_users.inc_dict[target_id].inc_name}")
print(f"Avro Read Name:     {avro_users.inc_dict[target_id].inc_name}")

# Nested object reconstructed from binary/SQL text
print(f"Original JSON City: {users.inc_dict[target_id].address.city}")
print(f"SQLite Read City:   {sql_users.inc_dict[target_id].address.city}")
print(f"Avro Read City:     {avro_users.inc_dict[target_id].address.city}")

# Output:
# Original JSON City: Gwenborough
# SQLite Read City:   Gwenborough
# Avro Read City:     Gwenborough
```

Note: SQLite has no native boolean type — `True`/`False` are stored as integers `1`/`0`. Pass `sql_bool_columns=["col_name"]` on read to recover the original bool semantics. (`binary.py:53-68`)

### Putting It Together

The arc: nested JSON web payload → relational SQLite → Apache Avro binary file → fully reconstructed Python objects. Three steps (`test`, `incorp`, `export`), all sharing the same vocabulary.

Because the data is parked in `.inc_dict` registries, you can immediately use `link_to(sql_users)` or `link_to(avro_users)` to fuse this data with live REST API responses — no ORM or Avro schema file required.

---

## Run it from the CLI

This is the simplest CLI case — fetch JSON, write to SQLite (or Avro, or Parquet, or any other supported format). Pure JSON, no sidecar file needed: see [`pipeline.json`](pipeline.json), ships next to the entry script, and the run addendum at the bottom of this page.

**Switch the export target by changing the file extension** — Incorporator infers the handler from the path:

* `users_warehouse.db` → SQLite
* `users_datalake.avro` → Apache Avro *(requires `pip install incorporator[avro]`)*
* `users.parquet` → Parquet *(requires `pip install incorporator[parquet]`)*
* `users.ndjson` / `.csv` / `.xlsx` → all native

For a containerised daemon that polls + refreshes on a schedule, see [`examples/cli-templates/daemon-mode.json`](../../examples/cli-templates/daemon-mode.json) and [the deployment guide](../../docs/deployment.md).

---

## Where to Go Next

> **Up next: [Tutorial 3 — Universal Formats](../03-universal-formats/README.md).** T2 showed the pivot arc on one source; T3 expands it into a multi-format snapshot warehouse round-trip on a typed CoinGecko payload.

| Goal | Read |
|---|---|
| Build a per-tick snapshot warehouse across multiple formats | [Tutorial 3 — Universal Formats](../03-universal-formats/README.md) |
| Audit a warehouse against a federal source | [Tutorial 4 — XML Post Audit](../04-xml-post-audit/README.md) |
| Land Parquet artifacts at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Stream massive files through chunking + paginators | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |
| See the format kwarg reference | [Formats & Compression](../../docs/formats_and_compression.md) |

---

## 🐳 Run It From the CLI (+ Docker)

Reference material — three ways to run the exact same pivot, in order.

**1. Python entry** (what every section above walked through):

```bash
cd examples/02-data-lake-pivot
python data_lake_pivot.py
```

**2. CLI form** — [`pipeline.json`](pipeline.json) ships next to the entry
script; no inline JSON duplicate here (see it drift once, trust it forever).
Pure JSON, no sidecar needed for this pipeline.

```bash
cd examples/02-data-lake-pivot      # see caveat below
incorporator validate pipeline.json
incorporator stream pipeline.json
```

> **Run from inside this directory.** `export_params.file_path`
> (`"out/users_warehouse.db"`) is CWD-relative. Running
> `incorporator stream examples/02-data-lake-pivot/pipeline.json` from the
> repo root silently writes to `<repo-root>/out/` instead.

**3. Docker** — reasoned from the `Dockerfile`/`docker-compose.yml`, **NOT
run or verified** (no Docker available in this pass — confirm before
relying on it):

```bash
# Reasoned, unverified.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/examples/02-data-lake-pivot:/app/config:ro" \
  -v "$(pwd)/examples/02-data-lake-pivot/out:/app/out" \
  incorporator:latest \
  stream /app/config/pipeline.json
```

The image's `WORKDIR` is `/app`, and `export_params.file_path` is
CWD-relative (never rebased against the config's directory) — so
`pipeline.json`'s `"out/users_warehouse.db"` resolves to `/app/out/...`
inside the container. The mount target must therefore be `/app/out`, not
one of the three paths the `Dockerfile` prepares (`/app/config`,
`/app/data`, `/app/logs`). Because `/app/out` is not one of the
pre-`chown`'d directories, `--user` overrides to the invoking host user so
the non-root `appuser` can still write.

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/02-data-lake-pivot/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
