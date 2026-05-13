# ⚡ Performance Characteristics

This page documents the measured throughput of Incorporator's format
handlers and the engine-level optimisations users get automatically (no
code changes required).

Every number below is reproduced in CI via `pytest -m benchmark`. See
[`tests/benchmarks/`](../tests/benchmarks/) for the full source.

---

## Throughput Matrix

Commodity hardware (Win 10, Python 3.13). All values in **rows/sec**.

| Format | Write | Parse | Notes |
|---|---|---|---|
| **JSON** | ~140k | **1,627k** | orjson dominates parse (10× write). |
| **NDJSON** | 141k | 211k | Streaming line-by-line both ways. |
| **CSV** | 137k | 171k | `csv.DictReader` / `csv.DictWriter`. |
| **TSV** | 144k | 166k | Same engine as CSV. |
| **PSV** | 140k | 169k | Same engine. |
| **Parquet** | 300k+ | 200k | Streaming row-groups (write) + `pyarrow.compute` (parse). |
| **Feather** | 249k | 214k | Memory-mapped reads; competitive with NDJSON. |
| **ORC** | 249k | 191k | Same Arrow pipeline as Parquet. |
| **SQLite** | 157k | 201k | `executemany()` bulk insert; `cursor.fetchall()` for parse. |
| **XML** | 55k | 41k | Element-tree serialisation; lxml when installed. |
| **Avro** | 62k | 149k | fastavro generator-based. |
| **HTML** | n/a | 17k | Parse-only by design (stdlib `html.parser`; 2-3× faster with `[speedups]` lxml). |
| **XLSX** | 11k | n/a | openpyxl cell-by-cell; benchmark uses 10k rows. |

Notable finding: **Parquet / Feather parse are *slower than* NDJSON
parse** on this synthetic dataset. The columnar advantage shows up in
*file size* and *write batching* — but on parse we materialise back to
`List[Dict]` via `pyarrow.Table.to_pylist()`, which is the dominant
cost. If you keep data in Arrow form downstream (pyarrow / polars), you
get the true columnar parse speed. Incorporator's storage model
prioritises dict-native ergonomics over columnar throughput.

---

## Automatic Optimisations

These apply to every pipeline — no kwargs to set, no opt-in required.

### HTTP / network layer

* **HTTP/2 multiplexing** in the shared `httpx.AsyncClient`. One TCP/TLS
  connection carries every concurrent request, eliminating per-batch
  handshake overhead.
* **Long-lived connection pool** decoupled from worker count
  (`max_keepalive_connections=10, max_connections=concurrency_limit`).
  Idle connections amortise across pipeline runs.
* **Tenacity exponential backoff** with jitter; fatal HTTP 4xx (except
  429) breaks the retry loop immediately.

### Schema / Pydantic compile

* **LRU `SCHEMA_REGISTRY`** via `collections.OrderedDict` —
  `move_to_end()` on cache hit keeps hot schemas; `popitem(last=False)`
  evicts coldest. Long-running daemons that see many distinct shapes no
  longer thrash the cache.
* **Batched `Pydantic.model_validate`** in 1000-row chunks. Pydantic's
  Rust core amortises field-offset lookups across the batch instead of
  re-resolving per row.
* **Shallow copy of class attrs** (`dict(attr_val)`) on dynamic-class
  compilation instead of `copy.deepcopy()`. Long runs with 1000+
  compiled schemas no longer allocate gigabytes of redundant attribute
  state.
* **Stratified schema sampling** (up to 100 evenly-spaced records)
  instead of `data[:50]`. Rare field types at row 1M are now more
  likely to be discovered during inference.

### ETL transform

* **Loop inversion (rows outer, keys inner)** in `apply_etl_transformations`
  for exclusions and renames. One dict stays warm in CPU cache during
  the inner key loop instead of jumping between 10M+ distinct dicts.
* **Per-key sanitisation hoisted** out of Avro and XML write loops.
  `sanitize_json_key()` (and XML's clean-key logic) is cached in a dict
  on first occurrence — subsequent rows hit O(1) lookup instead of
  re-running the regex and reserved-name guard.

### Columnar I/O

* **In-place columnar parse** for Parquet / Feather / ORC. The
  `_table_to_dicts` helper mutates the dicts `to_pylist()` already
  allocated (no double-allocation) and only touches string columns
  (where JSON-encoded nested values could live).
* **`pyarrow.compute` vectorised JSON-prefix scan** on those string
  columns. `pc.starts_with(...)` runs in C across the whole column;
  `pc.any()` short-circuits in O(1). For the common case (string
  columns hold names/labels/ids — never JSON), the per-row Python loop
  is skipped entirely. Parquet parse: 159k → 200k rows/sec (+26%);
  Feather: 165k → 214k (+30%).

### Pipeline engine

* **`asyncio.to_thread` for user `outflow_fn`**. CPU-heavy joins in
  `fjord()` no longer block refresh / export daemons running on other
  sources.
* **Conditional `incorp_params.copy()`** in the chunked engine — single-
  shot mode (no paginator) reuses the original dict directly.
* **Removed manual `gc.collect()`** from the chunking hot loop. Python's
  generational GC handles short-lived datasets without manual
  intervention.

---

## Opt-In Performance Extras

These DO require an install flag:

| Extra | What it unlocks |
|---|---|
| `[speedups]` | `orjson` (GIL-releasing JSON), `lxml` (GIL-releasing XML/HTML), `cramjam` (Rust `zstd` / `lz4` / `snappy` / `brotli`). |
| `[parquet]` | `pyarrow` (~30 MB) — Parquet, Feather, ORC. Heavyweight; opt-in explicit. |
| `[avro]` | `fastavro` — generator-based Avro read/write. |
| `[xlsx]` | `openpyxl` (~250 KB) — Excel read/write. |

See [`installation.md`](./installation.md) for the full extras list and
[`formats_and_compression.md`](./formats_and_compression.md) for the
format-by-format support matrix.

---

## Reproducing the numbers

```bash
# Run only the benchmark suite (skipped by default in normal pytest runs)
pytest -m benchmark -v -s

# Run a single benchmark with prints visible
pytest tests/benchmarks/test_parse_throughput.py::test_parquet_parse_throughput -v -s
```

The `-s` flag is important — benchmark prints (e.g. `Parquet parse:
200,000 rows/sec`) come through `print()` and pytest swallows them
without it.

Floors are set conservatively below measured values so CI noise doesn't
flake the suite. If your numbers come in materially below the matrix
above, that's worth a GitHub issue — share your Python version, OS,
and installed extras.

---

## What's NOT optimised (yet)

Honest about the remaining gaps:

* **Columnar end-to-end.** Parquet → ETL → Parquet currently round-trips
  through `List[Dict]`. A future `return_arrow=True` opt-in mode could
  keep data in Arrow form for the whole pipeline.
* **Distributed rate limiting.** The token-bucket rate limiter is
  per-process; multi-instance deployments need Redis or similar.
* **HTTP/3 (QUIC).** httpx doesn't support QUIC yet. We follow upstream.

For the architectural rationale behind these trade-offs, see the
[Library reference](./library_reference.md) and
[`CONTRIBUTING.md`](../CONTRIBUTING.md).
