# ⚡ Performance

This page answers two questions:

1. **Is Incorporator fast enough for my workload?** — see the
   [throughput matrix](#throughput-at-a-glance) and
   [memory profile](#memory-profile).
2. **What knobs do I have if it isn't?** — see
   [getting more out of it](#getting-more-out-of-it) and
   [opt-in extras](#opt-in-extras).

Numbers below are measured locally on Windows 10, Python 3.13, 500k-row
synthetic datasets — captured on the v1.1.3 release run
([`docs/benchmark_results_v1.1.3_historical.md`](./benchmark_results_v1.1.3_historical.md)
has the full per-test trace).  Every line is reproducible via
[`pytest -m benchmark`](#reproducing-the-numbers) and enforced by
conservative CI floors so regressions are caught on every PR.

> **Post-v1.1.3 note.** The headline throughput numbers below are
> pinned to the v1.1.3 baseline.  Subsequent releases haven't moved
> them meaningfully for typed-input workloads, but the converter
> dispatch path (`inc` / `calc` / `pluck` / `link_to` /
> `split_and_get`) gained a unified null-handling pre-check that
> short-circuits garbage values (`None`, `""`, `"n/a"`, `"null"`,
> `"unknown"`, `"nan"`, `"undefined"`) before the user callable
> runs — eliminating a per-row exception raise plus a `logger.warning`
> call on garbage-heavy datasets.  Net effect: ~95% faster dispatch
> on garbage-heavy data; <0.5% overhead on clean data.  Run
> `pytest -m benchmark` locally to measure on your hardware.

---

## Throughput at a glance

| Format | Streaming write | Parse | Notes |
|---|---|---|---|
| **JSON** | 377k rows/sec | **1,678k rows/sec** | orjson dominates parse — ~4× the write rate. |
| **NDJSON** | 434k rows/sec | 543k rows/sec | Line-by-line in both directions; ideal for append + tail workloads. |
| **CSV** | 119k rows/sec | 173k rows/sec | `csv.DictReader` / `csv.DictWriter`; stdlib only. |
| **TSV** | 119k rows/sec | 172k rows/sec | Same engine as CSV. |
| **PSV** | 119k rows/sec | 171k rows/sec | Same engine as CSV. |
| **Parquet** | 278k rows/sec | 237k rows/sec | Streaming row-group writes; vectorised string scans on parse. |
| **Feather** | 242k rows/sec | 236k rows/sec | Memory-mapped reads; fastest columnar write. |
| **ORC** | 333k rows/sec | 239k rows/sec | Same Arrow pipeline as Parquet. |
| **SQLite** | 174k rows/sec | 218k rows/sec | `executemany()` bulk insert; full cursor fetch. |
| **XML** | 58k rows/sec | 40k rows/sec | Element-tree serialisation; 2–3× faster with `[speedups]` lxml. |
| **Avro** | 61k rows/sec | 155k rows/sec | fastavro generator-based, schema-on-write. |
| **HTML** | n/a | 19k rows/sec | Parse-only; closes the `pandas.read_html` gap. |
| **XLSX** | 13k rows/sec | n/a | openpyxl cell-by-cell; meant for human-scale spreadsheets, not analytics. |

A surprise worth calling out: **on dict-shaped output, JSON / NDJSON
parse beat the columnar formats**. The reason is that Incorporator
materialises results to `List[Dict]` to keep dot-notation access cheap.
If you keep data in Arrow form downstream (pyarrow, polars), you skip
that materialisation and the columnar formats reclaim the speed lead.
For the storage-and-go case the project optimises for, dict-native
ergonomics win.

---

## Memory profile

The chunked engine processes datasets of arbitrary size in
**bounded memory** — peak RSS stays flat as you stream more chunks
through the pipeline.

The `test_chunking_memory_stays_flat` benchmark asserts that
processing 1,000 chunks of 100 rows each (100,000 rows total) grows
the working set by **less than 5 MB**. The pipeline reuses chunk
buffers; nothing accumulates per-chunk except the optional Wave log
record.

What this means in practice:

- **`incorp()` (one-shot)** holds the whole payload in memory by
  design — fits in `O(rows × row_size)`.
- **`stream()` and `fjord()`** are O(1) in chunk size, independent of
  total rows. You can ingest a 10M-row CSV in under 100 MB of RSS
  with `chunk_size=10_000`.
- **`export()`** streams through the same buffer; writing Parquet,
  Feather, ORC, NDJSON, or CSV keeps memory flat regardless of
  output size.

---

## What you should know

These behaviours are on by default. You don't configure them; they
shape the runtime you observe.

**Schemas are inferred once per shape.** The first time a class sees
a new field structure, it samples up to 100 evenly-spaced rows and
compiles a Pydantic model — typically 50–100 ms. Subsequent calls
with the same shape hit the schema cache and skip inference entirely.
Long-running daemons see effectively zero schema-compile cost after
warmup. Distinct shapes age off via LRU eviction, so a polling
process that sees 1,000+ shapes won't leak memory.

**The HTTP layer is shared across calls.** A single `httpx.AsyncClient`
with HTTP/2 multiplexing carries every concurrent request — one TLS
handshake serves the lifetime of the process. The connection pool
keeps up to 10 idle sockets ready and scales out under concurrent
load. Retries (HTTP 429, 5xx) use Tenacity's exponential backoff
with jitter; fatal 4xx errors break the loop immediately so you don't
burn budget on permanent failures.

**Pydantic validation is batched.** Rows are validated 1,000 at a
time, which lets Pydantic's Rust core amortise field-offset lookups
across the batch. The cost is invisible to callers — you see
`List[Incorporator]` either way — but it's why the framework keeps
up with orjson's parse rate on JSON workloads.

---

## Getting more out of it

If the matrix above isn't fast enough for your workload, here's what
to try, in roughly the order that gives the most return:

### 1. Install `[speedups]` — replaces stdlib with C/Rust

```bash
pip install 'incorporator[speedups]'
```

Unlocks three drop-in replacements that the runtime detects
automatically:

- **`orjson`** — releases the GIL on JSON parse/write. Multi-gigabyte
  payloads now deserialise on background threads in parallel with
  ETL work.
- **`lxml`** — releases the GIL on XML/HTML. Same parallelism benefit
  plus C-level security flags (`resolve_entities=False`) that
  shield you from XXE bombs.
- **`cramjam`** — Rust bindings for `zstd` / `lz4` / `snappy` /
  `brotli` decompression. Decompression-bomb protection is enforced
  via a configurable byte ceiling (`INCORPORATOR_MAX_DECOMPRESSED_BYTES`,
  defaults to 1 GB).

No code change required — handlers see the optional library at import
time and route through it transparently.

### 2. Install `[parquet]` for big-data formats

```bash
pip install 'incorporator[parquet]'
```

Adds pyarrow (~30 MB) and unlocks Parquet, Feather, ORC. Hits the
rates in the matrix on those rows. On Windows the install also pulls
in `tzdata` so pyarrow's ORC reader works out of the box.

### 3. Use the streaming verbs for bounded memory

For multi-million-row pipelines, switch from `incorp()` to `stream()`
or `fjord()`:

```python
async for wave in MyClass.stream(inc_url=..., chunk_size=10_000):
    print(wave.rows_processed, wave.processing_time_sec)
```

The chunked engine processes 10k-row windows at a time, releasing
each window's memory before fetching the next. Both verbs accept
`outflow=` to plug in a user-defined reducer, and `refresh_params={}`
to re-poll the source on an interval.

### 4. Tune `chunk_size`

Larger chunks amortise per-batch overhead (HTTP round trip, Pydantic
batch setup) but consume proportionally more RAM. The sweet spot
depends on row size:

- Small rows (< 1 KB each): `chunk_size=10_000` to `50_000`.
- Medium rows (1–10 KB): `chunk_size=1_000` to `5_000`.
- Large rows (> 10 KB, e.g. nested JSON): `chunk_size=100` to `1_000`.

The default `chunk_size=1_000` is conservative — bump it up if your
profile shows the HTTP/parse work is dominating per-chunk overhead.

### 5. Let `stream()` tune its own `chunk_size` (v1.2.1+)

Set `adapt_chunk_size=True` and the chunked engine resizes
`paginator.chunk_size` between chunks via AIMD (additive-increase /
multiplicative-decrease).  The bounds and latency window are explicit:

```python
async for wave in Cls.stream(
    incorp_params={...},
    adapt_chunk_size=True,
    chunk_size_min=100, chunk_size_max=100_000,
    target_min_sec=0.030, target_max_sec=0.100,
):
    ...
```

Each chunk's observed processing time decides the next chunk: below
`target_min_sec` the engine grows `chunk_size` additively; above
`target_max_sec` it shrinks multiplicatively.  Useful when the
per-chunk cost is dominated by something the caller can't predict
(slow upstream, variable row size, GC pressure).

### 6. Extend the next-pass wait under Tideweaver backlog (v1.2.1+)

When a `Tideweaver` scheduler is consistently saturated (every pass
runs over `pass_interval` because in-flight ticks haven't completed),
pass `backlog_backoff_factor=2.0` on the constructor to multiplicatively
extend the wait until the heap drains.  The default `1.0` is disabled —
behaviour identical to v1.2.0.

```python
Tideweaver(watershed, backlog_backoff_factor=2.0).run()
```

Reach for it when `tide.next_due_in_sec` is consistently negative;
leave it at `1.0` when passes finish well inside their interval.

### 7. Write efficient `conv_dict` reducers

Per-row converters run inside the validation hot loop. From fastest
to slowest:

1. **Direct type calls** — `int`, `float`, `bool`, `str` are C-speed.
2. **stdlib helpers** — `datetime.fromisoformat`, `Decimal`,
   `pathlib.Path`. ~10× slower than a bare type call but still fast.
3. **Compiled regex** — `re.compile(...)` once, reuse the match
   object. Avoid `re.match()` with an inline pattern (recompiles
   per row).
4. **Custom Python callables** — fine for small datasets; consider
   pushing logic into a vectorised post-processing step
   (`pyarrow.compute`, `pandas`) if the row count climbs past 1M.

---

## Opt-in extras

| Extra | Adds | When to use |
|---|---|---|
| `[speedups]` | orjson, lxml, cramjam | Almost always — drops in C-speed JSON/XML/decompression. |
| `[parquet]` | pyarrow, tzdata (Windows) | Working with Parquet, Feather, or ORC. |
| `[avro]` | fastavro | Kafka, Hadoop, or schema-registry pipelines. |
| `[xlsx]` | openpyxl | Human spreadsheets (< 100k rows). |
| `[all]` | speedups + avro + xlsx | Everything except pyarrow. |

`[all]` deliberately omits `[parquet]` because pyarrow's 30 MB
footprint is the wrong default for users who never touch columnar
formats. See [`installation.md`](./installation.md) for the full
extras list and [`formats_and_compression.md`](./formats_and_compression.md)
for the format-by-format support matrix.

---

## Reproducing the numbers

```bash
# Run only the benchmark suite (skipped by default — opt-in marker).
pytest -m benchmark -v -s

# Run a single benchmark with stdout visible.
pytest tests/benchmarks/test_parse_throughput.py::test_parquet_parse_throughput -v -s
```

The `-s` flag matters — the benchmarks print rows/sec via `print()`
and pytest swallows that without it.

The same suite runs in CI on every PR via
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) (well — the
suite is opt-in there too, gated behind `-m benchmark`; the
non-benchmark tests run on every push). Floors in
[`tests/benchmarks/`](../tests/benchmarks/) are set conservatively
below the measured rates so transient CI noise doesn't flake
the suite. If your local numbers come in materially below the matrix
above, open an issue with your Python version, OS, and installed
extras — there's usually a missing speedup install behind it.

---

## Known performance boundaries

Honest about the limits:

- **Columnar end-to-end.** Parquet → ETL → Parquet currently
  round-trips through `List[Dict]`. The parse-rate gap to NDJSON in
  the matrix above comes from this materialisation, not from
  pyarrow itself. A future `return_arrow=True` opt-in mode could
  preserve Arrow form across the pipeline; for now, if you need
  columnar throughput end-to-end, post-process the parsed list with
  pyarrow / polars directly.
- **Rate limiting is per-process.** The token-bucket rate limiter
  applies inside a single Python process. Multi-instance deployments
  (e.g. several Docker containers hitting the same upstream API)
  need Redis or a similar shared bucket — not yet built in.
- **HTTP/3 (QUIC) is not supported.** httpx itself doesn't ship
  QUIC yet; the framework follows upstream. HTTP/2 multiplexing
  closes most of the practical gap for paginated APIs.
- **XLSX is human-scale.** openpyxl's cell-by-cell write is
  fundamentally row-bound; ~13k rows/sec is close to the library
  ceiling on commodity hardware. For analytics, pick Parquet, Feather,
  or ORC instead.
- **Sustained ingest above ~10k rows/sec hits a `WeakValueDictionary`
  ceiling.** `Incorporator.inc_dict` is a `WeakValueDictionary` (the
  chunking-mode lifetime model lets instances die naturally when the
  caller stops holding a reference, so long-running daemons don't
  leak). Insertion costs ~100-200 ns per row beyond a plain `dict`,
  driven by Python's `weakref` proxy + callback registration. Under
  sustained ingest above ~10k rows/sec the weakref machinery becomes
  measurable in the per-row hot path.

  If you need higher sustained throughput, hold strong references at
  the call site. Inside a Tideweaver, read `cls._tideweaver_snapshot`
  (which the scheduler parks after each Stream tick) instead of
  iterating `cls.inc_dict.values()`. Outside a Tideweaver, accumulate
  into a plain `list` yourself if the lifetime model permits. The
  `WeakValueDictionary` insert is the dominant per-row cost above
  this threshold; everything else (Pydantic validation,
  `conv_dict` ops, `is_garbage_value` checks) is amortised by
  Pydantic's Rust core.
- **`incorp()` peak memory is O(N), not O(chunk_size).** As of v1.2.1
  (A-F-4), `incorp()` validates the full payload in a single
  `TypeAdapter(List[Cls]).validate_python(rows)` call — measured
  1.3-2.0× faster than the prior row-by-row loop (per
  [`tests/benchmarks/test_validate_batch_vs_per_row.py`](../tests/benchmarks/test_validate_batch_vs_per_row.py)),
  but it materialises all N instances simultaneously. For payloads
  larger than ~100k rows where peak memory matters, use chunking-mode
  `Cls.stream(...)` instead: the paginator-driven `stream()` invokes
  `incorp()` once per page-sized chunk, preserving the per-call
  O(chunk_size) memory bound at the stream layer regardless of
  `incorp()`'s internal shape.
  Validation-error reporting also changed: errors now accumulate
  across all rows in a single `ValidationError` rather than raising on
  the first bad row — more informative messages, larger error
  surfaces under pathological input.

For the architectural rationale behind these trade-offs, see
[`CONTRIBUTING.md`](../CONTRIBUTING.md) and the relevant docstrings
in [`incorporator/`](../incorporator/).

---

## Where to Go Next

| Goal | Read |
|---|---|
| Switch to chunking mode to keep memory flat | [Streaming & Pagination Deep Dive](./streaming_and_pagination.md) |
| Pick append-friendly vs columnar formats deliberately | [Formats & Compression](./formats_and_compression.md) |
| Drain a paginated API into a warehouse without OOM | [Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md) |
| Coordinate per-source intervals to spread load | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) |
| Ship with Docker healthchecks + heartbeat files | [Deployment Guide](./deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/performance.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
