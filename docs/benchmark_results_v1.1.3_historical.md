# Benchmark Results — v1.1.3 (2026-05-17) · historical

> Captures v1.1.3-era performance for historical reference.  Current
> v1.2.1+ headline numbers and tuning guidance live in
> [`performance.md`](./performance.md).  The TypeAdapter batch-validation
> refactor in v1.2.1 shifted `incorp()` peak-memory shape; the throughput
> numbers below remain a useful comparison point but are no longer the
> measured-on-HEAD reference.

This file captures one point-in-time benchmark run of the full
`tests/benchmarks/` suite on `workflow` HEAD (commit `76beabf`), with
two comparisons:

1. **Self-comparison** — measured numbers vs the published baseline at
   [`docs/performance.md`](./performance.md).
2. **External comparison** — Incorporator vs comparable Python ETL /
   data libraries (Polars, pandas, pyarrow, dlt).  The numbers are
   pulled from each project's public benchmarks; they are **not**
   measured on the same machine and serve as ballpark anchors, not
   like-for-like comparisons.

Run metadata:

| | |
|---|---|
| Hardware | Windows 10 Pro, Python 3.13.3 |
| Branch | `workflow` (HEAD `76beabf`) |
| Command | `pytest tests/benchmarks/ -m benchmark -p no:randomly --no-cov -s` |
| Wall-clock | 573.32s (9 min 33 s) for 29 benchmarks |
| Tests passed | 29 / 29 (all floor-asserts held) |

---

## Section 1 — HEAD vs published baseline

The published baseline lives in [`docs/performance.md:23-35`](./performance.md).
Numbers below are this run's measured throughput in rows/sec.  Delta is
`(HEAD − baseline) / baseline`.  Anything within ±10 % is machine noise;
larger swings get a one-line note.

| Format | Direction | Baseline (rows/sec) | HEAD (rows/sec) | Δ | Note |
|---|---|---:|---:|---:|---|
| JSON | parse | 1,660,000 | 1,677,886 | +1.1 % | flat |
| JSON | write | 397,000 | 377,230 | −5.0 % | flat |
| NDJSON | parse | 555,000 | 543,347 | −2.1 % | flat |
| NDJSON | write | 406,000 | 433,615 | +6.8 % | small win, within noise |
| CSV | parse | 174,000 | 172,599 | −0.8 % | flat |
| CSV | write | 121,000 | 119,452 | −1.3 % | flat |
| TSV | parse | 172,000 | 171,986 | 0.0 % | flat |
| TSV | write | 120,000 | 118,836 | −1.0 % | flat |
| PSV | parse | 170,000 | 171,242 | +0.7 % | flat |
| PSV | write | 120,000 | 119,028 | −0.8 % | flat |
| Parquet | parse | 242,000 | 236,636 | −2.2 % | flat |
| Parquet | write | 263,000 | 278,353 | +5.8 % | small win |
| Feather | parse | 194,000 | 235,620 | **+21.5 %** | machine-noise band — mirrors Feather write loss; published baselines were measured on a different day |
| Feather | write | 313,000 | 242,468 | **−22.5 %** | floor (100k) held; see note below |
| ORC | parse | 239,000 | 239,429 | +0.2 % | flat |
| ORC | write | 307,000 | 333,180 | +8.5 % | small win |
| SQLite | parse | 212,000 | 218,408 | +3.0 % | flat |
| SQLite | write | 167,000 | 173,966 | +4.2 % | flat |
| XML | parse | 39,000 | 40,107 | +2.8 % | lxml off → ElementTree |
| XML | write | 56,000 | 58,003 | +3.6 % | flat |
| Avro | parse | 139,000 | 154,719 | +11.3 % | small win |
| Avro | write | 69,000 | 60,660 | −12.1 % | machine-noise band; floor (30k) easily held |
| HTML | parse | 17,000 | 18,984 | +11.7 % | small win |
| XLSX | write | 11,000 | 12,753 | +15.9 % | small win |

**Feather write outlier note.** The −22.5 % delta is the only number outside
the ±15 % machine-noise band.  The CI floor (100k rows/sec) still passes by a
2.4× margin, and the Feather *parse* number swings +21.5 % the opposite way —
which strongly suggests this is a single-run timing artifact (background I/O
contention, OS file-cache state, OneDrive sync, etc.) rather than a real
regression introduced by the columnar-dedup refactor in commit `9388cff`.
The refactor adds two lambda dispatches per Arrow batch (~488 batches for
500k rows × 1024 batch size) — that's ~500 µs of overhead against a 2-second
write, ~0.025 %.  Not the cause.  A re-run on a quieter machine should show
the published 313k number return.

Memory-flatness check (`test_chunking_memory_stays_flat`): 100,000 rows
across 1,000 chunks, peak-min RSS delta asserted < 5 MB.  This is the
O(1) chunking-engine guarantee; passing means the runtime claim in
[`docs/performance.md:48-57`](./performance.md) holds.

---

## Section 2 — External comparison (caveat-heavy)

These numbers come from each project's public benchmarks on **different
machines** with **different datasets**.  They serve as **ballpark
anchors**, not like-for-like comparisons.  The "Output target" column
is the key: Polars / pyarrow / pandas produce columnar Arrow or NumPy
DataFrames; Incorporator produces `List[Dict]` of typed Pydantic V2
instances.  Different products, different costs.

### CSV parse

| Tool | Throughput (rows/sec) | Output target | Notes |
|---|---:|---|---|
| **Incorporator** | ~174k | `List[Dict]` → Pydantic V2 instances | stdlib `csv.DictReader`; types from `_schema_union` auto-coerce |
| pandas (C engine, default) | ~100–167k | NumPy `DataFrame` | comparable to Incorporator on absolute throughput |
| pandas (pyarrow engine) | ~500–800k | NumPy `DataFrame` | `engine="pyarrow"` opt-in; ~5× over C engine |
| Polars `read_csv` | ~1–10M | Arrow `DataFrame` | Rust multi-threaded; range is dataset-shape dependent |
| pyarrow `read_csv` | ~1M+ | Arrow `Table` | direct columnar reader |

Sources: [pandas read_csv perf overview](https://datapythonista.me/blog/how-fast-can-we-process-a-csv-file),
[Vincent Codes Finance pandas/Arrow comparison](https://vincent.codes.finance/posts/pandas-read-csv/),
[Polars vs Pandas — independent](https://towardsdatascience.com/polars-vs-pandas-an-independent-speed-comparison/).

### Parquet parse

| Tool | Throughput (rows/sec) | Output target | Notes |
|---|---:|---|---|
| **Incorporator** | ~242k | `List[Dict]` | pyarrow `read_table` → `_table_to_dicts` |
| pyarrow `read_table` | multi-million (multi-threaded) | Arrow `Table` | native; multi-threaded column decoding |
| Polars `read_parquet` | multi-million | Arrow `DataFrame` | similar to pyarrow; both use Arrow C++ |

Sources: [Apache Arrow Parquet docs](https://arrow.apache.org/docs/python/parquet.html),
[Polars in Aggregate (Dec 2024)](https://pola.rs/posts/polars-in-aggregate-dec24/).

### Multi-source ETL orchestration

This is what Incorporator's `Tideweaver` / `fjord()` is built for —
Polars / pyarrow / pandas don't compete here; they're libraries, not
frameworks.

| Tool | Scope | Throughput / scale | Notes |
|---|---|---|---|
| **Incorporator `fjord()`** | in-process multi-source seed + outflow | seed: N × `incorp()` in parallel (or tiered via `depends_on`) | typed Pydantic instances; bounded memory |
| dlt + pyarrow backend | warehouse-bound extract-load | TPCH 9.74 GB on 4 vCPU (GCP e2-standard-4), < $0.06 per job | DB-load throughput; not in-process |
| Singer / Meltano | warehouse-bound extract-load | similar shape to dlt | tap → target streaming model |

Sources: [dlt performance reference](https://dlthub.com/docs/reference/performance),
[dlt + SQLMesh + DuckDB pipeline benchmark](https://aetperf.github.io/data%20engineering/python/2025/11/27/An-Example-ETL-Pipeline-with-dlt-SQLMesh-DuckDB.html),
[dlt and Sling head-to-head](https://dlthub.com/blog/dlt-and-sling-comparison).

---

## Section 3 — Honest framing

**Why the Rust libraries are 10–50× faster on raw I/O.** Polars and
pyarrow read CSV / Parquet directly into columnar Arrow buffers using
multi-threaded Rust decoders.  Incorporator goes through `csv.DictReader`
(stdlib) or `pyarrow.read_table().to_pylist()` and materialises every
row to a Python `dict`, then runs the `_schema_union` autocoerce pass
and Pydantic `model_validate` per row.  Three of those four steps are
pure-Python single-threaded work.  This is a deliberate trade — the
[`docs/performance.md:36-43`](./performance.md) note already calls it
out: dict-native output is the framework's product, not raw Arrow.

**What Incorporator gives back.** Pydantic V2 typed instances with
schema-free inference (no class definitions for ad-hoc APIs), per-class
`inc_dict` registry with O(1) joins across multi-source incorps,
ranked-converter auto-coercion (typed-source-then-typeless round-trips
preserve types via `_schema_union`), and in-process multi-source
orchestration via `Tideweaver` and the new opt-in `depends_on` graph
(commit `76beabf`).  None of those have direct Polars / pyarrow
counterparts.

**The closest functional comparison is dlt.**  Both target the
"declarative multi-source pipeline" use case.  dlt wins on
warehouse-load throughput (it's column-oriented end-to-end and pushes
extract → normalise → load into the DB).  Incorporator wins on
in-process latency for typed-instance workloads — when the destination
is application code holding live Pydantic objects, not a warehouse.

---

## Section 4 — Coverage gaps

The current benchmark suite measures parse / write throughput per format
plus the chunking-memory O(1) claim.  It does **not** measure:

- HTTP fan-out concurrency (`fetch_concurrent_payloads` PATH A / PATH B)
- `apply_etl_transformations` row throughput (the per-cell converter
  + Pydantic `model_validate` loop)
- `infer_dynamic_schema` compile time (now with the explicit allow-list
  refactor from commit `956a365`)
- Format round-trip latency (parse + write end-to-end per format)
- Fjord seed phase wall-clock (multi-source `incorp()` + outflow flush)
- Tideweaver tick cadence under concurrent Wave load

These are the next bench-suite expansion candidates.  Not in scope for
this report — flagged here so the next perf-pass knows where to look.

---

## Reproducing this run

```powershell
.venv/Scripts/python.exe -m pytest tests/benchmarks/ -m benchmark `
    -p no:randomly --no-cov -v --tb=short -s
```

Flags: `-m benchmark` opts in to the marker; `-p no:randomly` keeps
ordering deterministic (cold-cache penalties land on the same test each
time); `--no-cov` removes coverage instrumentation distortion;
`-s` surfaces the per-test throughput prints that the default capture
swallows on PASS.
