***

# 📦 Tutorial 3 — Universal Formats: Build a Crypto Snapshot Warehouse

The Incorporator promise: **same syntax for every format**.  The `incorp()` call you wrote
for a JSON API works without modification against a CSV from a stakeholder, a Parquet
file in your data lake, a SQLite snapshot from your nightly cron, or an Avro stream from
Kafka.  The framework auto-detects format from the file extension and dispatches to the
matching handler.

We'll use that uniformity to build the real-world crypto-ETL pattern: **periodically
snapshot CoinGecko's top-100 markets into a multi-format warehouse**.  Append-friendly
formats (NDJSON / CSV / SQLite) accumulate a time-series log; columnar formats
(Parquet / Feather / ORC) get snapshot-and-replaced each tick.  Both shapes share one
`Coin(Incorporator)` class and one schema inference pass.

**Prerequisites:** [Tutorial 1 — First Steps](../01-first-steps/README.md) (`incorp()`, `test()`,
`inc_dict`, basic schema inference); [Tutorial 2 — Data Lake Pivot](../02-data-lake-pivot/README.md)
(the pivot arc on a single source).

---

## Format Coverage at a Glance

| Format | Extension | Read | Write | **Append?** | Install |
|---|---|---|---|---|---|
| **JSON** | `.json` | ✅ | ✅ | ❌ rebuilds | core |
| **NDJSON** *(streaming JSON)* | `.ndjson` / `.jsonl` | ✅ | ✅ | ✅ append | core |
| **CSV / TSV / PSV** | `.csv` / `.tsv` / `.psv` | ✅ | ✅ | ✅ append | core |
| **XML** | `.xml` | ✅ | ✅ | ❌ rebuilds | core (lxml in `[speedups]`) |
| **SQLite** | `.db` / `.sqlite` / `.sqlite3` | ✅ | ✅ | ✅ upsert | core |
| **HTML tables** | `.html` | ✅ | ❌ | — | `pip install incorporator[speedups]` |
| **Parquet** | `.parquet` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **Feather (Arrow IPC)** | `.feather` / `.arrow` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **ORC** | `.orc` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **Avro** | `.avro` | ✅ | ✅ | ✅ append | `pip install incorporator[avro]` |
| **Excel** | `.xlsx` | ✅ | ✅ | ❌ rebuilds | `pip install incorporator[xlsx]` |

Compression is **transparent** for `.gz`, `.bz2`, `.xz`, `.lzma`, `.zip`, `.tar`, `.tgz`
— the framework decompresses before parsing, no extra calls.  See
[Formats & Compression](../../docs/formats_and_compression.md) for the full cheat sheet.

> **Why the append column matters.** Append-friendly formats let you accumulate a
> time-series snapshot warehouse cheaply — every tick appends a row block, the file
> grows linearly, and crash recovery is cheap.  Columnar formats (Parquet / Feather /
> ORC) write a footer at the *end* of the file that indexes every column's row groups;
> appending requires reading the old footer, rewriting both the data and the footer, and
> atomically replacing the file.  That's slow at scale and wrong for per-tick writes.
> Pick append-friendly formats for *accumulating* warehouses; pick columnar for
> *snapshot* artifacts (hourly / daily rebuilds, query-friendly final outputs).

---

## The Universal Call Shape

Every format uses the same `incorp()` signature:

```python
result = await SomeClass.incorp(inc_file="path/to/data.<ext>", inc_code="...")
```

The framework infers the format from the file extension.  Same return shape, same
dot-notation, same `inc_dict` registry, regardless of source file.

---

## Step 1: Snapshot the Source Once

Same `incorp()` call you wrote in T1 — top-100 coins, CoinGecko's
`/coins/markets`:

```python
import asyncio

from incorporator import Incorporator


class Coin(Incorporator):
    pass


async def snapshot():
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 100, "page": 1},
        inc_code="id",
        inc_name="name",
    )
    print(f"📥 Loaded {len(coins)} coins from CoinGecko.")
    return coins
```

You now have a typed object graph in memory.  Time to land it in the warehouse.

---

### A quick word on `export()`

`incorp()` reads.  `export()` writes — the symmetric counterpart, with the same
class, the same format auto-detection from the file extension, and the same kwargs
idiom you just used.  Steps 2–4 below land the snapshot in three different stores by
varying only the `file_path` extension and a couple of format-specific kwargs;
everything else is identical between the three calls.

---

## Step 2: Append to NDJSON and CSV (Append-Friendly Log)

Both NDJSON and CSV grow row-wise.  Pass `if_exists="append"` to `export()` and each
call adds a row block at the end of the file:

```python
from pathlib import Path

DATA = Path("data")
DATA.mkdir(exist_ok=True)


async def append_log(coins):
    # NDJSON: one JSON object per line; safe to grep, tail -f, ship to log aggregators.
    await Coin.export(
        instance=coins,
        file_path=DATA / "coins_log.ndjson",
        if_exists="append",
    )
    # CSV: header written once, rows appended.  Good for spreadsheet consumers.
    await Coin.export(
        instance=coins,
        file_path=DATA / "coins_log.csv",
        if_exists="append",
    )
    print(f"📜 Appended {len(coins)} rows to NDJSON and CSV log.")
```

Run `snapshot()` and `append_log()` on a cron / interval; the files grow linearly.

---

## Step 3: Upsert into SQLite (Append-Friendly Warehouse)

SQLite uses `if_exists="append"` too, but the framework also supports
`if_exists="replace"` (drop the table and rewrite) and the more interesting upsert
pattern via `INSERT OR REPLACE` semantics — keyed by `inc_code`:

```python
async def upsert_warehouse(coins):
    await Coin.export(
        instance=coins,
        file_path=DATA / "coins_warehouse.sqlite",
        sql_table="coin_snapshots",
        if_exists="append",                              # accumulate rows; query later
    )
    print(f"🗃️  Upserted {len(coins)} rows into SQLite warehouse.")
```

Now your analysts can `SELECT id, current_price, snapshot_time FROM coin_snapshots
ORDER BY snapshot_time DESC` to walk the time series.

---

## Step 4: Snapshot-Write Parquet (Columnar, Query-Friendly)

Parquet can't append per tick (the column-statistics footer would have to be rewritten),
so `export()` to a `.parquet` path **rebuilds the file atomically** every call — write
to a sibling tempfile, then `os.replace()` on success.  A crash mid-write leaves the
*previous* snapshot in place; never a half-written corrupt-footer file.

```python
async def snapshot_parquet(coins):
    await Coin.export(
        instance=coins,
        file_path=DATA / "coins_latest.parquet",         # atomic snapshot-and-replace
        parquet_compression="snappy",
    )
    print(f"📊 Wrote Parquet snapshot of {len(coins)} coins.")
```

This is the right pattern for hourly / daily *artifact* dumps that downstream consumers
(Athena, DuckDB, Spark, Snowflake) query directly.  For per-tick accumulation, stay in
NDJSON / CSV / SQLite and let a downstream batch job convert to Parquet at window close
— see [Appendix: Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md).

---

## Step 5: Round-Trip — Re-`incorp()` Each Artifact

The warehouse is only as useful as the round-trip.  Read every format back and verify
the object graph is identical:

```python
async def verify_round_trip():
    # NDJSON / CSV / Parquet — pure file paths; format detected from extension.
    from_ndjson = await Coin.incorp(inc_file=DATA / "coins_log.ndjson", inc_code="id")
    from_csv    = await Coin.incorp(inc_file=DATA / "coins_log.csv",    inc_code="id")
    from_parquet = await Coin.incorp(inc_file=DATA / "coins_latest.parquet", inc_code="id")

    # SQLite needs the SQL query (extension isn't enough to pick a table).
    from_sqlite = await Coin.incorp(
        inc_file=DATA / "coins_warehouse.sqlite",
        sql_query="SELECT * FROM coin_snapshots WHERE snapshot_time = "
                  "(SELECT MAX(snapshot_time) FROM coin_snapshots)",
        inc_code="id",
    )

    for label, snap in [("ndjson", from_ndjson), ("csv", from_csv),
                         ("parquet", from_parquet), ("sqlite", from_sqlite)]:
        btc = snap.inc_dict["bitcoin"]
        print(f"  {label:8s} → BTC ${btc.current_price:,.2f}")
```

You wrote one Pydantic schema (zero lines — the framework inferred it from CoinGecko).
You're round-tripping that schema through five storage substrates.  No schema
duplication, no per-format models.

---

## Format-Specific Kwargs

The universal call accepts a small number of **format-specific kwargs** when the file
format needs them:

| Format | Extra kwarg | Why |
|---|---|---|
| SQLite | `sql_query="SELECT ..."` | SQL is execution-shaped, not extension-shaped |
| SQLite | `sql_table="coin_snapshots"` | `export()` target table name |
| CSV / TSV / PSV | `if_exists="append"` | append on write |
| NDJSON | `if_exists="append"` | append on write |
| Avro | `if_exists="append"` | append on write |
| Parquet | `parquet_compression="snappy"` | column-encoding compression |
| Feather | `feather_compression="lz4"` | Feather V2's default |
| Excel | `sheet_name="..."` | non-default sheet |
| HTML | `table_index=0` | pick which `<table>` on the page |
| Archive | `archive_target="data.json"` | name the file inside a multi-member archive |

Everything else (`inc_code`, `inc_name`, `conv_dict`, `excl_lst`, `name_chg`) stays
format-agnostic.

---

## Streaming Massive Files

The synchronous `incorp(inc_file=...)` path materialises the whole file before parsing
— fine for the typical "a few hundred MB" case, but it OOMs on multi-GB inputs.  For
files larger than RAM, use the **local paginators** in
[`incorporator.io.pagination`](../../docs/streaming_and_pagination.md):

```python
from incorporator.io.pagination import SQLitePaginator

# Yields one chunk at a time; peak memory is one chunk.
db_streamer = SQLitePaginator(
    db_path=DATA / "coins_warehouse.sqlite",
    sql_query="SELECT * FROM coin_snapshots",
    chunk_size=10_000,
)
async for wave in Coin.stream(
    incorp_params={"inc_url": "local_warehouse", "inc_page": db_streamer},
    export_params={"file_path": DATA / "coins_export.parquet"},
):
    print(f"Streamed {wave.rows_processed} rows in chunk {wave.chunk_index}")
```

`CSVPaginator` and `AvroPaginator` follow the same shape.  T8 covers `stream()`
end-to-end — for a paginated source like a 10,000+ coin pull, the same paginator powers
the chunking-mode pipeline.

---

## Where to Go Next

> 👉 **Up next: [Tutorial 4 — XML Post Audit](../04-xml-post-audit/README.md).**  Compliance teams audit warehouses like the one you just built every day.  T4 walks through a used-car fraud case: an XML invoice ledger enriched via one batched POST against NHTSA's federal VIN database, joined on VIN to flag discrepancies.  Teaches XML inflow + POST shapes — and lets CoinGecko's per-minute window refresh before T5's 11 child drills.

| Goal | Read |
|---|---|
| Discover the right kwargs for an unknown source first | [Tutorial 1 — First Steps + DX Inspector](../01-first-steps/README.md) |
| Audit a warehouse against a federal source (XML + POST) | [Tutorial 4 — XML Post Audit](../04-xml-post-audit/README.md) |
| Join a parent endpoint to per-record detail children | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Keep the warehouse source data fresh | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| Run the warehouse loader as a long-running daemon | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Land columnar Parquet from an orchestrated pipeline | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Stream a file too big for RAM | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/03-universal-formats/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
