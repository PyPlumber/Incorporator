***

# 📦 Tutorial 3 — Universal Formats: Build a Crypto Snapshot Warehouse

Every hour, snapshot CoinGecko's top-100 markets into a multi-format warehouse — NDJSON / CSV / SQLite for the time-series log, Parquet for the hourly artifact. One `Coin(Incorporator)` class, one schema inference pass, four storage substrates. The framework auto-detects format from the file extension; the `incorp()` and `export()` calls stay identical across all of them.

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
| **SQLite** | `.db` / `.sqlite` / `.sqlite3` | ✅ | ✅ | ✅ append | core |
| **HTML tables** | `.html` / `.htm` | ✅ | ❌ | — | `pip install incorporator[speedups]` |
| **Parquet** | `.parquet` / `.pq` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **Feather (Arrow IPC)** | `.feather` / `.arrow` / `.ipc` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **ORC** | `.orc` | ✅ | ✅ | ❌ snapshot-only | `pip install incorporator[parquet]` |
| **Avro** | `.avro` | ✅ | ✅ | ✅ append | `pip install incorporator[avro]` |
| **Excel** | `.xlsx` / `.xlsm` | ✅ | ✅ | ❌ rebuilds | `pip install incorporator[xlsx]` |

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

## The Warehouse Script

Create `universal_formats.py` (the runnable version ships in this directory).
One `incorp()` call, one `Coin` class, then four `export()` calls that vary
only the `file_path` extension and a couple of format-specific kwargs:

```python
import asyncio
from pathlib import Path

from incorporator import Incorporator, register_host_penstock

# Pace api.coingecko.com at 0.2 req/sec (12/min) — the free-tier ceiling
# is 5-15/min documented.
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)


class Coin(Incorporator):
    """CoinGecko market row — auto-keyed by ``id``."""


COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


async def main() -> None:
    here = Path(__file__).resolve().parent
    data_dir = here / "out"
    data_dir.mkdir(exist_ok=True)
    print(f"Warehouse root: {data_dir}\n")

    # 1. One snapshot of CoinGecko's top-100 markets.
    coins = await Coin.incorp(
        inc_url=COINGECKO_MARKETS_URL,
        params={"vs_currency": "usd", "per_page": 100, "page": 1},
        inc_code="id",
        inc_name="name",
        excl_lst=["image"],  # heavy field — see Tutorial 1 inspector output
    )
    print(f"Loaded {len(coins)} coins from CoinGecko.")

    # 2. Append to NDJSON + CSV — both grow row-wise.
    await Coin.export(instance=coins, file_path=data_dir / "coins_log.ndjson", if_exists="append")
    await Coin.export(instance=coins, file_path=data_dir / "coins_log.csv", if_exists="append")
    print(f"Appended {len(coins)} rows to NDJSON + CSV log.")

    # 3. Append into a SQLite warehouse table.
    await Coin.export(
        instance=coins,
        file_path=data_dir / "coins_warehouse.sqlite",
        sql_table="coin_snapshots",
        if_exists="append",
    )
    print(f"Upserted {len(coins)} rows into SQLite warehouse.")

    # 4. Atomically snapshot-replace a Parquet file.
    parquet_path = data_dir / "coins_latest.parquet"
    try:
        await Coin.export(instance=coins, file_path=parquet_path, parquet_compression="snappy")
        print(f"Wrote Parquet snapshot ({parquet_path.stat().st_size} bytes).")
    except Exception as e:  # noqa: BLE001
        print(f"Parquet export skipped ({e!s}) — install incorporator[parquet]")

    # 5. Re-incorp every artifact and prove the object graph round-trips.
    print("\nRound-trip verification:")

    from_ndjson = await Coin.incorp(inc_file=data_dir / "coins_log.ndjson", inc_code="id")
    btc = from_ndjson.inc_dict["bitcoin"]
    print(f"  ndjson  -> BTC ${btc.current_price:,.2f}")

    from_csv = await Coin.incorp(inc_file=data_dir / "coins_log.csv", inc_code="id")
    btc = from_csv.inc_dict["bitcoin"]
    print(f"  csv     -> BTC ${btc.current_price:,.2f}")

    from_sqlite = await Coin.incorp(
        inc_file=data_dir / "coins_warehouse.sqlite",
        sql_query="SELECT * FROM coin_snapshots",
        inc_code="id",
    )
    btc = from_sqlite.inc_dict["bitcoin"]
    print(f"  sqlite  -> BTC ${btc.current_price:,.2f}")

    if parquet_path.exists():
        from_parquet = await Coin.incorp(inc_file=parquet_path, inc_code="id")
        btc = from_parquet.inc_dict["bitcoin"]
        print(f"  parquet -> BTC ${btc.current_price:,.2f}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Framework Highlights

### 1. `incorp()` Reads, `export()` Writes — Same Vocabulary

`export()` is the symmetric counterpart to `incorp()`: same class, same format
auto-detection from the file extension, same kwargs idiom. Steps 2–4 land the
snapshot in three different stores by varying only the `file_path` extension
and a couple of format-specific kwargs — everything else is identical.

### 2. Append-Friendly Log: NDJSON + CSV

Both NDJSON and CSV grow row-wise. Pass `if_exists="append"` and each call adds
a row block at the end of the file:

```python
await Coin.export(instance=coins, file_path=data_dir / "coins_log.ndjson", if_exists="append")
await Coin.export(instance=coins, file_path=data_dir / "coins_log.csv", if_exists="append")
```

NDJSON is safe to `grep`/`tail -f`/ship to log aggregators; CSV writes the
header once and appends rows after — good for spreadsheet consumers. Run this
on a cron / interval and the files grow linearly.

### 3. Append-Friendly Warehouse: SQLite

SQLite also supports `if_exists="replace"` (drop the table and rewrite), but
`append` here gives a true time-series log — one row block per tick, queryable
by snapshot order:

```python
await Coin.export(
    instance=coins, file_path=data_dir / "coins_warehouse.sqlite", sql_table="coin_snapshots", if_exists="append"
)
```

Analysts can then `SELECT id, current_price, last_updated FROM coin_snapshots
ORDER BY last_updated DESC` to walk the appended rows in insertion order.

### 4. Snapshot-Only: Parquet (Atomic Replace)

Parquet can't append per tick (the column-statistics footer would have to be
rewritten), so `export()` to a `.parquet` path **rebuilds the file atomically**
every call — write to a sibling tempfile, then `os.replace()` on success. A
crash mid-write leaves the *previous* snapshot in place, never a half-written
corrupt-footer file. This is the right pattern for hourly / daily *artifact*
dumps that downstream consumers (Athena, DuckDB, Spark, Snowflake) query
directly — for per-tick accumulation, stay in NDJSON / CSV / SQLite and let a
downstream batch job convert to Parquet at window close, see
[Appendix: Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md).

### 5. The Round Trip Proves the Schema Held

Read every format back and confirm the object graph is identical — SQLite
needs the SQL query since the extension alone can't pick a table; NDJSON,
CSV, and Parquet are pure file paths with the format detected from the
extension:

```python
from_sqlite = await Coin.incorp(
    inc_file=data_dir / "coins_warehouse.sqlite", sql_query="SELECT * FROM coin_snapshots", inc_code="id"
)
```

One Pydantic schema (zero lines — the framework inferred it from CoinGecko),
round-tripped through four storage substrates. No schema duplication, no
per-format models.

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
from incorporator import SQLitePaginator, SustainedPenstock

# Yields one chunk at a time; peak memory is one chunk.
db_streamer = SQLitePaginator(
    db_path=DATA / "coins_warehouse.sqlite",
    sql_query="SELECT * FROM coin_snapshots",
    chunk_size=10_000,
    penstock=SustainedPenstock(rate_per_sec=5.0),   # pace chunk reads
)
async for wave in Coin.stream(
    incorp_params={"inc_url": "local_warehouse", "inc_page": db_streamer},
    export_params={"file_path": DATA / "coins_export.parquet"},
):
    print(f"Streamed {wave.rows_processed} rows in chunk {wave.chunk_index}")
```

`CSVPaginator` and `AvroPaginator` follow the same shape and accept the same
keyword-only `penstock=` kwarg as the HTTP layer — one rate-limit primitive
across both.  T8 covers `stream()` end-to-end — for a paginated source like a
10,000+ coin pull, the same paginator powers the chunking-mode pipeline.

---

## Run it

```bash
python examples/03-universal-formats/universal_formats.py
```

No CLI form fits this one: it fans a single `incorp()` into four export formats
and then re-reads two of them to prove the round-trip preserves types. A
`stream` / `fjord` config produces one output — not a multi-format fan-out plus
verification — so this tutorial stays in Python.

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
