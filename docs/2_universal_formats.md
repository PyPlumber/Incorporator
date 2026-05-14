***

# 📦 Universal Formats: One Verb, Any File

The Incorporator promise: **same syntax for every format**. The
`incorp()` call you wrote for a JSON API works without modification
against a CSV from a stakeholder, a Parquet file in your data lake, a
SQLite snapshot from your nightly cron, an XML feed from a vendor, or
an Excel sheet someone emailed you. The framework auto-detects format
from the file extension and dispatches to the matching handler.

This tutorial walks the universal call shape across every supported
format and closes with the data-lake "pivot" — round-trip from a REST
API into Parquet for analytics consumers and SQLite for OLTP
consumers, all from one `incorp()` source.

---

## Format Coverage at a Glance

| Format | Extension | Read | Write | Append | Install |
|---|---|---|---|---|---|
| **JSON** | `.json` | ✅ | ✅ | ❌ | core |
| **NDJSON** *(streaming JSON)* | `.ndjson` / `.jsonl` | ✅ | ✅ | ✅ | core |
| **CSV / TSV / PSV** | `.csv` / `.tsv` / `.psv` | ✅ | ✅ | ✅ | core |
| **XML** | `.xml` | ✅ | ✅ | ❌ | core (lxml in `[speedups]`) |
| **SQLite** | `.db` / `.sqlite` / `.sqlite3` | ✅ | ✅ | ✅ | core |
| **HTML tables** | `.html` | ✅ | ❌ | — | `pip install incorporator[speedups]` |
| **Parquet** | `.parquet` | ✅ | ✅ | ❌ | `pip install incorporator[parquet]` |
| **Feather (Arrow IPC)** | `.feather` / `.arrow` | ✅ | ✅ | ❌ | `pip install incorporator[parquet]` |
| **ORC** | `.orc` | ✅ | ✅ | ❌ | `pip install incorporator[parquet]` |
| **Avro** | `.avro` | ✅ | ✅ | ✅ | `pip install incorporator[avro]` |
| **Excel** | `.xlsx` | ✅ | ✅ | ❌ | `pip install incorporator[xlsx]` |

Compression is **transparent** for `.gz`, `.bz2`, `.xz`, `.lzma`,
`.zip`, `.tar`, `.tgz` — the framework decompresses before parsing,
no extra calls. See [Formats & Compression](./formats_and_compression.md)
for the full cheat sheet.

---

## The Universal Call Shape

Every format uses the same `incorp()` signature:

```python
result = await SomeClass.incorp(inc_file="path/to/data.<ext>", inc_code="...")
```

The framework infers the format from the file extension. Same return
shape, same dot-notation, same `inc_dict` registry, regardless of the
source file.

---

## Step 1: The Same Data, Five Formats

The fixtures below contain identical content — three trades from a
toy ledger — encoded in five different formats:

```python
# trades.json
[
    {"trade_id": "T001", "symbol": "AAPL", "qty": 100, "price": 175.50},
    {"trade_id": "T002", "symbol": "MSFT", "qty": 50,  "price": 410.25},
    {"trade_id": "T003", "symbol": "GOOG", "qty": 25,  "price": 162.80}
]
```

```csv
# trades.csv
trade_id,symbol,qty,price
T001,AAPL,100,175.50
T002,MSFT,50,410.25
T003,GOOG,25,162.80
```

```xml
<!-- trades.xml -->
<root>
  <item><trade_id>T001</trade_id><symbol>AAPL</symbol><qty>100</qty><price>175.50</price></item>
  <item><trade_id>T002</trade_id><symbol>MSFT</symbol><qty>50</qty>  <price>410.25</price></item>
  <item><trade_id>T003</trade_id><symbol>GOOG</symbol><qty>25</qty>  <price>162.80</price></item>
</root>
```

Plus `trades.parquet`, `trades.sqlite`, `trades.xlsx`, etc. produced
by any data-pipeline tool you like.

**One class, five reads:**

```python
import asyncio
from incorporator import Incorporator


class Trade(Incorporator):
    pass


async def main():
    for path in ["trades.json", "trades.csv", "trades.xml",
                 "trades.parquet", "trades.sqlite"]:
        kwargs = {"inc_file": path, "inc_code": "trade_id"}
        if path.endswith(".sqlite"):
            kwargs["sql_query"] = "SELECT * FROM trades"
        elif path.endswith(".xml"):
            # xml_to_dict wraps every doc in {<root_tag>: ...} and groups
            # identically-named children into a list — drill the dotted path
            # to the leaf array.
            kwargs["rec_path"] = "root.item"
        trades = await Trade.incorp(**kwargs)

        print(f"{path:20s} → {len(trades)} rows; AAPL qty: {Trade.inc_dict['T001'].qty}")


asyncio.run(main())
```

Every iteration prints the same logical result. The framework
absorbed format diversity at the I/O boundary; your application code
stays uniform.

---

## Round-Trip Pattern: Data-Lake Pivot

A common production pattern: pull from a REST API once, then write
the data to **two** stores — Parquet for analytics, SQLite for
transactional lookups.

```python
import asyncio
from incorporator import Incorporator


class Coin(Incorporator):
    pass


async def pivot():
    # 1. Pull from a REST API.
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 100},
        inc_code="id",
        inc_name="name",
    )
    print(f"Loaded {len(coins)} coins from CoinGecko.")

    # 2. Write to Parquet for the analytics team.
    await Coin.export(instance=coins, file_path="data/coins.parquet")

    # 3. Write to SQLite for the API team.
    await Coin.export(
        instance=coins,
        file_path="data/coins.sqlite",
        sql_table="coins",
        if_exists="replace",
    )

    # 4. Verify both reads round-trip cleanly.
    from_parquet = await Coin.incorp(inc_file="data/coins.parquet", inc_code="id")
    from_sqlite = await Coin.incorp(
        inc_file="data/coins.sqlite",
        sql_query="SELECT * FROM coins",
        inc_code="id",
    )
    assert from_parquet.inc_dict["bitcoin"].current_price > 0
    assert from_sqlite.inc_dict["bitcoin"].current_price > 0
    print("Round-trip OK: Parquet and SQLite both produce the same object graph.")


asyncio.run(pivot())
```

Two `export()` calls, two different stores, one source. The Parquet
file is consumable by pandas / DuckDB / Snowflake / BigQuery. The
SQLite file is consumable by `sqlite3` or any ORM. Both came from the
same Pydantic instances in memory — no schema duplication.

---

## Format-Specific Kwargs

The universal call accepts a small number of **format-specific
kwargs** when the file format needs them:

| Format | Extra kwarg | Why |
|---|---|---|
| SQLite | `sql_query="SELECT ..."` | SQL is execution-shaped, not extension-shaped |
| SQLite | `sql_table="trades"` | export() target table name |
| CSV / TSV / PSV | `if_exists="append"` | NDJSON-style append on write |
| NDJSON | `if_exists="append"` | append on write |
| Avro | `if_exists="append"` | append on write |
| Parquet | `parquet_compression="snappy"` | column-encoding compression |
| Feather | `feather_compression="lz4"` | Feather V2's default |
| Excel | `sheet_name="..."` | non-default sheet |
| HTML | `table_index=0` | pick which `<table>` on the page |
| Archive | `archive_target="data.json"` | name the file inside a multi-member archive |

Everything else (`inc_code`, `inc_name`, `conv_dict`, `excl_lst`,
`name_chg`) stays format-agnostic.

---

## Streaming Massive Files

For files larger than RAM (databases, multi-GB CSVs, billion-row
Parquet), use the **local paginators** in the
[Streaming & Pagination guide](./streaming_and_pagination.md):

```python
from incorporator.io.pagination import SQLitePaginator, CSVPaginator

# Yields one chunk at a time, never materialises the whole table.
db_streamer = SQLitePaginator(
    db_path="warehouse.db",
    sql_query="SELECT * FROM trades",
    chunk_size=10_000,
)
async for wave in Trade.stream(
    incorp_params={"inc_url": "local_warehouse", "inc_page": db_streamer},
    export_params={"file_path": "data/trades_export.parquet"},
):
    print(f"Streamed {wave.rows_processed} rows in chunk {wave.chunk_index}")
```

Memory stays flat regardless of total row count.

---

## See Also

* **[Formats & Compression Cheat Sheet](./formats_and_compression.md)** —
  every kwarg per format, plus compression details.
* **[Streaming & Pagination](./streaming_and_pagination.md)** — the
  paginator family for files too big to fit in RAM.
* **[Tutorial 3 — DX Inspector](./3_dx_inspector.md)** — let `test()`
  print the kwargs you need against an unknown file or endpoint.
* **[Library reference](./library_reference.md)** — full `incorp()`
  and `export()` signatures.
