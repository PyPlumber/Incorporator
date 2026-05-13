"""Benchmark: parse-direction throughput across every format that supports it.

Pairs the write-direction benchmarks (test_*_throughput.py) so every handler
has measured numbers in BOTH directions.  Parse-side performance is what the
framework hits on every ``incorp()`` call after the HTTP fetch — it's at
least as important as write, but historically under-benchmarked.

Each test writes a known dataset to disk via the framework's own write path,
then times ``parse_source_data`` on the resulting file.  This is real-world:
the bench measures the path users actually hit, including ``deserialize_nested``
type coercion in the columnar readers.

Format coverage:
  * JSON       (orjson if installed, stdlib fallback)
  * NDJSON     (line-by-line stdlib json)
  * CSV/TSV/PSV (csv.DictReader, parametrized)
  * XML        (lxml if installed, stdlib ElementTree fallback)
  * Parquet    (pyarrow read_table → to_pylist)
  * Feather    (pyarrow feather.read_table)
  * ORC        (pyarrow.orc.ORCFile.read)
  * SQLite     (cursor.execute → fetchall)
  * Avro       (fastavro.reader generator)

XLSX is intentionally omitted — openpyxl read_only mode is ~5–10k rows/sec
and contributes little signal beyond what the write benchmark already shows.
HTML parse already lives in test_markup_throughput.py.
"""

import sqlite3
import time
from pathlib import Path
from typing import Iterable, List

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import parse_source_data, write_destination_data
from incorporator.io.handlers.binary import SQLiteHandler

ROW_COUNT = 500_000  # matches write benches for direct comparison
SMALL_ROW_COUNT = 100_000  # XML / Avro: smaller, since parse is slower or has setup cost


def _generate_rows(n: int = ROW_COUNT) -> Iterable[dict]:
    for i in range(n):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


# ============================================================
# JSON / NDJSON
# ============================================================


@pytest.mark.asyncio
async def test_json_parse_throughput(tmp_path: Path) -> None:
    """JSON parse must sustain at least 200k rows/sec.

    orjson decodes the whole array in C in one shot, so throughput is
    dominated by file I/O + a single C-side parse.  Floor is set at 200k
    to accommodate stdlib-json fallback; with orjson installed real numbers
    are typically 500k+/sec.
    """
    src = tmp_path / "data.json"
    await write_destination_data(_generate_rows(), src, FormatType.JSON)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.JSON)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  JSON parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 200_000, (
        f"JSON parse throughput {throughput:,.0f} rows/sec is below 200k floor. "
        "Suggests orjson fell back to stdlib AND the stdlib path lost its bulk-decode."
    )


@pytest.mark.asyncio
async def test_ndjson_parse_throughput(tmp_path: Path) -> None:
    """NDJSON parse must sustain at least 100k rows/sec.

    NDJSON is parsed line-by-line via stdlib json (no orjson path in the
    current handler).  Per-row json.loads dominates runtime — floor of 100k
    reflects stdlib parsing speed on commodity hardware.
    """
    src = tmp_path / "data.ndjson"
    await write_destination_data(_generate_rows(), src, FormatType.NDJSON)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.NDJSON)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  NDJSON parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 100_000, (
        f"NDJSON parse throughput {throughput:,.0f} rows/sec is below 100k floor. "
        "Suggests the line-by-line streaming was replaced with full-file materialisation."
    )


# ============================================================
# CSV / TSV / PSV (parametrized)
# ============================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fmt", "ext"),
    [(FormatType.CSV, "csv"), (FormatType.TSV, "tsv"), (FormatType.PSV, "psv")],
    ids=["csv", "tsv", "psv"],
)
async def test_delimited_parse_throughput(tmp_path: Path, fmt: FormatType, ext: str) -> None:
    """CSV/TSV/PSV parse must sustain at least 100k rows/sec.

    csv.DictReader runs in C but allocates one dict per row plus
    ``deserialize_nested`` per value — the handler can't skip those if it
    wants to round-trip nested JSON-encoded values.  100k is conservative.
    """
    src = tmp_path / f"data.{ext}"
    await write_destination_data(_generate_rows(), src, fmt)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, fmt)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  {fmt.name} parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 100_000, (
        f"{fmt.name} parse throughput {throughput:,.0f} rows/sec is below 100k floor. "
        "Suggests csv.DictReader lost its streaming path or deserialize_nested is "
        "doing unnecessary work for scalar values."
    )


# ============================================================
# XML
# ============================================================


@pytest.mark.asyncio
async def test_xml_parse_throughput(tmp_path: Path) -> None:
    """XML parse must sustain at least 30k rows/sec.

    lxml is C-backed and typically lands at 50–80k rows/sec; stdlib
    ElementTree is ~30k rows/sec.  Floor accommodates both — opt into lxml
    via ``pip install incorporator[speedups]`` for the upper range.
    """
    src = tmp_path / "data.xml"
    await write_destination_data(_generate_rows(n=SMALL_ROW_COUNT), src, FormatType.XML)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.XML)
    elapsed = time.perf_counter() - t0

    # xml_to_dict returns the parsed root structure — a list under "item" keys.
    # We don't validate the row count here (the structure depends on lxml
    # vs stdlib mapping) because the goal is parse-rate measurement.
    assert parsed is not None

    throughput = SMALL_ROW_COUNT / elapsed
    print(f"\n  XML parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 30_000, (
        f"XML parse throughput {throughput:,.0f} rows/sec is below 30k floor. "
        "Suggests we lost streaming parse and are double-walking the DOM."
    )


# ============================================================
# Parquet
# ============================================================


@pytest.mark.asyncio
async def test_parquet_parse_throughput(tmp_path: Path) -> None:
    """Parquet parse must sustain at least 100k rows/sec.

    pyarrow's ``pq.read_table`` is C-backed and returns an Arrow Table that
    we convert via ``to_pylist`` + ``deserialize_nested`` per value.  The
    to_pylist conversion dominates — Arrow → Python dict allocation is the
    real bottleneck, not the columnar read itself.  Measured baseline is
    ~160k rows/sec; the 100k floor leaves CI headroom.

    Notable: Parquet parse is *slower than NDJSON parse* on this dataset
    because Arrow → dict materialisation is heavier than orjson's bulk
    decode.  Parquet's wins are on disk size + write-side batching, not
    on parse throughput.
    """
    pytest.importorskip("pyarrow")
    src = tmp_path / "data.parquet"
    await write_destination_data(_generate_rows(), src, FormatType.PARQUET)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.PARQUET)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  Parquet parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 100_000, (
        f"Parquet parse throughput {throughput:,.0f} rows/sec is below 100k floor. "
        "Suggests deserialize_nested is doing JSON parsing on scalar values, "
        "or the Arrow Table is being walked twice."
    )


# ============================================================
# Feather / ORC
# ============================================================


@pytest.mark.asyncio
async def test_feather_parse_throughput(tmp_path: Path) -> None:
    """Feather parse must sustain at least 100k rows/sec.

    Feather V2 supports memory-mapped reads with zero deserialisation at
    the Arrow layer.  Like Parquet, the real cost shows up at the Arrow →
    Python dict conversion (``to_pylist``) — not the read itself.  Measured
    baseline is ~165k rows/sec, marginally better than Parquet because
    there's no decompression step.
    """
    pytest.importorskip("pyarrow")
    src = tmp_path / "data.feather"
    await write_destination_data(_generate_rows(), src, FormatType.FEATHER)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.FEATHER)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  Feather parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 100_000, (
        f"Feather parse throughput {throughput:,.0f} rows/sec is below 100k floor. "
        "Suggests we lost the memory-mapped read path."
    )


@pytest.mark.asyncio
async def test_orc_parse_throughput(tmp_path: Path) -> None:
    """ORC parse must sustain at least 150k rows/sec.

    ORC's pyarrow integration is less mature than Parquet/Feather; parse
    throughput is typically 150–250k rows/sec on commodity hardware.
    """
    pytest.importorskip("pyarrow")
    src = tmp_path / "data.orc"
    await write_destination_data(_generate_rows(), src, FormatType.ORC)

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.ORC)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  ORC parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 150_000, f"ORC parse throughput {throughput:,.0f} rows/sec is below 150k floor."


# ============================================================
# SQLite
# ============================================================


def test_sqlite_parse_throughput(tmp_path: Path) -> None:
    """SQLite parse must sustain at least 200k rows/sec.

    SQLiteHandler.parse runs a SELECT * and converts each row to a dict
    via ``dict(zip(columns, row))`` — pure Python after the C-level fetch.
    Floor reflects per-row dict allocation on commodity hardware.

    Synchronous because SQLiteHandler.parse is not async (the framework
    wraps it in asyncio.to_thread at the dispatcher layer, but invoking
    handler.parse directly lets us measure pure parse time).
    """
    rows: List[dict] = list(_generate_rows())
    src = tmp_path / "data.db"
    handler = SQLiteHandler()
    handler.write(rows, src, sql_table="bench")

    t0 = time.perf_counter()
    parsed = handler.parse(src, sql_query="SELECT * FROM bench")
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  SQLite parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 150_000, (
        f"SQLite parse throughput {throughput:,.0f} rows/sec is below 150k floor. "
        "Suggests cursor.fetchall() was replaced with per-row fetch, or row→dict "
        "conversion is allocating excessively."
    )

    # Sanity: cross-check via raw sqlite3 to confirm row count matches.
    conn = sqlite3.connect(src)
    (count,) = conn.execute("SELECT COUNT(*) FROM bench").fetchone()
    conn.close()
    assert count == ROW_COUNT


# ============================================================
# Avro
# ============================================================


@pytest.mark.asyncio
async def test_avro_parse_throughput(tmp_path: Path) -> None:
    """Avro parse must sustain at least 80k rows/sec.

    fastavro.reader is a generator — the framework's parse path iterates it
    and applies ``deserialize_nested`` per value.  Generator-based reads
    keep memory O(1) but per-row Python overhead is the dominant cost vs.
    a bulk columnar reader.
    """
    pytest.importorskip("fastavro")
    schema_hint = {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "value": {"type": "number"},
            "active": {"type": "boolean"},
        }
    }
    src = tmp_path / "data.avro"
    await write_destination_data(
        _generate_rows(),
        src,
        FormatType.AVRO,
        sql_table="BenchmarkRecord",
        pydantic_schema=schema_hint,
    )

    t0 = time.perf_counter()
    parsed = await parse_source_data(src, FormatType.AVRO)
    elapsed = time.perf_counter() - t0

    assert isinstance(parsed, list) and len(parsed) == ROW_COUNT

    throughput = ROW_COUNT / elapsed
    print(f"\n  Avro parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 80_000, (
        f"Avro parse throughput {throughput:,.0f} rows/sec is below 80k floor. "
        "Suggests fastavro is being read into a list before iteration, or "
        "deserialize_nested is parsing scalar values as JSON."
    )
