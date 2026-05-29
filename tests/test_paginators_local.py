"""Unit tests for the local-file paginator subclasses (SQLite, CSV, Avro)."""

import sqlite3
from pathlib import Path

import pytest

from incorporator.io.pagination import CSVPaginator, SQLitePaginator


# ==========================================
# 1. SQLitePaginator
# ==========================================


@pytest.mark.asyncio
async def test_sqlite_paginator_yields_fixed_size_chunks(tmp_path: Path) -> None:
    """SQLitePaginator must stream rows in chunk_size groups via fetchmany."""
    db_path = tmp_path / "stream.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO items (name) VALUES (?)", [(f"row{i}",) for i in range(11)])
    conn.commit()
    conn.close()

    p = SQLitePaginator(db_path=str(db_path), sql_query="SELECT * FROM items", chunk_size=4)
    chunks = [chunk async for chunk in p.paginate(start_url=str(db_path))]

    # 11 rows / chunk_size 4 → chunks of 4, 4, 3
    assert [len(c) for c in chunks] == [4, 4, 3]
    assert p.is_exhausted is True
    # Every row is a dict (DictReader-style deserialisation)
    assert all(isinstance(row, dict) for chunk in chunks for row in chunk)


@pytest.mark.asyncio
async def test_sqlite_paginator_reset_closes_connection(tmp_path: Path) -> None:
    """reset() must close the live SQLite connection."""
    db_path = tmp_path / "reset.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()

    p = SQLitePaginator(db_path=str(db_path), sql_query="SELECT * FROM t")
    # Open the connection by walking one chunk
    [chunk async for chunk in p.paginate(str(db_path))]
    assert p._conn is None or p.is_exhausted  # closed on exhaustion

    # Even if exhausted, reset must clear state
    p.reset()
    assert p._conn is None
    assert p._cursor is None
    assert p.is_exhausted is False


@pytest.mark.asyncio
async def test_sqlite_paginator_empty_table_exhausts_immediately(tmp_path: Path) -> None:
    """An empty table must produce zero chunks and mark exhausted."""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE empty_t (id INTEGER)")
    conn.commit()
    conn.close()

    p = SQLitePaginator(db_path=str(db_path), sql_query="SELECT * FROM empty_t")
    chunks = [chunk async for chunk in p.paginate(str(db_path))]
    assert chunks == []
    assert p.is_exhausted is True


# ==========================================
# 2. CSVPaginator
# ==========================================


@pytest.mark.asyncio
async def test_csv_paginator_yields_fixed_size_chunks(tmp_path: Path) -> None:
    """CSVPaginator must yield chunks of chunk_size rows from DictReader."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(
        "id,name\n" + "\n".join(f"{i},row{i}" for i in range(7)) + "\n",
        encoding="utf-8",
    )

    p = CSVPaginator(file_path=str(csv_file), chunk_size=3)
    chunks = [chunk async for chunk in p.paginate(str(csv_file))]

    # 7 rows / 3 → 3, 3, 1
    assert [len(c) for c in chunks] == [3, 3, 1]
    assert p.is_exhausted is True
    # DictReader yields dicts
    assert chunks[0][0] == {"id": "0", "name": "row0"}


@pytest.mark.asyncio
async def test_csv_paginator_tsv_via_delimiter_override(tmp_path: Path) -> None:
    """Setting delimiter='\\t' must parse TSV files correctly."""
    tsv_file = tmp_path / "data.tsv"
    tsv_file.write_text("id\tname\n1\talice\n2\tbob\n", encoding="utf-8")

    p = CSVPaginator(file_path=str(tsv_file), chunk_size=10, delimiter="\t")
    chunks = [chunk async for chunk in p.paginate(str(tsv_file))]

    # All rows fit in one chunk
    assert len(chunks) == 1
    assert chunks[0] == [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]


@pytest.mark.asyncio
async def test_csv_paginator_reset_closes_file(tmp_path: Path) -> None:
    """reset() must close the open file handle."""
    csv_file = tmp_path / "r.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")

    p = CSVPaginator(file_path=str(csv_file))
    [chunk async for chunk in p.paginate(str(csv_file))]
    assert p._file is None  # closed on exhaustion

    p.reset()
    assert p._file is None
    assert p._reader is None
    assert p.is_exhausted is False


@pytest.mark.asyncio
async def test_csv_paginator_empty_file_exhausts_with_zero_chunks(tmp_path: Path) -> None:
    """A header-only CSV must yield zero data chunks and exhaust."""
    csv_file = tmp_path / "header_only.csv"
    csv_file.write_text("col_a,col_b\n", encoding="utf-8")  # header but no rows

    p = CSVPaginator(file_path=str(csv_file))
    chunks = [chunk async for chunk in p.paginate(str(csv_file))]
    assert chunks == []
    assert p.is_exhausted is True


# ==========================================
# 3. AvroPaginator (gated on optional fastavro extra)
# ==========================================


@pytest.mark.asyncio
async def test_avro_paginator_reads_blocks(tmp_path: Path) -> None:
    """AvroPaginator must yield chunked records from a real Avro file (requires fastavro)."""
    fastavro = pytest.importorskip("fastavro")
    from incorporator.io.pagination import AvroPaginator

    avro_path = tmp_path / "data.avro"
    schema = {
        "type": "record",
        "name": "Item",
        "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
    }
    records = [{"id": i, "name": f"row{i}"} for i in range(5)]
    with open(avro_path, "wb") as fh:
        fastavro.writer(fh, schema, records)

    p = AvroPaginator(file_path=str(avro_path), chunk_size=2)
    chunks = [chunk async for chunk in p.paginate(str(avro_path))]

    # 5 rows / 2 → 2, 2, 1
    assert [len(c) for c in chunks] == [2, 2, 1]
    assert p.is_exhausted is True
    assert chunks[0][0]["name"] == "row0"


# ==========================================
# 4. Regression: truncated mid-row CSV
# ==========================================


@pytest.mark.asyncio
async def test_csv_paginator_truncated_midrow(tmp_path: Path) -> None:
    """A CSV that ends mid-row must yield clean chunks + the truncated last row,
    NOT raise and NOT corrupt the file handle.

    Real-world failure: a partially-uploaded CSV (interrupted download, killed
    rsync, etc.) lands on disk with the final row missing one or more columns.
    The paginator should:
      * yield the cleanly-terminated rows as a normal chunk,
      * yield the truncated final row with the missing columns absent (or None),
      * exhaust without raising,
      * close the underlying file handle on exhaustion.
    """
    csv_path = tmp_path / "truncated.csv"
    # Three full rows + one row missing the last column (no trailing newline).
    csv_path.write_text(
        "id,name,score\n1,alice,90\n2,bob,80\n3,carol,70\n4,dave",  # missing "score" column and trailing newline
        encoding="utf-8",
    )

    p = CSVPaginator(file_path=str(csv_path), chunk_size=2)
    chunks = [chunk async for chunk in p.paginate(str(csv_path))]

    # 4 rows in 2-row chunks → [2, 2]
    assert [len(c) for c in chunks] == [2, 2]
    assert p.is_exhausted is True

    # The truncated row is yielded with the missing column absent or None — the
    # important contract is "no exception, no half-corrupt chunk".  csv.DictReader
    # supplies None for missing trailing fields in 3.13.
    last_row = chunks[-1][-1]
    assert last_row["id"] == "4"
    assert last_row["name"] == "dave"
    assert last_row.get("score") in (None, "")

    # Underlying file handle must be released after exhaustion so daemon-mode
    # polling (`reset()` then re-paginate) can re-open cleanly.
    assert p._file is None


@pytest.mark.asyncio
async def test_csv_paginator_reset_reopens_cleanly(tmp_path: Path) -> None:
    """After reset(), the paginator must re-open the file from the top."""
    csv_path = tmp_path / "small.csv"
    csv_path.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")

    p = CSVPaginator(file_path=str(csv_path), chunk_size=10)
    chunks1 = [chunk async for chunk in p.paginate(str(csv_path))]
    assert sum(len(c) for c in chunks1) == 3

    p.reset()
    chunks2 = [chunk async for chunk in p.paginate(str(csv_path))]
    assert sum(len(c) for c in chunks2) == 3, "reset() did not re-open at the start of the file"
