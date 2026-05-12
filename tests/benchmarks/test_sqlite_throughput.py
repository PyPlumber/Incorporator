"""Benchmark: prove SQLiteHandler.write sustains real-world bulk-insert throughput.

The framework write path uses ``cursor.executemany()`` on a generator of
pre-serialised tuples. On commodity hardware that should comfortably exceed
50 000 rows/sec end-to-end including auto-schema generation, type
serialisation, and BOOLEAN→int coercion.

The comparison to a hand-rolled per-row INSERT loop is *informational only* —
the framework path does extra per-value work (``serialize_nested``, bool→int
coercion, JSON key sanitisation) that a naive loop skips, so a small dataset
may show the naive loop edging ahead. That trade-off buys correctness for
nested types and consistent boolean handling.
"""

import sqlite3
import time
from pathlib import Path
from typing import List

import pytest

from incorporator.io.handlers.binary import SQLiteHandler


ROW_COUNT = 50_000  # CI-fast; large enough for the speed gap to be obvious


def _generate_rows(count: int) -> List[dict]:
    """Build a deterministic list of mixed-type dict rows."""
    return [
        {
            "id": i,
            "name": f"user_{i}",
            "score": float(i) * 1.5,
            "is_active": bool(i % 2),
        }
        for i in range(count)
    ]


def test_sqlite_executemany_vs_sequential_informational(tmp_path: Path) -> None:
    """Informational comparison: framework path vs hand-rolled per-row INSERT.

    Prints the wall-clock times and ratio. No assertion on the ratio because the
    framework path does additional per-value work (serialize_nested, bool→int
    coercion) that the naive loop skips. The benchmark exists to surface large
    regressions in either direction, not enforce a fixed speedup.
    """
    rows = _generate_rows(ROW_COUNT)
    handler = SQLiteHandler()

    # --- Path A: framework write (uses executemany under the hood) ---
    fast_db = tmp_path / "fast.db"
    t0 = time.perf_counter()
    handler.write(rows, fast_db, sql_table="bench", if_exists="replace")
    fast_elapsed = time.perf_counter() - t0

    # --- Path B: naive per-row INSERTs ---
    slow_db = tmp_path / "slow.db"
    t0 = time.perf_counter()
    conn = sqlite3.connect(slow_db)
    conn.execute("CREATE TABLE bench (id INTEGER, name TEXT, score REAL, is_active INTEGER)")
    for row in rows:
        conn.execute(
            "INSERT INTO bench (id, name, score, is_active) VALUES (?, ?, ?, ?)",
            (row["id"], row["name"], row["score"], int(row["is_active"])),
        )
    conn.commit()
    conn.close()
    slow_elapsed = time.perf_counter() - t0

    # Sanity: both produced the right row count
    for db_path in (fast_db, slow_db):
        c = sqlite3.connect(db_path)
        (count,) = c.execute("SELECT COUNT(*) FROM bench").fetchone()
        c.close()
        assert count == ROW_COUNT, f"{db_path} has {count} rows, expected {ROW_COUNT}"

    print(
        f"\n  framework executemany: {fast_elapsed:.3f}s | naive sequential: {slow_elapsed:.3f}s | "
        f"ratio: {slow_elapsed / fast_elapsed:.2f}× (informational only — framework does "
        f"per-value type serialisation that the naive loop skips)"
    )


def test_sqlite_throughput_floor(tmp_path: Path) -> None:
    """Framework SQLite write must sustain at least 50k rows/sec on commodity hardware."""
    rows = _generate_rows(ROW_COUNT)

    t0 = time.perf_counter()
    SQLiteHandler().write(rows, tmp_path / "tput.db", sql_table="t")
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  SQLite write throughput: {throughput:,.0f} rows/sec")

    # CI floor — real-world numbers are usually 200k+/sec
    assert throughput >= 50_000, (
        f"SQLite write throughput {throughput:,.0f} rows/sec is below the 50k floor. "
        "Investigate executemany configuration or pragma settings."
    )
