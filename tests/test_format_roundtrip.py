"""Phase 3 — data-correctness round-trip regressions.

The senior-review audit found round-trip mismatches that surface as
silent data loss when a Pydantic model is exported and re-imported.
These tests lock the new contract:

  * CSV empty cells → None (was: empty string sentinel by default).
  * SQLite bool round-trip via sql_bool_columns kwarg (was: int only).
  * xml_to_dict force_list kwarg for stable list shapes across docs.

Phase 3 also added docstring warnings for Feather/ORC RAM usage, but
that's a fundamental pyarrow API constraint — not testable as a fix.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from incorporator.io.formats import xml_to_dict
from incorporator.io.handlers.binary import SQLiteHandler
from incorporator.io.handlers.delimited import CSVHandler


# ==========================================
# 3c — CSV empty cell → None
# ==========================================


def test_csv_empty_cells_become_none_by_default(tmp_path: Path) -> None:
    """Default behaviour: empty cells parse to None, matching Pydantic Optional[T]."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name,note\n1,Alice,\n2,,absent name\n", encoding="utf-8")

    rows = CSVHandler().parse(csv_path)
    assert isinstance(rows, list)
    assert rows[0]["note"] is None                  # blank trailing cell
    assert rows[1]["name"] is None                  # blank middle cell
    assert rows[1]["note"] == "absent name"         # non-blank passes through


def test_csv_empty_cells_opt_out_keeps_empty_string(tmp_path: Path) -> None:
    """csv_empty_as_none=False preserves the empty-string sentinel."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,\n", encoding="utf-8")

    rows = CSVHandler().parse(csv_path, csv_empty_as_none=False)
    assert isinstance(rows, list)
    assert rows[0]["name"] == ""                    # explicit empty string


# ==========================================
# 3a — SQLite bool round-trip
# ==========================================


def test_sqlite_bool_round_trip_via_kwarg(tmp_path: Path) -> None:
    """sql_bool_columns coerces 0/1 ints back to Python bool on read."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE flags (id INTEGER, active INTEGER, archived INTEGER)")
        conn.executemany(
            "INSERT INTO flags VALUES (?, ?, ?)",
            [(1, 1, 0), (2, 0, 1), (3, 1, 1)],
        )
        conn.commit()
    finally:
        conn.close()

    rows = SQLiteHandler().parse(
        db_path,
        sql_query="SELECT * FROM flags",
        sql_bool_columns=["active", "archived"],
    )
    assert isinstance(rows, list)
    assert rows[0]["active"] is True
    assert rows[0]["archived"] is False
    assert rows[1]["active"] is False
    assert rows[1]["archived"] is True
    # `id` not in sql_bool_columns → stays int
    assert rows[0]["id"] == 1
    assert isinstance(rows[0]["id"], int)


def test_sqlite_without_bool_kwarg_returns_ints(tmp_path: Path) -> None:
    """Without sql_bool_columns, 0/1 stay as ints — documented behaviour."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE flags (id INTEGER, active INTEGER)")
        conn.execute("INSERT INTO flags VALUES (1, 1)")
        conn.commit()
    finally:
        conn.close()

    rows = SQLiteHandler().parse(db_path, sql_query="SELECT * FROM flags")
    assert isinstance(rows, list)
    assert rows[0]["active"] == 1
    assert isinstance(rows[0]["active"], int)       # NOT bool


# ==========================================
# 3b — xml_to_dict force_list shape consistency
# ==========================================


def test_xml_to_dict_force_list_wraps_single_child() -> None:
    """force_list={'item'} ensures single-item docs come back as a list."""
    xml = "<root><item><name>only</name></item></root>"
    result = xml_to_dict(ET.fromstring(xml), force_list={"item"})
    assert isinstance(result["root"]["item"], list)
    assert len(result["root"]["item"]) == 1
    assert result["root"]["item"][0] == {"name": "only"}


def test_xml_to_dict_force_list_preserves_multi_child_list() -> None:
    """Multi-sibling case still produces a list (the existing behaviour)."""
    xml = "<root><item><name>a</name></item><item><name>b</name></item></root>"
    result = xml_to_dict(ET.fromstring(xml), force_list={"item"})
    assert isinstance(result["root"]["item"], list)
    assert len(result["root"]["item"]) == 2


def test_xml_to_dict_without_force_list_collapses_single() -> None:
    """Default behaviour: single child collapses to a dict (the docs note)."""
    xml = "<root><item><name>only</name></item></root>"
    result = xml_to_dict(ET.fromstring(xml))
    # Single child → dict, not list
    assert isinstance(result["root"]["item"], dict)


def test_xml_to_dict_force_list_only_affects_named_tags() -> None:
    """force_list={'item'} doesn't wrap other tags."""
    xml = "<root><item>a</item><other>b</other></root>"
    result = xml_to_dict(ET.fromstring(xml), force_list={"item"})
    assert isinstance(result["root"]["item"], list)
    assert result["root"]["other"] == "b"           # other stays scalar
