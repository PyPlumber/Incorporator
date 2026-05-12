import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.handlers.binary import AvroHandler, SQLiteHandler

# 🛡️ DUMMY DATA: Features missing keys, nulls, booleans, and deeply nested graphs!
DUMMY_DATA: List[Dict[str, Any]] = [
    {
        "id": 1,
        "name": "Alice",
        "score": 95.5,
        "is_active": True,
        "tags": ["admin", "user"],
        "metadata": {"role": "ops", "tier": 1},
    },
    {
        "id": 2,
        "name": "Bob",
        "score": None,  # Testing Null/None handling
        "is_active": False,
        "tags": ["user"],
        "metadata": {},  # Empty dict testing
    },
]


# ==========================================
# 1. SQLITE HANDLER TESTS
# ==========================================
def test_sqlite_roundtrip_and_schema_generation(tmp_path: Path) -> None:
    """Tests writing data to an SQLite file, auto-schema generation, and reading it back."""
    db_path = tmp_path / "warehouse.db"
    handler = SQLiteHandler()

    # 1. WRITE: Auto-generates the 'users' table and bulk-inserts
    handler.write(DUMMY_DATA, db_path, sql_table="users", if_exists="replace")

    assert db_path.exists(), "SQLite database file was not created."

    # 2. READ: Queries the database and extracts the rows
    results = handler.parse(db_path, sql_query="SELECT * FROM users")

    assert len(results) == 2

    # 3. VERIFY: Ensure nested structures unflattened successfully!
    alice = results[0]
    assert alice["name"] == "Alice"
    assert alice["tags"] == ["admin", "user"]  # Successfully deserialized from JSON string!
    assert alice["metadata"]["role"] == "ops"

    # Note: SQLite stores booleans natively as 1/0. Incorporator's ETL pipeline (conv_dict)
    # handles casting this back to True/False during the actual model compilation phase.
    assert alice["is_active"] == 1

    bob = results[1]
    assert bob["score"] is None  # Nulls preserved


def test_sqlite_read_missing_query_error(tmp_path: Path) -> None:
    """Ensures a descriptive error is thrown if the user forgets the SQL query."""
    handler = SQLiteHandler()
    with pytest.raises(IncorporatorFormatError, match="requires an 'sql_query' kwarg"):
        handler.parse(tmp_path / "warehouse.db")


# ==========================================
# 2. APACHE AVRO HANDLER TESTS
# ==========================================
def test_avro_roundtrip_and_schema_generation(tmp_path: Path) -> None:
    """Tests writing to Avro, auto-generating strict schemas, and reading back binary bytes."""
    pytest.importorskip("fastavro")

    avro_path = tmp_path / "data.avro"
    handler = AvroHandler()

    # Provide the mock Pydantic Schema that the framework would normally inject
    mock_pydantic_schema = {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "score": {"anyOf": [{"type": "number"}, {"type": "null"}]},  # Testing null unions
            "is_active": {"type": "boolean"},
            "tags": {"type": "string"},  # Flattened to string
            "metadata": {"type": "string"},  # Flattened to string
        }
    }

    # 1. WRITE: Use the mock schema to generate the Avro binary format
    handler.write(DUMMY_DATA, avro_path, sql_table="UserRecord", pydantic_schema=mock_pydantic_schema)

    assert avro_path.exists(), "Avro file was not created."

    # 2. READ: Extract bytes seamlessly without a predefined schema
    results = handler.parse(avro_path)

    assert len(results) == 2

    # 3. VERIFY: Ensure Avro maintained structure and unflattened nested dicts/lists
    alice = results[0]
    assert alice["name"] == "Alice"
    assert alice["score"] == 95.5
    assert alice["is_active"] is True
    assert alice["tags"] == ["admin", "user"]  # Successfully deserialized from JSON string!

    bob = results[1]
    assert bob["score"] is None  # Null union typing succeeded


def test_avro_missing_dependency_graceful_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves the lazy-loader throws an actionable error if fastavro is missing."""
    handler = AvroHandler()

    # Force Python to pretend 'fastavro' doesn't exist in the environment
    monkeypatch.setitem(sys.modules, "fastavro", None)  # type: ignore

    # Test Write failure
    with pytest.raises(IncorporatorFormatError, match="fastavro not installed"):
        handler.write(DUMMY_DATA, tmp_path / "fail.avro")

    # Test Read failure
    with pytest.raises(IncorporatorFormatError, match="fastavro not installed"):
        handler.parse(tmp_path / "fail.avro")


# ==========================================
# 3. EDGE CASES — if_exists, schema hints
# ==========================================


def test_sqlite_write_if_exists_fail_raises(tmp_path: Path) -> None:
    """SQLiteHandler with if_exists='fail' must raise when the table already exists."""
    db_path = tmp_path / "fail.db"
    handler = SQLiteHandler()

    # First write creates the table
    handler.write(DUMMY_DATA, db_path, sql_table="users", if_exists="replace")

    # Second write with if_exists='fail' must raise
    with pytest.raises(IncorporatorFormatError):
        handler.write(DUMMY_DATA, db_path, sql_table="users", if_exists="fail")


def test_sqlite_write_append_mode_concatenates(tmp_path: Path) -> None:
    """SQLiteHandler if_exists='append' must extend an existing table rather than replacing it."""
    db_path = tmp_path / "append.db"
    handler = SQLiteHandler()

    handler.write(DUMMY_DATA, db_path, sql_table="users", if_exists="replace")
    handler.write(DUMMY_DATA, db_path, sql_table="users", if_exists="append")

    rows = handler.parse(db_path, sql_query="SELECT * FROM users")
    assert len(rows) == 4  # Original 2 + appended 2


def test_avro_complex_union_schema(tmp_path: Path) -> None:
    """AvroHandler.write must handle anyOf schemas where the first type is non-null (complex unions).

    Tests two patterns:
    - A nullable numeric field where null appears last in anyOf.
    - A field whose anyOf union lists null FIRST and the concrete type second.
    """
    pytest.importorskip("fastavro")

    avro_path = tmp_path / "complex.avro"
    handler = AvroHandler()

    # Nested anyOf: null-first (handler must skip null, pick "integer")
    # and object-type union (handler picks "object" → serialised as "string" by Avro converter)
    complex_schema = {
        "properties": {
            "id": {"type": "integer"},
            # null listed first — handler must skip it and pick the concrete type
            "count": {
                "anyOf": [
                    {"type": "null"},
                    {"type": "integer"},
                ]
            },
            # object union — tests the anyOf branch where json_type resolves to "object"
            "meta": {
                "anyOf": [
                    {"type": "object", "properties": {"key": {"type": "string"}}},
                    {"type": "null"},
                ]
            },
        }
    }

    rows = [
        {"id": 1, "count": 42, "meta": {"key": "alpha"}},
        {"id": 2, "count": None, "meta": None},
    ]

    # Must not raise regardless of how the anyOf types resolve
    handler.write(rows, avro_path, sql_table="ComplexRecord", pydantic_schema=complex_schema)
    assert avro_path.exists(), "Avro file must be created"

    parsed = handler.parse(avro_path)
    assert len(parsed) == 2
    assert parsed[0]["id"] == 1
    assert parsed[1]["id"] == 2


def test_sqlite_write_with_all_field_names_hint(tmp_path: Path) -> None:
    """Passing all_field_names explicitly must let the handler skip the full-row scan path."""
    db_path = tmp_path / "hinted.db"
    handler = SQLiteHandler()

    # Provide an explicit field-name list — this is the memory-optimised path
    field_names = ["id", "name", "score", "is_active", "tags", "metadata"]
    handler.write(DUMMY_DATA, db_path, sql_table="users", all_field_names=field_names)

    rows = handler.parse(db_path, sql_query="SELECT * FROM users")
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    # Every declared column must be present in the result
    for col in field_names:
        assert col in rows[0]
