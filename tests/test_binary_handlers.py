import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

from incorporator.methods.exceptions import IncorporatorFormatError
from incorporator.methods.format_parsers import AvroHandler, SQLiteHandler

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
