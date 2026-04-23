"""Pytest fixtures and mock data for Incorporator testing."""

import json
from pathlib import Path

import pytest

# --- JSON FIXTURES ---

@pytest.fixture
def clean_json_file(tmp_path: Path) -> str:
    """Creates a temporary valid JSON file on the disk and returns its path."""
    payload =[
        {"id": 1, "name": "Bulbasaur", "weight": 69},
        {"id": 2, "name": "Ivysaur", "weight": 130}
    ]
    file_path = tmp_path / "clean_data.json"
    file_path.write_text(json.dumps(payload), encoding='utf-8')
    return str(file_path)


@pytest.fixture
def broken_json_file(tmp_path: Path) -> str:
    """Creates a temporary malformed JSON file on the disk."""
    payload = '{"id": 1, "name": "Missing Quotes}' # Intentionally broken
    file_path = tmp_path / "broken_data.json"
    file_path.write_text(payload, encoding='utf-8')
    return str(file_path)

# --- CSV FIXTURES ---

@pytest.fixture
def csv_users_payload() -> str:
    """Provides a standardized CSV string for testing type conversions."""
    return (
        "id,username,is_active,account_balance\n"
        "101,alice_smith,true,1500.50\n"
        "102,bob_jones,false,0.00\n"
    )

# --- XML FIXTURES ---

@pytest.fixture
def xml_catalog_payload() -> str:
    """Provides a nested XML string for testing rPath and node extraction."""
    return (
        "<?xml version='1.0'?>\n"
        "<catalog>\n"
        "   <metadata>\n"
        "       <updated>2026-04-20</updated>\n"
        "   </metadata>\n"
        "   <book id='bk101'>\n"
        "       <author>Gambardella, Matthew</author>\n"
        "       <title>XML Developer's Guide</title>\n"
        "       <price>44.95</price>\n"
        "   </book>\n"
        "   <book id='bk102'>\n"
        "       <author>Ralls, Kim</author>\n"
        "       <title>Midnight Rain</title>\n"
        "       <price>5.95</price>\n"
        "   </book>\n"
        "</catalog>"
    )