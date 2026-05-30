"""Pytest fixtures and mock data for Incorporator testing."""

import json
import os
import sys
from pathlib import Path
from typing import Generator
import pytest


# --- ENVIRONMENT BOOTSTRAP (must run before pyarrow.orc imports its tz data) ---

if sys.platform == "win32" and not os.environ.get("TZDIR"):
    # PyArrow's ORC reader hardcodes /usr/share/zoneinfo lookups regardless of
    # OS.  On Windows this path doesn't exist, so we point TZDIR at the IANA
    # data shipped by the `tzdata` PyPI package (installed via the [parquet]
    # extra on Windows).  Skip silently if tzdata isn't importable — ORC tests
    # will then fall back to pytest-skip via the marker below.
    try:
        import tzdata  # type: ignore[import-not-found]

        _tz_root = Path(tzdata.__file__).parent / "zoneinfo"
        if _tz_root.is_dir():
            os.environ["TZDIR"] = str(_tz_root)
    except ImportError:
        pass


# --- LOGGER STATE FIXTURES ---


@pytest.fixture
def reset_active_listeners() -> Generator[None, None, None]:
    """Isolate _ACTIVE_LISTENERS state across tests.

    Snapshots which background QueueListener threads are registered at entry,
    then on teardown stops and removes any listener added during the test.
    Tests that exercise ``setup_class_logger()`` should depend on this fixture
    so a leaked listener never affects an unrelated subsequent test.
    """
    from incorporator.observability.logger import _ACTIVE_LISTENERS

    snapshot = set(_ACTIVE_LISTENERS.keys())
    try:
        yield
    finally:
        for key in list(_ACTIVE_LISTENERS.keys()):
            if key not in snapshot:
                listener = _ACTIVE_LISTENERS[key]
                # Python 3.11 QueueListener.stop() raises AttributeError if
                # _thread is None (already-stopped or never-started); guard so
                # one bad listener doesn't abort cleanup of the rest.
                if getattr(listener, "_thread", None) is not None:
                    try:
                        listener.stop()
                    except Exception:
                        pass
                del _ACTIVE_LISTENERS[key]


# --- JSON FIXTURES ---


@pytest.fixture
def clean_json_file(tmp_path: Path) -> str:
    """Creates a temporary valid JSON file on the disk and returns its path."""
    payload = [
        {"id": 1, "name": "Bulbasaur", "weight": 69},
        {"id": 2, "name": "Ivysaur", "weight": 130},
    ]
    file_path = tmp_path / "clean_data.json"
    file_path.write_text(json.dumps(payload), encoding="utf-8")
    return str(file_path)


@pytest.fixture
def broken_json_file(tmp_path: Path) -> str:
    """Creates a temporary malformed JSON file on the disk."""
    payload = '{"id": 1, "name": "Missing Quotes}'  # Intentionally broken
    file_path = tmp_path / "broken_data.json"
    file_path.write_text(payload, encoding="utf-8")
    return str(file_path)


# --- CSV FIXTURES ---


@pytest.fixture
def csv_users_payload() -> str:
    """Provides a standardized CSV string for testing type conversions."""
    return "id,username,is_active,account_balance\n101,alice_smith,true,1500.50\n102,bob_jones,false,0.00\n"


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


@pytest.fixture
def mock_no_speedups(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """
    Forces Incorporator to use the Python Standard Library fallbacks
    by setting the _deps module constants to None.
    """
    from incorporator._deps import cramjam as _cramjam_mod
    from incorporator._deps import fastavro as _fastavro_mod
    from incorporator._deps import lxml as _lxml_mod
    from incorporator._deps import orjson as _orjson_mod

    monkeypatch.setattr(_orjson_mod, "ORJSON", None)
    monkeypatch.setattr(_lxml_mod, "LXML_ETREE", None)
    monkeypatch.setattr(_cramjam_mod, "CRAMJAM", None)
    monkeypatch.setattr(_fastavro_mod, "FASTAVRO", None)
    yield
