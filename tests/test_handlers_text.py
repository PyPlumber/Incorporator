"""Unit tests for text-format handlers: JSON, NDJSON, XML write/parse round-trips."""

import json
from pathlib import Path

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.handlers.text import JSONHandler, NDJSONHandler, XMLHandler


# ==========================================
# 1. JSONHandler — streaming write
# ==========================================


def test_json_write_streaming_round_trip(tmp_path: Path) -> None:
    """JSONHandler.write must emit a valid JSON array from a lazy iterable."""

    def generator():
        for i in range(3):
            yield {"id": i, "name": f"row{i}"}

    out = tmp_path / "out.json"
    JSONHandler().write(generator(), out)

    parsed = json.loads(out.read_text(encoding="utf-8"))
    assert isinstance(parsed, list)
    assert len(parsed) == 3
    assert parsed[0] == {"id": 0, "name": "row0"}
    assert parsed[2] == {"id": 2, "name": "row2"}


def test_json_write_rejects_append_mode(tmp_path: Path) -> None:
    """JSON arrays cannot be appended structurally — handler must raise."""
    with pytest.raises(IncorporatorFormatError, match="(?i)append"):
        JSONHandler().write([{"id": 1}], tmp_path / "out.json", if_exists="append")


# ==========================================
# 2. NDJSONHandler — append + malformed
# ==========================================


def test_ndjson_write_and_parse_round_trip(tmp_path: Path) -> None:
    """NDJSON write produces one JSON object per line; parse reverses it."""
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    path = tmp_path / "out.ndjson"
    NDJSONHandler().write(iter(rows), path)

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert [json.loads(line) for line in lines] == rows

    parsed = NDJSONHandler().parse(path)
    assert parsed == rows


def test_ndjson_append_mode_concatenates(tmp_path: Path) -> None:
    """if_exists='append' must extend an existing NDJSON file in-place."""
    path = tmp_path / "stream.ndjson"
    NDJSONHandler().write([{"id": 1}], path)
    NDJSONHandler().write([{"id": 2}], path, if_exists="append")

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert [json.loads(line) for line in lines] == [{"id": 1}, {"id": 2}]


def test_ndjson_parse_malformed_line_raises() -> None:
    """An invalid JSON line must surface as IncorporatorFormatError with line number."""
    bad = '{"id": 1}\nnot-json\n{"id": 2}\n'
    with pytest.raises(IncorporatorFormatError, match="line 2"):
        NDJSONHandler().parse(bad)


def test_ndjson_parse_skips_blank_lines() -> None:
    """Empty lines in the NDJSON stream must be silently skipped."""
    src = '{"id": 1}\n\n\n{"id": 2}\n'
    parsed = NDJSONHandler().parse(src)
    assert parsed == [{"id": 1}, {"id": 2}]


# ==========================================
# 3. XMLHandler — write key sanitisation
# ==========================================


def test_xml_write_sanitises_keys_with_spaces_and_digits(tmp_path: Path) -> None:
    """Field names with spaces and digit-prefixes must be transformed into valid XML tag names."""
    rows = [
        {
            "id 123": "abc",  # space → underscore
            "123_year": 2026,  # digit-prefix → leading underscore
            "name": "Alice",  # untouched
        }
    ]
    out = tmp_path / "out.xml"
    XMLHandler().write(iter(rows), out)

    content = out.read_text(encoding="utf-8")
    # Space-only sanitisation: "id 123" → "id_123" (no leading underscore — starts with letter)
    assert "<id_123>" in content
    # Digit-prefix sanitisation: "123_year" → "_123_year"
    assert "<_123_year>" in content
    assert "<name>Alice</name>" in content
    # Raw forms must NOT appear — XML rejects them
    assert "<id 123>" not in content
    assert "<123_year>" not in content


def test_xml_round_trip_writes_then_parses(tmp_path: Path) -> None:
    """XMLHandler write → parse must preserve simple field values."""
    rows = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    out = tmp_path / "round.xml"
    XMLHandler().write(iter(rows), out)

    parsed = XMLHandler().parse(out)
    # The root structure depends on lxml vs stdlib but values must survive
    assert "Alice" in str(parsed)
    assert "Bob" in str(parsed)


def test_xml_write_rejects_append_mode(tmp_path: Path) -> None:
    """XML cannot be appended structurally — handler must raise."""
    with pytest.raises(IncorporatorFormatError, match="(?i)append"):
        XMLHandler().write([{"id": 1}], tmp_path / "x.xml", if_exists="append")


def test_xml_parse_xxe_blocked_via_stdlib_fallback(mock_no_speedups, tmp_path: Path) -> None:
    """The stdlib XML path must invoke check_xml_security() and block DOCTYPE."""
    evil = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        "<root><item><name>&xxe;</name></item></root>"
    )
    with pytest.raises(IncorporatorFormatError, match="Security Policy Violation"):
        XMLHandler().parse(evil)
