"""Unit tests for text-format handlers: JSON, NDJSON, XML write/parse round-trips."""

import json
from pathlib import Path

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.handlers.text import JSONHandler, NDJSONHandler, XMLHandler


# ==========================================
# 0. JSONHandler — stdlib fallback (no orjson)
# ==========================================


def test_json_parse_stdlib_fallback(mock_no_speedups) -> None:
    """JSONHandler.parse must succeed through the stdlib json path when orjson absent."""
    raw = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
    result = JSONHandler().parse(raw)
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


def test_json_write_stdlib_fallback(mock_no_speedups, tmp_path: Path) -> None:
    """JSONHandler.write must produce a valid JSON array via stdlib json when orjson absent."""
    import json

    rows = [{"id": i, "val": i * 10} for i in range(3)]
    out = tmp_path / "stdlib.json"
    JSONHandler().write(iter(rows), out)

    parsed = json.loads(out.read_text(encoding="utf-8"))
    assert len(parsed) == 3
    assert parsed[2]["val"] == 20


def test_xml_parse_unrecoverable_stdlib_raises(mock_no_speedups) -> None:
    """Stdlib path: an XML string that fails even after .strip() must raise IncorporatorFormatError."""
    not_xml = "this is just plain text, not XML at all <<<>>>"
    with pytest.raises(IncorporatorFormatError, match="Invalid XML"):
        XMLHandler().parse(not_xml)


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


def test_xml_parse_stdlib_fallback(mock_no_speedups) -> None:
    """XMLHandler.parse must succeed on safe XML when lxml is absent (stdlib happy path)."""
    safe_xml = "<root><item><name>Alice</name><age>30</age></item></root>"
    result = XMLHandler().parse(safe_xml)
    text = str(result)
    assert "Alice" in text
    assert "30" in text


def test_xml_parse_recovery_from_whitespace(mock_no_speedups, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stdlib recovery branch: ET.ParseError on first fromstring triggers .strip() retry."""
    import xml.etree.ElementTree as ET

    original_fromstring = ET.fromstring
    call_count = 0

    def patched_fromstring(text: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ET.ParseError("synthetic whitespace-preamble error")
        # Second call receives the stripped string and must succeed
        return original_fromstring(text)

    monkeypatch.setattr("xml.etree.ElementTree.fromstring", patched_fromstring)

    result = XMLHandler().parse("<root><item><name>recovered</name></item></root>")
    assert call_count == 2, "Recovery branch must issue exactly two fromstring() calls"
    assert "recovered" in str(result)



# ==========================================
# Auto-mkdir on export (handler dispatcher behaviour)
# ==========================================


import pytest as _pytest


@_pytest.mark.asyncio
async def test_write_destination_data_creates_missing_parent_dir(tmp_path: Path) -> None:
    """Streaming pipelines target paths like ``data/foo.ndjson``; the dispatcher
    must create the missing parent dir rather than failing every export tick.

    Pre-fix: the example tutorial 6 failed at first export with
        NDJSON File IO Error: [Errno 2] No such file or directory: 'data/...'
    because the user never had a chance to mkdir before the daemon started.
    """
    from incorporator.io.formats import FormatType
    from incorporator.io.handlers import write_destination_data

    # nested non-existent path — three levels deep to confirm parents=True
    target = tmp_path / "a" / "b" / "c" / "out.ndjson"
    assert not target.parent.exists()

    await write_destination_data(
        iter([{"id": "X", "v": 1}, {"id": "Y", "v": 2}]),
        str(target),
        FormatType.NDJSON,
    )

    assert target.exists()
    assert target.parent.is_dir()
    text = target.read_text(encoding="utf-8")
    assert '"id": "X"' in text
    assert '"id": "Y"' in text
