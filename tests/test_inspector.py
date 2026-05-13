"""Unit tests for the DX Inspector JIT API profiler."""

from typing import Any
import httpx
import pytest

from incorporator.exceptions import IncorporatorFormatError, IncorporatorNetworkError
from incorporator.tools.inspector import _print_tree, analyze_data, analyze_error


# ==========================================
# 1. _print_tree DEPTH + RENDERING
# ==========================================


def test_print_tree_renders_dict(capsys: pytest.CaptureFixture[str]) -> None:
    """_print_tree must emit a tree representation of nested dicts with type and value."""
    _print_tree({"name": "Alice", "age": 30, "active": True})
    captured = capsys.readouterr().out
    # Each scalar field shows its type and value
    assert "name: str = Alice" in captured
    assert "age: int = 30" in captured
    assert "active: bool = True" in captured


def test_print_tree_respects_max_depth(capsys: pytest.CaptureFixture[str]) -> None:
    """_print_tree must truncate at max_depth with an ellipsis marker."""
    deep = {"a": {"b": {"c": {"d": {"e": "leaf"}}}}}
    _print_tree(deep, max_depth=2)
    captured = capsys.readouterr().out
    # Beyond depth 2 we should see the ellipsis sentinel
    assert "..." in captured
    # The deepest leaf must NOT be rendered
    assert "leaf" not in captured


def test_print_tree_handles_nested_list(capsys: pytest.CaptureFixture[str]) -> None:
    """_print_tree must show list length and recurse into the first element."""
    _print_tree({"items": [{"id": 1, "name": "First"}, {"id": 2, "name": "Second"}]})
    captured = capsys.readouterr().out
    assert "items (list, len=2)" in captured
    # Only the first item is recursed into
    assert "id: int = 1" in captured


def test_print_tree_truncates_long_values(capsys: pytest.CaptureFixture[str]) -> None:
    """_print_tree must truncate scalar values longer than 30 chars."""
    _print_tree({"description": "a" * 100})
    captured = capsys.readouterr().out
    # Truncated form ends in ellipsis
    assert "..." in captured


# ==========================================
# 2. analyze_data — STRUCTURE + IDENTITY SUGGESTIONS
# ==========================================


def test_analyze_data_empty_payload(capsys: pytest.CaptureFixture[str]) -> None:
    """analyze_data must exit cleanly on an empty list with a clear notice."""
    analyze_data([], {})
    captured = capsys.readouterr().out
    assert "No data returned to inspect" in captured


def test_analyze_data_rec_path_suggestion(capsys: pytest.CaptureFixture[str]) -> None:
    """A root dict containing a list of dicts must trigger a rec_path suggestion."""
    # Payload shape mimics the typical {"results": [...]} pagination wrapper
    sample = {
        "page": 1,
        "results": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
    }
    analyze_data([sample], provided_kwargs={})
    captured = capsys.readouterr().out
    assert "rec_path='results'" in captured


def test_analyze_data_rec_path_skipped_when_already_provided(capsys: pytest.CaptureFixture[str]) -> None:
    """When the user already passed rec_path, the rec_path WARNING must be suppressed."""
    sample = {"results": [{"id": 1, "name": "Alice"}]}
    analyze_data([sample], provided_kwargs={"rec_path": "results"})
    captured = capsys.readouterr().out
    # The specific rec_path-recommendation block must NOT fire
    assert "rec_path='results'" not in captured
    assert "WARNING: The root object is a dictionary, but it contains arrays" not in captured


def test_analyze_data_uuid_primary_key(capsys: pytest.CaptureFixture[str]) -> None:
    """UUID-shaped string values must score highly as inc_code candidates."""
    sample = {"user_uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "full_name": "Alice"}
    analyze_data([sample], provided_kwargs={})
    captured = capsys.readouterr().out
    assert "inc_code='user_uuid'" in captured
    assert "inc_name='full_name'" in captured


def test_analyze_data_int_id_primary_key(capsys: pytest.CaptureFixture[str]) -> None:
    """Integer ``id`` fields must be detected as the primary key."""
    sample = {"id": 42, "title": "Hello World"}
    analyze_data([sample], provided_kwargs={})
    captured = capsys.readouterr().out
    assert "inc_code='id'" in captured
    assert "inc_name='title'" in captured


def test_analyze_data_datetime_string_detection(capsys: pytest.CaptureFixture[str]) -> None:
    """ISO-shaped string values must be flagged for inc(datetime) conversion."""
    sample = {"id": 1, "created_at": "2026-05-12T14:32:00Z", "name": "Alice"}
    analyze_data([sample], provided_kwargs={})
    captured = capsys.readouterr().out
    # The ETL suggestion block must propose inc(datetime) for created_at
    assert "inc(datetime)" in captured
    assert "'created_at'" in captured


def test_analyze_data_non_dict_top_level(capsys: pytest.CaptureFixture[str]) -> None:
    """A non-dict top-level sample (e.g. a primitive list) emits a friendly notice."""
    analyze_data([["just", "strings"]], provided_kwargs={})
    captured = capsys.readouterr().out
    # The renderer should mention the array typing and exit without dict-suggestions
    assert "No further attribute suggestions" in captured


# ==========================================
# 3. analyze_error — ROUTING TO ACTIONABLE HINTS
# ==========================================


def test_analyze_error_http_401(capsys: pytest.CaptureFixture[str]) -> None:
    """A wrapped HTTP 401 must suggest passing an Authorization header."""
    req = httpx.Request("GET", "https://api.example.com")
    resp = httpx.Response(401, request=req)
    underlying = httpx.HTTPStatusError("401", request=req, response=resp)

    err = IncorporatorNetworkError("Fatal client error 401")
    err.__cause__ = underlying

    analyze_error(err)
    captured = capsys.readouterr().out
    assert "Auth Blocked" in captured
    assert "Authorization" in captured


def test_analyze_error_json_decode(capsys: pytest.CaptureFixture[str]) -> None:
    """A JSON-decode format error must surface the NDJSON hint."""
    analyze_error(IncorporatorFormatError("JSON decode failed: invalid token at position 0"))
    captured = capsys.readouterr().out
    assert "JSON Decode Failed" in captured
    assert "NDJSON" in captured


def test_analyze_error_avro_missing(capsys: pytest.CaptureFixture[str]) -> None:
    """An Avro-related format error must suggest installing fastavro."""
    analyze_error(IncorporatorFormatError("fastavro not installed"))
    captured = capsys.readouterr().out
    assert "fastavro" in captured.lower()


def test_analyze_error_generic_schema(capsys: pytest.CaptureFixture[str]) -> None:
    """A plain exception falls through to the generic schema fallback message."""

    class CustomError(Exception):
        pass

    analyze_error(CustomError("something exploded"))
    captured = capsys.readouterr().out
    assert "schema or configuration error" in captured
