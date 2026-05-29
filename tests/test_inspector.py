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
# 2b. WRONG-TARGET-OBJECT REGRESSION
# ==========================================


@pytest.fixture
def spacex_like_launch() -> dict[str, Any]:
    """SpaceX /launches/latest-shaped fixture — single resource with nested arrays.

    This is the user's original failing case: a rich top-level record whose
    identity + dates we care about, but with nested list-of-dicts that the
    pre-fix inspector silently drilled into.
    """
    return {
        "id": "5eb87cdaffd86e000604b330",
        "name": "FalconSat",
        "date_local": "2008-09-20T13:23:00-04:00",
        "date_utc": "2008-09-20T17:23:00.000Z",
        "flight_number": 4,
        "rocket": "5e9d0d95eda69955f709d1eb",
        "success": False,
        "cores": [
            {"core": "5e9e289df3591855a26b4ac0", "flight": 1, "gridfins": False},
            {"core": "abc123def456", "flight": 2, "gridfins": True},
        ],
        "failures": [{"time": 33, "altitude": None, "reason": "merlin engine failure"}],
    }


def test_analyze_data_does_not_drift_into_nested_lists(
    capsys: pytest.CaptureFixture[str], spacex_like_launch: dict[str, Any]
) -> None:
    """Regression for the wrong-target-object bug.

    The pre-fix inspector silently rebound ``target_obj`` to
    ``sample["cores"][0]`` whenever the top-level dict contained any list of
    dicts. That meant identity mapping and ETL ran against the WRONG object —
    nested core specs instead of the top-level launch. This test asserts the
    fix: identity + ETL are evaluated against the top-level launch record.
    """
    analyze_data([spacex_like_launch], provided_kwargs={})
    out = capsys.readouterr().out

    # Identity must come from the launch itself, not from the nested core.
    assert "inc_code='id'" in out
    assert "inc_name='name'" in out
    # Neither nested-list key should show up as the chosen identity.
    assert "inc_code='core'" not in out
    assert "inc_code='flight'" not in out

    # Both top-level date fields must be flagged for inc(datetime).
    assert "'date_local'" in out
    assert "'date_utc'" in out
    assert "inc(datetime)" in out


def test_analyze_data_prints_drill_down_hint_for_nested_arrays(
    capsys: pytest.CaptureFixture[str], spacex_like_launch: dict[str, Any]
) -> None:
    """When nested list-of-dicts are present, surface a copy-pasteable drill cmd."""
    analyze_data([spacex_like_launch], provided_kwargs={})
    out = capsys.readouterr().out

    # The drill-down section must mention the nested arrays with their sizes.
    assert "nested arrays" in out
    assert "cores (2)" in out
    assert "failures (1)" in out

    # And the suggested re-run command names rec_path explicitly.
    assert "rec_path=" in out
    assert "await YourClass.test" in out


# ==========================================
# 2c. DATE / TYPE CASTING — VARIANTS THAT FAILED BEFORE
# ==========================================


@pytest.mark.parametrize(
    "value",
    [
        "2022-10-05T12:00:00-04:00",  # RFC-3339 with timezone offset (the bug)
        "2026-05-12T14:32:00Z",  # UTC suffix
        "2008-09-20T17:23:00.000Z",  # fractional + Z
        "2008-09-20",  # plain date
        "2026-04-22 23:59:59",  # SQL-ish space separator
    ],
)
def test_analyze_data_flags_every_datetime_variant_the_runtime_accepts(
    capsys: pytest.CaptureFixture[str], value: str
) -> None:
    """Any ISO/RFC-3339 variant the framework runtime accepts must be flagged.

    Inspector detection routes through the same ``_fallback_date`` the
    runtime uses, so this matrix is the structural contract: if ``inc(datetime)``
    would accept it, the inspector recommends it.
    """
    sample = {"id": 1, "happened_at": value, "name": "Sample"}
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    assert "inc(datetime)" in out
    assert "'happened_at'" in out


@pytest.mark.parametrize("junk", ["", "N/A", "n/a", "Unknown", "null", "undefined"])
def test_analyze_data_does_not_flag_garbage_values_as_dates(capsys: pytest.CaptureFixture[str], junk: str) -> None:
    """Garbage values must never be suggested for conversion — even in date-named keys."""
    sample = {"id": 1, "created_at": junk, "name": "Sample"}
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    # The ETL block should report nothing-to-do for created_at.
    assert "inc(datetime)" not in out
    assert "No string fields look like dates or numbers" in out


# 2d. PAGINATION HINTS


def test_pagination_hint_next_url_paginator(capsys: pytest.CaptureFixture[str]) -> None:
    """A ``next`` URL field must trigger a NextUrlPaginator suggestion."""
    sample = {
        "results": [{"id": 1}],
        "next": "https://api.example.com/items?page=2",
        "count": 100,
    }
    analyze_data([sample], provided_kwargs={"rec_path": "results"})
    out = capsys.readouterr().out
    assert "PAGINATION HINTS" in out
    assert "NextUrlPaginator('next')" in out


def test_pagination_hint_cursor_paginator(capsys: pytest.CaptureFixture[str]) -> None:
    """A ``next_cursor`` field must trigger a CursorPaginator suggestion."""
    sample = {"items": [{"id": 1}], "next_cursor": "abc123token"}
    analyze_data([sample], provided_kwargs={"rec_path": "items"})
    out = capsys.readouterr().out
    assert "CursorPaginator(cursor_param='next_cursor')" in out


def test_pagination_hint_offset_paginator(capsys: pytest.CaptureFixture[str]) -> None:
    """An ``offset`` + ``limit`` pair must trigger an OffsetPaginator suggestion."""
    sample = {"results": [{"id": 1}], "offset": 0, "limit": 50}
    analyze_data([sample], provided_kwargs={"rec_path": "results"})
    out = capsys.readouterr().out
    assert "OffsetPaginator(limit=50)" in out


def test_pagination_hint_silent_when_no_signal(capsys: pytest.CaptureFixture[str]) -> None:
    """A flat single-resource response should NOT spuriously suggest pagination."""
    sample = {"id": 1, "name": "Solo", "created_at": "2026-05-12T14:32:00Z"}
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    assert "PAGINATION HINTS" not in out


# 2e. HEAVY-FIELD HINTS


def test_heavy_field_hint_flags_asset_urls(capsys: pytest.CaptureFixture[str]) -> None:
    """Image / video URLs at the top level get nominated for excl_lst."""
    sample = {
        "id": 1,
        "name": "Bulbasaur",
        "front_default": "https://raw.githubusercontent.com/sprites/1.png",
        "thumbnail_url": "https://cdn.example.com/thumbs/" + "x" * 250,
    }
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    assert "HEAVY-FIELD HINTS" in out
    assert "excl_lst=" in out
    assert "'front_default'" in out or "'thumbnail_url'" in out


def test_heavy_field_hint_flags_base64_image(capsys: pytest.CaptureFixture[str]) -> None:
    """Base64-encoded image blobs trigger the excl_lst suggestion."""
    sample = {"id": 1, "preview": "data:image/png;base64," + "A" * 100, "name": "X"}
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    assert "HEAVY-FIELD HINTS" in out
    assert "'preview'" in out


def test_heavy_field_hint_silent_for_lean_payload(capsys: pytest.CaptureFixture[str]) -> None:
    """A small clean payload must NOT emit heavy-field hints."""
    sample = {"id": 1, "name": "Lean", "active": True}
    analyze_data([sample], provided_kwargs={})
    out = capsys.readouterr().out
    assert "HEAVY-FIELD HINTS" not in out


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
