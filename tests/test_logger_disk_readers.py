"""Tests for LoggingMixin.get_rejects, get_api, get_error and LoggedTideweaver.get_tides / get_rejects."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from incorporator import LoggedIncorporator, setup_class_logger
from incorporator.observability.logger import (
    _ACTIVE_LISTENERS,
    _route_reject_to_log,
    _safe_log_filename,
)
from incorporator.observability.tideweaver.logged import LoggedTideweaver


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _RejectSource(LoggedIncorporator):
    """Stand-in for get_rejects disk-reader tests."""


class _RejectSource2(LoggedIncorporator):
    """Second stand-in to verify class-name isolation."""


def _write_jsonl(path: Path, records: list[dict]) -> None:
    """Write a sequence of dicts as JSONL to *path*, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------------------
# A1 — LoggingMixin.get_rejects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_rejects_filters_to_reject_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_rejects() returns only records that contain a top-level 'reject' key.

    Mixed error.log with wave records + reject records — only the reject
    records should be returned; wave records must be excluded.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "_RejectSource_error.log"
    _write_jsonl(
        error_log,
        [
            {"level": "ERROR", "msg": "chunk failed", "wave": {"chunk_index": 1}},
            {"level": "ERROR", "msg": "reject 1", "reject": {"source": "url1", "error_kind": "Timeout"}},
            {"level": "ERROR", "msg": "reject 2", "reject": {"source": "url2", "error_kind": "HTTPStatusError"}},
        ],
    )

    result = await _RejectSource.get_rejects()

    assert len(result) == 2
    assert all("reject" in r for r in result)
    sources = {r["reject"]["source"] for r in result}
    assert sources == {"url1", "url2"}


@pytest.mark.asyncio
async def test_get_rejects_inherited_by_logged_incorporator(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_rejects() is callable on any LoggedIncorporator subclass.

    Verifies the method resolves the correct log filename from cls.__name__.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "_RejectSource2_error.log"
    _write_jsonl(
        error_log,
        [
            {
                "level": "ERROR",
                "msg": "canal reject",
                "reject": {"source": "ArbitrageFjord", "error_kind": "PenstockLimited"},
            },
        ],
    )

    result = await _RejectSource2.get_rejects()

    assert len(result) == 1
    assert result[0]["reject"]["error_kind"] == "PenstockLimited"


@pytest.mark.asyncio
async def test_get_rejects_returns_empty_when_no_log_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_rejects() returns [] when the log file does not yet exist."""
    monkeypatch.chdir(tmp_path)

    result = await _RejectSource.get_rejects()

    assert result == []


# ---------------------------------------------------------------------------
# A2 — LoggedTideweaver.get_tides
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tides_reads_tide_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_tides() reads from tide.log and returns records sorted by tide_number.

    All tides — fired (INFO/ERROR) and no-op (DEBUG) — land in the dedicated
    tide.log via TideFilter.  get_tides() reads a single file and returns the
    records sorted ascending by tide_number.
    """
    monkeypatch.chdir(tmp_path)

    tide_log = tmp_path / "logs" / "TidesSession_tide.log"

    _write_jsonl(
        tide_log,
        [
            {"level": "INFO", "msg": "tide 1", "tide": {"tide_number": 1, "fired": ["prices"]}},
            {"level": "ERROR", "msg": "tide 3", "tide": {"tide_number": 3, "canal_rejects_added": 1}},
            {"level": "DEBUG", "msg": "tide 2", "tide": {"tide_number": 2, "fired": []}},
        ],
    )

    result = await LoggedTideweaver.get_tides("TidesSession")

    tide_numbers = [r["tide"]["tide_number"] for r in result]
    # Sorted ascending — 3 unique tides from a single file.
    assert tide_numbers == [1, 2, 3]
    assert result[0]["tide"]["fired"] == ["prices"]
    assert result[1]["tide"]["fired"] == []
    assert result[2]["tide"]["canal_rejects_added"] == 1


@pytest.mark.asyncio
async def test_get_tides_empty_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_tides() returns [] when the tide.log file does not exist."""
    monkeypatch.chdir(tmp_path)

    result = await LoggedTideweaver.get_tides("NoSuchSession")

    assert result == []


# ---------------------------------------------------------------------------
# A3 — LoggedTideweaver.get_rejects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_get_rejects_uses_logger_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_rejects(logger_name) reads from the named log file.

    Two different logger_names must resolve to independent log files; records
    from one session must not bleed into the other.
    """
    monkeypatch.chdir(tmp_path)

    session_a_log = tmp_path / "logs" / "SessionA_error.log"
    session_b_log = tmp_path / "logs" / "SessionB_error.log"

    _write_jsonl(
        session_a_log,
        [{"level": "ERROR", "msg": "r", "reject": {"source": "a_source", "error_kind": "Timeout"}}],
    )
    _write_jsonl(
        session_b_log,
        [
            {"level": "ERROR", "msg": "r1", "reject": {"source": "b_source1", "error_kind": "HTTPStatusError"}},
            {"level": "ERROR", "msg": "r2", "reject": {"source": "b_source2", "error_kind": "PenstockLimited"}},
        ],
    )

    rejects_a = await LoggedTideweaver.get_rejects("SessionA")
    rejects_b = await LoggedTideweaver.get_rejects("SessionB")

    assert len(rejects_a) == 1
    assert rejects_a[0]["reject"]["source"] == "a_source"

    assert len(rejects_b) == 2
    sources_b = {r["reject"]["source"] for r in rejects_b}
    assert sources_b == {"b_source1", "b_source2"}


# ---------------------------------------------------------------------------
# B1 — LoggingMixin.get_api (new reader)
# ---------------------------------------------------------------------------


class _ApiLogSource(LoggedIncorporator):
    """Stand-in for get_api disk-reader tests."""


@pytest.mark.asyncio
async def test_get_api_returns_all_records_from_api_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_api() returns all records from api.log (rejects and hand-logged).

    Pre-baked api.log contains one URL-traffic reject and one lifecycle record.
    get_api() must return both (it reads the full file, not just reject records).
    """
    monkeypatch.chdir(tmp_path)

    api_log = tmp_path / "logs" / "_ApiLogSource_api.log"
    _write_jsonl(
        api_log,
        [
            {
                "level": "ERROR",
                "msg": "ReadTimeout: https://api.example.com/data",
                "reject": {"source": "https://api.example.com/data", "error_kind": "ReadTimeout", "is_url_traffic_error": True},
            },
            {"level": "INFO", "msg": "Initiating export process with kwargs={}"},
        ],
    )

    result = await _ApiLogSource.get_api()

    assert len(result) == 2
    assert result[0]["reject"]["error_kind"] == "ReadTimeout"
    assert result[1]["msg"] == "Initiating export process with kwargs={}"


@pytest.mark.asyncio
async def test_get_api_returns_empty_when_no_log_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_api() returns [] when the api.log file does not yet exist."""
    monkeypatch.chdir(tmp_path)

    result = await _ApiLogSource.get_api()

    assert result == []


# ---------------------------------------------------------------------------
# B2 — get_rejects reads both error.log and api.log
# ---------------------------------------------------------------------------


class _BothFilesSource(LoggedIncorporator):
    """Stand-in for union-reader tests."""


@pytest.mark.asyncio
async def test_get_rejects_reads_both_error_and_api_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_rejects() returns reject records from both error.log and api.log.

    error.log carries parse-error and canal rejects; api.log carries URL-traffic
    rejects. get_rejects() must union both so callers need not distinguish the
    routing.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "_BothFilesSource_error.log"
    api_log = tmp_path / "logs" / "_BothFilesSource_api.log"

    _write_jsonl(
        error_log,
        [
            {"level": "ERROR", "msg": "r1", "reject": {"source": "file://bad.json", "error_kind": "IncorporatorFormatError"}},
            {"level": "ERROR", "msg": "r2", "reject": {"source": "CanalSource", "error_kind": "PenstockLimited"}},
        ],
    )
    _write_jsonl(
        api_log,
        [
            {
                "level": "ERROR",
                "msg": "r3",
                "reject": {"source": "https://api.example.com/data", "error_kind": "ReadTimeout"},
            },
        ],
    )

    result = await _BothFilesSource.get_rejects()

    assert len(result) == 3
    sources = {r["reject"]["source"] for r in result}
    assert sources == {"file://bad.json", "CanalSource", "https://api.example.com/data"}


@pytest.mark.asyncio
async def test_get_rejects_union_when_api_log_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_rejects() returns error.log rejects even when api.log does not exist.

    Covers the common case where no URL-traffic errors have occurred yet.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "_BothFilesSource_error.log"
    _write_jsonl(
        error_log,
        [
            {"level": "ERROR", "msg": "r1", "reject": {"source": "seed_error", "error_kind": "KeyError"}},
        ],
    )

    result = await _BothFilesSource.get_rejects()

    assert len(result) == 1
    assert result[0]["reject"]["source"] == "seed_error"


# ---------------------------------------------------------------------------
# B3 — get_error does NOT return URL-traffic rejects (they're in api.log)
# ---------------------------------------------------------------------------


class _ErrorLogOnlySource(LoggedIncorporator):
    """Stand-in for get_error exclusion tests."""


@pytest.mark.asyncio
async def test_get_error_excludes_url_traffic_rejects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_error() reads only error.log — URL-traffic rejects in api.log are invisible.

    Pre-baked error.log has a wave record and a parse-error reject.
    Pre-baked api.log has a URL-traffic reject.
    get_error() must return only the two error.log records and not the api.log reject.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "_ErrorLogOnlySource_error.log"
    api_log = tmp_path / "logs" / "_ErrorLogOnlySource_api.log"

    _write_jsonl(
        error_log,
        [
            {"level": "ERROR", "msg": "wave failed", "wave": {"chunk_index": 1, "failed_sources": ["x"]}},
            {"level": "ERROR", "msg": "parse reject", "reject": {"source": "f.json", "error_kind": "IncorporatorFormatError"}},
        ],
    )
    _write_jsonl(
        api_log,
        [
            {
                "level": "ERROR",
                "msg": "url reject",
                "reject": {"source": "https://api.example.com/", "error_kind": "ReadTimeout"},
            },
        ],
    )

    result = await _ErrorLogOnlySource.get_error()

    assert len(result) == 2
    msgs = {r["msg"] for r in result}
    assert "wave failed" in msgs
    assert "parse reject" in msgs
    assert "url reject" not in msgs


# ---------------------------------------------------------------------------
# B4 — LoggedIncorporator.incorp reach: URL-traffic reject routes to api.log
# ---------------------------------------------------------------------------


class _IncorpReachSource(LoggedIncorporator):
    """Stand-in for LoggedIncorporator incorp reject-reach tests."""


@pytest.mark.asyncio
async def test_logged_incorp_url_traffic_reject_routes_via_route_reject_to_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """logged incorp with a failing URL calls _route_reject_to_log with a URL-traffic reject.

    Mocks execute_request to raise httpx.ReadTimeout and verifies that
    _route_reject_to_log is called with a RejectEntry that has
    is_url_traffic_error=True.  Uses patch rather than reading a real log file
    to avoid QueueHandler flush-timing races.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    async def _raise_read_timeout(url: str, *args: object, **kwargs: object) -> object:
        raise httpx.ReadTimeout("connection timed out")

    monkeypatch.setattr(fetch, "execute_request", _raise_read_timeout)

    routed_rejects = []

    def _capture_route(cls_name: str, reject: object) -> None:
        routed_rejects.append((cls_name, reject))

    with patch("incorporator.observability.logger._route_reject_to_log", side_effect=_capture_route):
        await _IncorpReachSource.incorp(
            inc_url="https://api.example.com/data",
            enable_logging=True,
        )

    assert len(routed_rejects) == 1
    cls_name, reject = routed_rejects[0]
    assert cls_name == "_IncorpReachSource"
    assert reject.is_url_traffic_error is True
    assert reject.error_kind == "ReadTimeout"


@pytest.mark.asyncio
async def test_logged_incorp_format_error_routes_to_error_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """logged incorp with a malformed local file calls _route_reject_to_log with is_url_traffic_error=False.

    Writes a temp file with invalid JSON so IncorporatorFormatError is raised.
    Verifies that the reject routed by LoggedIncorporator.incorp has
    is_url_traffic_error=False — it stays in error.log, not api.log.
    """
    monkeypatch.chdir(tmp_path)

    bad_file = tmp_path / "bad.json"
    bad_file.write_text("this is not valid json {{{", encoding="utf-8")

    routed_rejects = []

    def _capture_route(cls_name: str, reject: object) -> None:
        routed_rejects.append((cls_name, reject))

    with patch("incorporator.observability.logger._route_reject_to_log", side_effect=_capture_route):
        await _IncorpReachSource.incorp(
            inc_file=str(bad_file),
            enable_logging=True,
        )

    assert len(routed_rejects) == 1
    _, reject = routed_rejects[0]
    assert reject.is_url_traffic_error is False
    assert reject.error_kind == "IncorporatorFormatError"
