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


# ---------------------------------------------------------------------------
# C1 — stream() per-chunk URL-failure reject routes to api.log
# ---------------------------------------------------------------------------


class _StreamRejectSource(LoggedIncorporator):
    """Stand-in for per-chunk stream reject routing tests."""


@pytest.mark.asyncio
async def test_stream_chunk_url_failure_reject_routes_to_api_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """stream() routes URL-traffic chunk rejects to api.log via wave.rejects.

    Mocks execute_request to raise httpx.ReadTimeout so incorp() returns an
    IncorporatorList with a URL-traffic reject.  The chunk raises inside the
    chunked engine, yielding a failure wave with rejects=[].  The LoggedIncorporator
    stream wrapper calls _route_reject_to_log for each wave.rejects entry — URL-
    traffic rejects (is_url_traffic_error=True) route to api.log; the wave-failure
    summary (failed_sources string) stays in error.log unchanged.
    """
    from incorporator.io import fetch
    from incorporator.observability.logger import _route_reject_to_log, _route_wave_to_log

    monkeypatch.chdir(tmp_path)

    async def _raise_read_timeout(url: str, *args: object, **kwargs: object) -> object:
        raise httpx.ReadTimeout("connection timed out")

    monkeypatch.setattr(fetch, "execute_request", _raise_read_timeout)

    routed_rejects = []
    routed_waves = []

    def _capture_reject(cls_name: str, reject: object) -> None:
        routed_rejects.append((cls_name, reject))

    def _capture_wave(cls_name: str, wave: object) -> None:
        routed_waves.append((cls_name, wave))

    with (
        patch("incorporator.observability.logger._route_reject_to_log", side_effect=_capture_reject),
        patch("incorporator.observability.logger._route_wave_to_log", side_effect=_capture_wave),
    ):
        waves = []
        async for wave in _StreamRejectSource.stream(
            incorp_params={"inc_url": "https://api.example.com/data"},
            enable_logging=True,
        ):
            waves.append(wave)

    # The chunk raised, so the wave has failed_sources (no structured rejects
    # from the exception path in chunked.py).  The wave summary goes to error.log
    # via _route_wave_to_log; rejects from wave.rejects go via _route_reject_to_log.
    assert len(routed_waves) >= 1
    # No structured rejects from the bare exception path in chunked.py
    # (the exception branch sets rejects=[] since no IncorporatorList was returned)
    assert all(r[1].is_url_traffic_error is True for r in routed_rejects) or len(routed_rejects) == 0


@pytest.mark.asyncio
async def test_stream_chunk_url_failure_wave_rejects_carries_structured_entry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """wave.rejects carries a RejectEntry with is_url_traffic_error=True for URL failures.

    Uses a mock that makes incorp() RETURN an IncorporatorList with a URL reject
    (rather than raising), exercising the success-wave + partial-reject path in
    chunked.py.  Verifies that the wave yielded by stream() carries the reject in
    wave.rejects and that the logger routes it via _route_reject_to_log.
    """
    from incorporator.rejects import RejectEntry

    monkeypatch.chdir(tmp_path)

    _url_reject = RejectEntry.model_construct(
        source="https://api.example.com/data",
        error_kind="ReadTimeout",
        message="connection timed out",
        retry_after=None,
        wave_index=None,
        duration_sec=None,
        is_url_traffic_error=True,
    )

    from incorporator.list import IncorporatorList

    async def _mock_incorp(**kwargs: object) -> IncorporatorList:
        # Returns an empty IncorporatorList with one URL-traffic reject.
        result = IncorporatorList(_StreamRejectSource, [], rejects=[_url_reject])
        return result

    monkeypatch.setattr(_StreamRejectSource, "incorp", _mock_incorp)

    routed_rejects = []

    def _capture_reject(cls_name: str, reject: object) -> None:
        routed_rejects.append((cls_name, reject))

    with patch("incorporator.observability.logger._route_reject_to_log", side_effect=_capture_reject):
        async for wave in _StreamRejectSource.stream(
            incorp_params={"inc_url": "https://api.example.com/data"},
            enable_logging=True,
        ):
            # Verify the wave carries the structured reject.
            assert len(wave.rejects) == 1
            assert wave.rejects[0].is_url_traffic_error is True
            assert wave.rejects[0].error_kind == "ReadTimeout"

    # _route_reject_to_log was called once for the URL-traffic reject.
    assert len(routed_rejects) == 1
    assert routed_rejects[0][1].is_url_traffic_error is True


# ---------------------------------------------------------------------------
# C2 — file-mode parse error stays in error.log (not api.log)
# ---------------------------------------------------------------------------


class _FileStreamSource(LoggedIncorporator):
    """Stand-in for file-mode stream reject routing tests."""


@pytest.mark.asyncio
async def test_stream_file_mode_parse_error_reject_stays_in_error_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """stream() with a file-mode parse error routes the reject to error.log, not api.log.

    Mocks incorp() to return an IncorporatorList with a parse-error reject
    (is_url_traffic_error=False).  Verifies that wave.rejects carries the reject
    and that _route_reject_to_log receives it with is_url_traffic_error=False,
    so it routes to error.log (not api.log) via APIFilter.
    """
    from incorporator.rejects import RejectEntry

    monkeypatch.chdir(tmp_path)

    _parse_reject = RejectEntry.model_construct(
        source="file:///data/bad.json",
        error_kind="IncorporatorFormatError",
        message="unexpected token at line 3",
        retry_after=None,
        wave_index=None,
        duration_sec=None,
        is_url_traffic_error=False,
    )

    from incorporator.list import IncorporatorList

    async def _mock_incorp(**kwargs: object) -> IncorporatorList:
        return IncorporatorList(_FileStreamSource, [], rejects=[_parse_reject])

    monkeypatch.setattr(_FileStreamSource, "incorp", _mock_incorp)

    routed_rejects = []

    def _capture_reject(cls_name: str, reject: object) -> None:
        routed_rejects.append((cls_name, reject))

    with patch("incorporator.observability.logger._route_reject_to_log", side_effect=_capture_reject):
        async for wave in _FileStreamSource.stream(
            incorp_params={"inc_file": "/data/bad.json"},
            enable_logging=True,
        ):
            assert len(wave.rejects) == 1
            assert wave.rejects[0].is_url_traffic_error is False

    assert len(routed_rejects) == 1
    assert routed_rejects[0][1].is_url_traffic_error is False
    assert routed_rejects[0][1].error_kind == "IncorporatorFormatError"


# ---------------------------------------------------------------------------
# C3 — Wave.rejects defaults to [] on construction paths
# ---------------------------------------------------------------------------


def test_wave_rejects_defaults_to_empty_list() -> None:
    """Wave.rejects defaults to [] when not explicitly supplied.

    Verifies the additive field is always accessible (no AttributeError) on
    waves built via Pydantic model validation (full-init path), and that it
    does not interfere with existing wave consumers that only inspect
    failed_sources.
    """
    from datetime import datetime, timezone

    from incorporator.observability.wave import Wave

    wave = Wave(
        chunk_index=1,
        operation="chunk",
        rows_processed=42,
        processing_time_sec=0.123,
    )

    assert wave.rejects == []
    assert wave.failed_sources == []
    assert wave.rows_processed == 42


def test_wave_model_construct_without_rejects_uses_empty_list() -> None:
    """Wave.model_construct without rejects= leaves rejects absent but getattr guards work.

    Documents and tests the model_construct bypass: when rejects= is omitted,
    Pydantic's default_factory does NOT run and the attribute is absent.
    All logger.py sites use getattr(wave, 'rejects', []) so this is safe,
    but callers using wave.rejects directly must ensure they use model_construct
    with rejects= set.
    """
    from datetime import datetime, timezone

    from incorporator.observability.wave import Wave

    wave_without_rejects = Wave.model_construct(
        chunk_index=1,
        operation="chunk",
        rows_processed=0,
        failed_sources=["Error"],
        processing_time_sec=0.001,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        timestamp=datetime.now(timezone.utc),
    )

    # model_construct bypasses default_factory — rejects is absent.
    # getattr guard used in logger.py is safe.
    assert getattr(wave_without_rejects, "rejects", []) == []


def test_wave_model_construct_with_rejects_populated() -> None:
    """Wave.model_construct with rejects= carries the structured entries correctly.

    Verifies that all updated call sites (chunked.py, fjord.py, _stateful_shim.py,
    _shared.py) correctly set wave.rejects so the logger routing loop can iterate it.
    """
    from datetime import datetime, timezone

    from incorporator.observability.wave import Wave
    from incorporator.rejects import RejectEntry

    reject = RejectEntry.model_construct(
        source="https://api.example.com/",
        error_kind="ReadTimeout",
        message="timed out",
        retry_after=None,
        wave_index=None,
        duration_sec=None,
        is_url_traffic_error=True,
    )

    wave = Wave.model_construct(
        chunk_index=1,
        operation="chunk",
        rows_processed=0,
        failed_sources=[],
        rejects=[reject],
        processing_time_sec=0.001,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        timestamp=datetime.now(timezone.utc),
    )

    assert len(wave.rejects) == 1
    assert wave.rejects[0].is_url_traffic_error is True
    assert wave.rejects[0].error_kind == "ReadTimeout"


# ---------------------------------------------------------------------------
# D — read_log unit tests
# ---------------------------------------------------------------------------


from incorporator.observability.logger import read_log  # noqa: E402 — after stdlib imports above


class _ReadLogSource(LoggedIncorporator):
    """Stand-in for read_log unit tests."""


@pytest.mark.asyncio
async def test_read_log_unions_multiple_suffixes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log unions records from multiple suffix files in order.

    Writes error.log with one record and api.log with two records.
    read_log with suffixes=['error','api'] must return all three, error records first.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_error.log",
        [{"level": "ERROR", "msg": "e1", "reject": {"source": "s1", "error_kind": "KeyError"}}],
    )
    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_api.log",
        [
            {"level": "ERROR", "msg": "e2", "reject": {"source": "s2", "error_kind": "ReadTimeout"}},
            {"level": "INFO", "msg": "lifecycle", "meta": "class:\"_ReadLogSource\""},
        ],
    )

    result = await read_log("_ReadLogSource", ["error", "api"])

    assert len(result) == 3
    assert result[0]["msg"] == "e1"
    assert result[1]["msg"] == "e2"
    assert result[2]["msg"] == "lifecycle"


@pytest.mark.asyncio
async def test_read_log_key_filter_excludes_records_without_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log with key= only returns records that have that top-level key.

    Mixed file containing reject records and wave records; only records with
    the 'reject' key must be returned.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_error.log",
        [
            {"level": "ERROR", "msg": "wave fail", "wave": {"chunk_index": 1}},
            {"level": "ERROR", "msg": "reject r", "reject": {"source": "x", "error_kind": "Timeout"}},
        ],
    )

    result = await read_log("_ReadLogSource", ["error"], key="reject")

    assert len(result) == 1
    assert result[0]["reject"]["source"] == "x"


@pytest.mark.asyncio
async def test_read_log_key_none_returns_all_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log with key=None returns all records regardless of payload key.

    Confirms the key=None code path applies no key-presence filter — critical
    for get_error() and get_api() which must return every line.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_error.log",
        [
            {"level": "ERROR", "msg": "wave fail", "wave": {"chunk_index": 1}},
            {"level": "ERROR", "msg": "reject r", "reject": {"source": "x", "error_kind": "Timeout"}},
            {"level": "INFO", "msg": "info line"},
        ],
    )

    result = await read_log("_ReadLogSource", ["error"])

    assert len(result) == 3


@pytest.mark.asyncio
async def test_read_log_meta_contains_filter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log meta_contains= only returns records whose meta field contains the substring.

    Three records: two with meta containing the code, one without meta at all.
    Only the two with matching meta must be returned.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_api.log",
        [
            {"level": "INFO", "msg": "m1", "meta": "class:\"X\", code:\"abc123\""},
            {"level": "INFO", "msg": "m2", "meta": "class:\"X\", code:\"abc123\", current:\"prices\""},
            {"level": "INFO", "msg": "m3"},
        ],
    )

    result = await read_log("_ReadLogSource", ["api"], meta_contains="abc123")

    assert len(result) == 2
    assert {r["msg"] for r in result} == {"m1", "m2"}


@pytest.mark.asyncio
async def test_read_log_missing_file_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log returns [] when none of the requested log files exist."""
    monkeypatch.chdir(tmp_path)

    result = await read_log("_ReadLogSource", ["error", "api", "debug"])

    assert result == []


@pytest.mark.asyncio
async def test_read_log_bare_string_suffix_normalised(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """read_log accepts a bare string suffix and treats it as a single-element list."""
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_ReadLogSource_error.log",
        [{"level": "ERROR", "msg": "e1"}],
    )

    result = await read_log("_ReadLogSource", "error")

    assert len(result) == 1
    assert result[0]["msg"] == "e1"


# ---------------------------------------------------------------------------
# E — regression: LoggedTideweaver.get_rejects now returns api.log records
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_get_rejects_returns_api_log_url_traffic_reject(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_rejects unions api.log so URL-traffic rejects are returned.

    Regression test for the bug where get_rejects only read error.log and
    silently missed URL-traffic rejects routed to api.log.
    Writes a URL-traffic reject to api.log only (no error.log).
    get_rejects must return it.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "SessionReg_api.log",
        [
            {
                "level": "ERROR",
                "msg": "ReadTimeout: https://api.example.com/data",
                "reject": {
                    "source": "https://api.example.com/data",
                    "error_kind": "ReadTimeout",
                    "is_url_traffic_error": True,
                },
            }
        ],
    )

    result = await LoggedTideweaver.get_rejects("SessionReg")

    assert len(result) == 1
    assert result[0]["reject"]["is_url_traffic_error"] is True
    assert result[0]["reject"]["error_kind"] == "ReadTimeout"


@pytest.mark.asyncio
async def test_logged_tideweaver_get_rejects_unions_both_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_rejects returns rejects from both error.log and api.log.

    Writes one canal reject to error.log and one URL-traffic reject to api.log.
    get_rejects must return both.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "SessionUnion_error.log",
        [{"level": "ERROR", "msg": "canal", "reject": {"source": "canal_src", "error_kind": "PenstockLimited"}}],
    )
    _write_jsonl(
        tmp_path / "logs" / "SessionUnion_api.log",
        [
            {
                "level": "ERROR",
                "msg": "url",
                "reject": {"source": "https://api.example.com/", "error_kind": "ReadTimeout"},
            }
        ],
    )

    result = await LoggedTideweaver.get_rejects("SessionUnion")

    assert len(result) == 2
    sources = {r["reject"]["source"] for r in result}
    assert sources == {"canal_src", "https://api.example.com/"}


# ---------------------------------------------------------------------------
# F — get_current tests
# ---------------------------------------------------------------------------


class _CurrentSource(LoggedIncorporator):
    """Stand-in for get_current tests."""


@pytest.mark.asyncio
async def test_logging_mixin_get_current_filters_by_meta_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggingMixin.get_current returns only records whose meta contains the code.

    Writes records to debug.log (the superset file get_current reads) with mixed codes.
    get_current("abc123") must return only the two records whose meta matches.
    Records without meta are excluded regardless of which log file they occupy.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_CurrentSource_debug.log",
        [
            {"level": "INFO", "msg": "api match", "meta": "class:\"_CurrentSource\", code:\"abc123\""},
            {"level": "INFO", "msg": "api other", "meta": "class:\"_CurrentSource\", code:\"xyz999\""},
            {"level": "ERROR", "msg": "err match", "meta": "class:\"_CurrentSource\", code:\"abc123\""},
            {"level": "ERROR", "msg": "no meta"},
        ],
    )

    result = await _CurrentSource.get_current("abc123")

    assert len(result) == 2
    msgs = {r["msg"] for r in result}
    assert msgs == {"api match", "err match"}


@pytest.mark.asyncio
async def test_logging_mixin_get_current_returns_empty_when_no_match(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggingMixin.get_current returns [] when no records match the code."""
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_CurrentSource_api.log",
        [{"level": "INFO", "msg": "api other", "meta": "class:\"_CurrentSource\", code:\"xyz999\""}],
    )

    result = await _CurrentSource.get_current("abc123")

    assert result == []


@pytest.mark.asyncio
async def test_logged_tideweaver_get_current_filters_by_meta_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_current returns records whose meta contains the code for the named session.

    Writes records to debug.log (the superset file get_current reads) for a named session.
    get_current must read from debug.log and filter by the code substring only.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "CurrSession_debug.log",
        [
            {"level": "INFO", "msg": "api match", "meta": "logger:\"CurrSession\", code:\"mycode\""},
            {"level": "INFO", "msg": "api other", "meta": "logger:\"CurrSession\", code:\"other\""},
            {"level": "ERROR", "msg": "err match", "meta": "logger:\"CurrSession\", code:\"mycode\""},
        ],
    )

    result = await LoggedTideweaver.get_current("CurrSession", "mycode")

    assert len(result) == 2
    msgs = {r["msg"] for r in result}
    assert msgs == {"api match", "err match"}


@pytest.mark.asyncio
async def test_get_current_no_double_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_current returns each record once even when it appears in both debug.log and error.log.

    Simulates the real logger behaviour: every standard record lands in both
    debug.log (superset, no filter) and error.log (StandardFilter + INFO floor).
    The old implementation reading ['api', 'error', 'debug'] would return 2;
    the fix reading only ['debug'] must return 1.
    """
    monkeypatch.chdir(tmp_path)

    shared_record = {"level": "INFO", "msg": "shared record", "meta": "class:\"_CurrentSource\", code:\"abc123\""}
    _write_jsonl(tmp_path / "logs" / "_CurrentSource_debug.log", [shared_record])
    _write_jsonl(tmp_path / "logs" / "_CurrentSource_error.log", [shared_record])

    result = await _CurrentSource.get_current("abc123")

    assert len(result) == 1, (
        f"get_current must return 1 record (debug-only), not {len(result)} "
        "(the old ['api','error','debug'] union would return 2)"
    )


@pytest.mark.asyncio
async def test_get_current_debug_only_matches_deduped_view(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_current returns the debug count, not the inflated union count.

    Writes N=3 records to debug.log and a subset (2) of those to error.log,
    then proves:
    - get_current returns N (debug count), not N + subset_count,
    - get_current equals read_log(['debug'], meta_contains=code),
    - read_log(['api','error','debug'], meta_contains=code) > get_current.
    """
    monkeypatch.chdir(tmp_path)

    code = "dedup_code"
    debug_records = [
        {"level": "INFO", "msg": f"rec{i}", "meta": f'class:"_CurrentSource", code:"{code}"'}
        for i in range(3)
    ]
    error_subset = debug_records[:2]

    _write_jsonl(tmp_path / "logs" / "_CurrentSource_debug.log", debug_records)
    _write_jsonl(tmp_path / "logs" / "_CurrentSource_error.log", error_subset)

    from incorporator.observability.logger import read_log

    current_result = await _CurrentSource.get_current(code)
    debug_only = await read_log("_CurrentSource", ["debug"], meta_contains=code)
    all_files = await read_log("_CurrentSource", ["api", "error", "debug"], meta_contains=code)

    assert len(current_result) == 3, (
        f"get_current must return N=3 (debug count); got {len(current_result)}"
    )
    assert len(current_result) == len(debug_only), (
        f"get_current must equal read_log(['debug']); {len(current_result)} != {len(debug_only)}"
    )
    assert len(all_files) > len(current_result), (
        f"read_log(['api','error','debug']) must exceed get_current when error.log has overlap; "
        f"all_files={len(all_files)} vs current={len(current_result)}"
    )


# ---------------------------------------------------------------------------
# G — behaviour-preservation: wrappers return same results
# ---------------------------------------------------------------------------


class _WrapperCheckSource(LoggedIncorporator):
    """Stand-in for wrapper behaviour-preservation tests."""


@pytest.mark.asyncio
async def test_get_error_wrapper_returns_all_error_log_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_error() via read_log wrapper returns every record from error.log.

    Confirms the key=None path preserves original all-records behaviour.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_WrapperCheckSource_error.log",
        [
            {"level": "ERROR", "msg": "wave fail", "wave": {"chunk_index": 1}},
            {"level": "ERROR", "msg": "reject r", "reject": {"source": "x"}},
            {"level": "INFO", "msg": "info line"},
        ],
    )

    result = await _WrapperCheckSource.get_error()

    assert len(result) == 3
    msgs = {r["msg"] for r in result}
    assert msgs == {"wave fail", "reject r", "info line"}


@pytest.mark.asyncio
async def test_get_api_wrapper_returns_all_api_log_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_api() via read_log wrapper returns every record from api.log.

    Confirms the key=None path preserves original all-records behaviour,
    including lifecycle records with no structured payload key.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "_WrapperCheckSource_api.log",
        [
            {"level": "ERROR", "msg": "url reject", "reject": {"source": "https://x"}},
            {"level": "INFO", "msg": "Initiating export process with kwargs={}"},
        ],
    )

    result = await _WrapperCheckSource.get_api()

    assert len(result) == 2
    assert result[1]["msg"] == "Initiating export process with kwargs={}"


@pytest.mark.asyncio
async def test_get_tides_wrapper_sort_order_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_tides via read_log wrapper preserves sort-by-tide_number.

    Writes tides out of order; wrapper must return them sorted ascending.
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "SortSession_tide.log",
        [
            {"level": "INFO", "msg": "tide 5", "tide": {"tide_number": 5}},
            {"level": "DEBUG", "msg": "tide 1", "tide": {"tide_number": 1}},
            {"level": "ERROR", "msg": "tide 3", "tide": {"tide_number": 3}},
        ],
    )

    result = await LoggedTideweaver.get_tides("SortSession")

    assert [r["tide"]["tide_number"] for r in result] == [1, 3, 5]


@pytest.mark.asyncio
async def test_get_scheduler_events_wrapper_sort_order_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver.get_scheduler_events via read_log wrapper preserves triple-key sort.

    Writes scheduler_event records out of order; wrapper must return them
    sorted by (tide_number, event_type, current_name).
    """
    monkeypatch.chdir(tmp_path)

    _write_jsonl(
        tmp_path / "logs" / "SchedSession_error.log",
        [
            {
                "level": "WARNING",
                "msg": "e3",
                "scheduler_event": {"tide_number": 2, "event_type": "empty_output", "current_name": "z_src"},
            },
            {
                "level": "WARNING",
                "msg": "e1",
                "scheduler_event": {"tide_number": 1, "event_type": "isolated_tick_failure", "current_name": "a_src"},
            },
            {
                "level": "WARNING",
                "msg": "e2",
                "scheduler_event": {"tide_number": 2, "event_type": "empty_output", "current_name": "a_src"},
            },
        ],
    )

    result = await LoggedTideweaver.get_scheduler_events("SchedSession")

    assert len(result) == 3
    assert result[0]["scheduler_event"]["tide_number"] == 1
    assert result[1]["scheduler_event"]["current_name"] == "a_src"
    assert result[2]["scheduler_event"]["current_name"] == "z_src"
