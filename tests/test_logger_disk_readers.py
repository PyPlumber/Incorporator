"""Tests for LoggingMixin.get_rejects and LoggedTideweaver.get_tides / get_rejects."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from incorporator import LoggedIncorporator, setup_class_logger
from incorporator.observability.logger import (
    _ACTIVE_LISTENERS,
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
            {"level": "ERROR", "msg": "canal reject", "reject": {"source": "ArbitrageFjord", "error_kind": "PenstockLimited"}},
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
async def test_get_tides_reads_error_and_debug_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_tides() merges tide records from both error.log and debug.log.

    Tides at INFO/ERROR level land in error.log; no-op DEBUG tides land
    in debug.log.  get_tides() must return a combined, deduped, ascending
    list of all tide records.
    """
    monkeypatch.chdir(tmp_path)

    error_log = tmp_path / "logs" / "TidesSession_error.log"
    debug_log = tmp_path / "logs" / "TidesSession_debug.log"

    _write_jsonl(
        error_log,
        [
            {"level": "INFO", "msg": "tide 1", "tide": {"tide_number": 1, "fired": ["prices"]}},
            {"level": "ERROR", "msg": "tide 3", "tide": {"tide_number": 3, "canal_rejects_added": 1}},
        ],
    )
    _write_jsonl(
        debug_log,
        [
            {"level": "DEBUG", "msg": "tide 2", "tide": {"tide_number": 2, "fired": []}},
            # Duplicate of tide 1 — deduped; debug.log entry wins because it is
            # appended after error.log in the merge list, so the dict update lands last.
            {"level": "DEBUG", "msg": "tide 1 dup", "tide": {"tide_number": 1, "fired": [], "dup": True}},
        ],
    )

    result = await LoggedTideweaver.get_tides("TidesSession")

    tide_numbers = [r["tide"]["tide_number"] for r in result]
    # Sorted ascending, deduped — 3 unique tides.
    assert tide_numbers == [1, 2, 3]
    # Tide 1 duplicate: debug.log entry is last in the all_records list, so it wins.
    assert result[0]["tide"].get("dup") is True


@pytest.mark.asyncio
async def test_get_tides_empty_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_tides() returns [] when neither log file exists."""
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
