"""Integration tests for the non-blocking QueueHandler Observability Engine."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from incorporator import LoggedIncorporator, setup_class_logger
from incorporator.observability.logger import (
    _ACTIVE_LISTENERS,
    JSONFormatter,
    _cleanup_listeners,
)


# Isolate mock classes so tests don't share the global _ACTIVE_LISTENERS state
class MockAPIEndpoint1(LoggedIncorporator):
    pass


class MockAPIEndpoint2(LoggedIncorporator):
    pass


@pytest.mark.asyncio
async def test_multiplex_file_routing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """Proves QueueHandler creates 3 files, writes JSONL, and routes traffic via Filters."""

    monkeypatch.chdir(tmp_path)
    setup_class_logger(MockAPIEndpoint1)

    # UPDATED: Use inc_code and inc_name to match the refactored base API
    obj = MockAPIEndpoint1(inc_code=99, inc_name="TestInstance")

    obj.log_info("Standard info trace")
    obj.log_error("Standard error trace")
    obj.log_api("Web traffic trace")

    # EXPLICIT THREAD FLUSH: Stop the listener to synchronously flush the queue to the disk
    _ACTIVE_LISTENERS["MockAPIEndpoint1"].stop()

    # Path assertions updated to look inside the dedicated 'logs/' directory
    debug_file = tmp_path / "logs" / "MockAPIEndpoint1_debug.log"
    error_file = tmp_path / "logs" / "MockAPIEndpoint1_error.log"
    api_file = tmp_path / "logs" / "MockAPIEndpoint1_api.log"

    assert debug_file.exists()
    assert error_file.exists()
    assert api_file.exists()

    error_text = error_file.read_text(encoding="utf-8")
    api_text = api_file.read_text(encoding="utf-8")

    assert "Standard info trace" in error_text
    assert "Standard error trace" in error_text
    assert "Web traffic trace" not in error_text

    assert "Web traffic trace" in api_text
    assert "Standard error trace" not in api_text


@pytest.mark.asyncio
async def test_cleanup_listeners_stops_all(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """_cleanup_listeners() must stop every active QueueListener and clear the registry."""
    monkeypatch.chdir(tmp_path)

    class CleanupTarget(LoggedIncorporator):
        pass

    setup_class_logger(CleanupTarget)
    assert "CleanupTarget" in _ACTIVE_LISTENERS

    _cleanup_listeners()

    assert _ACTIVE_LISTENERS == {}


def test_setup_class_logger_duplicate_listener_guard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """Calling setup_class_logger twice for the same class must be a no-op on the second call."""
    monkeypatch.chdir(tmp_path)

    class DuplicateTarget(LoggedIncorporator):
        pass

    setup_class_logger(DuplicateTarget)
    listener_before = _ACTIVE_LISTENERS["DuplicateTarget"]

    setup_class_logger(DuplicateTarget)  # second call — must return early
    assert _ACTIVE_LISTENERS["DuplicateTarget"] is listener_before  # same object


def test_setup_class_logger_max_threads_eviction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """When MAX_LOG_THREADS is reached, the oldest listener must be evicted."""
    import incorporator.observability.logger as logger_module

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(logger_module, "MAX_LOG_THREADS", 2)

    class EvictA(LoggedIncorporator):
        pass

    class EvictB(LoggedIncorporator):
        pass

    class EvictC(LoggedIncorporator):
        pass

    setup_class_logger(EvictA)
    setup_class_logger(EvictB)
    # Both A and B registered — now at the limit
    assert len(_ACTIVE_LISTENERS) >= 2

    setup_class_logger(EvictC)  # must evict EvictA (the oldest)

    assert "EvictC" in _ACTIVE_LISTENERS
    assert "EvictA" not in _ACTIVE_LISTENERS


def test_safe_log_filename_default_uses_relative_logs_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``INCORPORATOR_LOG_DIR`` unset → logs land in ``./logs`` relative to CWD."""
    from incorporator.observability.logger import _safe_log_filename

    monkeypatch.delenv("INCORPORATOR_LOG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    path = _safe_log_filename("MyClass", "error.log")
    resolved = Path(path).resolve()
    assert resolved.parent.name == "logs"
    assert resolved.parent == (tmp_path / "logs").resolve()
    assert resolved.parent.exists()


def test_safe_log_filename_honours_env_var_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``INCORPORATOR_LOG_DIR`` set → logs land at that absolute path.

    Senior-review pass-2 finding M-DOC2: containerised deployments
    (ECS / CloudRun / K8s) need a way to redirect logs to a mounted
    volume or stdout-collector path without changing the working dir.
    """
    from incorporator.observability.logger import _safe_log_filename

    target = tmp_path / "container_logs"
    monkeypatch.setenv("INCORPORATOR_LOG_DIR", str(target))

    path = _safe_log_filename("MyClass", "error.log")
    resolved = Path(path).resolve()
    assert resolved.parent == target.resolve()
    assert resolved.parent.exists()  # created lazily
    assert resolved.name == "MyClass_error.log"


def test_json_formatter_includes_exc_info() -> None:
    """JSONFormatter.format must include 'exc_info' when a record carries exception info."""
    formatter = JSONFormatter()
    try:
        raise ValueError("test error")
    except ValueError:
        import sys

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="something broke",
            args=(),
            exc_info=sys.exc_info(),
        )

    output = formatter.format(record)
    import json

    parsed = json.loads(output)
    assert "exc_info" in parsed
    assert "ValueError" in parsed["exc_info"]


def test_log_cls_info_and_error_callable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """log_cls_info and log_cls_error must emit records when the logger is enabled."""
    monkeypatch.chdir(tmp_path)

    class CLS(LoggedIncorporator):
        pass

    setup_class_logger(CLS)

    # These call through to the logger without raising
    CLS.log_cls_info("info message from cls")
    CLS.log_cls_error("error message from cls")

    # Stop the listener to flush to disk
    _ACTIVE_LISTENERS["CLS"].stop()

    debug_log = tmp_path / "logs" / "CLS_debug.log"
    assert debug_log.exists()
    content = debug_log.read_text(encoding="utf-8")
    assert "info message from cls" in content
    assert "error message from cls" in content


def test_log_instance_methods_callable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """log_debug, log_info, log_error, log_api must all emit records without raising."""
    monkeypatch.chdir(tmp_path)

    class InstanceLog(LoggedIncorporator):
        pass

    setup_class_logger(InstanceLog)
    obj = InstanceLog(inc_code=1, inc_name="test")

    obj.log_debug("debug msg")
    obj.log_info("info msg")
    obj.log_error("error msg")
    obj.log_api("api msg")

    _ACTIVE_LISTENERS["InstanceLog"].stop()

    debug_log = tmp_path / "logs" / "InstanceLog_debug.log"
    assert debug_log.exists()
    content = debug_log.read_text(encoding="utf-8")
    assert "debug msg" in content
    assert "info msg" in content


@pytest.mark.asyncio
async def test_logged_incorp_enable_logging_registers_listener(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """LoggedIncorporator.incorp with enable_logging=True must register a QueueListener."""
    monkeypatch.chdir(tmp_path)

    class LogIncModel(LoggedIncorporator):
        id: int = 0

    data_file = tmp_path / "data.json"
    data_file.write_text('[{"id": 1}]', encoding="utf-8")

    result = await LogIncModel.incorp(inc_file=str(data_file), enable_logging=True)

    assert "LogIncModel" in _ACTIVE_LISTENERS
    assert result is not None


@pytest.mark.asyncio
async def test_logged_export_enable_logging(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """LoggedIncorporator.export with enable_logging=True must log start and completion."""
    monkeypatch.chdir(tmp_path)

    class ExportLogModel(LoggedIncorporator):
        id: int = 0

    data_file = tmp_path / "data.json"
    data_file.write_text('[{"id": 1}]', encoding="utf-8")
    out_file = tmp_path / "out.json"

    dataset = await ExportLogModel.incorp(inc_file=str(data_file))
    await ExportLogModel.export(instance=dataset, file_path=str(out_file), enable_logging=True)

    # Export must complete without error; log file created on enable_logging path
    assert out_file.exists()


@pytest.mark.asyncio
async def test_logged_stream_enable_logging_emits_waves(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """stream() with enable_logging=True must yield Waves and log chunk metrics."""
    monkeypatch.chdir(tmp_path)

    class StreamLogModel(LoggedIncorporator):
        id: int = 0

    data_file = tmp_path / "data.json"
    data_file.write_text('[{"id": 1}, {"id": 2}]', encoding="utf-8")

    results = []
    async for wave in StreamLogModel.stream(
        incorp_params={"inc_file": str(data_file)},
        refresh_params=None,                          # one-shot: skip refresh daemon
        enable_logging=True,
    ):
        results.append(wave)

    assert len(results) >= 1
    assert results[0].rows_processed >= 1
    assert "StreamLogModel" in _ACTIVE_LISTENERS


@pytest.mark.asyncio
async def test_logged_stream_exception_logs_and_reraises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """A fatal exception inside stream() with enable_logging=True must be re-raised."""
    monkeypatch.chdir(tmp_path)

    class StreamErrModel(LoggedIncorporator):
        id: int = 0

    with patch.object(LoggedIncorporator, "stream", side_effect=RuntimeError("pipeline crash")):
        with pytest.raises(RuntimeError, match="pipeline crash"):
            async for _ in LoggedIncorporator.stream(  # type: ignore[attr-defined]
                incorp_params={}, enable_logging=True
            ):
                pass


@pytest.mark.asyncio
async def test_get_error_async_reader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """Proves the async get_error() classmethod successfully parses JSON Lines."""

    monkeypatch.chdir(tmp_path)
    setup_class_logger(MockAPIEndpoint2)

    # UPDATED: Use inc_code and inc_name to match the refactored base API
    obj = MockAPIEndpoint2(inc_code=99, inc_name="TestInstance")

    obj.log_error("Disk read test")

    # EXPLICIT THREAD FLUSH: Ensure the background thread writes to disk before we read
    _ACTIVE_LISTENERS["MockAPIEndpoint2"].stop()

    errors = await MockAPIEndpoint2.get_error()

    assert isinstance(errors, list)
    assert len(errors) >= 1

    last_error = errors[-1]
    assert last_error["level"] == "ERROR"
    assert last_error["msg"] == "Disk read test"
    assert 'class:"MockAPIEndpoint2"' in last_error["meta"]


# ===========================================================================
# AUDIT ↔ LOG_META / GET_ERROR INTEGRATION
# ===========================================================================


def test_wave_log_meta_shape() -> None:
    """Wave.log_meta() exposes the wave's fields in a flat key:value form."""
    from incorporator.observability.logger import Wave

    wave = Wave(
        chunk_index=3,
        operation="chunk",
        rows_processed=42,
        processing_time_sec=1.234,
        failed_sources=["x", "y"],
    )
    meta = wave.log_meta()
    assert 'operation:"chunk"' in meta
    assert "chunk_index:3" in meta
    assert "rows:42" in meta
    assert "time_sec:1.234" in meta
    assert "failed:2" in meta


def test_redact_scrubs_query_string_secrets() -> None:
    """_redact replaces query-string auth values with ***REDACTED***."""
    from incorporator.observability.logger import _redact

    url = "https://api.example.com/v1/users?api_key=abc123&token=xyz789&page=2"
    out = _redact(url)
    assert "abc123" not in out
    assert "xyz789" not in out
    assert "***REDACTED***" in out
    # Non-secret query params survive.
    assert "page=2" in out


def test_redact_is_noop_on_clean_strings() -> None:
    from incorporator.observability.logger import _redact

    assert _redact("https://api.example.com/v1/users") == "https://api.example.com/v1/users"
    assert _redact("") == ""


@pytest.mark.asyncio
async def test_route_wave_to_log_writes_structured_wave_to_get_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """A Wave with failed_sources lands in error.log under a structured 'wave' key.

    The Pydantic dump rides on the log record under the ``wave`` key, so
    callers of ``Class.get_error()`` can read structured data directly.
    """
    monkeypatch.chdir(tmp_path)
    from incorporator.observability.logger import Wave, _route_wave_to_log

    class WaveLogModel(LoggedIncorporator):
        pass

    setup_class_logger(WaveLogModel)

    wave = Wave(
        chunk_index=1,
        operation="chunk",
        rows_processed=10,
        processing_time_sec=0.5,
        failed_sources=["https://dead.example.com/x?api_key=should_be_redacted"],
    )
    _route_wave_to_log(WaveLogModel, wave)

    # Flush the queue to disk so get_error can read.
    _ACTIVE_LISTENERS["WaveLogModel"].stop()

    errors = await WaveLogModel.get_error()
    assert errors, "expected the wave failure to land in error.log"

    record = errors[-1]
    # The structured wave dump should be on the record under "wave".
    assert "wave" in record
    wave_dump = record["wave"]
    assert wave_dump["chunk_index"] == 1
    assert wave_dump["rows_processed"] == 10
    # Failed sources were redacted before being written.
    assert any("***REDACTED***" in s for s in wave_dump["failed_sources"])
    assert all("should_be_redacted" not in s for s in wave_dump["failed_sources"])
    # log_meta() summary on the record.
    assert "operation:" in record["meta"]


def test_route_wave_to_log_skips_zero_row_no_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reset_active_listeners: None
) -> None:
    """A zero-row, zero-failure wave is treated as no-op (nothing logged)."""
    monkeypatch.chdir(tmp_path)
    from incorporator.observability.logger import Wave, _route_wave_to_log

    class QuietModel(LoggedIncorporator):
        pass

    setup_class_logger(QuietModel)

    _route_wave_to_log(
        QuietModel,
        Wave(chunk_index=1, operation="chunk", rows_processed=0, processing_time_sec=0.01),
    )

    _ACTIVE_LISTENERS["QuietModel"].stop()

    info_log = tmp_path / "logs" / "QuietModel_api.log"
    error_log = tmp_path / "logs" / "QuietModel_error.log"
    # Neither path should have been triggered by this no-op wave.
    assert not info_log.exists() or info_log.read_text(encoding="utf-8").strip() == ""
    assert not error_log.exists() or error_log.read_text(encoding="utf-8").strip() == ""
