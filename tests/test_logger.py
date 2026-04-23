"""Integration tests for the non-blocking QueueHandler Observability Engine."""

from pathlib import Path

import pytest

from incorporator import LoggedIncorporator, setup_class_logger
from incorporator.methods.logger import _ACTIVE_LISTENERS


# Isolate mock classes so tests don't share the global _ACTIVE_LISTENERS state
class MockAPIEndpoint1(LoggedIncorporator):
    pass


class MockAPIEndpoint2(LoggedIncorporator):
    pass


@pytest.mark.asyncio
async def test_multiplex_file_routing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Proves QueueHandler creates 3 files, writes JSONL, and routes traffic via Filters."""

    monkeypatch.chdir(tmp_path)
    setup_class_logger(MockAPIEndpoint1)
    obj = MockAPIEndpoint1(code=99, name="TestInstance")

    obj.log_info("Standard info trace")
    obj.log_error("Standard error trace")
    obj.log_api("Web traffic trace")

    # EXPLICIT THREAD FLUSH: Stop the listener to synchronously flush the queue to the disk
    _ACTIVE_LISTENERS["MockAPIEndpoint1"].stop()

    debug_file = tmp_path / "MockAPIEndpoint1_debug.log"
    error_file = tmp_path / "MockAPIEndpoint1_error.log"
    api_file = tmp_path / "MockAPIEndpoint1_api.log"

    assert debug_file.exists()
    assert error_file.exists()
    assert api_file.exists()

    error_text = error_file.read_text(encoding='utf-8')
    api_text = api_file.read_text(encoding='utf-8')

    assert "Standard info trace" in error_text
    assert "Standard error trace" in error_text
    assert "Web traffic trace" not in error_text

    assert "Web traffic trace" in api_text
    assert "Standard error trace" not in api_text


@pytest.mark.asyncio
async def test_get_error_async_reader(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Proves the async getError() classmethod successfully parses JSON Lines."""

    monkeypatch.chdir(tmp_path)
    setup_class_logger(MockAPIEndpoint2)
    obj = MockAPIEndpoint2(code=99, name="TestInstance")

    obj.log_error("Disk read test")

    # EXPLICIT THREAD FLUSH: Ensure the background thread writes to disk before we read
    _ACTIVE_LISTENERS["MockAPIEndpoint2"].stop()

    errors = await MockAPIEndpoint2.getError()

    assert isinstance(errors, list)
    assert len(errors) >= 1

    last_error = errors[-1]
    assert last_error["level"] == "ERROR"
    assert last_error["msg"] == "Disk read test"
    assert "class:\"MockAPIEndpoint2\"" in last_error["meta"]