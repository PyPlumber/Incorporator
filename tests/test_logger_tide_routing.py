"""Unit tests for _route_tide_to_log and the JSONFormatter 'tide' key."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List, Tuple
from unittest.mock import MagicMock, patch

import pytest

from incorporator.observability.logger import JSONFormatter, _route_tide_to_log
from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.tideweaver.tide import Tide


def _make_tide(
    tide_number: int = 1,
    fired: List[str] | None = None,
    skipped: List[Tuple[str, str]] | None = None,
    canal_rejects_added: int = 0,
    duration_sec: float = 0.05,
) -> Tide:
    """Build a minimal Tide for routing tests."""
    return Tide.model_construct(
        tide_number=tide_number,
        fired=fired or [],
        skipped=skipped or [],
        current_outcomes=[],
        duration_sec=duration_sec,
        wake_reason="timer",
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=canal_rejects_added,
        next_due_in_sec=None,
        timestamp=datetime.now(timezone.utc),
    )


def test_route_tide_no_op_pass_debug() -> None:
    """Empty fired + empty skipped + canal_rejects_added=0 routes to DEBUG."""
    tide = _make_tide(fired=[], skipped=[], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.debug.assert_called_once()
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()


def test_route_tide_successful_pass_info() -> None:
    """A tide with fired currents and no errors routes to INFO."""
    tide = _make_tide(fired=["prices"], skipped=[], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.info.assert_called_once()
    mock_logger.error.assert_not_called()
    mock_logger.debug.assert_not_called()


def test_route_tide_canal_rejects_error() -> None:
    """canal_rejects_added > 0 routes to ERROR regardless of fired list."""
    tide = _make_tide(fired=["prices"], skipped=[], canal_rejects_added=2)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.error.assert_called_once()
    mock_logger.info.assert_not_called()
    msg = mock_logger.error.call_args[0][0]
    assert "2 canal reject(s)" in msg


def test_route_tide_surge_halted_error() -> None:
    """skipped containing ('name', 'surge_halted') routes to ERROR."""
    tide = _make_tide(fired=[], skipped=[("arb", "surge_halted")], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.error.assert_called_once()
    mock_logger.info.assert_not_called()
    msg = mock_logger.error.call_args[0][0]
    assert "surge_halted" in msg


def test_route_tide_skip_ahead_error() -> None:
    """skipped containing ('name', 'skip_ahead') routes to ERROR."""
    tide = _make_tide(fired=[], skipped=[("arb", "skip_ahead")], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.error.assert_called_once()
    msg = mock_logger.error.call_args[0][0]
    assert "skip_ahead" in msg


def test_route_tide_json_dump_has_tide_key() -> None:
    """JSONFormatter.format includes a top-level 'tide' key when the record carries tide extra."""
    tide = _make_tide(fired=["a"], canal_rejects_added=0)
    dump = tide.model_dump(mode="json")

    record = logging.LogRecord(
        name="TestLogger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="tide 1: fired 1",
        args=(),
        exc_info=None,
    )
    record.tide = dump  # type: ignore[attr-defined]
    record.meta = tide.log_meta()  # type: ignore[attr-defined]
    record.is_api = False  # type: ignore[attr-defined]

    formatter = JSONFormatter()
    output = formatter.format(record)
    parsed = json.loads(output)

    assert "tide" in parsed
    assert parsed["tide"]["tide_number"] == 1
    assert parsed["tide"]["fired"] == ["a"]


def test_route_tide_not_due_skip_is_not_error() -> None:
    """'not_due' skip reason is normal gating and must NOT route to ERROR."""
    tide = _make_tide(fired=[], skipped=[("b", "not_due")], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.error.assert_not_called()
    # 'not_due' + no fired → no-op pass → DEBUG
    mock_logger.debug.assert_called_once()
