"""Unit tests for _route_tide_to_log, TideFilter, and the JSONFormatter 'tide' key."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List, Tuple
from unittest.mock import MagicMock, call, patch

import pytest

from incorporator.observability.logger import JSONFormatter, TideFilter, _route_tide_to_log
from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.tideweaver.reasons import SkipReason, WakeReason
from incorporator.observability.tideweaver.tide import Tide


def _make_tide(
    tide_number: int = 1,
    fired: List[str] | None = None,
    skipped: List[Tuple[str, SkipReason]] | None = None,
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
        wake_reason=WakeReason.TIMER,
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=canal_rejects_added,
        next_due_in_sec=None,
        timestamp=datetime.now(timezone.utc),
    )


def test_route_tide_no_op_pass_debug() -> None:
    """Empty fired + empty skipped + canal_rejects_added=0 routes to DEBUG via logger.log."""
    tide = _make_tide(fired=[], skipped=[], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.DEBUG


def test_route_tide_successful_pass_info() -> None:
    """A tide with fired currents and no errors routes to INFO via logger.log."""
    tide = _make_tide(fired=["prices"], skipped=[], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.INFO


def test_route_tide_canal_rejects_error() -> None:
    """canal_rejects_added > 0 routes to ERROR regardless of fired list."""
    tide = _make_tide(fired=["prices"], skipped=[], canal_rejects_added=2)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
    assert "2 canal reject(s)" in msg


def test_route_tide_surge_halted_error() -> None:
    """skipped containing ('name', 'surge_halted') routes to ERROR."""
    tide = _make_tide(fired=[], skipped=[("arb", SkipReason.SURGE_HALTED)], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
    assert "surge_halted" in msg


def test_route_tide_skip_ahead_error() -> None:
    """skipped containing ('name', 'skip_ahead') routes to ERROR."""
    tide = _make_tide(fired=[], skipped=[("arb", SkipReason.SKIP_AHEAD)], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
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
    tide = _make_tide(fired=[], skipped=[("b", SkipReason.NOT_DUE)], canal_rejects_added=0)
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_tide_to_log("TestLogger", tide)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    # 'not_due' + no fired → no-op pass → DEBUG
    assert level_arg == logging.DEBUG


def test_route_tide_sets_is_tide_in_extra() -> None:
    """_route_tide_to_log sets is_tide=True in the extra dict for all tide records.

    TideFilter routes records to tide.log by inspecting this flag; all tide
    severity branches (ERROR, INFO, DEBUG) must carry it.
    """
    for tide in [
        _make_tide(fired=[], skipped=[], canal_rejects_added=0),  # DEBUG branch
        _make_tide(fired=["prices"], skipped=[], canal_rejects_added=0),  # INFO branch
        _make_tide(fired=[], skipped=[], canal_rejects_added=1),  # ERROR branch
    ]:
        mock_logger = MagicMock(spec=logging.Logger)
        mock_logger.isEnabledFor.return_value = True

        with patch("logging.getLogger", return_value=mock_logger):
            _route_tide_to_log("TestLogger", tide)

        mock_logger.log.assert_called_once()
        extra = mock_logger.log.call_args[1]["extra"]
        assert extra.get("is_tide") is True, f"Missing is_tide=True for tide: {tide}"


def test_tide_filter_accepts_is_tide_true() -> None:
    """TideFilter.filter returns True when record.is_tide is True."""
    filt = TideFilter()
    record = logging.LogRecord(
        name="TestLogger",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="tide 1: no-op",
        args=(),
        exc_info=None,
    )
    record.is_tide = True  # type: ignore[attr-defined]
    assert filt.filter(record) is True


def test_tide_filter_rejects_missing_is_tide() -> None:
    """TideFilter.filter returns False when record has no is_tide attribute."""
    filt = TideFilter()
    record = logging.LogRecord(
        name="TestLogger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="wave chunk complete",
        args=(),
        exc_info=None,
    )
    assert filt.filter(record) is False


def test_tide_filter_rejects_is_tide_false() -> None:
    """TideFilter.filter returns False when record.is_tide is False."""
    filt = TideFilter()
    record = logging.LogRecord(
        name="TestLogger",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="chunk failed",
        args=(),
        exc_info=None,
    )
    record.is_tide = False  # type: ignore[attr-defined]
    assert filt.filter(record) is False
