"""Unit tests for _route_reject_to_log and the JSONFormatter 'reject' key."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from incorporator.observability.logger import JSONFormatter, _route_reject_to_log
from incorporator.rejects import RejectEntry


def _make_reject(
    source: str = "https://api.example.com/prices",
    error_kind: str = "HTTPStatusError",
    message: str = "429 Too Many Requests",
    from_name: str | None = None,
    to_name: str | None = None,
    host: str | None = None,
    status_code: int | None = None,
) -> RejectEntry:
    """Build a minimal RejectEntry for routing tests."""
    return RejectEntry.model_construct(
        source=source,
        error_kind=error_kind,
        message=message,
        retry_after=None,
        wave_index=1,
        from_name=from_name,
        to_name=to_name,
        host=host,
        status_code=status_code,
        attempt_number=None,
        duration_sec=None,
        cooldown_sec=None,
    )


def test_route_reject_http_error() -> None:
    """HTTPStatusError with status_code=429 routes to ERROR with '[HTTP 429]' in the message."""
    reject = _make_reject(
        source="https://api.example.com/prices",
        error_kind="HTTPStatusError",
        status_code=429,
        host="api.example.com",
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.error.assert_called_once()
    msg = mock_logger.error.call_args[0][0]
    assert "[HTTP 429]" in msg
    assert "HTTPStatusError" in msg


def test_route_reject_canal_with_edge() -> None:
    """PenstockLimited reject with from_name/to_name routes to ERROR with edge in message."""
    reject = _make_reject(
        source="ArbitrageFjord",
        error_kind="PenstockLimited",
        message="edge prices->arb: penstock_limited",
        from_name="prices",
        to_name="arb",
        host=None,
        status_code=None,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.error.assert_called_once()
    msg = mock_logger.error.call_args[0][0]
    assert "(prices->arb)" in msg
    assert "PenstockLimited" in msg


def test_route_reject_json_dump_has_reject_key() -> None:
    """JSONFormatter.format includes a top-level 'reject' key when the record carries reject extra."""
    reject = _make_reject(status_code=500, host="api.example.com")
    dump = reject.model_dump(mode="json")

    record = logging.LogRecord(
        name="TestLogger",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg="HTTPStatusError: https://api.example.com/prices [HTTP 500]",
        args=(),
        exc_info=None,
    )
    record.reject = dump  # type: ignore[attr-defined]
    record.meta = 'class:"TestLogger"'  # type: ignore[attr-defined]
    record.is_api = False  # type: ignore[attr-defined]

    formatter = JSONFormatter()
    output = formatter.format(record)
    parsed = json.loads(output)

    assert "reject" in parsed
    assert parsed["reject"]["error_kind"] == "HTTPStatusError"
    assert parsed["reject"]["status_code"] == 500


def test_route_reject_meta_has_all_fields() -> None:
    """The meta string passed to the log record contains all required fields."""
    reject = _make_reject(
        source="https://api.example.com/prices",
        error_kind="SurgeHalted",
        from_name="prices",
        to_name="arb",
        host="api.example.com",
        status_code=None,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("MyClass", reject)

    mock_logger.error.assert_called_once()
    extra = mock_logger.error.call_args[1]["extra"]
    meta = extra["meta"]

    assert 'class:"MyClass"' in meta
    assert 'source:"https://api.example.com/prices"' in meta
    assert 'error_kind:"SurgeHalted"' in meta
    assert 'from:"prices"' in meta
    assert 'to:"arb"' in meta
    assert 'host:"api.example.com"' in meta
    assert "status_code:None" in meta


def test_route_reject_no_edge_no_status() -> None:
    """Reject with no from_name and no status_code produces a clean message without edge or HTTP suffix."""
    reject = _make_reject(
        source="SomeClass",
        error_kind="GateBlocked",
        from_name=None,
        to_name=None,
        host=None,
        status_code=None,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.error.assert_called_once()
    msg = mock_logger.error.call_args[0][0]
    assert "GateBlocked: SomeClass" == msg
