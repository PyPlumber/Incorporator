"""Unit tests for _route_reject_to_log, _route_to_log (RejectEntry), and the JSONFormatter 'reject' key."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from incorporator.observability.logger import JSONFormatter, _route_reject_to_log, _route_to_log
from incorporator.rejects import RejectEntry


def _make_reject(
    source: str = "https://api.example.com/prices",
    error_kind: str = "HTTPStatusError",
    message: str = "429 Too Many Requests",
    from_name: str | None = None,
    to_name: str | None = None,
    host: str | None = None,
    status_code: int | None = None,
    is_url_traffic_error: bool = False,
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
        is_url_traffic_error=is_url_traffic_error,
    )


def test_route_reject_http_error() -> None:
    """HTTPStatusError with status_code=429 routes to ERROR with '[HTTP 429 Too Many Requests]' in the message."""
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

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
    assert "[HTTP 429 Too Many Requests]" in msg
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

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
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
    record._payload_key = "reject"  # type: ignore[attr-defined]
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

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    extra = mock_logger.log.call_args[1]["extra"]
    meta = extra["meta"]

    assert 'class:"MyClass"' in meta
    assert 'source:"https://api.example.com/prices"' in meta
    assert 'error_kind:"SurgeHalted"' in meta
    assert 'from:"prices"' in meta
    assert 'to:"arb"' in meta
    assert 'host:"api.example.com"' in meta
    assert "status_code:None" in meta


def test_route_reject_no_edge_no_status() -> None:
    """Reject with no from_name, no status_code, and no message renders as 'kind: source'."""
    reject = _make_reject(
        source="SomeClass",
        error_kind="GateBlocked",
        message="",
        from_name=None,
        to_name=None,
        host=None,
        status_code=None,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.log.assert_called_once()
    level_arg = mock_logger.log.call_args[0][0]
    assert level_arg == logging.ERROR
    msg = mock_logger.log.call_args[0][1]
    assert msg == "GateBlocked: SomeClass"


def test_route_reject_url_traffic_error_passes_is_api_true() -> None:
    """A URL-traffic reject (is_url_traffic_error=True) routes with is_api=True.

    Proves that _route_reject_to_log forwards is_url_traffic_error to
    _emit_payload as is_api=True, so the APIFilter routes the record to
    api.log rather than error.log.
    """
    reject = _make_reject(
        source="https://api.example.com/data",
        error_kind="ReadTimeout",
        message="timed out",
        host="api.example.com",
        is_url_traffic_error=True,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.log.assert_called_once()
    extra = mock_logger.log.call_args[1]["extra"]
    assert extra["is_api"] is True


def test_route_reject_format_error_passes_is_api_false() -> None:
    """A parse/format reject (is_url_traffic_error=False) routes with is_api=False.

    Proves that IncorporatorFormatError rejects, canal-layer skips, and other
    non-URL-traffic failures are not forwarded to api.log — they stay in
    error.log via the default is_api=False path.
    """
    reject = _make_reject(
        source="https://api.example.com/malformed",
        error_kind="IncorporatorFormatError",
        message="JSON parse error",
        host="api.example.com",
        is_url_traffic_error=False,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_reject_to_log("TestLogger", reject)

    mock_logger.log.assert_called_once()
    extra = mock_logger.log.call_args[1]["extra"]
    assert extra["is_api"] is False


# ---------------------------------------------------------------------------
# _route_to_log dispatch — RejectEntry
# ---------------------------------------------------------------------------


def test_route_to_log_reject_url_traffic_is_api_true() -> None:
    """_route_to_log with a URL-traffic RejectEntry produces is_api=True, matching the legacy wrapper.

    Proves that the unified dispatcher forwards is_url_traffic_error → is_api
    identically to _route_reject_to_log so the APIFilter routes the record to
    api.log.
    """
    reject = _make_reject(
        source="https://api.example.com/data",
        error_kind="ReadTimeout",
        message="timed out",
        host="api.example.com",
        is_url_traffic_error=True,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_to_log("TestLogger", reject)

    mock_logger.log.assert_called_once()
    extra = mock_logger.log.call_args[1]["extra"]
    assert extra["is_api"] is True
    assert extra["reject"]["error_kind"] == "ReadTimeout"


def test_route_to_log_reject_format_error_is_api_false() -> None:
    """_route_to_log with a non-URL-traffic RejectEntry produces is_api=False.

    Mirrors test_route_reject_format_error_passes_is_api_false but calls the
    unified dispatcher directly to prove behavior parity.
    """
    reject = _make_reject(
        source="https://api.example.com/malformed",
        error_kind="IncorporatorFormatError",
        message="JSON parse error",
        host="api.example.com",
        is_url_traffic_error=False,
    )
    mock_logger = MagicMock(spec=logging.Logger)
    mock_logger.isEnabledFor.return_value = True

    with patch("logging.getLogger", return_value=mock_logger):
        _route_to_log("TestLogger", reject)

    mock_logger.log.assert_called_once()
    extra = mock_logger.log.call_args[1]["extra"]
    assert extra["is_api"] is False


def test_route_to_log_reject_meta_shape() -> None:
    """_route_to_log with a RejectEntry produces the same meta fields as the legacy wrapper.

    Asserts that class/source/error_kind/from/to/host/status_code all appear
    in the meta string emitted by the unified dispatcher.
    """
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
        _route_to_log("MyClass", reject)

    mock_logger.log.assert_called_once()
    extra = mock_logger.log.call_args[1]["extra"]
    meta = extra["meta"]

    assert 'class:"MyClass"' in meta
    assert 'source:"https://api.example.com/prices"' in meta
    assert 'error_kind:"SurgeHalted"' in meta
    assert 'from:"prices"' in meta
    assert 'to:"arb"' in meta
    assert 'host:"api.example.com"' in meta
    assert "status_code:None" in meta
