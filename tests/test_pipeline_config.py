"""Unit tests for the Pydantic StreamConfig + FjordConfig models.

These cover the schema in isolation (no CLI, no sidecar import).  The
matching CLI integration is exercised by ``tests/test_cli_validate.py``
and the broader stream / fjord pipeline tests, which keep running
unchanged in D2a.
"""

from __future__ import annotations

from typing import Any, Dict

import pytest
from pydantic import ValidationError

from incorporator.cli._pipeline_config import (
    FjordConfig,
    StreamConfig,
    parse_pipeline_config,
)

# ---------------------------------------------------------------------------
# StreamConfig
# ---------------------------------------------------------------------------


def test_stream_minimum_happy_path() -> None:
    """A bare config with one source key validates cleanly."""
    cfg = StreamConfig.model_validate({"incorp_params": {"inc_url": "https://x"}})
    assert cfg.incorp_params["inc_url"] == "https://x"
    assert cfg.stateful_polling is False


def test_stream_kitchen_sink_happy_path() -> None:
    """Every documented optional field populated validates cleanly."""
    data: Dict[str, Any] = {
        "incorp_params": {"inc_url": "https://x"},
        "refresh_params": {"refresh_url": "https://y"},
        "export_params": {"file_path": "out.ndjson"},
        "poll_interval": 30.0,
        "refresh_interval": 60.0,
        "export_interval": {"MyClass": 120.0},
        "stateful_polling": True,
        "inflow": "inflow.py",
        "outflow": "outflow.py",
    }
    cfg = StreamConfig.model_validate(data)
    assert cfg.stateful_polling is True
    assert cfg.export_interval == {"MyClass": 120.0}


def test_stream_rejects_missing_incorp_params() -> None:
    """``incorp_params`` is required at the top level."""
    with pytest.raises(ValidationError, match="incorp_params"):
        StreamConfig.model_validate({})


def test_stream_rejects_incorp_params_without_source_key() -> None:
    """``incorp_params`` must contain at least one of the recognised source keys."""
    with pytest.raises(ValidationError, match="source key"):
        StreamConfig.model_validate({"incorp_params": {"inc_code": "id"}})


def test_stream_rejects_non_dict_refresh_params() -> None:
    """``refresh_params``, if present, must be a JSON object."""
    with pytest.raises(ValidationError, match="refresh_params"):
        StreamConfig.model_validate({"incorp_params": {"inc_url": "x"}, "refresh_params": "nope"})


def test_stream_rejects_non_dict_export_params() -> None:
    """``export_params``, if present, must be a JSON object."""
    with pytest.raises(ValidationError, match="export_params"):
        StreamConfig.model_validate({"incorp_params": {"inc_url": "x"}, "export_params": "nope"})


def test_stream_rejects_string_poll_interval() -> None:
    """``poll_interval`` must be numeric — strings are rejected."""
    with pytest.raises(ValidationError, match="poll_interval"):
        StreamConfig.model_validate({"incorp_params": {"inc_url": "x"}, "poll_interval": "soon"})


def test_stream_accepts_dict_refresh_interval() -> None:
    """``refresh_interval`` as a dict of ``{class_name: seconds}`` is valid."""
    cfg = StreamConfig.model_validate(
        {
            "incorp_params": {"inc_url": "x"},
            "refresh_interval": {"Alpha": 30.0, "Beta": 60.0},
        }
    )
    assert cfg.refresh_interval == {"Alpha": 30.0, "Beta": 60.0}


def test_stream_rejects_dict_refresh_interval_non_numeric_value() -> None:
    """Per-source ``refresh_interval`` dict values must be numeric."""
    with pytest.raises(ValidationError, match="refresh_interval"):
        StreamConfig.model_validate(
            {
                "incorp_params": {"inc_url": "x"},
                "refresh_interval": {"Alpha": "soon"},
            }
        )


def test_stream_rejects_outflow_without_stateful_polling() -> None:
    """``outflow`` on stream requires ``stateful_polling=True``."""
    with pytest.raises(ValidationError, match="stateful_polling"):
        StreamConfig.model_validate({"incorp_params": {"inc_url": "x"}, "outflow": "outflow.py"})


def test_stream_accepts_outflow_with_stateful_polling() -> None:
    """``outflow`` + ``stateful_polling=True`` is the documented happy path."""
    cfg = StreamConfig.model_validate(
        {
            "incorp_params": {"inc_url": "x"},
            "outflow": "outflow.py",
            "stateful_polling": True,
        }
    )
    assert cfg.outflow == "outflow.py"


# ---------------------------------------------------------------------------
# FjordConfig
# ---------------------------------------------------------------------------


_FJORD_OK: Dict[str, Any] = {
    "outflow": "outflow.py",
    "stream_params": [{"cls_name": "A", "incorp_params": {"inc_url": "https://x"}}],
    "export_params": {"file_path": "out.ndjson"},
}


def test_fjord_happy_path() -> None:
    """A minimum fjord config (outflow + one stream + export_params) validates."""
    cfg = FjordConfig.model_validate(_FJORD_OK)
    assert cfg.outflow == "outflow.py"
    assert cfg.stream_params[0].cls_name == "A"


def test_fjord_rejects_missing_outflow() -> None:
    """``outflow`` is a required string."""
    data = {k: v for k, v in _FJORD_OK.items() if k != "outflow"}
    with pytest.raises(ValidationError, match="outflow"):
        FjordConfig.model_validate(data)


def test_fjord_rejects_empty_stream_params() -> None:
    """``stream_params`` must contain at least one entry."""
    data = {**_FJORD_OK, "stream_params": []}
    with pytest.raises(ValidationError, match="stream_params"):
        FjordConfig.model_validate(data)


def test_fjord_rejects_non_list_stream_params() -> None:
    """``stream_params`` must be a list, not a dict."""
    data = {**_FJORD_OK, "stream_params": {"not": "a list"}}
    with pytest.raises(ValidationError, match="stream_params"):
        FjordConfig.model_validate(data)


def test_fjord_rejects_missing_export_params() -> None:
    """``export_params`` is a required dict."""
    data = {k: v for k, v in _FJORD_OK.items() if k != "export_params"}
    with pytest.raises(ValidationError, match="export_params"):
        FjordConfig.model_validate(data)


def test_fjord_entry_rejects_missing_cls_name() -> None:
    """Each ``stream_params`` entry must declare ``cls_name``."""
    data = {
        **_FJORD_OK,
        "stream_params": [{"incorp_params": {"inc_url": "https://x"}}],
    }
    with pytest.raises(ValidationError, match="cls_name"):
        FjordConfig.model_validate(data)


def test_fjord_entry_rejects_missing_incorp_params() -> None:
    """Each ``stream_params`` entry must declare ``incorp_params`` (dict)."""
    data = {**_FJORD_OK, "stream_params": [{"cls_name": "A"}]}
    with pytest.raises(ValidationError, match="incorp_params"):
        FjordConfig.model_validate(data)


def test_fjord_entry_accepts_optional_per_source_overrides() -> None:
    """Per-source ``refresh_params`` and ``export_params`` are optional but allowed."""
    data = {
        **_FJORD_OK,
        "stream_params": [
            {
                "cls_name": "A",
                "incorp_params": {"inc_url": "https://x"},
                "refresh_params": {"refresh_url": "y"},
                "export_params": {"file_path": "per-source.ndjson"},
            }
        ],
    }
    cfg = FjordConfig.model_validate(data)
    assert cfg.stream_params[0].refresh_params == {"refresh_url": "y"}


# ---------------------------------------------------------------------------
# dispatcher
# ---------------------------------------------------------------------------


def test_parse_pipeline_config_dispatches_stream() -> None:
    """``kind='stream'`` returns a :class:`StreamConfig` instance."""
    result = parse_pipeline_config({"incorp_params": {"inc_url": "x"}}, kind="stream")
    assert isinstance(result, StreamConfig)


def test_parse_pipeline_config_dispatches_fjord() -> None:
    """``kind='fjord'`` returns a :class:`FjordConfig` instance."""
    result = parse_pipeline_config(_FJORD_OK, kind="fjord")
    assert isinstance(result, FjordConfig)
