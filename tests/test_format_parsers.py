"""tests/test_format_parsers.py"""

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import parse_source_data


@pytest.mark.asyncio
async def test_json_parsing_with_rust_speedups():
    """Tests the ultra-fast orjson path (assumes pip install incorporator[speedups])."""
    data = b'{"hello": "world"}'
    result = await parse_source_data(data, FormatType.JSON)
    assert isinstance(result, dict)
    assert result == {"hello": "world"}
    assert isinstance(result["hello"], str)


@pytest.mark.asyncio
async def test_json_parsing_standard_fallback(mock_no_speedups):
    """Tests the graceful degradation standard `json` path."""
    data = b'{"hello": "world"}'

    # Because of the fixture, orjson will throw an ImportError natively!
    result = await parse_source_data(data, FormatType.JSON)
    assert isinstance(result, dict)
    assert result == {"hello": "world"}
    assert isinstance(result["hello"], str)
    # Equivalence with the orjson path — both code paths must produce identical output
    assert list(result.keys()) == ["hello"]


def test_deserialize_nested_orjson_path() -> None:
    """deserialize_nested round-trips JSON cells via orjson when available."""
    from incorporator.io.formats import deserialize_nested

    assert deserialize_nested('{"a": 1}') == {"a": 1}
    assert deserialize_nested("[1, 2, 3]") == [1, 2, 3]
    assert deserialize_nested("plain string") == "plain string"
    assert deserialize_nested(42) == 42
    assert deserialize_nested(None) is None
    assert deserialize_nested("{not valid json") == "{not valid json"


def test_deserialize_nested_stdlib_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """deserialize_nested falls back to stdlib json when orjson unavailable."""
    from incorporator.io import formats
    from incorporator.io.formats import deserialize_nested

    monkeypatch.setattr(formats._orjson_mod, "ORJSON", None)

    assert deserialize_nested('{"a": 1}') == {"a": 1}
    assert deserialize_nested("[1, 2, 3]") == [1, 2, 3]
    assert deserialize_nested("plain string") == "plain string"
    assert deserialize_nested(42) == 42
    assert deserialize_nested(None) is None
    assert deserialize_nested("{not valid json") == "{not valid json"
