"""tests/test_format_parsers.py"""

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import parse_source_data


@pytest.mark.asyncio
async def test_json_parsing_with_rust_speedups():
    """Tests the ultra-fast orjson path (assumes pip install incorporator[speedups])."""
    data = b'{"hello": "world"}'
    result = await parse_source_data(data, FormatType.JSON)
    assert result["hello"] == "world"


@pytest.mark.asyncio
async def test_json_parsing_standard_fallback(mock_no_speedups):
    """Tests the graceful degradation standard `json` path."""
    data = b'{"hello": "world"}'

    # Because of the fixture, orjson will throw an ImportError natively!
    result = await parse_source_data(data, FormatType.JSON)
    assert result["hello"] == "world"
