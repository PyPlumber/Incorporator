"""Tests for the minimal vertical slice: Local JSON File to Incorporator Object."""

import pytest
from typing import List

from incorporator import Incorporator
from incorporator.methods.exceptions import IncorporatorFormatError


@pytest.mark.asyncio
async def test_incorporator_reads_local_json_successfully(clean_json_file: str) -> None:
    """Ensures a local JSON file is read, parsed, and converted to dynamic objects."""

    # 1. Execute the minimal vertical slice
    # Note: format_type is omitted here to test the _infer_format auto-detection
    results = await Incorporator.incorp(file=clean_json_file)

    # 2. Assert orchestration returned the correct type
    assert isinstance(results, list)
    assert len(results) == 2

    first_item = results[0]

    # 3. Assert the returned object inherits from Incorporator (proving schema_builder worked)
    assert isinstance(first_item, Incorporator)

    # 4. Assert dot-notation and schema generation worked
    assert first_item.name == "Bulbasaur"

    # Notice: 'weight' was dynamically added by schema_builder because it was in the JSON!
    assert getattr(first_item, "weight") == 69


@pytest.mark.asyncio
async def test_incorporator_raises_custom_error_on_bad_json(broken_json_file: str) -> None:
    """Ensures json.JSONDecodeError is gracefully caught and converted."""

    # Assert that our specific custom error is raised, protecting the user from messy tracebacks
    with pytest.raises(IncorporatorFormatError) as exc_info:
        await Incorporator.incorp(file=broken_json_file)

    # Assert the error message contains helpful context
    assert "Invalid JSON" in str(exc_info.value)