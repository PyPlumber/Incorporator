"""Tests for the minimal vertical slice: Local JSON File to Incorporator Object."""

import logging

import pytest

from incorporator import Incorporator


@pytest.mark.asyncio
async def test_incorporator_reads_local_json_successfully(clean_json_file: str) -> None:
    """Ensures a local JSON file is read, parsed, and converted to dynamic objects."""

    # 1. Execute the minimal vertical slice
    # Note: format_type is omitted here to test the _infer_format auto-detection
    # UPDATED: Use inc_file= to match the new API contract
    results = await Incorporator.incorp(inc_file=clean_json_file)

    # 2. Assert orchestration returned the correct type
    assert isinstance(results, list)
    assert len(results) == 2

    first_item = results[0]

    # 3. Assert the returned object inherits from Incorporator (proving schema_builder worked)
    assert isinstance(first_item, Incorporator)

    # 4. Assert dot-notation and schema generation worked
    assert first_item.name == "Bulbasaur"

    # Notice: 'weight' was dynamically added by schema_builder because it was in the JSON!
    assert first_item.weight == 69


@pytest.mark.asyncio
async def test_incorporator_warns_and_skips_on_bad_json(
    broken_json_file: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Ensures json.JSONDecodeError is gracefully caught, logged as a warning, and skipped without crashing."""

    # 1. Capture the logs at the WARNING level
    with caplog.at_level(logging.WARNING):
        # We no longer expect a crash! Incorporator should survive this gracefully.
        results = await Incorporator.incorp(inc_file=broken_json_file)

    # 2. Assert that the framework survived and safely returned an empty list
    assert isinstance(results, list)
    assert len(results) == 0

    # 3. Assert the warning message contains our helpful fault-tolerance context
    assert "PARSE FAILED for format" in caplog.text
    assert "The payload may be malformed" in caplog.text
