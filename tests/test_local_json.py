"""Tests for the minimal vertical slice: Local JSON File to Incorporator Object."""

import logging

import pytest

from incorporator import Incorporator


@pytest.mark.asyncio
async def test_incorporator_reads_local_json_successfully(clean_json_file: str) -> None:
    """Subclass reads a local JSON file and returns dot-notation Python objects."""

    class Pokemon(Incorporator):
        pass

    # Format is auto-inferred from the .json extension.
    results = await Pokemon.incorp(inc_file=clean_json_file)

    # 2. Assert orchestration returned the correct type
    assert isinstance(results, list)
    assert len(results) == 2

    first_item = results[0]

    # 3. The returned object is an instance of the user-defined subclass.
    assert isinstance(first_item, Pokemon)

    # 4. Assert dot-notation and schema generation worked
    assert first_item.name == "Bulbasaur"

    # Notice: 'weight' was dynamically added by schema inference because it was in the JSON.
    assert first_item.weight == 69


@pytest.mark.asyncio
async def test_incorporator_warns_and_skips_on_bad_json(
    broken_json_file: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Malformed JSON is logged as a warning and the call returns an empty list.

    The log now uses ``str(reject)`` — format is
    ``"IncorporatorFormatError: {path} — {error detail}"``.
    A ``UserWarning`` is also emitted via ``warnings.warn`` after the
    ``asyncio.to_thread`` join in ``base.py``.
    """

    class BadPokemon(Incorporator):
        pass

    import warnings

    with caplog.at_level(logging.WARNING):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            results = await BadPokemon.incorp(inc_file=broken_json_file)

    # Assert that the framework survived and safely returned an empty list.
    assert isinstance(results, list)
    assert len(results) == 0

    # Parse errors are now logged via str(reject): "IncorporatorFormatError: {path} — {detail}".
    assert "IncorporatorFormatError" in caplog.text
    assert "Invalid JSON" in caplog.text
