"""Unit tests for the Incorporator Prefect Integration."""

import importlib
import json
import sys
from pathlib import Path
from typing import Any, AsyncGenerator, List
from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from incorporator.observability.logger import Wave
from incorporator.integrations.prefect import run_incorporator_flow, run_incorporator_stream


@pytest.fixture(scope="session")
def prefect_test_fixture():
    """Spins up an isolated in-process Prefect environment for the duration of this module.

    NOT autouse — must be requested explicitly so the Prefect SQLite backing store
    and env-var overrides don't bleed into unrelated tests.
    """
    with prefect_test_harness():
        yield


async def mock_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    yield Wave(chunk_index=1, rows_processed=1000, failed_sources=[], processing_time_sec=0.8)


@pytest.mark.asyncio
async def test_prefect_flow_missing_file(prefect_test_fixture: None) -> None:
    """Ensures the Flow throws a standard FileNotFoundError if config is missing."""
    with pytest.raises(FileNotFoundError):
        await run_incorporator_flow(config_path="missing_prefect_config.json")


@pytest.mark.asyncio
async def test_prefect_flow_success(tmp_path: Path, prefect_test_fixture: None) -> None:
    """Ensures the Prefect flow executes the stream task and aggregates results."""
    config_file = tmp_path / "prefect_pipeline.json"
    config_file.write_text(
        json.dumps({"incorp_params": {"inc_file": "dummy.csv"}, "export_params": {"file_path": "out.db"}}),
        encoding="utf-8",
    )

    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        results: List[Wave] = await run_incorporator_flow(config_path=str(config_file), poll_interval=None)

        assert len(results) == 1
        assert results[0].chunk_index == 1
        assert results[0].rows_processed == 1000


@pytest.mark.asyncio
async def test_prefect_run_incorporator_stream_task_returns_list_of_wave(prefect_test_fixture: None) -> None:
    """Platform-review Stage 0 pin: the task body directly returns a list of Wave objects.

    Calls ``run_incorporator_stream`` (the ``@task``-decorated function)
    DIRECTLY — not through ``run_incorporator_flow`` — so a later change to
    the flow's aggregation/summary shape only touches the flow-level test
    (``test_prefect_flow_success``), not this task-level return-shape pin.
    Prefect 3.x tasks can be invoked directly outside an active ``@flow``
    context (confirmed against the installed version); no thin test-local
    flow wrapper is needed.
    """
    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        results = await run_incorporator_stream(incorp_params={"inc_file": "dummy.csv"})

    assert isinstance(results, list)
    assert len(results) == 1
    assert isinstance(results[0], Wave)
    assert results[0].chunk_index == 1
    assert results[0].rows_processed == 1000


def test_prefect_absent_dummy_branch_exposes_both_symbols() -> None:
    """`incorporator.integrations.prefect` imports cleanly with Prefect ABSENT.

    Pins the dummy-decorator fallback branch (incorporator/integrations/
    prefect.py's ``if not HAS_PREFECT:`` block): with ``prefect`` forced
    unimportable, a fresh import of the module must succeed, report
    ``HAS_PREFECT is False``, and still expose ``run_incorporator_stream``
    and ``run_incorporator_flow`` as plain (undecorated) callables.

    Isolation: snapshots the three affected ``sys.modules`` entries, sets
    ``sys.modules["prefect"] = None`` (the documented sentinel that forces
    ``import prefect`` to raise ``ImportError``), deletes the two
    incorporator-side modules so the next ``importlib.import_module`` call
    re-execs them fresh, then restores the ORIGINAL module objects (not a
    re-import) on teardown. Deliberately does NOT use ``importlib.reload()``
    on the live module — this test file already holds a
    ``from incorporator.integrations.prefect import run_incorporator_flow``
    reference at module scope, and ``reload()`` mutates that same shared
    object in place, which would corrupt it (and this module's own import)
    for the rest of the test session.
    """
    prefect_mod = sys.modules.get("prefect")
    deps_prefect_mod = sys.modules.get("incorporator._deps.prefect")
    integrations_prefect_mod = sys.modules.get("incorporator.integrations.prefect")

    try:
        sys.modules["prefect"] = None  # type: ignore[assignment]
        del sys.modules["incorporator._deps.prefect"]
        del sys.modules["incorporator.integrations.prefect"]

        fresh = importlib.import_module("incorporator.integrations.prefect")

        assert fresh.HAS_PREFECT is False
        assert hasattr(fresh, "run_incorporator_stream")
        assert hasattr(fresh, "run_incorporator_flow")
    finally:
        if prefect_mod is not None:
            sys.modules["prefect"] = prefect_mod
        else:
            sys.modules.pop("prefect", None)
        if deps_prefect_mod is not None:
            sys.modules["incorporator._deps.prefect"] = deps_prefect_mod
        if integrations_prefect_mod is not None:
            sys.modules["incorporator.integrations.prefect"] = integrations_prefect_mod
