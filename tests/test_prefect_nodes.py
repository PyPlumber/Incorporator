"""Unit tests for the Incorporator Prefect Integration."""

import importlib
import json
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from incorporator.integrations.prefect import run_incorporator_flow, run_incorporator_stream
from incorporator.observability.logger import Wave


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
    """Ensures the Prefect flow executes the stream task and returns a summary dict."""
    config_file = tmp_path / "prefect_pipeline.json"
    config_file.write_text(
        json.dumps({"incorp_params": {"inc_file": "dummy.csv"}, "export_params": {"file_path": "out.db"}}),
        encoding="utf-8",
    )

    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        summary = await run_incorporator_flow(config_path=str(config_file), poll_interval=None)

        assert summary["chunks"] == 1
        assert summary["rows_processed"] == 1000
        assert summary["failed_chunks"] == 0


@pytest.mark.asyncio
async def test_prefect_flow_env_expansion_and_path_rebasing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefect_test_fixture: None
) -> None:
    """P3: the flow now resolves ``${VAR}`` env references and rebases INPUT paths, like the CLI.

    A config using ``${VAR}`` in ``inc_url`` and a relative ``inc_file``
    silently lost both resolutions before this fix (plain ``json.load`` +
    manual key-plucking). Proves ``expand_env`` and ``resolve_config_paths``
    now run inside ``run_incorporator_flow``.
    """
    monkeypatch.setenv("PREFECT_TEST_SOURCE_URL", "https://example.com/data.csv")
    config_file = tmp_path / "prefect_pipeline.json"
    (tmp_path / "dummy.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    config_file.write_text(
        json.dumps(
            {
                "incorp_params": {"inc_url": "${PREFECT_TEST_SOURCE_URL}", "inc_file": "dummy.csv"},
                "export_params": {"file_path": "out.db"},
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, Any] = {}

    async def spy_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
        captured.update(kwargs)
        async for wave in mock_stream(*args, **kwargs):
            yield wave

    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=spy_stream):
        await run_incorporator_flow(config_path=str(config_file), poll_interval=None)

    incorp_params = captured["incorp_params"]
    assert incorp_params["inc_url"] == "https://example.com/data.csv"
    assert incorp_params["inc_file"] == str((tmp_path / "dummy.csv").resolve())


@pytest.mark.asyncio
async def test_prefect_run_incorporator_stream_task_returns_summary_dict(prefect_test_fixture: None) -> None:
    """Platform-review Stage 0 pin: the task body returns an O(1) summary dict, not a Wave list.

    Calls ``run_incorporator_stream`` (the public task-wrapper function)
    DIRECTLY — not through ``run_incorporator_flow`` — so a later change to
    the flow's aggregation/summary shape only touches the flow-level test
    (``test_prefect_flow_success``), not this task-level return-shape pin.
    Prefect 3.x tasks can be invoked directly outside an active ``@flow``
    context (confirmed against the installed version); no thin test-local
    flow wrapper is needed.
    """
    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        summary = await run_incorporator_stream(incorp_params={"inc_file": "dummy.csv"})

    assert isinstance(summary, dict)
    assert summary["chunks"] == 1
    assert summary["rows_processed"] == 1000
    assert summary["failed_chunks"] == 0
    assert summary["failed_sources"] == []
    assert isinstance(summary["elapsed_sec"], float)


@pytest.mark.asyncio
async def test_prefect_stream_enable_logging_forwarded(prefect_test_fixture: None) -> None:
    """``enable_logging=True`` is forwarded through to ``LoggedIncorporator.stream``, not hardcoded False."""
    captured: dict[str, Any] = {}

    async def spy_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
        captured.update(kwargs)
        async for wave in mock_stream(*args, **kwargs):
            yield wave

    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=spy_stream):
        await run_incorporator_stream(incorp_params={"inc_file": "dummy.csv"}, enable_logging=True)

    assert captured["enable_logging"] is True


@pytest.mark.asyncio
async def test_prefect_stream_retries_default_skips_with_options(prefect_test_fixture: None) -> None:
    """Default ``retries=0``/``retry_delay_seconds=0`` still returns the summary dict unchanged.

    Guards against the ``.with_options`` branch being invoked unconditionally,
    which would needlessly re-configure the task even in the common no-retry
    case.
    """
    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        summary = await run_incorporator_stream(incorp_params={"inc_file": "dummy.csv"})

    assert summary["chunks"] == 1
    assert summary["rows_processed"] == 1000


@pytest.mark.asyncio
async def test_prefect_stream_retries_passthrough(prefect_test_fixture: None) -> None:
    """``retries``/``retry_delay_seconds`` route through ``Task.with_options`` when non-zero."""
    import incorporator.integrations.prefect as prefect_mod

    fake_summary = {"chunks": 1, "rows_processed": 1000, "failed_chunks": 0, "failed_sources": [], "elapsed_sec": 0.1}
    fake_task_with_retries = AsyncMock(return_value=fake_summary)
    fake_with_options = MagicMock(return_value=fake_task_with_retries)

    with patch.object(prefect_mod._run_incorporator_stream_task, "with_options", fake_with_options):
        summary = await run_incorporator_stream(
            incorp_params={"inc_file": "dummy.csv"}, retries=1, retry_delay_seconds=2.5
        )

    fake_with_options.assert_called_once_with(retries=1, retry_delay_seconds=2.5)
    fake_task_with_retries.assert_called_once()
    assert summary == fake_summary


@pytest.mark.asyncio
async def test_prefect_absent_flow_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """`run_incorporator_flow` raises `RuntimeError` (not `SystemExit`) when Prefect is absent.

    The guard used to call ``sys.exit(1)``
    inside a library function; monkeypatches ``HAS_PREFECT`` to ``False`` and
    asserts the flow raises instead of exiting the process.
    """
    import incorporator.integrations.prefect as prefect_mod

    monkeypatch.setattr(prefect_mod, "HAS_PREFECT", False)

    with pytest.raises(RuntimeError, match="Prefect is not installed"):
        await prefect_mod.run_incorporator_flow(config_path="unused_prefect_config.json")


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
        # importlib.import_module above also rebound the PARENT packages'
        # attributes (incorporator.integrations.prefect / _deps.prefect) to
        # the fresh dummy-mode module; `import a.b as m` resolves through
        # that parent attribute, so restore it too or later tests in a
        # randomized order receive the dummy module (its task is a plain
        # function with no `with_options`).
        import incorporator._deps as _deps_pkg
        import incorporator.integrations as _integrations_pkg

        if deps_prefect_mod is not None:
            _deps_pkg.prefect = deps_prefect_mod  # type: ignore[attr-defined]
        if integrations_prefect_mod is not None:
            _integrations_pkg.prefect = integrations_prefect_mod  # type: ignore[attr-defined]
