"""Prefect orchestration nodes for Incorporator stream and flow pipelines.

Requires the ``[orchestrate]`` extra (``pip install incorporator[orchestrate]``).
Provides ``run_incorporator_stream``, a public wrapper around a Prefect
``@task`` that drives :meth:`Incorporator.stream`, and ``run_incorporator_flow``,
a ``@flow`` entry-point that loads a ``pipeline.json`` configuration through
the same env-expansion / path-rebasing / token-resolution steps the CLI
uses. When Prefect is not installed the decorators fall back to no-ops so
the module can be imported without raising.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from incorporator import LoggedIncorporator
from incorporator._deps.prefect import PREFECT
from incorporator.config.envexpand import expand_env
from incorporator.config.pipeline import resolve_sidecar_tokens
from incorporator.io.config_paths import resolve_config_paths

HAS_PREFECT = PREFECT is not None

if HAS_PREFECT:
    from prefect import flow, get_run_logger, task  # type: ignore[import-not-found, import-untyped, unused-ignore]

# Assign fallbacks as variables instead of redefining functions
if not HAS_PREFECT:

    def _dummy_decorator(*args: Any, **kwargs: Any) -> Any:
        def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return wrapper if not args else wrapper(args[0])

    def _dummy_logger(*args: Any, **kwargs: Any) -> Any:
        import logging

        return logging.getLogger("dummy_prefect")

    task = _dummy_decorator
    flow = _dummy_decorator  # type: ignore
    get_run_logger = _dummy_logger


# Bounded sample size for the failed-source URIs surfaced in the summary
# dict — accumulating the full list across every wave would defeat the
# O(1)-memory fix this wrapper exists to provide.
_MAX_FAILED_SOURCES_SAMPLE = 20


@task(name="incorporator_stream_task", log_prints=True)
async def _run_incorporator_stream_task(
    incorp_params: dict[str, Any],
    refresh_params: dict[str, Any] | None = None,
    export_params: dict[str, Any] | None = None,
    poll_interval: float | None = None,
    stateful_polling: bool = False,
    enable_logging: bool = False,
) -> dict[str, Any]:
    """Prefect task wrapping :meth:`Incorporator.stream` (with optional disk logging via ``enable_logging=True``).

    Streams each :class:`Wave` to the Prefect run logger as it arrives and
    accumulates only O(1) summary state — no per-wave list is retained.
    """
    if not HAS_PREFECT:
        raise RuntimeError("Prefect is not installed. Run: pip install incorporator[orchestrate]")

    logger = get_run_logger()
    logger.info("Starting Incorporator stream orchestration.")

    chunks = 0
    rows_processed = 0
    failed_chunks = 0
    failed_sources: list[str] = []
    start = time.monotonic()

    try:
        async for wave in LoggedIncorporator.stream(
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            enable_logging=enable_logging,
        ):
            chunks += 1
            rows_processed += wave.rows_processed

            status = f"Chunk {wave.chunk_index} | {wave.rows_processed} rows in {wave.processing_time_sec:.2f}s"
            if wave.failed_sources:
                failed_chunks += 1
                if len(failed_sources) < _MAX_FAILED_SOURCES_SAMPLE:
                    failed_sources.extend(wave.failed_sources)
                logger.warning(f"{status} with failures: {wave.failed_sources}")
            else:
                logger.info(f"{status}")

        logger.info("Incorporator stream completed successfully.")
        return {
            "chunks": chunks,
            "rows_processed": rows_processed,
            "failed_chunks": failed_chunks,
            "failed_sources": failed_sources[:_MAX_FAILED_SOURCES_SAMPLE],
            "elapsed_sec": time.monotonic() - start,
        }

    except Exception as e:
        logger.error(f"Fatal Pipeline Error: {str(e)}", exc_info=True)
        raise


async def run_incorporator_stream(
    incorp_params: dict[str, Any],
    refresh_params: dict[str, Any] | None = None,
    export_params: dict[str, Any] | None = None,
    poll_interval: float | None = None,
    stateful_polling: bool = False,
    enable_logging: bool = False,
    retries: int = 0,
    retry_delay_seconds: float = 0,
) -> dict[str, Any]:
    """Public task-wrapper function driving an O(1)-memory Incorporator stream.

    Consumes :meth:`Incorporator.stream`, routing each :class:`Wave` to the
    Prefect run logger (info normally, warning on ``failed_sources``) as it
    arrives, and returns a bounded summary of the run instead of retaining
    every ``Wave`` in memory.

    Args:
        incorp_params: Forwarded to :meth:`Incorporator.stream`.
        refresh_params: Forwarded to :meth:`Incorporator.stream`.
        export_params: Forwarded to :meth:`Incorporator.stream`.
        poll_interval: Forwarded to :meth:`Incorporator.stream`.
        stateful_polling: Forwarded to :meth:`Incorporator.stream`.
        enable_logging: When ``True``, also wires up
            :class:`~incorporator.observability.logger.LoggedIncorporator`'s
            own JSON-line disk logger. Prefect's run logger already
            receives every ``Wave`` regardless of this flag, so setting it
            ``True`` is a genuine double-write the caller opts into — not
            a bug. Off by default.
        retries: Forwarded to Prefect's ``Task.with_options`` when non-zero
            (or when ``retry_delay_seconds`` is non-zero). ``0`` reproduces
            today's no-retry behavior exactly — ``.with_options`` is not
            invoked at all in that case.
        retry_delay_seconds: See ``retries``.

    Returns:
        A summary dict: ``chunks`` (total waves consumed), ``rows_processed``
        (sum across waves), ``failed_chunks`` (count of waves with
        ``failed_sources``), ``failed_sources`` (a capped sample of failed
        source URIs, not a full accumulation), and ``elapsed_sec``.
    """
    kwargs: dict[str, Any] = {
        "incorp_params": incorp_params,
        "refresh_params": refresh_params,
        "export_params": export_params,
        "poll_interval": poll_interval,
        "stateful_polling": stateful_polling,
        "enable_logging": enable_logging,
    }
    if HAS_PREFECT and (retries or retry_delay_seconds):
        task_with_retries = _run_incorporator_stream_task.with_options(
            retries=retries, retry_delay_seconds=retry_delay_seconds
        )
        return cast(dict[str, Any], await task_with_retries(**kwargs))
    return cast(dict[str, Any], await _run_incorporator_stream_task(**kwargs))


@flow(name="incorporator_pipeline_flow")
async def run_incorporator_flow(config_path: str, poll_interval: float | None = None) -> dict[str, Any]:
    """Prefect flow entry point: load ``pipeline.json`` and run the stream task.

    Routes the loaded config through the same env-expansion,
    config-dir path-rebasing, and sidecar-token-resolution steps the CLI
    performs (``incorporator.cli.runners._load_pipeline_config``) so a
    config that works under ``incorporator stream`` behaves identically
    here — ``${VAR}``/``${file:...}`` references, ``@sigil``/call-grammar
    tokens, and input-path rebasing are no longer silently skipped.

    Raises ``RuntimeError`` instead of exiting the process when Prefect is
    absent — this is a library function, not a CLI entry point.

    Raises:
        RuntimeError: Prefect is not installed.
        FileNotFoundError: ``config_path`` does not resolve to a file, or
            (via sidecar-token resolution) the inflow/outflow sidecar does
            not resolve to a file.
        EnvExpansionError: A required env var is unset or a
            ``${file:...}`` reference points at a missing path.
        ImportError: The inflow/outflow sidecar cannot be loaded as a
            Python module.
        SyntaxError: The inflow/outflow sidecar has invalid Python syntax.
        TokenResolutionError: A JSON-text token references an unsafe or
            unknown symbol.
        ValueError: ``incorp_params`` is missing from the configuration.
    """
    if not HAS_PREFECT:
        raise RuntimeError("Prefect is not installed. Run: pip install incorporator[orchestrate]")

    path = Path(config_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found at {path}")

    with open(path, encoding="utf-8") as f:
        parsed = cast(dict[str, Any], json.load(f))

    expanded = cast(dict[str, Any], expand_env(parsed))
    rebased = resolve_config_paths(expanded, path.parent.resolve())
    # strict_outflow=True (the default): unlike the CLI, this flow has no
    # later aggregated-validation step to defer a broken outflow sidecar
    # to — raise immediately, same as inflow.
    config = resolve_sidecar_tokens(rebased)

    incorp_params = config.get("incorp_params", {})
    refresh_params = config.get("refresh_params")
    export_params = config.get("export_params")
    stateful_polling = config.get("stateful_polling", False)

    if not incorp_params:
        raise ValueError("'incorp_params' must be defined in the configuration JSON.")

    return await run_incorporator_stream(
        incorp_params=incorp_params,
        refresh_params=refresh_params,
        export_params=export_params,
        poll_interval=poll_interval,
        stateful_polling=stateful_polling,
    )
