"""Prefect orchestration nodes for Incorporator stream and flow pipelines.

Requires the ``[orchestrate]`` extra (``pip install incorporator[orchestrate]``).
Provides a Prefect ``@task`` wrapping :meth:`LoggedIncorporator.stream` and
a ``@flow`` entry-point that loads a ``pipeline.json`` configuration.
When Prefect is not installed the decorators fall back to no-ops so the
module can be imported without raising.
"""

import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

from incorporator import LoggedIncorporator
from incorporator.observability.logger import Wave

try:
    from prefect import flow, get_run_logger, task

    HAS_PREFECT = True
except ImportError:
    HAS_PREFECT = False

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


@task(name="incorporator_stream_task", log_prints=True)
async def run_incorporator_stream(
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]] = None,
    export_params: Optional[Dict[str, Any]] = None,
    poll_interval: Optional[float] = None,
    stateful_polling: bool = False,
) -> List[Wave]:
    """Prefect task wrapping :meth:`LoggedIncorporator.stream`.

    Drives an O(1)-memory incorporator stream and routes each
    :class:`Wave` to the Prefect run logger. Returns the full list
    of ``Wave`` records on completion.
    """
    if not HAS_PREFECT:
        raise RuntimeError("Prefect is not installed. Run: pip install incorporator[orchestrate]")

    logger = get_run_logger()
    logger.info("🚀 Starting Incorporator stream orchestration.")

    results: List[Wave] = []

    try:
        async for wave in LoggedIncorporator.stream(
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            enable_logging=False,
        ):
            results.append(wave)

            status = f"Chunk {wave.chunk_index} | {wave.rows_processed} rows in {wave.processing_time_sec:.2f}s"
            if wave.failed_sources:
                logger.warning(f"⚠️ {status} with failures: {wave.failed_sources}")
            else:
                logger.info(f"✅ {status}")

        logger.info("🏁 Incorporator stream completed successfully.")
        return results

    except Exception as e:
        logger.error(f"❌ Fatal Pipeline Error: {str(e)}", exc_info=True)
        raise


@flow(name="incorporator_pipeline_flow")
async def run_incorporator_flow(config_path: str, poll_interval: Optional[float] = None) -> List[Wave]:
    """Prefect flow entry point: load ``pipeline.json`` and run the stream task."""
    if not HAS_PREFECT:
        print("❌ Prefect is not installed. Run: pip install incorporator[orchestrate]")
        sys.exit(1)

    path = Path(config_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found at {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = cast(Dict[str, Any], json.load(f))

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
