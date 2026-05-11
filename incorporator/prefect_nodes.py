"""
Prefect Orchestration Nodes for the Incorporator Framework.
Requires the `[orchestrate]` extras (Prefect).
"""

import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

from incorporator import LoggedIncorporator
from incorporator.methods.logger import AuditResult

# 1. ZERO-BLOAT DEPENDENCY SHIELD
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
) -> List[AuditResult]:
    """
    Prefect Task executing the O(1) Memory Incorporator Stream.
    Captures telemetry natively into the Prefect dashboard.
    """
    if not HAS_PREFECT:
        raise RuntimeError("Prefect is not installed. Run: pip install incorporator[orchestrate]")

    logger = get_run_logger()
    logger.info("🚀 Starting Incorporator stream orchestration.")

    results: List[AuditResult] = []

    try:
        async for audit in LoggedIncorporator.stream(
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            enable_logging=False,
        ):
            results.append(audit)

            # Route telemetry to the Prefect Cloud Dashboard
            status = f"Chunk {audit.chunk_index} | {audit.rows_processed} rows in {audit.processing_time_sec:.2f}s"
            if audit.failed_sources:
                logger.warning(f"⚠️ {status} with failures: {audit.failed_sources}")
            else:
                logger.info(f"✅ {status}")

        logger.info("🏁 Incorporator stream completed successfully.")
        return results

    except Exception as e:
        logger.error(f"❌ Fatal Pipeline Error: {str(e)}", exc_info=True)
        raise


@flow(name="incorporator_pipeline_flow")
async def run_incorporator_flow(config_path: str, poll_interval: Optional[float] = None) -> List[AuditResult]:
    """
    Prefect Flow entrypoint. Loads JSON configuration and triggers the stream task.
    """
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

    # Execute the Prefect Task
    return await run_incorporator_stream(
        incorp_params=incorp_params,
        refresh_params=refresh_params,
        export_params=export_params,
        poll_interval=poll_interval,
        stateful_polling=stateful_polling,  # 🛡️ PASS TO TASK
    )
