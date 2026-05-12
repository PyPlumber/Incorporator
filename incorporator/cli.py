"""
Command Line Interface for the Incorporator Orchestration Platform.
Requires the `[orchestrate]` extras (Typer).
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, cast

try:
    import typer
except ImportError:
    typer = None  # type: ignore[assignment]

from incorporator import LoggedIncorporator

logger = logging.getLogger(__name__)

if typer:
    app: Any = typer.Typer(
        name="incorporator",
        help="Zero-Boilerplate Universal Data Gateway & Pipeline Orchestrator.",
        no_args_is_help=True,
    )

    @app.callback()  # type: ignore[untyped-decorator]
    def main_callback() -> None:
        pass

else:
    # Failsafe for entrypoint if Typer is missing
    app = None


def _load_pipeline_config(config_path: Path) -> Dict[str, Any]:
    """Safely loads and validates the pipeline JSON configuration."""
    if not config_path.is_file():
        if typer:
            typer.secho(f"Error: Configuration file not found at {config_path}", fg=typer.colors.RED)
        sys.exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return cast(Dict[str, Any], json.load(f))
    except json.JSONDecodeError as e:
        if typer:
            typer.secho(f"Error: Invalid JSON in {config_path}: {e}", fg=typer.colors.RED)
        sys.exit(1)


async def _run_stream(config: Dict[str, Any], poll_interval: Optional[float], enable_logging: bool) -> None:
    incorp_params = config.get("incorp_params", {})
    refresh_params = config.get("refresh_params")
    export_params = config.get("export_params")
    stateful_polling = config.get("stateful_polling", False)
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")

    if not incorp_params:
        if typer:
            typer.secho(
                "Error: 'incorp_params' must be defined in the configuration JSON.",
                fg=typer.colors.RED,
            )
        sys.exit(1)

    if typer:
        typer.secho("🚀 Starting Incorporator Stream...", fg=typer.colors.GREEN)

    try:
        async for audit in LoggedIncorporator.stream(
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            refresh_interval=refresh_interval,
            export_interval=export_interval,
            enable_logging=enable_logging,
        ):
            if not enable_logging and typer:
                status = f"Chunk {audit.chunk_index} | {audit.rows_processed} rows | {audit.processing_time_sec:.2f}s"
                typer.secho(status, fg=typer.colors.CYAN)
                if audit.failed_sources:
                    typer.secho(f"Failures: {audit.failed_sources}", fg=typer.colors.YELLOW)

    except asyncio.CancelledError:
        if typer:
            typer.secho("\n🛑 Stream stopped by user.", fg=typer.colors.YELLOW)
    except Exception as e:
        if typer:
            typer.secho(f"\n❌ Fatal Pipeline Error: {e}", fg=typer.colors.RED)
        sys.exit(1)


if typer:

    @app.command()  # type: ignore[untyped-decorator]
    def stream(
        config: Path = typer.Argument(..., help="Path to the pipeline.json configuration file."),  # noqa: B008
        poll: Optional[float] = typer.Option(  # noqa: B008
            None, "--poll", help="Interval in seconds to keep the pipeline alive as a daemon (e.g., 60.0)."
        ),
        logs: bool = typer.Option(  # noqa: B008
            False, "--logs", help="Enable background multiplex disk logging (error.log, api.log, debug.log)."
        ),
    ) -> None:
        """
        Execute an Autonomous Pipeline Stream from a JSON configuration file.
        """
        pipeline_config = _load_pipeline_config(config)

        try:
            asyncio.run(_run_stream(pipeline_config, poll_interval=poll, enable_logging=logs))
        except KeyboardInterrupt:
            typer.secho("\n🛑 Stream stopped by user.", fg=typer.colors.YELLOW)


def main() -> None:
    """Entry point for the setup.py / pyproject.toml console script."""
    if app is None:
        print("❌ Typer is not installed. To use the CLI, run: pip install incorporator[orchestrate]")
        sys.exit(1)
    app()


if __name__ == "__main__":
    main()
