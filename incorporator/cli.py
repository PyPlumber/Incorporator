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

from incorporator import Incorporator, LoggedIncorporator

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


def _load_user_module(code_file: Path) -> Any:
    """Import the user's Python file once; return its module object.

    The fjord CLI uses this to resolve Incorporator subclass names (declared
    in JSON as strings) back to actual class objects, and to make the
    ``combine()`` function available to ``fjord()``.
    """
    import importlib.util

    code_path = code_file.resolve()
    if not code_path.exists():
        if typer:
            typer.secho(f"Error: code_file not found: {code_path}", fg=typer.colors.RED)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("_inc_fjord_user_module", code_path)
    if spec is None or spec.loader is None:
        if typer:
            typer.secho(f"Error: Cannot load module spec from: {code_path}", fg=typer.colors.RED)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _resolve_incorporator_class(module: Any, class_name: str, module_path: Path) -> Any:
    """Resolve a class name string to an Incorporator subclass, or exit 1."""
    target = getattr(module, class_name, None)
    if target is None:
        if typer:
            typer.secho(
                f"Error: class '{class_name}' not found in {module_path}", fg=typer.colors.RED
            )
        sys.exit(1)
    if not isinstance(target, type) or not issubclass(target, Incorporator):
        if typer:
            typer.secho(
                f"Error: '{class_name}' in {module_path} is not an Incorporator subclass.",
                fg=typer.colors.RED,
            )
        sys.exit(1)
    return target


async def _run_fjord(config: Dict[str, Any], config_dir: Path) -> None:
    """Resolve user classes from the code_file, then drive cls.fjord()."""
    code_file_raw = config.get("code_file")
    output_class_name = config.get("output_class")
    stream_params_cfg = config.get("stream_params")
    export_params = config.get("export_params")
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")

    if not code_file_raw or not output_class_name or not stream_params_cfg or not export_params:
        if typer:
            typer.secho(
                "Error: fjord config requires 'code_file', 'output_class', 'stream_params', "
                "and 'export_params'.",
                fg=typer.colors.RED,
            )
        sys.exit(1)

    code_file_path = Path(code_file_raw)
    if not code_file_path.is_absolute():
        code_file_path = (config_dir / code_file_path).resolve()

    user_module = _load_user_module(code_file_path)
    output_class = _resolve_incorporator_class(user_module, output_class_name, code_file_path)

    # Resolve stream_params[i]['cls_name'] strings to class references.
    resolved_streams: list[Dict[str, Any]] = []
    for idx, entry in enumerate(stream_params_cfg):
        cls_name = entry.get("cls_name")
        if not cls_name:
            if typer:
                typer.secho(
                    f"Error: stream_params[{idx}] missing 'cls_name' field.", fg=typer.colors.RED
                )
            sys.exit(1)
        resolved_entry = {k: v for k, v in entry.items() if k != "cls_name"}
        resolved_entry["cls"] = _resolve_incorporator_class(user_module, cls_name, code_file_path)
        resolved_streams.append(resolved_entry)

    if typer:
        typer.secho("🌊 Starting Incorporator Fjord...", fg=typer.colors.GREEN)

    try:
        async for audit in output_class.fjord(
            stream_params=resolved_streams,
            code_file=code_file_path,
            export_params=export_params,
            refresh_interval=refresh_interval,
            export_interval=export_interval,
        ):
            if typer:
                status = f"{audit.operation} | {audit.rows_processed} rows | {audit.processing_time_sec:.2f}s"
                typer.secho(status, fg=typer.colors.CYAN)
                if audit.failed_sources:
                    typer.secho(f"Failures: {audit.failed_sources}", fg=typer.colors.YELLOW)

    except asyncio.CancelledError:
        if typer:
            typer.secho("\n🛑 Fjord stopped by user.", fg=typer.colors.YELLOW)
    except Exception as e:
        if typer:
            typer.secho(f"\n❌ Fatal Fjord Error: {e}", fg=typer.colors.RED)
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

    @app.command()  # type: ignore[untyped-decorator]
    def fjord(
        config: Path = typer.Argument(..., help="Path to the fjord pipeline.json configuration file."),  # noqa: B008
        logs: bool = typer.Option(  # noqa: B008
            False, "--logs", help="Enable background multiplex disk logging (error.log, api.log, debug.log)."
        ),
    ) -> None:
        """
        Execute a Multi-Source Stateful Fjord Pipeline from a JSON configuration file.

        The JSON must declare:
          - code_file (path): Python file with Incorporator subclasses + a combine(state) function.
          - output_class (str): name of the combined output Incorporator subclass.
          - stream_params (list): one entry per source with cls_name, incorp_params, refresh_params, etc.
          - export_params (dict): destination for the combined output.
          - refresh_interval / export_interval (floats, optional): daemon cadence.
        """
        pipeline_config = _load_pipeline_config(config)

        # Surface the logging-toggle without changing how user classes are loaded.
        if logs:
            import logging as _logging

            _logging.basicConfig(level=_logging.INFO)

        try:
            asyncio.run(_run_fjord(pipeline_config, config_dir=config.parent.resolve()))
        except KeyboardInterrupt:
            typer.secho("\n🛑 Fjord stopped by user.", fg=typer.colors.YELLOW)


def main() -> None:
    """Entry point for the setup.py / pyproject.toml console script."""
    if app is None:
        print("❌ Typer is not installed. To use the CLI, run: pip install incorporator[orchestrate]")
        sys.exit(1)
    app()


if __name__ == "__main__":
    main()
