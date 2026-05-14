"""
Command Line Interface for the Incorporator Orchestration Platform.
Requires the `[orchestrate]` extras (Typer).

Implementation note: the heavy async runners and config-loading helpers live
in :mod:`incorporator.cli.runners` so this file can stay focused on Typer
app construction + command registration.  The legacy private symbols
(``_load_pipeline_config``, ``_run_validation``, ``_emit_wave``,
``_install_sigterm_handler``, ``_err``, ``_run_stream``, ``_run_fjord``,
``_load_user_module``, ``_resolve_incorporator_class``, ``_JSON_OUTPUT_MODE``)
are re-exported below for backwards compatibility with any tests that
import them directly from this module.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Optional

try:
    import typer
except ImportError:
    typer = None  # type: ignore[assignment]

from .. import Incorporator, LoggedIncorporator  # re-exports preserved for test patching
from .runners import (
    _JSON_OUTPUT_MODE,
    _emit_wave,
    _err,
    _install_sigterm_handler,
    _load_pipeline_config,
    _load_user_module,
    _resolve_incorporator_class,
    _run_fjord,
    _run_stream,
    _run_validation,
    set_json_output_mode,
)
from .scaffold import write_scaffold

logger = logging.getLogger(__name__)

__all__ = [
    "app",
    "main",
    # Re-exports for backwards compatibility with existing test imports.
    "Incorporator",
    "LoggedIncorporator",
    "_emit_wave",
    "_err",
    "_install_sigterm_handler",
    "_JSON_OUTPUT_MODE",
    "_load_pipeline_config",
    "_load_user_module",
    "_resolve_incorporator_class",
    "_run_fjord",
    "_run_stream",
    "_run_validation",
]


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


# ---------------------------------------------------------------------------
# Typer commands
# ---------------------------------------------------------------------------


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
        json_output: bool = typer.Option(  # noqa: B008
            False,
            "--json-output",
            help="Emit one NDJSON Wave per line on stdout (status text goes to stderr). For CI/CD pipelines.",
        ),
        heartbeat_file: Optional[Path] = typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every wave; pairs with the Docker HEALTHCHECK.",
        ),
    ) -> None:
        """
        Execute an Autonomous Pipeline Stream from a JSON configuration file.
        """
        set_json_output_mode(json_output)

        pipeline_config = _load_pipeline_config(config)
        _run_validation(pipeline_config, config.parent.resolve(), type_override="stream")

        try:
            asyncio.run(
                _run_stream(
                    pipeline_config,
                    poll_interval=poll,
                    enable_logging=logs,
                    json_output=json_output,
                    heartbeat_file=heartbeat_file,
                )
            )
        except KeyboardInterrupt:
            _err("\n🛑 Stream stopped by user.", fg=typer.colors.YELLOW)

    @app.command()  # type: ignore[untyped-decorator]
    def fjord(
        config: Path = typer.Argument(..., help="Path to the fjord pipeline.json configuration file."),  # noqa: B008
        logs: bool = typer.Option(  # noqa: B008
            False, "--logs", help="Enable background multiplex disk logging (error.log, api.log, debug.log)."
        ),
        json_output: bool = typer.Option(  # noqa: B008
            False,
            "--json-output",
            help="Emit one NDJSON Wave per line on stdout (status text goes to stderr). For CI/CD pipelines.",
        ),
        heartbeat_file: Optional[Path] = typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every wave; pairs with the Docker HEALTHCHECK.",
        ),
    ) -> None:
        """
        Execute a Multi-Source Stateful Fjord Pipeline from a JSON configuration file.

        The JSON must declare:
          - outflow (path): Python file with source Incorporator subclasses + a top-level
            outflow(state) function. The filename's stem becomes the output class name
            (snake_case → PascalCase; e.g. coin_market.py → CoinMarket).
          - stream_params (list): one entry per source with cls_name, incorp_params, refresh_params, etc.
          - export_params (dict): destination for the combined output.
          - refresh_interval / export_interval (floats, optional): daemon cadence.
        """
        set_json_output_mode(json_output)

        pipeline_config = _load_pipeline_config(config)
        _run_validation(pipeline_config, config.parent.resolve(), type_override="fjord")

        if logs:
            logging.basicConfig(level=logging.INFO)

        try:
            asyncio.run(
                _run_fjord(
                    pipeline_config,
                    config_dir=config.parent.resolve(),
                    enable_logging=logs,
                    json_output=json_output,
                    heartbeat_file=heartbeat_file,
                )
            )
        except KeyboardInterrupt:
            _err("\n🛑 Fjord stopped by user.", fg=typer.colors.YELLOW)

    @app.command()  # type: ignore[untyped-decorator]
    def validate(
        config: Path = typer.Argument(..., help="Path to the pipeline.json configuration file."),  # noqa: B008
        type_: Optional[str] = typer.Option(  # noqa: B008
            None,
            "--type",
            help="Force 'stream' or 'fjord' validation. Defaults to auto-detect from the JSON keys.",
        ),
    ) -> None:
        """
        Validate a pipeline.json without executing anything.

        Resolves ${VAR} / ${file:...} references, checks required keys,
        and (for fjord) confirms cls_name targets and outflow() arity in
        the referenced outflow file.

        Exits 0 if the config is valid, 1 with a diagnostic report otherwise.
        """
        pipeline_config = _load_pipeline_config(config)
        detected = _run_validation(pipeline_config, config.parent.resolve(), type_override=type_)
        typer.secho(f"✅ {config} is valid ({detected}).", fg=typer.colors.GREEN)

    @app.command()  # type: ignore[untyped-decorator]
    def init(
        type_: str = typer.Option(  # noqa: B008
            "stream",
            "--type",
            help="Scaffold type: 'stream' (one source) or 'fjord' (multi-source + outflow.py).",
        ),
        output_dir: Path = typer.Option(  # noqa: B008
            Path("."),
            "--output-dir",
            "-o",
            help="Directory to write the starter files into (created if missing).",
        ),
        with_inflow: bool = typer.Option(  # noqa: B008
            False,
            "--with-inflow",
            help="Also scaffold an inflow.py for user-defined helpers (calc reducers, custom converters).",
        ),
    ) -> None:
        """
        Generate a starter pipeline.json (and, for fjord, an outflow.py).

        Refuses to overwrite existing files. After running, fill in the
        placeholders, then ``incorporator validate <path>`` to confirm,
        then ``incorporator stream <path>`` or ``incorporator fjord <path>``.
        """
        try:
            written = write_scaffold(type_, output_dir.resolve(), with_inflow=with_inflow)
        except FileExistsError as e:
            typer.secho(f"Error: {e}", fg=typer.colors.RED)
            sys.exit(1)
        except ValueError as e:
            typer.secho(f"Error: {e}", fg=typer.colors.RED)
            sys.exit(1)

        typer.secho(f"✅ Wrote {len(written)} starter file(s):", fg=typer.colors.GREEN)
        for path in written:
            typer.secho(f"  - {path}", fg=typer.colors.CYAN)
        pipeline_path = next((p for p in written if p.name == "pipeline.json"), written[0])
        typer.secho(
            "\nNext steps:\n"
            "  1. Edit the file(s) above and replace the placeholders.\n"
            f"  2. incorporator validate {pipeline_path}\n"
            f"  3. incorporator {type_} {pipeline_path}",
            fg=typer.colors.WHITE,
        )


def main() -> None:
    """Entry point for the setup.py / pyproject.toml console script."""
    if app is None:
        print("❌ Typer is not installed. To use the CLI, run: pip install incorporator[orchestrate]")
        sys.exit(1)
    app()


if __name__ == "__main__":
    main()
