"""
Command Line Interface for the Incorporator Orchestration Platform.
Requires the `[orchestrate]` extras (Typer).
"""

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any, Dict, Optional, cast

try:
    import typer
except ImportError:
    typer = None  # type: ignore[assignment]

from incorporator import Incorporator, LoggedIncorporator

from .envexpand import EnvExpansionError, expand_env
from .scaffold import write_scaffold
from .tokens import TokenResolutionError, resolve_tokens
from .validate import validate_config

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


# ---------------------------------------------------------------------------
# stderr / stdout split for --json-output mode
# ---------------------------------------------------------------------------

# Set to True by stream / fjord when --json-output is requested. Routes the
# status banners + error messages to stderr so stdout stays pure NDJSON.
# Outside --json-output mode (default) everything goes to stdout — back-
# compat with the original CLI output and with tests that read result.stdout.
_JSON_OUTPUT_MODE: bool = False


def _err(msg: str, fg: Any = None) -> None:
    """Print a status or error message.

    Routes to **stderr** when ``--json-output`` is active (so NDJSON on
    stdout stays parseable); otherwise prints to stdout — back-compat
    default for human terminal users and existing test assertions.
    """
    if typer:
        typer.secho(msg, fg=fg, err=_JSON_OUTPUT_MODE)


# ---------------------------------------------------------------------------
# Config loading + env expansion
# ---------------------------------------------------------------------------


def _load_pipeline_config(config_path: Path) -> Dict[str, Any]:
    """Load and env-expand a pipeline JSON configuration.

    Env-var and ``${file:...}`` references are resolved at load time so the
    rest of the CLI (and the validators) work against a fully-resolved
    config. Missing references surface here with a clear error.
    """
    if not config_path.is_file():
        _err(f"Error: Configuration file not found at {config_path}", fg=typer.colors.RED if typer else None)
        sys.exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            parsed = cast(Dict[str, Any], json.load(f))
    except json.JSONDecodeError as e:
        _err(f"Error: Invalid JSON in {config_path}: {e}", fg=typer.colors.RED if typer else None)
        sys.exit(1)

    try:
        expanded = cast(Dict[str, Any], expand_env(parsed))
    except EnvExpansionError as e:
        _err(f"Error: env-var expansion failed: {e}", fg=typer.colors.RED if typer else None)
        sys.exit(1)

    # Resolve JSON-text tokens (e.g. "NextUrlPaginator('next')",
    # "inc(datetime)", "join_all(';')") into real Python objects before the
    # config reaches the engine.  Tokens needing user-defined functions or
    # classes still require a code_file — those raise TokenResolutionError
    # here with a clear allow-list message.
    try:
        return cast(Dict[str, Any], resolve_tokens(expanded))
    except TokenResolutionError as e:
        _err(f"Error: token resolution failed: {e}", fg=typer.colors.RED if typer else None)
        sys.exit(1)


def _run_validation(config: Dict[str, Any], config_dir: Path, type_override: Optional[str]) -> str:
    """Run validators, print results, and return the detected type. Exits on error."""
    requested_type = cast(Any, type_override) if type_override else None
    detected, errors = validate_config(config, config_dir, requested_type)
    if errors:
        _err(f"Config invalid (detected type: {detected}):", fg=typer.colors.RED if typer else None)
        for err in errors:
            _err(f"  - {err}", fg=typer.colors.RED if typer else None)
        sys.exit(1)
    return detected


# ---------------------------------------------------------------------------
# Audit emit / heartbeat
# ---------------------------------------------------------------------------


def _emit_audit(audit: Any, *, json_output: bool, heartbeat_file: Optional[Path]) -> None:
    """Per-audit side effects: print line + touch the heartbeat file."""
    if json_output:
        # NDJSON on stdout for CI / log shippers.
        print(audit.model_dump_json(), flush=True)
    else:
        if typer:
            status = (
                f"Chunk {audit.chunk_index} | {audit.operation} | "
                f"{audit.rows_processed} rows | {audit.processing_time_sec:.2f}s"
            )
            typer.secho(status, fg=typer.colors.CYAN)
            if audit.failed_sources:
                typer.secho(f"Failures: {audit.failed_sources}", fg=typer.colors.YELLOW)

    if heartbeat_file is not None:
        try:
            heartbeat_file.touch()
        except OSError as exc:
            # Logged once but never fatal — heartbeat is best-effort.
            logger.warning("Could not update heartbeat file %s: %s", heartbeat_file, exc)


# ---------------------------------------------------------------------------
# SIGTERM handling
# ---------------------------------------------------------------------------


def _install_sigterm_handler(shutdown_signal: asyncio.Event) -> None:
    """Wire SIGTERM to set ``shutdown_signal``. Best-effort on Windows."""
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, shutdown_signal.set)
    except (NotImplementedError, RuntimeError):
        # Windows: add_signal_handler isn't supported on the proactor/selector
        # event loops. Falls back to KeyboardInterrupt at the asyncio.run layer.
        pass


# ---------------------------------------------------------------------------
# Stream runner
# ---------------------------------------------------------------------------


async def _run_stream(
    config: Dict[str, Any],
    poll_interval: Optional[float],
    enable_logging: bool,
    json_output: bool,
    heartbeat_file: Optional[Path],
) -> None:
    incorp_params = config.get("incorp_params", {})
    refresh_params = config.get("refresh_params")
    export_params = config.get("export_params")
    stateful_polling = config.get("stateful_polling", False)
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")

    shutdown = asyncio.Event()
    _install_sigterm_handler(shutdown)

    _err("🚀 Starting Incorporator Stream...", fg=typer.colors.GREEN if typer else None)

    stream_gen = LoggedIncorporator.stream(
        incorp_params=incorp_params,
        refresh_params=refresh_params,
        export_params=export_params,
        poll_interval=poll_interval,
        stateful_polling=stateful_polling,
        refresh_interval=refresh_interval,
        export_interval=export_interval,
        enable_logging=enable_logging,
    )

    try:
        async for audit in stream_gen:
            _emit_audit(audit, json_output=json_output, heartbeat_file=heartbeat_file)
            if shutdown.is_set():
                # Polite exit: cancel the underlying generator to trigger its
                # finally-block (daemons drained, queue shut down).
                await stream_gen.aclose()
                break
    except asyncio.CancelledError:
        _err("\n🛑 Stream stopped by user.", fg=typer.colors.YELLOW if typer else None)
    except Exception as e:
        _err(f"\n❌ Fatal Pipeline Error: {e}", fg=typer.colors.RED if typer else None)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Fjord runner — resolves cls_name strings, then delegates to LoggedIncorporator
# ---------------------------------------------------------------------------


def _load_user_module(code_file: Path) -> Any:
    """Import the user's Python file once; return its module object.

    The fjord CLI uses this to resolve Incorporator subclass names (declared
    in JSON as strings) back to actual class objects, and to make the
    ``outflow()`` function available to ``fjord()``.
    """
    import importlib.util

    code_path = code_file.resolve()
    if not code_path.exists():
        _err(f"Error: code_file not found: {code_path}", fg=typer.colors.RED if typer else None)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("_inc_fjord_user_module", code_path)
    if spec is None or spec.loader is None:
        _err(f"Error: Cannot load module spec from: {code_path}", fg=typer.colors.RED if typer else None)
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _resolve_incorporator_class(module: Any, class_name: str, module_path: Path) -> Any:
    """Resolve a class name string to an Incorporator subclass, or exit 1."""
    target = getattr(module, class_name, None)
    if target is None:
        _err(f"Error: class '{class_name}' not found in {module_path}", fg=typer.colors.RED if typer else None)
        sys.exit(1)
    if not isinstance(target, type) or not issubclass(target, Incorporator):
        _err(
            f"Error: '{class_name}' in {module_path} is not an Incorporator subclass.",
            fg=typer.colors.RED if typer else None,
        )
        sys.exit(1)
    return target


async def _run_fjord(
    config: Dict[str, Any],
    config_dir: Path,
    enable_logging: bool,
    json_output: bool,
    heartbeat_file: Optional[Path],
) -> None:
    """Resolve source classes from the code_file, then drive Incorporator.fjord().

    The output class is built dynamically from the code_file's filename
    (snake_case → PascalCase); there is no ``output_class`` JSON key.
    """
    code_file_raw = config["code_file"]
    stream_params_cfg = config["stream_params"]
    export_params = config["export_params"]
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")

    code_file_path = Path(code_file_raw)
    if not code_file_path.is_absolute():
        code_file_path = (config_dir / code_file_path).resolve()

    user_module = _load_user_module(code_file_path)

    resolved_streams: list[Dict[str, Any]] = []
    for entry in stream_params_cfg:
        cls_name = entry["cls_name"]
        resolved_entry = {k: v for k, v in entry.items() if k != "cls_name"}
        resolved_entry["cls"] = _resolve_incorporator_class(user_module, cls_name, code_file_path)
        resolved_streams.append(resolved_entry)

    shutdown = asyncio.Event()
    _install_sigterm_handler(shutdown)

    _err("🌊 Starting Incorporator Fjord...", fg=typer.colors.GREEN if typer else None)

    fjord_gen = LoggedIncorporator.fjord(
        stream_params=resolved_streams,
        code_file=code_file_path,
        export_params=export_params,
        refresh_interval=refresh_interval,
        export_interval=export_interval,
        enable_logging=enable_logging,
    )

    try:
        async for audit in fjord_gen:
            _emit_audit(audit, json_output=json_output, heartbeat_file=heartbeat_file)
            if shutdown.is_set():
                await fjord_gen.aclose()
                break
    except asyncio.CancelledError:
        _err("\n🛑 Fjord stopped by user.", fg=typer.colors.YELLOW if typer else None)
    except Exception as e:
        _err(f"\n❌ Fatal Fjord Error: {e}", fg=typer.colors.RED if typer else None)
        sys.exit(1)


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
            help="Emit one NDJSON AuditResult per line on stdout (status text goes to stderr). For CI/CD pipelines.",
        ),
        heartbeat_file: Optional[Path] = typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every audit; pairs with the Docker HEALTHCHECK.",
        ),
    ) -> None:
        """
        Execute an Autonomous Pipeline Stream from a JSON configuration file.
        """
        global _JSON_OUTPUT_MODE
        _JSON_OUTPUT_MODE = json_output

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
            help="Emit one NDJSON AuditResult per line on stdout (status text goes to stderr). For CI/CD pipelines.",
        ),
        heartbeat_file: Optional[Path] = typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every audit; pairs with the Docker HEALTHCHECK.",
        ),
    ) -> None:
        """
        Execute a Multi-Source Stateful Fjord Pipeline from a JSON configuration file.

        The JSON must declare:
          - code_file (path): Python file with source Incorporator subclasses + a top-level
            outflow(state) function. The filename's stem becomes the output class name
            (snake_case → PascalCase; e.g. coin_market.py → CoinMarket).
          - stream_params (list): one entry per source with cls_name, incorp_params, refresh_params, etc.
          - export_params (dict): destination for the combined output.
          - refresh_interval / export_interval (floats, optional): daemon cadence.
        """
        global _JSON_OUTPUT_MODE
        _JSON_OUTPUT_MODE = json_output

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
        the referenced code_file.

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
    ) -> None:
        """
        Generate a starter pipeline.json (and, for fjord, an outflow.py).

        Refuses to overwrite existing files. After running, fill in the
        placeholders, then ``incorporator validate <path>`` to confirm,
        then ``incorporator stream <path>`` or ``incorporator fjord <path>``.
        """
        try:
            written = write_scaffold(type_, output_dir.resolve())
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
