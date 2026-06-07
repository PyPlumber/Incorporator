"""Async runners and CLI helpers shared by the ``stream`` and ``fjord`` commands.

Extracted from ``cli/__init__.py`` so the entry-point file can stay focused
on Typer app construction and command registration.  Nothing here imports
Typer at module-import time — the Typer-bound parts live in
``cli/__init__.py`` and call into this module.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any, cast

from incorporator._deps.typer import TYPER as _typer

from .. import Incorporator, LoggedIncorporator
from ._pipeline_config import parse_pipeline_config
from .envexpand import EnvExpansionError, expand_env
from .tokens import TokenResolutionError, resolve_tokens
from .validate import validate_config

logger = logging.getLogger(__name__)

# Set to True by the ``stream`` / ``fjord`` commands when ``--json-output``
# is requested.  Routes the status banners + error messages to stderr so
# stdout stays pure NDJSON.  Outside ``--json-output`` mode (default)
# everything goes to stdout — back-compat with the original CLI output
# and with tests that read ``result.stdout``.
_JSON_OUTPUT_MODE: bool = False


def set_json_output_mode(enabled: bool) -> None:
    """Toggle stderr/stdout routing for ``_err``."""
    global _JSON_OUTPUT_MODE
    _JSON_OUTPUT_MODE = enabled


def _err(msg: str, fg: Any = None) -> None:
    """Print a status or error message.

    Routes to **stderr** when ``--json-output`` is active (so NDJSON on
    stdout stays parseable); otherwise prints to stdout — back-compat
    default for human terminal users and existing test assertions.
    """
    if _typer:
        _typer.secho(msg, fg=fg, err=_JSON_OUTPUT_MODE)


def _red() -> Any:
    return _typer.colors.RED if _typer else None


def _yellow() -> Any:
    return _typer.colors.YELLOW if _typer else None


def _green() -> Any:
    return _typer.colors.GREEN if _typer else None


# ---------------------------------------------------------------------------
# Config loading + env expansion
# ---------------------------------------------------------------------------


def _load_pipeline_config(config_path: Path) -> dict[str, Any]:
    """Load and env-expand a pipeline JSON configuration.

    Env-var and ``${file:...}`` references are resolved at load time so the
    rest of the CLI (and the validators) work against a fully-resolved
    config.  Missing references surface here with a clear error.

    **Validate / run invariant.**  This function is the SINGLE config-loading
    entry point shared by ``incorporator validate <cfg>`` and every run verb
    (``stream`` / ``fjord`` / ``tideweaver run``).  Every step performed here
    — JSON parse, ``${VAR}`` / ``${file:...}`` expansion, inflow sidecar
    load + token resolution — must execute on BOTH paths so ``validate``
    never accepts a config that ``run`` would reject.  Do not introduce a
    "validate-fast" mode that short-circuits any step; surface the same
    errors at both entry points.
    """
    if not config_path.is_file():
        _err(f"Error: Configuration file not found at {config_path}", fg=_red())
        sys.exit(1)

    try:
        with open(config_path, encoding="utf-8") as f:
            parsed = cast(dict[str, Any], json.load(f))
    except json.JSONDecodeError as e:
        _err(f"Error: Invalid JSON in {config_path}: {e}", fg=_red())
        sys.exit(1)

    try:
        expanded = cast(dict[str, Any], expand_env(parsed))
    except EnvExpansionError as e:
        _err(f"Error: env-var expansion failed: {e}", fg=_red())
        sys.exit(1)

    # Load the optional inflow= sidecar once.  Its public symbols extend the
    # token resolver's allow-list, so JSON strings like "@calc_bst" or
    # "calc(my_reducer, 'stats')" resolve to real callables before the engine
    # ever sees the config.  importlib's sys.modules cache absorbs any
    # later re-load via the same path.
    extra_names: dict[str, Any] = {}
    inflow_field = expanded.get("inflow")
    if inflow_field:
        try:
            from ..usercode import extract_public_names, load_user_module

            inflow_path = (config_path.parent / str(inflow_field)).resolve()
            module = load_user_module(inflow_path, name_hint="_inc_cli_inflow")
            extra_names = extract_public_names(module)
        except (FileNotFoundError, ImportError, SyntaxError) as e:
            _err(f"Error: failed to load inflow file {inflow_field!r}: {e}", fg=_red())
            sys.exit(1)

    # Resolve JSON-text tokens (e.g. "@my_pager", "inc(datetime)",
    # "join_all(';')") into real Python objects before the config reaches
    # the engine.  Tokens needing user-defined classes still require an
    # outflow file (fjord pattern).
    try:
        return cast(dict[str, Any], resolve_tokens(expanded, extra_names=extra_names))
    except TokenResolutionError as e:
        _err(f"Error: token resolution failed: {e}", fg=_red())
        sys.exit(1)


def _run_validation(config: dict[str, Any], config_dir: Path, type_override: str | None) -> str:
    """Run validators, print results, and return the detected type. Exits on error."""
    requested_type = cast(Any, type_override) if type_override else None
    detected, errors = validate_config(config, config_dir, requested_type)
    if errors:
        _err(f"Config invalid (detected type: {detected}):", fg=_red())
        for err in errors:
            _err(f"  - {err}", fg=_red())
        sys.exit(1)
    return detected


# ---------------------------------------------------------------------------
# Audit emit / heartbeat
# ---------------------------------------------------------------------------


def _emit_wave(wave: Any, *, json_output: bool, heartbeat_file: Path | None) -> None:
    """Per-wave side effects: print line + touch the heartbeat file.

    The heartbeat touch runs in a ``finally`` block so it fires even when
    serialization of the wave raises — the healthcheck must not be gated
    on the emitter succeeding.
    """
    try:
        if json_output:
            # NDJSON on stdout for CI / log shippers.
            print(wave.model_dump_json(), flush=True)
        else:
            if _typer:
                status = (
                    f"Chunk {wave.chunk_index} | {wave.operation} | "
                    f"{wave.rows_processed} rows | {wave.processing_time_sec:.2f}s"
                )
                _typer.secho(status, fg=_typer.colors.CYAN)
                if wave.failed_sources:
                    _typer.secho(f"Failures: {wave.failed_sources}", fg=_yellow())
    finally:
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
    config: dict[str, Any],
    poll_interval: float | None,
    enable_logging: bool,
    json_output: bool,
    heartbeat_file: Path | None,
) -> None:
    # Gate: enforce the Pydantic schema before the async pipeline boots.
    # Errors surface here instead of mid-pipeline.
    parse_pipeline_config(config, kind="stream")

    incorp_params = config.get("incorp_params", {})
    refresh_params = config.get("refresh_params")
    export_params = config.get("export_params")
    stateful_polling = config.get("stateful_polling", False)
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")
    inflow = config.get("inflow")
    outflow = config.get("outflow")

    shutdown = asyncio.Event()
    _install_sigterm_handler(shutdown)

    _err("Starting Incorporator Stream...", fg=_green())

    stream_gen = LoggedIncorporator.stream(
        incorp_params=incorp_params,
        refresh_params=refresh_params,
        export_params=export_params,
        poll_interval=poll_interval,
        stateful_polling=stateful_polling,
        refresh_interval=refresh_interval,
        export_interval=export_interval,
        enable_logging=enable_logging,
        inflow=inflow,
        outflow=outflow,
    )

    try:
        async for wave in stream_gen:
            _emit_wave(wave, json_output=json_output, heartbeat_file=heartbeat_file)
            if shutdown.is_set():
                # Polite exit: cancel the underlying generator to trigger its
                # finally-block (daemons drained, queue shut down).
                await stream_gen.aclose()
                break
    except asyncio.CancelledError:
        _err("\nStream stopped by user.", fg=_yellow())
    except Exception as e:
        _err(f"\nFatal Pipeline Error: {e}", fg=_red())
        sys.exit(1)


# ---------------------------------------------------------------------------
# Fjord runner — resolves cls_name strings, then delegates to LoggedIncorporator
# ---------------------------------------------------------------------------


def _load_user_module(outflow_path: Path) -> Any:
    """Import the user's outflow.py once; return its module object.

    Thin CLI-friendly wrapper around :func:`incorporator.usercode.load_user_module`:
    exits with code 1 + a readable diagnostic instead of raising.  The fjord
    CLI uses this to resolve ``cls_name`` strings back to Incorporator
    subclasses and to make the ``outflow(state)`` function available to
    :meth:`Incorporator.fjord`.
    """
    from ..usercode import load_user_module as _ucm_load_user_module

    try:
        return _ucm_load_user_module(outflow_path, name_hint="_inc_fjord_user_module")
    except (FileNotFoundError, ImportError, SyntaxError) as exc:
        _err(f"Error: failed to load outflow file {outflow_path}: {exc}", fg=_red())
        sys.exit(1)


def _resolve_incorporator_class(module: Any, class_name: str, module_path: Path) -> Any:
    """Resolve a class name string to an Incorporator subclass, or exit 1."""
    target = getattr(module, class_name, None)
    if target is None:
        _err(f"Error: class '{class_name}' not found in {module_path}", fg=_red())
        sys.exit(1)
    if not isinstance(target, type) or not issubclass(target, Incorporator):
        _err(f"Error: '{class_name}' in {module_path} is not an Incorporator subclass.", fg=_red())
        sys.exit(1)
    return target


async def _run_fjord(
    config: dict[str, Any],
    config_dir: Path,
    enable_logging: bool,
    json_output: bool,
    heartbeat_file: Path | None,
) -> None:
    """Resolve source classes from the outflow file, then drive Incorporator.fjord().

    The output class is built dynamically from the outflow file's filename
    (snake_case → PascalCase); there is no ``output_class`` JSON key.
    """
    # Gate: enforce the Pydantic schema before sidecar import / class resolution.
    parse_pipeline_config(config, kind="fjord")

    outflow_raw = config.get("outflow")
    if outflow_raw is None:
        _err("Error: pipeline.json must declare 'outflow' (path to outflow.py).", fg=_red())
        sys.exit(1)

    stream_params_cfg = config["stream_params"]
    export_params = config["export_params"]
    refresh_interval = config.get("refresh_interval")
    export_interval = config.get("export_interval")

    outflow_path = Path(outflow_raw)
    if not outflow_path.is_absolute():
        outflow_path = (config_dir / outflow_path).resolve()

    user_module = _load_user_module(outflow_path)

    resolved_streams: list[dict[str, Any]] = []
    for entry in stream_params_cfg:
        cls_name = entry["cls_name"]
        resolved_entry = {k: v for k, v in entry.items() if k != "cls_name"}
        resolved_entry["cls"] = _resolve_incorporator_class(user_module, cls_name, outflow_path)
        resolved_streams.append(resolved_entry)

    shutdown = asyncio.Event()
    _install_sigterm_handler(shutdown)

    _err("Starting Incorporator Fjord...", fg=_green())

    fjord_gen = LoggedIncorporator.fjord(
        stream_params=resolved_streams,
        outflow=outflow_path,
        export_params=export_params,
        refresh_interval=refresh_interval,
        export_interval=export_interval,
        enable_logging=enable_logging,
    )

    try:
        async for wave in fjord_gen:
            _emit_wave(wave, json_output=json_output, heartbeat_file=heartbeat_file)
            if shutdown.is_set():
                await fjord_gen.aclose()
                break
    except asyncio.CancelledError:
        _err("\nFjord stopped by user.", fg=_yellow())
    except Exception as e:
        _err(f"\nFatal Fjord Error: {e}", fg=_red())
        sys.exit(1)
