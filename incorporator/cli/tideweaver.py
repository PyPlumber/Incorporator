"""CLI verb: ``incorporator tideweaver run|validate <file>.json``.

Loads a ``watershed.json``, runs the same structural validator the top-level
``incorporator validate`` command uses (`validate_watershed_config`), and either
prints a clean diagnostic (`validate`) or builds + runs a
:class:`~incorporator.tideweaver.Tideweaver` (`run`).  One ``Tide``
log record is emitted per scheduler pass.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from incorporator._deps.typer import TYPER as _typer

from ..io.config_paths import resolve_output_path
from ..tideweaver import LoggedTideweaver, Tide, Tideweaver
from ..tideweaver.config import build_watershed

logger = logging.getLogger(__name__)


def _resolve_drain_timeout(cli_override: float | None) -> float | None:
    """Compose the drain-timeout from CLI flag → env-var → watershed.json fallback.

    Precedence (highest first):
      1. ``--drain-timeout`` CLI flag (if set).
      2. ``INCORPORATOR_DRAIN_TIMEOUT`` env-var (container-friendly default).
      3. ``None`` → use the watershed.json ``drain_timeout`` field (or its
         Pydantic default of 30s).

    The env-var is the canonical knob for Docker / Kubernetes deployments:
    set it in ``docker-compose.yml``'s ``environment:`` block to a value
    matching the orchestrator's ``stop_grace_period`` so SIGTERM-then-SIGKILL
    cycles don't truncate in-flight drains.
    """
    if cli_override is not None:
        return cli_override
    env_val = os.environ.get("INCORPORATOR_DRAIN_TIMEOUT")
    if env_val is None:
        return None
    try:
        return float(env_val)
    except ValueError:
        logger.warning(
            "INCORPORATOR_DRAIN_TIMEOUT=%r is not a valid float; ignoring and "
            "falling back to watershed.json drain_timeout.",
            env_val,
        )
        return None


async def _run_tideweaver(
    config_path: Path,
    *,
    json_output: bool,
    heartbeat_file: Path | None,
    drain_timeout_override: float | None = None,
    logs: bool = False,
) -> None:
    """Async runner — pre-flight validate, build the Watershed, emit Tides.

    Args:
        config_path: Path to the watershed.json configuration file.
        json_output: When ``True``, emit one NDJSON :class:`~incorporator.tideweaver.Tide`
            per line on stdout instead of the human-readable coloured summary.
        heartbeat_file: When set, touch this path after every Tide so a Docker
            ``HEALTHCHECK`` can monitor liveness.
        drain_timeout_override: Override the watershed.json ``drain_timeout`` field.
            ``None`` leaves the value set by the JSON (or its Pydantic default of 30s).
        logs: When ``True``, wrap the scheduler in
            :class:`~incorporator.tideweaver.LoggedTideweaver` so every
            :class:`Tide` and :class:`~incorporator.RejectEntry` is routed to disk via
            the :class:`~logging.handlers.QueueHandler`-backed background thread.
            When ``False`` (default), a bare :class:`~incorporator.tideweaver.Tideweaver`
            is used with no disk I/O.
    """
    # Lazy imports to avoid cli/__init__.py circular load when this module is
    # imported during the cli package's own load.
    from .runners import _load_pipeline_config, _run_validation

    raw_config = _load_pipeline_config(config_path)
    _run_validation(raw_config, config_path.parent.resolve(), type_override="tideweaver")
    watershed = build_watershed(raw_config, config_path.parent.resolve())
    if drain_timeout_override is not None:
        # Watershed is not frozen — direct assignment is the contract here.
        watershed.drain_timeout = drain_timeout_override

    # Route the heartbeat file through resolve_output_path so its parent
    # directory is auto-created and the path is CWD/WORKDIR-relative (not
    # config-dir-relative — heartbeat is an OUTPUT, not an INPUT source).
    resolved_hb: Path | None = resolve_output_path(heartbeat_file) if heartbeat_file is not None else None

    tw: Tideweaver
    if logs:
        tw = LoggedTideweaver(watershed, enable_logging=True, logger_name=watershed.name or "Tideweaver")
    else:
        tw = Tideweaver(watershed)
    async for tide in tw.run():
        try:
            _emit_tide(tide, json_output=json_output)
        finally:
            if resolved_hb is not None:
                try:
                    resolved_hb.touch()
                except OSError as exc:
                    logger.warning("Could not update heartbeat file %s: %s", resolved_hb, exc)

    # When a current produced zero rows because a source failed to load, report a
    # summary and exit non-zero so callers (CI, schedulers) can detect it. A run
    # that legitimately returned empty data records no SourceLoadFailure reject and
    # exits 0.
    source_failures = [r for r in tw.rejects if r.error_kind == "SourceLoadFailure"]
    if source_failures:
        failure_lines = "; ".join(str(r) for r in source_failures)
        logger.warning(
            "Tideweaver: run completed with %d source-load failure(s) — %s. Check inc_file paths and the error log.",
            len(source_failures),
            failure_lines,
        )
        sys.exit(1)


def _emit_tide(tide: Tide, *, json_output: bool) -> None:
    if json_output:
        print(tide.model_dump_json(), flush=True)
        return
    if _typer is None:
        return
    status = (
        f"Tide {tide.tide_number} | fired: {','.join(tide.fired) or '-'} | "
        f"skipped: {len(tide.skipped)} | {tide.duration_sec:.3f}s"
    )
    _typer.secho(status, fg=_typer.colors.CYAN)


def build_app() -> Any:
    """Build the ``tideweaver`` Typer sub-app with ``run`` and ``validate`` commands.

    Returns ``None`` if Typer is not installed so ``cli/__init__.py`` can skip
    registration cleanly under the same ``try/except ImportError`` guard it
    already uses for the top-level app.
    """
    if _typer is None:
        return None
    tideweaver_app: Any = _typer.Typer(
        name="tideweaver",
        help="Run or validate a watershed.json: orchestrate streams + fjord flushes + exports.",
        no_args_is_help=True,
    )

    @tideweaver_app.command("run")  # type: ignore[untyped-decorator]
    def run(
        config: Path = _typer.Argument(..., help="Path to the watershed.json configuration file."),  # noqa: B008
        logs: bool = _typer.Option(False, "--logs", help="Enable background multiplex disk logging."),  # noqa: B008
        json_output: bool = _typer.Option(  # noqa: B008
            False, "--json-output", help="Emit one NDJSON Tide per line on stdout."
        ),
        heartbeat_file: Path | None = _typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every tide; pairs with Docker HEALTHCHECK.",
        ),
        drain_timeout: float | None = _typer.Option(  # noqa: B008
            None,
            "--drain-timeout",
            help=(
                "Override watershed.json drain_timeout (seconds) — how long the "
                "scheduler waits for in-flight ticks to finish before exiting on "
                "window close or SIGTERM.  Falls back to INCORPORATOR_DRAIN_TIMEOUT "
                "env-var, then to the JSON value, then to 30s.  Set this to match "
                "your container orchestrator's stop_grace_period."
            ),
        ),
    ) -> None:
        """Execute a Tideweaver watershed from a JSON configuration file."""
        if not config.is_file():
            _typer.secho(f"Error: Configuration file not found at {config}", fg=_typer.colors.RED)
            sys.exit(1)
        try:
            asyncio.run(
                _run_tideweaver(
                    config,
                    json_output=json_output,
                    heartbeat_file=heartbeat_file,
                    drain_timeout_override=_resolve_drain_timeout(drain_timeout),
                    logs=logs,
                )
            )
        except KeyboardInterrupt:
            _typer.secho("\nTideweaver stopped by user.", fg=_typer.colors.YELLOW)
        except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
            _typer.secho(f"Error: {exc}", fg=_typer.colors.RED)
            sys.exit(1)

    @tideweaver_app.command("validate")  # type: ignore[untyped-decorator]
    def validate(
        config: Path = _typer.Argument(..., help="Path to the watershed.json file to validate."),  # noqa: B008
    ) -> None:
        """Validate a watershed.json without executing anything.

        Resolves ${VAR} / ${file:...} references, checks the shape contract
        and every current's required keys, imports sidecars, and confirms
        any Fjord current's outflow(state) function arity.  Exits 0 if
        valid, 1 with a diagnostic report otherwise.
        """
        from .runners import _load_pipeline_config, _run_validation

        if not config.is_file():
            _typer.secho(f"Error: Configuration file not found at {config}", fg=_typer.colors.RED)
            sys.exit(1)
        raw_config = _load_pipeline_config(config)
        _run_validation(raw_config, config.parent.resolve(), type_override="tideweaver")
        _typer.secho(f"{config} is valid (tideweaver).", fg=_typer.colors.GREEN)

    return tideweaver_app
