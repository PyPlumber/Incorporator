"""CLI verb: ``incorporator tideweaver run|validate <file>.json``.

Loads a ``watershed.json``, runs the same structural validator the top-level
``incorporator validate`` command uses (`validate_watershed_config`), and either
prints a clean diagnostic (`validate`) or builds + runs a
:class:`~incorporator.observability.tideweaver.Tideweaver` (`run`).  One ``Tide``
log record is emitted per scheduler pass.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

try:
    import typer as _typer
except ImportError:  # pragma: no cover — orchestrate extra is optional
    _typer = None  # type: ignore[assignment]

from ..observability.tideweaver import Tide, Tideweaver
from ..observability.tideweaver.config import build_watershed

logger = logging.getLogger(__name__)


async def _run_tideweaver(
    config_path: Path,
    *,
    json_output: bool,
    heartbeat_file: Optional[Path],
) -> None:
    """Async runner — pre-flight validate, build the Watershed, emit Tides."""
    # Lazy imports to avoid cli/__init__.py circular load when this module is
    # imported during the cli package's own load.
    from .runners import _load_pipeline_config, _run_validation

    raw_config = _load_pipeline_config(config_path)
    _run_validation(raw_config, config_path.parent.resolve(), type_override="tideweaver")
    watershed = build_watershed(raw_config, config_path.parent.resolve())
    tw = Tideweaver(watershed)
    async for tide in tw.run():
        _emit_tide(tide, json_output=json_output)
        if heartbeat_file is not None:
            try:
                heartbeat_file.touch()
            except OSError as exc:
                logger.warning("Could not update heartbeat file %s: %s", heartbeat_file, exc)


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
        heartbeat_file: Optional[Path] = _typer.Option(  # noqa: B008
            None,
            "--heartbeat-file",
            help="Touch this path after every tide; pairs with Docker HEALTHCHECK.",
        ),
    ) -> None:
        """Execute a Tideweaver watershed from a JSON configuration file."""
        if logs:
            logging.basicConfig(level=logging.INFO)
        if not config.is_file():
            _typer.secho(f"Error: Configuration file not found at {config}", fg=_typer.colors.RED)
            sys.exit(1)
        try:
            asyncio.run(
                _run_tideweaver(
                    config,
                    json_output=json_output,
                    heartbeat_file=heartbeat_file,
                )
            )
        except KeyboardInterrupt:
            _typer.secho("\n🛑 Tideweaver stopped by user.", fg=_typer.colors.YELLOW)
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
        _typer.secho(f"✅ {config} is valid (tideweaver).", fg=_typer.colors.GREEN)

    return tideweaver_app
