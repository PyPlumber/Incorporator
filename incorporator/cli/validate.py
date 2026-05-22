"""Config-shape validation for ``incorporator stream``, ``fjord``, and ``tideweaver``.

Each validator returns a list of human-readable error strings — empty list
means the config is valid. The CLI commands (`validate`, `stream`, `fjord`,
`tideweaver run`) all funnel through these so a developer sees the same
diagnostics whether they ran ``validate`` standalone or hit a failure
mid-execution.

Validation is intentionally **structural**, not behavioural:

- We never make network calls.
- We *do* import the user's ``outflow`` and ``inflow`` files (Step 1 of
  fjord is "load and resolve classes anyway"; running it here surfaces
  ImportErrors at validate-time instead of pipeline-startup-time).
- For ``fjord`` and ``tideweaver`` we confirm the ``outflow(state)`` function
  is defined with the right arity using ``usercode.load_outflow_module``.

The three configs are auto-detected by their distinctive top-level keys —
the developer can override with ``--type stream|fjord|tideweaver``.

D2b implementation: schema rules delegate to the Pydantic models in
:mod:`incorporator.cli._pipeline_config` (stream / fjord) and to
:func:`incorporator.observability.tideweaver.config.build_watershed`
(tideweaver).  Only sidecar-file existence, sidecar-module import, and
``outflow(state)`` arity / ``cls_name`` symbol resolution remain in this
file — they're runtime concerns the Pydantic schemas deliberately don't
cover.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from pydantic import ValidationError

from ..base import Incorporator
from ._pipeline_config import parse_pipeline_config

logger = logging.getLogger(__name__)

ConfigType = Literal["stream", "fjord", "tideweaver"]


def autodetect_type(config: Dict[str, Any]) -> ConfigType:
    """Infer 'stream' / 'fjord' / 'tideweaver' from distinguishing top-level keys.

    Tideweaver configs always declare a ``window`` object plus a ``shape``
    discriminator.  Fjord configs declare ``outflow`` + a list
    ``stream_params``.  Stream configs declare ``incorp_params``.  If
    nothing matches we default to 'stream' (the older, simpler shape) and
    let the validator surface the missing keys.

    When the detected type is ``stream`` but fjord-shaped keys are present
    (``outflow`` and/or ``stream_params``), emit a one-line warning so the
    user spots the structural confusion before the validator surfaces it
    as a deep field error.  Same for stream-shaped keys leaking into a
    Watershed candidate.
    """
    if isinstance(config.get("window"), dict) and isinstance(config.get("shape"), str):
        if "incorp_params" in config or "stream_params" in config:
            logger.warning(
                "Config has Watershed keys (window + shape) AND %s — auto-detecting "
                "as 'tideweaver'.  Drop the stream/fjord keys or pass --type to silence.",
                "incorp_params" if "incorp_params" in config else "stream_params",
            )
        return "tideweaver"
    if "outflow" in config and isinstance(config.get("stream_params"), list):
        return "fjord"
    if "outflow" in config or isinstance(config.get("stream_params"), list):
        logger.warning(
            "Config has fjord-shaped key(s) (%s) but lacks the full pair — "
            "auto-detecting as 'stream'.  Add the missing 'stream_params' / 'outflow' "
            "to make it a fjord, or pass --type to silence.",
            "outflow" if "outflow" in config else "stream_params",
        )
    return "stream"


def validate_config(
    config: Dict[str, Any],
    config_dir: Path,
    config_type: ConfigType | None = None,
) -> Tuple[ConfigType, List[str]]:
    """Run the right validator for ``config`` (auto-detect type if not given).

    Returns (detected_type, errors). An empty error list means the config is
    valid and ready to execute.
    """
    detected = config_type or autodetect_type(config)
    if detected == "fjord":
        return detected, validate_fjord_config(config, config_dir)
    if detected == "tideweaver":
        return detected, validate_watershed_config(config, config_dir)
    return detected, validate_stream_config(config, config_dir)


# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------


def validate_stream_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator stream`` pipeline.json.

    Schema rules delegate to
    :class:`incorporator.cli._pipeline_config.StreamConfig`; only sidecar
    file existence + import-check remain here.
    """
    errors: List[str] = []
    try:
        parse_pipeline_config(config, kind="stream")
    except ValidationError as exc:
        errors.extend(_format_pydantic_errors(exc))

    # Sidecar checks — runtime concerns, deliberately out of the Pydantic model.
    _validate_sidecar_file(config, "inflow", config_dir, errors)
    if config.get("outflow") and config.get("stateful_polling"):
        _validate_sidecar_file(config, "outflow", config_dir, errors)
    export_params = config.get("export_params")
    if isinstance(export_params, dict):
        _validate_sidecar_file(export_params, "outflow", config_dir, errors, file_label="export_params.outflow")
    return errors


# ---------------------------------------------------------------------------
# Fjord
# ---------------------------------------------------------------------------


def validate_fjord_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator fjord`` pipeline.json.

    Schema rules delegate to
    :class:`incorporator.cli._pipeline_config.FjordConfig`; this function
    adds the runtime checks the Pydantic model deliberately skips:
    outflow file import, ``outflow(state)`` arity, and ``cls_name``
    symbol resolution against the loaded outflow module.
    """
    errors: List[str] = []
    try:
        parse_pipeline_config(config, kind="fjord")
    except ValidationError as exc:
        errors.extend(_format_pydantic_errors(exc))
        # If schema is wrong, deeper checks would all fail the same way.
        return errors

    # Optional inflow file — must import cleanly if specified.
    _validate_sidecar_file(config, "inflow", config_dir, errors)

    # Required outflow — capture the loaded module for downstream symbol checks.
    # Bare ``"outflow"`` label (no "file") preserves the fjord-specific wording
    # asserted by tests/test_cli.py::test_cli_fjord_outflow_not_found.
    outflow_path, module = _validate_sidecar_file(
        config, "outflow", config_dir, errors, capture_module=True, file_label="outflow"
    )
    if outflow_path is None or module is None:
        return errors

    # outflow() arity check via usercode.load_outflow_module — it already
    # raises with the right diagnostic if the function is missing or has the
    # wrong signature.
    from ..usercode import load_outflow_module

    try:
        load_outflow_module(outflow_path)
    except (FileNotFoundError, ImportError, ValueError) as exc:
        errors.append(str(exc))

    # Per-entry cls_name resolution — the Pydantic FjordStreamEntry checks
    # the field is a non-empty string, but resolving against the loaded user
    # module is a runtime concern done here.
    stream_params = config.get("stream_params") or []
    if isinstance(stream_params, list):
        for idx, entry in enumerate(stream_params):
            if not isinstance(entry, dict):
                continue
            cls_name = entry.get("cls_name")
            if not isinstance(cls_name, str) or not cls_name:
                continue
            target = getattr(module, cls_name, None)
            if target is None:
                errors.append(f"stream_params[{idx}].cls_name='{cls_name}' is not defined in {outflow_path}.")
            elif not (isinstance(target, type) and issubclass(target, Incorporator)):
                errors.append(
                    f"stream_params[{idx}].cls_name='{cls_name}' in {outflow_path} is not an Incorporator subclass."
                )
    return errors


# ---------------------------------------------------------------------------
# Tideweaver / Watershed
# ---------------------------------------------------------------------------


def validate_watershed_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator tideweaver`` watershed.json.

    Delegates to
    :func:`incorporator.observability.tideweaver.config.build_watershed` —
    constructing the in-memory Watershed exercises every schema rule
    (window timestamps, shape discriminator, gate_mode/flow mutual
    exclusion, sidecar import, class resolution, FlowControl Pydantic
    validation).  Adds the ``outflow(state)`` arity check that
    ``build_watershed`` skips (it uses ``load_user_module`` which doesn't
    enforce arity; the arity check is in
    ``incorporator.usercode.load_outflow_module``).

    Returns one error per failure — usually the first one
    ``build_watershed`` hits.  Multi-error reports are not promised; the
    caller is expected to fix one issue at a time.
    """
    errors: List[str] = []

    # Delegate the whole shape contract to build_watershed.  Side-effect:
    # imports any inflow/outflow sidecars (same as we used to do in this
    # file via _validate_sidecar_file).
    from ..observability.tideweaver import Fjord
    from ..observability.tideweaver.config import build_watershed

    try:
        watershed = build_watershed(config, config_dir)
    except (FileNotFoundError, ValueError, KeyError, TypeError, ValidationError) as exc:
        errors.append(str(exc))
        return errors

    # Arity check for Fjord currents — build_watershed loaded the module via
    # load_user_module which doesn't validate ``outflow(state)`` arity; do
    # that here so the CLI report surfaces it at validate-time instead of
    # mid-tick.
    needs_outflow = any(isinstance(c, Fjord) for c in watershed.currents)
    if needs_outflow and watershed.outflow is not None:
        from ..usercode import load_outflow_module

        try:
            load_outflow_module(watershed.outflow)
        except (FileNotFoundError, ImportError, ValueError) as exc:
            errors.append(str(exc))

    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_VALUE_ERROR_PREFIX = "Value error, "


def _format_pydantic_errors(error: ValidationError) -> List[str]:
    """Convert a :class:`pydantic.ValidationError` into one error string per failure.

    The substring contract callers (and tests) depend on — e.g. a field name
    like ``"incorp_params"`` or ``"refresh_interval"`` appearing in the
    error text — is preserved by including the field path before the
    message.

    ``model_validator``-raised ``ValueError`` come through Pydantic V2 with
    a literal ``"Value error, "`` prefix on the message that adds noise
    without information (the field path already discriminates kind, the
    "Value error" label is redundant in CLI output).  Strip it so users
    see a clean ``"stream_params[0]: missing 'cls'"`` instead of
    ``"stream_params[0]: Value error, missing 'cls'"``.
    """
    out: List[str] = []
    for item in error.errors():
        loc = item.get("loc", ())
        msg = item.get("msg", "")
        if msg.startswith(_VALUE_ERROR_PREFIX):
            msg = msg[len(_VALUE_ERROR_PREFIX) :]
        path = ".".join(str(p) for p in loc) if loc else ""
        out.append(f"{path}: {msg}" if path else str(msg))
    return out


def _resolve_outflow_file(raw: str, config_dir: Path) -> Path:
    """Resolve a sidecar (inflow/outflow) path either as absolute or relative to the config."""
    p = Path(raw)
    if not p.is_absolute():
        p = (config_dir / p).resolve()
    return p


def _try_import(code_path: Path) -> str | None:
    """Try to load the user module. Return error string on failure, None on success."""
    try:
        _import_module(code_path)
        return None
    except Exception as exc:  # noqa: BLE001  — surface to the user verbatim
        return f"{type(exc).__name__}: {exc}"


def _import_module(code_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("_inc_validate_user_module", code_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module spec from {code_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _validate_sidecar_file(
    config: Dict[str, Any],
    key: str,
    config_dir: Path,
    errors: List[str],
    *,
    capture_module: bool = False,
    file_label: str | None = None,
) -> Tuple[Path | None, Any | None]:
    """Validate an optional sidecar-file path declared in ``config[key]``.

    Centralises the three-step check (string type → file exists → imports
    cleanly) shared by every sidecar reference in stream / fjord configs.
    Tideweaver no longer routes through this helper — ``build_watershed``
    imports its own sidecars via ``load_user_module``.

    Args:
        config: Validator-scope config dict (e.g. the top-level config or a
            nested ``export_params``).
        key: The dict key naming the sidecar (``"inflow"``, ``"outflow"``).
        config_dir: Directory used to resolve relative paths.
        errors: Validator's running error list — mutated in place.
        capture_module: When True, load the module once and return it so
            the caller can inspect symbols.  When False the helper validates
            via ``_try_import`` and skips the second exec to avoid running
            the user's sidecar twice.
        file_label: Human label used in error messages.  Defaults to
            ``f"{key} file"`` (matches inflow / stream-outflow wording).
            Pass ``"outflow"`` (no "file") for the fjord wording asserted
            by ``test_cli.py::test_cli_fjord_outflow_not_found``, or
            ``"export_params.outflow"`` for the nested case.

    Returns:
        ``(path, module)``.  ``path`` is the resolved Path when the file
        exists, otherwise ``None``.  ``module`` is the loaded module on
        full success with ``capture_module=True``, otherwise ``None``.
    """
    raw = config.get(key)
    if not raw:
        return None, None
    if not isinstance(raw, str):
        errors.append(f"'{key}', if present, must be a string path.")
        return None, None
    label = file_label if file_label is not None else f"{key} file"
    path = _resolve_outflow_file(raw, config_dir)
    if not path.is_file():
        errors.append(f"{label} not found: {path}")
        return None, None
    if capture_module:
        try:
            module = _import_module(path)
        except Exception as exc:  # noqa: BLE001 — surface as a clean diagnostic
            errors.append(f"{label} failed to import: {type(exc).__name__}: {exc}")
            return None, None
        return path, module
    load_err = _try_import(path)
    if load_err:
        errors.append(f"{label} failed to import: {load_err}")
        return None, None
    return path, None
