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

Shape/schema rules delegate to the Pydantic models in
:mod:`incorporator.cli._pipeline_config` (stream / fjord) and to
:func:`incorporator.observability.tideweaver.config.build_watershed`
(tideweaver).  This module covers the runtime concerns the schemas don't:
sidecar-file existence, sidecar-module import, and ``outflow(state)`` arity /
``cls_name`` symbol resolution.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from ..base import Incorporator
from ..io.config_paths import resolve_config_paths
from ._pipeline_config import parse_pipeline_config

logger = logging.getLogger(__name__)

ConfigType = Literal["stream", "fjord", "tideweaver"]

# All recognised per-current keys. Keys not in this set and not starting with
# '_' trigger a WARNING so users discover typos before they become silent no-ops.
_KNOWN_CURRENT_KEYS: frozenset[str] = frozenset(
    {
        # Current base
        "name",
        "class",
        "verb",
        "interval",
        "depends_on",
        "on_error",
        "phase_offset_sec",
        "inflow",
        "outflow",
        # Stream
        "incorp_params",
        "refresh_params",
        "export_params",
        "parent_current",
        # Fjord
        "parent_currents",
        # Export (shares export_params with Stream/Fjord)
    }
)


def autodetect_type(config: dict[str, Any]) -> ConfigType:
    """Infer 'stream' / 'fjord' / 'tideweaver' from distinguishing top-level keys.

    Tideweaver configs always declare a ``window`` object plus a ``shape``
    discriminator.  Fjord configs declare ``outflow`` + a list
    ``stream_params``.  Stream configs declare ``incorp_params``.  If
    nothing matches we default to 'stream' (the older, simpler shape) and
    let the validator surface the missing keys.
    """
    if isinstance(config.get("window"), dict) and isinstance(config.get("shape"), str):
        return "tideweaver"
    if "outflow" in config and isinstance(config.get("stream_params"), list):
        return "fjord"
    return "stream"


def validate_config(
    config: dict[str, Any],
    config_dir: Path,
    config_type: ConfigType | None = None,
) -> tuple[ConfigType, list[str]]:
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


def validate_stream_config(config: dict[str, Any], config_dir: Path) -> list[str]:
    """Structural validation for an ``incorporator stream`` pipeline.json.

    Schema rules delegate to
    :class:`incorporator.cli._pipeline_config.StreamConfig`; only sidecar
    file existence + import-check remain here.
    """
    errors: list[str] = []
    try:
        parse_pipeline_config(config, kind="stream")
    except ValidationError as exc:
        errors.extend(_format_pydantic_errors(exc))

    # Rebase INPUT paths to config-dir before existence checks — matches what the
    # runtime does via _load_pipeline_config so validate and run agree.
    rebased = resolve_config_paths(config, config_dir)

    # Sidecar checks — runtime concerns, deliberately out of the Pydantic model.
    _validate_sidecar_file(rebased, "inflow", config_dir, errors)
    if rebased.get("outflow") and rebased.get("stateful_polling"):
        _validate_sidecar_file(rebased, "outflow", config_dir, errors)
    export_params = rebased.get("export_params")
    if isinstance(export_params, dict):
        _validate_sidecar_file(export_params, "outflow", config_dir, errors, file_label="export_params.outflow")

    # inc_file / new_file — INPUT source files that must exist at runtime.
    incorp_params = rebased.get("incorp_params")
    if isinstance(incorp_params, dict):
        _validate_input_file(incorp_params, "inc_file", errors, label="incorp_params.inc_file")
    refresh_params = rebased.get("refresh_params")
    if isinstance(refresh_params, dict):
        _validate_input_file(refresh_params, "new_file", errors, label="refresh_params.new_file")

    return errors


# ---------------------------------------------------------------------------
# Fjord
# ---------------------------------------------------------------------------


def validate_fjord_config(config: dict[str, Any], config_dir: Path) -> list[str]:
    """Structural validation for an ``incorporator fjord`` pipeline.json.

    Schema rules delegate to
    :class:`incorporator.cli._pipeline_config.FjordConfig`; this function
    adds the runtime checks the Pydantic model deliberately skips:
    outflow file import, ``outflow(state)`` arity, and ``cls_name``
    symbol resolution against the loaded outflow module.
    """
    errors: list[str] = []
    try:
        parse_pipeline_config(config, kind="fjord")
    except ValidationError as exc:
        errors.extend(_format_pydantic_errors(exc))
        # If schema is wrong, deeper checks would all fail the same way.
        return errors

    # Rebase INPUT paths before existence checks — mirrors runtime resolution.
    rebased = resolve_config_paths(config, config_dir)

    # Optional inflow file — must import cleanly if specified.
    _validate_sidecar_file(rebased, "inflow", config_dir, errors)

    # Required outflow — capture the loaded module for downstream symbol checks.
    # Bare ``"outflow"`` label (no "file") preserves the fjord-specific wording
    # asserted by tests/test_cli.py::test_cli_fjord_outflow_not_found.
    outflow_path, module = _validate_sidecar_file(
        rebased, "outflow", config_dir, errors, capture_module=True, file_label="outflow"
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
    stream_params = rebased.get("stream_params") or []
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


def _collect_current_entries(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract all per-current entry dicts from a raw watershed config.

    Args:
        raw: The top-level watershed config dict (after env-expand; before
            or after token resolution — key names are unchanged either way).

    Returns:
        A flat list of every current entry dict found under the
        recognised positional keys (``head``, ``tail``, ``source`` as
        single-dict entries; ``middle``, ``sinks``, ``currents`` as list
        entries).
    """
    entries: list[dict[str, Any]] = []
    for key in ("head", "tail", "source"):
        value = raw.get(key)
        if isinstance(value, dict):
            entries.append(value)
    for key in ("middle", "sinks", "currents"):
        value = raw.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    entries.append(item)
    return entries


def validate_watershed_config(config: dict[str, Any], config_dir: Path) -> list[str]:
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
    errors: list[str] = []

    # Delegate the whole shape contract to build_watershed (which imports any inflow/outflow sidecars).
    from ..observability.tideweaver import Fjord
    from ..observability.tideweaver.config import build_watershed

    try:
        watershed = build_watershed(config, config_dir)
    except (FileNotFoundError, ValueError, KeyError, TypeError, ValidationError) as exc:
        errors.append(str(exc))
        return errors

    # Unknown-key WARNING: walk every current entry and warn on keys that are
    # not in _KNOWN_CURRENT_KEYS and do not start with '_' (comment/doc keys).
    for entry in _collect_current_entries(config):
        name = entry.get("name", "<unknown>")
        for key in entry:
            if not key.startswith("_") and key not in _KNOWN_CURRENT_KEYS:
                logger.warning(
                    "watershed current %r has unrecognised key %r — possible typo; "
                    "the key was silently ignored by build_watershed.",
                    name,
                    key,
                )

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


def _format_pydantic_errors(error: ValidationError) -> list[str]:
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
    out: list[str] = []
    for item in error.errors():
        loc = item.get("loc", ())
        msg = item.get("msg", "")
        if msg.startswith(_VALUE_ERROR_PREFIX):
            msg = msg[len(_VALUE_ERROR_PREFIX) :]
        path = ".".join(str(p) for p in loc) if loc else ""
        out.append(f"{path}: {msg}" if path else str(msg))
    return out


def _validate_input_file(params: dict[str, Any], key: str, errors: list[str], *, label: str) -> None:
    """Check that an INPUT source file declared in *params[key]* exists on disk.

    The path must already be config-dir-rebased (via
    :func:`incorporator.io.config_paths.resolve_config_paths`) before this
    helper is called.  Only non-empty string values are checked; absent or
    ``None`` values are silently skipped.

    Args:
        params: Dict containing the key (e.g. ``incorp_params``).
        key: The field name to check (e.g. ``"inc_file"``, ``"new_file"``).
        errors: Running error list mutated in place.
        label: Human-readable label for error messages.
    """
    raw = params.get(key)
    if not raw or not isinstance(raw, str):
        return
    p = Path(raw)
    if not p.is_file():
        errors.append(f"{label} not found: {p}")


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
    config: dict[str, Any],
    key: str,
    config_dir: Path,
    errors: list[str],
    *,
    capture_module: bool = False,
    file_label: str | None = None,
) -> tuple[Path | None, Any | None]:
    """Validate an optional sidecar-file path declared in ``config[key]``.

    Centralises the three-step check (string type → file exists → imports
    cleanly) shared by every sidecar reference in stream / fjord configs.

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
    # When the caller passes a rebased config (as stream/fjord validators do),
    # the path is already absolute.  For any remaining relative-path callers
    # (e.g. nested export_params dict) fall back to config_dir resolution.
    p = Path(raw)
    path = p if p.is_absolute() else (config_dir / p).resolve()
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
