"""Config-shape validation for ``incorporator stream`` and ``incorporator fjord``.

Each validator returns a list of human-readable error strings — empty list
means the config is valid. The CLI commands (`validate`, `stream`, `fjord`)
all funnel through these so a developer sees the same diagnostics whether
they ran ``validate`` standalone or hit a failure mid-execution.

Validation is intentionally **structural**, not behavioural:

- We never make network calls.
- We *do* import the user's ``outflow`` and ``inflow`` files (Step 1 of
  fjord is "load and resolve classes anyway"; running it here surfaces
  ImportErrors at validate-time instead of pipeline-startup-time).
- For ``fjord`` we confirm the ``outflow(state)`` function is defined
  with the right arity using ``usercode.load_outflow_function``.

The two configs are auto-detected by their distinctive top-level keys —
the developer can override with ``--type stream|fjord``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from ..base import Incorporator

ConfigType = Literal["stream", "fjord"]

# Source keys recognised by incorp() — at least one must be present in
# `incorp_params` for a stream config to be valid.
_STREAM_SOURCE_KEYS = {"inc_url", "inc_file", "inc_parent", "payload_list"}


def autodetect_type(config: Dict[str, Any]) -> ConfigType:
    """Infer 'stream' vs 'fjord' from the JSON's distinguishing top-level keys.

    Fjord configs always declare ``outflow`` and a list ``stream_params``;
    stream configs declare a dict ``incorp_params``. If neither pattern is
    a clean match we default to 'stream' (the older, simpler shape) — the
    validator will then surface the missing keys.
    """
    if "outflow" in config and isinstance(config.get("stream_params"), list):
        return "fjord"
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
    return detected, validate_stream_config(config, config_dir)


def validate_stream_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator stream`` pipeline.json."""
    errors: List[str] = []

    incorp_params = config.get("incorp_params")
    if not isinstance(incorp_params, dict):
        errors.append("'incorp_params' (dict) is required at the top level.")
    else:
        source_keys = _STREAM_SOURCE_KEYS & set(incorp_params)
        if not source_keys:
            errors.append("'incorp_params' must contain at least one source key: " f"{sorted(_STREAM_SOURCE_KEYS)}.")

    # Optional refresh/export params must be dicts when present.
    for key in ("refresh_params", "export_params"):
        if key in config and not isinstance(config[key], dict):
            errors.append(f"'{key}', if present, must be a JSON object.")

    # Optional interval/poll values must be numeric or, for fjord/stream
    # refresh_interval and export_interval, a dict keyed by class name with
    # numeric values (per-source cadence).  poll_interval is always scalar.
    if "poll_interval" in config and not isinstance(config["poll_interval"], (int, float)):
        errors.append("'poll_interval', if present, must be a number (seconds).")
    for key in ("refresh_interval", "export_interval"):
        if key not in config:
            continue
        val = config[key]
        if isinstance(val, dict):
            for src, secs in val.items():
                if not isinstance(secs, (int, float)):
                    errors.append(
                        f"'{key}[{src!r}]' must be a number (seconds); got {type(secs).__name__}."
                    )
        elif not isinstance(val, (int, float)):
            errors.append(
                f"'{key}', if present, must be a number (seconds) "
                f"or a dict of {{class_name: seconds}}."
            )

    if "stateful_polling" in config and not isinstance(config["stateful_polling"], bool):
        errors.append("'stateful_polling', if present, must be a boolean.")

    # Optional inflow file — must import cleanly if specified.
    inflow_raw = config.get("inflow")
    if inflow_raw:
        if not isinstance(inflow_raw, str):
            errors.append("'inflow', if present, must be a string path.")
        else:
            inflow_path = _resolve_outflow_file(inflow_raw, config_dir)
            if not inflow_path.is_file():
                errors.append(f"inflow file not found: {inflow_path}")
            else:
                load_err = _try_import(inflow_path)
                if load_err:
                    errors.append(f"inflow file failed to import: {load_err}")

    # Optional outflow — stateful-polling pipelines only.
    outflow_raw = config.get("outflow")
    if outflow_raw:
        if not config.get("stateful_polling"):
            errors.append(
                "'outflow' requires 'stateful_polling': true.  Chunking-mode streams "
                "release per-chunk state and have no persistent registry for a "
                "user-defined Incorporator subclass to attach to.  Drop 'outflow', "
                "or switch to stateful polling."
            )
        else:
            outflow_path = _resolve_outflow_file(str(outflow_raw), config_dir)
            if not outflow_path.is_file():
                errors.append(f"outflow file not found: {outflow_path}")
            else:
                load_err = _try_import(outflow_path)
                if load_err:
                    errors.append(f"outflow file failed to import: {load_err}")

    # Optional export_params.outflow — confirm the file loads (transform optional).
    export_params = config.get("export_params")
    if isinstance(export_params, dict):
        export_outflow = export_params.get("outflow")
        if export_outflow:
            resolved = _resolve_outflow_file(export_outflow, config_dir)
            if not resolved.is_file():
                errors.append(f"export_params.outflow not found: {resolved}")
            else:
                load_err = _try_import(resolved)
                if load_err:
                    errors.append(f"export_params.outflow failed to import: {load_err}")

    return errors


def validate_fjord_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator fjord`` pipeline.json."""
    errors: List[str] = []

    outflow_raw = config.get("outflow")
    stream_params = config.get("stream_params")
    export_params = config.get("export_params")

    if not outflow_raw:
        errors.append("'outflow' (path to outflow.py) is required.")
    if not isinstance(stream_params, list) or not stream_params:
        errors.append("'stream_params' must be a non-empty JSON array.")
    if not isinstance(export_params, dict):
        errors.append("'export_params' (dict) is required.")

    # If the basic shape is wrong, stop early — deeper checks would all fail
    # the same way.
    if errors:
        return errors

    # Optional inflow file — must import cleanly if specified.
    inflow_raw = config.get("inflow")
    if inflow_raw:
        if not isinstance(inflow_raw, str):
            errors.append("'inflow', if present, must be a string path.")
        else:
            inflow_path = _resolve_outflow_file(inflow_raw, config_dir)
            if not inflow_path.is_file():
                errors.append(f"inflow file not found: {inflow_path}")
            else:
                load_err_inflow = _try_import(inflow_path)
                if load_err_inflow:
                    errors.append(f"inflow file failed to import: {load_err_inflow}")

    outflow_path = _resolve_outflow_file(str(outflow_raw), config_dir)
    if not outflow_path.is_file():
        errors.append(f"outflow not found: {outflow_path}")
        return errors

    load_err = _try_import(outflow_path)
    if load_err:
        errors.append(f"outflow failed to import: {load_err}")
        return errors

    # Reload via importlib for symbol access (cheap — same module spec).
    module = _import_module(outflow_path)

    # outflow() arity check via usercode.load_outflow_function — it already
    # raises with the right diagnostic if the function is missing or has the
    # wrong signature. We swallow the ValueError into the report.
    from ..usercode import load_outflow_function

    try:
        load_outflow_function(outflow_path)
    except (FileNotFoundError, ImportError, ValueError) as exc:
        errors.append(str(exc))

    # Per-entry checks: cls_name + incorp_params.
    # The early-return above guarantees stream_params is a non-empty list;
    # the `if not isinstance` is a redundant runtime/type guard for mypy.
    if not isinstance(stream_params, list):
        return errors
    for idx, entry in enumerate(stream_params):
        if not isinstance(entry, dict):
            errors.append(f"stream_params[{idx}] must be a JSON object.")
            continue
        cls_name = entry.get("cls_name")
        if not cls_name:
            errors.append(f"stream_params[{idx}] missing 'cls_name' field.")
            continue
        target = getattr(module, cls_name, None)
        if target is None:
            errors.append(f"stream_params[{idx}].cls_name='{cls_name}' is not defined in " f"{outflow_path}.")
        elif not (isinstance(target, type) and issubclass(target, Incorporator)):
            errors.append(
                f"stream_params[{idx}].cls_name='{cls_name}' in {outflow_path} " "is not an Incorporator subclass."
            )

        if not isinstance(entry.get("incorp_params"), dict):
            errors.append(f"stream_params[{idx}] missing 'incorp_params' (dict).")

    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
