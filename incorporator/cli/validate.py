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
"""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from ..base import Incorporator

ConfigType = Literal["stream", "fjord", "tideweaver"]

_TIDEWEAVER_SHAPES = {"chain", "diamond", "fanout", "parallel", "custom"}
_TIDEWEAVER_VERBS = {"stream", "fjord", "export"}
_TIDEWEAVER_ON_ERROR = {"restart", "isolate", "fail_watershed"}
_TIDEWEAVER_EDGE_MODES = {"hard", "soft", "weir"}

# Source keys recognised by incorp() — at least one must be present in
# `incorp_params` for a stream config to be valid.
_STREAM_SOURCE_KEYS = {"inc_url", "inc_file", "inc_parent", "payload_list"}


def autodetect_type(config: Dict[str, Any]) -> ConfigType:
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


def validate_stream_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator stream`` pipeline.json."""
    errors: List[str] = []

    incorp_params = config.get("incorp_params")
    if not isinstance(incorp_params, dict):
        errors.append("'incorp_params' (dict) is required at the top level.")
    else:
        source_keys = _STREAM_SOURCE_KEYS & set(incorp_params)
        if not source_keys:
            errors.append(f"'incorp_params' must contain at least one source key: {sorted(_STREAM_SOURCE_KEYS)}.")

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
                    errors.append(f"'{key}[{src!r}]' must be a number (seconds); got {type(secs).__name__}.")
        elif not isinstance(val, (int, float)):
            errors.append(f"'{key}', if present, must be a number (seconds) or a dict of {{class_name: seconds}}.")

    if "stateful_polling" in config and not isinstance(config["stateful_polling"], bool):
        errors.append("'stateful_polling', if present, must be a boolean.")

    # Optional inflow file — must import cleanly if specified.
    _validate_sidecar_file(config, "inflow", config_dir, errors)

    # Optional outflow — stateful-polling pipelines only.
    if config.get("outflow") and not config.get("stateful_polling"):
        errors.append(
            "'outflow' requires 'stateful_polling': true.  Chunking-mode streams "
            "release per-chunk state and have no persistent registry for a "
            "user-defined Incorporator subclass to attach to.  Drop 'outflow', "
            "or switch to stateful polling."
        )
    elif config.get("outflow"):
        _validate_sidecar_file(config, "outflow", config_dir, errors)

    # Optional export_params.outflow — confirm the file loads (transform optional).
    export_params = config.get("export_params")
    if isinstance(export_params, dict):
        _validate_sidecar_file(export_params, "outflow", config_dir, errors, file_label="export_params.outflow")

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
    # wrong signature.  We discard the loaded module and just surface the
    # ValueError into the report.
    from ..usercode import load_outflow_module

    try:
        load_outflow_module(outflow_path)
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
            errors.append(f"stream_params[{idx}].cls_name='{cls_name}' is not defined in {outflow_path}.")
        elif not (isinstance(target, type) and issubclass(target, Incorporator)):
            errors.append(
                f"stream_params[{idx}].cls_name='{cls_name}' in {outflow_path} is not an Incorporator subclass."
            )

        if not isinstance(entry.get("incorp_params"), dict):
            errors.append(f"stream_params[{idx}] missing 'incorp_params' (dict).")

    return errors


def validate_watershed_config(config: Dict[str, Any], config_dir: Path) -> List[str]:
    """Structural validation for an ``incorporator tideweaver`` watershed.json.

    Checks window shape, the shape-key contract (chain / diamond / fanout /
    parallel / custom), per-current sanity (name uniqueness, verb in the
    allowed set, positive interval, sane ``on_error``), edge endpoints +
    cycles, and imports the outflow / inflow sidecars so user-code
    ImportErrors surface here.  For every Fjord current the resolved
    outflow path is checked for an ``outflow(state)`` callable of arity 1
    (reuses :func:`incorporator.usercode.load_outflow_module`).
    """
    errors: List[str] = []

    # --- window ---------------------------------------------------------
    window = config.get("window")
    if not isinstance(window, dict) or "start" not in window or "end" not in window:
        errors.append("'window' must be an object with 'start' and 'end' ISO 8601 timestamps.")
    else:
        start_dt = _parse_iso_or_none(window.get("start"))
        end_dt = _parse_iso_or_none(window.get("end"))
        if start_dt is None:
            errors.append(f"'window.start' must be an ISO 8601 timestamp; got {window.get('start')!r}.")
        if end_dt is None:
            errors.append(f"'window.end' must be an ISO 8601 timestamp; got {window.get('end')!r}.")
        if start_dt is not None and end_dt is not None and end_dt <= start_dt:
            errors.append(f"'window.end' ({end_dt}) must be after 'window.start' ({start_dt}).")

    # --- shape discriminator -------------------------------------------
    shape = config.get("shape")
    if shape not in _TIDEWEAVER_SHAPES:
        errors.append(f"'shape' must be one of {sorted(_TIDEWEAVER_SHAPES)}; got {shape!r}.")
        return errors  # downstream checks all assume a known shape

    # --- drain_timeout / gate_mode / flow ------------------------------
    # Top-level flow control accepts either ``gate_mode`` (string shorthand)
    # or ``flow`` (full FlowControl dict).  Mutually exclusive; neither is
    # valid with shape='parallel'.  Deep validation of the FlowControl dict
    # is deferred to load time — Pydantic surfaces a clear, structured error
    # there with field paths.
    if "drain_timeout" in config:
        dt = config["drain_timeout"]
        if not isinstance(dt, (int, float)) or dt < 0:
            errors.append("'drain_timeout', if present, must be a non-negative number (seconds).")
    mode_key = "gate_mode" if "gate_mode" in config else "dependency_mode"
    has_mode = mode_key in config
    has_flow = "flow" in config
    if has_mode and has_flow:
        errors.append(f"Pass {mode_key!r} (shorthand) or 'flow' (full dict), not both.")
    if shape == "parallel" and (has_mode or has_flow):
        errors.append(f"{mode_key!r}/'flow' is not valid with shape='parallel' (no edges to govern).")
    if has_mode and config.get(mode_key) not in _TIDEWEAVER_EDGE_MODES:
        errors.append(f"{mode_key!r} must be one of {sorted(_TIDEWEAVER_EDGE_MODES)}.")
    if has_flow and not isinstance(config.get("flow"), dict):
        errors.append("'flow', if present, must be a JSON object (FlowControl dict).")

    # --- sidecars (import-check) ---------------------------------------
    _, inflow_module = _validate_sidecar_file(config, "inflow", config_dir, errors, capture_module=True)
    outflow_path, outflow_module = _validate_sidecar_file(config, "outflow", config_dir, errors, capture_module=True)

    # --- gather currents per shape -------------------------------------
    current_entries: List[Tuple[str, Dict[str, Any]]] = []  # (path-label, entry)
    if shape in ("chain", "parallel", "custom"):
        cs = config.get("currents")
        if not isinstance(cs, list) or not cs:
            errors.append(f"shape='{shape}' requires a non-empty 'currents' list.")
        else:
            for i, entry in enumerate(cs):
                if isinstance(entry, dict):
                    current_entries.append((f"currents[{i}]", entry))
                else:
                    errors.append(f"currents[{i}] must be a JSON object.")
    elif shape == "diamond":
        for key in ("head", "tail"):
            entry = config.get(key)
            if not isinstance(entry, dict):
                errors.append(f"shape='diamond' requires '{key}' to be a current object.")
            else:
                current_entries.append((key, entry))
        middle = config.get("middle")
        if not isinstance(middle, list) or not middle:
            errors.append("shape='diamond' requires a non-empty 'middle' list.")
        else:
            for i, entry in enumerate(middle):
                if isinstance(entry, dict):
                    current_entries.append((f"middle[{i}]", entry))
                else:
                    errors.append(f"middle[{i}] must be a JSON object.")
    elif shape == "fanout":
        source = config.get("source")
        if not isinstance(source, dict):
            errors.append("shape='fanout' requires 'source' to be a current object.")
        else:
            current_entries.append(("source", source))
        sinks = config.get("sinks")
        if not isinstance(sinks, list) or not sinks:
            errors.append("shape='fanout' requires a non-empty 'sinks' list.")
        else:
            for i, entry in enumerate(sinks):
                if isinstance(entry, dict):
                    current_entries.append((f"sinks[{i}]", entry))
                else:
                    errors.append(f"sinks[{i}] must be a JSON object.")

    # --- per-current checks --------------------------------------------
    names: List[str] = []
    for label, entry in current_entries:
        errors.extend(_validate_current_entry(label, entry, outflow_module, inflow_module, config_dir))
        n = entry.get("name")
        if isinstance(n, str):
            names.append(n)
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        errors.append(f"Watershed current names must be unique; duplicates: {dupes}")
    name_set = set(names)

    # --- custom edges ---------------------------------------------------
    edges_to_check: List[Tuple[str, str]] = []
    if shape == "custom":
        edges = config.get("edges", [])
        if not isinstance(edges, list):
            errors.append("shape='custom' requires 'edges' to be a list of {from,to,gate_mode?|flow?} objects.")
        else:
            for i, edge in enumerate(edges):
                if not isinstance(edge, dict):
                    errors.append(f"edges[{i}] must be a JSON object.")
                    continue
                f = edge.get("from")
                t = edge.get("to")
                # Per-edge flow control: ``mode`` / ``gate_mode`` string OR
                # ``flow`` dict — mutually exclusive.  Default = hard.
                edge_mode = edge.get("gate_mode") or edge.get("mode")
                edge_flow = edge.get("flow")
                if not isinstance(f, str) or not isinstance(t, str):
                    errors.append(f"edges[{i}] must have string 'from' and 'to' fields.")
                    continue
                if f not in name_set:
                    errors.append(f"edges[{i}].from references unknown current {f!r}.")
                if t not in name_set:
                    errors.append(f"edges[{i}].to references unknown current {t!r}.")
                if edge_mode is not None and edge_flow is not None:
                    errors.append(f"edges[{i}]: pass 'gate_mode' (string shorthand) or 'flow' (full dict), not both.")
                if edge_mode is not None and edge_mode not in _TIDEWEAVER_EDGE_MODES:
                    errors.append(
                        f"edges[{i}].gate_mode must be one of {sorted(_TIDEWEAVER_EDGE_MODES)}; got {edge_mode!r}."
                    )
                if edge_flow is not None and not isinstance(edge_flow, dict):
                    errors.append(f"edges[{i}].flow, if present, must be a JSON object (FlowControl dict).")
                if isinstance(f, str) and isinstance(t, str) and f in name_set and t in name_set:
                    edges_to_check.append((f, t))

    # --- depends_on -----------------------------------------------------
    for label, entry in current_entries:
        deps = entry.get("depends_on")
        if deps is None:
            continue
        if not isinstance(deps, list):
            errors.append(f"{label}.depends_on must be a list of strings.")
            continue
        for dep in deps:
            if not isinstance(dep, str):
                errors.append(f"{label}.depends_on entries must be strings.")
            elif dep not in name_set:
                errors.append(f"{label}.depends_on references unknown current {dep!r}.")
            elif isinstance(entry.get("name"), str):
                edges_to_check.append((dep, entry["name"]))

    # Shape-derived edges (chain / diamond / fanout) are well-formed by
    # construction — we don't need to repeat the check; toposort below
    # would not catch a problem the constructor would already reject.
    if edges_to_check:
        errors.extend(_detect_cycle(list(name_set), edges_to_check))

    # --- Fjord outflow arity ------------------------------------------
    needs_outflow = any(e.get("verb") == "fjord" for _label, e in current_entries)
    if needs_outflow:
        if outflow_path is None:
            errors.append(
                "At least one Fjord current is declared but no top-level 'outflow' path "
                "is set; fjord-flush ticks need an outflow(state) sidecar."
            )
        elif outflow_path.is_file() and outflow_module is not None:
            from ..usercode import load_outflow_module

            try:
                load_outflow_module(outflow_path)
            except (FileNotFoundError, ImportError, ValueError) as exc:
                errors.append(str(exc))

    return errors


def _validate_current_entry(
    label: str,
    entry: Dict[str, Any],
    outflow_module: Any | None,
    inflow_module: Any | None,
    config_dir: Path,
) -> List[str]:
    """Structural checks on a single current entry."""
    errors: List[str] = []

    name = entry.get("name")
    if not isinstance(name, str) or not name:
        errors.append(f"{label}.name is required and must be a non-empty string.")

    verb = entry.get("verb", "stream")
    if verb not in _TIDEWEAVER_VERBS:
        errors.append(f"{label}.verb must be one of {sorted(_TIDEWEAVER_VERBS)}; got {verb!r}.")

    interval = entry.get("interval")
    if not isinstance(interval, (int, float)) or interval <= 0:
        errors.append(f"{label}.interval must be a positive number (seconds).")

    if "on_error" in entry and entry["on_error"] not in _TIDEWEAVER_ON_ERROR:
        errors.append(f"{label}.on_error must be one of {sorted(_TIDEWEAVER_ON_ERROR)}.")
    if "skip_threshold" in entry:
        errors.append(
            f"{label}.skip_threshold moved to per-edge SurgeBarrier(threshold_multiple=..., action=...) on FlowControl."
        )

    # class string must resolve against inflow/outflow modules.
    cls_name = entry.get("class")
    if not isinstance(cls_name, str) or not cls_name:
        errors.append(f"{label}.class is required and must be a string.")
    elif outflow_module is None and inflow_module is None:
        errors.append(
            f"{label}.class={cls_name!r} references a class but no 'outflow' or 'inflow' "
            "sidecar is declared to resolve it from."
        )
    else:
        resolved = None
        for mod in (outflow_module, inflow_module):
            if mod is None:
                continue
            target = getattr(mod, cls_name, None)
            if isinstance(target, type) and issubclass(target, Incorporator):
                resolved = target
                break
        if resolved is None:
            errors.append(
                f"{label}.class={cls_name!r} is not defined as an Incorporator subclass in the "
                "outflow or inflow sidecar."
            )

    # Per-current inflow/outflow overrides — file must exist if declared.
    for key in ("inflow", "outflow"):
        if key in entry:
            raw = entry[key]
            if not isinstance(raw, str):
                errors.append(f"{label}.{key}, if present, must be a string path.")
                continue
            p = _resolve_outflow_file(raw, config_dir)
            if not p.is_file():
                errors.append(f"{label}.{key} not found: {p}")

    return errors


def _detect_cycle(names: List[str], edges: List[Tuple[str, str]]) -> List[str]:
    """Return [error] if ``edges`` form a cycle over ``names``; else []."""
    indeg: Dict[str, int] = dict.fromkeys(names, 0)
    adj: Dict[str, List[str]] = {n: [] for n in names}
    for f, t in edges:
        if t in indeg:
            indeg[t] += 1
            adj.setdefault(f, []).append(t)
    queue = [n for n in names if indeg[n] == 0]
    visited = 0
    while queue:
        n = queue.pop(0)
        visited += 1
        for m in adj.get(n, []):
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if visited != len(names):
        cyclic = sorted({n for n in names if indeg[n] > 0})
        return [f"Watershed graph has a cycle involving: {cyclic}."]
    return []


def _parse_iso_or_none(value: Any) -> datetime | None:
    """Parse an ISO 8601 timestamp; return ``None`` on any failure."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


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
    cleanly) shared by every config validator (stream / fjord / tideweaver)
    and every sidecar key (``"inflow"``, ``"outflow"``,
    ``"export_params.outflow"``).  Before this helper existed the same
    ~12-line block was inlined seven times across three validators — same
    anti-pattern that produced the duplicate ``load_outflow_function`` /
    ``load_outflow_module`` pair in ``usercode.py``.

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
            ``f"{key} file"`` (matches inflow / stream-outflow / tideweaver
            wording).  Pass ``"outflow"`` (no "file") for the fjord wording
            asserted by ``test_cli.py::test_cli_fjord_outflow_not_found``,
            or ``"export_params.outflow"`` for the nested case.

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
