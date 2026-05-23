"""JSON config loader for :class:`Watershed`.

A ``watershed.json`` file describes the full plan declaratively.  The loader
applies the same env-var interpolation and token-resolution pipeline used by
the stream/fjord configs, then dispatches on the ``shape`` key to the
matching :class:`Watershed` constructor.

Class strings (``"class": "LapData"``) resolve against the outflow sidecar
module — the same convention used by ``fjord()``'s CLI runner.  If no outflow
path is set, ``"class"`` strings must reference Incorporator subclasses
imported directly (rare escape hatch for non-standard configurations).
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from ...base import Incorporator
from ...usercode import load_user_module
from .current import Current, Export, Fjord, Stream
from .flow import FlowControl, GateMode, flow_from_mode
from .watershed import Edge, Watershed


def load_watershed(path: Path) -> Watershed:
    """Load, env-expand, token-resolve, and construct a :class:`Watershed`.

    Args:
        path: Path to a ``watershed.json`` file.  Relative paths inside the
            JSON (``inflow``, ``outflow``) are resolved against the JSON
            file's parent directory.

    Returns:
        A validated :class:`Watershed` ready for :class:`Tideweaver`.

    Raises:
        FileNotFoundError: ``path`` doesn't exist.
        ValueError: The JSON is malformed, the ``shape`` is unknown, or a
            referenced class can't be resolved.
    """
    # Lazy imports so loading this module never triggers cli/__init__.py,
    # which would create a circular import (cli registers a tideweaver sub-app
    # that imports back into this module).
    from ...cli.envexpand import expand_env
    from ...cli.tokens import resolve_tokens

    if not path.is_file():
        raise FileNotFoundError(f"watershed config not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8"))
    raw = expand_env(raw)
    raw = resolve_tokens(raw)
    if not isinstance(raw, dict):
        raise ValueError(f"watershed.json must be a JSON object at the top level; got {type(raw).__name__}.")

    base_dir = path.parent.resolve()
    return build_watershed(raw, base_dir)


def build_watershed(raw: dict[str, Any], base_dir: Path) -> Watershed:
    window = _parse_window(raw.get("window"))
    inflow = _resolve_sidecar(raw.get("inflow"), base_dir)
    outflow = _resolve_sidecar(raw.get("outflow"), base_dir)
    drain_timeout = float(raw.get("drain_timeout", 30.0))

    outflow_module = load_user_module(outflow) if outflow is not None else None
    inflow_module = load_user_module(inflow) if inflow is not None else None

    shape = raw.get("shape", "custom")
    common: dict[str, Any] = {
        "window": window,
        "inflow": inflow,
        "outflow": outflow,
        "drain_timeout": drain_timeout,
    }

    # Resolve the top-level flow shorthand:
    #   ``gate_mode`` (string) or ``flow`` (full dict) — mutually exclusive.
    def _top_level_flow(raw_obj: dict[str, Any]) -> tuple[GateMode | None, FlowControl | None]:
        if "dependency_mode" in raw_obj:
            raise ValueError(
                "watershed.json: 'dependency_mode' was removed in v1.3.0.  "
                "Rename the key to 'gate_mode' (same accepted values: "
                '"hard" / "soft" / "weir").'
            )
        raw_flow = raw_obj.get("flow")
        raw_mode = raw_obj.get("gate_mode")
        if raw_flow is not None and raw_mode is not None:
            raise ValueError(
                "watershed.json: pass top-level 'flow' (full dict) or 'gate_mode' (string shorthand), not both."
            )
        if raw_flow is not None:
            return None, _build_flow(raw_flow, outflow_module, inflow_module)
        if raw_mode is not None:
            return cast(GateMode, raw_mode), None
        return None, None

    if shape == "chain":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        gate_mode, flow = _top_level_flow(raw)
        if flow is not None:
            return Watershed.chain(currents=currents, flow=flow, **common)
        return Watershed.chain(currents=currents, gate_mode=gate_mode or "hard", **common)

    if shape == "diamond":
        head = _build_current(raw["head"], outflow_module, inflow_module)
        middle = _build_currents(raw.get("middle", []), outflow_module, inflow_module)
        tail = _build_current(raw["tail"], outflow_module, inflow_module)
        gate_mode, flow = _top_level_flow(raw)
        if flow is not None:
            return Watershed.diamond(head=head, middle=middle, tail=tail, flow=flow, **common)
        return Watershed.diamond(head=head, middle=middle, tail=tail, gate_mode=gate_mode or "hard", **common)

    if shape == "fanout":
        source = _build_current(raw["source"], outflow_module, inflow_module)
        sinks = _build_currents(raw.get("sinks", []), outflow_module, inflow_module)
        gate_mode, flow = _top_level_flow(raw)
        if flow is not None:
            return Watershed.fanout(source=source, sinks=sinks, flow=flow, **common)
        return Watershed.fanout(source=source, sinks=sinks, gate_mode=gate_mode or "hard", **common)

    if shape == "parallel":
        if "dependency_mode" in raw:
            raise ValueError(
                "watershed.json: 'dependency_mode' was removed in v1.3.0.  "
                "shape='parallel' has no edges to govern; drop the key entirely."
            )
        if "gate_mode" in raw or "flow" in raw:
            raise ValueError("shape='parallel' does not accept gate_mode/flow — there are no edges to govern.")
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        return Watershed.parallel(currents=currents, **common)

    if shape == "custom":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        edges = []
        for e in raw.get("edges", []):
            if "mode" in e:
                raise ValueError(
                    f"watershed.json edge {e.get('from', '?')}->{e.get('to', '?')}: "
                    "'mode' was removed in v1.3.0.  Rename the key to 'gate_mode' "
                    'in the edge entry (same accepted values: "hard" / "soft" / "weir").'
                )
            raw_flow = e.get("flow")
            raw_mode = e.get("gate_mode")
            if raw_flow is not None and raw_mode is not None:
                raise ValueError(
                    f"edge {e.get('from', '?')}→{e.get('to', '?')}: "
                    "pass 'flow' (full dict) or 'gate_mode' (string shorthand), not both."
                )
            if raw_flow is not None:
                edge_flow = _build_flow(raw_flow, outflow_module, inflow_module)
            elif raw_mode is not None:
                edge_flow = flow_from_mode(cast(GateMode, raw_mode))
            else:
                edge_flow = FlowControl()
            edges.append(Edge(from_name=e["from"], to_name=e["to"], flow=edge_flow))
        return Watershed(currents=currents, edges=edges, **common)

    raise ValueError(f"Unknown shape: {shape!r}. Expected one of: 'chain', 'diamond', 'fanout', 'parallel', 'custom'.")


def _parse_window(raw: Any) -> tuple[datetime, datetime]:
    if not isinstance(raw, dict) or "start" not in raw or "end" not in raw:
        raise ValueError("watershed.json 'window' must be an object with 'start' and 'end' ISO 8601 timestamps.")
    return (_parse_dt(raw["start"]), _parse_dt(raw["end"]))


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # fromisoformat handles 'Z' as of 3.11; explicit replace covers older inputs.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError(f"window timestamps must be ISO-8601 strings; got {type(value).__name__}.")


def _resolve_sidecar(value: Any, base_dir: Path) -> Path | None:
    if value is None:
        return None
    p = Path(value)
    return p if p.is_absolute() else (base_dir / p).resolve()


def _build_currents(
    entries: list[dict[str, Any]],
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> list[Current]:
    return [_build_current(e, outflow_module, inflow_module) for e in entries]


def _build_current(
    entry: dict[str, Any],
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> Current:
    verb = entry.get("verb", "stream")
    cls = _resolve_class(entry["class"], outflow_module, inflow_module)
    common: dict[str, Any] = {
        "name": entry["name"],
        "cls": cls,
        "interval": float(entry["interval"]),
    }
    for key in ("depends_on", "on_error", "phase_offset_sec", "inflow", "outflow"):
        if key in entry:
            common[key] = entry[key]
    if "skip_threshold" in entry:
        raise ValueError(
            f"Current {entry.get('name', '?')!r}: 'skip_threshold' moved to per-edge "
            "SurgeBarrier(threshold_multiple=..., action=...) on FlowControl. "
            "See incorporator.observability.tideweaver.flow.SurgeBarrier."
        )

    if verb == "stream":
        return Stream(
            **common,
            incorp_params=entry.get("incorp_params", {}),
            refresh_params=entry.get("refresh_params"),
            export_params=entry.get("export_params"),
        )
    if verb == "fjord":
        return Fjord(**common, export_params=entry.get("export_params", {}))
    if verb == "export":
        return Export(**common, export_params=entry.get("export_params", {}))
    raise ValueError(
        f"Unknown verb {verb!r} for current {entry.get('name', '?')!r}. Expected one of: 'stream', 'fjord', 'export'."
    )


def _lookup_sidecar_symbol(
    name: str,
    modules: tuple[ModuleType | None, ...],
    predicate: Callable[[Any], bool],
    not_found_message: str,
) -> Any:
    """Walk the sidecar modules in order; return the first symbol matching ``predicate``.

    Single helper behind :func:`_resolve_class` / :func:`_resolve_archive_class`
    / :func:`_resolve_callable`.  Modules with value ``None`` are skipped
    (the caller may have loaded neither inflow nor outflow).  Raises
    ``ValueError(not_found_message)`` when no module yields a match — the
    caller supplies the human-readable wording so each resolver can keep
    its specific guidance.
    """
    for module in modules:
        if module is None:
            continue
        target = getattr(module, name, None)
        if predicate(target):
            return target
    raise ValueError(not_found_message)


def _resolve_class(
    class_name: str,
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> type:
    return cast(
        type,
        _lookup_sidecar_symbol(
            class_name,
            (outflow_module, inflow_module),
            lambda target: isinstance(target, type) and issubclass(target, Incorporator),
            not_found_message=(
                f"watershed.json references class {class_name!r}, but no such Incorporator subclass "
                "was found in the outflow or inflow sidecar modules.  Define the class in your "
                "outflow.py (or inflow.py)."
            ),
        ),
    )


def _resolve_archive_class(
    class_name: str,
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> type:
    """Look up an archive class by name on the sidecar modules.  Need not be an Incorporator subclass."""
    return cast(
        type,
        _lookup_sidecar_symbol(
            class_name,
            (outflow_module, inflow_module),
            lambda target: isinstance(target, type),
            not_found_message=(
                f"watershed.json references archive_cls={class_name!r}, but no such class was found "
                "in the outflow or inflow sidecar modules.  Define the class in your outflow.py "
                "(or inflow.py) — typically an :class:`Incorporator` subclass that an out-of-band "
                "drain will consume from."
            ),
        ),
    )


def _resolve_callable(
    ref: str,
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> Callable[..., Any]:
    """Resolve a callable from a string reference.

    Two forms:

    * ``"fn_name"`` — looked up on the outflow / inflow sidecar modules.
    * ``"package.module:fn_name"`` — imports ``package.module`` then
      ``getattr(module, fn_name)``.

    The bare-name form delegates to :func:`_lookup_sidecar_symbol`; the
    ``module:name`` form is callable-specific (Python classes registered
    in user sidecars are the typical class case; module-path imports are
    the typical callable case).
    """
    if ":" in ref:
        # Module:function path.
        module_path, _, fn_name = ref.partition(":")
        if not module_path or not fn_name:
            raise ValueError(
                f"watershed.json: callable ref {ref!r} has empty module-path or function name. "
                "Expected ``package.module:fn_name``."
            )
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise ValueError(
                f"watershed.json: failed to import module {module_path!r} from callable ref {ref!r}: {exc}"
            ) from exc
        target = getattr(module, fn_name, None)
        if not callable(target):
            raise ValueError(
                f"watershed.json: {ref!r} did not resolve to a callable "
                f"({fn_name!r} on {module_path!r} is {type(target).__name__!r})."
            )
        return cast(Callable[..., Any], target)

    return cast(
        Callable[..., Any],
        _lookup_sidecar_symbol(
            ref,
            (outflow_module, inflow_module),
            callable,
            not_found_message=(
                f"watershed.json references callable {ref!r}, but no such name was found "
                "in the outflow or inflow sidecar modules.  Define the function in your "
                "outflow.py (or inflow.py), or use the ``module.path:fn_name`` form."
            ),
        ),
    )


def _build_flow(
    raw_flow: dict[str, Any],
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
) -> FlowControl:
    """Inflate a :class:`FlowControl` from a JSON dict.

    Resolves the three string-reference fields — ``SignalPenstock.rate_fn``,
    ``ExportToArchive.archive_cls``, and ``SignalObserver.callback`` —
    into Python objects, then delegates to ``FlowControl.model_validate(...)``.
    """
    # 1. SignalPenstock.rate_fn — resolve string → callable.
    pen = raw_flow.get("penstock")
    if isinstance(pen, dict) and pen.get("type") == "signal":
        rate_fn = pen.get("rate_fn")
        if isinstance(rate_fn, str):
            raw_flow = {
                **raw_flow,
                "penstock": {
                    **pen,
                    "rate_fn": _resolve_callable(rate_fn, outflow_module, inflow_module),
                },
            }

    # 2. ExportToArchive.archive_cls — resolve string → class.
    sw = raw_flow.get("spillway")
    if isinstance(sw, dict) and sw.get("type") == "export_to_archive":
        archive = sw.get("archive_cls")
        if isinstance(archive, str):
            raw_flow = {
                **raw_flow,
                "spillway": {
                    **sw,
                    "archive_cls": _resolve_archive_class(archive, outflow_module, inflow_module),
                },
            }

    # 3. SignalObserver.callback — resolve string → callable.
    obs = raw_flow.get("observer")
    if isinstance(obs, dict) and obs.get("type") == "signal":
        callback = obs.get("callback")
        if isinstance(callback, str):
            raw_flow = {
                **raw_flow,
                "observer": {
                    **obs,
                    "callback": _resolve_callable(callback, outflow_module, inflow_module),
                },
            }

    return FlowControl.model_validate(raw_flow)
