"""JSON config loader for :class:`Watershed`.

A ``watershed.json`` file describes the full plan declaratively.  The loader
applies the same env-var interpolation and token-resolution pipeline used by
the stream/fjord configs, then dispatches on the ``shape`` key to the
matching :class:`Watershed` constructor.

Class strings (``"class": "LapData"``) resolve against the outflow sidecar
module — the same convention used by ``fjord()``'s CLI runner.  If no outflow
path is set, ``"class"`` strings must reference Incorporator subclasses
imported directly (rare; mostly an escape hatch for tests).
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, cast

from ...base import Incorporator
from ...usercode import load_user_module
from .current import Current, Export, Fjord, Stream
from .watershed import DependencyMode, Edge, Watershed


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
    return _build_watershed(raw, base_dir)


def _build_watershed(raw: Dict[str, Any], base_dir: Path) -> Watershed:
    window = _parse_window(raw.get("window"))
    inflow = _resolve_sidecar(raw.get("inflow"), base_dir)
    outflow = _resolve_sidecar(raw.get("outflow"), base_dir)
    drain_timeout = float(raw.get("drain_timeout", 30.0))

    outflow_module = load_user_module(outflow) if outflow is not None else None
    inflow_module = load_user_module(inflow) if inflow is not None else None

    shape = raw.get("shape", "custom")
    common: Dict[str, Any] = {
        "window": window,
        "inflow": inflow,
        "outflow": outflow,
        "drain_timeout": drain_timeout,
    }

    if shape == "chain":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        mode = cast(DependencyMode, raw.get("dependency_mode", "hard"))
        return Watershed.chain(currents=currents, dependency_mode=mode, **common)

    if shape == "diamond":
        head = _build_current(raw["head"], outflow_module, inflow_module)
        middle = _build_currents(raw.get("middle", []), outflow_module, inflow_module)
        tail = _build_current(raw["tail"], outflow_module, inflow_module)
        mode = cast(DependencyMode, raw.get("dependency_mode", "hard"))
        return Watershed.diamond(head=head, middle=middle, tail=tail, dependency_mode=mode, **common)

    if shape == "fanout":
        source = _build_current(raw["source"], outflow_module, inflow_module)
        sinks = _build_currents(raw.get("sinks", []), outflow_module, inflow_module)
        mode = cast(DependencyMode, raw.get("dependency_mode", "hard"))
        return Watershed.fanout(source=source, sinks=sinks, dependency_mode=mode, **common)

    if shape == "parallel":
        if "dependency_mode" in raw:
            raise ValueError("shape='parallel' does not accept dependency_mode — there are no edges.")
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        return Watershed.parallel(currents=currents, **common)

    if shape == "custom":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module)
        edges = [Edge(from_name=e["from"], to_name=e["to"], mode=e.get("mode", "hard")) for e in raw.get("edges", [])]
        return Watershed(currents=currents, edges=edges, **common)

    raise ValueError(f"Unknown shape: {shape!r}. Expected one of: 'chain', 'diamond', 'fanout', 'parallel', 'custom'.")


def _parse_window(raw: Any) -> Tuple[datetime, datetime]:
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


def _resolve_sidecar(value: Any, base_dir: Path) -> Optional[Path]:
    if value is None:
        return None
    p = Path(value)
    return p if p.is_absolute() else (base_dir / p).resolve()


def _build_currents(
    entries: List[Dict[str, Any]],
    outflow_module: Optional[ModuleType],
    inflow_module: Optional[ModuleType],
) -> List[Current]:
    return [_build_current(e, outflow_module, inflow_module) for e in entries]


def _build_current(
    entry: Dict[str, Any],
    outflow_module: Optional[ModuleType],
    inflow_module: Optional[ModuleType],
) -> Current:
    verb = entry.get("verb", "stream")
    cls = _resolve_class(entry["class"], outflow_module, inflow_module)
    common: Dict[str, Any] = {
        "name": entry["name"],
        "cls": cls,
        "interval": float(entry["interval"]),
    }
    for key in ("depends_on", "on_error", "skip_threshold", "inflow", "outflow"):
        if key in entry:
            common[key] = entry[key]

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


def _resolve_class(
    class_name: str,
    outflow_module: Optional[ModuleType],
    inflow_module: Optional[ModuleType],
) -> type:
    for module in (outflow_module, inflow_module):
        if module is None:
            continue
        target = getattr(module, class_name, None)
        if isinstance(target, type) and issubclass(target, Incorporator):
            return target
    raise ValueError(
        f"watershed.json references class {class_name!r}, but no such Incorporator subclass "
        "was found in the outflow or inflow sidecar modules.  Define the class in your "
        "outflow.py (or inflow.py)."
    )
