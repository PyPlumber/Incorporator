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

from ..base import Incorporator
from ..io.config_paths import resolve_config_paths
from ..io.penstock import register_host_penstock
from ..usercode import load_user_module, merge_sidecar_extra_names
from .current import Current, Export, Fjord, Stream
from .flow import FlowControl, GateMode, flow_from_mode
from .watershed import Edge, Watershed


def load_watershed(path: Path) -> Watershed:
    """Load, env-expand, token-resolve, and construct a :class:`Watershed`.

    Mirrors the trinity path (:func:`incorporator.usercode.apply_inflow_resolution`):
    the top-level ``inflow``/``outflow`` sidecar modules are loaded BEFORE
    token resolution, so a ``conv_dict`` string anywhere in the config
    (e.g. inside a current's ``incorp_params``) may reference a public
    sidecar helper name, not just the built-in token grammar.

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
    from ..cli.envexpand import expand_env
    from ..cli.tokens import resolve_tokens

    if not path.is_file():
        raise FileNotFoundError(f"watershed config not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8"))
    raw = expand_env(raw)
    if not isinstance(raw, dict):
        raise ValueError(f"watershed.json must be a JSON object at the top level; got {type(raw).__name__}.")

    # base_dir only depends on `path`, so it can be computed before the raw
    # dict is otherwise inspected.  resolve_config_paths is idempotent
    # (already-absolute paths pass through unchanged), so calling it here
    # AND again inside build_watershed is safe — it's needed here to rebase
    # the top-level inflow/outflow sidecar paths before load_user_module can
    # find them, since build_watershed's own rebase runs after this function
    # returns.
    base_dir = path.parent.resolve()
    raw = resolve_config_paths(raw, base_dir)

    inflow_val = raw.get("inflow")
    outflow_val = raw.get("outflow")
    inflow = Path(inflow_val) if isinstance(inflow_val, str) and inflow_val else None
    outflow = Path(outflow_val) if isinstance(outflow_val, str) and outflow_val else None

    # Union outflow-then-inflow public names into one allow-list extension —
    # shared with the CLI's _load_pipeline_config via merge_sidecar_extra_names
    # so both paths resolve conv_dict tokens against the same sidecar symbols.
    # An inflow helper wins over an outflow helper of the same name.
    extra_names = merge_sidecar_extra_names(inflow, outflow)

    raw = resolve_tokens(raw, extra_names=extra_names or None)
    if not isinstance(raw, dict):
        raise ValueError(f"watershed.json must be a JSON object at the top level; got {type(raw).__name__}.")

    return build_watershed(raw, base_dir)


def build_watershed(raw: dict[str, Any], base_dir: Path) -> Watershed:
    # Rebase all INPUT file fields (inflow, outflow, incorp_params.inc_file,
    # inc_files, refresh_params.new_file) relative to the config directory.
    # Called here (in addition to _load_pipeline_config for stream/fjord) so
    # the tideweaver-specific nested current entries are also covered.
    # Idempotent: already-absolute paths from a prior call pass through.
    raw = resolve_config_paths(raw, base_dir)
    _register_host_penstocks(raw.get("host_penstocks"))
    window = _parse_window(raw.get("window"))
    inflow_val = raw.get("inflow")
    outflow_val = raw.get("outflow")
    inflow = Path(inflow_val) if isinstance(inflow_val, str) and inflow_val else None
    outflow = Path(outflow_val) if isinstance(outflow_val, str) and outflow_val else None
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
            return GateMode(raw_mode), None
        return None, None

    if shape == "chain":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module, base_dir)
        gate_mode, flow = _top_level_flow(raw)
        if flow is not None:
            return Watershed.chain(currents=currents, flow=flow, **common)
        return Watershed.chain(currents=currents, gate_mode=gate_mode or "hard", **common)

    if shape == "diamond":
        head = _build_current(raw["head"], outflow_module, inflow_module, base_dir)
        middle = _build_currents(raw.get("middle", []), outflow_module, inflow_module, base_dir)
        tail = _build_current(raw["tail"], outflow_module, inflow_module, base_dir)
        gate_mode, flow = _top_level_flow(raw)
        if flow is not None:
            return Watershed.diamond(head=head, middle=middle, tail=tail, flow=flow, **common)
        return Watershed.diamond(head=head, middle=middle, tail=tail, gate_mode=gate_mode or "hard", **common)

    if shape == "fanout":
        source = _build_current(raw["source"], outflow_module, inflow_module, base_dir)
        sinks = _build_currents(raw.get("sinks", []), outflow_module, inflow_module, base_dir)
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
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module, base_dir)
        return Watershed.parallel(currents=currents, **common)

    if shape == "custom":
        currents = _build_currents(raw.get("currents", []), outflow_module, inflow_module, base_dir)
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
                edge_flow = flow_from_mode(GateMode(raw_mode))
            else:
                edge_flow = FlowControl()
            edges.append(Edge(from_name=e["from"], to_name=e["to"], flow=edge_flow))
        return Watershed(currents=currents, edges=edges, **common)

    raise ValueError(f"Unknown shape: {shape!r}. Expected one of: 'chain', 'diamond', 'fanout', 'parallel', 'custom'.")


def _register_host_penstocks(raw: Any) -> None:
    """Register the ``host_penstocks`` block against the HOST-layer registry.

    Shape: ``{"<host>": {"rate_per_sec": <float>, "burst": <int, optional>}, ...}``
    — shorthand only.  Full :class:`~incorporator.io.penstock.Penstock` subclass
    declarations (``WindowPenstock`` / ``SignalPenstock`` / ``BackpressurePenstock``)
    are not expressible from JSON; call :func:`register_host_penstock` directly
    from a sidecar module (which already runs at load time) for those.

    This configures the HOST layer (:mod:`incorporator.io.penstock`'s global
    registry, keyed by hostname) — distinct from the per-edge ``flow.penstock``
    block (:func:`_build_flow`), which configures one edge's own throttling.
    The two layers do not stack: a current's ``incorp_params.requests_per_second``
    short-circuits the registry entirely for that source (see
    :func:`incorporator.io.penstock.resolve_penstock`'s precedence order).

    Registration is a plain dict overwrite (see
    :func:`~incorporator.io.penstock.register_host_penstock`), so calling this
    (and therefore :func:`build_watershed`) more than once on the same config —
    e.g. ``validate`` followed by ``run`` — is harmless; the second call just
    re-writes identical values.  No dedup/guard logic is needed or added.

    Args:
        raw: The top-level ``host_penstocks`` value (``None`` when absent).

    Raises:
        ValueError: ``raw`` is not a dict, a per-host spec is not a dict, or
            a per-host spec is missing ``rate_per_sec``.
    """
    if raw is None:
        return
    if not isinstance(raw, dict):
        raise ValueError(
            f"watershed.json 'host_penstocks' must be an object keyed by hostname; got {type(raw).__name__}."
        )
    for host, spec in raw.items():
        if not isinstance(spec, dict):
            raise ValueError(f"watershed.json host_penstocks[{host!r}] must be an object; got {type(spec).__name__}.")
        if "rate_per_sec" not in spec:
            raise ValueError(f"watershed.json host_penstocks[{host!r}] is missing required key 'rate_per_sec'.")
        burst = spec.get("burst")
        register_host_penstock(
            host,
            rate_per_sec=float(spec["rate_per_sec"]),
            burst=int(burst) if burst is not None else None,
        )


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


def _build_currents(
    entries: list[dict[str, Any]],
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
    base_dir: Path,
) -> list[Current]:
    return [_build_current(e, outflow_module, inflow_module, base_dir) for e in entries]


def _build_current(
    entry: dict[str, Any],
    outflow_module: ModuleType | None,
    inflow_module: ModuleType | None,
    base_dir: Path,
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
            "See incorporator.tideweaver.flow.SurgeBarrier."
        )

    if verb == "stream":
        # incorp_params.inc_file is already config-dir-absolute — resolve_config_paths
        # ran at the top of build_watershed before this current is built.
        incorp_params: dict[str, Any] = dict(entry.get("incorp_params", {}))
        return Stream(
            **common,
            incorp_params=incorp_params,
            refresh_params=entry.get("refresh_params"),
            export_params=entry.get("export_params"),
            parent_current=entry.get("parent_current"),
        )
    if verb == "fjord":
        return Fjord(
            **common,
            export_params=entry.get("export_params", {}),
            parent_currents=entry.get("parent_currents", []),
        )
    if verb == "export":
        return Export(**common, export_params=entry.get("export_params", {}))
    if verb == "custom":
        raise ValueError(
            f"Current {entry.get('name', '?')!r} uses verb='custom', which cannot be declared in watershed.json. "
            "CustomCurrent subclasses require a Python tick() body — register them via the Python API directly "
            "(e.g. watershed.currents.append(MyCurrent(...))) instead of through the JSON config."
        )
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
