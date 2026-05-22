"""Multi-source orchestration probe — the engine behind ``cls.architect()``.

Probes N sources (URLs / files / dict-form incorp kwargs), runs cross-source
analysis on the captured :class:`SourceProfile` bundles, and emits a
paste-ready scaffold in one of three formats:

* ``"report"`` — pretty-printed report (extends inspector's 5-section output
  with a cross-source ``ORCHESTRATION HINTS`` section).
* ``"python"`` — self-contained Python snippet (class defs + ``Watershed.<shape>(...)``
  + ``Tideweaver(...).run()``).
* ``"json"`` — complete ``watershed.json`` body, round-trippable through
  :func:`incorporator.observability.tideweaver.config.build_watershed`.

Detection is shared with :func:`incorporator.Incorporator.test` via the
inspector module's :func:`incorporator.tools.inspector.capture_signals`
sidechannel: ``test()`` prints the per-source report, ``architect`` captures
the same :class:`~incorporator.tools.inspector.SourceProfile` and runs
cross-source analysis on top.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Type, Union, cast
from urllib.parse import urlparse

from ...io.penstock import known_host_rates
from ...io.source_ref import SourceRef
from ...tools.inspector import ResponseMeta, SourceProfile, analyze_data

# Mapping-typed source values may be runtime dicts (Mapping) — we accept any
# Mapping for input, but the internal source list normalises to Dict[str, Any].
# Kept as the architect public input alias; internal classification routes
# through :class:`incorporator.io.source_ref.SourceRef` after the loose union
# input is narrowed.
SourceValue = Union[str, Path, Mapping[str, Any]]


# ---------------------------------------------------------------------------
# Plan dataclasses — what _analyze_topology produces and the renderers consume.
# ---------------------------------------------------------------------------


@dataclass
class PenstockSpec:
    """Architect's per-edge ``Penstock`` recommendation.

    Three-tier confidence:

    * ``"high"`` — host hit ``_KNOWN_API_RATE_LIMITS``.
    * ``"medium"`` — 429 observed during the probe.
    * ``"low"`` — never produced (architect emits ``None`` for unknowns).
    """

    kind: str  # "sustained" | "burst" | "window" | "backpressure" | "signal"
    rate_per_sec: float
    confidence: str
    rationale: str


@dataclass
class CurrentSpec:
    """Architect's per-source ``Current`` recommendation.

    Carries everything a renderer needs to emit either a Python ``Stream(...)``
    constructor call or a JSON ``{"verb": "stream", "incorp_params": {...}}``
    block: the incorp kwargs, the inferred pk / conv_dict / excl_lst, and
    the pagination paginator suggestion.
    """

    name: str
    verb: str  # "stream" | "fjord" | "export"
    incorp_params: Dict[str, Any]
    pk_field: Optional[str]
    name_field: Optional[str]
    conv_dict_template: Dict[str, str]  # field name -> "datetime" | "int" | "float"
    excl_lst: List[str]
    inc_page_suggestion: Optional[str]  # paginator call expression
    interval_hint: int
    class_name: str  # PascalCase class name for Python emission


@dataclass
class EdgeSpec:
    """Architect's per-edge recommendation in the orchestration plan."""

    from_name: str
    to_name: str
    gate_mode: str  # "hard" | "soft" | "weir"
    penstock: Optional[PenstockSpec]


@dataclass
class OrchestrationPlan:
    """Cross-source recommendation produced by :func:`_analyze_topology`.

    Renderers dispatch on :attr:`shape` to pick the right scaffold; the
    :meth:`to_watershed` method materialises an in-memory
    :class:`~incorporator.observability.tideweaver.Watershed` so callers
    can probe → tune → run in one expression instead of round-tripping
    through disk.
    """

    shape: str  # "parallel" | "diamond" | "fanout" | "custom"
    currents: List[CurrentSpec]
    edges: List[EdgeSpec]
    shape_rationale: str
    needs_tail_current: bool = False
    notes: List[str] = field(default_factory=list)

    def to_watershed(
        self,
        window: Optional[Tuple[Any, Any]] = None,
        *,
        classes: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Materialise the plan into a runnable :class:`Watershed`.

        Args:
            window: ``(start, end)`` UTC datetimes.  Defaults to ``(now,
                now + 1h)``.  Pass an explicit window for production.
            classes: Optional ``name -> Incorporator subclass`` mapping.
                Used when the caller already has the classes the
                ``CurrentSpec``\\s point at (typical when ``architect``
                was invoked on a user's own subclass tree).  Names
                missing from the mapping get an anonymous subclass of
                :class:`Incorporator` whose ``__name__`` matches
                :attr:`CurrentSpec.class_name`.

        Returns:
            A validated :class:`~incorporator.observability.tideweaver.Watershed`
            that can be handed straight to ``Tideweaver(watershed).run()``.

        Raises:
            ValueError: when :attr:`needs_tail_current` is ``True`` and
                the caller hasn't nominated a tail Current via the
                ``classes`` mapping — the diamond shape requires a Fjord
                to fuse the merging upstreams.
        """
        from datetime import datetime, timedelta, timezone

        from ...base import Incorporator
        from .current import Current, Export, Fjord, Stream
        from .flow import SustainedPenstock, flow_from_mode
        from .watershed import Edge, Watershed

        if window is None:
            now = datetime.now(timezone.utc)
            window = (now, now + timedelta(hours=1))

        if self.needs_tail_current and not (classes and any(spec.verb == "fjord" for spec in self.currents)):
            raise ValueError(
                f"OrchestrationPlan.to_watershed: shape={self.shape!r} requires a tail "
                "Fjord current.  Re-run architect() with a {'verb': 'fjord'} entry in "
                "sources, or pass a classes={...} mapping that maps one of the current "
                "names to a Fjord-typed class."
            )

        verb_to_class = {"stream": Stream, "fjord": Fjord, "export": Export}

        # Resolve user-supplied or fabricate per-spec Incorporator subclasses.
        resolved_classes: Dict[str, Any] = dict(classes) if classes else {}
        built_currents: Dict[str, Current] = {}
        for spec in self.currents:
            inc_cls = resolved_classes.get(spec.name)
            if inc_cls is None:
                inc_cls = type(spec.class_name, (Incorporator,), {})
            current_cls = verb_to_class[spec.verb]
            built_currents[spec.name] = current_cls(
                name=spec.name,
                cls=inc_cls,
                interval=spec.interval_hint,
                incorp_params=dict(spec.incorp_params),
            )

        # Materialise edges with the recommended FlowControl shape.  ``Edge``
        # accepts ``gate_mode=`` (shorthand) XOR ``flow=`` (full dict); pick
        # the shorthand when there's no per-edge penstock and the full form
        # when the architect recommended a tier-1/2 Penstock.
        built_edges: List[Edge] = []
        for edge_spec in self.edges:
            edge_kwargs: Dict[str, Any] = {
                "from_name": edge_spec.from_name,
                "to_name": edge_spec.to_name,
            }
            if edge_spec.penstock is not None and edge_spec.penstock.kind == "sustained":
                # Build the FlowControl from gate_mode + the recommended penstock.
                base_flow = flow_from_mode(edge_spec.gate_mode)  # type: ignore[arg-type]
                edge_kwargs["flow"] = base_flow.model_copy(
                    update={
                        "penstock": SustainedPenstock(
                            rate_per_sec=edge_spec.penstock.rate_per_sec,
                        ),
                    }
                )
            else:
                edge_kwargs["gate_mode"] = edge_spec.gate_mode
            built_edges.append(Edge(**edge_kwargs))

        return Watershed(
            window=window,
            currents=list(built_currents.values()),
            edges=built_edges,
            drain_timeout=30.0,
        )


# ---------------------------------------------------------------------------
# Source-value resolution.
# ---------------------------------------------------------------------------


def _resolve_sources(
    sources: Mapping[str, SourceValue],
    shared_kwargs: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Convert the user's source mapping into ``[(name, incorp_kwargs), ...]``.

    Per-source value forms:

    1. ``Path`` instance, or string starting with ``./`` / ``../`` / ``/`` / ``~``,
       or any string matching an existing local file → ``inc_file=``.
    2. ``http://`` / ``https://`` URL string → ``inc_url=``.
    3. ``Mapping`` of incorp() kwargs → spread verbatim (escape hatch).

    ``shared_kwargs`` are applied to every probe BEFORE per-source kwargs
    land, so a single ``timeout=10.0`` or ``headers={...}`` propagates
    without forcing the escape-hatch form.  Per-source kwargs win on
    conflict.

    Internal: classifies each value as a :class:`SourceRef` (architect's
    strict mode — bare strings that don't look like paths or URLs raise
    immediately) and then converts the ref to its ``incorp()`` kwargs
    form.  Routing through ``SourceRef`` keeps the "what kind of source
    is this?" dispatch consistent with the fetch layer's
    :func:`_normalize_source_list`.
    """
    shared = dict(shared_kwargs) if shared_kwargs else {}
    resolved: List[Tuple[str, Dict[str, Any]]] = []
    for name, value in sources.items():
        ref = _classify_source(name, value)
        resolved.append((name, {**shared, **_ref_to_kwargs(ref)}))
    return resolved


def _classify_source(name: str, value: Any) -> SourceRef:
    """Architect's strict source classifier — narrows the loose input union to a SourceRef.

    Stricter than :meth:`SourceRef.parse`: a bare string is only a file
    if it starts with one of ``./``, ``../``, ``/``, ``~`` OR an existing
    file lives at that path.  Anything else raises ``ValueError`` so
    typos in source names surface immediately at architect()-time
    instead of failing mid-probe.
    """
    if isinstance(value, Mapping):
        return SourceRef.from_kwargs(value)
    if isinstance(value, Path):
        return SourceRef.from_file(value)
    if isinstance(value, str):
        if value.startswith(("http://", "https://")):
            return SourceRef.from_url(value)
        if value.startswith(("./", "../", "/", "~")) or Path(value).exists():
            return SourceRef.from_file(value)
        raise ValueError(
            f"Cannot resolve source {name!r}={value!r}.  Accepted forms: "
            "URL string (http(s)://...), file path or pathlib.Path "
            "(must exist or start with ./ ../ / or ~), "
            "or a dict of incorp() kwargs."
        )
    raise ValueError(
        f"Cannot resolve source {name!r}={value!r}.  Accepted forms: "
        "URL string, file path / pathlib.Path, or dict of incorp() kwargs."
    )


def _ref_to_kwargs(ref: SourceRef) -> Dict[str, Any]:
    """Map a :class:`SourceRef` into the ``incorp()`` kwargs it represents."""
    if ref.kind == "url":
        return {"inc_url": ref.value}
    if ref.kind == "file":
        return {"inc_file": str(ref.value)}
    if ref.kind == "kwargs":
        return dict(ref.value)
    # parent / payload — not produced by architect's classifier; defensive.
    raise ValueError(f"_ref_to_kwargs: unsupported source kind {ref.kind!r} for architect probes.")


# ---------------------------------------------------------------------------
# Per-source probe.
# ---------------------------------------------------------------------------


async def _probe_one(
    cls: Type[Any],
    name: str,
    kwargs: Dict[str, Any],
) -> Tuple[str, SourceProfile]:
    """Probe one source via a throwaway subclass and capture the SourceProfile.

    Each probe gets a fresh ``type(f"_ArchitectProbe_{name}", (cls,), {})``
    so that mutable class state (``cls.inc_url`` / ``cls.inc_file`` /
    ``cls._incorp_kwargs`` / ``cls.inc_dict``) lands on the throwaway and
    NOT on the user's class.  Without this, calling
    ``Incorporator.architect(...)`` would set ``Incorporator.inc_file``
    to the last probed source path, and every later
    ``MySubclass.refresh()`` that doesn't explicitly set its own
    ``inc_file`` would inherit that path via MRO and treat URLs as
    files.  See the v1.2.0 isolation contract in AGENTS.md.

    Threads a mutable list as ``__capture_into`` so the inspector's
    capture path runs and the SourceProfile lands in our caller.
    """
    # Throwaway subclass — discarded once this function returns.
    probe_cls = cast(Type[Any], type(f"_ArchitectProbe_{name}", (cls,), {}))
    capture: List[SourceProfile] = []
    probe_kwargs = {**kwargs, "__capture_into": capture}
    # Re-use test()'s safety guards: 5s timeout, single-page when paginated.
    try:
        await probe_cls.test(**probe_kwargs)
    except Exception as exc:  # noqa: BLE001
        # Surface the failure as an empty profile with a note in provided_kwargs
        # — the topology analyzer treats it as "no signal" and skips it.
        empty = SourceProfile(parsed_data=[], provided_kwargs={"__probe_error__": repr(exc)})
        return (name, empty)
    if not capture:
        # Inspector path didn't run (e.g. fetch failed quietly) — return empty.
        return (name, SourceProfile(parsed_data=[], provided_kwargs={}))
    profile = capture[0]
    # Populate response_meta.host from the resolved kwargs so _penstock_for can
    # consult the host rate registry.  File-mode probes leave host=None.
    if profile.response_meta is None:
        meta = ResponseMeta()
        inc_url = kwargs.get("inc_url")
        if isinstance(inc_url, str):
            parsed = urlparse(inc_url)
            meta.host = parsed.hostname
        profile.response_meta = meta
    return (name, profile)


# ---------------------------------------------------------------------------
# Pure analysis — SourceProfile list → OrchestrationPlan.
# ---------------------------------------------------------------------------


def _penstock_for(profile: SourceProfile) -> Optional[PenstockSpec]:
    """Return a ``PenstockSpec`` per the three-tier confidence ladder, or ``None``.

    Tier 1 reads :func:`incorporator.io.penstock.known_host_rates` — the
    live registry view.  The framework no longer ships any implicit per-host
    throttling; tier 1 fires only when the caller has previously called
    :func:`incorporator.io.penstock.register_host_penstock` (or
    ``incorporator.register_host_penstock``) for the host.
    """
    meta = profile.response_meta
    if meta and meta.host:
        registered_rates = known_host_rates()
        if meta.host in registered_rates:
            rate = registered_rates[meta.host]
            return PenstockSpec(
                kind="sustained",
                rate_per_sec=rate,
                confidence="high",
                rationale=f"{meta.host} is in the host-penstock registry ({rate} req/sec)",
            )
    if meta and meta.rate_limited:
        return PenstockSpec(
            kind="sustained",
            rate_per_sec=1.0,
            confidence="medium",
            rationale="429 observed during probe — defensive 1 req/sec",
        )
    return None


def _pascal_case(name: str) -> str:
    """Convert a snake / kebab / lower name into PascalCase for class emission."""
    return "".join(part.capitalize() for part in name.replace("-", "_").split("_") if part) or "Source"


def _current_spec_for(
    name: str,
    profile: SourceProfile,
    incorp_kwargs: Dict[str, Any],
) -> CurrentSpec:
    """Build a per-source ``CurrentSpec`` from the captured probe + original kwargs."""
    conv_dict_template: Dict[str, str] = {}
    for f in profile.datetime_fields:
        conv_dict_template[f] = "datetime"
    for f in profile.int_fields:
        conv_dict_template[f] = "int"
    for f in profile.float_fields:
        conv_dict_template[f] = "float"

    # Allow per-source dict-form to override verb (architect lets the user
    # nominate a Fjord tail explicitly via {"verb": "fjord"} in the sources
    # mapping; everything else defaults to Stream).
    verb = str(incorp_kwargs.pop("verb", "stream"))

    return CurrentSpec(
        name=name,
        verb=verb,
        incorp_params=dict(incorp_kwargs),
        pk_field=profile.primary_key_field,
        name_field=profile.display_name_field,
        conv_dict_template=conv_dict_template,
        excl_lst=list(profile.heavy_fields),
        inc_page_suggestion=profile.pagination_suggestion,
        interval_hint=60,
        class_name=_pascal_case(name),
    )


def _analyze_topology(
    named_profiles: List[Tuple[str, SourceProfile]],
    incorp_kwargs_by_name: Dict[str, Dict[str, Any]],
) -> OrchestrationPlan:
    """Run the cross-source heuristic and return an :class:`OrchestrationPlan`.

    Pure function — same input always yields same output.

    Heuristic precedence:

    1. **Fanout** — if exactly one source's pk field name appears as a field
       in every OTHER source, that source is the fanout head.
    2. **Diamond** — if 2+ sources share the same pk field name (and #1
       didn't fire), emit a diamond candidate.  ``needs_tail_current=True``;
       the renderer emits a ``_TODO_`` placeholder where the Fjord tail
       goes.
    3. **Parallel** — if all top-level field sets are pairwise disjoint.
    4. **Custom** — anything else (some overlap but no clear pattern).
       ``edges=[]``; the renderer surfaces the overlapping field pairs in
       a comment so the user can decide.
    """
    current_specs = [
        _current_spec_for(name, profile, dict(incorp_kwargs_by_name.get(name, {}))) for name, profile in named_profiles
    ]

    # 1. Fanout: one source's pk appears as a (non-pk) field in all others.
    # Same-pk-everywhere goes to the diamond branch below, not fanout —
    # parallel views of the same domain (e.g. laps + pits both keyed on
    # user_id) merge in a Fjord rather than fan out from one source.
    fanout_head: Optional[int] = None
    for i, (_name, profile) in enumerate(named_profiles):
        pk = profile.primary_key_field
        if not pk:
            continue
        others = [op for j, (_n, op) in enumerate(named_profiles) if j != i]
        if not others:
            continue
        if all(pk in op.top_level_fields and op.primary_key_field != pk for op in others):
            fanout_head = i
            break

    if fanout_head is not None:
        head_name, head_profile = named_profiles[fanout_head]
        head_pk = head_profile.primary_key_field
        edges = [
            EdgeSpec(
                from_name=head_name,
                to_name=other_name,
                gate_mode="hard",
                penstock=_penstock_for(other_profile),
            )
            for j, (other_name, other_profile) in enumerate(named_profiles)
            if j != fanout_head
        ]
        return OrchestrationPlan(
            shape="fanout",
            currents=current_specs,
            edges=edges,
            shape_rationale=(f"primary key {head_pk!r} from {head_name!r} appears in all other sources"),
            needs_tail_current=False,
            notes=[],
        )

    # 2. Diamond: 2+ sources share the same pk field name.
    pks = [p.primary_key_field for _n, p in named_profiles if p.primary_key_field]
    pk_counts = Counter(pks)
    shared_pks = [pk for pk, c in pk_counts.items() if c >= 2]
    if shared_pks:
        shared_pk = shared_pks[0]
        return OrchestrationPlan(
            shape="diamond",
            currents=current_specs,
            edges=[],  # tail is _TODO_ — renderer prompts for it
            shape_rationale=(
                f"primary key {shared_pk!r} appears on {pk_counts[shared_pk]} sources — diamond merge candidate"
            ),
            needs_tail_current=True,
            notes=[
                "Wire a Fjord at the diamond's tail to fuse the merging upstreams into a single mark-to-market view.",
            ],
        )

    # 3 / 4: pairwise overlap → parallel vs custom.
    field_sets: List[Set[str]] = [p.top_level_fields for _n, p in named_profiles]
    overlap_pairs: List[Tuple[str, str, List[str]]] = []
    for i in range(len(field_sets)):
        for j in range(i + 1, len(field_sets)):
            common = field_sets[i] & field_sets[j]
            if common:
                overlap_pairs.append((named_profiles[i][0], named_profiles[j][0], sorted(common)))

    if not overlap_pairs:
        return OrchestrationPlan(
            shape="parallel",
            currents=current_specs,
            edges=[],
            shape_rationale="all sources have disjoint top-level field sets",
            needs_tail_current=False,
            notes=[],
        )

    notes = [f"Overlap between {a!r} and {b!r}: {common}" for a, b, common in overlap_pairs[:5]]
    return OrchestrationPlan(
        shape="custom",
        currents=current_specs,
        edges=[],
        shape_rationale="field-name overlap detected but no clear fanout / diamond pattern",
        needs_tail_current=False,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Renderers.
# ---------------------------------------------------------------------------


def render_report(named_profiles: List[Tuple[str, SourceProfile]], plan: OrchestrationPlan) -> None:
    """Print the per-source inspector report + cross-source orchestration section.

    Each source's 5-section inspector report is printed via
    :func:`incorporator.tools.inspector.analyze_data`; the architect
    appends a sixth section summarising the cross-source plan.
    """
    for name, profile in named_profiles:
        print(f"\n{'#' * 70}")
        print(f"#  SOURCE: {name}")
        print(f"{'#' * 70}")
        analyze_data(profile.parsed_data, profile.provided_kwargs)

    print("\n" + "=" * 70)
    print("🌊  6. ORCHESTRATION HINTS (cross-source)")
    print("=" * 70)
    print(f"   Suggested watershed shape: {plan.shape}")
    print(f"     ({plan.shape_rationale})")
    print("   Currents:")
    for spec in plan.currents:
        pag = f"[pagination: {spec.inc_page_suggestion}]" if spec.inc_page_suggestion else "[no pagination]"
        print(f"     - {spec.name:<12} → {spec.verb.capitalize()}(interval={spec.interval_hint})  {pag}")
    if plan.edges:
        print("   Edges:")
        for edge in plan.edges:
            penstock_note = (
                f", {edge.penstock.kind.capitalize()}Penstock(rate_per_sec={edge.penstock.rate_per_sec})"
                if edge.penstock
                else ""
            )
            print(f"     - {edge.from_name} → {edge.to_name}  (gate_mode={edge.gate_mode!r}{penstock_note})")
    else:
        print("   Edges: (none — see notes)")
    if plan.needs_tail_current:
        print("   ⚠️  Tail current not provided — pass `{'verb': 'fjord'}` for the merging current")
        print("       to make this a runnable diamond watershed.")
    if plan.notes:
        print("   Notes:")
        for note in plan.notes:
            print(f"     - {note}")
    print("=" * 70 + "\n")


def _python_class_block(spec: CurrentSpec) -> str:
    """Emit the ``class Foo(Incorporator): pass`` block for one current."""
    parts = [f"class {spec.class_name}(Incorporator):"]
    if spec.conv_dict_template:
        parts.append("    # TODO: uncomment the conv_dict if you want explicit type coercion")
        parts.append("    # conv_dict = {")
        for f, t in spec.conv_dict_template.items():
            parts.append(f"    #     {f!r}: inc({t}),")
        parts.append("    # }")
        parts.append("    pass")
    else:
        parts.append("    pass")
    return "\n".join(parts)


def _python_current_block(spec: CurrentSpec) -> str:
    """Emit a ``Stream(...) / Fjord(...) / Export(...)`` constructor call."""
    cls_name = {"stream": "Stream", "fjord": "Fjord", "export": "Export"}[spec.verb]
    params_json = json.dumps(spec.incorp_params, indent=4, default=str)
    # indent the JSON consistently with the surrounding code
    params_indented = "\n".join(("    " + line if line else line) for line in params_json.splitlines())
    body = (
        f"{spec.name} = {cls_name}(\n"
        f"    name={spec.name!r},\n"
        f"    cls={spec.class_name},\n"
        f"    interval={spec.interval_hint},  # TODO: tune to your freshness SLO\n"
        f"    incorp_params={params_indented.lstrip()},\n"
        f")"
    )
    return body


def _python_watershed_block(plan: OrchestrationPlan) -> str:
    """Emit ``Watershed.<shape>(...)`` (or bare ``Watershed(...)`` for custom)."""
    shared_penstock: Optional[PenstockSpec] = None
    if plan.edges:
        # If every edge has the same penstock spec, lift it to flow=.
        first = plan.edges[0].penstock
        if first and all(edge.penstock and edge.penstock.rate_per_sec == first.rate_per_sec for edge in plan.edges):
            shared_penstock = first

    penstock_line = ""
    flow_block = ""
    if shared_penstock:
        penstock_line = (
            f"        penstock={shared_penstock.kind.capitalize()}Penstock("
            f"rate_per_sec={shared_penstock.rate_per_sec}),  # {shared_penstock.rationale}\n"
        )
        flow_block = f"    flow=FlowControl(\n        gate=HardLock(),\n{penstock_line}    ),\n"

    if plan.shape == "parallel":
        currents_names = ", ".join(spec.name for spec in plan.currents)
        return f"watershed = Watershed.parallel(\n    window=(start, end),\n    currents=[{currents_names}],\n)"
    if plan.shape == "fanout":
        # Identify head from edges (all edges share from_name).
        head = plan.edges[0].from_name if plan.edges else plan.currents[0].name
        sinks = [edge.to_name for edge in plan.edges]
        sinks_repr = ", ".join(sinks)
        return (
            "watershed = Watershed.fanout(\n"
            "    window=(start, end),\n"
            f"    source={head},\n"
            f"    sinks=[{sinks_repr}],\n"
            '    gate_mode="hard",\n'
            f"{flow_block}"
            ")"
        )
    if plan.shape == "diamond":
        names = [spec.name for spec in plan.currents]
        head = names[0]
        middle = names[1:]
        middle_repr = ", ".join(middle)
        return (
            "watershed = Watershed.diamond(\n"
            "    window=(start, end),\n"
            f"    head={head},\n"
            f"    middle=[{middle_repr}],\n"
            "    tail=...,  # TODO: define a Fjord that fuses head + middle into one mark\n"
            '    gate_mode="hard",\n'
            f"{flow_block}"
            ")"
        )
    # custom
    currents_names = ", ".join(spec.name for spec in plan.currents)
    return (
        "watershed = Watershed(\n"
        "    window=(start, end),\n"
        f"    currents=[{currents_names}],\n"
        "    edges=[],  # TODO: wire edges per the overlap notes in the report\n"
        ")"
    )


def render_python(named_profiles: List[Tuple[str, SourceProfile]], plan: OrchestrationPlan) -> str:
    """Return a self-contained Python module body that builds and runs the watershed."""
    needs_penstock_import = any(edge.penstock for edge in plan.edges)
    penstock_imports = ""
    if needs_penstock_import:
        penstock_imports = ", FlowControl, HardLock, SustainedPenstock"

    header = (
        "# Generated by Incorporator.architect()\n"
        "# Inspect, tune intervals, fill in the TODOs, then run.\n"
        "from datetime import datetime, timedelta, timezone\n\n"
        "from incorporator import Incorporator\n"
        "from incorporator.observability.tideweaver import (\n"
        "    Tideweaver, Watershed, Stream, Fjord, Export"
        f"{penstock_imports}\n"
        ")\n"
    )
    class_blocks = "\n\n\n".join(_python_class_block(spec) for spec in plan.currents)
    current_blocks = "\n\n".join(_python_current_block(spec) for spec in plan.currents)

    window_block = (
        "\nstart = datetime.now(timezone.utc)\nend = start + timedelta(hours=4)  # TODO: set your actual window\n"
    )

    watershed_block = _python_watershed_block(plan)
    runner_block = (
        "\n\n"
        "async def main() -> None:\n"
        "    async for tide in Tideweaver(watershed).run():\n"
        "        print(tide.tide_number, tide.fired, tide.skipped)\n\n\n"
        'if __name__ == "__main__":\n'
        "    import asyncio\n"
        "    asyncio.run(main())\n"
    )

    rendered = (
        header
        + "\n\n"
        + class_blocks
        + "\n\n"
        + window_block
        + "\n"
        + current_blocks
        + "\n\n"
        + watershed_block
        + runner_block
    )
    print(rendered)
    return rendered


def _json_current_entry(spec: CurrentSpec) -> Dict[str, Any]:
    """Build the JSON-form current entry for a watershed.json shape."""
    entry: Dict[str, Any] = {
        "name": spec.name,
        "class": spec.class_name,
        "verb": spec.verb,
        "interval": spec.interval_hint,
        "incorp_params": dict(spec.incorp_params),
    }
    if spec.verb in ("fjord", "export"):
        entry["export_params"] = {"_TODO_": "fill in file_path + format for the sink"}
    return entry


def render_json(named_profiles: List[Tuple[str, SourceProfile]], plan: OrchestrationPlan) -> str:
    """Return a complete ``watershed.json`` body as a JSON string.

    Loadable via :func:`incorporator.observability.tideweaver.config.load_watershed`
    once the user fills in window timestamps, ``outflow.py`` path, and any
    ``_TODO_`` placeholders.
    """
    # Determine shared flow block from a uniform-penstock edge set.
    shared_penstock: Optional[PenstockSpec] = None
    if plan.edges:
        first = plan.edges[0].penstock
        if first and all(edge.penstock and edge.penstock.rate_per_sec == first.rate_per_sec for edge in plan.edges):
            shared_penstock = first
    flow_dict: Optional[Dict[str, Any]] = None
    if shared_penstock:
        flow_dict = {
            "gate": {"type": "hard"},
            "penstock": {"type": shared_penstock.kind, "rate_per_sec": shared_penstock.rate_per_sec},
        }

    body: Dict[str, Any] = {
        "window": {"start": "${WINDOW_START}", "end": "${WINDOW_END}"},
        "shape": plan.shape,
        "outflow": "outflow.py",  # user must point this at their sidecar
        "gate_mode": "hard",
    }
    if flow_dict:
        # gate_mode + flow are mutex in the loader — drop gate_mode when emitting flow.
        body.pop("gate_mode")
        body["flow"] = flow_dict

    if plan.shape == "parallel":
        body.pop("gate_mode", None)  # parallel doesn't accept gate_mode
        body.pop("flow", None)
        body["currents"] = [_json_current_entry(spec) for spec in plan.currents]
    elif plan.shape == "fanout":
        head_name = plan.edges[0].from_name if plan.edges else plan.currents[0].name
        sink_names = [edge.to_name for edge in plan.edges]
        head_spec = next(spec for spec in plan.currents if spec.name == head_name)
        sink_specs = [next(spec for spec in plan.currents if spec.name == sn) for sn in sink_names]
        body["source"] = _json_current_entry(head_spec)
        body["sinks"] = [_json_current_entry(s) for s in sink_specs]
    elif plan.shape == "diamond":
        names = [spec.name for spec in plan.currents]
        head_spec = plan.currents[0]
        middle_specs = plan.currents[1:]
        body["head"] = _json_current_entry(head_spec)
        body["middle"] = [_json_current_entry(s) for s in middle_specs]
        body["tail"] = {"_TODO_": "wire a Fjord here that fuses head + middle into one mark"}
        _ = names  # acknowledge name list (kept for future debug)
    else:  # custom
        body["currents"] = [_json_current_entry(spec) for spec in plan.currents]
        body["edges"] = []  # user fills in based on the overlap notes

    rendered = json.dumps(body, indent=2)
    print(rendered)
    return rendered


# ---------------------------------------------------------------------------
# Public entry point.
# ---------------------------------------------------------------------------


async def run(
    cls: Type[Any],
    sources: Mapping[str, SourceValue],
    *,
    output: str = "report",
    shared_kwargs: Optional[Mapping[str, Any]] = None,
) -> Optional[Union[str, OrchestrationPlan]]:
    """Probe ``sources``, run cross-source analysis, dispatch to a renderer.

    Args:
        cls: The :class:`Incorporator` subclass acting as the default base
            for discovered records.
        sources: Mapping of ``name -> URL | Path | dict``.  See
            :func:`_resolve_sources` for the value forms.
        output: One of ``"report"`` (prints, returns ``None``),
            ``"python"`` (paste-ready snippet, returns the rendered str),
            ``"json"`` (paste-ready watershed.json, returns the rendered
            str), or ``"plan"`` (returns the structured
            :class:`OrchestrationPlan` directly — no print, no rendering).
        shared_kwargs: Common incorp() kwargs applied to every probe
            (timeout, headers, ...).  Per-source kwargs win on conflict.

    Returns:
        ``None`` for ``output="report"`` (prints only).
        The rendered scaffold as a string for ``"python"`` and ``"json"``
        (also printed to stdout).
        The :class:`OrchestrationPlan` directly for ``"plan"``.  Pair with
        :meth:`OrchestrationPlan.to_watershed` to run the plan in-memory
        without disk round-tripping.
    """
    if output not in ("report", "python", "json", "plan"):
        raise ValueError(f"output must be one of 'report' / 'python' / 'json' / 'plan'; got {output!r}")

    resolved = _resolve_sources(sources, shared_kwargs)
    incorp_kwargs_by_name = {name: dict(kw) for name, kw in resolved}
    probes = await asyncio.gather(*(_probe_one(cls, name, kw) for name, kw in resolved))
    plan = _analyze_topology(probes, incorp_kwargs_by_name)

    if output == "plan":
        return plan
    if output == "report":
        render_report(probes, plan)
        return None
    if output == "python":
        return render_python(probes, plan)
    return render_json(probes, plan)
