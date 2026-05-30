"""Multi-source orchestration probe + post-run tuning — the engine behind ``cls.architect()`` and :func:`tune`.

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

After a :class:`Tideweaver` run, :func:`tune` closes the feedback loop —
consume the accumulated :class:`~incorporator.RejectEntry` records and
the per-pass :class:`Tide` records and receive a structured
:class:`TuningReport` of severity-sorted :class:`TuningHint` recommendations
across ``chunk_size``, penstock rate, surge threshold, ``pass_interval``,
and retry policy.  :class:`Tideweaver` and :class:`LoggedTideweaver` also
expose ``summary()`` as an instance-method shortcut to the same report.
"""

from __future__ import annotations

import asyncio
import json
import statistics
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField

from ...io.penstock import known_host_rates
from ...io.source_ref import SourceRef
from ...rejects import RejectEntry
from ...tools.inspector import ResponseMeta, SourceProfile, analyze_data
from ..wave import Wave
from ._retry_defaults import (
    _CANAL_OUTER_STOP,
    _COMPOUND_RETRY_BUDGET_SEC,
    _HTTP_INNER_STOP,
    _HTTP_INNER_WAIT_MAX,
)
from .reasons import WakeReason
from .tide import Tide

# Mapping-typed source values may be runtime dicts (Mapping) — we accept any
# Mapping for input, but the internal source list normalises to Dict[str, Any].
# Kept as the architect public input alias; internal classification routes
# through :class:`incorporator.io.source_ref.SourceRef` after the loose union
# input is narrowed.
SourceValue = str | Path | Mapping[str, Any]

#: Error kinds that carry retry-policy telemetry — broadened from HTTP-only
#: to include canal-layer skip kinds so ``_tune_retry_policy`` covers
#: both penstock and surge-barrier rejects alongside HTTP failures.
_RETRY_POLICY_KINDS: frozenset[str] = frozenset(
    {"HTTPStatusError", "PenstockLimited", "SurgeHalted", "SkipAhead", "GateBlocked"}
)

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
    incorp_params: dict[str, Any]
    pk_field: str | None
    name_field: str | None
    conv_dict_template: dict[str, str]  # field name -> "datetime" | "int" | "float"
    excl_lst: list[str]
    inc_page_suggestion: str | None  # paginator call expression
    interval_hint: int
    class_name: str  # PascalCase class name for Python emission


@dataclass
class EdgeSpec:
    """Architect's per-edge recommendation in the orchestration plan."""

    from_name: str
    to_name: str
    gate_mode: str  # "hard" | "soft" | "weir"
    penstock: PenstockSpec | None


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
    currents: list[CurrentSpec]
    edges: list[EdgeSpec]
    shape_rationale: str
    needs_tail_current: bool = False
    notes: list[str] = field(default_factory=list)

    def to_watershed(
        self,
        window: tuple[Any, Any] | None = None,
        *,
        classes: Mapping[str, Any] | None = None,
    ) -> Any:
        """Materialise the plan into a runnable :class:`Watershed`.

        Args:
            window: ``(start, end)`` UTC datetimes.  Defaults to ``(now,
                now + 1h)``.  Pass an explicit window for production.
            classes: Optional ``name -> class`` mapping.  Overloaded by
                verb:

                * For verb-typed specs (``"stream"`` / ``"fjord"`` /
                  ``"export"``), the value is the :class:`Incorporator`
                  subclass to use.  Names missing from the mapping get
                  an anonymous subclass of :class:`Incorporator` whose
                  ``__name__`` matches :attr:`CurrentSpec.class_name`.
                * For ``verb="custom"`` (or any other non-verb-typed
                  spec), the value is the :class:`CustomCurrent`
                  subclass itself — the framework can't fabricate one
                  because :meth:`CustomCurrent.tick` is abstract.
                  Missing entries raise ``ValueError`` at materialise
                  time with the subclass-skeleton hint.

        Returns:
            A validated :class:`~incorporator.observability.tideweaver.Watershed`
            that can be handed straight to ``Tideweaver(watershed).run()``.

        Raises:
            ValueError: when :attr:`needs_tail_current` is ``True`` and
                the caller hasn't nominated a tail Current via the
                ``classes`` mapping; or when a non-verb-typed spec
                doesn't have a :class:`Current`-subclass entry in
                ``classes``.
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

        verb_to_class: dict[str, type[Current]] = {"stream": Stream, "fjord": Fjord, "export": Export}

        # Resolve user-supplied or fabricate per-spec Incorporator subclasses.
        # ``classes`` is overloaded: for verb-typed specs the value is the
        # :class:`Incorporator` subclass to use; for ``verb="custom"`` (or any
        # future verb the framework doesn't know about) the value is the
        # :class:`CustomCurrent` subclass itself — CustomCurrent's ``tick``
        # is abstract, so the framework can't fabricate a working one.
        resolved_classes: dict[str, Any] = dict(classes) if classes else {}
        built_currents: dict[str, Current] = {}
        for spec in self.currents:
            current_kwargs: dict[str, Any] = {
                "name": spec.name,
                "interval": spec.interval_hint,
            }
            if spec.verb in verb_to_class:
                current_cls = verb_to_class[spec.verb]
                inc_cls = resolved_classes.get(spec.name)
                if inc_cls is None:
                    inc_cls = type(spec.class_name, (Incorporator,), {})
                current_kwargs["cls"] = inc_cls
                # Stream / Fjord / Export accept incorp_params; CustomCurrent does not
                # (Pydantic ``extra="forbid"`` on Current's config).
                current_kwargs["incorp_params"] = dict(spec.incorp_params)
            else:
                custom_entry = resolved_classes.get(spec.name)
                if not (isinstance(custom_entry, type) and issubclass(custom_entry, Current)):
                    raise ValueError(
                        f"OrchestrationPlan.to_watershed: current {spec.name!r} has "
                        f"verb={spec.verb!r}, which requires a Current subclass passed "
                        f"via classes={{...}}.  Subclass CustomCurrent with your tick() "
                        f"body, then pass classes={{{spec.name!r}: YourCustomCurrent}}.  "
                        f"Verb-typed specs accept {sorted(verb_to_class)}; everything "
                        f"else is treated as a CustomCurrent."
                    )
                current_cls = custom_entry
                current_kwargs["cls"] = type(spec.class_name, (Incorporator,), {})
            built_currents[spec.name] = current_cls(**current_kwargs)

        # Materialise edges with the recommended FlowControl shape.  ``Edge``
        # accepts ``gate_mode=`` (shorthand) XOR ``flow=`` (full dict); pick
        # the shorthand when there's no per-edge penstock and the full form
        # when the architect recommended a tier-1/2 Penstock.
        built_edges: list[Edge] = []
        for edge_spec in self.edges:
            edge_kwargs: dict[str, Any] = {
                "from_name": edge_spec.from_name,
                "to_name": edge_spec.to_name,
            }
            if edge_spec.penstock is not None and edge_spec.penstock.kind == "sustained":
                # Build the FlowControl from gate_mode + the recommended penstock.
                base_flow = flow_from_mode(edge_spec.gate_mode)
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
    shared_kwargs: Mapping[str, Any] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
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
    resolved: list[tuple[str, dict[str, Any]]] = []
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


def _ref_to_kwargs(ref: SourceRef) -> dict[str, Any]:
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
    cls: type[Any],
    name: str,
    kwargs: dict[str, Any],
) -> tuple[str, SourceProfile]:
    """Probe one source via a throwaway subclass and capture the SourceProfile.

    Each probe gets a fresh dynamic subclass so mutable class state
    (``inc_url`` / ``inc_file`` / ``_incorp_kwargs`` / ``inc_dict``) lands on the
    throwaway and NOT on the user's class, preventing MRO bleed-through.

    Threads a mutable list as ``__capture_into`` so the inspector's
    capture path runs and the SourceProfile lands in our caller.
    """
    # Throwaway subclass — discarded once this function returns.
    probe_cls = cast(type[Any], type(f"_ArchitectProbe_{name}", (cls,), {}))
    capture: list[SourceProfile] = []
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


def _penstock_for(profile: SourceProfile) -> PenstockSpec | None:
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
    incorp_kwargs: dict[str, Any],
) -> CurrentSpec:
    """Build a per-source ``CurrentSpec`` from the captured probe + original kwargs."""
    conv_dict_template: dict[str, str] = {}
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
    named_profiles: list[tuple[str, SourceProfile]],
    incorp_kwargs_by_name: dict[str, dict[str, Any]],
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
    fanout_head: int | None = None
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
    field_sets: list[set[str]] = [p.top_level_fields for _n, p in named_profiles]
    overlap_pairs: list[tuple[str, str, list[str]]] = []
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


def render_report(named_profiles: list[tuple[str, SourceProfile]], plan: OrchestrationPlan) -> None:
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
    shared_penstock: PenstockSpec | None = None
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


def render_python(named_profiles: list[tuple[str, SourceProfile]], plan: OrchestrationPlan) -> str:
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


def _json_current_entry(spec: CurrentSpec) -> dict[str, Any]:
    """Build the JSON-form current entry for a watershed.json shape."""
    entry: dict[str, Any] = {
        "name": spec.name,
        "class": spec.class_name,
        "verb": spec.verb,
        "interval": spec.interval_hint,
        "incorp_params": dict(spec.incorp_params),
    }
    if spec.verb in ("fjord", "export"):
        entry["export_params"] = {"_TODO_": "fill in file_path + format for the sink"}
    return entry


def render_json(named_profiles: list[tuple[str, SourceProfile]], plan: OrchestrationPlan) -> str:
    """Return a complete ``watershed.json`` body as a JSON string.

    Loadable via :func:`incorporator.observability.tideweaver.config.load_watershed`
    once the user fills in window timestamps, ``outflow.py`` path, and any
    ``_TODO_`` placeholders.
    """
    # Determine shared flow block from a uniform-penstock edge set.
    shared_penstock: PenstockSpec | None = None
    if plan.edges:
        first = plan.edges[0].penstock
        if first and all(edge.penstock and edge.penstock.rate_per_sec == first.rate_per_sec for edge in plan.edges):
            shared_penstock = first
    flow_dict: dict[str, Any] | None = None
    if shared_penstock:
        flow_dict = {
            "gate": {"type": "hard"},
            "penstock": {"type": shared_penstock.kind, "rate_per_sec": shared_penstock.rate_per_sec},
        }

    body: dict[str, Any] = {
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
    cls: type[Any],
    sources: Mapping[str, SourceValue],
    *,
    output: str = "report",
    shared_kwargs: Mapping[str, Any] | None = None,
) -> str | OrchestrationPlan | None:
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


# ---------------------------------------------------------------------------
# Tuner: TuningHint, TuningReport, rule functions, tune() entry point.
# ---------------------------------------------------------------------------

_SEVERITY_ORDER: dict[str, int] = {"high": 0, "med": 1, "low": 2, "info": 3}


def _percentile(data: list[float], p: float) -> float:
    """Return the value at percentile p (0-100) of data using statistics.quantiles.

    Args:
        data: Non-empty list of floats.
        p: Target percentile in the range 0–100 (e.g. 50 for median, 99 for p99).

    Returns:
        The value at the requested percentile, or ``data[0]`` for single-element
        lists (``statistics.quantiles`` requires at least two data points).
    """
    if len(data) < 2:
        return data[0] if data else 0.0
    quantiles = statistics.quantiles(data, n=100)
    index = max(0, min(int(p) - 1, len(quantiles) - 1))
    return quantiles[index]


class TuningHint(BaseModel):
    """One structured tuning recommendation produced by :func:`tune`.

    Each hint carries a severity, the name of the tunable parameter
    (``knob``), the scope it applies to (source, edge, host, or global),
    the observed and recommended values, a one-line signal summary, a
    multi-line rationale, and the number of records the hint was computed
    from.

    Attributes:
        severity: Urgency level — ``"high"`` (likely misconfig), ``"med"``
            (suboptimal), ``"low"`` (minor tuning opportunity), or
            ``"info"`` (no action needed, informational only).
        knob: The tunable parameter name, e.g. ``"chunk_size"`` or
            ``"penstock.rate_per_sec"``.
        scope: Where this hint applies.  Keys are one of ``"source"``,
            ``"edge"``, ``"host"``, or ``"global"``; values are the
            identifier string.
        current_value: What the knob is set to today, or ``None`` if not
            recoverable from the records.
        recommended_value: What to change it to, or ``None`` when no
            numeric recommendation can be derived.
        signal: One-line summary of the observation that triggered this
            hint.
        rationale: Multi-line explanation including representative data
            points.
        sample_size: Number of records the hint was computed from.
    """

    model_config = ConfigDict(frozen=True)

    severity: Literal["high", "med", "low", "info"] = PydanticField(...)
    knob: str = PydanticField(..., description="The tunable parameter (e.g., 'chunk_size', 'penstock.rate_per_sec').")
    scope: dict[str, str] = PydanticField(
        default_factory=dict,
        description="Where this hint applies — keys like 'source', 'edge', 'host', 'global'.",
    )
    current_value: Any | None = PydanticField(
        default=None,
        description="What the knob is set to today (None if not recoverable from records).",
    )
    recommended_value: Any | None = PydanticField(
        default=None,
        description="What to change it to (None if no numeric recommendation).",
    )
    signal: str = PydanticField(..., description="One-line summary of the observation.")
    rationale: str = PydanticField(..., description="Multi-line explanation including data points.")
    sample_size: int = PydanticField(default=0, description="Number of records the hint was computed from.")


class TuningReport(BaseModel):
    """Full output of :func:`tune` — a collection of :class:`TuningHint` objects plus summary stats.

    Attributes:
        hints: List of structured tuning recommendations, ordered by
            severity (high → med → low → info) when rendered.
        summary: Aggregate stats for the analyzed window: ``total_chunks``,
            ``total_passes``, ``total_rejects``, ``window_start``, and
            ``window_end``.
        analyzed_at: UTC timestamp when the report was generated.
    """

    model_config = ConfigDict(frozen=True)

    hints: list[TuningHint] = PydanticField(default_factory=list)
    summary: dict[str, Any] = PydanticField(default_factory=dict)
    analyzed_at: datetime = PydanticField(default_factory=lambda: datetime.now(timezone.utc))

    def render(self) -> str:
        """Return a human-readable formatted tuning report.

        Hints are sorted by severity (high → med → low → info) before
        rendering.  Each hint is emitted as a labelled block; the report
        closes with a footer showing summary statistics.

        Returns:
            Multi-line string suitable for printing or writing to a log
            file.
        """
        sorted_hints = sorted(self.hints, key=lambda h: _SEVERITY_ORDER.get(h.severity, 99))
        lines: list[str] = []
        for hint in sorted_hints:
            label = hint.severity.upper()
            scope_str = ", ".join(f"{k}={v}" for k, v in hint.scope.items())
            scope_part = f"  [{scope_str}]" if scope_str else ""
            lines.append(f"[{label}]  {hint.knob}{scope_part}")
            lines.append(f"        Signal: {hint.signal}")
            for rline in hint.rationale.splitlines():
                lines.append(f"        {rline}")
            if hint.current_value is not None:
                lines.append(f"        Current value: {hint.current_value}")
            if hint.recommended_value is not None:
                lines.append(f"        Recommended: {hint.recommended_value}")
            lines.append(f"        Sample size: {hint.sample_size}")
            lines.append("")

        lines.append("--- Summary ---")
        for k, v in self.summary.items():
            lines.append(f"  {k}: {v}")
        lines.append(f"  analyzed_at: {self.analyzed_at.isoformat()}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rule functions.
# ---------------------------------------------------------------------------


def _tune_chunk_size(waves: list[Wave]) -> list[TuningHint]:
    """Analyse Wave timing records and recommend chunk_size adjustments.

    Groups waves by ``source_url``, computes p50 and p99 of
    ``processing_time_sec`` per group, and emits a hint when the
    distribution is either far too fast (chunk too small) or too slow
    (memory pressure).

    Args:
        waves: List of :class:`~incorporator.Wave` records from a run.

    Returns:
        List of :class:`TuningHint` — one per source group that
        triggered a rule, including INFO hints for well-tuned or
        data-insufficient groups.
    """
    # Group by source_url (may be None for file-mode or one-shot sources).
    groups: dict[str | None, list[Wave]] = {}
    for w in waves:
        groups.setdefault(w.source_url, []).append(w)

    hints: list[TuningHint] = []
    for source_url, group in groups.items():
        scope = {"source": str(source_url)}
        n = len(group)
        if n < 20:
            hints.append(
                TuningHint.model_construct(
                    severity="info",
                    knob="chunk_size",
                    scope=scope,
                    current_value=None,
                    recommended_value=None,
                    signal=f"insufficient data (n={n})",
                    rationale=f"Need at least 20 waves per source to compute reliable p50/p99. Got {n}.",
                    sample_size=n,
                )
            )
            continue

        times = [w.processing_time_sec for w in group]
        p50 = _percentile(times, 50)
        p99 = _percentile(times, 99)

        if p50 < 0.010 and p99 < 0.050:
            hints.append(
                TuningHint.model_construct(
                    severity="high",
                    knob="chunk_size",
                    scope=scope,
                    current_value=None,
                    recommended_value="raise from current (target ~50 ms p50)",
                    signal=f"p50={p50 * 1000:.1f}ms, p99={p99 * 1000:.1f}ms — chunks finishing too fast",
                    rationale=(
                        f"p50={p50 * 1000:.1f}ms and p99={p99 * 1000:.1f}ms are both well below the 50ms target.\n"
                        "Current chunk_size is not recoverable from Wave records alone"
                        " — check the Stream/incorp call.\n"
                        "Raising chunk_size reduces per-chunk overhead and improves throughput."
                    ),
                    sample_size=n,
                )
            )
        elif p99 > 0.500:
            hints.append(
                TuningHint.model_construct(
                    severity="med",
                    knob="chunk_size",
                    scope=scope,
                    current_value=None,
                    recommended_value="lower from current (memory pressure)",
                    signal=f"p99={p99 * 1000:.1f}ms — chunks taking too long, possible memory pressure",
                    rationale=(
                        f"p99={p99 * 1000:.1f}ms exceeds 500ms, suggesting chunk_size is too large.\n"
                        f"p50={p50 * 1000:.1f}ms. Lower chunk_size to reduce memory footprint per chunk."
                    ),
                    sample_size=n,
                )
            )
        else:
            hints.append(
                TuningHint.model_construct(
                    severity="info",
                    knob="chunk_size",
                    scope=scope,
                    current_value=None,
                    recommended_value=None,
                    signal=f"well-tuned. p50={p50 * 1000:.1f}ms, p99={p99 * 1000:.1f}ms",
                    rationale=(
                        f"p50={p50 * 1000:.1f}ms and p99={p99 * 1000:.1f}ms are within the target range.\n"
                        "No chunk_size change recommended."
                    ),
                    sample_size=n,
                )
            )
    return hints


def _tune_penstock_rate(rejects: list[RejectEntry], window_sec: float = 600.0) -> list[TuningHint]:
    """Analyse reject records to recommend penstock rate adjustments.

    Filters to ``PenstockLimited`` and HTTP 429 rejects, then groups
    canal rejects by ``(from_name, to_name)`` edge and HTTP rejects by
    ``host``.  For groups with more than 5 entries, computes the median
    ``cooldown_sec`` and derives a recommended ``rate_per_sec``.

    Args:
        rejects: List of :class:`~incorporator.RejectEntry` records.
        window_sec: Observation window in seconds (informational only;
            not used in rate computation).

    Returns:
        List of :class:`TuningHint`.
    """
    # Filter to penstock-related rejects only.
    penstock_rejects = [
        r
        for r in rejects
        if r.error_kind == "PenstockLimited" or (r.error_kind == "HTTPStatusError" and r.status_code == 429)
    ]

    hints: list[TuningHint] = []

    # Canal rejects: from_name is not None.
    canal = [r for r in penstock_rejects if r.from_name is not None]
    canal_groups: dict[tuple[str | None, str | None], list[RejectEntry]] = {}
    for r in canal:
        canal_groups.setdefault((r.from_name, r.to_name), []).append(r)

    for (from_name, to_name), group in canal_groups.items():
        if len(group) <= 5:
            continue
        edge_label = f"{from_name}->{to_name}"
        cooldowns = [r.cooldown_sec for r in group if r.cooldown_sec is not None]
        if cooldowns:
            median_cooldown = statistics.median(cooldowns)
            recommended = round(1.0 / median_cooldown, 4) if median_cooldown > 0 else None
            hints.append(
                TuningHint.model_construct(
                    severity="high",
                    knob="penstock.rate_per_sec",
                    scope={"edge": edge_label},
                    current_value=None,
                    recommended_value=recommended,
                    signal=f"{len(group)} PenstockLimited rejects on edge {edge_label}",
                    rationale=(
                        f"{len(group)} rejects on edge {edge_label}.\n"
                        f"Median cooldown_sec={median_cooldown:.3f}s "
                        f"→ recommended rate_per_sec={recommended} (1 req per cooldown period)."
                    ),
                    sample_size=len(group),
                )
            )
        else:
            hints.append(
                TuningHint.model_construct(
                    severity="med",
                    knob="penstock.rate_per_sec",
                    scope={"edge": edge_label},
                    current_value=None,
                    recommended_value=None,
                    signal=f"{len(group)} PenstockLimited rejects on edge {edge_label}; no cooldown_sec data",
                    rationale=(
                        f"{len(group)} rejects on edge {edge_label}, but no cooldown_sec values were recorded.\n"
                        "Raise rate; cooldown_sec data unavailable for precise recommendation."
                    ),
                    sample_size=len(group),
                )
            )

    # HTTP rejects: host is not None.
    http_rejects = [r for r in penstock_rejects if r.host is not None]
    http_groups: dict[str, list[RejectEntry]] = {}
    for r in http_rejects:
        if r.host is not None:
            http_groups.setdefault(r.host, []).append(r)

    for host, group in http_groups.items():
        if len(group) <= 5:
            continue
        cooldowns = [r.cooldown_sec for r in group if r.cooldown_sec is not None]
        if cooldowns:
            median_cooldown = statistics.median(cooldowns)
            recommended = round(1.0 / median_cooldown, 4) if median_cooldown > 0 else None
            hints.append(
                TuningHint.model_construct(
                    severity="high",
                    knob="penstock.rate_per_sec",
                    scope={"host": host},
                    current_value=None,
                    recommended_value=recommended,
                    signal=f"{len(group)} HTTP 429 rejects for host {host!r}",
                    rationale=(
                        f"{len(group)} HTTPStatusError(429) rejects for host {host!r}.\n"
                        f"Median cooldown_sec={median_cooldown:.3f}s "
                        f"→ recommended rate_per_sec={recommended} (1 req per cooldown period)."
                    ),
                    sample_size=len(group),
                )
            )
        else:
            hints.append(
                TuningHint.model_construct(
                    severity="med",
                    knob="penstock.rate_per_sec",
                    scope={"host": host},
                    current_value=None,
                    recommended_value=None,
                    signal=f"{len(group)} HTTP 429 rejects for host {host!r}; no cooldown_sec data",
                    rationale=(
                        f"{len(group)} HTTPStatusError(429) rejects for host {host!r}, "
                        "but no cooldown_sec values were recorded.\n"
                        "Raise rate; cooldown_sec data unavailable for precise recommendation."
                    ),
                    sample_size=len(group),
                )
            )

    return hints


def _tune_surge_threshold(rejects: list[RejectEntry], tides: list[Tide]) -> list[TuningHint]:
    """Analyse surge-related rejects and in-flight data to recommend threshold adjustments.

    Filters to ``SkipAhead`` / ``SurgeHalted`` canal rejects, groups by
    ``(from_name, to_name)`` edge, then gathers ``in_flight_sec`` from
    matching :class:`~incorporator.observability.tideweaver.tide.Tide`
    outcomes to derive a recommended ``threshold_multiple``.

    Args:
        rejects: List of :class:`~incorporator.RejectEntry` records.
        tides: List of :class:`~incorporator.observability.tideweaver.tide.Tide`
            records from the same run.

    Returns:
        List of :class:`TuningHint`.
    """
    # Filter to surge-related canal rejects only (from_name must be set).
    surge_rejects = [r for r in rejects if r.error_kind in ("SkipAhead", "SurgeHalted") and r.from_name is not None]

    # Group by (from_name, to_name).
    groups: dict[tuple[str | None, str | None], list[RejectEntry]] = {}
    for r in surge_rejects:
        groups.setdefault((r.from_name, r.to_name), []).append(r)

    hints: list[TuningHint] = []
    for (from_name, to_name), group in groups.items():
        if len(group) <= 5:
            continue
        edge_label = f"{from_name}->{to_name}"

        # Gather in_flight_sec from tide outcomes where outcome.name == from_name.
        in_flight_values: list[float] = []
        for tide in tides:
            for outcome in tide.current_outcomes:
                if outcome.name == from_name and outcome.in_flight_sec is not None:
                    in_flight_values.append(outcome.in_flight_sec)

        if not in_flight_values:
            hints.append(
                TuningHint.model_construct(
                    severity="info",
                    knob="surge_barrier.threshold_multiple",
                    scope={"edge": edge_label},
                    current_value=None,
                    recommended_value=None,
                    signal=f"{len(group)} surge rejects on edge {edge_label}; insufficient in_flight_sec data",
                    rationale=(
                        f"{len(group)} surge rejects on edge {edge_label}, but no in_flight_sec data "
                        "was found in the tide records for the upstream current.\n"
                        "Insufficient in_flight_sec data for edge — cannot derive threshold recommendation."
                    ),
                    sample_size=len(group),
                )
            )
            continue

        median_in_flight = statistics.median(in_flight_values)
        hints.append(
            TuningHint.model_construct(
                severity="med",
                knob="surge_barrier.threshold_multiple",
                scope={"edge": edge_label},
                current_value=None,
                recommended_value=None,
                signal=f"{len(group)} surge rejects on edge {edge_label}; median in_flight_sec={median_in_flight:.2f}s",
                rationale=(
                    f"{len(group)} rejects; median in_flight_sec={median_in_flight:.2f}s; "
                    "compare against the from_name current's configured interval — "
                    "raise threshold_multiple if median > 2.0 × interval."
                ),
                sample_size=len(group),
            )
        )
    return hints


def _tune_pass_interval(tides: list[Tide], current_pass_interval: float) -> list[TuningHint]:
    """Analyse Tide duration records to recommend pass_interval adjustments.

    Computes p50/p99 of ``duration_sec`` across tides and the fraction
    that woke via the ``"pass_interval"`` fallback (heap-empty).  Emits
    HIGH when p99 saturates the current interval or MED when the heap-empty
    fallback fraction exceeds 30%.

    Args:
        tides: List of :class:`~incorporator.observability.tideweaver.tide.Tide`
            records from a run.
        current_pass_interval: The ``pass_interval`` the scheduler was
            configured with, in seconds.

    Returns:
        List of :class:`TuningHint` — at most two (one per rule).
    """
    n = len(tides)
    if n < 20:
        return [
            TuningHint.model_construct(
                severity="info",
                knob="pass_interval",
                scope={"global": "true"},
                current_value=current_pass_interval,
                recommended_value=None,
                signal=f"insufficient data (n={n})",
                rationale=f"Need at least 20 tides to compute reliable p50/p99. Got {n}.",
                sample_size=n,
            )
        ]

    durations = [t.duration_sec for t in tides]
    p50 = _percentile(durations, 50)
    p99 = _percentile(durations, 99)

    fallback_count = sum(1 for t in tides if t.wake_reason == WakeReason.PASS_INTERVAL)
    fallback_fraction = fallback_count / n

    hints: list[TuningHint] = []

    # Rule A: p99 saturates the current interval.
    if p99 > 0.8 * current_pass_interval:
        recommended = round(p99 * 1.25, 4)
        hints.append(
            TuningHint.model_construct(
                severity="high",
                knob="pass_interval",
                scope={"global": "true"},
                current_value=current_pass_interval,
                recommended_value=recommended,
                signal=(
                    f"p99 duration ({p99 * 1000:.1f}ms) exceeds 80% of"
                    f" pass_interval ({current_pass_interval * 1000:.1f}ms)"
                ),
                rationale=(
                    f"p99={p99 * 1000:.1f}ms is {p99 / current_pass_interval:.1%} of the current "
                    f"pass_interval={current_pass_interval * 1000:.1f}ms.\n"
                    f"p50={p50 * 1000:.1f}ms. "
                    f"Recommended pass_interval={recommended}s covers p99 with 25% headroom."
                ),
                sample_size=n,
            )
        )

    # Rule B: heap-empty fallback degeneracy (independent of Rule A).
    if fallback_fraction > 0.30:
        hints.append(
            TuningHint.model_construct(
                severity="med",
                knob="pass_interval",
                scope={"global": "true"},
                current_value=current_pass_interval,
                recommended_value=None,
                signal=f"{fallback_fraction:.0%} of passes woke via pass_interval fallback (heap empty)",
                rationale=(
                    f"{fallback_count}/{n} passes ({fallback_fraction:.1%}) woke because the due-heap "
                    "was empty (wake_reason='pass_interval').\n"
                    "This indicates heap-empty fallback degeneracy — currents are not re-scheduling "
                    "themselves on the adaptive heap. Review current intervals and gate conditions."
                ),
                sample_size=n,
            )
        )

    if not hints:
        hints.append(
            TuningHint.model_construct(
                severity="info",
                knob="pass_interval",
                scope={"global": "true"},
                current_value=current_pass_interval,
                recommended_value=None,
                signal=f"pass_interval well-sized. p50={p50 * 1000:.1f}ms, p99={p99 * 1000:.1f}ms",
                rationale=(
                    f"p50={p50 * 1000:.1f}ms, p99={p99 * 1000:.1f}ms are well below the 80% saturation "
                    f"threshold ({0.8 * current_pass_interval * 1000:.1f}ms).\n"
                    f"Fallback fraction={fallback_fraction:.1%} is below 30%. No change recommended."
                ),
                sample_size=n,
            )
        )

    return hints


def _tune_retry_policy(rejects: list[RejectEntry]) -> list[TuningHint]:
    """Analyse retry-relevant rejects to recommend retry policy adjustments.

    Filters to ``_RETRY_POLICY_KINDS`` (HTTP errors and canal-layer skip
    kinds).  Groups canal-kind rejects by ``(from_name, to_name)`` edge
    and HTTP-kind rejects by ``host or source``.  For each group emits:

    * MED hint to raise ``stop_after_attempt`` when the majority of
      ``attempt_number`` values hit the ceiling.
    * LOW hint to tune ``wait_random_exponential`` / ``wait_penstock``
      when ``duration_sec`` and ``cooldown_sec`` medians are close.

    Args:
        rejects: List of :class:`~incorporator.RejectEntry` records.

    Returns:
        List of :class:`TuningHint`.
    """
    relevant = [r for r in rejects if r.error_kind in _RETRY_POLICY_KINDS]
    if not relevant:
        return []

    # Group by (from_name, to_name) for canal kinds; by host/source for HTTP.
    groups: dict[str, list[RejectEntry]] = {}
    for r in relevant:
        if r.error_kind == "HTTPStatusError":
            key = r.host or r.source
        else:
            # Canal kinds — key on edge tuple stringified for display.
            key = f"{r.from_name}->{r.to_name}" if r.from_name is not None else r.source
        groups.setdefault(key, []).append(r)

    hints: list[TuningHint] = []
    for group_key, group in groups.items():
        is_canal = group[0].error_kind != "HTTPStatusError"
        stop_knob = "canal.stop_after_attempt" if is_canal else "http.stop_after_attempt"
        wait_knob = "canal.wait_penstock" if is_canal else "http.wait_random_exponential"
        scope: dict[str, str] = {"edge": group_key} if is_canal else {"host": group_key}

        # Attempt-number ceiling detection.
        with_attempt = [r for r in group if r.attempt_number is not None]
        if with_attempt:
            attempt_numbers = [r.attempt_number for r in with_attempt]
            # attempt_numbers is List[int | None] but filtered to non-None above.
            non_none_attempts: list[int] = [a for a in attempt_numbers if a is not None]
            if non_none_attempts:
                max_attempt = max(non_none_attempts)
                at_ceiling = sum(1 for a in non_none_attempts if a == max_attempt)
                if at_ceiling > len(non_none_attempts) / 2:
                    hints.append(
                        TuningHint.model_construct(
                            severity="med",
                            knob=stop_knob,
                            scope=scope,
                            current_value=max_attempt,
                            recommended_value=None,
                            signal=(
                                f"{at_ceiling}/{len(non_none_attempts)} retries hit ceiling"
                                f" (attempt={max_attempt}) for {group_key!r}"
                            ),
                            rationale=(
                                f"Majority ({at_ceiling}/{len(non_none_attempts)}) of rejects for {group_key!r} "
                                f"reached the retry ceiling (attempt_number={max_attempt}).\n"
                                "Raise stop_after_attempt to allow more retries before giving up."
                            ),
                            sample_size=len(group),
                        )
                    )

        # Wait-exponential / wait-penstock tuning from duration_sec vs cooldown_sec.
        with_timing = [r for r in group if r.duration_sec is not None and r.cooldown_sec is not None]
        if with_timing:
            median_duration = statistics.median(r.duration_sec for r in with_timing if r.duration_sec is not None)
            median_cooldown = statistics.median(r.cooldown_sec for r in with_timing if r.cooldown_sec is not None)
            if median_cooldown > 0 and abs(median_duration - median_cooldown) / median_cooldown < 0.5:
                hints.append(
                    TuningHint.model_construct(
                        severity="low",
                        knob=wait_knob,
                        scope=scope,
                        current_value=None,
                        recommended_value=None,
                        signal=(
                            f"median duration ({median_duration:.2f}s) ≈ median cooldown"
                            f" ({median_cooldown:.2f}s) for {group_key!r}"
                        ),
                        rationale=(
                            f"median duration_sec={median_duration:.2f}s is within 50% of "
                            f"median cooldown_sec={median_cooldown:.2f}s for {group_key!r}.\n"
                            f"tune {wait_knob}(max≈{median_cooldown:.2f})"
                        ),
                        sample_size=len(with_timing),
                    )
                )

    return hints


def _tune_compound_budget(pass_interval: float | None) -> list[TuningHint]:
    """Check whether the worst-case compound retry budget exceeds the configured pass_interval.

    A pure static-cap check — no records consumed.  Fires when
    ``_CANAL_OUTER_STOP × _HTTP_INNER_STOP × _HTTP_INNER_WAIT_MAX >= pass_interval``,
    meaning a single stalled tick could block the scheduler for a multiple of the
    intended pass window.

    Args:
        pass_interval: The ``pass_interval`` the scheduler was configured with, in
            seconds.  ``None`` or non-positive values are silently skipped.

    Returns:
        A list containing one HIGH :class:`TuningHint` when the budget exceeds
        ``pass_interval``, or an empty list otherwise.
    """
    if pass_interval is None or pass_interval <= 0:
        return []
    if _COMPOUND_RETRY_BUDGET_SEC < pass_interval:
        return []
    return [
        TuningHint.model_construct(
            severity="high",
            knob="compound_retry_budget",
            scope={"global": "tideweaver"},
            current_value=_COMPOUND_RETRY_BUDGET_SEC,  # 1200.0
            recommended_value=None,
            signal=(
                f"{_CANAL_OUTER_STOP} × {_HTTP_INNER_STOP} × {_HTTP_INNER_WAIT_MAX:.0f}s"
                f" = {_COMPOUND_RETRY_BUDGET_SEC:.0f}s exceeds pass_interval={pass_interval:.1f}s"
            ),
            rationale=(
                f"Worst-case compound retry budget ({_CANAL_OUTER_STOP} outer × "
                f"{_HTTP_INNER_STOP} inner × {_HTTP_INNER_WAIT_MAX:.0f}s max wait = "
                f"{_COMPOUND_RETRY_BUDGET_SEC:.0f}s) exceeds the configured "
                f"pass_interval={pass_interval:.1f}s. A single stalled tick may block "
                f"the scheduler for up to {_COMPOUND_RETRY_BUDGET_SEC / pass_interval:.1f}× "
                f"the intended pass window.\n"
                "To remediate: lower pass_interval (if ticks complete well within "
                "budget) OR lower stop_after_attempt / wait max on the HTTP inner "
                "retry and/or the canal outer retry in on_error='restart' currents."
            ),
            sample_size=0,  # static rule — no records consumed
        )
    ]


def _tune_parent_child(
    tides: list[Tide],
    waves: list[Wave],
    *,
    threshold: int = 5,
) -> list[TuningHint]:
    """Detect parent-child silent-skip patterns from Tide + Wave records.

    Two layered signals:

    1. Wave-field scan (high-confidence, forward-looking): waves with
       ``parent_snapshot_size == 0`` indicate the upstream snapshot was
       empty. The current scheduler returns early BEFORE emitting a Wave
       on these paths, so this branch is dormant under the v1.2.x scheduler
       but ready to activate if/when a future chain instruments those fields
       before early-return.
    2. Tide-frequency rule (coarse, currently-actionable): a current that
       fires >= threshold tides AND the wave pool is empty globally signals
       a current is firing without producing waves. Per-current scope.

    Args:
        tides: Scheduler pass records from a :class:`Tideweaver` run.
        waves: Chunk telemetry records from a streaming pipeline.
        threshold: Minimum tide count required to trigger analysis; also the
            minimum per-current fire count for the tide-frequency rule.

    Returns:
        A list of :class:`TuningHint` objects with ``knob`` set to
        ``"parent_current"``.
    """
    hints: list[TuningHint] = []

    if len(tides) < threshold:
        return hints

    # Wave-field scan (forward-looking — dormant under v1.2.x scheduler).
    zero_snap_count = sum(1 for w in waves if w.parent_snapshot_size == 0)

    if zero_snap_count > 0:
        hints.append(
            TuningHint.model_construct(
                severity="high",
                knob="parent_current",
                scope={"global": "watershed"},
                current_value=None,
                recommended_value=None,
                signal=f"{zero_snap_count} waves with parent_snapshot_size=0",
                rationale=(
                    "Upstream snapshot was empty; confirm the parent current is firing "
                    "and producing rows before the child tick runs."
                ),
                sample_size=zero_snap_count,
            )
        )

    # Tide-frequency rule (currently-actionable, coarse).
    # Fires only when len(waves) == 0 globally to avoid false positives when
    # waves are present but parent-child fields are simply unpopulated.
    if len(waves) == 0:
        fire_counts: dict[str, int] = {}
        for tide in tides:
            for name in tide.fired:
                fire_counts[name] = fire_counts.get(name, 0) + 1
        for name, count in fire_counts.items():
            if count >= threshold:
                hints.append(
                    TuningHint.model_construct(
                        severity="med",
                        knob="parent_current",
                        scope={"current": name},
                        current_value=None,
                        recommended_value=None,
                        signal=f"{name} fired {count} tides but 0 waves recorded",
                        rationale=(
                            f"Current {name!r} fired {count} times but produced no waves — "
                            f"possible parent-child silent-skip; check parent_current is firing "
                            f"and the parent's source-side filter is not too restrictive."
                        ),
                        sample_size=count,
                    )
                )

    return hints


def tune(
    *,
    rejects: list[RejectEntry] | None = None,
    tides: list[Tide] | None = None,
    waves: list[Wave] | None = None,
    pass_interval: float | None = None,
) -> TuningReport:
    """Generate tuning recommendations from accumulated outcome records.

    Runs up to five rule functions over the supplied record lists and
    aggregates the resulting :class:`TuningHint` objects into a
    :class:`TuningReport`.  Any argument may be omitted; rules that
    require missing inputs are skipped.

    Args:
        rejects: Canal-layer and HTTP :class:`~incorporator.RejectEntry`
            records, typically from :attr:`Tideweaver.rejects` or
            :attr:`IncorporatorList.rejects`.
        tides: Scheduler pass records from a :class:`Tideweaver` run,
            typically collected by iterating :meth:`Tideweaver.run`.
        waves: Chunk telemetry records from a streaming pipeline,
            typically collected by iterating :meth:`Incorporator.stream`.
        pass_interval: The ``pass_interval`` the scheduler was configured
            with, in seconds.  Required for the ``_tune_pass_interval``
            rule; ignored if ``tides`` is also omitted.

    Returns:
        A :class:`TuningReport` with structured hints and a summary dict.
    """
    rejects = rejects or []
    tides = tides or []
    waves = waves or []

    hints: list[TuningHint] = []
    if waves:
        hints.extend(_tune_chunk_size(waves))
    if rejects:
        hints.extend(_tune_penstock_rate(rejects))
    if rejects and tides:
        hints.extend(_tune_surge_threshold(rejects, tides))
    if tides and pass_interval is not None:
        hints.extend(_tune_pass_interval(tides, pass_interval))
    if rejects:
        hints.extend(_tune_retry_policy(rejects))
    hints.extend(_tune_compound_budget(pass_interval))
    if tides:
        hints.extend(_tune_parent_child(tides, waves))

    # Window timestamps from waves + tides only (RejectEntry has no timestamp).
    timestamps = [w.timestamp for w in waves] + [t.timestamp for t in tides]
    summary: dict[str, Any] = {
        "total_chunks": len(waves),
        "total_passes": len(tides),
        "total_rejects": len(rejects),
        "window_start": min(timestamps) if timestamps else None,
        "window_end": max(timestamps) if timestamps else None,
    }

    return TuningReport.model_construct(
        hints=hints,
        summary=summary,
        analyzed_at=datetime.now(timezone.utc),
    )
