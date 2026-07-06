"""The :class:`Watershed` plan — currents + edges over a single time window.

A ``Watershed`` is a declarative plan for one Tideweaver run: when the window
opens and closes, which :class:`Current` nodes are in the graph, and which
edges connect them.  Four shape constructors cover the common topologies
(``chain`` / ``diamond`` / ``fanout`` / ``parallel``); the bare ``Watershed(...)``
constructor stays available for custom shapes with mixed-mode edges.

Build a ``Watershed`` from ``watershed.json`` via
:func:`~incorporator.tideweaver.config.load_watershed`.  A ``Watershed``
instance is not itself JSON-dumpable: ``Current.cls`` holds a live class
object, so ``model_dump_json()`` raises, and a python-mode dump-then-validate
round-trip downgrades ``Stream``/``Fjord``/``Export`` currents to bare
``Current``.

Each edge carries a :class:`~.flow.FlowControl` — the per-edge composition of
gate / surge barrier / penstock / reservoir / spillway that the scheduler
honours.  The simpler ``gate_mode=`` shorthand on the shape constructors maps
to ``FlowControl`` via :func:`~.flow.flow_from_mode`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator

from .current import Current, Export, Fjord, Stream
from .flow import FlowControl, GateMode, flow_from_mode


class Edge(BaseModel):
    """One directed edge in a :class:`Watershed`, governed by a :class:`~.flow.FlowControl`.

    .. code-block:: python

        # Default — hard gating, single-wave snapshot.
        Edge(from_name="binance", to_name="arb_fjord")

        # Mode shorthand (mutually exclusive with ``flow=``).
        Edge(from_name="binance", to_name="arb_fjord", gate_mode="weir")

        # Full control.
        Edge(
            from_name="binance",
            to_name="arb_fjord",
            flow=FlowControl(
                gate=Weir(),
                reservoir=Reservoir(depth=5),
                surge_barrier=SurgeBarrier(threshold_multiple=10.0, action="bypass"),
            ),
        )

    JSON aliases: ``from_name`` / ``to_name`` accept ``"from"`` / ``"to"``
    when constructing from a dict (since ``from`` is a Python keyword).

    Attributes:
        from_name: The upstream current name.
        to_name: The dependent current name.
        flow: The :class:`FlowControl` governing this edge.
        auto_derived: ``True`` when the edge was synthesised by Watershed
            validation from a :attr:`Stream.parent_current` declaration;
            ``False`` for user-declared edges.
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    from_name: str = Field(validation_alias=AliasChoices("from_name", "from"))
    to_name: str = Field(validation_alias=AliasChoices("to_name", "to"))
    flow: FlowControl = Field(default_factory=FlowControl)
    auto_derived: bool = False

    @model_validator(mode="before")
    @classmethod
    def _gate_mode_shorthand(cls, data: Any) -> Any:
        """Translate ``gate_mode=<mode>`` into ``flow=flow_from_mode(<mode>)``.  Mutex with ``flow=``.

        Note the asymmetry: bare ``Edge()`` (no kwargs) yields a default
        :class:`FlowControl` with ``surge_barrier=None``, while
        ``Edge(gate_mode="hard")`` invokes :func:`flow_from_mode` which
        attaches a default :class:`SurgeBarrier` (threshold ``2.0``,
        action ``"skip"``).  ``"soft"`` and ``"weir"`` do not add a
        SurgeBarrier.  Pass ``flow=`` explicitly to opt out of the
        implicit barrier on ``"hard"``.
        """
        if isinstance(data, dict) and "gate_mode" in data:
            mode = data.pop("gate_mode")
            if data.get("flow") is not None:
                raise ValueError("Edge: pass gate_mode= (shorthand) or flow= (full FlowControl), not both.")
            data["flow"] = flow_from_mode(mode)
        return data


def _toposort(currents: Sequence[Current], edges: Sequence[Edge]) -> list[str]:
    """Return a topological order of current names; raise on cycles."""
    names = [c.name for c in currents]
    indeg: dict[str, int] = dict.fromkeys(names, 0)
    adj: dict[str, list[str]] = {n: [] for n in names}
    for e in edges:
        adj[e.from_name].append(e.to_name)
        indeg[e.to_name] += 1
    order: list[str] = []
    queue = [n for n in names if indeg[n] == 0]
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if len(order) != len(names):
        cyclic = [n for n in names if indeg[n] > 0]
        raise ValueError(
            f"Watershed graph has a cycle involving: {sorted(cyclic)}. "
            "Tideweaver requires a directed acyclic graph of currents."
        )
    return order


def _resolve_flow(
    gate_mode: GateMode | str | None,
    flow: FlowControl | None,
    default_mode: GateMode | str = "hard",
) -> FlowControl:
    """Resolve a (gate_mode, flow) pair into a single :class:`FlowControl`.

    Mutually exclusive: passing both raises ``ValueError``.  Passing neither
    returns the canonical default for ``default_mode``.
    """
    if gate_mode is not None and flow is not None:
        raise ValueError("Pass either gate_mode= (shorthand) or flow= (full FlowControl), not both.")
    if flow is not None:
        return flow
    return flow_from_mode(gate_mode or default_mode)


class Watershed(BaseModel):
    """The declarative plan describing what runs, when, and in what order inside a Tideweaver window.

    Instead of writing custom asyncio code to coordinate multi-source
    pipelines, declare the topology — window, currents, edges — and
    let :class:`Tideweaver` schedule it.  The four shape constructors
    (:meth:`chain`, :meth:`diamond`, :meth:`fanout`, :meth:`parallel`)
    cover the common topologies; the bare ``Watershed(...)``
    constructor stays available for custom shapes with mixed-mode
    edges:

    .. code-block:: python

        watershed = Watershed.diamond(
            window=(start, end),
            head=binance_stream,
            middle=[coinbase_stream, kraken_stream],
            tail=arb_fjord,
        )
        async for tide in Tideweaver(watershed).run():
            ...

    Attributes:
        window: ``(start, end)`` UTC bounds — start inclusive, end
            exclusive.  Defines the orchestration window.
        currents: The :class:`Current` nodes in the graph; names must
            be unique.
        edges: Directed :class:`Edge` list; any
            ``Current.depends_on`` declarations are folded in as
            default-FlowControl ``"hard"`` edges at validation time.
        inflow: Graph-level default sidecar path; per-current
            ``inflow`` overrides.
        outflow: Graph-level default outflow path; per-current
            ``outflow`` overrides.
        drain_timeout: Seconds the scheduler waits for in-flight ticks
            to finish after the window closes.

    The post-construction validator enforces unique names, a
    window with ``end > start``, edge endpoints that reference real
    currents, and a directed acyclic graph (cycles are rejected).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    window: tuple[datetime, datetime] = Field(..., description="Inclusive start, exclusive end.")
    name: str | None = Field(
        default=None,
        description=(
            "Optional human-readable label for this run; drives the default logger_name in "
            "LoggedTideweaver when no explicit logger_name is passed."
        ),
    )
    currents: list[Current] = Field(..., min_length=1)
    edges: list[Edge] = Field(default_factory=list)
    inflow: Path | None = None
    outflow: Path | None = None
    drain_timeout: float = Field(30.0, ge=0.0)

    @model_validator(mode="after")
    def _validate_graph(self) -> Watershed:
        """Enforce: unique names, window order, edge endpoints exist, no cycles.

        Also folds any ``Current.depends_on`` declarations into ``edges``
        with default ``FlowControl`` (hard gating) so users can specify
        dependencies the short way without losing the explicit-flow option.
        """
        names = [c.name for c in self.currents]
        counts = Counter(names)
        if any(v > 1 for v in counts.values()):
            dupes = sorted(n for n, v in counts.items() if v > 1)
            raise ValueError(f"Watershed currents must have unique names; duplicates: {dupes}")

        # Two currents that PRODUCE data (Stream / Fjord / CustomCurrent —
        # anything but Export) sharing a ``cls.__name__`` silently collide on
        # the scheduler's ``cls.__name__``-keyed state: the fjord flush's
        # ``state[dep.cls.__name__]`` dict and the class-level
        # ``_tideweaver_snapshot`` strong-ref both key/park by name/identity.
        # ``Export`` is deliberately excluded — it's read-only (snapshots an
        # upstream's registry to disk) and is INTENDED to share ``cls`` with
        # its upstream Stream/Fjord; that's not a collision, it's the
        # documented usage pattern (see Export's class docstring).
        producer_currents = [c for c in self.currents if not isinstance(c, Export)]
        cls_names = [c.cls.__name__ for c in producer_currents]
        cls_counts = Counter(cls_names)
        if any(v > 1 for v in cls_counts.values()):
            dupe_cls = sorted(n for n, v in cls_counts.items() if v > 1)
            offenders = {
                cls_name: sorted(c.name for c in producer_currents if c.cls.__name__ == cls_name)
                for cls_name in dupe_cls
            }
            raise ValueError(
                f"Watershed currents must bind distinct Incorporator classes; "
                f"cls.__name__ collision(s): {offenders}. "
                "Two producing currents (Stream/Fjord/CustomCurrent) sharing a class "
                "silently overwrite each other's fjord state (keyed by cls.__name__) and "
                "_tideweaver_snapshot (parked on the class). Subclass or rename so each "
                "current's cls.__name__ is unique. (Export is exempt — it legitimately "
                "shares cls with its upstream to snapshot that registry to disk.)"
            )

        start, end = self.window
        if end <= start:
            raise ValueError(f"Watershed window end ({end}) must be after start ({start}).")

        name_set = set(names)

        # Fold depends_on into edges (default FlowControl = hard) unless an
        # explicit edge already covers that pair.
        existing = {(e.from_name, e.to_name) for e in self.edges}
        for c in self.currents:
            for dep in c.depends_on:
                if (dep, c.name) not in existing:
                    self.edges.append(Edge(from_name=dep, to_name=c.name))
                    existing.add((dep, c.name))

        for c in self.currents:
            if isinstance(c, Stream) and c.parent_current is not None:
                if c.parent_current not in name_set:
                    raise ValueError(
                        f"Stream {c.name!r}: parent_current={c.parent_current!r} "
                        f"does not match any current in the watershed."
                    )
                if (c.parent_current, c.name) not in existing:
                    self.edges.append(Edge(from_name=c.parent_current, to_name=c.name, auto_derived=True))
                    existing.add((c.parent_current, c.name))

        for c in self.currents:
            if isinstance(c, Fjord) and c.parent_currents:
                for parent_name in c.parent_currents:
                    if parent_name not in name_set:
                        raise ValueError(
                            f"Fjord {c.name!r}: parent_currents references {parent_name!r} "
                            f"which does not match any current in the watershed."
                        )
                    if (parent_name, c.name) not in existing:
                        self.edges.append(Edge(from_name=parent_name, to_name=c.name, auto_derived=True))
                        existing.add((parent_name, c.name))

        for e in self.edges:
            if e.from_name not in name_set:
                raise ValueError(f"Edge references unknown current {e.from_name!r} (from).")
            if e.to_name not in name_set:
                raise ValueError(f"Edge references unknown current {e.to_name!r} (to).")

        # Toposort raises on cycles.
        _toposort(self.currents, self.edges)
        return self

    def toposort(self) -> list[str]:
        """Return current names in a valid topological order."""
        return _toposort(self.currents, self.edges)

    # -------------------------------------------------------------------
    # Shape constructors
    # -------------------------------------------------------------------

    @classmethod
    def chain(
        cls,
        *,
        window: tuple[datetime, datetime],
        currents: Sequence[Current],
        gate_mode: GateMode | str | None = None,
        flow: FlowControl | None = None,
        inflow: Path | None = None,
        outflow: Path | None = None,
        drain_timeout: float = 30.0,
        **kwargs: Any,
    ) -> Watershed:
        """Sequential pipeline where each current waits for its upstream's data.

        Use it for ETL stages where stage N depends on stage N-1 —
        load, then enrich, then validate, then export — so each stage
        runs against the freshest output of the previous one:

        .. code-block:: python

            watershed = Watershed.chain(
                window=(start, end),
                currents=[load, enrich, validate, export],
                gate_mode="weir",
            )

        Builds ``currents[0] → currents[1] → ... → currents[-1]`` with
        every edge sharing one :class:`FlowControl`.  Pass ``gate_mode=``
        for the simple case (``"hard"`` / ``"soft"`` / ``"weir"``) or
        ``flow=`` for full per-edge control; passing both raises.
        """
        resolved = _resolve_flow(gate_mode, flow)
        currents = list(currents)
        edges = [Edge(from_name=a.name, to_name=b.name, flow=resolved) for a, b in pairwise(currents)]
        return cls(
            window=window,
            currents=currents,
            edges=edges,
            inflow=inflow,
            outflow=outflow,
            drain_timeout=drain_timeout,
            **kwargs,
        )

    @classmethod
    def diamond(
        cls,
        *,
        window: tuple[datetime, datetime],
        head: Current,
        middle: Sequence[Current],
        tail: Current,
        gate_mode: GateMode | str | None = None,
        flow: FlowControl | None = None,
        inflow: Path | None = None,
        outflow: Path | None = None,
        drain_timeout: float = 30.0,
        **kwargs: Any,
    ) -> Watershed:
        """The canonical multi-source fusion shape — one head feeds N middle stages that all converge into one tail.

        Use it for cross-exchange arbitrage scanners (Binance +
        Coinbase + Kraken → composite best market), fantasy NASCAR
        diamonds (qualifying + practice + race feeds → fused driver
        scoreboard), and multi-region replica fusion.  The tail is
        typically a :class:`Fjord` current that mark-to-markets the
        merged upstream state:

        .. code-block:: python

            watershed = Watershed.diamond(
                window=(start, end),
                head=binance_stream,
                middle=[coinbase_stream, kraken_stream],
                tail=arb_fjord,
                gate_mode="weir",
            )

        Builds ``head → each middle → tail`` with every edge sharing
        one :class:`FlowControl`.
        """
        resolved = _resolve_flow(gate_mode, flow)
        middle = list(middle)
        if not middle:
            raise ValueError("Watershed.diamond requires at least one middle current.")
        currents: list[Current] = [head, *middle, tail]
        edges: list[Edge] = []
        for m in middle:
            edges.append(Edge(from_name=head.name, to_name=m.name, flow=resolved))
            edges.append(Edge(from_name=m.name, to_name=tail.name, flow=resolved))
        return cls(
            window=window,
            currents=currents,
            edges=edges,
            inflow=inflow,
            outflow=outflow,
            drain_timeout=drain_timeout,
            **kwargs,
        )

    @classmethod
    def fanout(
        cls,
        *,
        window: tuple[datetime, datetime],
        source: Current,
        sinks: Sequence[Current],
        gate_mode: GateMode | str | None = None,
        flow: FlowControl | None = None,
        inflow: Path | None = None,
        outflow: Path | None = None,
        drain_timeout: float = 30.0,
        **kwargs: Any,
    ) -> Watershed:
        """Broadcast a single source to multiple downstream consumers, each on its own interval.

        Use it for one upstream feed with N output formats — raw
        orders fanning out to NDJSON for downstream pipelines, Parquet
        for analysts, and SQLite for ops dashboards — where each sink
        ticks on its own cadence:

        .. code-block:: python

            watershed = Watershed.fanout(
                window=(start, end),
                source=raw_orders_stream,
                sinks=[ndjson_export, parquet_export, sqlite_export],
            )

        Builds ``source → each sink`` with every edge sharing one
        :class:`FlowControl`.
        """
        resolved = _resolve_flow(gate_mode, flow)
        sinks = list(sinks)
        if not sinks:
            raise ValueError("Watershed.fanout requires at least one sink current.")
        currents: list[Current] = [source, *sinks]
        edges = [Edge(from_name=source.name, to_name=s.name, flow=resolved) for s in sinks]
        return cls(
            window=window,
            currents=currents,
            edges=edges,
            inflow=inflow,
            outflow=outflow,
            drain_timeout=drain_timeout,
            **kwargs,
        )

    @classmethod
    def parallel(
        cls,
        *,
        window: tuple[datetime, datetime],
        currents: Iterable[Current],
        inflow: Path | None = None,
        outflow: Path | None = None,
        drain_timeout: float = 30.0,
        **kwargs: Any,
    ) -> Watershed:
        """Run N independent pipelines concurrently in the same orchestration window, with no edges between them.

        Use it when you want an overnight chunked drain across
        unrelated sources — Binance, CoinGecko, NHTSA — all running
        on their own cadences within a shared window, none of them
        waiting on each other:

        .. code-block:: python

            watershed = Watershed.parallel(
                window=(start, end),
                currents=[binance_stream, coingecko_stream, nhtsa_stream],
            )

        Rejects any ``gate_mode`` or ``flow`` kwarg — parallel has no
        edges to govern.
        """
        for blocked in ("gate_mode", "flow"):
            if blocked in kwargs:
                raise TypeError(
                    f"Watershed.parallel does not accept a {blocked} — "
                    "there are no edges to govern.  Use chain/diamond/fanout, "
                    "or pass an explicit edges= list to the Watershed(...) constructor."
                )
        return cls(
            window=window,
            currents=list(currents),
            edges=[],
            inflow=inflow,
            outflow=outflow,
            drain_timeout=drain_timeout,
            **kwargs,
        )
