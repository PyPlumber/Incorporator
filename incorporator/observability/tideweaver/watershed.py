"""The :class:`Watershed` plan — currents + edges over a single time window.

A ``Watershed`` is a serialisable description of one Tideweaver run: when the
window opens and closes, which :class:`Current` nodes are in the graph, and
which edges connect them.  Four shape constructors cover the common topologies
(``chain`` / ``diamond`` / ``fanout`` / ``parallel``); the bare ``Watershed(...)``
constructor stays available for custom shapes with mixed-mode edges.

Each edge carries a :class:`~.flow.FlowControl` — the per-edge composition of
gate / surge barrier / penstock / reservoir / spillway that the scheduler
honours.  The simpler ``gate_mode=`` shorthand on the shape constructors maps
to ``FlowControl`` via :func:`~.flow.flow_from_mode`.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .current import Current
from .flow import FlowControl, GateMode, flow_from_mode


class Edge(BaseModel):
    """The dependency-edge contract: link one current to another with a :class:`FlowControl`.

    Each edge carries a :class:`~.flow.FlowControl` describing how waves
    pass through it — gate (pass/hold), optional surge barrier, optional
    penstock (rate limit), reservoir (buffer depth), spillway (overflow
    handler).  Defaults match today's behavior: ``FlowControl()`` is
    ``HardLock + no penstock + Reservoir(depth=1) + DropOldest + no
    surge barrier``.

    .. code-block:: python

        # Default — hard gating, single-wave snapshot.
        Edge(from_name="binance", to_name="arb_fjord")

        # Shorthand via mode string.
        Edge(from_name="binance", to_name="arb_fjord", flow=flow_from_mode("weir"))

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

    Attributes:
        from_name: The upstream current name.
        to_name: The dependent current name.
        flow: The :class:`FlowControl` governing this edge.
    """

    model_config = ConfigDict(frozen=True)

    from_name: str
    to_name: str
    flow: FlowControl = Field(default_factory=FlowControl)


def _toposort(currents: Sequence[Current], edges: Sequence[Edge]) -> List[str]:
    """Return a topological order of current names; raise on cycles."""
    names = [c.name for c in currents]
    indeg: dict[str, int] = dict.fromkeys(names, 0)
    adj: dict[str, list[str]] = {n: [] for n in names}
    for e in edges:
        adj[e.from_name].append(e.to_name)
        indeg[e.to_name] += 1
    order: List[str] = []
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
    gate_mode: Optional[GateMode],
    flow: Optional[FlowControl],
    default_mode: GateMode = "hard",
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

    window: Tuple[datetime, datetime] = Field(..., description="Inclusive start, exclusive end.")
    currents: List[Current] = Field(..., min_length=1)
    edges: List[Edge] = Field(default_factory=list)
    inflow: Optional[Path] = None
    outflow: Optional[Path] = None
    drain_timeout: float = Field(30.0, ge=0.0)

    @model_validator(mode="after")
    def _validate_graph(self) -> "Watershed":
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

        for e in self.edges:
            if e.from_name not in name_set:
                raise ValueError(f"Edge references unknown current {e.from_name!r} (from).")
            if e.to_name not in name_set:
                raise ValueError(f"Edge references unknown current {e.to_name!r} (to).")

        # Toposort raises on cycles.
        _toposort(self.currents, self.edges)
        return self

    def toposort(self) -> List[str]:
        """Return current names in a valid topological order."""
        return _toposort(self.currents, self.edges)

    # -------------------------------------------------------------------
    # Shape constructors
    # -------------------------------------------------------------------

    @classmethod
    def chain(
        cls,
        *,
        window: Tuple[datetime, datetime],
        currents: Sequence[Current],
        gate_mode: Optional[GateMode] = None,
        flow: Optional[FlowControl] = None,
        **kwargs: Any,
    ) -> "Watershed":
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
        edges = [Edge(from_name=a.name, to_name=b.name, flow=resolved) for a, b in zip(currents[:-1], currents[1:])]
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def diamond(
        cls,
        *,
        window: Tuple[datetime, datetime],
        head: Current,
        middle: Sequence[Current],
        tail: Current,
        gate_mode: Optional[GateMode] = None,
        flow: Optional[FlowControl] = None,
        **kwargs: Any,
    ) -> "Watershed":
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
        currents: List[Current] = [head, *middle, tail]
        edges: List[Edge] = []
        for m in middle:
            edges.append(Edge(from_name=head.name, to_name=m.name, flow=resolved))
            edges.append(Edge(from_name=m.name, to_name=tail.name, flow=resolved))
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def fanout(
        cls,
        *,
        window: Tuple[datetime, datetime],
        source: Current,
        sinks: Sequence[Current],
        gate_mode: Optional[GateMode] = None,
        flow: Optional[FlowControl] = None,
        **kwargs: Any,
    ) -> "Watershed":
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
        currents: List[Current] = [source, *sinks]
        edges = [Edge(from_name=source.name, to_name=s.name, flow=resolved) for s in sinks]
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def parallel(
        cls,
        *,
        window: Tuple[datetime, datetime],
        currents: Iterable[Current],
        **kwargs: Any,
    ) -> "Watershed":
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
        return cls(window=window, currents=list(currents), edges=[], **kwargs)
