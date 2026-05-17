"""The :class:`Watershed` plan — currents + edges over a single time window.

A ``Watershed`` is a serialisable description of one Tideweaver run: when the
window opens and closes, which :class:`Current` nodes are in the graph, and
which edges connect them.  Four shape constructors cover the common topologies
(``chain`` / ``diamond`` / ``fanout`` / ``parallel``); the bare ``Watershed(...)``
constructor stays available for custom shapes with mixed-mode edges.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Literal, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .current import Current

DependencyMode = Literal["hard", "soft"]


class Edge(BaseModel):
    """One directed edge in a :class:`Watershed` graph.

    Attributes:
        from_name: The upstream current name.
        to_name: The dependent current name.
        mode: ``"hard"`` gates the dependent until the upstream emits a new
            wave; ``"soft"`` only sequences the in-tick order without a
            data wait.
    """

    model_config = ConfigDict(frozen=True)

    from_name: str
    to_name: str
    mode: DependencyMode = "hard"


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


class Watershed(BaseModel):
    """A Tideweaver plan: window + currents + edges.

    Sidecar paths (``inflow`` / ``outflow``) on ``Watershed`` are graph-level
    defaults.  A :class:`Current` can override them via its own ``inflow`` /
    ``outflow`` fields.

    Use one of the shape constructors for common topologies; fall back to
    ``Watershed(currents=..., edges=...)`` for custom graphs with mixed-mode
    edges.
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
        with mode ``"hard"`` so users can specify dependencies the short
        way without losing the explicit-mode option.
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

        # Fold depends_on into edges (mode='hard') unless an explicit edge
        # already covers that pair.
        existing = {(e.from_name, e.to_name) for e in self.edges}
        for c in self.currents:
            for dep in c.depends_on:
                if (dep, c.name) not in existing:
                    self.edges.append(Edge(from_name=dep, to_name=c.name, mode="hard"))
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
        dependency_mode: DependencyMode = "hard",
        **kwargs: Any,
    ) -> "Watershed":
        """Build a linear chain: ``currents[0] → currents[1] → ... → currents[-1]``."""
        currents = list(currents)
        edges = [
            Edge(from_name=a.name, to_name=b.name, mode=dependency_mode) for a, b in zip(currents[:-1], currents[1:])
        ]
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def diamond(
        cls,
        *,
        window: Tuple[datetime, datetime],
        head: Current,
        middle: Sequence[Current],
        tail: Current,
        dependency_mode: DependencyMode = "hard",
        **kwargs: Any,
    ) -> "Watershed":
        """Build a diamond: ``head → each middle → tail``."""
        middle = list(middle)
        if not middle:
            raise ValueError("Watershed.diamond requires at least one middle current.")
        currents: List[Current] = [head, *middle, tail]
        edges: List[Edge] = []
        for m in middle:
            edges.append(Edge(from_name=head.name, to_name=m.name, mode=dependency_mode))
            edges.append(Edge(from_name=m.name, to_name=tail.name, mode=dependency_mode))
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def fanout(
        cls,
        *,
        window: Tuple[datetime, datetime],
        source: Current,
        sinks: Sequence[Current],
        dependency_mode: DependencyMode = "hard",
        **kwargs: Any,
    ) -> "Watershed":
        """Build a fan-out: one ``source`` with N independent ``sinks``."""
        sinks = list(sinks)
        if not sinks:
            raise ValueError("Watershed.fanout requires at least one sink current.")
        currents: List[Current] = [source, *sinks]
        edges = [Edge(from_name=source.name, to_name=s.name, mode=dependency_mode) for s in sinks]
        return cls(window=window, currents=currents, edges=edges, **kwargs)

    @classmethod
    def parallel(
        cls,
        *,
        window: Tuple[datetime, datetime],
        currents: Iterable[Current],
        **kwargs: Any,
    ) -> "Watershed":
        """Build a parallel watershed: N unrelated currents sharing only the window.

        Rejects any ``dependency_mode`` kwarg — parallel has no edges to mode.
        """
        if "dependency_mode" in kwargs:
            raise TypeError(
                "Watershed.parallel does not accept a dependency_mode — "
                "there are no edges to apply a mode to.  Use chain/diamond/fanout, "
                "or pass an explicit edges= list to the Watershed(...) constructor."
            )
        return cls(window=window, currents=list(currents), edges=[], **kwargs)
