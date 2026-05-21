"""Tideweaver: lightweight orchestration over ``stream()`` / fjord-flush / ``export()``.

A :class:`Watershed` declares the plan — a time window plus a graph of named
:class:`Current` nodes connected by edges.  :class:`Tideweaver` runs the plan:
each current ticks on its own interval; each inbound edge's
:class:`FlowControl` decides whether the dependent fires this pass.  One
:class:`Tide` log record is emitted per scheduler pass.

The vocabulary is deliberately small:

* :class:`Tideweaver` — the orchestrator.
* :class:`Watershed` — the plan (window + currents + edges).
* :class:`Current` (and the verb-typed subclasses :class:`Stream`,
  :class:`Fjord`, :class:`Export`) — one node in the graph.
* :class:`Tide` — one scheduler pass; emitted as a log record.
* :class:`~incorporator.Wave` — already exists; one emit from a stream call
  or a fjord flush.  Unmodified by this module.
* :class:`FlowControl` — the per-edge composition of gate + surge_barrier
  + penstock + reservoir + spillway.  Mirrors real canal/dam engineering:
  :class:`HardLock` / :class:`SoftPass` / :class:`Weir` decide pass/hold;
  :class:`SurgeBarrier` overrides under extreme upstream-in-flight load;
  :class:`Penstock` rate-limits flow across the edge; :class:`Reservoir`
  buffers N recent waves; :class:`Spillway` handles overflow.

A "fjord flush" is the scheduling primitive of a :class:`Fjord` current:
snapshot the upstream currents' registries, run the user-supplied
``outflow(state)`` function, build the dynamic output class, export.  It is
NOT a call to ``cls.fjord()`` (which is a long-running daemon ill-suited to
per-interval ticking).  Stream source ingestion is owned by the upstream
:class:`Stream` currents in the graph.
"""

from .current import Current, Export, Fjord, Stream
from .flow import (
    DropOldest,
    ExportToArchive,
    FlowControl,
    Gate,
    GateMode,
    HardLock,
    Penstock,
    RaiseOverflow,
    Reservoir,
    SoftPass,
    Spillway,
    SurgeAction,
    SurgeBarrier,
    Weir,
    flow_from_mode,
)
from .scheduler import Tideweaver
from .tide import Tide
from .watershed import Edge, Watershed

__all__ = [
    "Current",
    "DropOldest",
    "Edge",
    "Export",
    "ExportToArchive",
    "Fjord",
    "FlowControl",
    "Gate",
    "GateMode",
    "HardLock",
    "Penstock",
    "RaiseOverflow",
    "Reservoir",
    "SoftPass",
    "Spillway",
    "Stream",
    "SurgeAction",
    "SurgeBarrier",
    "Tide",
    "Tideweaver",
    "Watershed",
    "Weir",
    "flow_from_mode",
]
