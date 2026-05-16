"""Tideweaver: lightweight orchestration over ``stream()`` / fjord-flush / ``export()``.

A :class:`Watershed` declares the plan — a time window plus a graph of named
:class:`Current` nodes connected by edges.  :class:`Tideweaver` runs the plan:
each current ticks on its own interval; hard edges gate dependents until the
upstream emits a new :class:`~incorporator.Wave`; soft edges only sequence the
in-pass order without a data wait.  One :class:`Tide` log record is emitted per
scheduler pass.

The vocabulary is deliberately small:

* :class:`Tideweaver` — the orchestrator.
* :class:`Watershed` — the plan (window + currents + edges).
* :class:`Current` (and the verb-typed subclasses :class:`Stream`,
  :class:`Fjord`, :class:`Export`) — one node in the graph.
* :class:`Tide` — one scheduler pass; emitted as a log record.
* :class:`~incorporator.Wave` — already exists; one emit from a stream call
  or a fjord flush.  Unmodified by this module.

A "fjord flush" is the tick unit of a :class:`Fjord` current: snapshot the
upstream currents' registries, run the user-supplied ``outflow(state)``
function, build the dynamic output class, export.  It is NOT a call to
``cls.fjord()`` (which is a long-running daemon ill-suited to per-interval
ticking).  Stream source ingestion is owned by the upstream :class:`Stream`
currents in the graph.
"""

from .current import Current, Export, Fjord, Stream
from .scheduler import Tideweaver
from .tide import Tide
from .watershed import Edge, Watershed

__all__ = [
    "Current",
    "Edge",
    "Export",
    "Fjord",
    "Stream",
    "Tide",
    "Tideweaver",
    "Watershed",
]
