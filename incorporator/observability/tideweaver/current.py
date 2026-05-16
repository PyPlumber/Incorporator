"""The :class:`Current` base + verb-typed subclasses for Tideweaver.

A ``Current`` is one node in a :class:`Watershed`: a name, an
:class:`~incorporator.Incorporator` subclass to invoke, a tick interval,
optional ``depends_on`` names, and an error policy.  The verb-typed
subclasses :class:`Stream`, :class:`Fjord`, and :class:`Export` carry the
kwargs their tick action needs, giving callers good mypy ergonomics.

The bare ``Current(...)`` constructor stays available as the escape hatch
for tests or unusual integrations that need to drive their own tick body.

Scheduler-side bookkeeping (last tick time, last wave timestamp,
last-consumed-from-upstream map) lives in :class:`Tideweaver`, not on these
models — these models are pure plan, not state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...base import Incorporator

OnErrorPolicy = Literal["restart", "isolate", "fail_watershed"]


class Current(BaseModel):
    """One node in a :class:`Watershed`.

    Attributes:
        name: Unique identifier within the watershed.
        cls: The :class:`~incorporator.Incorporator` subclass this current drives.
        interval: Minimum seconds between ticks.  The scheduler may run later
            (when dependencies gate it) but never sooner.
        depends_on: Names of upstream currents whose wave emissions gate this
            current's ticks when their edge mode is ``"hard"``.
        on_error: ``"restart"`` retries the tick (tenacity-backed exp backoff,
            5 attempts), ``"isolate"`` logs and continues siblings,
            ``"fail_watershed"`` re-raises and cancels the whole graph.
        skip_threshold: Multiplier on ``interval``.  If an upstream has been
            running longer than ``skip_threshold * interval`` for this tick,
            the current skips with reason ``"skip_ahead"``.
        inflow: Optional sidecar ``.py`` path (per-current override of the
            watershed-level default).
        outflow: Optional sidecar ``.py`` path (per-current override of the
            watershed-level default).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    name: str
    cls: Type[Incorporator]
    interval: float = Field(..., gt=0.0, description="Seconds between ticks; must be positive.")
    depends_on: List[str] = Field(default_factory=list)
    on_error: OnErrorPolicy = "restart"
    skip_threshold: float = Field(2.0, gt=0.0)
    inflow: Optional[Path] = None
    outflow: Optional[Path] = None


class Stream(Current):
    """A per-tick chunking-mode ``stream()`` drain.

    Each tick consumes :meth:`Incorporator.stream` in chunking mode
    (``stateful_polling=False``) until the source is drained, then exits.
    Tideweaver's ``interval`` IS the polling cadence between drains.

    The Stream's class registry persists between ticks via a
    ``_tideweaver_snapshot`` strong-ref the scheduler parks on the class,
    so a downstream :class:`Fjord` current sees accumulated upstream state
    on each flush.  If what you actually want is the standalone
    ``stream(stateful_polling=True)`` long-running daemon — with its own
    internal ``refresh_interval`` / ``export_interval`` — call that verb
    directly (``cls.stream(stateful_polling=True, ...)``) instead of
    wrapping it in a Watershed.
    """

    incorp_params: Dict[str, Any] = Field(default_factory=dict)
    refresh_params: Optional[Dict[str, Any]] = None
    export_params: Optional[Dict[str, Any]] = None

    @model_validator(mode="before")
    @classmethod
    def _reject_stateful_polling(cls, data: Any) -> Any:
        """Reject ``stateful_polling=True`` with concrete alternatives.

        ``stream()``'s stateful-daemon mode runs indefinitely with its own
        internal ``refresh_interval`` / ``export_interval``, which conflicts
        with Tideweaver's per-interval tick model.  Two alternatives,
        depending on intent:

        * If you want a **long-running stateful daemon**, drop Tideweaver
          for that source and call ``cls.stream(stateful_polling=True, ...)``
          directly — that's the standalone daemon verb.
        * If you want **fan-in inside a Watershed** that accumulates upstream
          state across ticks, leave this ``Stream(stateful_polling=False)``
          (the upstream registry persists via ``_tideweaver_snapshot``) and
          add a :class:`Fjord` current at the tail to join it each tick.
        """
        if isinstance(data, dict) and "stateful_polling" in data:
            raise ValueError(
                "Stream(stateful_polling=...) is not supported inside a Watershed — "
                "Tideweaver's 'interval' IS the polling cadence and stream() is always "
                "called in chunking mode here.  Two alternatives: "
                "(a) for a long-running stateful daemon, call cls.stream(stateful_polling=True) "
                "directly outside any Watershed; "
                "(b) for fan-in across upstream Stream registries, leave this Stream as-is and "
                "add a Fjord current at the tail — upstream Stream snapshots persist between "
                "ticks for the flush to read."
            )
        return data


class Fjord(Current):
    """A per-tick "fjord flush": snapshot upstream + ``outflow(state)`` + export.

    The flush does NOT call ``cls.fjord()`` (which is a long-running daemon).
    Each tick snapshots the upstream :class:`Current` classes' registries
    (``cls.inc_dict``), invokes the user-supplied ``outflow(state)`` function
    loaded from the resolved outflow sidecar, builds (or looks up) a dynamic
    output class, and exports.

    The outflow sidecar is resolved per-current first, then per-watershed,
    then erroring if neither is set.

    Attributes:
        export_params: Forwarded to :meth:`Incorporator.export`.  May be a
            single-output dict (``{"file_path": "..."}``) or a multi-output
            dict keyed by derived class name (matching ``fjord()``'s shape).
    """

    export_params: Dict[str, Any] = Field(default_factory=dict)


class Export(Current):
    """A per-tick :meth:`Incorporator.export` call.

    The simplest verb — useful for periodic snapshots of an existing
    Incorporator subclass's registry (populated by an upstream
    :class:`Stream` or :class:`Fjord`).
    """

    export_params: Dict[str, Any] = Field(default_factory=dict)
