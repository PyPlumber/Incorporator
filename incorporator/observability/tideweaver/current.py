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

from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...base import Incorporator

OnErrorPolicy = Literal["restart", "isolate", "fail_watershed"]


class Current(BaseModel):
    """One node in a :class:`Watershed` graph.

    Most users reach for the verb-typed subclasses :class:`Stream`,
    :class:`Fjord`, or :class:`Export` — they carry the kwargs each tick
    action needs and give callers good mypy ergonomics. The bare
    ``Current(...)`` constructor stays available as the escape hatch for
    tests or unusual integrations that need to drive their own tick body.

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
        phase_offset_sec: Delay this current's FIRST tick by this many
            seconds after the run starts.  Green-wave coordination: by
            offsetting downstream's first tick to land just after upstream's
            expected wave, fewer ``"awaiting_upstream"`` gating skips fire
            on the warm-up pass.  Default ``0.0`` — first tick fires on
            pass 1 with no delay.
        inflow: Optional sidecar ``.py`` path (per-current override of the
            watershed-level default).
        outflow: Optional sidecar ``.py`` path (per-current override of the
            watershed-level default).

    Note: the old ``skip_threshold`` field moved to per-edge
    :class:`~.flow.SurgeBarrier`.  Edge-level placement matches the
    architectural reality (one dependent can declare different surge
    tolerances per upstream); the canonical default (``2.0``, action
    ``"skip"``) lives on :class:`~.flow.SurgeBarrier` itself.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    name: str
    cls: type[Incorporator]
    interval: float = Field(..., gt=0.0, description="Seconds between ticks; must be positive.")
    depends_on: list[str] = Field(default_factory=list)
    on_error: OnErrorPolicy = "restart"
    phase_offset_sec: float = Field(0.0, ge=0.0, description="Delay first tick by N seconds for green-wave alignment.")
    inflow: Path | None = None
    outflow: Path | None = None


class Stream(Current):
    """One source, pulled fresh on every tick of a Watershed window.

    Use a ``Stream`` current when you want a single :meth:`incorp` call
    to run on a steady cadence inside a Watershed — the equivalent of
    ``cls.stream()`` as a single node in an orchestrated graph. Each
    tick fires a chunking-mode :meth:`Incorporator.stream` drain against
    the source until exhausted, then exits; the watershed's
    ``interval`` IS the polling cadence between drains. The class
    registry persists between ticks (via a ``_tideweaver_snapshot``
    strong-ref the scheduler parks on the class), so a downstream
    :class:`Fjord` current sees accumulated upstream state on each
    flush.

    Example — a head Stream in a diamond watershed pulling Binance
    top-of-book every 15 seconds::

        head = Stream(
            name="binance",
            cls=BinanceBook,
            interval=15,
            incorp_params={
                "inc_url": "https://api.binance.us/api/v3/ticker/bookTicker",
                "inc_code": "symbol",
            },
        )

    If you instead want a long-running stateful daemon — with its own
    internal ``refresh_interval`` / ``export_interval`` — call
    ``cls.stream(stateful_polling=True, ...)`` directly outside any
    Watershed. ``Stream(stateful_polling=True)`` is rejected at
    construction time precisely because it conflicts with the
    watershed's tick model; see :meth:`_reject_stateful_polling` for
    the two intent-aware alternatives.

    Attributes:
        incorp_params: Forwarded to :meth:`Incorporator.stream` as the
            ``incorp_params`` dict (``inc_url``, ``inc_code``, headers,
            params, paginator, etc.).
        refresh_params: Optional override of refresh-time kwargs. Rarely
            needed inside a Watershed since Tideweaver controls cadence.
        export_params: Optional per-tick export target. Most pipelines
            leave this empty and let a downstream :class:`Fjord` or
            :class:`Export` current handle persistence.
    """

    incorp_params: dict[str, Any] = Field(default_factory=dict)
    refresh_params: dict[str, Any] | None = None
    export_params: dict[str, Any] | None = None

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
    """Fuse one or more upstream currents into a composite output every tick.

    Use a ``Fjord`` current as the tail of a multi-source Watershed
    shape (``diamond``, ``fanout-into-flush``, custom edges) when you
    need to join live data from upstream :class:`Stream` or
    :class:`Fjord` currents into a single derived dataset on a steady
    cadence — the live mark-to-market dashboard, the fantasy-NASCAR
    Sunday fusion, the cross-exchange arbitrage spread.

    Example — diamond tail joining three exchange Streams into a
    best-market record every 30 seconds, exporting to NDJSON::

        tail = Fjord(
            name="best_market",
            cls=BestMarket,
            interval=30,
            export_params={"file_path": "arb_signals.ndjson"},
        )
        watershed = Watershed.diamond(
            window=(start, end),
            head=binance_stream,
            middle=[coinbase_stream, kraken_stream],
            tail=tail,
            outflow="arb_outflow.py",
        )

    On every tick the current snapshots the upstream classes'
    registries (``cls.inc_dict``), invokes the user-supplied
    ``outflow(state)`` from the resolved outflow sidecar, materialises
    the returned rows into a dynamic output class, and exports.  The
    outflow sidecar is resolved per-current first, then per-watershed,
    then errors if neither is set.

    This is the per-tick *flush* primitive — it does NOT call
    ``cls.fjord()`` (which is a long-running daemon, ill-suited to
    windowed orchestration).  For multi-source live streaming OUTSIDE a
    Watershed, call :meth:`Incorporator.fjord` directly.

    Attributes:
        export_params: Forwarded to :meth:`Incorporator.export`. Pass a
            single-output dict (``{"file_path": "..."}``) for the common
            case, or a multi-output dict keyed by derived class name
            (matching :meth:`Incorporator.fjord`'s shape) when the
            ``outflow(state)`` function returns multiple class rosters.
    """

    export_params: dict[str, Any] = Field(default_factory=dict)


class Export(Current):
    """Periodically snapshot an existing Incorporator subclass's registry to disk.

    Use an ``Export`` current when you want to persist an upstream's
    data on a different cadence than the upstream produces it — for
    example, capture a Parquet snapshot at the close of a Watershed
    window while an upstream :class:`Stream` keeps refreshing every 30
    seconds. The current calls :meth:`Incorporator.export` against the
    referenced class's ``inc_dict`` registry.

    Example — daily Parquet snapshot of a Stream-fed registry at
    midnight::

        snapshot = Export(
            name="daily_parquet",
            cls=BinanceBook,
            interval=86400,
            depends_on=["binance"],
            export_params={"file_path": "binance_daily.parquet"},
        )

    The simplest of the three verb-typed Currents — no outflow sidecar
    needed, no upstream snapshotting, just one ``export()`` call per
    tick.

    Attributes:
        export_params: Forwarded to :meth:`Incorporator.export`. The
            ``file_path`` extension picks the format
            (``.parquet`` / ``.ndjson`` / ``.csv`` / ``.sqlite`` / etc.).
    """

    export_params: dict[str, Any] = Field(default_factory=dict)


class CustomCurrent(Current):
    """Escape-hatch Current for users with a non-verb-typed tick body.

    Subclass and implement ``async tick(self, scheduler)`` to run your
    own per-tick logic.  The scheduler calls ``current.tick(scheduler)``
    directly, bypassing the :class:`Stream` / :class:`Fjord` /
    :class:`Export` dispatch.

    Example — periodic health-check ping that doesn't fit the
    standard verbs::

        class HealthcheckPing(CustomCurrent):
            async def tick(self, scheduler):
                response = await httpx.get("https://internal.acme/health")
                if response.status_code != 200:
                    raise RuntimeError(f"health check failed: {response.status_code}")

        watershed = Watershed.parallel(
            window=(start, end),
            currents=[
                HealthcheckPing(name="health", cls=PingResult, interval=30),
            ],
        )

    Before :class:`CustomCurrent`, users wanting custom tick bodies
    reached for ``Tideweaver(..., tick_factory=...)`` — that hook is
    still available but lives outside the per-current type, making the
    plan less self-describing.  ``tick_factory`` is now the test-only
    override.

    Subclasses MUST override :meth:`tick`; the base raises
    ``NotImplementedError`` at call time.

    **Immutability contract.**  ``tick()`` must NOT register new
    :class:`Current`\\s or :class:`Edge`\\s, nor mutate
    ``scheduler.watershed.currents`` / ``scheduler.watershed.edges``,
    after :meth:`Tideweaver.run` has started.  The scheduler memoises
    transitive-upstream lookups once per instance for O(1) gate
    evaluation; runtime topology mutations would silently invalidate
    that cache and produce wrong gating decisions.  If you need to
    add a current mid-run, stop the current watershed and start a new
    :class:`Tideweaver` instance.
    """

    auto_park_snapshot: ClassVar[bool] = True

    async def _run_tick(self, scheduler: Any) -> None:
        """Wrap :meth:`tick` with automatic snapshot parking.

        After :meth:`tick` returns, if ``auto_park_snapshot`` is ``True`` and
        the user's ``tick()`` body did not manually assign
        ``cls._tideweaver_snapshot`` (identity check on the pre-tick sentinel),
        the scheduler parks ``list(cls.inc_dict.values())`` as the snapshot.
        A manual assignment inside ``tick()`` produces a new list object,
        so ``is pre`` is ``False`` and auto-park is skipped.

        Args:
            scheduler: The :class:`~.scheduler.Tideweaver` instance driving
                this current.
        """
        pre = getattr(self.cls, "_tideweaver_snapshot", None)
        await self.tick(scheduler)
        if self.auto_park_snapshot and getattr(self.cls, "_tideweaver_snapshot", None) is pre:
            cast(Any, self.cls)._tideweaver_snapshot = list(self.cls.inc_dict.values())

    async def tick(self, scheduler: Any) -> None:
        """Per-tick body.  Subclasses MUST override this method."""
        raise NotImplementedError(
            f"{type(self).__name__} must override async tick(self, scheduler). "
            f"Subclass CustomCurrent and implement the tick coroutine, "
            f"or use Stream/Fjord/Export for the standard verb tick bodies."
        )


class FilteredDrillCurrent(CustomCurrent):
    """Declarative T5-drill-with-filter primitive.

    Filters an upstream class's parked ``_tideweaver_snapshot`` by a
    predicate, then fan-outs a ``cls.incorp(inc_parent=filtered, ...)``
    call.  ``CustomCurrent._run_tick`` auto-parks the downstream
    ``_tideweaver_snapshot`` automatically — no manual park required.

    Predicate forms:

    - **Callable**: ``lambda row: row.division_id == 201``.  Receives the
      row object; returns ``bool``.
    - **Structured tuple**: ``(attr_name, op, value)``, e.g.
      ``("division_id", operator.eq, 201)``.  Reads ``getattr(row, attr,
      None)`` then calls ``op(value_or_none, value)``.

    Null safety of structured-tuple form: ``operator.eq(None, x)`` and
    ``operator.ne(None, x)`` are safe — they return ``False`` / ``True``
    respectively.  **Ordered operators** (``operator.lt``, ``operator.le``,
    ``operator.gt``, ``operator.ge``) raise ``TypeError`` when one operand
    is ``None`` — this is intentional fail-fast behavior; do not wrap in
    try/except.  Use the callable form if you need null-safe ordered
    comparisons.

    First-tick / no-upstream-data: if the parent's
    ``_tideweaver_snapshot`` is ``None`` (Tideweaver hasn't reached the
    upstream yet) or ``[]``, ``tick()`` silently returns.

    Empty filtered list: if the predicate rejects every row, ``tick()``
    returns without making an ``incorp()`` call (no wire traffic).

    Example::

        FilteredDrillCurrent(
            name="hitting_drill",
            cls=MLBHitting,
            interval=30.0,
            parent=MLBAllTeam,
            predicate=("division_id", operator.eq, 201),
            inc_url="https://statsapi.mlb.com/api/v1/teams/{}/stats?...",
            inc_child="inc_code",
            rec_path="stats.0.splits.0",
            incorp_code="team.id",
            conv_dict=HITTING_CONV,
        )
    """

    parent: type[Incorporator]
    # Loose tuple arm lets Pydantic accept the 3-tuple regardless of element types;
    # _validate_predicate then enforces the callable constraint so the error message
    # is human-readable rather than Pydantic's generic union-failure text.
    predicate: Callable[[Any], bool] | tuple[str, Any, Any]
    inc_url: str
    inc_child: str = "inc_code"
    rec_path: str | None = None
    incorp_code: str | None = None
    conv_dict: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_predicate(self) -> FilteredDrillCurrent:
        """Validate the structured-tuple predicate form at construction time."""
        if isinstance(self.predicate, tuple):
            if len(self.predicate) != 3 or not isinstance(self.predicate[0], str) or not callable(self.predicate[1]):
                raise ValueError(
                    f"FilteredDrillCurrent predicate tuple must be "
                    f"(attr: str, op: Callable, value: Any); got {self.predicate!r}"
                )
        return self

    async def tick(self, scheduler: Any) -> None:
        """Filter the parent snapshot and fan-out incorp() for matching rows.

        Args:
            scheduler: The :class:`~.scheduler.Tideweaver` instance driving
                this current (not used directly; present for interface
                compatibility).
        """
        pre_snap = getattr(self.parent, "_tideweaver_snapshot", None)
        if not pre_snap:
            return
        if callable(self.predicate):
            filtered = [row for row in pre_snap if self.predicate(row)]
        else:
            attr, op, value = self.predicate
            filtered = [row for row in pre_snap if op(getattr(row, attr, None), value)]
        if not filtered:
            return
        await self.cls.incorp(
            # cast: filtered is list[Incorporator] at runtime; IncorporatorList is list[T] subclass
            inc_parent=cast(Any, filtered),
            inc_child=self.inc_child,
            inc_url=self.inc_url,
            inc_code=self.incorp_code,
            conv_dict=self.conv_dict,
            rec_path=self.rec_path,
        )
        # _run_tick auto-parks _tideweaver_snapshot via auto_park_snapshot=True.
