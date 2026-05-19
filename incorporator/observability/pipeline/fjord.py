"""Fjord engine (Engine 3): multi-source stateful streaming with combined outflow."""

import asyncio
import inspect
import logging
import time
from types import ModuleType
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Union, cast

from ..logger import Wave
from ._daemons import _export_daemon, _refresh_daemon
from ._outflow import _outflow_daemon
from ._shared import _row_count

logger = logging.getLogger(__name__)


async def _maybe_await(fn: Callable[[Any], Any], state: Dict[str, Any]) -> Any:
    """Call ``fn(state)``; await the result if ``fn`` is a coroutine function.

    Lets users define ``inflow(state)`` as either sync or async.  Pays
    one ``iscoroutinefunction`` check per call — negligible against the
    network I/O the inflow function typically gates.
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(state)
    return fn(state)


def _has_any_depends_on(stream_params: List[Dict[str, Any]]) -> bool:
    """True when at least one source entry declares ``depends_on``.

    Acts as the opt-in switch for the tiered-seed path: when ``False``, the
    engine uses the legacy declaration-order sequential seed (bit-identical
    to pre-feature behaviour).  When ``True``, the engine validates the
    graph and runs entries in topo tiers — within-tier parallel, between-
    tier sequential so each tier sees prior tiers' state.
    """
    return any(entry.get("depends_on") for entry in stream_params)


def _validate_depends_on(stream_params: List[Dict[str, Any]]) -> None:
    """Validate that every ``depends_on`` name resolves to a peer in the seed.

    Raises ``ValueError`` with a clear message if a name doesn't match any
    peer entry's ``cls.__name__`` — fails fast on typos rather than later at
    ``state[...]`` KeyError time, which is harder to debug.
    """
    peer_names = {entry["cls"].__name__ for entry in stream_params}
    for entry in stream_params:
        deps = entry.get("depends_on") or []
        for dep in deps:
            if dep not in peer_names:
                raise ValueError(
                    f"depends_on references unknown peer class {dep!r}; available peers: {sorted(peer_names)}"
                )


def _tiered_seed_order(
    stream_params: List[Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    """Topo-sort entries into tiers; entries in a tier have no inter-tier deps.

    Tier 0 contains all entries with no ``depends_on`` (or an empty list).
    Tier N contains all entries whose declared dependencies are fully
    satisfied by entries in tiers 0..N-1.  Within-tier order matches
    ``stream_params`` declaration order so the gather() call returns
    results in a stable shape.

    Raises ``ValueError`` on a cycle (no entry's deps can be satisfied
    by the resolved set, but unresolved entries remain).
    """
    unresolved = list(stream_params)
    resolved_names: set[str] = set()
    tiers: List[List[Dict[str, Any]]] = []

    while unresolved:
        ready = [e for e in unresolved if all(d in resolved_names for d in (e.get("depends_on") or []))]
        if not ready:
            unresolved_names = [e["cls"].__name__ for e in unresolved]
            raise ValueError(f"depends_on cycle detected among: {unresolved_names}")
        # Preserve declaration order within the tier.
        ready_ids = {id(e) for e in ready}
        ready_in_order = [e for e in stream_params if id(e) in ready_ids]
        tiers.append(ready_in_order)
        for e in ready_in_order:
            resolved_names.add(e["cls"].__name__)
            unresolved.remove(e)
    return tiers


def _format_seed_error(cls_name: str, exc: Exception, inflow_active: bool) -> str:
    """Build the ``failed_sources`` entry for a seed exception.

    ``KeyError`` raised under an active ``inflow_callable`` is almost always the
    ``state[ClassName]`` lookup failing — message it that way so the user
    knows to either guard ``inflow(state)`` against missing peers or declare
    a ``depends_on`` edge so the prerequisite seeds first.  Everything else
    falls back to a typed-message format that still names the source class
    (raw ``str(exc)`` alone often gives just the missing key in quotes —
    ``"'Track'"`` — useless without context).
    """
    exc_type = type(exc).__name__
    if inflow_active and isinstance(exc, KeyError) and exc.args:
        missing = exc.args[0]
        return (
            f"inflow(state) for source {cls_name!r} raised KeyError on missing peer "
            f"{missing!r} — guard inflow(state) against missing keys "
            f"(e.g. state.get({missing!r}) or add depends_on={[missing]!r} to enforce ordering)"
        )
    return f"Seed Error in source {cls_name!r}: {exc_type}: {exc}"


def _resolve_seed_order(
    stream_params: List[Dict[str, Any]],
    refresh_interval: Union[float, Dict[Any, float], None],
) -> List[Dict[str, Any]]:
    """Return ``stream_params`` re-ordered for the sequential-seed phase.

    Order resolution (matches the user's "DX knows the best order"):
      1. When ``refresh_interval`` is a ``dict[ClassName | Class, float]``,
         that dict's key insertion order drives the seed sequence.  Any
         stream_params entry whose class isn't keyed in the dict is
         appended at the end in declaration order (defensive — don't
         drop sources just because their interval was omitted).
      2. Otherwise (scalar or None), fall back to ``stream_params``
         declaration order — what the user wrote in the JSON list.
    """
    if not isinstance(refresh_interval, dict):
        return list(stream_params)

    def _entry_keys(entry: Dict[str, Any]) -> List[Any]:
        cls = entry.get("cls")
        return [cls, getattr(cls, "__name__", None)]

    ordered: List[Dict[str, Any]] = []
    remaining = list(stream_params)
    for key in refresh_interval.keys():
        for entry in remaining:
            if key in _entry_keys(entry):
                ordered.append(entry)
                remaining.remove(entry)
                break
    ordered.extend(remaining)  # append anything not keyed in the dict
    return ordered


async def _seed_one_source(
    entry: Dict[str, Any],
    state: Dict[str, Any],
    inflow_callable: Optional[Callable[[Any], Any]],
) -> Any:
    """Run a single source's ``incorp()`` with optional state-aware inflow overrides.

    Calls ``inflow(state)`` first (if defined) and merges any
    ``{ClassName: {conv_dict: {...}}}`` override into the source's
    ``incorp_params``.  Inflow-returned conv_dict values WIN on
    conflicting keys (the user's framing: inflow is the state-aware
    override; stream_params is the static baseline).
    """
    cls = entry["cls"]
    base_params: Dict[str, Any] = dict(entry["incorp_params"])

    if inflow_callable is not None:
        overrides = await _maybe_await(inflow_callable, state)
        if not isinstance(overrides, dict):
            raise TypeError(f"inflow(state) must return a dict[ClassName, dict], got {type(overrides).__name__}")
        extra = overrides.get(cls.__name__, {})
        if not isinstance(extra, dict):
            raise TypeError(
                f"inflow(state)[{cls.__name__!r}] must be a dict (got {type(extra).__name__}); "
                f"expected shape {{'conv_dict': {{...}}}}"
            )
        extra_conv = extra.get("conv_dict")
        if extra_conv is not None:
            if not isinstance(extra_conv, dict):
                raise TypeError(
                    f"inflow(state)[{cls.__name__!r}]['conv_dict'] must be a dict, got {type(extra_conv).__name__}"
                )
            merged_conv = {**base_params.get("conv_dict", {}), **extra_conv}
            base_params["conv_dict"] = merged_conv

    return await cls.incorp(**base_params)


def _resolve_per_source_interval(
    top_level: Union[float, Dict[Any, float], None],
    entry: Dict[str, Any],
    key: str,
) -> Optional[float]:
    """Pick the interval value for one fjord stream entry.

    Priority chain:
      1. Per-entry override — ``entry[key]`` if explicitly set.
      2. Top-level dict — ``top_level[class_name]`` or ``top_level[cls]``
         when the top-level kwarg is a dict keyed by class name (string,
         JSON-compatible) or class object (Python-ergonomic).
      3. Top-level scalar — used as the default for every source.
      4. ``None`` — when nothing matches.  The pipeline-level cascade in
         ``observability/pipeline/__init__.py`` then applies the
         framework default (60 s for refresh, 300 s for export).
    """
    if key in entry:
        # ``entry`` is typed ``Dict[str, Any]`` so the lookup widens to
        # ``Any``; narrow back to the function's declared return type so
        # mypy strict mode stays happy.
        return cast(Optional[float], entry[key])
    if isinstance(top_level, dict):
        cls = entry.get("cls")
        if cls is not None and cls in top_level:
            return top_level[cls]
        cls_name = getattr(cls, "__name__", None)
        if cls_name is not None and cls_name in top_level:
            return top_level[cls_name]
        return None
    return top_level


async def _run_fjord_engine(
    output_class_name: str,
    base_class: Any,
    stream_params: List[Dict[str, Any]],
    outflow_fn: Any,
    export_params: Dict[str, Any],
    r_interval: Optional[float],
    e_interval: Optional[float],
    outflow_module: Optional[ModuleType] = None,
    inflow_callable: Optional[Callable[[Any], Any]] = None,
) -> AsyncGenerator[Wave, None]:
    """Multi-source stateful streaming engine for ``Incorporator.fjord()``.

    Generalisation of ``_run_stateful_engine`` to N sources with one
    outflow-and-export daemon. The output class is built dynamically by
    ``_outflow_daemon`` on the first non-empty tick — this engine just
    plumbs the name + base class through.

    Lifecycle:
      1. Seed phase:
         * **No inflow callable** → concurrent ``entry["cls"].incorp(**...)``
           across all entries via ``asyncio.gather`` (today's behaviour;
           zero added latency).
         * **inflow callable present** → sequential seed in the order
           established by ``refresh_interval`` (when dict) or
           ``stream_params`` declaration order.  Before each source's
           ``incorp()`` the engine calls ``inflow(state)`` with the
           cumulative state-so-far and merges any returned ``conv_dict``
           overrides into that source's ``incorp_params`` — this is how
           ``link_to(state["Planet"], …)`` gets a live registry handle.
         One ``incorp`` wave yielded per source.
      2. Daemon phase: per-source refresh daemons (always), per-source export
         daemons (when ``export_params`` is set on the entry), and one outflow
         daemon. All coordinate via a single shared ``asyncio.Lock``.
      3. Shutdown: ``shutdown_event.set()`` → cancel tasks → drain queue → exit.

    ``inflow_callable`` and ``outflow_module`` are optional kwargs that
    default to ``None`` so existing callers keep working unchanged.
    """
    # ------------------------------------------------------------------
    # 1. Seed phase
    # ------------------------------------------------------------------
    source_classes: List[Any] = [entry["cls"] for entry in stream_params]
    source_refs: List[List[Any]] = [[None] for _ in stream_params]
    seed_order = _resolve_seed_order(stream_params, r_interval)

    seed_start = time.perf_counter()
    if inflow_callable is None:
        # Today's parallel seed — pure async, no ordering, no latency cost.
        seed_results = await asyncio.gather(
            *[entry["cls"].incorp(**entry["incorp_params"]) for entry in stream_params],
            return_exceptions=True,
        )
        results_by_idx: Dict[int, Any] = dict(enumerate(seed_results))
    elif _has_any_depends_on(stream_params):
        # Opt-in tiered seed: within-tier parallel, between-tier sequential.
        # Entries without depends_on (or whose deps are satisfied) run together
        # via gather; tier N waits for tiers 0..N-1 to finish so state[...] is
        # populated for each tier's inflow(state) call.  Failures are surfaced
        # via return_exceptions and stored in results_by_idx for the wave loop
        # below to translate into per-source failure waves.
        _validate_depends_on(stream_params)
        state: Dict[str, Any] = {}
        results_by_idx = dict.fromkeys(range(len(stream_params)))
        idx_by_id = {id(entry): i for i, entry in enumerate(stream_params)}
        for tier in _tiered_seed_order(stream_params):
            tier_results = await asyncio.gather(
                *[_seed_one_source(entry, state, inflow_callable) for entry in tier],
                return_exceptions=True,
            )
            for entry, result in zip(tier, tier_results):
                orig_idx = idx_by_id[id(entry)]
                results_by_idx[orig_idx] = result
                if not isinstance(result, Exception):
                    state[entry["cls"].__name__] = result
    else:
        # Legacy sequential seed in resolved order so each source can
        # ``link_to`` its predecessors via ``state[...]``.  Bit-identical to
        # pre-feature behaviour: no depends_on declared anywhere → fall
        # through here, no parallelism, no semantic change for callers.
        state = {}
        results_by_idx = dict.fromkeys(range(len(stream_params)))
        idx_by_id = {id(entry): i for i, entry in enumerate(stream_params)}
        for entry in seed_order:
            idx = idx_by_id[id(entry)]
            try:
                result = await _seed_one_source(entry, state, inflow_callable)
                results_by_idx[idx] = result
                state[entry["cls"].__name__] = result
            except Exception as exc:
                results_by_idx[idx] = exc
                # Don't break — co-equal peers later in seed_order may not
                # depend on this source's state.  Their ``inflow(state)``
                # call will simply KeyError if they DO depend on it, which
                # surfaces as a clean per-source failure wave.
    seed_elapsed = time.perf_counter() - seed_start

    # Validate seed phase — every source must have produced data.
    for original_idx, entry in enumerate(stream_params):
        cls_name = entry["cls"].__name__
        result = results_by_idx[original_idx]
        if isinstance(result, Exception):
            yield Wave(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[_format_seed_error(cls_name, result, inflow_callable is not None)],
                processing_time_sec=seed_elapsed,
            )
            return
        if not result:
            yield Wave(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[f"Initial incorp() for {cls_name} yielded no data"],
                processing_time_sec=seed_elapsed,
            )
            return
        source_refs[original_idx][0] = result
        yield Wave(
            chunk_index=1,
            operation=f"fjord_incorp:{cls_name}",
            rows_processed=_row_count(result),
            processing_time_sec=seed_elapsed,
        )

    # ------------------------------------------------------------------
    # 2. Daemon phase — spawn refresh + per-stream export + outflow tasks.
    # ------------------------------------------------------------------
    lock = asyncio.Lock()
    wave_queue: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    tasks: List[asyncio.Task[Any]] = []

    for idx, entry in enumerate(stream_params):
        entry_cls = entry["cls"]
        # Refresh defaults to ON.  Pass "refresh_params": {} for explicit
        # default kwargs, or "refresh_params": None to opt OUT of refresh
        # for this specific source.  Missing key = default-on with {}.
        refresh_params = entry.get("refresh_params", {})
        stream_export_params = entry.get("export_params")
        # Per-entry interval overrides fall back to the top-level interval.
        # Top-level can be a scalar (applies to all sources) or a dict
        # keyed by class name / class object (per-source override).  When
        # the entire cascade returns None, fall back to the module-level
        # default so a daemon spawned with no intervals still ticks.
        from . import DEFAULT_EXPORT_INTERVAL_SEC, DEFAULT_REFRESH_INTERVAL_SEC

        entry_r_interval = (
            _resolve_per_source_interval(r_interval, entry, "refresh_interval") or DEFAULT_REFRESH_INTERVAL_SEC
        )
        entry_e_interval = (
            _resolve_per_source_interval(e_interval, entry, "export_interval") or DEFAULT_EXPORT_INTERVAL_SEC
        )

        if refresh_params is not None:
            # Note on refresh + inflow(state): refresh re-uses the original
            # ``conv_dict`` (with its captured ``link_to(state["X"], …)``
            # closures).  Because ``link_to`` holds a reference to the live
            # ``IncorporatorList`` object, in-place mutations on the peer's
            # registry are visible automatically — no per-tick re-resolution
            # needed for the common refresh-by-mutate case.  Pipelines that
            # actually swap the peer's IncorporatorList wholesale on refresh
            # are an edge case; document on the inflow.py docstring rather
            # than rebuilding state every refresh tick.
            tasks.append(
                asyncio.create_task(
                    _refresh_daemon(
                        cls=entry_cls,
                        dataset_ref=source_refs[idx],
                        refresh_params=refresh_params,
                        lock=lock,
                        wave_queue=wave_queue,
                        shutdown_event=shutdown_event,
                        r_interval=entry_r_interval,
                        operation_label=f"fjord_refresh:{entry_cls.__name__}",
                    )
                )
            )

        if stream_export_params is not None:
            tasks.append(
                asyncio.create_task(
                    _export_daemon(
                        cls=entry_cls,
                        dataset_ref=source_refs[idx],
                        export_params=stream_export_params,
                        lock=lock,
                        wave_queue=wave_queue,
                        shutdown_event=shutdown_event,
                        e_interval=entry_e_interval,
                        operation_label=f"export:{entry_cls.__name__}",
                    )
                )
            )

    # Always spawn the outflow daemon — it's the whole point of fjord.
    # Pass the outflow MODULE so the daemon can probe it for user-pre-declared
    # Incorporator subclasses matching the keys returned by ``outflow(state)``.
    tasks.append(
        asyncio.create_task(
            _outflow_daemon(
                output_class_name=output_class_name,
                base_class=base_class,
                source_refs=source_refs,
                source_classes=source_classes,
                outflow_fn=outflow_fn,
                export_params=export_params,
                lock=lock,
                wave_queue=wave_queue,
                shutdown_event=shutdown_event,
                e_interval=e_interval,
                outflow_module=outflow_module,
            )
        )
    )

    async def _waiter() -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
        await wave_queue.put(None)

    waiter_task = asyncio.create_task(_waiter())

    try:
        while True:
            wave = await wave_queue.get()
            if wave is None:
                break
            yield wave
    finally:
        shutdown_event.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        try:
            await waiter_task
        except asyncio.CancelledError:
            pass  # Expected during shutdown
        except Exception as exc:
            logger.warning("Fjord drain raised during shutdown: %s", exc)
