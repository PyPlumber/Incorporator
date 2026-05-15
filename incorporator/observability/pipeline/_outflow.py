"""Fjord-specific outflow daemon: snapshot sources, run user fn, export combined output(s).

``outflow(state)`` may return either ``list[dict]`` (single derived class) or
``dict[ClassName, list[dict]]`` (one derived class per key, one export file per
class).  Detection is by ``isinstance(result, dict)`` — list returns take the
single-output path.
"""

import asyncio
import logging
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, cast

from ..logger import Wave  # re-export for callers
from ._shared import _daemon_tick, _interruptible_sleep, _resolve_if_exists_for_export

__all__ = ["Wave", "_outflow_daemon"]

logger = logging.getLogger(__name__)


def _normalise_outflow_return(
    result: Any,
    default_class_name: str,
) -> Tuple[Dict[str, List[Dict[str, Any]]], bool]:
    """Coerce any ``outflow(state)`` return shape into ``{class_name: rows}``.

    Returns ``(grouped, is_multi_output)`` so the daemon can decide whether
    to look up per-class ``export_params`` or fall back to the flat
    single-output config.

    Shapes:
      * ``list[dict]`` → ``{default_class_name: list}`` (legacy single-output)
      * ``dict`` of ``str → list[dict]`` → multi-output (Phase 10 Design B)
      * ``dict`` of ``str → str`` (NOT list-valued) → ambiguous; treat as a
        single legacy row (wrap in list).  Backwards-compat for users who
        previously returned a single dict per tick.
      * ``None`` / falsy → empty single-output (zero-row tick).
    """
    if result is None:
        return {default_class_name: []}, False
    if isinstance(result, list):
        return {default_class_name: result}, False
    if isinstance(result, dict):
        # Empty dict in multi-output flavor → no derived classes, no waves.
        if not result:
            return {}, True
        # Heuristic: if EVERY value is a list, it's the multi-output shape.
        # Otherwise it's a single-row dict (current single-output behaviour).
        if all(isinstance(v, list) for v in result.values()):
            # Type-narrow each value to list[dict] (defensive cast for mypy).
            return {k: list(v) for k, v in result.items()}, True
        return {default_class_name: [result]}, False
    # Anything else — fall back to wrapping in a list to match the legacy
    # "auto-coerce to list[dict]" contract.
    return {default_class_name: list(result)}, False


def _resolve_export_params_for(
    derived_name: str,
    export_params: Dict[str, Any],
    is_multi_output: bool,
) -> Dict[str, Any]:
    """Pick the right ``export_params`` slice for one derived class.

    Detection rule (matches the user's pick — "detect by `file_path` key"):
      * If the top-level ``export_params`` has a ``file_path`` key,
        treat the whole dict as a single-output config.
      * Otherwise (multi-output shape), look up by class name.  Missing
        keys return an empty dict — the daemon will warn-and-skip (B5).
    """
    if "file_path" in export_params:
        # Single-output config — every derived class would write to the
        # same file, which is almost never what the user wants when
        # they returned a multi-output dict.  Log a warning but keep
        # writing — fail-safe over fail-hard.
        if is_multi_output:
            logger.warning(
                "outflow(state) returned multi-output dict but export_params "
                "is single-output shape (has top-level 'file_path'). "
                "All derived classes will write to the same file."
            )
        return export_params
    return cast(Dict[str, Any], export_params.get(derived_name, {}))


async def _outflow_daemon(
    output_class_name: str,
    base_class: Any,
    source_refs: List[List[Any]],
    source_classes: List[Any],
    outflow_fn: Any,
    export_params: Dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: "asyncio.Queue[Optional[Wave]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
    outflow_module: Optional[ModuleType] = None,
) -> None:
    """Periodic outflow-and-export daemon for the fjord engine.

    On every tick (two-phase, multi-output aware):

    **Phase 1 — snapshot + invoke user outflow_fn.**
      1. Snapshot each ``source_refs[i][0]`` under ``lock`` into a state
         dict keyed by ``source_classes[i].__name__`` (O(N) pointer
         copies, not deep copies — release the lock fast).
      2. Outside the lock, invoke ``outflow_fn(state)`` via
         ``asyncio.to_thread`` so CPU-heavy joins don't block sibling
         daemons.  An exception here enqueues one composite
         ``outflow:<output_class_name>`` failure wave; per-class waves
         are skipped (we can't attribute rows to classes that never
         ran).
      3. ``_normalise_outflow_return`` coerces the result to
         ``{class_name: rows}``:
           * ``list[dict]``  → single-output (legacy single class)
           * ``dict[str, list[dict]]`` → multi-output (one class per key,
             one file per key)
           * ``{}``          → no-op tick (zero waves)

    **Phase 2 — per-derived-class build + export.**  For every
    ``(derived_name, rows)`` pair the engine:
      1. Wraps the block in its own ``_daemon_tick`` so a failure in
         one derived class doesn't block the others — each gets its own
         success-or-failure ``Wave`` tagged ``outflow:<derived_name>``.
      2. Prefers a user-pre-declared ``Incorporator`` subclass on
         ``outflow_module`` when one exists with the matching name (gives
         DX full type control); otherwise builds the class via
         ``infer_dynamic_schema(derived_name, rows, base_class)``.  The
         schema registry is keyed by
         ``(name, frozenset(field_keys), id(base))`` so successive
         same-shape ticks reuse the cached class object.
      3. Clears the class's ``inc_dict``, materialises one instance
         per row (Pydantic ``model_post_init`` auto-registers), stashes
         a strong-ref ``_fjord_snapshot`` to defeat the
         WeakValueDictionary GC, and exports via the matching
         per-class ``export_params`` slice.

    Per-class export config lookup:
      * Top-level ``file_path`` in ``export_params``  →  single-output
        (every derived class writes to the same file; warning logged
        on multi-output mismatch).
      * ``export_params[derived_name]``  →  multi-output (recommended
        shape: ``{"JediArchive": {"file_path": "..."}, ...}``).
      * Missing key  →  warn-and-skip the export; the build count is
        still recorded on the Wave.

    Configured-but-unproduced classes are logged once per tick.

    Failures in any phase enqueue a Wave with ``failed_sources``
    populated but never crash the daemon.
    """
    # Local import keeps the observability layer free of a hard schema dep at
    # module-import time.
    from ...schema.builder import infer_dynamic_schema

    loop_idx = 0
    # Pre-compute state dict keys once — avoids re-allocating the key list on
    # every tick.  The key order is stable for the lifetime of the daemon.
    state_keys = [cls.__name__ for cls in source_classes]
    # Sentinel distinguishes "outflow_fn never produced a value" (raised, or
    # has not yet been invoked) from a legitimate ``None`` return.  ``None``
    # IS a valid outflow return (mapped to a zero-row tick by
    # ``_normalise_outflow_return``); the sentinel lets us tell those cases
    # apart without an extra flag variable.
    _OUTFLOW_NOT_RUN = object()

    while not shutdown_event.is_set():
        loop_idx += 1

        # ────────────────────────────────────────────────────────────
        # Phase 1: snapshot + run user outflow_fn.  On exception we emit
        # a single composite failure wave and skip Phase 2; on success
        # the per-class waves below carry the real signal.
        # ────────────────────────────────────────────────────────────
        result: Any = _OUTFLOW_NOT_RUN
        try:
            async with lock:
                state = dict(zip(state_keys, [ref[0] for ref in source_refs]))
            # asyncio.to_thread releases the GIL so CPU-heavy outflow functions
            # don't block refresh/export daemons running on other sources.
            result = await asyncio.to_thread(outflow_fn, state)
        except Exception as exc:
            # User outflow code raised — single composite failure wave keyed
            # by the default output class name (we don't know which derived
            # class would have produced what at this point).
            await wave_queue.put(
                Wave(
                    chunk_index=loop_idx,
                    operation=f"outflow:{output_class_name}",
                    rows_processed=0,
                    failed_sources=[f"Outflow Error: {exc}"],
                    processing_time_sec=0.0,
                )
            )

        if result is not _OUTFLOW_NOT_RUN:
            grouped, is_multi = _normalise_outflow_return(result, output_class_name)

            # ────────────────────────────────────────────────────────
            # Phase 2: per-derived-class build + export.
            #
            # Each (derived_name, rows) pair gets its own _daemon_tick
            # so a failure building Demographics doesn't prevent
            # JediArchive from exporting.  One Wave per derived class
            # per tick — matches the per-source Wave granularity.
            # ────────────────────────────────────────────────────────
            for derived_name, rows in grouped.items():
                row_count_holder: List[int] = [0]

                def _read_row_count(holder: List[int] = row_count_holder) -> int:
                    return holder[0]

                async with _daemon_tick(
                    wave_queue,
                    chunk_index=loop_idx,
                    operation=f"outflow:{derived_name}",
                    error_prefix="Outflow Error",
                    row_count_fn=_read_row_count,
                ):
                    if not rows:
                        # Zero-row branch — emit success wave with
                        # rows_processed=0; skip build/export.
                        continue

                    # Edge case B9: prefer a user-pre-declared Incorporator
                    # subclass with the matching name if outflow.py defined
                    # one.  Lets DX have full type control on derived classes.
                    user_cls = getattr(outflow_module, derived_name, None) if outflow_module is not None else None
                    if user_cls is not None and isinstance(user_cls, type) and issubclass(user_cls, base_class):
                        DerivedCls = cast(Any, user_cls)
                    else:
                        DerivedCls = cast(
                            Any,
                            infer_dynamic_schema(derived_name, rows, base_class),
                        )

                    # Reset registry for this tick's view, then materialise.
                    DerivedCls.inc_dict.clear()
                    instances = [DerivedCls(**row) for row in rows]

                    # Strong-ref snapshot keeps the WeakValueDictionary
                    # populated between ticks.  Phase 8 introduced this;
                    # preserve it per-class so each derived class survives
                    # independently.
                    DerivedCls._fjord_snapshot = instances

                    # Per-class export_params lookup + per-tick if_exists.
                    class_export = _resolve_export_params_for(derived_name, export_params, is_multi)
                    if not class_export:
                        # B5: outflow returned a class with no matching export
                        # config.  Skip the export but record the build count
                        # so the user sees what they produced.
                        logger.warning(
                            "outflow(state) emitted class %r but export_params has no matching key; skipping export.",
                            derived_name,
                        )
                        row_count_holder[0] = len(instances)
                        continue

                    # Stateful fjord semantics: each derived class is
                    # rebuilt from scratch every outflow tick (Phase 8
                    # ``inc_dict.clear()`` + re-materialise), so we
                    # always replace.  Users wanting forensic
                    # accumulation can pass ``if_exists="append"``
                    # explicitly in the per-class export_params.
                    resolved = _resolve_if_exists_for_export(
                        file_path=class_export.get("file_path"),
                        force_append=False,
                        user_override=class_export.get("if_exists"),
                    )
                    params = class_export if resolved is None else {**class_export, "if_exists": resolved}
                    await DerivedCls.export(instance=instances, **params)
                    row_count_holder[0] = len(instances)

            # B6: warn about export_params keys that outflow didn't fill.
            if is_multi:
                produced = set(grouped.keys())
                configured = {k for k, v in export_params.items() if k != "if_exists" and isinstance(v, dict)}
                orphan = configured - produced
                if orphan:
                    logger.warning(
                        "export_params declares output(s) %s but outflow(state) "
                        "did not produce any rows for them this tick.",
                        sorted(orphan),
                    )

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break
