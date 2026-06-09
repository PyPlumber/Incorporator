"""Fjord-specific outflow daemon: snapshot sources, run user fn, export combined output(s).

``outflow(state)`` may return either ``list[dict]`` (single derived class) or
``dict[ClassName, list[dict]]`` (one derived class per key, one export file per
class).  Detection is by ``isinstance(result, dict)`` — list returns take the
single-output path.

The :func:`flush` async generator factors the per-tick "outflow → build →
export" core out of :func:`_outflow_daemon` so other callers (notably
:class:`incorporator.observability.tideweaver.Tideweaver`'s ``_tick_fjord``)
can share the same primitive without re-implementing the dynamic-class
build + per-class export semantics.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, cast

from ...list import IncorporatorList
from ..logger import Wave  # re-export for callers
from ._shared import _daemon_tick, _interruptible_sleep, _resolve_if_exists_for_export

__all__ = ["Wave", "_outflow_daemon", "flush"]

logger = logging.getLogger(__name__)

# One-time-per-class WARNING dedup for ``_warn_on_bare_user_class`` so a
# long-running daemon doesn't spam the same diagnosis every wave.  Keyed
# by ``id(class)`` because user-declared classes are typically defined
# at module load and live forever.
_BARE_CLASS_WARNED: set[int] = set()


def _warn_on_bare_user_class(
    user_cls: Any,
    base_class: Any,
    sample_row: dict[str, Any] | None,
) -> None:
    """Warn once when a bare user class declaration suppresses field inference.

    ``flush()`` prefers a user-pre-declared subclass over
    :func:`infer_dynamic_schema` when the outflow module exposes a class
    with the matching ``__name__``.  But a bare declaration like::

        class Race(Incorporator):
            pass

    declares zero new fields beyond the base three, and the Incorporator
    base class doesn't set ``extra='allow'`` — so Pydantic V2's default
    ``extra='ignore'`` silently drops every row field on
    ``model_validate``.  The user thinks they're being explicit; the
    framework is dropping their data on the floor.

    This helper emits one WARNING per class identity when:

    1. The user class adds zero fields beyond the base, AND
    2. The class isn't opted into ``extra='allow'``, AND
    3. The first row carries field names the class won't accept.

    The fix suggested in the message is concrete: either declare the
    fields explicitly, or delete the class so inference takes over.
    """
    if user_cls is base_class or id(user_cls) in _BARE_CLASS_WARNED:
        return
    extra_fields = set(user_cls.model_fields) - set(base_class.model_fields)
    if extra_fields:
        return  # user declared fields explicitly — no inference suppression
    if user_cls.model_config.get("extra") == "allow":
        return  # extra='allow' → fields go to __pydantic_extra__, no data loss
    if not sample_row:
        return
    declared = set(user_cls.model_fields)
    dropped = [k for k in sample_row if k not in declared]
    if not dropped:
        return  # row only contains declared fields — no loss
    _BARE_CLASS_WARNED.add(id(user_cls))
    logger.warning(
        "Class %r is declared in your outflow module but adds no fields beyond "
        "Incorporator's base — %d field(s) on each row will be silently dropped "
        "on model_validate (%s%s).  Fix: either declare the fields explicitly "
        "(e.g. ``class %s(Incorporator): name: str = ...``) or delete the class "
        "so the engine can infer the schema from the data.",
        user_cls.__name__,
        len(dropped),
        ", ".join(dropped[:5]),
        ", ..." if len(dropped) > 5 else "",
        user_cls.__name__,
    )


def _normalise_outflow_return(
    result: Any,
    default_class_name: str,
) -> tuple[dict[str, list[Any]], bool]:
    """Coerce any ``outflow(state)`` return shape into ``{class_name: rows}``.

    Returns ``(grouped, is_multi_output)`` so the daemon can decide whether
    to look up per-class ``export_params`` or fall back to the flat
    single-output config.

    Shapes:
      * ``list[dict]`` → ``{default_class_name: list}`` (legacy single-output)
      * ``IncorporatorList`` → ``{default_class_name: list}`` (preserved
        verbatim so :func:`flush` can detect the pass-through identity case)
      * ``dict`` of ``str → list[dict]`` → multi-output (one derived class per key)
      * ``dict`` of ``str → IncorporatorList`` → multi-output, values preserved
        verbatim for the pass-through fast path
      * ``dict`` of ``str → str`` (NOT list-valued) → treated as a single
        flat row (wrapped in a list).
      * ``None`` / falsy → empty single-output (zero-row tick).
    """
    if result is None:
        return {default_class_name: []}, False
    if isinstance(result, list):
        # IncorporatorList is a subclass of list — keep it verbatim so the
        # pass-through fast path in flush() can detect ``rows._model_class``.
        return {default_class_name: result}, False
    if isinstance(result, dict):
        # Empty dict in multi-output flavor → no derived classes, no waves.
        if not result:
            return {}, True
        # Heuristic: if EVERY value is a list, it's the multi-output shape.
        # Otherwise it's a single-row dict (current single-output behaviour).
        if all(isinstance(v, list) for v in result.values()):
            # Degenerate single-key-matching-default shape (synthesised by
            # the stateful-stream shim's identity outflow) is morally single
            # output — the user's export_params is single-shape and they get
            # exactly one file.  Flagging it as multi-output here would emit
            # a spurious "multi-output dict but export_params is single-
            # output" warning at _resolve_export_params_for.
            if len(result) == 1 and default_class_name in result:
                return cast(dict[str, list[Any]], dict(result)), False
            # Preserve values verbatim — copying with ``list(v)`` would
            # strip IncorporatorList's ``_model_class`` attribute and
            # defeat the flush() pass-through fast path.
            return cast(dict[str, list[Any]], dict(result)), True
        return {default_class_name: [result]}, False
    # Anything else — fall back to wrapping in a list to match the legacy
    # "auto-coerce to list[dict]" contract.
    return {default_class_name: list(result)}, False


def _resolve_export_params_for(
    derived_name: str,
    export_params: dict[str, Any],
    is_multi_output: bool,
) -> dict[str, Any]:
    """Pick the right ``export_params`` slice for one derived class.

    Detection rule (matches the user's pick — "detect by `file_path` key"):
      * If the top-level ``export_params`` has a ``file_path`` key,
        treat the whole dict as a single-output config.
      * Otherwise (multi-output shape), look up by class name.  Missing
        keys return an empty dict — the daemon warns and skips that class.
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
    return cast(dict[str, Any], export_params.get(derived_name, {}))


async def flush(
    outflow_fn: Callable[[dict[str, Any]], Any],
    state: dict[str, list[Any]],
    *,
    default_output_class_name: str,
    base_class: Any,
    export_params: dict[str, Any],
    outflow_module: ModuleType | None = None,
) -> AsyncIterator[tuple[str, int, Exception | None]]:
    """Run one outflow flush; yield one ``(derived_name, row_count, error)`` per class.

    Calls ``outflow_fn(state)`` in :func:`asyncio.to_thread`, normalises the
    return via :func:`_normalise_outflow_return`, then iterates the resulting
    ``{class_name: rows}`` mapping.  For each derived class:

    1. Prefer a user-pre-declared :class:`~incorporator.Incorporator` subclass
       on ``outflow_module`` whose name matches; else build via
       :func:`infer_dynamic_schema`.
    2. Clear the class's ``inc_dict``, materialise instances, park a
       strong-ref ``_tideweaver_snapshot`` to defeat the
       :class:`weakref.WeakValueDictionary` registry — this is the same
       attribute :meth:`Tideweaver._tick_stream` parks on Stream
       classes, so downstream :class:`Fjord` / :class:`Export` ticks
       walking upstream output via ``getattr(dep.cls,
       "_tideweaver_snapshot", None)`` find Fjord outputs uniformly
       alongside Stream outputs.
    3. Resolve the per-class slice of ``export_params`` via
       :func:`_resolve_export_params_for`.
    4. ``await cls.export()`` when an export config is present.

    Per-class build/export errors are caught and surfaced as the third
    element of the yielded tuple — the caller decides whether to emit a
    per-class failure :class:`Wave`, log + continue, or abort.  Exceptions
    raised by ``outflow_fn`` itself **propagate** so the caller can shape
    its own composite failure response.

    Reused by both :func:`_outflow_daemon` (long-running fjord engine) and
    :class:`incorporator.observability.tideweaver.Tideweaver`'s
    ``_tick_fjord`` (per-interval fjord-flush in a Watershed).
    """
    # Local import keeps the observability layer free of a hard schema dep
    # at module-import time.
    from ...schema.builder import infer_dynamic_schema

    result = await asyncio.to_thread(outflow_fn, state)
    grouped, is_multi = _normalise_outflow_return(result, default_output_class_name)

    for derived_name, rows in grouped.items():
        try:
            if not rows:
                # Zero-row branch — success with no class built and no export.
                yield (derived_name, 0, None)
                continue

            # Prefer a user-pre-declared subclass with the matching name.
            user_cls = getattr(outflow_module, derived_name, None) if outflow_module is not None else None
            if user_cls is not None and isinstance(user_cls, type) and issubclass(user_cls, base_class):
                user_any = cast(Any, user_cls)
                extra_fields = set(user_any.model_fields) - set(base_class.model_fields)
                allows_extra = user_any.model_config.get("extra") == "allow"
                if extra_fields or allows_extra:
                    # User declared fields explicitly or opted into extra='allow' —
                    # use their class directly; warn only if bare-class conditions
                    # are somehow still triggered (safe: _warn_on_bare_user_class
                    # returns early when extra_fields is non-empty or extra='allow').
                    derived_cls = cast(Any, user_cls)
                    _warn_on_bare_user_class(derived_cls, base_class, rows[0] if rows else None)
                else:
                    # Class is bare (no declared fields beyond base, no extra='allow').
                    # Check whether the first row carries keys the bare class would drop.
                    sample = rows[0] if rows else {}
                    # Normalize to a plain key set: outflow() returns dicts, but a
                    # model-instance row (rare) is iterable as (field, value) pairs —
                    # iterating it directly would mis-key the drop check, so resolve
                    # keys explicitly for both shapes.
                    if isinstance(sample, dict):
                        sample_dict = sample
                    elif hasattr(sample, "model_dump"):
                        sample_dict = sample.model_dump()
                    else:
                        sample_dict = {}
                    undeclared = [k for k in sample_dict if k not in user_any.model_fields]
                    if undeclared:
                        # Row carries fields the bare class would silently drop.
                        # Warn once so the user knows their class was bypassed, then
                        # fall through to inference so every row field is preserved.
                        _warn_on_bare_user_class(user_cls, base_class, sample_dict)
                        derived_cls = cast(Any, infer_dynamic_schema(derived_name, rows, base_class))
                    else:
                        # Row only contains declared fields — bare class is safe to use.
                        derived_cls = cast(Any, user_cls)
                        _warn_on_bare_user_class(derived_cls, base_class, sample_dict or None)
            else:
                derived_cls = cast(Any, infer_dynamic_schema(derived_name, rows, base_class))

            # Pass-through fast path: when outflow returned the live
            # IncorporatorList of this derived class, the instances are
            # already in ``derived_cls.inc_dict`` — registered on __init__
            # and mutated in place by _refresh_daemon.  Skip clear+rebuild
            # so Python-object identity survives across waves (which is the
            # whole point of stateful streaming) and we don't pay the
            # allocation cost of materialising N new instances every tick.
            if isinstance(rows, IncorporatorList) and getattr(rows, "_model_class", None) is derived_cls:
                instances = list(rows)
            else:
                derived_cls._ensure_inc_dict()
                derived_cls.inc_dict.clear()
                # model_validate skips the ``**kwargs`` unpack per row and lets
                # Pydantic's Rust core amortise field-offset lookups across the
                # whole list — matches the build_instances:300 fast path.
                # Yield-point-safe: no ``await`` between the flag set and the
                # list comprehension, so concurrent ticks cannot interleave
                # this pair.  If a future refactor awaits inside the loop,
                # this gate becomes unsafe -- revisit then.
                derived_cls._BATCH_INSERT_MODE = True
                try:
                    instances = [derived_cls.model_validate(row) for row in rows]
                finally:
                    derived_cls._BATCH_INSERT_MODE = False
                from ...base import Incorporator as _Incorporator

                derived_cls.inc_dict.update({inst.inc_code: inst for inst in instances})
                if derived_cls.__bases__ and derived_cls.__bases__[0] is not _Incorporator:
                    for base in derived_cls.__bases__:
                        if issubclass(base, _Incorporator) and base is not _Incorporator:
                            base._ensure_inc_dict()
                            base.inc_dict.update({inst.inc_code: inst for inst in instances})
            # Strong-ref bridge: pins instances on the class object so
            # ``inc_dict`` (a ``WeakValueDictionary``) keeps them alive
            # AND so downstream ticks reading ``getattr(dep.cls,
            # "_tideweaver_snapshot", None)`` find them.  Same attribute
            # ``_tick_stream`` parks on Stream classes -- unified name.
            derived_cls._tideweaver_snapshot = instances

            class_export = _resolve_export_params_for(derived_name, export_params, is_multi)
            if not class_export:
                logger.warning(
                    "outflow(state) emitted class %r but export_params has no matching key; skipping export.",
                    derived_name,
                )
                yield (derived_name, len(instances), None)
                continue

            resolved = _resolve_if_exists_for_export(
                file_path=class_export.get("file_path"),
                force_append=False,
                user_override=class_export.get("if_exists"),
            )
            params = class_export if resolved is None else {**class_export, "if_exists": resolved}
            await derived_cls.export(instance=instances, **params)
            yield (derived_name, len(instances), None)
        except Exception as exc:  # noqa: BLE001 — caller decides what to do
            yield (derived_name, 0, exc)


async def _outflow_daemon(
    output_class_name: str,
    base_class: Any,
    source_refs: list[list[Any]],
    source_classes: list[Any],
    outflow_fn: Any,
    export_params: dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: asyncio.Queue[Wave | None],
    shutdown_event: asyncio.Event,
    e_interval: float | None,
    outflow_module: ModuleType | None = None,
) -> None:
    """Periodic outflow-and-export daemon for the fjord engine.

    On every tick (two-phase, multi-output aware):

    Each tick runs in two stages:

    **Stage 1 — snapshot + invoke user outflow_fn.**
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
           * ``list[dict]``  → single-output (one class)
           * ``dict[str, list[dict]]`` → multi-output (one class per key,
             one file per key)
           * ``{}``          → no-op tick (zero waves)

    **Stage 2 — per-derived-class build + export.**  For every
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
         a strong-ref ``_tideweaver_snapshot`` to defeat the
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
    loop_idx = 0
    # Pre-compute state dict keys once — avoids re-allocating the key list on
    # every tick.  The key order is stable for the lifetime of the daemon.
    state_keys = [cls.__name__ for cls in source_classes]

    while not shutdown_event.is_set():
        loop_idx += 1

        # Stage 1: snapshot under lock.
        async with lock:
            state = dict(zip(state_keys, [ref[0] for ref in source_refs], strict=False))

        # Stage 2: delegate the outflow + per-class build/export to flush().
        # outflow_fn-level exceptions propagate out of the async-for and land
        # in the except below as the single composite failure wave.  Per-class
        # build/export errors are surfaced as the third tuple element so the
        # _daemon_tick context emits a properly-shaped per-class failure wave.
        produced: set[str] = set()
        try:
            async for derived_name, count, err in flush(
                outflow_fn,
                state,
                default_output_class_name=output_class_name,
                base_class=base_class,
                export_params=export_params,
                outflow_module=outflow_module,
            ):
                produced.add(derived_name)
                row_count_holder: list[int] = [count]

                def _read_row_count(holder: list[int] = row_count_holder) -> int:
                    return holder[0]

                async with _daemon_tick(
                    wave_queue,
                    chunk_index=loop_idx,
                    operation=f"outflow:{derived_name}",
                    error_prefix="Outflow Error",
                    row_count_fn=_read_row_count,
                ):
                    if err is not None:
                        # Re-raise inside the _daemon_tick so it emits the
                        # standard per-class failure wave shape.
                        raise err

            # Warn about export_params keys that outflow didn't produce rows for.
            # Only meaningful when the config is multi-output (> 1 nested-dict
            # entries keyed by class name).
            configured = {k for k, v in export_params.items() if k != "if_exists" and isinstance(v, dict)}
            if len(configured) > 1:
                orphan = configured - produced
                if orphan:
                    logger.warning(
                        "export_params declares output(s) %s but outflow(state) "
                        "did not produce any rows for them this wave.",
                        sorted(orphan),
                    )
        except Exception as exc:  # noqa: BLE001 — surface to telemetry, never crash the daemon
            # outflow_fn itself raised.  Single composite failure wave keyed
            # by the default output class (we don't know which derived class
            # would have produced what at this point).
            await wave_queue.put(
                Wave.model_construct(
                    chunk_index=loop_idx,
                    operation=f"outflow:{output_class_name}",
                    rows_processed=0,
                    failed_sources=[f"Outflow Error: {exc}"],
                    rejects=[],
                    processing_time_sec=0.0,
                    source_url=None,
                    bytes_processed=None,
                    http_retry_count=0,
                    validation_error_count=0,
                    schema_cache_hit=True,
                    conv_dict_time_sec=None,
                    timestamp=datetime.now(timezone.utc),
                )
            )

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break
