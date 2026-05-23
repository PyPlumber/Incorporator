"""Stateful-stream → fjord shim.

``stream(stateful_polling=True)`` is morally a single-source variant of fjord:
seed once via ``incorp()``, hold instances in a ``WeakValueDictionary`` registry,
refresh in place, snapshot for export.  Rather than maintain two parallel engines
(legacy ``_run_stateful_engine`` was deleted in commit ``c79a3ac``), this module
hosts the thin adapter that synthesises a one-source fjord pipeline with an
identity outflow.

Two properties of this shim are load-bearing:

1. **Identity preservation across waves.**  The ``IncorporatorList`` pass-through
   fast path in :func:`._outflow.flush` detects ``outflow(state) →
   {ClassName: state[ClassName]}`` and skips the usual ``inc_dict.clear()`` +
   re-materialise dance.  Python-object identity in ``cls.inc_dict`` therefore
   survives the export tick — that's what callers holding cross-wave refs into
   the registry rely on.

2. **Wave op-string compatibility.**  Fjord emits ``"fjord_incorp:<Cls>"`` etc.;
   ``stream()`` documents ``"incorp"`` / ``"refresh"`` / ``"export"``.  This
   adapter rewrites ``operation`` (and trims the ``" for <ClassName>"`` from
   seed-empty failure messages) so existing wave consumers see no diff.

The class-agnostic signature accepts ``base_class`` as a parameter rather than
importing ``Incorporator`` directly — keeps the import graph one-directional
(``base.py → pipeline/`` never the reverse).
"""

import re
import time
import types
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Type

from ..logger import Wave
from . import DEFAULT_EXPORT_INTERVAL_SEC, DEFAULT_REFRESH_INTERVAL_SEC
from ._shared import _row_count
from .fjord import _run_fjord_engine

__all__ = ["stream_stateful_via_fjord"]


async def stream_stateful_via_fjord(
    *,
    receiver_cls: Type[Any],
    base_class: Type[Any],
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
    refresh_interval: Optional[float],
    export_interval: Optional[float],
    outflow_user_module: Optional[Any],
    inflow_callable: Optional[Callable[[Dict[str, Any]], Any]] = None,
) -> AsyncGenerator[Wave, None]:
    """Run ``stream(stateful_polling=True)`` semantics by adapting to fjord.

    Seed-only short-circuit: when the caller passed neither ``refresh_params``
    nor ``export_params``, this adapter does the seed in-line and emits one
    Wave — no daemons spawn.  Mirrors the legacy ``_run_stateful_engine``
    behaviour and avoids fjord's "always spawn the outflow daemon" path
    emitting an extra phantom wave.

    Args:
        receiver_cls: The Incorporator subclass whose registry is the source
            of truth.  ``state[receiver_cls.__name__]`` is what the identity
            outflow returns; ``flush()``'s pass-through fast path reuses
            ``receiver_cls.inc_dict`` directly.
        base_class: The Incorporator base class — passed through to
            ``_run_fjord_engine`` for its ``issubclass`` check against
            user-pre-declared subclasses on the outflow module.
        incorp_params: kwargs forwarded to ``receiver_cls.incorp()`` for seed.
        refresh_params: kwargs for ``receiver_cls.refresh()``; ``None`` opts
            out of refresh.
        export_params: kwargs for the (combined) export; ``None`` opts out.
        poll_interval, refresh_interval, export_interval: standard stream
            cascade — explicit interval > poll_interval > module default.
        outflow_user_module: optional preloaded outflow.py module.  When
            present and exposes a top-level ``outflow(state)`` function,
            that function is used in place of the synthesised identity
            outflow.  Either way the module is passed to ``flush()`` so its
            pre-declared subclass (by ``__name__``) wins the class match.

    Yields:
        Wave: with op-strings remapped to stream's documented contract
        (``"incorp"`` / ``"refresh"`` / ``"export"``).
    """
    cls_name = receiver_cls.__name__

    # Seed-only short-circuit: when no daemons are requested, return after
    # the seed wave so callers that just want "load the registry once and
    # exit" don't pay for an outflow-daemon tick (and see an extra phantom
    # wave).
    if refresh_params is None and export_params is None:
        seed_start = time.perf_counter()
        try:
            initial_dataset = await receiver_cls.incorp(**incorp_params)
        except Exception as exc:
            yield Wave.model_construct(
                chunk_index=1,
                operation="incorp",
                rows_processed=0,
                failed_sources=[f"Seed Error: {exc}"],
                processing_time_sec=time.perf_counter() - seed_start,
                source_url=None,
                bytes_processed=None,
                http_retry_count=0,
                validation_error_count=0,
                schema_cache_hit=True,
                conv_dict_time_sec=None,
                timestamp=datetime.now(timezone.utc),
            )
            return
        seed_elapsed = time.perf_counter() - seed_start
        if not initial_dataset:
            yield Wave.model_construct(
                chunk_index=1,
                operation="incorp",
                rows_processed=0,
                failed_sources=["Initial incorp() yielded no data"],
                processing_time_sec=seed_elapsed,
                source_url=None,
                bytes_processed=None,
                http_retry_count=0,
                validation_error_count=0,
                schema_cache_hit=True,
                conv_dict_time_sec=None,
                timestamp=datetime.now(timezone.utc),
            )
            return
        yield Wave.model_construct(
            chunk_index=1,
            operation="incorp",
            rows_processed=_row_count(initial_dataset),
            processing_time_sec=seed_elapsed,
            source_url=None,
            bytes_processed=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        return

    # Pick the outflow callable: prefer a user-defined ``outflow(state)`` in
    # the supplied outflow.py, otherwise synthesise an identity outflow that
    # returns the live IncorporatorList for the receiver class.  flush()'s
    # pass-through fast path detects this and preserves instance identity.
    def _identity_outflow(state: Dict[str, Any]) -> Dict[str, Any]:
        return {cls_name: state[cls_name]}

    outflow_fn: Any = _identity_outflow
    outflow_module: Optional[Any] = outflow_user_module
    if outflow_user_module is not None:
        candidate_fn = getattr(outflow_user_module, "outflow", None)
        if callable(candidate_fn):
            outflow_fn = candidate_fn

    # Stub module exposing receiver_cls under its __name__ so flush()'s
    # pre-declared-subclass match (by name) keeps the user's class identity
    # instead of building a new dynamic class.  Only built when no user
    # module was supplied (otherwise the user's module already exposes the
    # subclass and we want their authored definitions to win).
    if outflow_module is None:
        outflow_module = types.ModuleType("_inc_stream_identity_outflow_module")
        setattr(outflow_module, cls_name, receiver_cls)

    # Interval cascade — mirrors the legacy run_pipeline branch so a stateful
    # stream spawned without any interval kwargs still has a sensible tick
    # rate (legacy behaviour: 60 s refresh / 300 s export).
    r_interval = refresh_interval or poll_interval or DEFAULT_REFRESH_INTERVAL_SEC
    e_interval = export_interval or poll_interval or DEFAULT_EXPORT_INTERVAL_SEC

    stream_params: List[Dict[str, Any]] = [
        {
            "cls": receiver_cls,
            "incorp_params": incorp_params,
            "refresh_params": refresh_params,
        }
    ]
    effective_export_params: Dict[str, Any] = export_params if export_params is not None else {}

    # Anchored regex: only strip " for <cls_name>" when it precedes " yielded".
    # That's the exact shape fjord emits in the seed-empty failure message
    # (fjord.py:230 ``"Initial incorp() for {cls_name} yielded no data"``).
    # str.replace would also strip the literal " for <cls_name>" elsewhere,
    # which mangles failure messages when the class name is a common English
    # word (Latest, Initial, …) — see R3 in the risk audit.
    seed_suffix_re = re.compile(r" for " + re.escape(cls_name) + r"(?= yielded)")
    async for wave in _run_fjord_engine(
        output_class_name=cls_name,
        base_class=base_class,
        stream_params=stream_params,
        outflow_fn=outflow_fn,
        export_params=effective_export_params,
        r_interval=r_interval,
        e_interval=e_interval,
        outflow_module=outflow_module,
        inflow_callable=inflow_callable,
    ):
        op = wave.operation
        if op.startswith("fjord_incorp:"):
            new_op: Optional[str] = "incorp"
        elif op.startswith("fjord_refresh:"):
            new_op = "refresh"
        elif op.startswith("outflow:"):
            new_op = "export"
        else:
            new_op = None  # leave unchanged (e.g. "export:<Cls>" per-source export)

        new_failed = (
            [seed_suffix_re.sub("", s) for s in wave.failed_sources] if wave.failed_sources else wave.failed_sources
        )

        if new_op is None and new_failed == wave.failed_sources:
            yield wave
        else:
            yield wave.model_copy(
                update={
                    "operation": new_op if new_op is not None else wave.operation,
                    "failed_sources": new_failed,
                }
            )
