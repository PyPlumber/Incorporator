"""Benchmark: per-row ``model_validate`` vs ``TypeAdapter(List[Cls]).validate_python``.

A-F-3 of the canal-followup roadmap.  The integration audit at
``docs/canal_integration_audit.md §6`` flagged
`incorporator/schema/factory.py:300-313` as the highest-impact scaling
lever — but the rationale comment at lines 305-308 explicitly chose
per-row over batch:

    # model_validate avoids a redundant **kwargs unpack per row and allows
    # Pydantic's Rust core to amortise field-offset lookups across calls.
    # Batching in 1000-row chunks keeps peak memory predictable and gives
    # Pydantic's internal schema cache the best hit rate.

So we don't migrate on hope.  This benchmark measures both paths
across six realistic shapes and produces speedup ratios.  The
decision rule (per the consolidation roadmap): if the
``realistic_medium`` cell shows TypeAdapter winning by >20% (speedup
≥ 1.2×), propose the A-F-4 migration via a spawn plan; otherwise
document the negative result and defer A-F-4 indefinitely.

We use plain Pydantic ``BaseModel`` (not :class:`Incorporator`) so the
comparison isolates pure validation cost.  Incorporator subclasses
add a constant ``model_post_init`` overhead (~100-200 ns per instance
for the ``inc_dict`` WeakValueDictionary insert) that applies to both
methods equally — the speedup ratio is conserved.  Realistic
deployment speedup will be marginally smaller than the numbers
printed here because that constant cost dilutes the relative
validation gain, but the >20% decision threshold is comfortably above
any plausible dilution.

Cached ``TypeAdapter`` is constructed OUTSIDE the timer because the
production migration would cache it per-class too (mirroring
``factory.py:294-299``'s ``_cached_json_properties`` pattern).

This is an informational benchmark, not a regression-class one — the
``assert`` calls only verify that both methods returned the expected
instance count.  No floor assertion on speedup, since the WHOLE
POINT is to measure what the speedup actually is.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple, Type, cast

import pytest
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


# ---------------------------------------------------------------------------
# Synthetic row generators (one per type-mix shape)
# ---------------------------------------------------------------------------


def _gen_all_str(n: int, m: int) -> List[Dict[str, Any]]:
    """N rows, M fields, all values are short strings."""
    return [{f"f{i}": f"v{r}_{i}" for i in range(m)} for r in range(n)]


def _gen_mixed(n: int, m: int) -> List[Dict[str, Any]]:
    """N rows, M fields, types cycle through int/float/str/bool."""
    return [
        {
            f"f{i}": (
                r * 10 + i
                if i % 4 == 0
                else (r + i) * 1.5
                if i % 4 == 1
                else f"v{r}_{i}"
                if i % 4 == 2
                else bool(r % 2)
            )
            for i in range(m)
        }
        for r in range(n)
    ]


def _gen_mixed_with_optional(n: int, m: int) -> List[Dict[str, Any]]:
    """N rows, M fields; half the fields are sometimes ``None``."""
    return [
        {
            f"f{i}": (
                None
                if i >= m // 2 and r % 3 == 0
                else (
                    r * 10 + i
                    if i % 4 == 0
                    else (r + i) * 1.5
                    if i % 4 == 1
                    else f"v{r}_{i}"
                    if i % 4 == 2
                    else bool(r % 2)
                )
            )
            for i in range(m)
        }
        for r in range(n)
    ]


# ---------------------------------------------------------------------------
# Dynamic Pydantic-model builders (one per type-mix shape)
# ---------------------------------------------------------------------------


def _build_all_str_model(m: int) -> Type[BaseModel]:
    """Build a BaseModel subclass with M ``str`` fields."""
    fields: Dict[str, Any] = {f"f{i}": (str, ...) for i in range(m)}
    return cast(
        Type[BaseModel],
        type(
            "AllStrModel",
            (BaseModel,),
            {"__annotations__": {f"f{i}": str for i in range(m)}, "model_config": ConfigDict(extra="forbid")},
        ),
    )


def _build_mixed_model(m: int) -> Type[BaseModel]:
    """Build a BaseModel with int/float/str/bool fields cycling."""
    annotations: Dict[str, Any] = {}
    for i in range(m):
        if i % 4 == 0:
            annotations[f"f{i}"] = int
        elif i % 4 == 1:
            annotations[f"f{i}"] = float
        elif i % 4 == 2:
            annotations[f"f{i}"] = str
        else:
            annotations[f"f{i}"] = bool
    return cast(
        Type[BaseModel],
        type(
            "MixedModel",
            (BaseModel,),
            {"__annotations__": annotations, "model_config": ConfigDict(extra="forbid")},
        ),
    )


def _build_mixed_with_optional_model(m: int) -> Type[BaseModel]:
    """Build a BaseModel where half the fields are ``Optional[T]``."""
    annotations: Dict[str, Any] = {}
    for i in range(m):
        # Same base type cycle as _build_mixed_model; second half wrapped in Optional.
        if i % 4 == 0:
            base = int
        elif i % 4 == 1:
            base = float  # type: ignore[assignment]
        elif i % 4 == 2:
            base = str  # type: ignore[assignment]
        else:
            base = bool  # type: ignore[assignment]
        annotations[f"f{i}"] = Optional[base] if i >= m // 2 else base  # type: ignore[assignment]
    # Default ``None`` for the Optional fields so omitted values validate.
    namespace: Dict[str, Any] = {"__annotations__": annotations, "model_config": ConfigDict(extra="forbid")}
    for i in range(m // 2, m):
        namespace[f"f{i}"] = Field(default=None)
    return cast(Type[BaseModel], type("MixedOptModel", (BaseModel,), namespace))


# ---------------------------------------------------------------------------
# Measurement harness
# ---------------------------------------------------------------------------


def _measure_per_row(cls: Type[BaseModel], rows: List[Dict[str, Any]]) -> float:
    """Time the per-row ``model_validate`` loop that matches ``factory.py:309-312``."""
    _BATCH = 1000
    instances: List[Any] = []
    t0 = time.perf_counter()
    for i in range(0, len(rows), _BATCH):
        instances.extend(cls.model_validate(row) for row in rows[i : i + _BATCH])
    elapsed = time.perf_counter() - t0
    assert len(instances) == len(rows), f"per-row produced {len(instances)} != {len(rows)} instances"
    return elapsed


def _measure_batch(cls: Type[BaseModel], rows: List[Dict[str, Any]]) -> float:
    """Time ``TypeAdapter(List[Cls]).validate_python(rows)`` with the adapter cached out of band."""
    # Adapter construction happens OUTSIDE the timer — production migration
    # would cache it per-class via the same pattern as
    # ``factory.py:_cached_json_properties``.
    adapter: TypeAdapter[List[BaseModel]] = TypeAdapter(List[cls])  # type: ignore[valid-type]
    t0 = time.perf_counter()
    instances = adapter.validate_python(rows)
    elapsed = time.perf_counter() - t0
    assert len(instances) == len(rows), f"batch produced {len(instances)} != {len(rows)} instances"
    return elapsed


def _report(name: str, n: int, m: int, per_row: float, batch: float) -> Tuple[float, float, float]:
    """Compute speedup; print human-readable summary; return (per_row, batch, speedup)."""
    speedup = per_row / batch if batch > 0 else float("inf")
    rows_per_sec_per_row = n / per_row
    rows_per_sec_batch = n / batch
    print(
        f"\n  {name:<22} N={n:>7,} M={m:>2}: "
        f"per-row={per_row * 1000:6.1f}ms ({rows_per_sec_per_row:>10,.0f} rows/sec), "
        f"batch={batch * 1000:6.1f}ms ({rows_per_sec_batch:>10,.0f} rows/sec), "
        f"speedup={speedup:.2f}×"
    )
    return per_row, batch, speedup


# ---------------------------------------------------------------------------
# Six benchmark cells
# ---------------------------------------------------------------------------


def test_validate_small_all_str() -> None:
    """1k × 5 fields, all-str — homogeneous baseline."""
    n, m = 1_000, 5
    cls = _build_all_str_model(m)
    rows = _gen_all_str(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("small_all_str", n, m, per_row, batch)


def test_validate_small_mixed() -> None:
    """1k × 5 fields, mixed int/float/str/bool — small with type variety."""
    n, m = 1_000, 5
    cls = _build_mixed_model(m)
    rows = _gen_mixed(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("small_mixed", n, m, per_row, batch)


def test_validate_realistic_medium() -> None:
    """10k × 20 fields, mixed types — canonical realistic shape.

    **This is the cell that drives the A-F-4 decision.**  If speedup
    on this cell is ≥ 1.2×, propose the migration via the spawn plan;
    otherwise document the negative result and defer A-F-4.
    """
    n, m = 10_000, 20
    cls = _build_mixed_model(m)
    rows = _gen_mixed(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("realistic_medium", n, m, per_row, batch)


def test_validate_large() -> None:
    """100k × 20 fields, mixed types — high-row-count regime."""
    n, m = 100_000, 20
    cls = _build_mixed_model(m)
    rows = _gen_mixed(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("large", n, m, per_row, batch)


def test_validate_wide_schema() -> None:
    """10k × 50 fields, mixed types — wide-schema shape."""
    n, m = 10_000, 50
    cls = _build_mixed_model(m)
    rows = _gen_mixed(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("wide_schema", n, m, per_row, batch)


def test_validate_with_optional() -> None:
    """10k × 20 fields, mixed types, half ``Optional[T]`` — Optional-fallthrough cost."""
    n, m = 10_000, 20
    cls = _build_mixed_with_optional_model(m)
    rows = _gen_mixed_with_optional(n, m)
    per_row = _measure_per_row(cls, rows)
    batch = _measure_batch(cls, rows)
    _report("with_optional", n, m, per_row, batch)
