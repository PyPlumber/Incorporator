"""Validation and correctness tests for new guardrails added in the remediation phases."""

import asyncio
import json
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any, List

import pytest

from incorporator import Incorporator
from incorporator.schema.builder import MAX_REGISTRY_SIZE


# ==========================================
# 1. export() ISINSTANCE GUARD
# ==========================================


@pytest.mark.asyncio
async def test_export_rejects_invalid_instance_type(tmp_path: Path) -> None:
    """export() must raise TypeError when instance is not a list/Incorporator/BaseModel."""

    class ExportGuardModel(Incorporator):
        pass

    with pytest.raises(TypeError, match="instance"):
        await ExportGuardModel.export(
            instance="this_is_a_plain_string",  # type: ignore[arg-type]
            file_path=str(tmp_path / "out.csv"),
        )


@pytest.mark.asyncio
async def test_export_accepts_list_instance(tmp_path: Path) -> None:
    """export() must NOT raise when instance is a properly populated list."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    class ExportListModel(Incorporator):
        pass

    result = await ExportListModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_path = tmp_path / "out.json"

    # Must not raise
    await ExportListModel.export(instance=result, file_path=str(out_path))
    assert out_path.exists()


# ==========================================
# 2. transform() SIGNATURE VALIDATION
# ==========================================


@pytest.mark.asyncio
async def test_outflow_transform_wrong_arity_raises(tmp_path: Path) -> None:
    """apply_code_transform must raise ValueError when transform has wrong arity."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1}]), encoding="utf-8")

    bad_transform = tmp_path / "bad_transform.py"
    bad_transform.write_text(
        textwrap.dedent("""\
        def transform(instances, extra_param):
            return instances
        """),
        encoding="utf-8",
    )

    class TransformArityModel(Incorporator):
        pass

    result = await TransformArityModel.incorp(inc_file=str(json_file))
    with pytest.raises(ValueError, match="exactly 1 parameter"):
        await TransformArityModel.export(
            instance=result,
            file_path=str(tmp_path / "out.json"),
            outflow=str(bad_transform),
        )


@pytest.mark.asyncio
async def test_outflow_transform_correct_arity_passes(tmp_path: Path) -> None:
    """apply_code_transform must succeed when transform has exactly 1 parameter."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    good_transform = tmp_path / "good_transform.py"
    good_transform.write_text(
        textwrap.dedent("""\
        def transform(instances):
            return [{"id": obj.id, "name": obj.name, "upper_name": obj.name.upper()} for obj in instances]
        """),
        encoding="utf-8",
    )

    class TransformGoodModel(Incorporator):
        pass

    result = await TransformGoodModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_path = tmp_path / "out.json"
    await TransformGoodModel.export(instance=result, file_path=str(out_path), outflow=str(good_transform))

    content = out_path.read_text(encoding="utf-8")
    assert "ALICE" in content


# ==========================================
# 3. outflow SCHEMA DRIFT
# ==========================================


@pytest.mark.asyncio
async def test_export_outflow_new_field_in_csv(tmp_path: Path) -> None:
    """When outflow adds a new field, it must appear as a column in CSV output."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    transform_file = tmp_path / "transform.py"
    transform_file.write_text(
        textwrap.dedent("""\
        def transform(instances):
            return [
                {"id": obj.id, "name": obj.name, "synthesized_field": "generated"}
                for obj in instances
            ]
        """),
        encoding="utf-8",
    )

    class DriftTestModel(Incorporator):
        pass

    result = await DriftTestModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_csv = tmp_path / "out.csv"
    await DriftTestModel.export(
        instance=result,
        file_path=str(out_csv),
        outflow=str(transform_file),
    )

    content = out_csv.read_text(encoding="utf-8")
    # The synthesized_field column must appear in the CSV header
    assert "synthesized_field" in content
    assert "generated" in content


# ==========================================
# 4. _schema_union CONCURRENT SAFETY
# ==========================================


@pytest.mark.asyncio
async def test_schema_union_concurrent_gather_safety(tmp_path: Path) -> None:
    """Many concurrent incorp() calls on the same class must not crash or lose schema-union fields.

    ``build_instances`` runs inside ``asyncio.to_thread`` worker threads (real
    OS threads), so a reader iterating ``cls._schema_union`` (inside
    ``_expand_conv_dict_with_schema_union``) and a sibling writer inserting
    new keys into that same dict can genuinely run at the same time. Each
    round first seeds a nonempty ``_schema_union`` (sequential incorp) so the
    reader's ``if not schema_union: return conv_dict`` guard doesn't
    short-circuit, then fires a wide fan of concurrent ``incorp()`` calls that
    each add many new, distinct fields to the same class — widening the
    reader's iteration window and the writer's insertion loop enough, across
    enough concurrent workers, that real OS-thread scheduling actually
    interleaves them. Lowering ``sys.setswitchinterval`` makes the interpreter
    switch threads far more often, increasing the odds that overlap is hit.
    Empirically (worker-count/field-count calibrated against this exact
    shape): pre-fix (live-dict iteration in
    ``_expand_conv_dict_with_schema_union``) this reliably raises
    ``RuntimeError('dictionary changed size during iteration')`` within the
    10 rounds below; post-fix (snapshot via ``list(...)``) it cannot.
    """
    original_switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for round_idx in range(10):

            class ConcurrentModel(Incorporator):
                pass

            # Seed a nonempty union (sequential, single-item-free) so the
            # concurrent calls below hit a nonempty ``schema_union`` and the
            # reader actually iterates instead of short-circuiting.
            seed_file = tmp_path / f"seed_{round_idx}.json"
            seed_file.write_text(
                json.dumps([{f"seed_{round_idx}_{i}": i for i in range(40)}] * 2),
                encoding="utf-8",
            )
            await ConcurrentModel.incorp(inc_file=str(seed_file))

            n_workers = 20
            worker_field_sets = []
            worker_files = []
            for w in range(n_workers):
                fields = {f"field_{round_idx}_{w}_{i}": i for i in range(40)}
                worker_field_sets.append(fields)
                # 2+ items required: single-item files trigger the is_single path which skips _schema_union
                fp = tmp_path / f"w_{round_idx}_{w}.json"
                fp.write_text(json.dumps([fields, fields]), encoding="utf-8")
                worker_files.append(fp)

            await asyncio.gather(*[ConcurrentModel.incorp(inc_file=str(fp)) for fp in worker_files])

            # Every worker's fields must appear in the union regardless of finish order
            for fields in worker_field_sets:
                assert all(f in ConcurrentModel._schema_union for f in fields)  # type: ignore[attr-defined]
    finally:
        sys.setswitchinterval(original_switch_interval)


@pytest.mark.asyncio
async def test_schema_registry_concurrent_gather_integrity(tmp_path: Path) -> None:
    """Concurrent incorp() calls across MULTIPLE DISTINCT classes must not corrupt SCHEMA_REGISTRY.

    Unlike ``_schema_union`` (per-class), ``SCHEMA_REGISTRY`` is a single
    module-global ``OrderedDict`` shared by every ``Incorporator`` subclass.
    ``infer_dynamic_schema`` runs inside ``asyncio.to_thread`` worker threads,
    so two concurrent ``incorp()`` calls — on the SAME or DIFFERENT classes —
    can interleave the registry's ``move_to_end`` / ``popitem`` / item-insert
    calls, which are not atomic relative to each other. Each round fans out
    ``incorp()`` across several fresh classes with unique per-round field
    names, guaranteeing every call is a registry miss (insert path fires)
    rather than a hit, and lowers ``sys.setswitchinterval`` to maximize real
    OS-thread interleaving odds.

    Honesty note (per brief): unlike the D3-02 ``_schema_union`` fix, this is
    NOT a deterministic pre-fix crash reproduction — ``OrderedDict`` mutation
    races don't reliably raise under a short test run. This test is an
    integrity canary + regression lock: it asserts the registry stays within
    its size bound and every surviving entry is retrievable/functional after
    the fix, not that it demonstrably crashes without the fix. Observed:
    run pre-fix (no ``_SCHEMA_REGISTRY_LOCK``), this exact test passed anyway
    — the default ``MAX_REGISTRY_SIZE`` (1000) is never approached by this
    round count/worker count, so the eviction race window this test targets
    isn't hit. The deterministic reproduction of corruption lives in
    ``tests/test_schema_builder.py::test_infer_schema_registry_concurrent_eviction_deterministic``
    (small monkeypatched ``MAX_REGISTRY_SIZE`` forces the eviction path).
    """
    from incorporator.schema.builder import SCHEMA_REGISTRY

    original_switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for round_idx in range(10):
            n_workers = 8
            worker_files = []
            for w in range(n_workers):
                fields = {f"reg_{round_idx}_{w}_{i}": i for i in range(10)}
                fp = tmp_path / f"reg_{round_idx}_{w}.json"
                fp.write_text(json.dumps(fields), encoding="utf-8")
                worker_files.append(fp)

            worker_classes = []
            for _ in range(n_workers):

                class RegistryConcurrentModel(Incorporator):
                    pass

                worker_classes.append(RegistryConcurrentModel)

            await asyncio.gather(
                *[cls.incorp(inc_file=str(fp)) for cls, fp in zip(worker_classes, worker_files, strict=True)]
            )

        assert len(SCHEMA_REGISTRY) <= MAX_REGISTRY_SIZE
        # Every surviving entry must still be a usable Pydantic model class.
        for model_cls in list(SCHEMA_REGISTRY.values()):
            assert hasattr(model_cls, "model_fields")
    finally:
        sys.setswitchinterval(original_switch_interval)


def test_inc_dict_sibling_class_isolation() -> None:
    """Sibling user subclasses each own an isolated ``inc_dict`` after first write.

    Regression guard for the shared-registry bug: before
    ``Incorporator._ensure_inc_dict()`` forked the WeakValueDictionary
    per subclass, every user class shared the one defined on
    Incorporator, so instances of one class leaked into every other
    class's lookups.

    Allocation is now deferred to first write (``model_post_init`` calls
    ``cls._ensure_inc_dict()`` before registering), so the per-class
    fork happens as soon as either class is instantiated.  Plain
    construction (no incorp pipeline) is enough to exercise the
    registration path.
    """

    class SiblingRegistryA(Incorporator):
        pass

    class SiblingRegistryB(Incorporator):
        pass

    a = SiblingRegistryA(inc_code="a-row")
    b = SiblingRegistryB(inc_code="b-row")

    # After first write, each class owns a distinct dict — neither shares
    # with the other and neither shares with the base Incorporator.
    assert SiblingRegistryA.inc_dict is not SiblingRegistryB.inc_dict
    assert SiblingRegistryA.inc_dict is not Incorporator.inc_dict
    assert SiblingRegistryB.inc_dict is not Incorporator.inc_dict

    assert list(SiblingRegistryA.inc_dict.keys()) == ["a-row"]
    assert list(SiblingRegistryB.inc_dict.keys()) == ["b-row"]
    # The bubble-up registration stops short of Incorporator itself — the
    # global registry must stay empty so cross-class drilling can't see it.
    assert "a-row" not in Incorporator.inc_dict
    assert "b-row" not in Incorporator.inc_dict
    # Keep the instances alive until the assertions complete.
    assert a.inc_code == "a-row" and b.inc_code == "b-row"


@pytest.mark.asyncio
async def test_schema_union_sibling_class_isolation(tmp_path: Path) -> None:
    """Sibling subclasses must not share _schema_union state."""
    json_a = tmp_path / "a.json"
    json_b = tmp_path / "b.json"
    # 2+ items required: single-item files trigger the is_single path which skips _schema_union
    json_a.write_text(json.dumps([{"field_only_in_a": 1}, {"field_only_in_a": 2}]), encoding="utf-8")
    json_b.write_text(json.dumps([{"field_only_in_b": 3}, {"field_only_in_b": 4}]), encoding="utf-8")

    class SiblingA(Incorporator):
        pass

    class SiblingB(Incorporator):
        pass

    # Clear any residual state
    for cls in (SiblingA, SiblingB):
        if "_schema_union" in cls.__dict__:
            del cls._schema_union  # type: ignore[attr-defined]

    await SiblingA.incorp(inc_file=str(json_a))
    await SiblingB.incorp(inc_file=str(json_b))

    a_union = SiblingA._schema_union  # type: ignore[attr-defined]
    b_union = SiblingB._schema_union  # type: ignore[attr-defined]

    assert "field_only_in_a" in a_union
    assert "field_only_in_b" not in a_union  # Sibling B's field must NOT bleed into A

    assert "field_only_in_b" in b_union
    assert "field_only_in_a" not in b_union  # Sibling A's field must NOT bleed into B


@pytest.mark.asyncio
async def test_per_subclass_isolation_walks_mro(tmp_path: Path) -> None:
    """User-defined intermediate subclasses must still get per-subclass containers.

    Regression guard for the dir(base_class) -> allow-list refactor: when a
    user subclasses Incorporator once (without overriding inc_dict /
    _schema_union / _incorp_kwargs), the seed values live on Incorporator
    itself, not on the intermediate class.  The DynamicModel built from
    Alpha or Beta must still get its OWN container instances — getattr
    walks the MRO and finds Incorporator's seed; vars(base_class) would
    silently miss it and leak shared state across siblings.
    """
    json_a = tmp_path / "a.json"
    json_b = tmp_path / "b.json"
    json_a.write_text(json.dumps([{"alpha_only": 1}, {"alpha_only": 2}]), encoding="utf-8")
    json_b.write_text(json.dumps([{"beta_only": 3}, {"beta_only": 4}]), encoding="utf-8")

    class UserBase(Incorporator):
        """Intermediate subclass: no ClassVar overrides."""

    class Alpha(UserBase):
        pass

    class Beta(UserBase):
        pass

    for cls in (UserBase, Alpha, Beta):
        if "_schema_union" in cls.__dict__:
            del cls._schema_union  # type: ignore[attr-defined]

    alpha_list = await Alpha.incorp(inc_file=str(json_a))
    beta_list = await Beta.incorp(inc_file=str(json_b))

    alpha_union = Alpha._schema_union  # type: ignore[attr-defined]
    beta_union = Beta._schema_union  # type: ignore[attr-defined]

    assert "alpha_only" in alpha_union
    assert "beta_only" not in alpha_union
    assert "beta_only" in beta_union
    assert "alpha_only" not in beta_union

    # DynamicModel containers must be isolated even when both inherit from
    # the same UserBase (which itself inherits Incorporator's seeds).
    alpha_dyn = alpha_list._model_class  # type: ignore[attr-defined]
    beta_dyn = beta_list._model_class  # type: ignore[attr-defined]
    assert alpha_dyn.inc_dict is not beta_dyn.inc_dict
    assert alpha_dyn._schema_union is not beta_dyn._schema_union


# ==========================================
# 5. DYNAMIC MODEL IN-STATE EXPORT
# ==========================================


@pytest.mark.asyncio
async def test_dynamic_model_in_state_export(tmp_path: Path) -> None:
    """In-state export (no file_path arg) must write all records from cls.inc_dict."""
    json_file = tmp_path / "data.json"
    json_file.write_text(
        json.dumps([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]),
        encoding="utf-8",
    )

    class InStateModel(Incorporator):
        pass

    await InStateModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")

    out_path = tmp_path / "out.json"
    # In-state mode: instance is the output path, data comes from cls.inc_dict
    await InStateModel.export(instance=str(out_path))

    content = out_path.read_text(encoding="utf-8")
    assert "Alice" in content
    assert "Bob" in content


# ==========================================
# 5. A-F-4: effective_conv cache invalidation
# ==========================================


@pytest.mark.asyncio
async def test_effective_conv_cache_attaches_after_first_incorp(tmp_path: Path) -> None:
    """A-F-4: the per-class ``_cached_effective_conv`` slot is populated after ``incorp()``.

    Smoke test that the cache wiring lands at the call site in
    ``incorporator/schema/factory.py``.  Detailed invalidation
    behaviour is exercised indirectly by the existing schema_union
    tests above (a stale cache would break auto-coercion on a new
    field's second wave).
    """
    json_file = tmp_path / "cache_test.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]), encoding="utf-8")

    class CacheTestModel(Incorporator):
        pass

    # Cache slot does not exist before first incorp.
    assert getattr(CacheTestModel, "_cached_effective_conv", None) is None

    await CacheTestModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")

    # Cache attached on the class after the call.
    cached = getattr(CacheTestModel, "_cached_effective_conv", None)
    assert cached is not None, "expected _cached_effective_conv to be set after incorp()"
    key, effective_conv = cached
    # Cache key is a 3-tuple of (conv_dict object itself, len(schema_union),
    # declared_field_names). The key HOLDS conv_dict (not its id()) so the
    # cache keeps a strong reference alive — see D3-01 in factory.py.
    assert isinstance(key, tuple) and len(key) == 3


@pytest.mark.asyncio
async def test_effective_conv_cache_invalidates_on_schema_union_growth(tmp_path: Path) -> None:
    """A-F-4: schema_union growth (new field appearing) invalidates the cache.

    The cache key embeds ``len(schema_union)``, so a wave that introduces
    a previously-unseen field bumps the cache key and forces a rebuild
    of ``effective_conv``.  This test asserts the cache KEY changes
    between two waves where the second adds a new field.

    A stale cache here would mean the new field gets no auto-coercion
    on the second wave — its value would land as a string instead of
    being coerced by the freshly-synthesised ``inc()`` entry.
    """
    json_v1 = tmp_path / "v1.json"
    json_v2 = tmp_path / "v2.json"
    # Both files have ≥ 2 rows so the schema_union path runs (single-row
    # files take the is_single fast path that skips _schema_union).
    json_v1.write_text(json.dumps([{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]), encoding="utf-8")
    json_v2.write_text(
        json.dumps([{"id": 3, "name": "z", "extra": 42}, {"id": 4, "name": "w", "extra": 43}]),
        encoding="utf-8",
    )

    class GrowingModel(Incorporator):
        pass

    await GrowingModel.incorp(inc_file=str(json_v1), inc_code="id", inc_name="name")
    cached_after_v1 = getattr(GrowingModel, "_cached_effective_conv", None)
    assert cached_after_v1 is not None
    key_v1 = cached_after_v1[0]

    await GrowingModel.incorp(inc_file=str(json_v2), inc_code="id", inc_name="name")
    cached_after_v2 = getattr(GrowingModel, "_cached_effective_conv", None)
    assert cached_after_v2 is not None
    key_v2 = cached_after_v2[0]

    # Key embeds len(schema_union); v2 added the "extra" field so the
    # union grew → key differs → cache rebuilt.
    assert key_v1 != key_v2, f"cache key should change when schema_union grows; got identical keys {key_v1} == {key_v2}"

    # Behavioral check: the cached TUPLE-AS-A-WHOLE must be a fresh
    # object after the rebuild.  The cache-write site at factory.py
    # is ``setattr(cls, "_cached_effective_conv", (cache_key, effective_conv))``
    # — a fresh tuple is created every miss.  A hypothetical stale-cache
    # bug that reused the old tuple identity (e.g. mutating a stored
    # tuple — impossible for real tuples, but the ``Tuple`` protocol
    # could be subclassed to behave that way) would fail this check.
    #
    # We deliberately do NOT assert on ``effective_conv_v1 is not
    # effective_conv_v2`` because ``_expand_conv_dict_with_schema_union``
    # can return ``None`` when conv_dict is None and no schema_union
    # field needs synthesised coercion — in that case both calls return
    # ``None`` and identity-comparison gives the wrong answer.  The
    # tuple-identity check captures the same intent without that edge.
    assert cached_after_v1 is not cached_after_v2, (
        "cache must store a fresh tuple on rebuild — a stale tuple with "
        "a new key would silently drop the new schema_union expansion"
    )


@pytest.mark.asyncio
async def test_effective_conv_cache_misses_on_different_conv_dict_object(tmp_path: Path) -> None:
    """D3-01: a different conv_dict object must miss the cache, even with an equal-shaped key.

    Pre-fix, the cache key embedded ``id(conv_dict)``. Once the first
    ``conv_dict`` object is garbage-collected, a *different* dict can be
    allocated at the recycled address, producing an equal-looking key
    (same id, same len(schema_union), same declared_field_names) — a
    false cache hit that would silently keep running the OLD converters
    against NEW data. Real address recycling is CPython-allocator-state
    dependent and not deterministic to force from a test, so this test
    constructs the failure condition directly: it forges a stale cache
    entry on the class whose key component is an ``int`` equal to
    ``id(conv_dict_b)`` (exactly what an ``id()``-recycled pre-fix key
    would look like) paired with dict A's stale converters, then calls
    ``incorp()`` with dict B. Under the OLD ``==``-on-whole-tuple hit
    check this forged entry would compare equal and serve dict A's
    converters; the fix's identity check on the actual object must miss.
    """
    from incorporator.schema.converters import calc

    json_file = tmp_path / "cache_identity.json"
    json_file.write_text(
        json.dumps(
            [
                {"id": 1, "name": "a", "value": "10"},
                {"id": 2, "name": "b", "value": "20"},
            ]
        ),
        encoding="utf-8",
    )

    class CacheIdentityModel(Incorporator):
        pass

    conv_dict_b: dict[str, object] = {"value": calc(lambda v: int(v) * 100, "value", target_type=int)}

    # Forge a stale cache entry as a pre-fix id()-keyed cache would have left
    # it: key component 0 equal to id(conv_dict_b) (the "recycled address"),
    # paired with a stale ×1 effective_conv that must NOT be served.
    stale_effective_conv = {"value": calc(lambda v: int(v) * 1, "value", target_type=int)}
    schema_union = getattr(CacheIdentityModel, "_schema_union", {})
    declared_field_names = frozenset(CacheIdentityModel.model_fields.keys())
    forged_key = (id(conv_dict_b), len(schema_union), declared_field_names)
    CacheIdentityModel._cached_effective_conv = (forged_key, stale_effective_conv)  # type: ignore[attr-defined]

    result_b = await CacheIdentityModel.incorp(
        inc_file=str(json_file), inc_code="id", inc_name="name", conv_dict=conv_dict_b
    )
    values_b = sorted(inst.value for inst in result_b)

    assert values_b == [1000, 2000], (
        f"expected dict B's ×100 converter to apply; got {values_b} — a stale "
        "cache hit would incorrectly reuse the forged entry's ×1 converter (== [10, 20])"
    )


@pytest.mark.asyncio
async def test_effective_conv_cache_survives_fjord_inflow_fresh_dict_per_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3-01: fjord's per-tick fresh merged conv_dict must not false-hit the cache.

    Real-world trigger verified in ``pipeline/fjord.py::_seed_one_source``:
    ``merged_conv = {**base_params.get('conv_dict', {}), **extra_conv}``
    builds a BRAND NEW dict on every tick when an ``inflow(state)`` callable
    is active (see ``examples/09-nascar-fantasy-fjord/inflow.py``). The
    previous tick's dict becomes garbage immediately after ``incorp()``
    returns, so under the old id()-keyed cache, address recycling across
    ticks could silently keep serving a stale converter.

    Real CPython address recycling is allocator-state dependent (verified
    empirically — not reliably forced by ``del`` + ``gc.collect()`` once
    other allocations happen in between), so this test drives tick 1 through
    the REAL ``_seed_one_source`` fjord code path, then wraps
    ``factory._effective_conv_cache_key`` so that on tick 2's call it
    captures the ACTUAL merged ``conv_dict`` object ``_seed_one_source``
    builds internally and forges the class's ``_cached_effective_conv``
    slot to look exactly like a pre-fix ``id()``-recycled hit for that
    real object (identity slot == ``id(that real merged dict)``, paired
    with tick 1's now-stale ×1 converter) BEFORE the cache-key comparison
    runs. Tick 2's own ×100 converter must still apply through
    ``_seed_one_source`` unmodified.
    """
    from incorporator.pipeline.fjord import _seed_one_source
    from incorporator.schema import factory
    from incorporator.schema.converters import calc

    json_file = tmp_path / "fjord_tick.json"
    json_file.write_text(
        json.dumps(
            [
                {"id": 1, "name": "a", "value": "5"},
                {"id": 2, "name": "b", "value": "7"},
            ]
        ),
        encoding="utf-8",
    )

    class FjordTickModel(Incorporator):
        pass

    entry = {
        "cls": FjordTickModel,
        "incorp_params": {"inc_file": str(json_file), "inc_code": "id", "inc_name": "name"},
    }

    def make_inflow(multiplier: int) -> Callable[[dict[str, object]], Any]:
        def inflow(state: dict[str, object]) -> dict[str, object]:
            # Fresh dict every call — mirrors _seed_one_source's per-tick
            # ``{**base_params.get('conv_dict', {}), **extra_conv}`` merge.
            return {
                "FjordTickModel": {
                    "conv_dict": {"value": calc(lambda v, m=multiplier: int(v) * m, "value", target_type=int)}
                }
            }

        return inflow

    result_tick1 = await _seed_one_source(entry, state={}, inflow_callable=make_inflow(1))
    values_tick1 = sorted(inst.value for inst in result_tick1)
    assert values_tick1 == [5, 7]

    tick1_stale_conv = {"value": calc(lambda v: int(v) * 1, "value", target_type=int)}
    real_cache_key = factory._effective_conv_cache_key

    def forging_cache_key(
        conv_dict: dict[str, Any] | None,
        schema_union: Any,
        declared_field_names: frozenset[str],
    ) -> tuple[Any, int, frozenset[str]]:
        # Fires once, on tick 2's real merged conv_dict — forge a stale
        # cache entry keyed to THIS exact object's id(), simulating what a
        # pre-fix id()-recycled cache hit would have looked like for it.
        if conv_dict is not None:
            forged_key = (id(conv_dict), len(schema_union), declared_field_names)
            FjordTickModel._cached_effective_conv = (forged_key, tick1_stale_conv)  # type: ignore[attr-defined]
        return real_cache_key(conv_dict, schema_union, declared_field_names)

    monkeypatch.setattr(factory, "_effective_conv_cache_key", forging_cache_key)

    result_tick2 = await _seed_one_source(entry, state={}, inflow_callable=make_inflow(100))
    values_tick2 = sorted(inst.value for inst in result_tick2)

    assert values_tick2 == [500, 700], (
        f"expected tick 2's ×100 converter to apply; got {values_tick2} — a "
        "stale cache hit would incorrectly reuse tick 1's ×1 converter (== [5, 7])"
    )
