# Canal engineering ↔ pydantic v2 — v1.2.x review

**Branch:** `code-review` at `59af8c9` (== `main`, == current worktree —
zero diff; the audit applies to the v1.2.1 codebase)
**Companions:**
- [httpx-vs-canal-v1.2.1.md](httpx-vs-canal-v1.2.1.md) (httpx + tenacity)
- The deleted `canal_evaluation.md` and `canal_integration_audit.md`
  (recoverable from `eb5d96a^`) which closed F-3 and F-4 in v1.2.1

**Date:** 2026-05-27

---

## 1. Summary

1. **Is pydantic v2 used properly across the v1.2.x canal layer?** ✅
   Yes, with rare discipline. All 24 `BaseModel` classes are
   `frozen=True`. The `model_construct` vs `model_validate` boundary is
   correctly placed: 37 `model_construct` sites (all framework-internal
   hot paths), 5 `model_validate` sites (all external-input trust
   boundaries). No `Model(**dict)` raw construction in any hot loop.
   Every `TypeAdapter` instance is cached.

2. **Did the v1.2.1 cycle introduce regressions?** ❌ No. The work that
   landed (A-F-3 TypeAdapter benchmark, A-F-4 batched
   `TypeAdapter(list[Cls]).validate_python`, H3 reshape of
   `is_garbage_value`, H4 `failed_sources` as derived property,
   `model_construct` rollout to ~37 sites) tightened the surface
   without breaking any v1.2.0 invariant.

3. **Are there pydantic v2 features the newer strategies could adopt?**
   One worth doing: **`slots=True` on the hot-path telemetry models**
   (Wave, Tide, RejectEntry). They're already `frozen=True`; adding
   slots saves ~56 bytes/instance and tightens the contract. At
   long-running-daemon scale this is a real memory win. **Finding 1**
   below.

4. **Are there design choices that look gappy but are actually
   deliberate?** Four:
   - No strict mode on CLI configs (env-var interpolation needs coercion)
   - `failed_sources` as `@property` not `@computed_field` (back-compat alias)
   - `extra='allow'` on three CLI config classes (soft recommendation to revisit, not now)
   - No named type aliases (NonNegativeInt, PositiveFloat) — low-ROI repo-wide churn

**Five findings** total. One concrete code change (orchestrator-routed
for a future session). Four documented-as-deliberate or deferred. This
is the narrowest of the three v1.2.x reviews — precisely because the
v1.2.1 cycle already absorbed the major pydantic work.

---

## 2. What the prior audits already closed (v1.2.1 reference)

| Item | Source | Status |
|---|---|---|
| A-F-3: TypeAdapter-vs-per-row benchmark | `tests/test_validate_batch_vs_per_row.py` | ✅ Proven 1.3-2.0× speedup |
| A-F-4: Batched `TypeAdapter(list[Cls]).validate_python` | factory.py:418 + `_BATCH_INSERT_MODE` ClassVar gate | ✅ ~100-200 ns/row saved |
| H3 reshape: `is_garbage_value` null pre-check | All seven converter/extractor sites | ✅ Lambda-free idiom |
| H4: `failed_sources` as derived `@property` | base.py / rejects.py:106 | ✅ `rejects` is canonical |
| `model_construct` rollout to internal sites | ~37 call sites | ✅ Hot paths zero-validation |
| D2a/D2b: Pydantic-backed pipeline configs | cli/_pipeline_config.py + cli/validate.py | ✅ Source of truth |

This review picks up at the **still-open** pydantic surface and asks
which items are worth doing now versus documenting as deliberate.

---

## 3. Per-area sweep

### 3.1 Model definitions inventory

**24 BaseModel classes** in the package. Distribution:

| Directory | Count | Examples |
|---|---|---|
| `base.py` | 2 | Incorporator + mixin |
| `cli/_pipeline_config.py` | 3 | StreamConfig, FjordConfig, WebhookConfig |
| `io/penstock.py` | 1 | Penstock base |
| `observability/tideweaver/` | 10 | Gate × 4, Penstock × 5, Reservoir, Spillway × 3, SurgeBarrier, FlowControl, Current, Watershed, TuningHint, TuningReport |
| `observability/wave.py` | 1 | Wave |
| `observability/tideweaver/tide.py` | 1 | Tide |
| `rejects.py` | 1 | RejectEntry |
| `schema/builder.py` | 2 | dynamic-class machinery |

**Frozen discipline: 24/24** ✅. Every BaseModel uses
`model_config = ConfigDict(frozen=True)`. Documented locations:

- [rejects.py:106](../../incorporator/rejects.py)
- [wave.py:72](../../incorporator/observability/wave.py)
- [tide.py:86](../../incorporator/observability/tideweaver/tide.py)
- [flow.py:81/142/227/245/293/603](../../incorporator/observability/tideweaver/flow.py)
- [current.py:66](../../incorporator/observability/tideweaver/current.py),
  [watershed.py:173](../../incorporator/observability/tideweaver/watershed.py),
  [architect.py:942/975](../../incorporator/observability/tideweaver/architect.py)
- [cli/_pipeline_config.py:44/60/101](../../incorporator/cli/_pipeline_config.py)

**Slots discipline:** mixed but with intentional design.

- **Pydantic models do NOT use slots.** Pydantic v2 supports
  `ConfigDict(frozen=True, slots=True)` but it's nowhere in the repo.
- **`CurrentOutcome` uses slots** as a `@dataclass(frozen=True,
  slots=True)` at [current_outcome.py:14](../../incorporator/observability/tideweaver/current_outcome.py)
  — the documented choice for 5× cheaper construction (200 ns vs
  500 ns–2 µs).
- **`GateContext` / `SurgeContext`** are `@dataclass(frozen=True)` (no
  slots) at [flow.py:48, 63](../../incorporator/observability/tideweaver/flow.py)
  — these are constructed once per scheduler pass, not per Wave, so the
  slots win is marginal.

This split is the launchpad for Finding 1.

**`extra` policy:**

| Policy | Sites | Rationale |
|---|---|---|
| `extra='allow'` | cli/_pipeline_config.py:44/60/101, schema/builder.py:360 | Messy APIs (webhook headers), dynamic field preservation (dynamic-class) |
| `extra='forbid'` | current.py:66, watershed.py:173 | Strict config-layer validation |
| Default `extra='ignore'` (v2 default) | Everywhere else | |

### 3.2 Construction patterns — `model_construct` vs `model_validate`

**37 `model_construct` sites** — all framework-internal trust
boundaries:

| Type | Sites |
|---|---|
| Wave | chunked.py:111/149, fjord.py:345/361/377, _shared.py:49/66, _stateful_shim.py:102/119/134, _outflow.py:431 |
| Tide | scheduler.py:491 |
| RejectEntry | scheduler.py:551/566/601/625, fjord.py:118, fetch.py:78 |
| TuningHint / TuningReport | architect.py (18 sites, lines 1048-1565) |

**5 `model_validate` sites** — all external-input trust boundaries:

| Type | Site | Source of input |
|---|---|---|
| StreamConfig | cli/_pipeline_config.py:137 | pipeline.json / YAML |
| FjordConfig | cli/_pipeline_config.py:138 | pipeline.json / YAML |
| derived_cls (user override) | _outflow.py:262 | Per-row when user supplies override class |
| FlowControl | observability/tideweaver/config.py:407 | watershed.json |

**No `Model(**dict)` raw construction** in any hot loop. Either
validated (trust boundary) or `model_construct` (framework-internal).
Boundary placement is correct.

### 3.3 TypeAdapter & schema cache

**Memoization is comprehensive.** All `TypeAdapter` instances are
cached:

- `_get_cached_adapter(actual_type)` with `@functools.lru_cache(maxsize=4096)`
  at [converters.py:343-356](../../incorporator/schema/converters.py)
- Module-level singleton `TypeAdapter`s for bool / datetime / int /
  float / str at converters.py:331-335 (`RANKED_CONVERTERS`)
- Per-dynamic-class `_cached_type_adapter` ClassVar at
  [factory.py:406-411](../../incorporator/schema/factory.py); built
  once per inferred schema; reused across all `incorp()` calls for that
  shape
- Batch validation via `adapter.validate_python(transformed_data)` at
  [factory.py:418](../../incorporator/schema/factory.py) — all rows in
  one call, one `ValidationError` (not per-row)

**Schema cache:** `SCHEMA_REGISTRY` at
[builder.py:30](../../incorporator/schema/builder.py) is an
`OrderedDict` keyed on `(model_name, frozenset(field_types),
id(base_class))` with LRU eviction at `MAX_REGISTRY_SIZE=1000`.
`move_to_end` on cache hit; `popitem(last=False)` on overflow. Hit/miss
tracked via size-delta snapshot at factory.py:348-355 →
`_last_schema_cache_hit` ClassVar.

**No loose `TypeAdapter(...)` construction in any hot loop.** Confirmed.

### 3.4 Validators & serializers

**Six `@model_validator` sites** — all on cross-field invariants:

| Site | Mode | Purpose |
|---|---|---|
| StreamConfig._require_source_key | after | Source key presence |
| StreamConfig._outflow_requires_stateful_polling | after | Coupling enforcement |
| BackpressurePenstock._check_rate_ordering | after | `min_rate < max_rate` |
| Stream._reject_stateful_polling | before | Rejects `stateful_polling=True` inside Tideweaver |
| Edge._gate_mode_shorthand | before | Translates `gate_mode` → `FlowControl` |
| Watershed._validate_graph | after | Uniqueness, window order, cycle checks |

**Field validators:** none in production code (one in `tests/test_bulk_inc_dict.py` only).

**Two serializers, both intentional:**

- `@field_serializer("current_outcomes")` at
  [tide.py:118](../../incorporator/observability/tideweaver/tide.py) —
  converts `CurrentOutcome` dataclass list via `dataclasses.asdict()`
  for `model_dump(mode="json")`. Pydantic doesn't auto-serialize
  arbitrary dataclasses; this is the workaround.
- `@model_serializer(mode="wrap")` at
  [flow.py:612](../../incorporator/observability/tideweaver/flow.py) —
  `FlowControl._drop_default_observer` strips the default
  `NullObserver` from serialized output. Keeps `watershed.json` round
  trips clean.

**Zero `json_encoders` (v1 pattern)** anywhere.

### 3.5 Discriminated unions

Four discriminated unions at
[flow.py:571-586](../../incorporator/observability/tideweaver/flow.py),
all using the v2 `Annotated[Union[...], Field(discriminator="type")]`
pattern:

```python
_GateUnion       = Annotated[HardLock | SoftPass | Weir,
                              Field(discriminator="type")]
_PenstockUnion   = Annotated[SustainedPenstock | BurstPenstock | WindowPenstock |
                              BackpressurePenstock | SignalPenstock,
                              Field(discriminator="type")]
_SpillwayUnion   = Annotated[DropOldest | RaiseOverflow | ExportToArchive,
                              Field(discriminator="type")]
_ObserverUnion   = Annotated[NullObserver | LoggingObserver | SignalObserver,
                              Field(discriminator="type")]
```

Each member carries a `type: Literal[...]` field. **O(1) dispatch on
deserialization** (not try-each-arm). Used by
`FlowControl.model_validate(raw_flow)` for `watershed.json` parsing.
Correct v2 pattern.

### 3.6 Configuration / extras

| Feature | Status | Rationale |
|---|---|---|
| `arbitrary_types_allowed=True` | Used on Tide (tide.py:86) | Required for `CurrentOutcome` dataclass field |
| `strict=True` | Zero usage | Deliberate — env-var interpolation yields stringy numbers |
| `pydantic.StrictInt` / `StrictStr` / `StrictBool` | Zero usage | Same |
| `@computed_field` | Zero usage | `failed_sources` is a back-compat `@property` (H4); surfacing in `model_dump()` would duplicate `rejects` |
| `model_json_schema()` | Zero calls | No OpenAPI surface; ETL library |
| Generic `BaseModel`s | Zero usage | All BaseModel classes concrete |
| v1 leftover APIs | Zero | Migration complete: no `.dict()`, `.json()`, `parse_obj_as`, `root_validator`, `validator`, `Config` inner class, `.copy()` |

---

## 4. Confirmed clean — pydantic v2 discipline

| Area | Status | Evidence |
|---|---|---|
| Frozen discipline on value types | ✅ 24/24 BaseModel + 2 frozen dataclasses + 1 frozen-slotted dataclass | All cited above |
| `model_construct` for trust-internal writes | ✅ 37 sites, all hot-path framework-internal | Wave/Tide/RejectEntry/TuningHint |
| `model_validate` only at trust boundaries | ✅ 5 sites, all external input | Configs, override rows, watershed.json |
| `TypeAdapter` memoization | ✅ `lru_cache(4096)` + `_cached_type_adapter` ClassVar + module singletons | converters.py, factory.py |
| Batch validation | ✅ `TypeAdapter(list[Cls]).validate_python` at factory.py:418 | 1.3-2.0× benchmark |
| Discriminated unions | ✅ v2 `Annotated[..., Field(discriminator=...)]` pattern | flow.py:571-586 |
| Schema cache | ✅ `SCHEMA_REGISTRY` OrderedDict with LRU eviction | builder.py:30 |
| `model_dump_json()` preferred over `json.dumps(model_dump())` | ✅ Where it pays | cli/runners.py:163, cli/tideweaver.py:93 |
| `@field_serializer` for non-Pydantic types | ✅ Tide → CurrentOutcome via `dataclasses.asdict` | tide.py:118 |
| `@model_serializer(mode="wrap")` for default-stripping | ✅ FlowControl strips default NullObserver | flow.py:612 |
| Zero v1 leftover APIs | ✅ Migration complete | All seven v1 patterns absent |

---

## 5. Findings

### Finding 1 — `slots=True` on hot-path BaseModels (INVALIDATED 2026-05-27)

**Original framing (wrong).** This finding claimed `Wave`, `Tide`, and
`RejectEntry` could adopt `model_config = ConfigDict(frozen=True,
slots=True)` to save ~56 bytes/instance and gain a no-`__dict__`
contract.

**What Bundle E discovered.** Implementation attempt landed as commit
`d12da06` (test scaffolding only). The investigation revealed two
fatal problems:

1. **Pydantic 2.13.3 (the pinned version) does not include `slots` as
   a key in `ConfigDict`.** mypy strict reports
   `[typeddict-unknown-key]` at the three call sites. The `slots` key
   was present in some early v2 documentation but is absent from the
   `ConfigDict` TypedDict in 2.13.3 — confirmed empirically by
   reading `.venv/Lib/site-packages/pydantic/config.py`.
2. **`BaseModel.__slots__` already declares `'__dict__'`** as one of
   its slots: `('__dict__', '__pydantic_fields_set__',
   '__pydantic_extra__', '__pydantic_private__')` at
   `pydantic/main.py:251`. Every `BaseModel` subclass inherits this.
   The "no `__dict__`" contract this finding promised is structurally
   impossible for any `BaseModel` subclass — slots config or not.

**What landed instead.** Commit `d12da06` ships `tests/test_value_type_slots.py`
(10 contract tests asserting actual Pydantic v2 invariants — frozen
behavior, `model_construct` correctness, Tide `current_outcomes`
serializer round-trip) plus `tests/benchmarks/test_telemetry_construction.py`
(informational baseline benchmark). The three source files
(`wave.py`, `tide.py`, `rejects.py`) were reverted to their original
state.

**Alternative path NOT pursued.** Switching `Wave` / `Tide` /
`RejectEntry` to `pydantic.dataclasses.dataclass(slots=True)` would be
a substantially larger architectural change — different construction
API (no `model_construct`), different `model_dump` semantics,
different `@field_serializer` behavior. It would touch the 37
framework-internal `model_construct` call sites catalogued in §3.2 of
this doc. If the memory savings ever become a real production
constraint, that migration should be scoped as a standalone
architectural decision with its own benchmark justification — not as
a side effect of this finding.

**Status:** Closed as "premise invalidated, no code action." Re-open
only if (a) a future Pydantic pin bump restores `slots` in
`ConfigDict` AND removes `'__dict__'` from `BaseModel.__slots__`
(unlikely), or (b) a profiling pass identifies Wave/Tide/RejectEntry
construction as a real bottleneck warranting the dataclass migration.

### Finding 2 — `strict` mode on CLI configs (documented as deliberate)

`StreamConfig`, `FjordConfig`, and the watershed loader use standard
(coercive) validation. Env-var interpolation (`${VAR}` → string) and
human-edited JSON both commonly yield stringy numbers (`"interval":
"3.0"`). Strict mode would force users to pre-cast or quote-strip in
templates — a worse ergonomic trade.

**Action:** None. Documented here.

### Finding 3 — `extra='allow'` on Stream/Fjord configs (soft, deferred)

`StreamConfig`, `FjordConfig`, and `WebhookConfig` all set
`extra='allow'`. The rationale ("messy APIs, dynamic field
preservation") applies cleanly to `WebhookConfig` (provider-specific
headers/auth) and to `schema/builder.py:360` (dynamic-class
machinery).

For `StreamConfig` / `FjordConfig` it's weaker — these are the user's
own pipeline.json files, and typos silently flow into `extra` instead
of producing a validation error.

**Soft recommendation:** Consider tightening Stream/Fjord configs to
`extra='forbid'` in a future cycle. Trade-off: breaking change for
any user who put extra keys in their pipeline.json. An opt-in env var
(`INCORPORATOR_STRICT_CONFIG_KEYS=1`) makes it zero-risk by default.

**Action:** Defer. Document here; do not bundle with Finding 1.

### Finding 4 — `failed_sources` as `@property` (documented as deliberate)

Per AGENTS.md H4: `IncorporatorList.failed_sources` is a back-compat
alias. Converting it to `@computed_field` would duplicate `rejects`
in `model_dump()` output and muddy the canonical structured-DLQ
narrative (the whole point of v1.2.0's RejectEntry work).

**Action:** None. Documented here.

### Finding 5 — Named type aliases (style only, low ROI)

`Field(gt=0.0, ...)` / `Field(ge=0, le=1024, ...)` constraints are
written inline at every use site (Reservoir.depth,
SurgeBarrier.threshold_multiple, BackpressurePenstock.min_rate, etc.).
Pydantic ships `NonNegativeInt`, `PositiveFloat`, `StrictInt`, etc.
that could DRY the repeated `Field(ge=0, ...)` pattern into named
aliases.

**Trade-off:** Adds an import for marginal readability. Most `Field()`
calls also carry `description=` / `default=` / `examples=` args, so
the alias only removes the constraint half. Touching every model file
for style is a lot of churn for low ROI.

**Action:** Note; do not pursue.

---

## 6. Findings consolidated

| # | Area | Finding | Action |
|---|---|---|---|
| 1 | Hot-path models | `slots=True` absent on Wave / Tide / RejectEntry | INVALIDATED (see §5): no `slots` key in ConfigDict 2.13.3; `BaseModel.__slots__` declares `__dict__`; closed, no code action |
| 2 | CLI configs | Strict mode absent (no `strict=True`, no `StrictInt`) | Document as deliberate (env-var coercion) |
| 3 | CLI configs | `extra='allow'` on Stream/Fjord/Webhook configs | Defer; soft recommend opt-in `extra='forbid'` |
| 4 | `failed_sources` | Plain `@property`, not `@computed_field` | Document as deliberate (back-compat alias) |
| 5 | Constraint style | Inline `Field(ge=0, ...)` vs `NonNegativeInt` aliases | Note; low-ROI churn, do not pursue |

### Bundle map

- **Finding 1 (slots)**: INVALIDATED (see §5) — closed with no code
  action; only test scaffolding (commit `d12da06`) landed
- **Findings 2, 3, 4, 5**: captured in this doc; no code action this
  cycle

---

## 7. Out of scope (and the why)

- **Pydantic V1 → V2 migration items.** All done in earlier cycles.
- **`model_json_schema()` export.** No OpenAPI surface for an ETL
  library.
- **Generic `Incorporator[T]`.** Major architectural change; would
  catch more mypy bugs but at high cost for unclear benefit. Not
  requested.
- **Re-validating internal writes.** The 37 `model_construct` sites
  are correct; swapping for `model_validate` would tax hot paths for
  no gain.
- **Replacing `_cached_type_adapter` ClassVar.** Benchmark-validated;
  do not touch.
- **Adding `strict=True` to BaseModel hierarchy globally.** Not
  appropriate for an ETL library that ingests messy data — the whole
  `is_garbage_value` design (H3) is the deliberate counter-approach.

## 8. Summary

The pydantic surface in v1.2.x is already very well disciplined.
Frozen everywhere (24/24), `model_construct`-at-internal-writes
discipline is uniform, every `TypeAdapter` is cached, batched
validation is benchmark-validated, discriminated unions use the
correct v2 syntax, no v1 leftover APIs. One concrete code change
(`slots=True` on the three hot-path telemetry types) is recommended;
everything else is either documented-as-deliberate or deferred
pending stakeholder input.

This is the narrowest of the three v1.2.x reviews because the
v1.2.1 cycle already absorbed the major pydantic work
(`_BATCH_INSERT_MODE`, `model_construct` rollout, `is_garbage_value`
null pre-check, `failed_sources`-as-property). Future cycles can
revisit Finding 3 (`extra='forbid'`) once usage data tells us whether
typo-catching is worth the breakage risk.
