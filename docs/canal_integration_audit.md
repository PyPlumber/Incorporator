# Canal ↔ Tideweaver Integration Audit

**Branch:** `workflow` at `f8f9110` (== `v1.2.0` tag + one docs commit)
**Companion to:** [docs/canal_evaluation.md](canal_evaluation.md) (commit `f8f9110`)
**Date:** 2026-05-22

Tideweaver landed in v1.1.3; the per-edge canal toolkit (FlowControl + Gate / SurgeBarrier / Penstock / Reservoir / Spillway / FlowObserver) landed in v1.2.0. Two systems built at different times — this audit asks whether they've been intertwined as well as they can be, and where the seams remain.

---

## 1. Summary

1. **Does Tideweaver use every canal function it can?** ✅ At the scheduler ↔ canal boundary, yes — every hook is called from the scheduler at exactly the right site (table in §3). At the verb ↔ canal boundary, **no** — two real gaps remain (§4): canal-layer skips don't surface as `RejectEntry` records, and `stream()` / `fjord()` don't adopt the `Reservoir` / `Spillway` model for their own wave history.
2. **Have we intertwined the two systems as best as possible?** ⚠️ Mostly. The scheduler↔canal seam is tight (10/10 hook coverage, narrow contexts honored). The verb↔canal seam is the loose one: `RejectEntry` is HTTP-only (fetch.py:45-52), and the Reservoir/Spillway abstraction stops at the Tideweaver edge layer instead of extending down into the stream chunking and fjord daemons where it could also bound memory.
3. **Are there top-down structural improvements?** Small ones. The import DAG is acyclic and five-layer stratified — no structural reshaping needed. Two opportunities: (a) `flow.py` (685 LOC, 18 classes) could split into four files for navigation; (b) `FlowObserver` hooks still take `scheduler: "Tideweaver"` as their first arg (flow.py:364-400) — the sole A1 narrow-context violation. Neither is urgent.
4. **Are there base-Incorporator verb-level changes that improve scaling under large instance counts per wave?** Yes, three candidates ranked by expected impact (§6). The highest-impact one — `TypeAdapter(List[Cls]).validate_python(rows)` batch validation — needs a benchmark first because the existing comment at [factory.py:305-308](../incorporator/schema/factory.py) chose per-row `model_validate` for a documented Pydantic-schema-cache reason. The second highest — incremental snapshot maintenance — changes the strong-ref contract. The third is purely a doc warning around WeakValueDictionary churn ceilings.

**One urgent finding is promoted into a separate concrete plan** (§7-F-1): plumb canal skip reasons into `RejectEntry` so users get a structured-DLQ view of throttled / gated / surge-halted sources, not just `Tide` log records.

---

## 2. Methodology

### Sources

- Three parallel Explore agents covering: (A) canal-integration completeness, (B) top-down structure, (C) verb-level scaling under high-row-count
- First-hand verification reads of scheduler.py:380-549, fetch.py:30-110, factory.py:290-313
- Cross-reference against the v1.2.0 commit list (`git log v1.1.3..v1.2.0 --oneline`)

### What counts as "integration completeness"

For each canal primitive (Gate, SurgeBarrier, Penstock, Reservoir, Spillway, FlowObserver, FlowState), three checkboxes:

- ✅ **Reach** — the scheduler can invoke the primitive (i.e., it's wired in)
- ✅ **Coverage** — every documented hook is called from the right scheduler site, no missing fires
- ✅ **Contract** — the call respects the documented contract (e.g., bypass skips BOTH gate AND penstock)

For verb↔canal:
- ✅ **HTTP path** — `incorp` / `refresh` / `stream` / `fjord` route through `BoundPenstock.acquire` for outbound throttling
- ❓ **Wave-history path** — does anything outside Tideweaver use Reservoir / Spillway?
- ❓ **Observability path** — do canal-layer skips become structured records (`RejectEntry`)?

---

## 3. Canal integration completeness (scheduler ↔ canal)

Every canal hook has a verified call site in [incorporator/observability/tideweaver/scheduler.py](../incorporator/observability/tideweaver/scheduler.py):

| Primitive | Hook | Scheduler call site | Verdict |
|---|---|---|---|
| `Penstock` | `consume_reason(edge_state, flow, now)` | scheduler.py:416 | ✅ |
| `Penstock` | `post_consume(edge_state, now)` | scheduler.py:509 | ✅ |
| `Gate` | `gate_reason(GateContext)` | scheduler.py:404 | ✅ |
| `SurgeBarrier` | `is_tripped(SurgeContext)` | scheduler.py:387 | ✅ |
| `SurgeBarrier` | `action="bypass"` skips gate AND penstock | scheduler.py:395 (bypass set) + 508-510 (penstock + on_fire gated) | ✅ |
| `Reservoir` | `waves.append` + `popleft` (deque) | scheduler.py:528-530 | ✅ |
| `Spillway` | `overflow(edge_key, displaced_wave, count)` | scheduler.py:532 | ✅ |
| `FlowObserver` | `on_fire(scheduler, edge, wave_number)` | scheduler.py:511 | ✅ |
| `FlowObserver` | `on_skip(scheduler, edge, reason)` | scheduler.py:389, 392, 406, 418 | ✅ |
| `FlowObserver` | `on_spillway(scheduler, edge, displaced_wave, count)` | scheduler.py:536 | ✅ |
| `FlowObserver` | `on_reservoir_level(scheduler, edge, used, capacity)` | scheduler.py:540 | ✅ |
| `_EdgeState.flow_state: FlowState` | composed via i14 | scheduler.py:74 | ✅ |

**Verdict:** **10/10 hook coverage** at the scheduler boundary. Every canal primitive is reached, exercised at the right site, and the bypass contract is honored. The one observation worth flagging: `_last_consumed[edge_key]` is updated *unconditionally* (scheduler.py:498-502) — including for bypassed edges — but only `Penstock.post_consume` is skipped (508). This matches AGENTS.md's bypass spec exactly: "`_last_consumed` and `edge_state.flow_state.last_consumed_at` still update for bypassed edges — only the penstock-specific post-consumption is skipped."

---

## 4. Verb ↔ canal integration (the seams)

### 4.1 HTTP throttle path — clean

The unified Penstock primitive (G) serves outbound HTTP via `BoundPenstock.acquire`:

- `register_host_penstock(host, penstock)` registers per-host throttles ([io/penstock.py](../incorporator/io/penstock.py))
- `resolve_penstock(url)` routes inside [io/fetch.py:568, 577](../incorporator/io/fetch.py)
- `BoundPenstock.acquire()` is awaited before every outbound request (fetch.py:256)
- All four verbs (`incorp` / `refresh` / `stream` / `fjord`) route through `_process_single_source` → `bound_fetch` → `execute_request`, which honors the penstock

**Verdict:** ✅ The HTTP path is fully canal-integrated. The same `Penstock` hierarchy serves both layers (HTTP outbound + Tideweaver edge) via the single `Penstock.consume_reason` / `Penstock.acquire` pair.

### 4.2 Gap 1 — `RejectEntry` is HTTP-only

[incorporator/rejects.py:30-47](../incorporator/rejects.py) explicitly documents: *"Constructed at the framework's failure points (HTTP errors in `incorporator.io.fetch`, fjord seed errors in `incorporator.observability.pipeline.fjord`)"*. Notably **absent**: canal-layer skips.

The four scheduler skip-emit sites at scheduler.py:389/392/406/418 each emit:

```python
flow.observer.on_skip(self, edge, reason)
return reason, frozenset()
```

`reason` is one of: `"skip_ahead"`, `"surge_halted"`, `"awaiting_upstream"`, `"penstock_limited"`. None of them touch `IncorporatorList.rejects` or construct a `RejectEntry`. The information lives only in:

- the `Tide` log record's `skipped` field
- a `FlowObserver.on_skip(...)` callback (synchronous, opt-in)

**Impact:** A user running a Tideweaver with a SustainedPenstock that throttles their source has **no structured view** of which sources / waves got throttled. They have a Tide stream (verbose, per-pass) and an observer hook (which they must subclass), but no `wsList.rejects = [...]` they can iterate. This is the inverse of the canal-evaluation report's §5.3 finding from a different angle — *the routing tests don't assert on `rejects`* (which was the test-coverage gap) — but here the deeper reason is exposed: **there's nothing to assert on, because canal skips don't populate rejects in the first place.**

**Promoted to concrete plan F-1.**

### 4.3 Gap 2 — Stream / Fjord don't adopt Reservoir or Spillway

The `Reservoir(depth=N)` / `Spillway` abstraction lives only at the Tideweaver edge layer (`_EdgeState.waves: Deque[List[Any]]` at scheduler.py:528). Outside the scheduler:

- `stream()` accumulates chunks in a plain `list` ([io/fetch.py:354](../incorporator/io/fetch.py)) — there's no bounded wave history, no spillway behaviour
- `fjord()` daemon (outside Tideweaver) has no Penstock-style throttling of its flush cadence — it spins as fast as its own loop allows

This is **probably correct as a design**: `stream()` is paginator-driven (the page size IS the bound), and the standalone `fjord()` daemon has an explicit `interval=` arg. But the architectural symmetry is missing — a user reading "Reservoir bounds wave history with spillway overflow" might expect to apply that pattern to a stream's chunk buffer and find no public API for it. Not urgent; flagged as a stub in §7.

---

## 5. Top-down structural

### 5.1 Import DAG (acyclic, five-layer stratified)

```
tide.py (leaf)
  ↑
flow.py ←── io/penstock.py (cross-package; no reverse dep)
  ↑
current.py
  ↑
watershed.py
  ↑                          architect.py (standalone — no scheduler dep)
scheduler.py (top of run-loop layer)
  ↑
config.py (consumes all the above)
```

Five clean semantic layers (low → high):

1. **Value types** — `FlowState`, `GateContext`, `SurgeContext`, `Tide`, `RejectEntry`, `SourceRef`
2. **Per-edge strategies** — `Gate` subclasses, `Penstock` subclasses, `SurgeBarrier`, `Spillway` subclasses, `FlowObserver` subclasses
3. **Composition** — `FlowControl`, `Edge`
4. **Graph** — `Watershed`, `Current` / `Stream` / `Fjord` / `Export` / `CustomCurrent`
5. **Runtime** — `Tideweaver`, `Tide` emission

**Verdict:** Clean. No cycles, no reverse deps, no private internals (`_EdgeState`, `_state`, `_tick_wrapper`, `_last_consumed`) reached from outside the scheduler.

### 5.2 The one A1 leak — `FlowObserver` takes the scheduler

[incorporator/observability/tideweaver/flow.py:364-400](../incorporator/observability/tideweaver/flow.py):

```python
def on_fire(self, scheduler: "Tideweaver", edge: Tuple[str, str], wave_number: int) -> None:
def on_skip(self, scheduler: "Tideweaver", edge: Tuple[str, str], reason: str) -> None:
def on_spillway(self, scheduler: "Tideweaver", edge: Tuple[str, str], displaced_wave: object, overflow_count: int) -> None:
def on_reservoir_level(self, scheduler: "Tideweaver", edge: Tuple[str, str], used: int, capacity: int) -> None:
```

This is the **only** place the A1 narrow-context discipline isn't followed. Per the canal-evaluation report, the contract was pinned at commit `47127ee` ("docs(api): pin FlowObserver hook stability contract") — so the leak is deliberate. The trade-off: A1 elsewhere lets strategy classes be unit-tested without a Tideweaver; observers cannot.

Pre-extracting the useful scheduler facts into an `ObserverContext` value type would tighten this — but it's a small breaking change for marginal payoff, since (a) the in-tree subclasses (NullObserver, LoggingObserver, SignalObserver) don't read the scheduler reference, and (b) the contract is documented stable. **Stub only; not urgent.**

### 5.3 `flow.py` size — navigation, not defect

685 LOC, 18 classes. Could naturally split into:

- `flow/gate.py` — `GateContext`, `Gate`, `HardLock`, `SoftPass`, `Weir` (~150 LOC)
- `flow/penstock.py` — `BackpressurePenstock` (the one Penstock that lives in flow because of its reservoir dependency) (~60 LOC)
- `flow/spillway.py` — `Spillway`, `DropOldest`, `RaiseOverflow`, `ExportToArchive` (~80 LOC)
- `flow/observer.py` — `FlowObserver`, `NullObserver`, `LoggingObserver`, `SignalObserver` (~170 LOC)
- `flow/__init__.py` — re-export + `SurgeContext`, `SurgeBarrier`, `Reservoir`, `FlowControl`, `flow_from_mode` (~225 LOC)

Pros: easier to locate one subclass. Cons: harder to read the full canal vocabulary in one screen. **Defer until the file grows further.** Stub.

### 5.4 `_tick_wrapper` density

[scheduler.py:445+](../incorporator/observability/tideweaver/scheduler.py) bundles: async task lifecycle, tenacity restart policy, penstock post-consume bookkeeping, observer hook fires, snapshot listification, reservoir append, spillway overflow. Dense — approaching the "one concern per function" ceiling — but each step is necessary at this site, and the closure plumbing (`bypassed_upstreams: FrozenSet[str]`) is immutable and clean. No structural change needed.

---

## 6. Verb-level scaling under large instance counts per wave

The user's identified scaling axis: high-row-count sources (100k+ rows per wave). Where the costs live:

| Hot path | Cost shape | Bottleneck under high N? | Notes |
|---|---|---|---|
| `ActualClass.model_validate(row)` per row ([factory.py:312](../incorporator/schema/factory.py)) | O(N) baseline | **Maybe** | Per-row in 1000-row batches; deliberate per the rationale at factory.py:305-308 (Pydantic schema cache + predictable peak memory) |
| `inc_dict[key] = self` (`WeakValueDictionary` insertion in `model_post_init`) | ~100-200 ns/instance amortised | Unmeasured | Untested under sustained >10k rows/sec — weakref callback machinery overhead is real but unquantified for this workload |
| `_tideweaver_snapshot = list(...)` per tick ([scheduler.py:523](../incorporator/observability/tideweaver/scheduler.py)) | O(N rows) per tick | **Yes** at 100k+ rows/wave | Strong-ref anchor by design — cannot be incrementalised without changing the snapshot contract (which both the Fjord flush and the reservoir trim depend on) |
| `Reservoir(depth=D)` memory residency | O(N × D) instance refs per edge | **Yes** at D≥5, N≥100k | Documented trade-off; `Spillway` is the escape valve. Worth a CHANGELOG note for users tempted to set `depth=100` |
| `is_garbage_value()` per converter call (~50 ns) | trivial | No | Already saves 30 µs exception cost vs. naive lambda (per b0ec8e7) |
| `_schema_union` autocoerce on typeless reads | O(1) after stabilisation; O(N×M) on first wave | No | Membership check only after first wave; not the hot path |
| Converter / extractor user callables | strictly per-row | No | Batching infeasible given user-callable semantics |
| `_schema_union` json-properties cache (factory.py:294-299) | one-time per class | No | Already memoised on the class — long-running daemons don't pay this twice |

### Three improvement candidates (highest-impact first)

#### R1 — TypeAdapter batch validation (high impact, needs benchmark)

**Hypothesis:** Pydantic v2's `TypeAdapter(List[Cls]).validate_python(rows)` is 2–5× faster than `model_validate` per row, at the cost of accumulating all errors before raising (rather than raising on the first bad row).

**Risk:** The author of [factory.py:305-308](../incorporator/schema/factory.py) explicitly chose the per-row path with the rationale: *"model_validate avoids a redundant `**kwargs` unpack per row and allows Pydantic's Rust core to amortise field-offset lookups across calls. Batching in 1000-row chunks keeps peak memory predictable and gives Pydantic's internal schema cache the best hit rate."* This isn't a casual choice — there's a reason. A direct migration without measurement is premature.

**Recommendation:** Stub a benchmark plan (F-3 in §7). Only if `TypeAdapter` wins by >20% on a realistic shape (10k rows × 20 fields, mixed types), propose the migration. The benchmark itself is a useful artifact regardless of outcome.

#### R2 — Incremental snapshot maintenance (medium impact, contract change)

The `_tideweaver_snapshot = list(cls.inc_dict.values())` at scheduler.py:523 is O(N) per tick because it eagerly listifies. For 100k rows/wave × 60 ticks/hour, that's 6M list-element copies/hour per Stream — small per-tick (~1 ms at typical row counts) but cumulative.

**Alternative:** Maintain `_tideweaver_snapshot` incrementally (`append` on each row insertion via `model_post_init`, clear at the start of each tick). Saves the listification cost on hot paths.

**Risk:** Changes the snapshot contract: today the snapshot is "everything in `inc_dict` at the moment the tick finished"; under incremental maintenance, it would be "everything inserted since the last tick started." For most workloads the two are equivalent — but the Fjord flush reads the snapshot, and edge cases (mid-tick `inc_dict.clear()`, retry-after-partial-failure) might diverge.

**Recommendation:** Stub for future investigation. Not urgent — the current cost is measurable but small at typical row counts.

#### R3 — Document the WeakValueDictionary churn ceiling (low impact, docs)

Today `inc_dict` is a `WeakValueDictionary` (per AGENTS.md "Memory / lifetime" section — chunking-mode Stream registries are weak). Under sustained high-row-count churn, the weakref machinery (proxy creation + callback registration on insertion, callback firing on instance death) adds ~100-200 ns/instance.

**Recommendation:** Add a `docs/performance.md` paragraph documenting this ceiling (something like: *"For sustained >10k rows/sec ingest, consider holding strong references via `_tideweaver_snapshot` and reading via that path instead of `inc_dict`"*). No code change. Stub.

---

## 7. Proposed follow-up plans

Listed in priority order. **F-1 is promoted to a concrete spawn plan** at `C:\Users\Eric\.claude\plans\canal-rejects-integration.md`; the rest are stubs.

### F-1 (concrete plan written; **urgent**) — Plumb canal skips into `RejectEntry`

The verb↔canal observability gap from §4.2. Today, a SustainedPenstock that throttles a source emits a `Tide.skipped` entry + a `FlowObserver.on_skip` call, but nothing structured the caller can read from `IncorporatorList.rejects`. Plan resolves the design choice (extend `RejectEntry.error_kind` vs. introduce a sibling `SkipRecord`) and lists the three files to touch. **See [the plan file](../../C:/Users/Eric/.claude/plans/canal-rejects-integration.md) for the detail.** (Local path; not committed.)

### F-2 (stub) — Routing-test coverage for the canal layer

Carried over from the previous evaluation report. All 5 routing tests set `INCORPORATOR_RATE_LIMIT_BYPASS=1`, so the canal layer is effectively short-circuited in routing scenarios. Add one test (`tests/test_tideweaver_routing_canal.py`) that drops the bypass and asserts on penstock-induced skip behaviour under a diamond shape.

### F-3 (stub; **needs measurement before commit**) — TypeAdapter batch-validation benchmark

The R1 candidate from §6. Write `tests/benchmarks/test_validate_batch_vs_per_row.py` measuring `model_validate` per row × 1000 vs. `TypeAdapter(List[Cls]).validate_python` for N ∈ {1k, 10k, 100k, 1M} rows × M ∈ {5, 20, 50} fields × type-mix ∈ {all-str, mixed-numeric, mixed-with-optional}. Only if TypeAdapter wins by >20% on a realistic shape, propose the migration. Respects the rationale at [factory.py:305-308](../incorporator/schema/factory.py).

### F-4 (stub) — Incremental `_tideweaver_snapshot` maintenance

The R2 candidate from §6. Needs design work first (the snapshot contract change is non-trivial). Probably wait until F-3 results show whether validation cost or listification cost dominates the hot path.

### F-5 (stub) — Document WeakValueDictionary churn ceiling

The R3 candidate from §6. Two paragraphs in [docs/performance.md](performance.md). No code change.

### F-6 (stub) — Adopt `Reservoir` in `stream()` chunking

The §4.3 gap. Architectural unification: let `stream()` accept a `reservoir: Reservoir | None` arg that bounds the in-memory chunk buffer with spillway overflow behaviour. Low priority — `stream()` is already paginator-driven (page size IS the bound), so the benefit is small.

### F-7 (stub) — Narrow `FlowObserver` hooks to drop the `scheduler` arg

The §5.2 A1 leak. Small breaking change (third-party subclasses, none in repo). Pre-extract `wave_number`, `tide_number`, and any other useful scheduler fact into a frozen `ObserverContext`. Defer indefinitely unless an external user reports a need.

### F-8 (stub) — Split `flow.py` into four files

The §5.3 navigation point. Pure refactor; semantics unchanged. Defer until the file grows past ~900 LOC.

---

## 8. Diff vs. the previous evaluation

The canal-evaluation report (`docs/canal_evaluation.md`) answered "does the test suite hold up?" — affirmative across 127 tests. This report answers "is the architecture sound and complete?" — affirmative for scheduler ↔ canal, with two real seams (F-1, F-2) at verb ↔ canal and one minor structural leak (F-7). The scaling improvements section is new and orthogonal to both prior reports.

The two reports together close out the user's two related but distinct questions: **"do the tests still work after canal?"** (yes) and **"is canal best-integrated with everything else?"** (yes for scheduler; almost-yes for verb layer; structural improvements available but not urgent).
