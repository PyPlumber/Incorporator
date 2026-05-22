# Canal Engineering Evaluation

**Branch:** `workflow` at `d663f82` (== `v1.2.0` tag — `git rev-parse HEAD` and `git rev-parse v1.2.0` return the same sha)
**Test runs:** 2 × 127 tests (Python 3.13.3, Windows, mocked HTTP)
**Date:** 2026-05-22

This report revisits the Tideweaver routing & orchestration test suite that was added at commit `bdc7fa4` ("fix(tideweaver): subclass registry + snapshot + Export, add routing tests") in light of the **81 commits** of canal-engineering work that landed between that commit and HEAD.

---

## 1. Summary

1. **Did processing improve?** ✅ Yes — qualitatively. Eight `perf:` commits landed between the routing tests and HEAD (deque reservoir, adaptive heap-driven wakeup, AsyncClient pooling × 3, lazy ClassVar allocation, pre-classified conv_dict ops, inc-closure caching). The chain routing test itself documents an upgrade: it could re-introduce its `gate_mode="weir"` data dependency after the canal toolkit landed (previously had to drop to `gate_mode="soft"` to fit the test window). **But:** no Tideweaver benchmarks exist, so the perf claim has no quantitative basis.
2. **Does the canal engineering model work well?** ✅ Mostly. 127/127 tests pass twice with 1% wall-time variance. The `_EdgeState` → `FlowState` composition (i14) is clean, narrow context types (A1: `GateContext` / `SurgeContext`) are honored by Gate / SurgeBarrier subclasses, `Penstock.post_consume` (A2) is the single bookkeeping hook, and no test reads scheduler internals. **One small leak:** `FlowObserver` hooks (A3) still take `scheduler: "Tideweaver"` as their first positional arg — A1 discipline was not extended to observers.
3. **Where are the weaknesses?** Three real ones: **(a) zero benchmarks** for Tideweaver/canal/penstock (10 throughput benchmarks exist, all for format I/O); **(b)** all 5 routing tests set `INCORPORATOR_RATE_LIMIT_BYPASS=1`, so they short-circuit the penstock layer entirely; **(c)** none of the 5 routing tests touch `BackpressurePenstock`, `SurgeBarrier(action="bypass")`, `FlowObserver`, `phase_offset_sec`, `RejectEntry`, or `architect(output="plan")` — those features are exercised only inside `test_tideweaver.py`.
4. **Did we keep the project lightweight?** ✅ Yes. Canal core (`flow.py` + `scheduler.py` + `penstock.py`) is **2,056 LOC = 10.9%** of the project; the full `observability/tideweaver/` package is **3,632 LOC = 19.3%**. **No new dependencies** were added — `git diff v1.2.0^..HEAD -- pyproject.toml` is empty for the canal-introduction window. Top-level surface holds at 28 exports.

---

## 2. Methodology

### Scope

| File | LOC | Tests | Area |
|---|---:|---:|---|
| [tests/test_tideweaver.py](../tests/test_tideweaver.py) | 3,123 | 114 | shapes / scheduler / canal toolkit / JSON loader / CLI verb |
| [tests/test_tideweaver_dedup.py](../tests/test_tideweaver_dedup.py) | 123 | 2 | gate-dedup watermark regression (commit `94975b3`) |
| [tests/test_tideweaver_routing_chain.py](../tests/test_tideweaver_routing_chain.py) | 355 | 2 | three-Stream cascade + Fjord tail |
| [tests/test_tideweaver_routing_diamond.py](../tests/test_tideweaver_routing_diamond.py) | 458 | 2 | MLB diamond + Open Library two-middle |
| [tests/test_tideweaver_routing_fanout.py](../tests/test_tideweaver_routing_fanout.py) | 582 | 3 | one→three Fjord, two Fjord+Export, EPL |
| [tests/test_tideweaver_routing_parallel.py](../tests/test_tideweaver_routing_parallel.py) | 335 | 2 | independent parallel Streams |
| [tests/test_tideweaver_routing_custom.py](../tests/test_tideweaver_routing_custom.py) | 404 | 2 | hard/soft mix, three-Fjord cascade |
| **Total** | **5,380** | **127** | |

### Baseline limitation

There is **no naive before/after baseline.** The routing test files were added at commit `bdc7fa4`, *before* the canal toolkit existed (`FlowControl` was introduced at `ee8aba6`, several commits later). Running these test files against pre-canal incorporator code would mismatch — the tests use APIs (`Watershed.chain(..., gate_mode="weir")`, `flow_from_mode`, `Edge.flow`) that didn't exist yet. So "processing improved" is evaluated qualitatively through (i) `perf:` commit inventory, (ii) wall-time stability across two runs, and (iii) docstring evidence of capabilities the tests gained.

### Commit-class inventory (`bdc7fa4..HEAD`, 81 commits)

- **8 `perf:`** — including `0c2c776` (deque reservoir), `ad9041f` (adaptive heap-driven wakeup), `538c3a7 / 35e366d / 4ea3c29` (AsyncClient pooling), `02794ba` (lazy ClassVar), `b0ec8e7` (pre-classify conv_dict ops), `ee8c698` (inc-closure caching)
- **22 `feat:`** — including the entire FlowControl/Gate hierarchy (`ee8aba6`), Reservoir/Spillway/Penstock (`ed297a9`, `d56aac0`, `5fd8c6e`), `architect` (`b140e73`), `architect(output="plan")` (`df1d536`), FlowObserver slot (`3b6a75a`), explicit shape kwargs + CustomCurrent (`bd9e256`), Penstock unification (`c8bd28f`), CLI Pydantic configs (`66c30ab`)
- **12 `test:` / test-bearing** — including `c8c20da` ("canal-toolkit coverage gaps"), `2e0baa1` (Penstock+Reservoir+Spillway composition), `50bd83e` (routing-test conversion to weir mode), `d61ae31` (null-guard lambda migration)
- **8 `fix:`** — including the dedup direction bug (`94975b3`), bypassed-edges Penstock accounting (`72a1b34`), `OrchestrationPlan.to_watershed` CustomCurrent support (`ab826a7`), symmetric null guards (`724349b`)

---

## 3. Did processing improve?

### Test outcomes

```
Run 1: 127 passed, 1 warning in 113.21s
Run 2: 127 passed, 1 warning in 112.29s
Variance: 0.92s (≈0.8%)
```

The single warning is **intentional** — [tests/test_tideweaver_routing_parallel.py:test_parallel_isolate_on_error_keeps_siblings_firing](../tests/test_tideweaver_routing_parallel.py) deliberately raises in one branch's mock to exercise per-current error isolation; the warning is `IncorporatorList`'s "partial data returned" notice.

### Slowest tests (Run 1, top 5)

| Time | Test |
|---:|---|
| 10.18s | `test_tideweaver_routing_diamond::test_diamond_open_library_two_middle_fjords_to_catalog_tail` |
| 8.65s | `test_tideweaver_routing_diamond::test_diamond_mlb_teams_plus_players_fjord_join` |
| 8.08s | `test_tideweaver_routing_fanout::test_fanout_one_stream_to_three_heterogeneous_fjords` |
| 8.02s | `test_tideweaver_routing_chain::test_chain_streams_into_fjord_tail_reads_both_upstream_snapshots` |
| 8.02s | `test_tideweaver_routing_chain::test_chain_three_streams_apply_conv_dict_in_order` |

These wall times are driven by **scheduled `asyncio.sleep` intervals** in the tests (3.0s Stream intervals × multi-pass windows), not by per-pass scheduler overhead. The adaptive heap-driven wakeup (`ad9041f`) avoids polling between scheduled events, so these times are effectively the configured intervals × number of passes.

### Direct evidence of capability uplift

From [tests/test_tideweaver_routing_chain.py:127-136](../tests/test_tideweaver_routing_chain.py):

> Uses `Watershed.chain(..., gate_mode="weir")` — the third gating mode introduced in the canal toolkit refactor. `weir` keeps the data dependency (downstream waits for at least one upstream wave) but does NOT block on in-flight upstream the way `"hard"` does, so all three Streams fit their realistic 3.0s intervals inside the 8.0s test window. **Previously this test had to drop the data dependency entirely with `gate_mode="soft"` to make the window fit.**

This is the cleanest single sign of "did processing improve" — the canal toolkit gave the test a finer dial (weir) where previously only the binary hard/soft existed.

### Internals verification

- `_EdgeState.flow_state: FlowState` composition is fully landed (i14) — verified at [incorporator/observability/tideweaver/scheduler.py:74](../incorporator/observability/tideweaver/scheduler.py). The model uses only `waves`, `overflow_count`, and `flow_state` — no top-level penstock fields.
- `Penstock.post_consume(edge_state, now)` (A2) is implemented at [incorporator/io/penstock.py:207](../incorporator/io/penstock.py); the scheduler's single call site at [scheduler.py:509](../incorporator/observability/tideweaver/scheduler.py) is the only post-consume entry point.
- Narrow contexts (A1) are imported by the scheduler (`from .flow import FlowControl, GateContext, SurgeContext` at [scheduler.py:47](../incorporator/observability/tideweaver/scheduler.py)) and consumed by `Gate.gate_reason(ctx)` / `SurgeBarrier.is_tripped(ctx)`.
- No test file reads `scheduler._state`, `tw._state`, or `_last_consumed` — the two grep matches in `test_tideweaver.py` are docstring/comment references only.

---

## 4. Does the canal engineering model work well?

### Rubric

| Improvement | Test coverage | Public-surface clean | Lightweight | Verdict |
|---|---|---|---|---|
| A1 narrow contexts (`GateContext` / `SurgeContext`) | ✅ `test_tideweaver_dedup.py` exercises both via `Weir` + `HardLock` | ✅ Frozen dataclasses, no Pydantic surface | ✅ ~25 LOC for both contexts | ✅ Solid |
| A2 `Penstock.post_consume` | ✅ `test_tideweaver.py::test_bypass_does_not_charge_burst_penstock` + `test_bypass_does_not_log_window_penstock` | ✅ One hook, all six concrete penstocks inherit | ✅ Hook itself is 8 LOC | ✅ Solid |
| A3 `FlowObserver` declarative slot | ✅ `test_tideweaver.py::test_observer_does_not_fire_on_fire_for_bypassed_edges` | ⚠️ Hooks pass `scheduler: "Tideweaver"` — model leak vs. A1 discipline | ✅ NullObserver, LoggingObserver, SignalObserver are tight | ⚠️ Partial |
| A4 fold finally loops | ✅ Indirect — `_tick_wrapper` bypass plumbing covered by `test_surge_barrier_bypass_*` tests | ✅ Scheduler internal | ✅ Reduced cleanup duplication | ✅ Solid |
| C drop `dependency_mode` / `"mode"` aliases | ✅ `test_tideweaver.py` mode-shorthand tests; `gate_mode` used everywhere in routing tests | ✅ Single keyword, JSON discriminator field | ✅ Removed legacy paths | ✅ Solid |
| D2a/D2b Pydantic `StreamConfig` / `FjordConfig` | ✅ CLI validation tests | ✅ Single source of truth, `validate.py` delegates | ✅ Eliminates ad-hoc dict shape checks | ✅ Solid |
| E `architect(output="plan")` → `to_watershed()` | ✅ `tests/test_architect.py` (out of in-scope set, but exists) | ✅ `OrchestrationPlan` is a dataclass | ⚠️ `architect.py` at 875 LOC — largest single file in tideweaver/ | ✅ Solid (with size note) |
| F1 explicit shape kwargs + `CustomCurrent` | ✅ `test_tideweaver.py` shape constructor tests | ✅ `CustomCurrent` is an abstract subclass — small surface | ✅ Replaces dict-of-kwargs idiom | ✅ Solid |
| G Penstock unification (HTTP + edge layers) | ✅ `test_io_penstock.py` + `test_penstock_registry.py` | ✅ One `Penstock` hierarchy serves both layers | ✅ `io/throttle.py` deleted | ✅ Solid |
| H1 `SourceRef` | ⚠️ Internal use only — public API still uses kwarg unions | ✅ 5 factories + `parse()` | ✅ 148 LOC | ✅ Solid |
| H3 null-handling alignment | ✅ `tests/test_*_etl.py` migrated to lambda-free `calc(stdlib_fn, ...)` form (commit `d61ae31`) | ✅ Single `is_garbage_value` predicate | ✅ Eliminates defensive lambdas across converters | ✅ Solid |
| H4 `RejectEntry` | ⚠️ Routing tests do **not** assert on `rejects` — only `failed_sources` derived view is touched (1 test) | ✅ Frozen Pydantic, top-level export, derived `failed_sources` keeps back-compat | ✅ 104 LOC ([rejects.py](../incorporator/rejects.py)) | ⚠️ Partial (lacks routing-test coverage) |
| i14 `FlowState` composition into `_EdgeState` | ✅ Implicit — every penstock test depends on it | ✅ One `FlowState` shape, no field duplication between layers | ✅ Removes pre-i14 top-level penstock fields | ✅ Solid |

### Model-coherence verdict

The canal abstraction is **mostly leak-proof**:

- `Gate` / `SurgeBarrier` / `Penstock` / `Spillway` subclasses receive only their narrow context value type — no scheduler reference. Custom strategies can be unit-tested without a `Tideweaver`.
- `Penstock.consume_reason(edge_state, flow, now)` accepts either an `_EdgeState` (with `flow_state` attribute) or a bare `FlowState` via the `getattr(edge_state, "flow_state", edge_state)` fallback at [penstock.py:203](../incorporator/io/penstock.py) — same shape works in both HTTP and edge layers.
- `_EdgeState` is keyed by canonical `(from_name, to_name)` tuple; the dedup-watermark direction was the one place this leaked (fixed at `94975b3` — `test_tideweaver_dedup.py` pins the contract).

The one observed leak — `FlowObserver` hooks pass `scheduler: "Tideweaver"` — appears intentional per the contract pinned at commit `47127ee` ("docs(api): pin FlowObserver hook stability contract"). Pre-extracting all useful scheduler facts into an `ObserverContext` would be a small breaking change with marginal payoff; flagged in §7 as a minor-priority follow-up.

---

## 5. Where are the weaknesses?

### 5.1 No Tideweaver benchmarks exist (headline weakness)

```
$ find tests/benchmarks -name "*.py" -exec grep -l "tideweaver\|Watershed\|Penstock\|FlowControl\|Reservoir\|Spillway" {} \;
(empty)
```

`tests/benchmarks/` contains 10 throughput benchmarks (avro, parquet, sqlite, xlsx, csv, etc.) — **zero** for Tideweaver, canal-toolkit, or penstock. The 8 `perf:` commits between routing-test introduction and HEAD have no measured baseline. The deque-vs-list reservoir change (`0c2c776`) is asserted as O(1) in code comments but never benchmarked. Same for the adaptive heap-driven wakeup (`ad9041f`).

### 5.2 Routing tests short-circuit the canal layer

All 5 routing test files set `INCORPORATOR_RATE_LIMIT_BYPASS=1` via `monkeypatch.setenv` — example at [tests/test_tideweaver_routing_chain.py:138](../tests/test_tideweaver_routing_chain.py). This bypasses the penstock layer entirely. So the routing tests **do not exercise the canal under multi-current orchestration** — they validate dependency graph shapes and snapshot flow only.

### 5.3 Routing tests don't touch advanced canal features

Searches across the 5 routing files:

| Feature | Matches in routing tests |
|---|---:|
| `FlowObserver` / `LoggingObserver` / `SignalObserver` | 0 |
| `BackpressurePenstock` | 0 |
| `SurgeBarrier` | 0 (all routing tests rely on `gate_mode` shorthand, never construct a SurgeBarrier explicitly) |
| `SurgeBarrier(action="bypass")` | 0 |
| `phase_offset_sec` | 0 |
| `.rejects` / `RejectEntry` | 0 (`failed_sources` is touched indirectly via warning) |
| `architect(output="plan")` / `OrchestrationPlan` / `to_watershed` | 0 |

These features are covered in `test_tideweaver.py` (114 tests) and `test_architect.py`, so the broader Tideweaver test surface is solid — but the **routing** tests specifically are narrow. That may be appropriate (routing tests should test routing) but means the canal toolkit's interaction with multi-current shapes has no dedicated coverage.

### 5.4 `FlowObserver` hooks receive `scheduler: "Tideweaver"` (minor A1 incompleteness)

[incorporator/observability/tideweaver/flow.py:364-400](../incorporator/observability/tideweaver/flow.py):

```python
def on_fire(self, scheduler: "Tideweaver", edge: Tuple[str, str], wave_number: int) -> None:
def on_skip(self, scheduler: "Tideweaver", edge: Tuple[str, str], reason: str) -> None:
def on_spillway(self, scheduler: "Tideweaver", edge: Tuple[str, str], displaced_wave: object, overflow_count: int) -> None:
def on_reservoir_level(self, scheduler: "Tideweaver", edge: Tuple[str, str], used: int, capacity: int) -> None:
```

Per AGENTS.md, the A1 contract was: "Custom Gate / Penstock / SurgeBarrier / Spillway / SignalPenstock override against the narrow context value types, NOT the scheduler." Observers were not included in that list — but the omission isn't justified in code or docs.

### 5.5 `architect.py` size (minor)

[incorporator/observability/tideweaver/architect.py](../incorporator/observability/tideweaver/architect.py) is **875 LOC** — the largest single file in the package, larger than `scheduler.py` (783) or `flow.py` (685). It bundles `architect()` four output modes (`report` / `python` / `json` / `plan`), `OrchestrationPlan`, `to_watershed()`, and source-profiling helpers. The single-file design is convenient but worth a split if it grows further.

### 5.6 AGENTS.md framing is stale (process, not code)

AGENTS.md describes a "substantial unreleased band on workflow targeting v1.3.0" with H1/H3/H4/i14/etc. as "post-v1.2.0" work. Git state contradicts this — HEAD is exactly at the v1.2.0 tag (`d663f82`), and all those acronyms landed **in** v1.2.0. Not a bug in the project; just a stale internal briefing.

---

## 6. Did we keep the project lightweight?

### Numbers

| Metric | Value |
|---|---:|
| Canal core LOC (`flow.py` + `scheduler.py` + `penstock.py`) | **2,056** |
| Full Tideweaver package LOC (`observability/tideweaver/`) | **3,632** |
| Total `incorporator/` LOC | **18,821** |
| Canal share of project | **10.9%** |
| Tideweaver share of project | **19.3%** |
| Net LOC delta `bdc7fa4..HEAD` across 13 canal-relevant files | **+3,285** (+3,580 / -295) |
| Top-level package exports | **28** |
| `pyproject.toml` changes since v1.2.0 | **0 lines** |

### Dependencies

The canal toolkit was built on **stdlib only** (`collections.deque`, `asyncio`, `time`, `heapq`, `logging`) plus dependencies already present at v1.1.3 (Pydantic v2, httpx, tenacity). No new third-party dependency was added. The pre-existing 250-KB base install footprint claim from `docs/formats_and_compression.md` is unchanged.

### Per-file weights (`observability/tideweaver/`)

```
875 architect.py     ← largest
783 scheduler.py
685 flow.py
588 penstock.py     (io/penstock.py — outside the dir but canal-core)
419 watershed.py
409 config.py
294 current.py
 98 __init__.py
 69 tide.py
```

### Verdict

**Yes — lightweight in the conventional sense** (no new deps, stdlib-only canal primitives, tight public surface). 3,632 LOC for a full orchestration layer with 6 throttle models, 3 gates, 3 spillways, 3 observers, an architect probe with 4 output modes, and a JSON config loader is reasonable. The two caveats: (a) `architect.py` is single-file-large at 875 LOC; (b) the lack of benchmarks means the **runtime** weight (per-pass overhead, memory residency under deep reservoirs, observer hook overhead) is unmeasured.

---

## 7. Proposed follow-up plans

Each item is a standalone candidate plan — not executed in this session. Listed in suggested priority order.

### F-1 (high) — Add Tideweaver / canal / penstock benchmarks

The headline weakness from §5.1. Suggested scope: 3-4 benchmark files under `tests/benchmarks/`:
- `test_scheduler_pass_overhead.py` — empty Watershed with N currents, mock tick (zero work), measure passes/sec
- `test_reservoir_throughput.py` — deque vs list verification under deep reservoir + spillway overflow
- `test_penstock_overhead.py` — `consume_reason` call cost across all 6 throttle models, before/after `post_consume`
- `test_observer_hook_overhead.py` — `NullObserver` vs `LoggingObserver` vs `SignalObserver` hook cost per fire

Outcome: a quantitative baseline for the 8 `perf:` claims and a regression net for future canal work.

### F-2 (high) — Routing test that exercises the canal under multi-current orchestration

The single largest coverage gap (§5.2). Suggested scope: one new test in a new file `tests/test_tideweaver_routing_canal.py`. Reuse the diamond/fanout fixtures but:
- Drop `INCORPORATOR_RATE_LIMIT_BYPASS=1`
- Attach a `SustainedPenstock` to at least one edge
- Attach a `Reservoir(depth=3)` + `RaiseOverflow` spillway
- Attach a `SignalObserver(callback=...)` and assert on the observed events
- Compare end-state instance counts against the no-canal baseline

### F-3 (medium) — Routing-test coverage for advanced canal features

Per §5.3, fill the matrix:
- `FlowObserver` in a routing shape (covered partially by F-2)
- `SurgeBarrier(action="bypass")` in a fanout shape where one upstream is slow
- `phase_offset_sec` in a parallel shape for green-wave staging
- `.rejects` assertion (not just `failed_sources`) in the parallel-isolate-on-error test

### F-4 (low) — Narrow FlowObserver to an `ObserverContext`

Per §5.4, complete the A1 discipline. Pre-extract the scheduler facts each hook uses (current count, tick number, etc.) into a frozen `ObserverContext` dataclass. Small breaking change — third-party observer subclasses (none in the repo today) would need to adapt. Low priority unless an external user reports a need.

### F-5 (low) — Split `architect.py`

Per §5.5, if `architect.py` adds another output mode or another source-profiling helper, split into:
- `architect/__init__.py` — public surface + classmethod shim
- `architect/probe.py` — `_resolve_sources`, `SourceProfile`-consumption logic
- `architect/render.py` — the four `output=` renderers
- `architect/plan.py` — `OrchestrationPlan` + `to_watershed`

Defer until the file grows further.

### F-6 (housekeeping) — Update AGENTS.md framing

Per §5.6, drop the "unreleased band on workflow targeting v1.3.0" language; the post-v1.2.0 acronyms ARE v1.2.0. Either reframe as "v1.2.0 highlights" or move to a `docs/v1.2.0_release_notes.md` and remove from AGENTS.md.
