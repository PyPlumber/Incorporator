***

> 📎 **Appendix — `calc()` reductions over a HATEOAS drill.**
> Sums and aggregations across a paginated parent-child fan-out
> (PokéAPI). If you're new to parent-child, start with
> [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md)
> (CoinGecko top-N → `/coins/{id}` drill); reach for this appendix
> for the `calc()` reduction patterns layered on top.

***

# 🧬 Advanced ETL: HATEOAS & Declarative Reductions with `calc`

PokéAPI's `/pokemon/?offset=` paginates 1,025 records 20 at a time, and each Pokémon's `/pokemon/{id}` response carries `stats: [{base_stat, stat: {name}}, ...]` as a six-element array you need to reduce to a single "base_total" integer. T5's parent-child pattern handles the drill; `calc()` + `sum_attributes()` handle the array-reduction. This appendix walks both end-to-end.

Traditional Pydantic setups force you to define strict sub-models for `Stat`, `StatDetail`, and `TypeInfo` — just so you can write a `@property` later to calculate the sum. **Incorporator eliminates that overhead.** Using `calc()` and explicit `inc_parent` / `inc_child` routing, you concurrently fetch deep URLs, intercept raw JSON arrays, and reduce them into clean Python properties *before* the objects are even fully instantiated.

Verified: 150 Pokémon discovered + drilled, leaderboard rendered, ~100 s wall-clock.

---

## 🎯 The Scenario

Build a "Gen 1 Power Ranking" table:
1. **Shallow discovery:** fetch a paginated list of 150 Pokémon, which only gives names and a HATEOAS URL to their deep details.
2. **Deep enrichment:** concurrently fire 150 HTTP requests to those URLs to fetch their deep specs.
3. **Declarative ETL:** use `calc()` to intercept the massive `stats` and `types` JSON arrays, sum them up, format them, and drop the raw JSON to save memory.

> **Rate-limit note.** PokéAPI [documents a 100 req/min ceiling](https://pokeapi.co/docs/v2#fairuse).
> The framework default is 15 req/sec (900 req/min) — 9× too fast and would
> 429 most of the 150 child drills.  The companion script opts in to
> host-level throttling at module load:
>
> ```python
> from incorporator import register_host_penstock
>
> register_host_penstock("pokeapi.co", rate_per_sec=1.5)
> ```
>
> Register once at startup; every subsequent `incorp()` against `pokeapi.co`
> respects the 1.5 req/sec pace (90 req/min — under the 100/min ceiling).
> The script also passes `requests_per_second=1.5` on each `incorp()` call so
> the per-call knob is visible at the call site.  Total wall-clock: ~100 s
> for 150 drills.

---

## Optional: Probe with `test()` first

PokéAPI's deep `/pokemon/{id}` response is a textbook case for the JIT API
Profiler (introduced in [Tutorial 1](../../01-first-steps/README.md)) — nested
arrays, heavy fields, and a sub-structure worth flattening with `calc()`.
A single `test()` call surfaces every kwarg the Complete Code below uses:

```python
import asyncio
from incorporator import Incorporator


class Pokemon(Incorporator):
    pass


asyncio.run(Pokemon.test(inc_url="https://pokeapi.co/api/v2/pokemon/1/"))
```

What the inspector prints, abbreviated:

```text
🗑️  5. HEAVY-FIELD HINTS:
   💡 Fields likely to bloat the payload — consider excluding:
      excl_lst=[...]
```

The heavy-field hint flags the bloated payload candidates; the Complete
Code's `excl_lst` (`sprites`, `moves`, `game_indices`, `held_items`) drops
the heaviest of them.  The `stats`/`types` arrays you reduce with `calc()`
show up in the printed tree (section 1) as nested list-of-dicts — the
inspector doesn't auto-suggest a `calc()` reducer, so add those yourself.
Copy the suggested keys, swap `test()` for `incorp()`, and paste the kwargs.

---

## 💻 The Complete Code

```python
import asyncio
from typing import Any
from incorporator import Incorporator, NextUrlPaginator, calc, register_host_penstock

# Pace pokeapi.co at 1.5 req/sec (90/min — under the documented 100/min ceiling).
register_host_penstock("pokeapi.co", rate_per_sec=1.5)

# --- EXPLICIT SUBCLASSING ---
class Nav(Incorporator): pass
class Pokemon(Incorporator): pass

# --- DECLARATIVE REDUCTION FUNCTIONS ---
def calculate_bst(stats_array: Any) -> int:
    """Calculates Base Stat Total by summing the 'base_stat' of all entries."""
    if not isinstance(stats_array, list): return 0
    return sum(stat_obj.get("base_stat", 0) for stat_obj in stats_array if isinstance(stat_obj, dict))

def format_typing(types_array: Any) -> str:
    """Formats a nested types array into a clean string (e.g., 'Grass / Poison')."""
    if not isinstance(types_array, list): return "Unknown"
    type_names =[t.get("type", {}).get("name", "").capitalize() for t in types_array if isinstance(t, dict)]
    return " / ".join(type_names)

# --- MAIN EXECUTION ---
async def main() -> None:
    BASE_URL = "https://pokeapi.co/api/v2"

    print("⏳ Shallow discovery: fetching 150 records...")
    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        # Explicitly declare where the next URLs live!
        inc_child="url",
        inc_page=NextUrlPaginator("next"),
        call_lim=3,  # 3 pages * 50 = 150 Pokemon
        requests_per_second=1.5,  # 90 req/min — under PokéAPI's 100/min ceiling
    )

    print(f"✅ Discovered {len(pokemon_nav)} Pokémon. Commencing deep scan...")

    # pokemon_nav carries the inc_child="url" state from the discovery call;
    # passing it as inc_parent triggers 150 concurrent detail requests.
    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"],
        conv_dict={
            # Intercept the JSON arrays and apply our reduction functions dynamically
            "stats": calc(calculate_bst, "stats", default=0, target_type=int),
            "types": calc(format_typing, "types", default="Unknown", target_type=str)
        },
        name_chg=[("stats", "base_stat_total")],
        requests_per_second=1.5,
    )    
    
    if isinstance(enriched_pokemon, list):
        # Sort by our newly calculated integer!
        enriched_pokemon.sort(key=lambda p: getattr(p, "base_stat_total", 0), reverse=True)

        for p in enriched_pokemon[:5]:
            print(f"{p.inc_name.capitalize():<12} | Types: {p.types:<15} | Total Stats: {p.base_stat_total}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🧠 Architecture Deep Dive: How it Works

### 1. The Explicit Routing Engine & State Carrier (`inc_child`)
REST APIs often use HATEOAS — a shallow list of items, each carrying a `"url"` to get more data.

Incorporator makes this explicit:

1. The discovery call passes `inc_child="url"` to tell the engine where child URLs live.
2. The returned `pokemon_nav` list carries that path as state.
3. Passing `inc_parent=pokemon_nav` to the enrichment call reads the cached state, drills into all 150 objects, extracts the URLs, and provisions a rate-limited concurrency pool to download the detail payloads. No hand-written loops over the child list.

### 2. The Power of `calc()`
Look at the raw JSON for "stats":

```json
"stats":[
    {"base_stat": 45, "effort": 0, "stat": {"name": "hp", "url": "..."}},
    {"base_stat": 49, "effort": 0, "stat": {"name": "attack", "url": "..."}},
    // ... 4 more dictionaries
]
```
Auto-mapped, this becomes 6 sub-class instances per Pokémon — 900 extra objects in memory. We don't want the objects; we just want the sum.

We intercept with `calc`:
```python
"stats": calc(calculate_bst, "stats", default=0, target_type=int)
```

**Under the hood:**
* **Interception** — Incorporator sees the `"stats"` key and pauses auto-nesting.
* **Extraction (`*input_keys`)** — by passing `"stats"` as the second argument, Incorporator extracts the raw JSON list and passes it into `calculate_bst`.
* **Reduction** — your Python function iterates, sums, and returns a single integer (e.g. `318`).
* **Type guarantee** — `target_type=int` strictly validates the output.

> **Caching note.** `calc()` defaults to `pure=True`, which wraps the callable in `lru_cache(maxsize=10_000)` at construction. For this pattern each `stats` array is a distinct object, so the cache will rarely hit. If `calculate_bst` is computationally cheap (it is here), the default is fine. Pass `pure=False` to skip the cache on callables that always receive unique inputs.

*Memory benefit:* the raw JSON array is immediately garbage-collected. The final object stores one `int`.

### 3. Cleaning the Graph with `name_chg`
After `calc()` reduces `stats` to an integer, the attribute is still technically named `.stats`. While correct, `pokemon.stats` implies a list. Rename it during instantiation:

```python
name_chg=[("stats", "base_stat_total")]
```
Now the attribute is `pokemon.base_stat_total` — readable dot-notation.

### 4. Dropping Heavy Payloads (`excl_lst`)
Before `calc` or auto-nesting fires, `excl_lst` runs:
```python
excl_lst=["sprites", "moves", "game_indices", "held_items"]
```
PokéAPI returns massive base64 strings and thousand-item lists for `moves`. Excluding them deletes the keys the millisecond they're received, sparing Pydantic any wasted CPU on data you'll never touch.

---

## 🌟 Summary

You aren't just ingesting APIs — you are sculpting them. The explicit `inc_child` state carrier plus declarative `calc` tokens compress 150 deeply nested, fractured JSON payloads into a flat list of native Python objects — the companion script reports ~100 s wall-clock at 1.5 req/sec for the full 150-Pokémon drill.

---

## Run it from the CLI

The CLI form reproduces the Python entry's **exact two-phase drill**, not a
shallow wiring demo — this appendix is outside the numbered tutorial path, so
it's free to reach for [Tideweaver](../../11-tideweaver/README.md)'s
`Watershed.chain` shape even though Tideweaver itself is introduced later, in
Tutorial 11. Three currents, in order:

1. `nav` (`Stream`) — shallow discovery, all 150 `{name, url}` rows in one
   `?limit=150&offset=0` call.
2. `pokemon` (`Stream(parent_current="nav")`) — the T5 drill against `nav`'s
   parked snapshot; 150 concurrent `/pokemon/{id}/` detail fetches, real
   `stats`/`types`, reduced the same way as the Python entry.
3. `export_pokemon` (`Export`) — snapshots `pokemon`'s drilled registry to
   `out/pokemon.ndjson`. **Required, not cosmetic**:
   `Stream(parent_current=...)`'s own `export_params` is never consumed by
   the scheduler, so this third current is the only thing that writes the
   rows to disk.

See [`inflow.py`](inflow.py) and [`watershed.json`](watershed.json), which
ship next to the entry script — `inflow.py` now carries the `Nav` / `Pokemon`
classes, the `calculate_bst` / `format_typing` reducers (kept in sync with
`pokeapi_etl_calc.py` verbatim), and the `pokeapi.co` host-throttle
registration. That last one matters: the CLI path imports `inflow.py`,
never `pokeapi_etl_calc.py`, so `inflow.py` is the only place the 1.5
req/sec throttle can be registered before 150 concurrent detail requests
fire.

> **Suspected framework gap — why `nav` fetches one page (`limit=150`), not
> three (`limit=50` × `call_lim=3`) like the Python entry.** A Tideweaver
> `Stream` current with no `parent_current` always runs through
> `cls.stream()` (chunking mode). Chunking mode's `refresh_params` defaults
> to `{}` whenever the field is omitted — including from a `watershed.json`
> `Stream` node, which has no way to forward an explicit "no, really, skip
> refresh" down to `stream()` (`Stream.refresh_params=None`, the field's own
> default, is indistinguishable from "not set" once it reaches the
> scheduler). That default `{}` makes `stream()` silently call
> `cls.refresh()` after every chunk. For a
> **paginated** chunk, each row was extracted via `rec_path` and carries no
> per-instance origin URL, so `refresh()` raises and the chunked engine
> aborts pagination after page 1 — the CLI form would silently cap at 50
> rows instead of 150. Fetching all 150 in a single unpaginated page
> sidesteps the bug (no continuation needed) — `inc_code="name"` further
> ensures the implicit post-chunk refresh re-fetch upserts the same 150 rows
> in place instead of re-inserting them under new auto-increment keys.

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the canonical parent-child intro (no calc layer) | [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Apply `calc()` reductions in a fjord outflow | [Tutorial 9 — NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md) |
| Stream paginated APIs with custom paginators | [Streaming & Pagination Deep Dive](../../../docs/streaming_and_pagination.md) |
| Land the reduced output in a warehouse | [Tutorial 3 — Universal Formats](../../03-universal-formats/README.md) |

---

## 🐳 Run It From the CLI (+ Docker)

Reference material — three ways to run the exact same two-phase drill, in order.

**1. Python entry** (what every section above walked through — the full
parent-child drill, ~100 s wall-clock, unchanged):

```bash
cd examples/appendix/pokeapi-etl
python pokeapi_etl_calc.py
```

**2. CLI form** — [`inflow.py`](inflow.py) + [`watershed.json`](watershed.json)
ship next to the entry script; no inline JSON duplicate here (see it drift
once, trust it forever).

```bash
cd examples/appendix/pokeapi-etl
incorporator validate watershed.json
incorporator tideweaver run watershed.json
```

Expect ~100-120 s wall-clock (150 detail requests @ 1.5 req/sec) — that is
expected, not a hang. Produces `out/pokemon.ndjson` with 150 rows, real
`base_stat_total` values (Mewtwo = 680), matching the Python entry.

> **Run from inside this directory.** `export_params.file_path`
> (`"out/pokemon.ndjson"`) is CWD-relative, and `"inflow": "inflow.py"` is
> config-dir-relative — running
> `incorporator tideweaver run examples/appendix/pokeapi-etl/watershed.json`
> from the repo root writes output to `<repo-root>/out/` and would break the
> sidecar resolution if it were repo-root-relative instead.
>
> **The window is dateless.** `watershed.json`'s `window` references
> `@window_start` / `@window_end`, two public `datetime` names defined in
> `inflow.py` and evaluated fresh at sidecar-import time (a 3-minute span
> from "now") — no env vars, no editing timestamps before a re-run.

**3. Docker** — reasoned from the `Dockerfile`/`docker-compose.yml`, **NOT
run or verified** (no Docker available in this pass — confirm before
relying on it):

```bash
# Reasoned, unverified.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/examples/appendix/pokeapi-etl:/app/config:ro" \
  -v "$(pwd)/examples/appendix/pokeapi-etl/out:/app/out" \
  incorporator:latest \
  tideweaver run /app/config/watershed.json
```

The image's `WORKDIR` is `/app`, and `export_params.file_path` is
CWD-relative (never rebased against the config's directory) — so
`watershed.json`'s `"out/pokemon.ndjson"` resolves to `/app/out/...` inside
the container. The mount target must therefore be `/app/out`, not one of
the three paths the `Dockerfile` prepares (`/app/config`, `/app/data`,
`/app/logs`). Because `/app/out` is not one of the pre-`chown`'d
directories, `--user` overrides to the invoking host user so the non-root
`appuser` can still write.

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/pokeapi-etl/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
