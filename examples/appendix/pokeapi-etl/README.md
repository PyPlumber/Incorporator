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
> from incorporator.io.penstock import SustainedPenstock
>
> register_host_penstock("pokeapi.co", SustainedPenstock(rate_per_sec=1.5))
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
from incorporator.io.penstock import SustainedPenstock

# Pace pokeapi.co at 1.5 req/sec (90/min — under the documented 100/min ceiling).
register_host_penstock("pokeapi.co", SustainedPenstock(rate_per_sec=1.5))

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

Older frameworks rely on implicit "magic" to make this jump. Incorporator uses an explicit **State Carrier** pattern:

1. The discovery call passes `inc_child="url"` to tell the engine where child URLs live.
2. The returned `pokemon_nav` list carries that path as state.
3. Passing `inc_parent=pokemon_nav` to the enrichment call reads the cached state, drills into all 150 objects, extracts the URLs, and provisions a rate-limited concurrency pool to download the detail payloads. Zero boilerplate loops.

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
* **Reduction** — your pure Python function iterates, sums, and returns a single integer (e.g. `318`).
* **Type guarantee** — `target_type=int` strictly validates the output.

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

You aren't just ingesting APIs — you are sculpting them. The explicit `inc_child` state carrier plus declarative `calc` tokens compress 150 deeply nested, fractured JSON payloads into a flat list of native Python objects in a fraction of a second.

---

## 🐳 Run it from the CLI

The CLI handles user-defined reducers via an **`inflow.py` sidecar** — a single Python file containing the helper functions your pipeline.json references. No fjord wrapper, no outflow function, no second class. Just a vanilla `stream()` pipeline that uses your reducer.

### `inflow.py` — the helpers

```python
def calculate_bst(stats_array):
    """Same reducer from the Python example, exposed for the CLI."""
    return sum(s.get("base_stat", 0) for s in stats_array if isinstance(s, dict))
```

### `pipeline.json` — zero escape characters

```json
{
  "inflow": "inflow.py",
  "incorp_params": {
    "inc_url": "https://pokeapi.co/api/v2/pokemon/?limit=50",
    "rec_path": "results",
    "inc_code": "id",
    "inc_name": "name",
    "excl_lst": ["sprites", "moves", "game_indices", "held_items"],
    "conv_dict": {
      "stats": "calc(calculate_bst, 'stats', default=0, target_type=int)"
    },
    "name_chg": [["stats", "base_stat_total"]]
  },
  "export_params": {"file_path": "data/pokemon.ndjson"}
}
```

```bash
incorporator validate pipeline.json
incorporator stream pipeline.json
```

The token resolver imports `inflow.py` at config-load time, sees `calculate_bst` in its public symbols, and resolves the `calc(...)` string to a real Python callable before the engine runs. This shallow pipeline is a **wiring demo**: the `/pokemon/?limit=50` list rows carry only `name` + a HATEOAS `url` (no per-Pokémon `stats`), so `base_stat_total` resolves to the `default=0` for every row — for a real Base Stat Total you need the parent-child drill from the Python script above. What it does prove is that the reducer is wired and runs **before** format dispatch, so this same pipeline.json works for any export format — switch the extension to `.csv`, `.parquet`, `.avro`, etc., and the resolved field still lands in the cell.

> **Tip:** for paginators and pre-built converter instances, use the cleaner `@name` syntax. Define `next_page = NextUrlPaginator("next")` in `inflow.py`, then reference it as `"inc_page": "@next_page"` in pipeline.json — zero JSON escape characters. See [the CLI guide](../../../docs/cli_and_configuration.md#text-form-tokens-paginators-converters-etc) for the full pattern.

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the canonical parent-child intro (no calc layer) | [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Apply `calc()` reductions in a fjord outflow | [Tutorial 9 — NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md) |
| Stream paginated APIs with custom paginators | [Streaming & Pagination Deep Dive](../../../docs/streaming_and_pagination.md) |
| Land the reduced output in a warehouse | [Tutorial 3 — Universal Formats](../../03-universal-formats/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/pokeapi-etl/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
