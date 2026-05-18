***

> 📎 **Appendix — `calc()` reductions over a HATEOAS drill.**
> Sums and aggregations across a parent-child fan-out (PokéAPI).
> If you're new to parent-child, start with
> [Tutorial 3 — Parent-Child Drilling](../../03-parent-child-drilling/README.md)
> (CoinGecko top-N → `/coins/{id}` drill); reach for this appendix
> for the `calc()` reduction patterns layered on top.

***

# 🧬 Advanced ETL: HATEOAS & Declarative Reductions with `calc`

REST APIs are notorious for returning deeply nested, bloated arrays. 

For example, if you want a Pokémon's **Primary Types** (e.g., Grass / Poison) and its **Base Stat Total** (the sum of its HP, Attack, Defense, etc.), the PokéAPI returns massive dictionaries nested inside lists. 

Normally, traditional Pydantic setups force you to define strict Sub-Models for `Stat`, `StatDetail`, and `TypeInfo`, just so you can write a `@property` later to calculate the sum. 

**Incorporator eliminates this memory overhead.** Using `calc()` and our explicit `inc_parent` / `inc_child` routing, you can concurrently fetch deep URLs, intercept raw JSON arrays, and reduce them into clean, flattened Python properties *before* the objects are even fully instantiated.

---

## 🎯 The Scenario
We are going to build a "Gen 1 Power Ranking" table. To do this, we need to:
1. **Shallow discovery:** Fetch a paginated list of 150 Pokémon, which only gives us their names and a HATEOAS URL to their deep details.
2. **Deep enrichment:** Concurrently fire 150 HTTP requests to those URLs to fetch their deep specs.
3. **Declarative ETL:** Use `calc()` to intercept the massive `stats` and `types` JSON arrays, sum them up, format them, and drop the raw JSON to save memory.

> **Rate-limit note.** PokéAPI [documents a 100 req/min ceiling](https://pokeapi.co/docs/v2#fairuse).
> The default Incorporator rate (15 req/sec = 900 req/min) is 9× too fast and
> would 429 most of the 150 child drills.  Two safeguards are in play:
>
> 1. **Host-aware default.** When you don't pass `requests_per_second`, the
>    framework auto-throttles calls to `pokeapi.co` to 1.5 req/sec (90/min).
> 2. **Explicit throttle in the script.** The example passes
>    `requests_per_second=1.5` on both `incorp()` calls so the kwarg is
>    visible to readers.  Total wall-clock: ~100 s for 150 drills.

---

## 💻 The Complete Code

```python
import asyncio
from typing import Any
from incorporator import Incorporator, NextUrlPaginator
from incorporator.schema.converters import calc

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
        call_lim=3  # 3 pages * 50 = 150 Pokemon
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
        name_chg=[("stats", "base_stat_total")]
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
REST APIs often use HATEOAS (Hypermedia as the Engine of Application State), meaning they return a shallow list of items, each containing a `"url"` to get more data.

In older frameworks, you had to rely on implicit "magic" attributes to make this jump. Incorporator completely eliminates this via the **State Carrier** pattern:

1. The discovery call passes `inc_child="url"` to tell the engine where child URLs live.
2. The returned `pokemon_nav` list object carries that path as state.
3. Passing `inc_parent=pokemon_nav` to the enrichment call reads the cached state, drills into all 150 objects, extracts the URLs, and provisions a rate-limited concurrency pool to download the detail payloads simultaneously. Zero boilerplate loops required!

### 2. The Power of `calc()`
This is where Incorporator's ETL engine shines. Look at the raw JSON the API returns for "stats":

```json
"stats":[
    {"base_stat": 45, "effort": 0, "stat": {"name": "hp", "url": "..."}},
    {"base_stat": 49, "effort": 0, "stat": {"name": "attack", "url": "..."}},
    // ... 4 more dictionaries
]
```
If Incorporator mapped this automatically, it would generate 6 new Python sub-classes per Pokémon (900 extra objects in memory!). We don't want the objects; we just want the sum (`45 + 49 + ...`).

We intercept this using `calc`:
```python
"stats": calc(calculate_bst, "stats", default=0, target_type=int)
```

**How `calc` works under the hood:**
* **The Interception:** Incorporator sees the `"stats"` key. Instead of auto-nesting it into objects, it pauses.
* **The Extraction (`*input_keys`):** By passing `"stats"` as the second argument, Incorporator extracts the raw JSON list and passes it directly into your `calculate_bst` function.
* **The Reduction:** Your pure Python function iterates over the list, sums the integers, and returns a single integer (e.g., `318`).
* **The Type Guarantee:** Because we passed `target_type=int`, Incorporator strictly validates the output. 

*Memory Benefit:* The massive raw JSON array is immediately garbage-collected. Your final Python object only stores a single highly-efficient `int` footprint.

### 3. Cleaning the Graph with `name_chg`
After `calc()` successfully reduces the massive `stats` array into a single integer, the attribute on your Python object is still technically named `.stats`. 

While functionally correct, `pokemon.stats` implies it might be a list or object. To make our object-oriented API perfectly clean, we rename it dynamically during instantiation:

```python
name_chg=[("stats", "base_stat_total")]
```
Now, the attribute is securely attached as `pokemon.base_stat_total`, giving you beautiful, highly readable dot-notation.

### 4. Dropping Heavy Payloads (`excl_lst`)
Before Incorporator even looks at `calc` or auto-nesting, it processes `excl_lst`. 
```python
excl_lst=["sprites", "moves", "game_indices", "held_items"]
```
The PokéAPI returns massive base64 strings and thousand-item lists for `moves`. By excluding them, the JSON keys are deleted from the payload the millisecond they are received. This prevents `pydantic` from wasting CPU cycles validating data you never intend to use.

---

## 🌟 Summary
With **Incorporator**, you aren't just ingesting APIs—you are sculpting them. 

Using the explicit `inc_child` state carrier and declarative `calc` tokens, you can take 150 deeply nested, fractured JSON payloads and compress them into a highly optimized, flat list of native Python objects in just a fraction of a second.

---

## 🐳 Run it from the CLI

The CLI handles user-defined reducers via an **`inflow.py` sidecar** — a single Python file containing the helper functions your pipeline.json references. No fjord wrapper, no outflow function, no second class. Just a vanilla stream pipeline that uses your reducer.

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

The token resolver imports `inflow.py` at config-load time, sees `calculate_bst` in its public symbols, and resolves the `calc(...)` string to a real Python callable before the engine runs. The reducer runs **before** format dispatch, so this exact pipeline.json works for any export format — switch the extension to `.csv`, `.parquet`, `.avro`, etc., and the integer still lands in the cell.

> **Tip:** for paginators and pre-built converter instances, use the cleaner `@name` syntax. Define `next_page = NextUrlPaginator("next")` in `inflow.py`, then reference it as `"inc_page": "@next_page"` in pipeline.json — zero JSON escape characters. See [the CLI guide](../../../docs/cli_and_configuration.md#text-form-tokens-paginators-converters-etc) for the full pattern.

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the canonical parent-child intro (no calc layer) | [Tutorial 3 — Parent-Child Drilling](../../03-parent-child-drilling/README.md) |
| Apply `calc()` reductions in a fjord outflow | [Appendix — NASCAR Fantasy Fjord](../nascar-fantasy-fjord/README.md) |
| Stream paginated APIs with custom paginators | [Streaming & Pagination Deep Dive](../../../docs/streaming_and_pagination.md) |
| Land the reduced output in a warehouse | [Tutorial 2 — Universal Formats](../../02-universal-formats/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/pokeapi-etl/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)