***

# 🧬 Advanced ETL: HATEOAS & Declarative Reductions with `calc`

REST APIs are notorious for returning deeply nested, bloated arrays. 

For example, if you want a Pokémon's **Primary Types** (e.g., Grass / Poison) and its **Base Stat Total** (the sum of its HP, Attack, Defense, etc.), the PokéAPI returns massive dictionaries nested inside lists. 

Normally, traditional Pydantic setups force you to define strict Sub-Models for `Stat`, `StatDetail`, and `TypeInfo`, just so you can write a `@property` later to calculate the sum. 

**Incorporator eliminates this memory overhead.** Using `calc()` and `inc_parent`, you can concurrently fetch deep URLs, intercept raw JSON arrays, and reduce them into clean, flattened Python properties *before* the objects are even fully instantiated.

---

## 🎯 The Scenario
We are going to build a "Gen 1 Power Ranking" table. To do this, we need to:
1. **Phase 1 (Shallow Discovery):** Fetch a paginated list of 150 Pokémon, which only gives us their names and a HATEOAS URL to their deep details.
2. **Phase 2 (Deep Enrichment):** Concurrently fire 150 HTTP requests to those URLs to fetch their deep specs.
3. **Phase 3 (Declarative ETL):** Use `calc()` to intercept the massive `stats` and `types` JSON arrays, sum them up, format them, and drop the raw JSON to save memory.

---

## 💻 The Complete Code

```python
import asyncio
from typing import Any
from incorporator import Incorporator, NextUrlPaginator
from incorporator.methods.converters import calc

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
    # 1. SHALLOW DISCOVERY
    pokemon_nav = await Nav.incorp(
        inc_url="https://pokeapi.co/api/v2/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        name_chg=[('url', 'detail_url')], # Standardize the nested URL key
        inc_page=NextUrlPaginator("next"),
        call_lim=3
    )

    # 2. DEEP ENRICHMENT & ETL
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

    # 3. USE THE FLATTENED DATA
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

### 1. The HATEOAS Concurrency Engine (`inc_parent`)
REST APIs often use HATEOAS (Hypermedia as the Engine of Application State), meaning they return a shallow list of items, each containing a `"url"` to get more data.

By passing `inc_parent=pokemon_nav` to our `Pokemon` subclass, Incorporator does the heavy lifting:
1. It scans the `Nav` objects in memory.
2. It extracts the `.detail_url` (or `.url`) from each one.
3. It provisions a highly optimized, rate-limited HTTP connection pool.
4. It fires 150 concurrent requests to fetch the deep JSON payload for every single Pokémon simultaneously.

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

*Memory Benefit:* The massive raw JSON array is immediately garbage-collected. Your final Python object only stores a single `int` footprint.

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

Using `inc_parent` and `calc`, you can take 150 deeply nested, fractured JSON payloads and compress them into a highly optimized, flat list of native Python objects in just a fraction of a second.