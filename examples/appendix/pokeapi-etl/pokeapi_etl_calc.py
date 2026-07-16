"""
Advanced ETL Tutorial: The Pokédex Power Rankings
-------------------------------------------------
This example demonstrates how to use Incorporator for HATEOAS (Parent/Child)
deep enrichment and Declarative ETL.

Instead of generating deeply nested Python objects for 'stats' and 'types',
we use `calc()` to intercept the raw JSON arrays, run custom Python reduction
functions on them, and flatten them into simple strings and integers.
"""

import asyncio
from typing import Any

from incorporator import Incorporator, register_host_penstock
from incorporator.io.pagination import NextUrlPaginator
from incorporator.schema.converters import calc

# Pace pokeapi.co at 1.5 req/sec (90/min — under the 100/min documented
# ceiling).  The framework ships penstock-agnostic; register explicitly
# at startup.
register_host_penstock("pokeapi.co", rate_per_sec=1.5)


# --- EXPLICIT SUBCLASSING ---
class Nav(Incorporator):
    pass


class Pokemon(Incorporator):
    pass


# --- DECLARATIVE ETL FUNCTIONS ---
def calculate_bst(stats_array: Any) -> int:
    """Calculates Base Stat Total by summing the 'base_stat' of all entries."""
    if not isinstance(stats_array, list):
        return 0
    return sum(stat_obj.get("base_stat", 0) for stat_obj in stats_array if isinstance(stat_obj, dict))


def format_typing(types_array: Any) -> str:
    """Formats a nested types array into a clean string (e.g., 'Grass / Poison')."""
    if not isinstance(types_array, list):
        return "Unknown"
    type_names = [t.get("type", {}).get("name", "").capitalize() for t in types_array if isinstance(t, dict)]
    return " / ".join(type_names)


async def main() -> None:
    print("Booting up the Pokedex Terminal...")
    BASE_URL = "https://pokeapi.co/api/v2"

    # ==========================================
    # 1. PHASE 1: SHALLOW DISCOVERY
    # ==========================================
    print("Running Phase 1: Shallow Discovery (Fetching 150 records)...")
    # PokeAPI's free tier documents a 100 req/min ceiling.  The
    # ``register_host_penstock`` call at module top throttles every
    # ``pokeapi.co`` request to 1.5 req/sec (90/min); the explicit
    # ``requests_per_second=1.5`` kwarg below keeps the per-call knob
    # visible at the call site even with host-level registration in place.
    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        inc_child="url",
        inc_page=NextUrlPaginator("next"),
        call_lim=3,  # 3 pages * 50 = 150 Pokemon
        requests_per_second=1.5,  # 90 req/min — under PokeAPI's 100/min ceiling
    )

    print(f"Discovered {len(pokemon_nav)} Pokémon. Commencing deep scan...")

    # ==========================================
    # 2. PHASE 2: DEEP ENRICHMENT (HATEOAS)
    # ==========================================
    # Showcasing the State Carrier: `incorp` automatically reads the `inc_child_path`
    # ("url") directly off the `pokemon_nav` list wrapper and concurrently fetches
    # all 150 URLs seamlessly without throwing the Deprecation Warning!
    #
    # The host-level ``register_host_penstock`` at module top paces the 150
    # child drills inside PokeAPI's 100 req/min budget — without it, the
    # default 15 req/sec would 429 most of them.  The per-call
    # ``requests_per_second=1.5`` mirrors the host registration at the
    # call site.  Total wall-clock: ~100 s.
    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"],
        conv_dict={
            "stats": calc(calculate_bst, "stats", default=0, target_type=int),
            "types": calc(format_typing, "types", default="Unknown", target_type=str),
        },
        name_chg=[("stats", "base_stat_total")],
        requests_per_second=1.5,
    )

    print(f"Enrichment Complete. Loaded {len(enriched_pokemon)} Pokémon into memory.")

    # ==========================================
    # 3. LORE TABLE: The Gen 1 Power Rankings
    # ==========================================
    if isinstance(enriched_pokemon, list):
        # SORT LOGIC: Sort descending by our newly calculated Base Stat Total (BST)!
        enriched_pokemon.sort(key=lambda p: getattr(p, "base_stat_total", 0), reverse=True)

        print("\n" + "=" * 90)
        print("TABLE 1: KANTO POWER RANKINGS (Sorted by Base Stat Total)")
        print("    Showcasing: `inc_parent` Deep-Drill and `calc` Array Reductions.")
        print("=" * 90)
        print(f"{'POKEMON':<20} | {'BASE STAT TOTAL':<18} | {'PRIMARY TYPING':<25} | {'WEIGHT (hg)'}")
        print("-" * 90)

        # Display the Top 15 strongest Pokemon
        for p_rich in enriched_pokemon[:15]:
            name = str(getattr(p_rich, "inc_name", "N/A")).capitalize()
            bst = getattr(p_rich, "base_stat_total", 0)
            typing = str(getattr(p_rich, "types", "Unknown"))
            weight = getattr(p_rich, "weight", 0)

            print(f"{name:<20} | {bst:<18} | {typing:<25} | {weight}")

        print("=" * 90 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
