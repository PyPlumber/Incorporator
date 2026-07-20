"""
Advanced ETL Tutorial: The Pokedex Power Rankings
--------------------------------------------------
This example demonstrates how to use Incorporator for HATEOAS (Parent/Child)
deep enrichment and Declarative ETL.

Instead of generating deeply nested Python objects for 'stats' and 'types',
we use `calc()` to intercept the raw JSON arrays, run custom Python reduction
functions on them, and flatten them into simple strings and integers.

``Nav``/``Pokemon`` and the ``calculate_bst``/``format_typing`` reducers are
defined ONCE, here. ``inflow.py`` (the sibling CLI sidecar for
``watershed.json``) re-exports them via a plain import, rather than
redefining them, so both entry forms operate on the exact same
class/function objects -- see ``inflow.py``'s own docstring. This file
never builds a Watershed in-process -- ``incorporator tideweaver run
watershed.json`` is a separate CLI process that imports FROM here, the same
direction as ``examples/05-parent-child-drilling``.

Run with:
    python examples/appendix/pokeapi-etl/pokeapi_etl_calc.py
"""

import asyncio
import operator
from typing import Any

from incorporator import Incorporator, register_host_penstock
from incorporator.io.pagination import NextUrlPaginator
from incorporator.schema.converters import calc

# Pace pokeapi.co at 1.5 req/sec (90/min -- under the 100/min documented
# ceiling). The framework ships penstock-agnostic; register explicitly
# at startup.
register_host_penstock("pokeapi.co", rate_per_sec=1.5)


class Nav(Incorporator):
    """Shallow discovery registry -- name + HATEOAS url."""


class Pokemon(Incorporator):
    """Enriched detail registry, drilled per Pokemon via inc_parent."""


def calculate_bst(stats_array: list[dict[str, Any]]) -> int:
    """Base Stat Total -- sum each stat entry's base_stat."""
    return sum(stat["base_stat"] for stat in stats_array)


def format_typing(types_array: list[dict[str, Any]]) -> str:
    """Format the nested types array as 'Grass / Poison'."""
    return " / ".join(t["type"]["name"].capitalize() for t in types_array)


async def main() -> None:
    print("Booting up the Pokedex Terminal...")
    BASE_URL = "https://pokeapi.co/api/v2"

    # ==========================================
    # 1. PHASE 1: SHALLOW DISCOVERY
    # ==========================================
    print("Running Phase 1: Shallow Discovery (Fetching 150 records)...")
    # PokeAPI's free tier documents a 100 req/min ceiling. The
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

    print(f"Discovered {len(pokemon_nav)} Pokemon. Commencing deep scan...")

    # ==========================================
    # 2. PHASE 2: DEEP ENRICHMENT (HATEOAS)
    # ==========================================
    # Showcasing the State Carrier: `incorp` automatically reads the `inc_child`
    # path ("url") directly off the `pokemon_nav` list wrapper and concurrently
    # fetches all 150 URLs seamlessly.
    #
    # The host-level ``register_host_penstock`` at module top paces the 150
    # child drills inside PokeAPI's 100 req/min budget -- without it the
    # default 15 req/sec would 429 most of them. The per-call
    # ``requests_per_second=1.5`` mirrors the host registration at the
    # call site. Total wall-clock: ~100 s.
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

    print(f"Enrichment Complete. Loaded {len(enriched_pokemon)} Pokemon into memory.")

    # ==========================================
    # 3. LORE TABLE: The Gen 1 Power Rankings
    # ==========================================
    enriched_pokemon.sort(key=operator.attrgetter("base_stat_total"), reverse=True)

    print("\n" + "=" * 90)
    print("TABLE 1: KANTO POWER RANKINGS (Sorted by Base Stat Total)")
    print("    Showcasing: `inc_parent` Deep-Drill and `calc` Array Reductions.")
    print("=" * 90)
    print(f"{'POKEMON':<20} | {'BASE STAT TOTAL':<18} | {'PRIMARY TYPING':<25} | {'WEIGHT (hg)'}")
    print("-" * 90)

    # Display the Top 15 strongest Pokemon
    for p_rich in enriched_pokemon[:15]:
        name = p_rich.inc_name.capitalize()
        print(f"{name:<20} | {p_rich.base_stat_total:<18} | {p_rich.types:<25} | {p_rich.weight}")

    print("=" * 90 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
