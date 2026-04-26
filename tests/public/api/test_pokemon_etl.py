"""Integration tests for the "Discovery & Enrichment" pattern using the `inc_parent` parameter."""

import asyncio
import json
from typing import Any

import httpx
import pytest

from incorporator import (
    Incorporator,

)
from incorporator.methods.converters import calc
from incorporator.methods.paginate import NextUrlPaginator


# --- EXPLICIT SUBCLASSING ---
class Nav(Incorporator): pass


class Habitat(Incorporator): pass


class Species(Incorporator): pass


class Pokemon(Incorporator): pass


# --- MOCK NETWORK SETUP ---
import json
from typing import Any
import httpx


async def mock_pokeapi_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the PokeAPI REST endpoints for Shallow Pagination and Deep Enrichment."""

    # 1. SHALLOW PAGINATION MOCKS
    if "offset=0" in url:
        payload = {
            "next": "https://pokeapi.co/api/v2/pokemon/?limit=50&offset=50",
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
                {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/"}
            ]
        }
    elif "offset=50" in url:
        payload = {
            "next": None,  # End of pagination for the mock
            "results": [
                {"name": "mewtwo", "url": "https://pokeapi.co/api/v2/pokemon/150/"}
            ]
        }

    # 2. DEEP DRILL MOCKS (HATEOAS)
    elif "/pokemon/1/" in url:
        payload = {
            "id": 1,
            "name": "bulbasaur",
            "weight": 69,
            "types": [{"type": {"name": "grass"}}, {"type": {"name": "poison"}}],
            "stats": [
                {"base_stat": 45, "stat": {"name": "hp"}},
                {"base_stat": 49, "stat": {"name": "attack"}}
            ]  # BST = 94
        }
    elif "/pokemon/2/" in url:
        payload = {
            "id": 2,
            "name": "ivysaur",
            "weight": 130,
            "types": [{"type": {"name": "grass"}}, {"type": {"name": "poison"}}],
            "stats": [
                {"base_stat": 60, "stat": {"name": "hp"}},
                {"base_stat": 62, "stat": {"name": "attack"}}
            ]  # BST = 122
        }
    elif "/pokemon/150/" in url:
        payload = {
            "id": 150,
            "name": "mewtwo",
            "weight": 1220,
            "types": [{"type": {"name": "psychic"}}],
            "stats": [
                {"base_stat": 106, "stat": {"name": "hp"}},
                {"base_stat": 110, "stat": {"name": "attack"}},
                {"base_stat": 154, "stat": {"name": "special-attack"}}
            ]  # BST = 370
        }
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)

# --- DECLARATIVE ETL FUNCTIONS ---
def calculate_bst(stats_array: Any) -> int:
    """Calculates Base Stat Total by summing the 'base_stat' of all entries."""
    if not isinstance(stats_array, list): return 0
    return sum(stat_obj.get("base_stat", 0) for stat_obj in stats_array if isinstance(stat_obj, dict))


def format_typing(types_array: Any) -> str:
    """Formats a nested types array into a clean string (e.g., 'Grass / Poison')."""
    if not isinstance(types_array, list): return "Unknown"
    type_names = [t.get("type", {}).get("name", "").capitalize() for t in types_array if isinstance(t, dict)]
    return " / ".join(type_names)

# --- TESTS ---
@pytest.mark.asyncio
async def test_pokemon_parent_based_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_pokeapi_execute_get)
    print("🔴 Booting up the Pokedex Terminal...")
    BASE_URL = "https://pokeapi.co/api/v2"

    # ==========================================
    # 1. PHASE 1: SHALLOW DISCOVERY
    # ==========================================
    print("⏳ Running Phase 1: Shallow Discovery (Fetching 150 records)...")
    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        name_chg=[('url', 'detail_url')],
        inc_page=NextUrlPaginator("next"),
        call_lim=3  # 3 pages * 50 = 150 Pokemon
    )

    print(f"✅ Discovered {len(pokemon_nav)} Pokémon. Commencing deep scan...")

    # ==========================================
    # 2. PHASE 2: DEEP ENRICHMENT (HATEOAS)
    # ==========================================
    # Showcasing `inc_parent`: Automatically extracts `detail_url` from the Nav objects
    # and concurrently fetches all 150 detailed JSON payloads!
    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"],
        conv_dict={
            # Using the clean *input_keys syntax to target the 'stats' and 'types' JSON arrays
            "stats": calc(calculate_bst, "stats", default=0, target_type=int),
            "types": calc(format_typing, "types", default="Unknown", target_type=str)
        },
        name_chg=[("stats", "base_stat_total")]
    )

    # Assertions
    assert len(enriched_pokemon) == 3

    mewtwo = next(p for p in enriched_pokemon if p.inc_name == "mewtwo")
    assert getattr(mewtwo, "base_stat_total") == 370
    assert getattr(mewtwo, "types") == "Psychic"

    # ==========================================
    # 3. LORE TABLE: The Gen 1 Power Rankings
    # ==========================================
    if isinstance(enriched_pokemon, list):
        # SORT LOGIC: Sort descending by Base Stat Total (BST) to find the strongest!
        enriched_pokemon.sort(key=lambda p: getattr(p, "base_stat_total", 0), reverse=True)

        print("\n" + "=" * 90)
        print(" 🏆 TABLE 1: KANTO POWER RANKINGS (Sorted by Base Stat Total)")
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
