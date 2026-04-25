"""Integration tests for the "Discovery & Enrichment" pattern using the `inc_parent` parameter."""

import asyncio
import json
from typing import Any

import httpx
import pytest

from incorporator import (
    Incorporator,
    extract_url_id,
    pluck,
    link_to
)


# --- EXPLICIT SUBCLASSING ---
class Nav(Incorporator): pass


class Habitat(Incorporator): pass


class Species(Incorporator): pass


class Pokemon(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    if "pokemon-habitat" in url:
        payload = {"results": [
            {"id": 5, "name": "rare", "url": "https://api.com/pokemon-habitat/5/"},
            {"id": 1, "name": "grassland", "url": "https://api.com/pokemon-habitat/1/"}
        ]}
    elif "pokemon-species/150" in url:
        payload = {"id": 150, "name": "mewtwo", "is_legendary": True,
                   "habitat": {"name": "rare", "url": "https://api.com/pokemon-habitat/5/"}}
    elif "pokemon-species/1" in url:
        payload = {"id": 1, "name": "bulbasaur", "is_legendary": False,
                   "habitat": {"name": "grassland", "url": "https://api.com/pokemon-habitat/1/"}}
    elif "/pokemon/150" in url:
        payload = {"id": 150, "name": "mewtwo", "base_experience": 306,
                   "species": {"name": "mewtwo", "url": "https://api.com/pokemon-species/150/"}}
    elif "/pokemon/1" in url:
        payload = {"id": 1, "name": "bulbasaur", "base_experience": 64,
                   "species": {"name": "bulbasaur", "url": "https://api.com/pokemon-species/1/"}}
    elif "/pokemon-species" in url:
        payload = {"results": [{"name": "mewtwo", "url": "https://api.com/pokemon-species/150/"},
                               {"name": "bulbasaur", "url": "https://api.com/pokemon-species/1/"}]}
    elif "/pokemon" in url:
        payload = {"results": [{"name": "mewtwo", "url": "https://api.com/pokemon/150/"},
                               {"name": "bulbasaur", "url": "https://api.com/pokemon/1/"}]}
    else:
        payload = {}
    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---
@pytest.mark.asyncio
async def test_pokemon_parent_based_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_get)
    BASE_URL = "https://api.com"

    # 1. DISCOVERY PHASE
    habitats, species_nav, pokemon_nav = await asyncio.gather(
        Habitat.incorp(inc_url=f"{BASE_URL}/pokemon-habitat/", rec_path="results", inc_code="id", inc_name="name",
                       excl_lst=['url']),
        Nav.incorp(inc_url=f"{BASE_URL}/pokemon-species/?limit=2", rec_path="results", inc_name="name",
                   name_chg=[('url', 'detail_url')]),
        Nav.incorp(inc_url=f"{BASE_URL}/pokemon/?limit=2", rec_path="results", inc_name="name",
                   name_chg=[('url', 'detail_url')])
    )

    # 2. ENRICHMENT PHASE (The Magic)
    species = await Species.incorp(
        inc_parent=species_nav,
        inc_code="id", inc_name="name", excl_lst=["url"],
        conv_dict={"habitat": link_to(habitats, extractor=pluck("url", extract_url_id(int)))}
    )

    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id", inc_name="name", excl_lst=["url"],
        conv_dict={"species": link_to(species, extractor=pluck("url", extract_url_id(int)))}
    )

    # 3. ASSERTIONS & SORTING (Sort by Base Experience)
    assert isinstance(enriched_pokemon, list) and len(enriched_pokemon) == 2

    enriched_pokemon.sort(key=lambda p: getattr(p, "base_experience", 0), reverse=True)

    assert getattr(enriched_pokemon[0], "inc_name") == "mewtwo"
    assert getattr(enriched_pokemon[1], "inc_name") == "bulbasaur"

    # 4. SHOWCASE TABLE
    print("\n\n" + "=" * 85)
    print(" ✨ TABLE 1: ENRICHMENT PHASE (Sorted by Base Experience)")
    print("=" * 85)
    print(f"{'POKEMON':<15} | {'BASE EXP':<10} | {'LEGENDARY?':<15} | {'HABITAT'}")
    print("-" * 85)
    for p_rich in enriched_pokemon:
        name = str(getattr(p_rich, "inc_name", "N/A")).capitalize()
        exp = str(getattr(p_rich, "base_experience", "N/A"))

        s_obj = getattr(p_rich, "species", None)
        is_leg = "Yes" if getattr(s_obj, "is_legendary", False) else "No"

        h_obj = getattr(s_obj, "habitat", None) if s_obj else None
        hab = str(getattr(h_obj, "inc_name", "N/A")).capitalize()

        print(f"{name:<15} | {exp:<10} | {is_leg:<15} | {hab}")
    print("=" * 85 + "\n")