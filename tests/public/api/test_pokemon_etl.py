"""Integration tests for Concurrent API Fetching, Subclassing, and Relational Mapping."""

import asyncio
import json
from typing import Any, List

import httpx
import pytest

from incorporator import (
    Incorporator,
    extract_url_id,
    pluck,
    link_to
)


# --- EXPLICIT SUBCLASSING & WRAPPERS ---

class MockSpecies(Incorporator):
    pass


class MockPokemon(Incorporator):
    pass


class RegistryWrapper:
    """A wrapper to hold strong references to asyncio.gather results for link_to mapping."""

    def __init__(self, items: List[Any]) -> None:
        self.codeDict = {item.code: item for item in items}


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Mocks the REST 'Detail Endpoints' for specific Pokemon and Species."""

    if "pokemon-species/1/" in url:
        payload = {
            "id": 1, "name": "bulbasaur", "is_legendary": False, "is_mythical": False,
            "habitat": {"name": "grassland", "url": "https://pokeapi.co/api/v2/pokemon-habitat/1/"}
        }
    elif "pokemon-species/150/" in url:
        payload = {
            "id": 150, "name": "mewtwo", "is_legendary": True, "is_mythical": False,
            "habitat": {"name": "rare", "url": "https://pokeapi.co/api/v2/pokemon-habitat/5/"}
        }
    elif "pokemon/1/" in url:
        payload = {
            "id": 1, "name": "bulbasaur",
            "species": {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon-species/1/"}
        }
    elif "pokemon/150/" in url:
        payload = {
            "id": 150, "name": "mewtwo",
            "species": {"name": "mewtwo", "url": "https://pokeapi.co/api/v2/pokemon-species/150/"}
        }
    else:
        payload = {}

    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_pokemon_concurrent_detail_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests asyncio.gather concurrency, explicit subclassing, and nested dict plucking."""

    # 1. Intercept the network layer
    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)

    BASE_URL = "https://pokeapi.co/api/v2"
    target_ids = [1, 150]

    # 2. FETCH SPECIES CONCURRENTLY
    species_tasks = [
        MockSpecies.incorp(
            url=f"{BASE_URL}/pokemon-species/{pid}/",
            code="id", name="name", excl_lst=["url"],
            conv_dict={"habitat": pluck("name")}
        ) for pid in target_ids
    ]

    # Assert they are fetched concurrently and held in strong references
    loaded_species = await asyncio.gather(*species_tasks)
    assert len(loaded_species) == 2

    species_registry = RegistryWrapper(loaded_species)

    # 3. FETCH POKEMON CONCURRENTLY & MAP TO SPECIES
    pokemon_tasks = [
        MockPokemon.incorp(
            url=f"{BASE_URL}/pokemon/{pid}/",
            code="id", name="name", excl_lst=["url"],
            conv_dict={
                "species": link_to(species_registry, extractor=pluck("url", extract_url_id()))
            }
        ) for pid in target_ids
    ]

    loaded_pokemon = await asyncio.gather(*pokemon_tasks)
    assert len(loaded_pokemon) == 2

    # 4. VALIDATE THE GRAPH DATABASE
    pokemon_registry = RegistryWrapper(loaded_pokemon).codeDict

    # Validate Bulbasaur (ID 1)
    bulbasaur = pokemon_registry.get(1)
    assert bulbasaur is not None
    assert getattr(bulbasaur, "name") == "bulbasaur"

    b_species = getattr(bulbasaur, "species")
    assert getattr(b_species, "is_legendary") is False
    assert getattr(b_species, "habitat") == "grassland"

    # Validate Mewtwo (ID 150)
    mewtwo = pokemon_registry.get(150)
    assert mewtwo is not None
    assert getattr(mewtwo, "name") == "mewtwo"

    m_species = getattr(mewtwo, "species")
    assert getattr(m_species, "is_legendary") is True
    assert getattr(m_species, "habitat") == "rare"