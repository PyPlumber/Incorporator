"""Integration tests for Advanced Relational Mapping and Rick & Morty Lore Tables."""

import json
from typing import Any

import httpx
import pytest

from incorporator import (
    Incorporator,
    link_to,
    link_to_list,
    json_path_extractor,
    extract_url_id,
    pluck
)


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Mocks a curated slice of the R&M Multiverse to support the Lore Tables."""

    # 1. LOCATIONS
    if "location" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {"id": 1, "name": "Earth (C-137)", "type": "Planet", "dimension": "Dimension C-137"},
                {"id": 3, "name": "Citadel of Ricks", "type": "Space station", "dimension": "unknown"},
                {"id": 9, "name": "Cronenberg Earth", "type": "Planet", "dimension": "Cronenberg Dimension"}
            ]
        }
    # 2. EPISODES
    elif "episode" in url:
        payload = {
            "info": {"next": None},
            "results": [{
                "id": 28, "name": "The Ricklantis Mixup", "episode": "S03E07",
                "characters": ["https://api.com/character/1", "https://api.com/character/2",
                               "https://api.com/character/15"]
            }]
        }
    # 3. CHARACTERS
    else:
        payload = {
            "info": {"next": None},
            "results": [
                # Rick Sanchez: Citadel Resident, Alive
                {"id": 1, "name": "Rick Sanchez", "status": "Alive", "gender": "Male", "type": "", "species": "Human",
                 "origin": {"url": "https://api.com/location/1"}, "location": {"url": "https://api.com/location/3"},
                 "episode": ["https://api.com/episode/28"]},

                # Morty Smith: Earth Resident, Alive
                {"id": 2, "name": "Morty Smith", "status": "Alive", "gender": "Male", "type": "", "species": "Human",
                 "origin": {"url": "https://api.com/location/1"}, "location": {"url": "https://api.com/location/1"},
                 "episode": ["https://api.com/episode/28"]},

                # Aqua Rick: Cronenberg Resident, Dead Rick (Lived 3 episodes)
                {"id": 15, "name": "Aqua Rick", "status": "Dead", "gender": "Male", "type": "Fish-Person",
                 "species": "Humanoid", "origin": {"url": "https://api.com/location/9"},
                 "location": {"url": "https://api.com/location/9"}, "episode": ["https://api.com/episode/28"] * 3},

                # Maximums Rickimus: Citadel Resident, Dead Rick (Lived 10 episodes)
                {"id": 99, "name": "Maximums Rickimus", "status": "Dead", "gender": "Male", "type": "",
                 "species": "Human", "origin": {"url": "https://api.com/location/3"},
                 "location": {"url": "https://api.com/location/3"}, "episode": ["https://api.com/episode/28"] * 10}
            ]
        }

    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_rick_and_morty_lore_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validates Relational Graph sorting, array length processing, and variety extraction."""
    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)
    BASE_URL = "https://api.com"
    rm_pagination = json_path_extractor("info", "next")

    # ==========================================
    # 1. BUILD THE GRAPH PIPELINE
    # ==========================================
    locations = await Incorporator.incorp(
        url=f"{BASE_URL}/location/", rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['url', 'residents']
    )

    episodes = await Incorporator.incorp(
        url=f"{BASE_URL}/episode/", rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['url']
    )

    characters = await Incorporator.incorp(
        url=f"{BASE_URL}/character/", rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['image', 'url'],
        conv_dict={
            'location': link_to(locations, extractor=pluck("url", extract_url_id())),
            'origin': link_to(locations, extractor=pluck("url", extract_url_id())),
            'episode': link_to_list(episodes, extractor=extract_url_id())
        }
    )

    assert isinstance(locations, list) and isinstance(characters, list) and isinstance(episodes, list)

    # ==========================================
    # 2. VALIDATE TABLE 1: The Citadel Census
    # ==========================================
    citadel_residents = [c for c in characters if getattr(c.location, "code", None) == 3]
    assert len(citadel_residents) == 2  # Rick Sanchez & Maximums Rickimus

    # Validate the "Variant Type" logic
    aqua_rick = characters.codeDict.get(15)
    assert aqua_rick is not None
    spec_type = f"{getattr(aqua_rick, 'species')} ({getattr(aqua_rick, 'type')})"
    assert spec_type == "Humanoid (Fish-Person)"

    # ==========================================
    # 3. VALIDATE TABLE 2: Top Dead Ricks
    # ==========================================
    dead_ricks = [
        c for c in characters
        if "Rick" in getattr(c, "name", "") and getattr(c, "status", "") == "Dead"
    ]
    # Sort descending by the length of their `episode` array
    dead_ricks.sort(key=lambda r: len(getattr(r, "episode", [])), reverse=True)

    assert len(dead_ricks) == 2
    # Maximums Rickimus (10 eps) should beat Aqua Rick (3 eps)
    assert dead_ricks[0].name == "Maximums Rickimus"
    assert dead_ricks[1].name == "Aqua Rick"

    # ==========================================
    # 4. VALIDATE TABLE 3: The Ricklantis Mixup Cast (Sorted)
    # ==========================================
    ep28 = episodes.codeDict.get(28)
    assert ep28 is not None

    cast_url_list = getattr(ep28, "characters", [])
    assert len(cast_url_list) == 3

    # 1. Resolve actors
    actors = []
    for actor_url in cast_url_list:
        actor = characters.codeDict.get(extract_url_id(int)(actor_url))
        if actor: actors.append(actor)

    # 2. Sort actors alphabetically by Current Location Name
    actors.sort(key=lambda a: getattr(getattr(a, "location", None), "name", "Unknown"))

    # 3. Assert Alphabetical Location Sort:
    # "Citadel of Ricks" (Rick) -> "Cronenberg Earth" (Aqua Rick) -> "Earth (C-137)" (Morty)
    assert getattr(actors[0].location, "name") == "Citadel of Ricks"
    assert actors[0].name == "Rick Sanchez"

    assert getattr(actors[1].location, "name") == "Cronenberg Earth"
    assert actors[1].name == "Aqua Rick"

    assert getattr(actors[2].location, "name") == "Earth (C-137)"
    assert actors[2].name == "Morty Smith"