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


# --- EXPLICIT SUBCLASSING ---
class Location(Incorporator): pass


class Episode(Incorporator): pass


class Character(Incorporator): pass


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks a curated slice of the R&M Multiverse to support the Lore Tables.
    Absorbs *args and **kwargs to safely ignore httpx.AsyncClient and RateLimiter injections.
    """

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
                "characters": ["https://api.com/character/1", "https://api.com/character/15",
                               "https://api.com/character/99"]
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

                # Cyborg Rick: Citadel Resident, Dead Rick (Lived 3 episodes)
                {"id": 15, "name": "Cyborg Rick", "status": "Dead", "gender": "Male", "type": "Cyborg",
                 "species": "Humanoid", "origin": {"url": "https://api.com/location/9"},
                 "location": {"url": "https://api.com/location/3"}, "episode": ["https://api.com/episode/28"] * 3},

                # Cronenberg Morty: Cronenberg Resident, Dead (Not a Rick, lived 1 episode)
                {"id": 99, "name": "Cronenberg Morty", "status": "Dead", "gender": "Male", "type": "Mutant",
                 "species": "Human", "origin": {"url": "https://api.com/location/9"},
                 "location": {"url": "https://api.com/location/9"}, "episode": ["https://api.com/episode/28"]}
            ]
        }

    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_rick_and_morty_lore_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validates Relational Graph sorting, array length processing, and registry resolution."""
    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)
    BASE_URL = "https://api.com"
    rm_pagination = json_path_extractor("info", "next")

    # ==========================================
    # 1. BUILD THE GRAPH PIPELINE
    # ==========================================
    locations = await Location.incorp(
        inc_url=f"{BASE_URL}/location/", rec_path="results", paginate=True, next_url_extractor=rm_pagination,
        inc_code="id", inc_name="name", excl_lst=['url', 'residents']
    )

    episodes = await Episode.incorp(
        inc_url=f"{BASE_URL}/episode/", rec_path="results", paginate=True, next_url_extractor=rm_pagination,
        inc_code="id", inc_name="name", excl_lst=['url']
    )

    characters = await Character.incorp(
        inc_url=f"{BASE_URL}/character/", rec_path="results", paginate=True, next_url_extractor=rm_pagination,
        inc_code="id", inc_name="name", excl_lst=['image', 'url'],
        conv_dict={
            'location': link_to(locations, extractor=pluck("url", extract_url_id(int))),
            'origin': link_to(locations, extractor=pluck("url", extract_url_id(int))),
            'episode': link_to_list(episodes, extractor=extract_url_id(int))
        }
    )

    assert isinstance(locations, list) and isinstance(characters, list) and isinstance(episodes, list)

    # ==========================================
    # 2. VALIDATE TABLE 1: The Citadel Census
    # ==========================================
    citadel_residents = [c for c in characters if getattr(c.location, "inc_code", None) == 3]
    assert len(citadel_residents) == 2

    cyborg_rick = characters.codeDict.get(15)
    assert cyborg_rick is not None
    spec_type = f"{getattr(cyborg_rick, 'species')} ({getattr(cyborg_rick, 'type')})"
    assert spec_type == "Humanoid (Cyborg)"

    # ==========================================
    # 3. VALIDATE TABLE 2: Top Dead Ricks
    # ==========================================
    dead_ricks = [
        c for c in characters
        if "Rick" in getattr(c, "inc_name", "") and getattr(c, "status", "") == "Dead"
    ]
    dead_ricks.sort(key=lambda r: len(getattr(r, "episode", [])), reverse=True)

    assert len(dead_ricks) == 1
    assert getattr(dead_ricks[0], "inc_name") == "Cyborg Rick"
    assert len(getattr(dead_ricks[0], "episode", [])) == 3

    # ==========================================
    # 4. VALIDATE TABLE 3: The Ricklantis Mixup Cast (Sorted Unknown -> Dead -> Alive)
    # ==========================================
    ep28 = episodes.codeDict.get(28)
    assert ep28 is not None

    cast_url_list = getattr(ep28, "characters", [])
    assert len(cast_url_list) == 3

    actors = []
    for actor_url in cast_url_list:
        actor_id = extract_url_id(int)(actor_url)
        actor_obj = characters.codeDict.get(actor_id)
        if actor_obj:
            actors.append(actor_obj)

    # SORT LOGIC:
    # 1. Map Status to custom weights: unknown(0), Dead(1), Alive(2)
    # 2. Sort alphabetically by Name within those groups
    actors.sort(
        key=lambda a: (
            {"alive": 2, "dead": 1}.get(str(getattr(a, "status", "Unknown")).lower(), 0),
            getattr(a, "inc_name", "Unknown")
        )
    )

    # Assert new sorting order: Cronenberg (Dead) -> Cyborg (Dead) -> Rick Sanchez (Alive)
    assert getattr(actors[0], "inc_name") == "Cronenberg Morty"
    assert getattr(actors[1], "inc_name") == "Cyborg Rick"
    assert getattr(actors[2], "inc_name") == "Rick Sanchez"

    # PRINT ONLY TABLE 3 (as requested)
    print("\n\n" + "=" * 115)
    print(" 🎬 TABLE 3: CAST OF S03E07 (Sorted by Status: Unknown -> Dead -> Alive)")
    print("=" * 115)
    print(f"{'ACTOR NAME':<30} | {'STATUS':<10} | {'GENDER':<10} | {'CURRENT LOCATION':<30}")
    print("-" * 115)

    for actor in actors[:15]:
        actor_name = str(getattr(actor, "inc_name", "Unknown"))
        status = getattr(actor, "status", "Unknown")
        gender = getattr(actor, "gender", "Unknown")

        loc = getattr(actor, "location", None)
        loc_name = str(getattr(loc, "inc_name", "Unknown")) if loc else "Unknown"

        print(f"{actor_name:<30} | {status:<10} | {gender:<10} | {loc_name:<30}")

    print("=" * 115 + "\n")