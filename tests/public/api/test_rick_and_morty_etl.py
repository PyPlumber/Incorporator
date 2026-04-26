"""Integration tests for Graph Relational Mapping with lists (Rick & Morty API)."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.converters import calc, extract_url_id, link_to, link_to_list, pluck
from incorporator.methods.paginate import NextUrlPaginator


# --- EXPLICIT SUBCLASSING ---
class Location(Incorporator): pass


class Episode(Incorporator): pass


class Character(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the Rick & Morty REST endpoints."""
    if "location" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {"id": 20, "name": "Earth (Replacement Dimension)", "type": "Planet",
                 "url": "https://rickandmortyapi.com/api/location/20"}
            ]
        }
    elif "episode" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {"id": 1, "name": "Pilot", "episode": "S01E01", "url": "https://rickandmortyapi.com/api/episode/1"},
                {"id": 2, "name": "Lawnmower Dog", "episode": "S01E02",
                 "url": "https://rickandmortyapi.com/api/episode/2"}
            ]
        }
    elif "character" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {
                    "id": 1,
                    "name": "Rick Sanchez",
                    "status": "Alive",
                    "species": "Human",
                    # Complex Nested Dict (Needs Pluck)
                    "location": {
                        "name": "Earth (Replacement Dimension)",
                        "url": "https://rickandmortyapi.com/api/location/20"
                    },
                    # Complex List of Strings (Needs link_to_list)
                    "episode": [
                        "https://rickandmortyapi.com/api/episode/1",
                        "https://rickandmortyapi.com/api/episode/2"
                    ],
                    "url": "https://rickandmortyapi.com/api/character/1"
                }
            ]
        }
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_rick_and_morty_link_to_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves link_to_list perfectly hydrates a list of URLs into a list of Incorporator objects."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_get)
    BASE_URL = "https://rickandmortyapi.com/api"

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA
    # ==========================================
    locations = await Location.incorp(
        inc_url=f"{BASE_URL}/location/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['url', 'residents'],
        inc_page=NextUrlPaginator("info", "next")
    )

    episodes = await Episode.incorp(
        inc_url=f"{BASE_URL}/episode/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['url'],
        inc_page=NextUrlPaginator("info", "next")
    )

    # ==========================================
    # 2. FETCH CHARACTERS & MAP RELATIONS
    # ==========================================
    characters = await Character.incorp(
        inc_url=f"{BASE_URL}/character/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['image', 'url'],
        inc_page=NextUrlPaginator("info", "next"),
        conv_dict={
            # Uses Pluck to dig into the {"name": "...", "url": "..."} dict and extract the URL!
            'location': calc(link_to(locations, extractor=pluck("url", extract_url_id(int))), default=None),

            # Uses link_to_list to iterate over ["url", "url"] and extract the IDs!
            'episode': calc(link_to_list(episodes, extractor=extract_url_id(int)), default=[])
        }
    )

    # ==========================================
    # 3. ASSERTIONS
    # ==========================================
    assert isinstance(locations, list) and len(locations) == 1
    assert isinstance(episodes, list) and len(episodes) == 2
    assert isinstance(characters, list) and len(characters) == 1

    rick = characters[0]

    # Verify Basic Types
    assert rick.inc_name == "Rick Sanchez"
    assert rick.species == "Human"

    # Verify `link_to` + `pluck` Success
    assert rick.location is not None
    assert getattr(rick.location, "inc_name") == "Earth (Replacement Dimension)"
    assert getattr(rick.location, "type") == "Planet"

    # Verify `link_to_list` Success
    assert isinstance(rick.episode, list)
    assert len(rick.episode) == 2

    # Assert they are fully instantiated Episode objects, not just strings!
    ep1 = rick.episode[0]
    ep2 = rick.episode[1]

    assert getattr(ep1, "inc_name") == "Pilot"
    assert getattr(ep1, "episode") == "S01E01"

    assert getattr(ep2, "inc_name") == "Lawnmower Dog"
    assert getattr(ep2, "episode") == "S01E02"

    ep28 = episodes.inc_dict.get(28)

    if ep28 and getattr(ep28, "characters", None):
        cast_url_list = getattr(ep28, "characters")

        actors = []
        for url in cast_url_list:
            # Utilizing our functional extractor manually
            actor_id = extract_url_id(int)(url)

            actor_obj = characters.inc_dict.get(actor_id)
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

        # Print the sorted table (limiting to first 15)
        for actor in actors[:15]:
            actor_name = str(getattr(actor, "inc_name", "Unknown"))
            status = getattr(actor, "status", "Unknown")
            gender = getattr(actor, "gender", "Unknown")

            loc = getattr(actor, "location", None)
            loc_name = str(getattr(loc, "inc_name", "Unknown")) if loc else "Unknown"

            print(f"{actor_name:<30} | {status:<10} | {gender:<10} | {loc_name:<30}")

    print("=" * 115 + "\n")