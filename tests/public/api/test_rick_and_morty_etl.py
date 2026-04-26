"""Integration tests for Graph Relational Mapping and Declarative ETL (Rick & Morty API)."""

import json
from datetime import datetime
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.converters import (
    calc,
    extract_url_id,
    inc,
    link_to,
    link_to_list,
    pluck
)
from incorporator.methods.paginate import NextUrlPaginator


# --- EXPLICIT SUBCLASSING ---
class Location(Incorporator): pass


class Episode(Incorporator): pass


class Character(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the Rick & Morty REST endpoints with enriched data for ETL testing."""
    if "location" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {
                    "id": 20,
                    "name": "Earth (Replacement Dimension)",
                    "type": "Planet",
                    "url": "https://rickandmortyapi.com/api/location/20",
                    # Added residents to test the `calc` population logic
                    "residents": [
                        "https://rickandmortyapi.com/api/character/1",
                        "https://rickandmortyapi.com/api/character/2"
                    ]
                }
            ]
        }
    elif "episode" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {
                    "id": 1,
                    "name": "Pilot",
                    "episode": "S01E01",
                    "url": "https://rickandmortyapi.com/api/episode/1",
                    # Added air_date to test `inc(datetime)`
                    "air_date": "December 2, 2013",
                    "characters": [
                        "https://rickandmortyapi.com/api/character/1",
                        "https://rickandmortyapi.com/api/character/2"
                    ]
                }
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
                    "location": {
                        "name": "Earth (Replacement Dimension)",
                        "url": "https://rickandmortyapi.com/api/location/20"
                    },
                    "episode": ["https://rickandmortyapi.com/api/episode/1"],
                    "url": "https://rickandmortyapi.com/api/character/1"
                },
                {
                    "id": 2,
                    "name": "Morty Smith",
                    "status": "Dead",  # Set to Dead to test body count logic
                    "species": "Human",
                    "location": {
                        "name": "Earth (Replacement Dimension)",
                        "url": "https://rickandmortyapi.com/api/location/20"
                    },
                    "episode": ["https://rickandmortyapi.com/api/episode/1"],
                    "url": "https://rickandmortyapi.com/api/character/2"
                }
            ]
        }
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_rick_and_morty_advanced_etl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves Declarative ETL (calc, inc) and Graph Relational Mapping (link_to)."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_get)
    BASE_URL = "https://rickandmortyapi.com/api"

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA
    # ==========================================
    locations = await Location.incorp(
        inc_url=f"{BASE_URL}/location/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['url'],
        inc_page=NextUrlPaginator("info", "next"),
        conv_dict={
            # TEST: Dynamic population calculation
            'population': calc(len, 'residents', default=0)
        }
    )

    episodes = await Episode.incorp(
        inc_url=f"{BASE_URL}/episode/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['url'],
        inc_page=NextUrlPaginator("info", "next"),
        conv_dict={
            # TEST: Automatic datetime parsing
            'air_date': inc(datetime)
        }
    )

    # ==========================================
    # 2. FETCH CHARACTERS & MAP RELATIONS
    # ==========================================
    characters = await Character.incorp(
        inc_url=f"{BASE_URL}/character/", rec_path="results",
        inc_code="id", inc_name="name", excl_lst=['image', 'url'],
        inc_page=NextUrlPaginator("info", "next"),
        conv_dict={
            'location': link_to(locations, extractor=pluck("url", extract_url_id(int))),
            'episode': link_to_list(episodes, extractor=extract_url_id(int))
        }
    )

    # ==========================================
    # 3. ASSERTIONS
    # ==========================================
    assert isinstance(locations, list) and len(locations) == 1
    assert isinstance(episodes, list) and len(episodes) == 1
    assert isinstance(characters, list) and len(characters) == 2

    # --- Verify `calc` Population Logic ---
    earth = locations[0]
    assert getattr(earth, "population") == 2

    # --- Verify `inc(datetime)` Logic ---
    pilot = episodes[0]
    assert isinstance(getattr(pilot, "air_date"), datetime)
    assert getattr(pilot, "air_date").year == 2013
    assert getattr(pilot, "air_date").month == 12

    # --- Verify Graph Relations & Data ---
    rick = characters[0]
    morty = characters[1]

    # Verify Basic Types
    assert rick.inc_name == "Rick Sanchez"
    assert rick.status == "Alive"
    assert morty.inc_name == "Morty Smith"
    assert morty.status == "Dead"

    # Verify `link_to` + `pluck` Success
    assert rick.location is not None
    assert getattr(rick.location, "inc_name") == "Earth (Replacement Dimension)"

    # Verify `link_to_list` Success
    assert isinstance(rick.episode, list)
    assert len(rick.episode) == 1
    assert getattr(rick.episode[0], "inc_name") == "Pilot"

    # ==========================================
    # LORE TABLE 1: The Dimensional Census
    # ==========================================
    print("=" * 90)
    print(" 🪐 TABLE 1: THE DIMENSIONAL CENSUS (Top 10 Most Populated Locations)")
    print("    Showcasing: `calc()` dynamically generating a 'population' integer.")
    print("=" * 90)
    print(f"{'LOCATION NAME':<40} | {'TYPE':<20} | {'POPULATION':<15}")
    print("-" * 90)

    if isinstance(locations, list):
        # Sort by our dynamically calculated 'population' attribute
        sorted_locs = sorted(locations, key=lambda x: getattr(x, 'population', 0), reverse=True)
        for loc in sorted_locs[:10]:
            print(f"{loc.inc_name:<40} | {getattr(loc, 'type', 'Unknown'):<20} | {getattr(loc, 'population', 0):<15}")

    # ==========================================
    # LORE TABLE 2: The "Deadliest" Episodes
    # ==========================================
    print("\n" + "=" * 90)
    print(" 💀 TABLE 2: THE DEADLIEST EPISODES (Highest Mortality Rate)")
    print("    Showcasing: `inc(datetime)` parsing and Deep Graph Traversal.")
    print("=" * 90)
    print(f"{'EPISODE':<10} | {'TITLE':<35} | {'AIR DATE':<12} | {'BODY COUNT':<10}")
    print("-" * 90)

    if isinstance(episodes, list) and isinstance(characters, list):
        ep_stats = []
        for ep in episodes:
            cast_urls = getattr(ep, "characters", [])
            body_count = 0

            # Traverse the graph to check the status of every character in the episode
            for url in cast_urls:
                char_id = extract_url_id(int)(url)
                character = characters.inc_dict.get(char_id)
                if character and getattr(character, "status", "") == "Dead":
                    body_count += 1

            ep_stats.append((ep, body_count))

        # Sort by highest body count
        ep_stats.sort(key=lambda x: x[1], reverse=True)

        for ep, count in ep_stats[:10]:
            # Format the datetime object that inc(datetime) generated for us!
            air_date = getattr(ep, "air_date", None)
            date_str = air_date.strftime("%Y-%m-%d") if isinstance(air_date, datetime) else "Unknown"
            ep_code = getattr(ep, "episode", "Unknown")

            print(f"{ep_code:<10} | {ep.inc_name:<35} | {date_str:<12} | {count:<10}")

    # ==========================================
    # LORE TABLE 3: The Ricklantis Mixup Cast
    # ==========================================
    print("\n" + "=" * 90)
    print(" 🎬 TABLE 3: CAST OF S03E07 (Sorted by Status: Unknown -> Dead -> Alive)")
    print("    Showcasing: Direct O(1) Lookups via `inc_dict`.")
    print("=" * 90)
    print(f"{'ACTOR NAME':<30} | {'STATUS':<10} | {'GENDER':<10} | {'CURRENT LOCATION':<25}")
    print("-" * 90)

    if isinstance(episodes, list) and isinstance(characters, list):
        ep28 = episodes.inc_dict.get(28)

        if ep28 and getattr(ep28, "characters", None):
            cast_url_list = getattr(ep28, "characters")
            actors = []

            for url in cast_url_list:
                actor_id = extract_url_id(int)(url)
                actor_obj = characters.inc_dict.get(actor_id)
                if actor_obj:
                    actors.append(actor_obj)

            # Sort alphabetically within Status groups
            actors.sort(
                key=lambda a: (
                    {"alive": 2, "dead": 1}.get(str(getattr(a, "status", "Unknown")).lower(), 0),
                    getattr(a, "inc_name", "Unknown")
                )
            )

            for actor in actors[:15]:
                status = getattr(actor, "status", "Unknown")
                gender = getattr(actor, "gender", "Unknown")
                loc = getattr(actor, "location", None)
                loc_name = str(getattr(loc, "inc_name", "Unknown")) if loc else "Unknown"

                print(f"{str(actor.inc_name):<30} | {status:<10} | {gender:<10} | {loc_name:<25}")

    print("=" * 90 + "\n")
