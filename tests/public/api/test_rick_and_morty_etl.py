"""Integration tests for Advanced Relational Mapping and URL Extraction."""

import json
from datetime import datetime
from typing import Any

import httpx
import pytest

from incorporator import (
    Incorporator,
    to_date,
    cast_list_items,
    link_to,
    link_to_list,
    json_path_extractor,
    extract_url_id,
    pluck
)


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Mocks a paginated API response for Locations, Episodes, and Characters."""

    if "location" in url:
        payload = {
            "info": {"next": None},
            "results": [{
                "id": 1, "name": "Earth (C-137)", "url": "https://api.com/location/1",
                "residents": ["https://api.com/character/1", "https://api.com/character/2"]
            }]
        }
    elif "episode" in url:
        payload = {
            "info": {"next": None},
            "results": [{
                "id": 1, "name": "Pilot", "air_date": "December 2, 2013",
                "url": "https://api.com/episode/1",
                "characters": ["https://api.com/character/1", "https://api.com/character/2"]
            }]
        }
    elif "character/?page=2" in url:
        payload = {
            "info": {"next": None},
            "results": [{
                "id": 2, "name": "Morty Smith", "url": "https://api.com/character/2",
                "location": {"name": "Earth", "url": "https://api.com/location/1"},
                "origin": {"name": "Earth", "url": "https://api.com/location/1"},
                "episode": ["https://api.com/episode/1"]
            }]
        }
    else:  # Character Page 1
        payload = {
            "info": {"next": "https://api.com/character/?page=2"},
            "results": [{
                "id": 1, "name": "Rick Sanchez", "url": "https://api.com/character/1",
                "location": {"name": "Earth", "url": "https://api.com/location/1"},
                "origin": {"name": "Earth", "url": "https://api.com/location/1"},
                "episode": ["https://api.com/episode/1"]
            }]
        }

    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_rick_and_morty_relational_etl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests URL ID extraction, nested dict plucking, and in-memory relational linking."""

    # 1. Intercept the network layer
    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)

    # 2. Execute the Pipeline
    BASE_URL = "https://api.com"

    locations = await Incorporator.incorp(
        url=f"{BASE_URL}/location/",
        rPath="results",
        paginate=True, next_url_extractor=json_path_extractor("info", "next"),
        code="id", name="name", excl_lst=['url'],
        conv_dict={'residents': cast_list_items(extract_url_id())}
    )

    episodes = await Incorporator.incorp(
        url=f"{BASE_URL}/episode/",
        rPath="results",
        paginate=True, next_url_extractor=json_path_extractor("info", "next"),
        code="id", name="name", excl_lst=['url'],
        conv_dict={
            'air_date': to_date,
            'characters': cast_list_items(extract_url_id())
        }
    )

    characters = await Incorporator.incorp(
        url=f"{BASE_URL}/character/",
        rPath="results",
        paginate=True, next_url_extractor=json_path_extractor("info", "next"),
        code="id", name="name", excl_lst=['image', 'url'],
        conv_dict={
            'location': link_to(locations, extractor=pluck("url", extract_url_id())),
            'origin': link_to(locations, extractor=pluck("url", extract_url_id())),
            'episode': link_to_list(episodes, extractor=extract_url_id())
        }
    )

    # 3. Assertions
    assert isinstance(locations, list) and len(locations) == 1
    assert isinstance(episodes, list) and len(episodes) == 1
    assert isinstance(characters, list) and len(characters) == 2  # Proves pagination worked!

    # Look up Morty by Primary Key in the global registry
    morty = characters.codeDict[2]

    # Assert top level attrs
    assert getattr(morty, "name") == "Morty Smith"

    # Assert Relational Magic: The location should be an actual object, not a dict/string
    morty_loc = getattr(morty, "location")
    assert morty_loc is not None
    assert getattr(morty_loc, "name") == "Earth (C-137)"

    # Assert Relational List Magic: The episode should be a list of Episode objects
    morty_episodes = getattr(morty, "episode")
    assert isinstance(morty_episodes, list)
    assert len(morty_episodes) == 1

    first_episode = morty_episodes[0]
    assert getattr(first_episode, "name") == "Pilot"

    # Assert the universal to_date parser successfully parsed the Rick & Morty custom string format
    air_date = getattr(first_episode, "air_date")
    assert isinstance(air_date, datetime)
    assert air_date.year == 2013