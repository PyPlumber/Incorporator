"""Integration tests for the Declarative ETL pipeline and API Pagination."""

import json
from typing import Any, Optional

import httpx
import pytest

from incorporator import Incorporator


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Mocks a paginated API response (similar to PokeAPI)."""
    if "page=2" not in url:
        payload = {
            "next": "http://mock-api.com/pokemon?page=2",
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/", "junk_data": "drop_me"}
            ]
        }
        return httpx.Response(200, text=json.dumps(payload))
    else:
        payload = {
            "next": None,
            "results": [
                {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/", "junk_data": "drop_me"}
            ]
        }
        return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_pokemon_etl_with_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests the full ETL pipeline: Pagination, rPath drilling, and Declarative Schema Building."""

    # 1. Intercept the network layer
    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)

    # 2. Define the custom pagination extractor (PokeAPI puts the next URL in the JSON body)
    def extract_next(raw_text: str) -> Optional[str]:
        try:
            data = json.loads(raw_text)
            next_url = data.get("next")
            return str(next_url) if next_url else None
        except json.JSONDecodeError:
            return None

    # 3. Define a custom converter to extract the Pokemon ID from the URL string
    def extract_id_from_url(url_str: Any) -> int:
        if not isinstance(url_str, str):
            return 0
        # "https://pokeapi.co/api/v2/pokemon/1/" -> splits to ["...", "1", ""] -> grabs "1"
        clean_str = url_str.strip('/')
        return int(clean_str.split('/')[-1])

    # 4. Execute the "Zero-Boilerplate" Orchestrator
    results = await Incorporator.incorp(
        url="http://mock-api.com/pokemon",
        paginate=True,
        next_url_extractor=extract_next,
        rPath="results",
        static_dct={"is_active": True, "source": "PokeAPI"},
        excl_lst=["junk_data"],
        conv_dict={"url": extract_id_from_url},
        name_chg=[("url", "dex_number"), ("name", "species")]
    )

    # 5. Assertions
    assert isinstance(results, list)
    assert len(results) == 2  # Proves pagination successfully accumulated both pages

    bulbasaur = results[0]
    ivysaur = results[1]

    # Proves `name_chg` worked
    assert getattr(bulbasaur, "species") == "bulbasaur"
    assert getattr(ivysaur, "species") == "ivysaur"

    # 'name' still exists because it is a universal Incorporator base attribute,
    # but it safely defaulted to None because the JSON data was renamed!
    assert getattr(bulbasaur, "name") is None

    # Proves `conv_dict` and `name_chg` worked together (URL was converted to int, then renamed)
    assert getattr(bulbasaur, "dex_number") == 1
    assert getattr(ivysaur, "dex_number") == 2

    # Proves `excl_lst` successfully dropped the key before schema compilation
    assert not hasattr(bulbasaur, "junk_data")

    # Proves `static_dct` injected new data into the dynamic schema
    assert getattr(bulbasaur, "is_active") is True
    assert getattr(bulbasaur, "source") == "PokeAPI"