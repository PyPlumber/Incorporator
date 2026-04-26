"""Integration tests for pure HATEOAS Graph Relational Mapping (Star Wars API)."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.converters import calc, extract_url_id, flt, link_to, link_to_list
from incorporator.methods.paginate import NextUrlPaginator


# --- EXPLICIT SUBCLASSING ---
class Planet(Incorporator): pass


class Film(Incorporator): pass


class Person(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """
    Mocks pure HATEOAS REST 'List' endpoints for Planets, Films, and People.
    Absorbs *args and **kwargs to safely ignore httpx.AsyncClient injections.
    """
    if "planets" in url:
        payload = {
            "next": None,
            "results":[
                {"name": "Tatooine", "climate": "arid", "url": "https://swapi.dev/api/planets/1/"},
                {"name": "Alderaan", "climate": "temperate", "url": "https://swapi.dev/api/planets/2/"}
            ]
        }
    elif "films" in url:
        payload = {
            "next": None,
            "results":[
                {"title": "A New Hope", "episode_id": 4, "url": "https://swapi.dev/api/films/1/"},
                {"title": "The Empire Strikes Back", "episode_id": 5, "url": "https://swapi.dev/api/films/2/"}
            ]
        }
    elif "people" in url:
        payload = {
            "next": None,
            "results":[
                {
                    "name": "Luke Skywalker",
                    "height": "172", "mass": "77",
                    "homeworld": "https://swapi.dev/api/planets/1/",
                    "films":["https://swapi.dev/api/films/1/", "https://swapi.dev/api/films/2/"],
                    "url": "https://swapi.dev/api/people/1/"
                },
                {
                    "name": "C-3PO",
                    "height": "167", "mass": "unknown",  # Dirty data to test native fallbacks!
                    "homeworld": "https://swapi.dev/api/planets/1/",
                    "films":["https://swapi.dev/api/films/1/"],
                    "url": "https://swapi.dev/api/people/2/"
                }
            ]
        }
    else:
        payload = {}

    # Provide a mock Request object so httpx.Response doesn't complain about raise_for_status()
    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_swapi_hateoas_relational_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests dynamic URL extraction, list linking, and native dirty-data cleaning."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_get)
    BASE_URL = "https://swapi.dev/api"

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA (Planets & Films)
    # ==========================================
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"),
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results",
        inc_code="id", inc_name="title",
        inc_page=NextUrlPaginator("next"),
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 2. FETCH PEOPLE & MAP RELATIONS
    # ==========================================
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"),
        ignore_ssl=True,
        conv_dict={
            "url": extract_url_id(int),
            "height": calc(float, default=0.0, type=flt),
            "mass": calc(float, default=0.0, type=flt),
            "homeworld": calc(link_to(planets, extractor=extract_url_id(int)), default=None),
            "films": calc(link_to_list(films, extractor=extract_url_id(int)), default=[])
        },
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 3. MOCK ASSERTIONS (The Real Test)
    # ==========================================
    assert isinstance(planets, list) and len(planets) == 2
    assert isinstance(films, list) and len(films) == 2
    assert isinstance(people, list) and len(people) == 2

    luke = people[0]
    c3po = people[1]

    # Test Type Engine & Conversions
    assert luke.inc_name == "Luke Skywalker"
    assert luke.height == 172.0
    assert luke.mass == 77.0

    # Test Dirty Data Fallback (C-3PO's 'unknown' mass must safely become 0.0)
    assert c3po.mass == 0.0

    # Test Graph Relations (Homeworld is a Planet object, Films is a list of Film objects)
    assert luke.homeworld is not None
    assert getattr(luke.homeworld, "inc_name") == "Tatooine"

    assert isinstance(luke.films, list)
    assert len(luke.films) == 2
    assert getattr(luke.films[0], "inc_name") == "A New Hope"

    assert c3po.homeworld is not None
    assert getattr(c3po.homeworld, "inc_name") == "Tatooine"

    people.sort(key=lambda p: getattr(p, "height", 0.0), reverse=True)
    print(" 📖 TABLE 1: THE JEDI ARCHIVES (Top 10 Tallest Characters)")
    print(f"{'NAME':<25} | {'HEIGHT (cm)':<12} | {'MASS (kg)':<12} | {'HOMEWORLD'}")
    print("-" * 80)
    for p in people[:10]:
        h = getattr(p, "height", 0.0)
        m = getattr(p, "mass", 0.0)
        hw = getattr(p, "homeworld", None)
        hw_name = str(getattr(hw, "inc_name", "Unknown")) if hw else "Unknown"
        print(f"{p.inc_name:<25} | {h:<12} | {m:<12} | {hw_name}")

    people.sort(key=lambda p: len(getattr(p, "films",[])), reverse=True)
    print("\n 🎬 TABLE 2: CINEMATIC DEBUTS (Top 5 Most Prolific Characters)")
    print(f"{'CHARACTER':<25} | {'TOTAL FILMS':<12} | {'FIRST APPEARANCE'}")
    print("-" * 80)
    for p in people[:5]:
        f_lst = getattr(p, "films", [])
        f_name = str(getattr(f_lst[0], "inc_name", "Unknown")) if f_lst else "Unknown"
        print(f"{p.inc_name:<25} | {len(f_lst):<12} | {f_name}")

    print("=" * 85 + "\n")