"""Integration tests for pure HATEOAS Graph Relational Mapping (Star Wars API)."""

import json
from collections import defaultdict
from typing import Any

import httpx
import pytest

from incorporator import (
    Incorporator,
    json_path_extractor,
    extract_url_id,
    link_to,
    link_to_list,
    to_float
)


# --- EXPLICIT SUBCLASSING ---
# Isolates the in-memory WeakValueDictionary registries so IDs don't collide
class Planet(Incorporator): pass


class Film(Incorporator): pass


class Person(Incorporator): pass


# --- MOCK NETWORK SETUP ---

async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks pure HATEOAS REST 'List' endpoints for Planets, Films, and People.
    Absorbs *args and **kwargs to safely ignore httpx.AsyncClient and RateLimiter injections.
    """

    if "planets" in url:
        payload = {
            "next": None,
            "results": [
                {"name": "Tatooine", "climate": "arid", "url": "https://swapi.dev/api/planets/1/"},
                {"name": "Alderaan", "climate": "temperate", "url": "https://swapi.dev/api/planets/2/"}
            ]
        }
    elif "films" in url:
        payload = {
            "next": None,
            "results": [
                {"title": "A New Hope", "episode_id": 4, "url": "https://swapi.dev/api/films/1/"},
                {"title": "The Empire Strikes Back", "episode_id": 5, "url": "https://swapi.dev/api/films/2/"}
            ]
        }
    elif "people" in url:
        payload = {
            "next": None,
            "results": [
                {
                    "name": "Luke Skywalker",
                    "height": "172", "mass": "77",
                    "homeworld": "https://swapi.dev/api/planets/1/",
                    "films": ["https://swapi.dev/api/films/1/", "https://swapi.dev/api/films/2/"],
                    "url": "https://swapi.dev/api/people/1/"
                },
                {
                    "name": "C-3PO",
                    "height": "167", "mass": "unknown",  # Dirty data to test native fallbacks!
                    "homeworld": "https://swapi.dev/api/planets/1/",
                    "films": ["https://swapi.dev/api/films/1/"],
                    "url": "https://swapi.dev/api/people/2/"
                }
            ]
        }
    else:
        payload = {}

    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---

@pytest.mark.asyncio
async def test_swapi_hateoas_relational_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests dynamic URL extraction, list linking, and native dirty-data cleaning."""

    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)
    BASE_URL = "https://swapi.dev/api"
    swapi_pagination = json_path_extractor("next")

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA (Planets & Films)
    # ==========================================
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results", paginate=True, next_url_extractor=swapi_pagination,
        inc_code="id", inc_name="name",
        conv_dict={
            # SWAPI gives no IDs, only URLs. We extract the ID and rename the key on the fly!
            "url": extract_url_id(int)
        },
        name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results", paginate=True, next_url_extractor=swapi_pagination,
        inc_code="id", inc_name="title",
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 2. FETCH PEOPLE & MAP RELATIONS
    # ==========================================
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results", paginate=True, next_url_extractor=swapi_pagination,
        inc_code="id", inc_name="name",
        conv_dict={
            "url": extract_url_id(int),
            "height": to_float,  # Native handling of dirty strings/commas!
            "mass": to_float,  # Native handling of dirty strings/commas!

            # Relational Magic! Uses the fast IncorporatorList mapping natively!
            "homeworld": link_to(planets, extractor=extract_url_id(int)),
            "films": link_to_list(films, extractor=extract_url_id(int))
        },
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 3. ASSERTIONS & GRAPH VALIDATION
    # ==========================================
    assert isinstance(planets, list) and len(planets) == 2
    assert isinstance(films, list) and len(films) == 2
    assert isinstance(people, list) and len(people) == 2

    # --- Sorting Algorithm Validations ---
    # 1. Validate Height Sorting
    valid_heights = [p for p in people if getattr(p, 'height', None) is not None]
    tallest_people = sorted(valid_heights, key=lambda p: getattr(p, 'height'), reverse=True)
    assert getattr(tallest_people[0], "inc_name") == "Luke Skywalker"
    assert getattr(tallest_people[1], "inc_name") == "C-3PO"

    # 2. Validate Array Length Sorting (Most Prolific)
    most_prolific = sorted(people, key=lambda p: len(getattr(p, "films", [])), reverse=True)
    assert getattr(most_prolific[0], "inc_name") == "Luke Skywalker"  # 2 films
    assert getattr(most_prolific[1], "inc_name") == "C-3PO"  # 1 film

    # 3. Validate Demographic Grouping
    planet_groups = defaultdict(list)
    for p in people:
        hw_obj = getattr(p, "homeworld", None)
        hw_name = str(getattr(hw_obj, "inc_name", "Unknown")) if hw_obj else "Unknown"
        planet_groups[hw_name].append(str(getattr(p, "inc_name", "Unknown")))

    sorted_planets = sorted(planet_groups.items(), key=lambda item: len(item[1]), reverse=True)
    assert sorted_planets[0][0] == "Tatooine"
    assert len(sorted_planets[0][1]) == 2  # Both mock characters are from Tatooine

    # ==========================================
    # 4. SHOWCASE TABLES FOR TEST LOGS
    # ==========================================
    print("\n\n" + "=" * 85)
    print(" 📖 TABLE 1: THE JEDI ARCHIVES (Top Tallest Characters)")
    print("=" * 85)
    print(f"{'NAME':<25} | {'HEIGHT (cm)':<12} | {'MASS (kg)':<12} | {'HOMEWORLD'}")
    print("-" * 85)

    for p in tallest_people[:15]:
        name = str(getattr(p, 'inc_name', 'Unknown'))
        height = str(getattr(p, 'height', '?'))
        mass = str(getattr(p, 'mass', '?'))

        hw_obj = getattr(p, "homeworld", None)
        homeworld = str(getattr(hw_obj, "inc_name", "Unknown")) if hw_obj else "Unknown"

        print(f"{name:<25} | {height:<12} | {mass:<12} | {homeworld}")

    print("\n" + "=" * 85)
    print(" 🎬 TABLE 2: CINEMATIC DEBUTS (Top Most Prolific Characters)")
    print("=" * 85)
    print(f"{'CHARACTER':<25} | {'TOTAL FILMS':<12} | {'FIRST APPEARANCE'}")
    print("-" * 85)

    for p in most_prolific[:10]:
        film_list = getattr(p, "films", [])
        total_films = len(film_list)
        first_film = getattr(film_list[0], "inc_name", "Unknown") if film_list else "Unknown"
        print(f"{str(getattr(p, 'inc_name', 'Unknown')):<25} | {str(total_films):<12} | {first_film}")

    print("\n" + "=" * 85)
    print(" 🌍 TABLE 3: PLANETARY DEMOGRAPHICS (Most Populated)")
    print("=" * 85)

    for hw, citizens in sorted_planets[:5]:
        print(f"🪐 {hw:<15} ({len(citizens):>2} citizens) -> {', '.join(citizens[:3])}...")

    print("=" * 85 + "\n")