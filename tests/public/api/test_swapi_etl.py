"""Integration tests for SWAPI Relational Mapping and Type Engine."""

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
async def mock_swapi_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the SWAPI REST endpoints."""
    if "planets" in url:
        payload = {
            "next": None,
            "results": [
                {
                    "name": "Tatooine",
                    "url": "https://swapi.dev/api/planets/1/"
                }
            ]
        }
    elif "films" in url:
        payload = {
            "next": None,
            "results": [
                {
                    "title": "A New Hope",
                    "url": "https://swapi.dev/api/films/1/"
                },
                {
                    "title": "The Empire Strikes Back",
                    "url": "https://swapi.dev/api/films/2/"
                }
            ]
        }
    elif "people" in url:
        payload = {
            "next": None,
            "results": [
                {
                    "name": "Luke Skywalker",
                    "height": "172",  # String number (needs calc float)
                    "mass": "77",  # String number (needs calc float)
                    "homeworld": "https://swapi.dev/api/planets/1/",  # Needs link_to
                    "films": [
                        "https://swapi.dev/api/films/1/",
                        "https://swapi.dev/api/films/2/"
                    ],  # Needs link_to_list
                    "url": "https://swapi.dev/api/people/1/"
                }
            ]
        }
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_swapi_relational_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves type casting (string to float) and cross-endpoint relational mapping."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_swapi_execute_get)
    BASE_URL = "https://swapi.dev/api"

    # 1. Fetch Graph Nodes
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results",
        inc_code="id", inc_name="title",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    # 2. Fetch Graph Edges (People)
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={
            "url": extract_url_id(int),
            "height": calc(float, default=0.0, target_type=flt),
            "mass": calc(float, default=0.0, target_type=flt),
            "homeworld": calc(link_to(planets, extractor=extract_url_id(int)), default=None),
            "films": calc(link_to_list(films, extractor=extract_url_id(int)), default=[])
        },
        name_chg=[("url", "id")]
    )

    # 3. Assertions
    assert isinstance(planets, list) and len(planets) == 1
    assert isinstance(films, list) and len(films) == 2
    assert isinstance(people, list) and len(people) == 1

    luke = people[0]

    # Verify basic mapping
    assert luke.inc_name == "Luke Skywalker"

    # Verify `calc(float)` successfully cast the strings to floats
    assert isinstance(luke.height, float)
    assert luke.height == 172.0
    assert isinstance(luke.mass, float)
    assert luke.mass == 77.0

    # Verify `link_to` successfully mapped the homeworld URL to the Planet object
    assert luke.homeworld is not None
    assert getattr(luke.homeworld, "inc_name") == "Tatooine"

    # Verify `link_to_list` successfully mapped the film URLs to Film objects
    assert isinstance(luke.films, list)
    assert len(luke.films) == 2
    assert getattr(luke.films[0], "inc_name") == "A New Hope"
    assert getattr(luke.films[1], "inc_name") == "The Empire Strikes Back"

    print("🚀 Jumping to Hyperspace... Connecting to the Star Wars API...\n")
    BASE_URL = "https://swapi.dev/api"

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA (The Graph Nodes)
    # ==========================================
    print("⏳ Downloading Planetary and Cinematic Archives...")
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results",
        inc_code="id", inc_name="title",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 2. FETCH PEOPLE & MAP RELATIONS (The Graph Edges)
    # ==========================================
    print("⏳ Downloading Personnel Records and mapping relationships...")
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={
            "url": extract_url_id(int),
            # Implicitly passes the 'height' and 'mass' strings to float()
            "height": calc(float, default=0.0, target_type=flt),
            "mass": calc(float, default=0.0, target_type=flt),
            # Instantly links the URL strings to our in-memory Planet and Film objects
            "homeworld": calc(link_to(planets, extractor=extract_url_id(int)), default=None),
            "films": calc(link_to_list(films, extractor=extract_url_id(int)), default=[])
        },
        name_chg=[("url", "id")]
    )

    # ==========================================
    # 3. LORE TABLES & DATA MANIPULATION
    # ==========================================
    if isinstance(people, list):

        # --- TABLE 1: THE JEDI ARCHIVES ---
        tallest_people = sorted(people, key=lambda p: getattr(p, "height", 0.0), reverse=True)

        print("\n" + "=" * 85)
        print(" 📖 TABLE 1: THE JEDI ARCHIVES (Top 10 Tallest Characters)")
        print("=" * 85)
        print(f"{'NAME':<25} | {'HEIGHT (cm)':<12} | {'MASS (kg)':<12} | {'HOMEWORLD'}")
        print("-" * 85)
        for p in tallest_people[:10]:
            h = getattr(p, "height", 0.0)
            m = getattr(p, "mass", 0.0)
            hw = getattr(p, "homeworld", None)
            hw_name = str(getattr(hw, "inc_name", "Unknown")) if hw else "Unknown"
            print(f"{p.inc_name:<25} | {h:<12} | {m:<12} | {hw_name}")

        # --- TABLE 2: CINEMATIC DEBUTS ---
        most_prolific = sorted(people, key=lambda p: len(getattr(p, "films", [])), reverse=True)

        print("\n" + "=" * 85)
        print(" 🎬 TABLE 2: CINEMATIC DEBUTS (Top 5 Most Prolific Characters)")
        print("=" * 85)
        print(f"{'CHARACTER':<25} | {'TOTAL FILMS':<12} | {'FIRST APPEARANCE'}")
        print("-" * 85)
        for p in most_prolific[:5]:
            f_lst = getattr(p, "films", [])
            f_name = str(getattr(f_lst[0], "inc_name", "Unknown")) if f_lst else "Unknown"
            print(f"{p.inc_name:<25} | {len(f_lst):<12} | {f_name}")

        # --- TABLE 3: PLANETARY DEMOGRAPHICS ---
        # Grouping logic: Count citizens per planet
        planet_groups: Dict[str, List[str]] = {}
        for p in people:
            hw = getattr(p, "homeworld", None)
            hw_name = str(getattr(hw, "inc_name", "Unknown")) if hw else "Unknown"
            if hw_name not in planet_groups:
                planet_groups[hw_name] = []
            planet_groups[hw_name].append(str(getattr(p, "inc_name", "Unknown")))

        sorted_planets = sorted(planet_groups.items(), key=lambda x: len(x[1]), reverse=True)

        print("\n" + "=" * 85)
        print(" 🌍 TABLE 3: PLANETARY DEMOGRAPHICS (Most Populated)")
        print("=" * 85)
        for hw_name, citizens in sorted_planets[:5]:
            # Show the count, and a preview of up to 3 citizens
            preview = ", ".join(citizens[:3]) + ("..." if len(citizens) > 3 else "")
            print(f"🪐 {hw_name:<15} ({len(citizens):>2} citizens) -> {preview}")

        print("\n" + "=" * 85 + "\n")