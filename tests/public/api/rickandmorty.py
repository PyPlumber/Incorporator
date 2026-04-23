import asyncio
from typing import Any

from incorporator import (
    Incorporator,
    link_to,
    link_to_list,
    json_path_extractor,
    extract_url_id,
    pluck
)


async def main() -> None:
    print("🌍 Opening portal to the REAL Rick and Morty API...")
    print("⏳ Please wait ~25 seconds. Our Polite Paginator is downloading the entire Multiverse...\n")

    BASE_URL = "https://rickandmortyapi.com/api"
    rm_pagination = json_path_extractor("info", "next")

    # ==========================================
    # 1. FETCH ALL LOCATIONS (7 Pages)
    # ==========================================
    print("-> Fetching Locations...")
    locations = await Incorporator.incorp(
        url=f"{BASE_URL}/location/",
        rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['url', 'residents']
    )

    # ==========================================
    # 2. FETCH ALL EPISODES (3 Pages)
    # ==========================================
    print("-> Fetching Episodes...")
    episodes = await Incorporator.incorp(
        url=f"{BASE_URL}/episode/",
        rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['url']
    )

    # ==========================================
    # 3. FETCH ALL CHARACTERS (42 Pages) & MAP RELATIONS
    # ==========================================
    print("-> Fetching Characters & Mapping Graph Relations...")
    characters = await Incorporator.incorp(
        url=f"{BASE_URL}/character/",
        rPath="results", paginate=True, next_url_extractor=rm_pagination,
        code="id", name="name", excl_lst=['image', 'url'],
        conv_dict={
            'location': link_to(locations, extractor=pluck("url", extract_url_id())),
            'origin': link_to(locations, extractor=pluck("url", extract_url_id())),
            'episode': link_to_list(episodes, extractor=extract_url_id())
        }
    )

    # ==========================================
    # 4. LORE TABLE 1: The Citadel Census (Variety Upgrade)
    # ==========================================
    print("\n\n" + "=" * 115)
    print(" 🏙️ TABLE 1: THE CITADEL OF RICKS DEMOGRAPHICS (Expanded Census)")
    print("=" * 115)
    print(
        f"{'NAME':<25} | {'GENDER (STATUS)':<18} | {'SPECIES (VARIANT TYPE)':<30} | {'ORIGIN DIMENSION':<20} | {'APPEARANCES'}")
    print("-" * 115)

    if isinstance(characters, list):
        # Filter characters whose current location ID is 3 (Citadel of Ricks)
        citadel_residents = [c for c in characters if getattr(c.location, "code", None) == 3]

        # Display the first 15 to keep the console clean
        for c in citadel_residents[:15]:
            # Combine Gender and Status
            status = getattr(c, "status", "Unknown")  # type: ignore
            gender = getattr(c, "gender", "Unknown")  # type: ignore
            gen_stat = f"{gender} ({status})"

            # Combine Species and Variant Type for extreme variety
            species = getattr(c, "species", "Unknown")  # type: ignore
            c_type = getattr(c, "type", "")  # type: ignore
            spec_type = f"{species} ({c_type})" if c_type else species

            # Deep dot-drill: Character -> Origin Location -> Dimension
            origin_dim = getattr(c.origin, "dimension", "Unknown") if getattr(c, "origin",
                                                                              None) else "Unknown"  # type: ignore

            # Calculate total episodes appeared in
            appearances = len(getattr(c, "episode", []))  # type: ignore

            print(f"{c.name:<25} | {gen_stat:<18} | {spec_type:<30} | {origin_dim:<20} | {appearances}")  # type: ignore

    # ==========================================
    # 5. LORE TABLE 2: Top 10 Longest-Lived Dead Ricks
    # ==========================================
    print("\n" + "=" * 115)
    print(" 🪦 TABLE 2: TOP 10 LONGEST-LIVED DEAD RICKS")
    print("=" * 115)
    print(f"{'DECEASED RICK':<35} | {'ORIGIN DIMENSION':<35} | {'LIFESPAN (EPISODES)'}")
    print("-" * 115)

    if isinstance(characters, list):
        # Find every character named "Rick" whose status is "Dead"
        dead_ricks = [
            c for c in characters
            if "Rick" in getattr(c, "name", "") and getattr(c, "status", "") == "Dead"
        ]

        # Sort descending by the length of their `episode` array!
        dead_ricks.sort(key=lambda r: len(getattr(r, "episode", [])), reverse=True)

        for r in dead_ricks[:10]:
            origin_dim = getattr(r.origin, "dimension", "Unknown") if getattr(r, "origin",
                                                                              None) else "Unknown"  # type: ignore
            ep_count = len(getattr(r, "episode", []))  # type: ignore

            print(f"{r.name:<35} | {origin_dim:<35} | {ep_count} Episodes")  # type: ignore

    # ==========================================
    # 6. LORE TABLE 3: The Ricklantis Mixup Cast (Sorted by Location)
    # ==========================================
    print("\n" + "=" * 115)
    print(" 🎬 TABLE 3: CAST OF S03E07 (Sorted by Current Location)")
    print("=" * 115)
    print(f"{'ACTOR NAME':<25} | {'STATUS':<10} | {'GENDER':<10} | {'CURRENT LOCATION':<30}")
    print("-" * 115)

    if isinstance(episodes, list) and isinstance(characters, list):
        # Grab Episode 28 straight from the memory registry
        ep28 = episodes[0].__class__.codeDict.get(28)

        if ep28 and getattr(ep28, "characters", None):
            cast_url_list = getattr(ep28, "characters")

            # 1. Resolve all URL strings into actual Python Objects
            actors = []
            for actor_url in cast_url_list:
                actor_id = extract_url_id(int)(actor_url)
                actor = characters[0].__class__.codeDict.get(actor_id)
                if actor:
                    actors.append(actor)

            # 2. Sort the actors alphabetically by their Current Location Name
            actors.sort(key=lambda a: getattr(getattr(a, "location", None), "name", "Unknown"))

            # 3. Print the sorted table (limiting to first 15)
            for actor in actors[:15]:
                status = getattr(actor, "status", "Unknown")
                gender = getattr(actor, "gender", "Unknown")
                loc_name = getattr(actor.location, "name", "Unknown") if getattr(actor, "location", None) else "Unknown"

                print(f"{actor.name:<25} | {status:<10} | {gender:<10} | {loc_name:<30}")

    print("=" * 115 + "\n")


if __name__ == "__main__":
    asyncio.run(main())