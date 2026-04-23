import asyncio
import json
from datetime import datetime
from typing import Any

# ALL the tools we need from the new framework!
from incorporator import (
    Incorporator,
    split_and_get,
    cast_list_items,
    to_date,
    link_to,
    link_to_list
)


# --- HELPER FUNCTIONS ---

def rm_next_page(raw_json_str: str) -> str | None:
    """Extracts the pagination 'next' URL from the R&M API info block."""
    try:
        data = json.loads(raw_json_str)
        next_url = data.get("info", {}).get("next")
        return str(next_url) if next_url else None
    except Exception:
        return None


# The Upgraded Converter! Automatically splits the URL by '/' and casts the last element to an Integer.
extract_id = split_and_get('/', -1, int)


def extract_rm_url(val: Any) -> Any:
    """Extracts the URL from R&M's nested dicts: {'name': 'Earth', 'url': '...'}"""
    url = val.get("url") if isinstance(val, dict) else val
    return extract_id(url)


# --- MAIN PIPELINE ---

async def main() -> None:
    print("🛸 Opening portal to the Rick and Morty API...\n")
    BASE_URL = "https://rickandmortyapi.com/api"

    # 1. FETCH LOCATIONS
    print("-> Fetching Locations...")
    locations = await Incorporator.incorp(
        url=f"{BASE_URL}/location/",
        rPath="results",
        paginate=True, next_url_extractor=rm_next_page,
        code="id", name="name",
        excl_lst=['url'],
        conv_dict={'residents': cast_list_items(extract_id)}  # Uses framework native!
    )

    # 2. FETCH EPISODES
    print("-> Fetching Episodes...")
    episodes = await Incorporator.incorp(
        url=f"{BASE_URL}/episode/",
        rPath="results",
        paginate=True, next_url_extractor=rm_next_page,
        code="id", name="name",
        excl_lst=['url'],
        conv_dict={
            'air_date': to_date,
            'characters': cast_list_items(extract_id)  # Uses framework native!
        }
    )

    # 3. FETCH CHARACTERS & MAP RELATIONS
    print("-> Fetching Characters & Mapping Relations...")
    characters = await Incorporator.incorp(
        url=f"{BASE_URL}/character/",
        rPath="results",
        paginate=True, next_url_extractor=rm_next_page,
        code="id", name="name",
        excl_lst=['image', 'url'],
        conv_dict={
            # Relational Magic using the new link_to and link_to_list framework features!
            'location': link_to(locations, extractor=extract_rm_url),
            'origin': link_to(locations, extractor=extract_rm_url),
            'episode': link_to_list(episodes, extractor=extract_id)
        }
    )

    # --- VALIDATION & DX SHOWCASE ---
    print("\n✅ Pipeline Complete! Validating Graph Relations...")

    if isinstance(characters, list) and isinstance(locations, list) and isinstance(episodes, list):
        print(f"Loaded: {len(locations)} Locations, {len(episodes)} Episodes, {len(characters)} Characters.")

        # Grab Morty Smith (ID: 2) from the registry
        morty = characters.codeDict[2]

        print("\n--- Relational Magic Showcase ---")
        print(f"Character: {morty.name}")  # type: ignore

        if getattr(morty, "location", None):
            print(f"Current Location: {morty.location.name} ({morty.location.type})")  # type: ignore

        if getattr(morty, "origin", None):
            print(f"Origin Dimension: {morty.origin.dimension}")  # type: ignore

        if getattr(morty, "episode", None):
            first_appearance = morty.episode[0]  # type: ignore
            print(
                f"First Appearance: {first_appearance.name} (Aired: {first_appearance.air_date.strftime('%B %d, %Y')})")


if __name__ == "__main__":
    asyncio.run(main())


# story = epsList[7]
# print(f"Episode {story.code} was titled {story.name}.")
# print(f"The episode aired on {story.air_date.strftime('%d, %b %Y')}.")
# print(f"The API instance was created in {story.created.strftime('%Y')}.")
#
# print("\n")
#
# cast = story.characters
# print(f"It had {len(cast)} characters:")
# for character in cast:
#     character.displayInfo()
#
#
