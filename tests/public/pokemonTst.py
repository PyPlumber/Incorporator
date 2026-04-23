import asyncio
import json
from typing import Any
from incorporator import Incorporator


def extract_id_from_url(url_str: Any) -> int:
    """Extracts the trailing ID from a PokeAPI URL (e.g. '.../pokemon/1/' -> 1)."""
    if not isinstance(url_str, str):
        return 0
    return int(url_str.strip('/').split('/')[-1])


def poke_api_next_page(raw_json_str: str) -> str | None:
    """Finds the 'next' URL inside the PokeAPI JSON body."""
    data = json.loads(raw_json_str)
    next_url = data.get("next")
    return str(next_url) if next_url else None


async def main() -> None:
    BASE_URL = "https://pokeapi.co/api/v2"

    print("Fetching ALL Pokemon base data (This might take a few seconds to paginate 1000+ rows)...")

    # 1. Fetching Pokemon Base WITH PAGINATION
    pokemon_base = await Incorporator.incorp(
        url=f"{BASE_URL}/pokemon/",
        rPath="results",

        # --- PAGINATION ACTIVATED ---
        paginate=True,
        next_url_extractor=poke_api_next_page,

        code="id",
        name="name",
        conv_dict={"url": extract_id_from_url},
        name_chg=[("url", "id")]
    )

    print("Fetching Languages...")
    languages = await Incorporator.incorp(
        url=f"{BASE_URL}/language/",
        rPath="results",
        code="id",
        name="name",
        conv_dict={"url": extract_id_from_url},
        name_chg=[("url", "id")]
    )

    print("Fetching Pokemon Species...")
    poke_species = await Incorporator.incorp(
        url=f"{BASE_URL}/pokemon-species/",
        rPath="results",
        code="id",
        name="name",
        conv_dict={"url": extract_id_from_url},
        name_chg=[("url", "id")]
    )

    # --- Validation ---
    print("\n--- Pipeline Complete ---")
    if isinstance(pokemon_base, list):
        print(f"Loaded {len(pokemon_base)} Pokemon, {len(languages)} Languages, and {len(poke_species)} Species.")

        # Test the codeDict lookup using the integer IDs we extracted!
        bulbasaur = pokemon_base.codeDict[1]
        mewtwo = pokemon_base.codeDict[150]

        print(f"Lookup Test: ID 1 -> {bulbasaur.name}")  # type: ignore
        print(f"Lookup Test: ID 150 -> {mewtwo.name}")  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())