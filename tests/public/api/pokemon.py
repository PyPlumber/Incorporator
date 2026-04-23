import asyncio
from typing import Any

from incorporator import (
    Incorporator,
    extract_url_id,
    pluck,
    link_to
)


class RegistryWrapper:
    """A simple wrapper to feed a custom list of objects into link_to."""

    def __init__(self, items: list[Any]) -> None:
        self.codeDict = {item.code: item for item in items}


async def main() -> None:
    print("🌍 Opening portal to the REAL PokeAPI...\n")

    BASE_URL = "https://pokeapi.co/api/v2"
    target_ids = [1, 6, 25, 94, 143, 150, 151]  # Bulba, Charizard, Pika, Gengar, Snorlax, Mewtwo, Mew

    # ==========================================
    # 1. FETCH SPECIES DETAILS (Concurrent Requests!)
    # ==========================================
    print("-> Fetching Species Details Concurrently...")
    species_tasks = [
        Incorporator.incorp(
            url=f"{BASE_URL}/pokemon-species/{pid}/",
            code="id", name="name", excl_lst=["url"],
            conv_dict={"habitat": pluck("name")}
        ) for pid in target_ids
    ]
    # FIX: Assign to a variable to hold strong references and prevent Garbage Collection!
    loaded_species = await asyncio.gather(*species_tasks)

    # Wrap the loaded objects so `link_to` can look them up by ID
    species_registry = RegistryWrapper(loaded_species)

    # ==========================================
    # 2. FETCH POKEMON DETAILS (Concurrent Requests!)
    # ==========================================
    print("-> Fetching Pokemon Details Concurrently...")
    pokemon_tasks = [
        Incorporator.incorp(
            url=f"{BASE_URL}/pokemon/{pid}/",
            code="id", name="name", excl_lst=["url"],
            conv_dict={
                # Relational Magic: Connect the Pokemon to the species_registry we just built!
                "species": link_to(species_registry, extractor=pluck("url", extract_url_id()))
            }
        ) for pid in target_ids
    ]
    # FIX: Assign to a variable to hold strong references!
    loaded_pokemon = await asyncio.gather(*pokemon_tasks)

    # ==========================================
    # 3. THE DEVELOPER EXPERIENCE (DX) TABLE
    # ==========================================
    print("\n\n" + "=" * 70)
    print(" 🧬 THE REAL GEN 1 POKEMON LORE ARCHIVE (2-Tier Graph Database) 🧬")
    print("=" * 70)
    print(f"{'ID':<5} | {'POKEMON':<15} | {'LEGENDARY?':<12} | {'MYTHICAL?':<12} | {'HABITAT'}")
    print("-" * 70)

    # Iterate directly over our strongly-referenced list
    for p in loaded_pokemon:
        p_id = f"#{p.code}"
        name = str(p.name).capitalize()

        # Dot-drill into the nested Species object
        is_leg = "Yes" if getattr(p.species, "is_legendary", False) else "No"  # type: ignore
        is_myth = "Yes" if getattr(p.species, "is_mythical", False) else "No"  # type: ignore
        hab = str(getattr(p.species, "habitat", "Unknown")).capitalize()  # type: ignore

        print(f"{p_id:<5} | {name:<15} | {is_leg:<12} | {is_myth:<12} | {hab}")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())