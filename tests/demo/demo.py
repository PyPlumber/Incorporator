import asyncio
from incorporator import LoggedIncorp, split_and_get


async def main():
    print("--- 1. INCORP: FETCHING FROM API (WITH LOGGING) ---")

    # Notice we switched from Incorp -> LoggedIncorp
    pokemon_roster = await LoggedIncorp.incorp(
        url="https://pokeapi.co/api/v2/pokemon?limit=2",
        rPath="results",
        code="name",
        convAdds={"url": split_and_get('/', -2)},
        nameAdds=[("url", "api_endpoint")],
        enable_logging=True  # TURN ON THE OBSERVABILITY ENGINE!
    )

    # 1. TRIGGER THE LOGS (Non-Blocking)
    bulbasaur = pokemon_roster[0]
    bulbasaur.log_api(f"Successfully requested from endpoint {bulbasaur.api_endpoint}")
    bulbasaur.log_info("Applying ETL transformations...")

    # Simulate a fake error to test the JSON parser
    try:
        1 / 0
    except ZeroDivisionError:
        bulbasaur.log_error("Simulated ZeroDivisionError occurred!", exc_info=True)

    # Give the background QueueListener 0.1 seconds to flush to the disk
    await asyncio.sleep(0.1)

    print("\n--- 2. GET_ERROR: READING JSONL DISK LOGS ---")

    # Extract the dynamic class to read its specific error file
    DynamicPokemonClass = pokemon_roster[0].__class__

    # READ FROM THE DISK ASYNCHRONOUSLY!
    errors = await DynamicPokemonClass.getError()

    for err in errors:
        print(f"[{err['level']}] Time: {err['time']}")
        print(f"Meta: {err['meta']}")
        print(f"Message: {err['msg']}")
        if "exc_info" in err:
            print(f"Traceback Captured!")


if __name__ == "__main__":
    asyncio.run(main())