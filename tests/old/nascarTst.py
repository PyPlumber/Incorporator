import asyncio
from datetime import datetime
from typing import Any

from incorporator import Incorporator, to_date, link_to

# --- HELPER FUNCTIONS & CONSTANTS ---

SERIES_MAP = {
    1: "nascar-cup-series",
    2: "nascar-oreilly-auto-parts-series",
    3: "nascar-craftsman-truck-series"
}


def conv_driver_series(x: Any) -> str | None:
    """Safely maps the driver series ID to its string name."""
    if not x: return None
    try:
        return SERIES_MAP.get(int(x))
    except (ValueError, TypeError):
        return None


def _get_relation(registry: dict, key: Any) -> Any:
    """Safely checks a registry for both the raw key and integer-casted key."""
    if not key: return None
    if key in registry: return registry[key]
    try:
        return registry.get(int(key))
    except (ValueError, TypeError):
        return None


async def main() -> None:
    print("🏁 Initiating NASCAR Data Gateway...\n")

    CURRENT_YEAR = datetime.now().year
    CFC_BASE = "https://cf.nascar.com/cacher"
    PROD_BASE = f"https://cf.nascar.com/data/cacher/production/{CURRENT_YEAR}"

    # ==========================================
    # 1. FETCH FOUNDATIONAL DATA (Tracks & Drivers)
    # ==========================================
    print("-> Fetching Tracks...")
    tracks = await Incorporator.incorp(
        url=f"{CFC_BASE}/tracks.json",
        rPath="items",
        code="track_id",
        name="track_name"
    )

    print("-> Fetching Drivers...")
    drivers = await Incorporator.incorp(
        url=f"{CFC_BASE}/drivers.json",
        rPath="response",
        code="Nascar_Driver_ID",
        name="Full_Name",
        excl_lst=[
            'Series_Logo', 'Short_Name', 'Description', 'Hobbies', 'Children', 'Residing_City',
            'Residing_State', 'Residing_Country', 'Image_Transparent', 'SecondaryImage', 'Career_Stats',
            'Age', 'Rank', 'Points', 'Points_Behind', 'No_Wins', 'Poles', 'Top5', 'Top10', 'Laps_Led',
            'Stage_Wins', 'Playoff_Points', 'Playoff_Rank', 'Integrated_Sponsor_Name', 'Integrated_Sponsor',
            'Integrated_Sponsor_URL', 'Silly_Season_Change', 'Silly_Season_Change_Description',
            'Driver_Post_Status', 'Driver_Part_Time'
        ],
        conv_dict={
            'DOB': to_date,
            'DOD': to_date,
            'Driver_Series': conv_driver_series
        }
    )

    # ==========================================
    # 2. FETCH RELATIONAL DATA (Cup Races)
    # ==========================================
    print("-> Fetching Cup Schedule & Mapping Relationships...")

    date_fields = ['date_scheduled', 'race_date', 'qualifying_date', 'tunein_date']

    cup_races = await Incorporator.incorp(
        url=f"{CFC_BASE}/{CURRENT_YEAR}/race_list_basic.json",
        rPath="series_1",
        code="race_id",
        name="race_name",
        excl_lst=['schedule', 'track_name'],

        # Look how incredibly clean this is now!
        conv_dict={
            'track_id': link_to(tracks),
            'pole_winner_driver_id': link_to(drivers),
            **{key: to_date for key in date_fields}
        },

        name_chg=[('track_id', 'track')]
    )

    # ==========================================
    # 3. FETCH STANDINGS (Using DRY exclusion lists)
    # ==========================================
    print("-> Fetching Live Standings...")
    standings_excl = [
        'delta_playoff', 'is_clinch', 'starts', 'poles',
        'driver_first_name', 'driver_last_name', 'driver_suffix'
    ]

    cup_standings = await Incorporator.incorp(
        url=f"{PROD_BASE}/1/racinginsights-points-feed.json",
        code="driver_id", name="driver_name", excl_lst=standings_excl
    )

    # ==========================================
    # 4. VALIDATION & DX SHOWCASE
    # ==========================================
    print("\n✅ Pipeline Complete! Validating Data...")

    tracks_count = len(tracks) if isinstance(tracks, list) else 1
    drivers_count = len(drivers) if isinstance(drivers, list) else 1
    races_count = len(cup_races) if isinstance(cup_races, list) else 1

    print(f"Loaded: {tracks_count} Tracks, {drivers_count} Drivers, {races_count} Cup Races.")

    if isinstance(cup_races, list) and isinstance(cup_standings, list):
        sample_race = cup_races[5]

        print("\n--- Relational Magic Showcase ---")
        # Notice we can safely use .name now because the ETL actually ran and renamed "race_name"!
        print(f"Race: {getattr(sample_race, 'name', 'Unknown')}")

        if getattr(sample_race, 'track', None):
            print(f"Track Name: {sample_race.track.name}")  # type: ignore

        if getattr(sample_race, 'pole_winner_driver_id', None):
            # Because the ETL ran, this is now a Driver object, not an integer!
            print(f"Pole Winner: {sample_race.pole_winner_driver_id.name}")  # type: ignore

        print("\n--- Current Cup Leader ---")
        if cup_standings:
            print(f"Name: {getattr(cup_standings[0], 'name', 'Unknown')}")
            print(f"Points: {getattr(cup_standings[0], 'points', 'N/A')}")


if __name__ == "__main__":
    asyncio.run(main())