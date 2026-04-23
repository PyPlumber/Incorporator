"""
Incorporator Showcase: The Enterprise API Pipeline
Demonstrates ingesting a nested JSON user database, transforming it,
and exporting it to a flat CSV file in under 20 lines of code.
"""

import asyncio
from incorporator import Incorporator


async def main() -> None:
    print("🚀 Initiating Incorporator Gateway...\n")

    # 1. THE EXTRACT & TRANSFORM PHASE (incorp)
    # We fetch 10 mock users from a live API. The payload contains nested
    # data like "address": {"city": "Gwenborough", "geo": {"lat": "-37.3159"}}

    users = await Incorporator.incorp(
        url="https://jsonplaceholder.typicode.com/users",
        code="id",  # Set the universal PK
        name="name",  # Set the universal Name

        excl_lst=["phone", "website", "company"],  # Drop PII / irrelevant data
        static_dct={"is_migrated": True},  # Inject a new system flag
        name_chg=[("email", "contact_email")],  # Rename legacy keys

        # Lowercase the usernames on the fly!
        conv_dict={"username": lambda x: str(x).lower()}
    )

    # 2. THE DEVELOPER EXPERIENCE (DX)
    # Incorporator instantly parsed the nested JSON into strongly-typed objects.
    print("--- LIVE USER ROSTER ---")

    # We know users is a list because the API returns a JSON array
    if isinstance(users, list):
        for user in users[:3]:  # Display the first 3 users
            user.display()  # Uses our inherited Incorporator display() method

            # Look at this beautiful dot-notation for dynamically built schema fields!
            print(f"   Email: {user.contact_email}")  # type: ignore
            print(f"   Username: @{user.username}")  # type: ignore

            # Deeply nested JSON is automatically converted into nested Pydantic models
            print(f"   City: {user.address.city}")  # type: ignore
            print(f"   Lat/Lng: {user.address.geo.lat}, {user.address.geo.lng}\n")  # type: ignore

    # 3. THE LOAD PHASE (export)
    # Instantly flatten the nested Pydantic objects and save them as a CSV.
    export_file = "cleaned_users.csv"
    await Incorporator.export(users, file_path=export_file)

    print(f"✅ Pipeline Complete: Cleaned data safely exported to '{export_file}'.")


if __name__ == "__main__":
    asyncio.run(main())