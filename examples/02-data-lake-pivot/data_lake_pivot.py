"""
Tutorial 2 — Data Lake Pivot (JSON ➡️ SQLite ➡️ Avro)
-----------------------------------------------------
This example demonstrates how Incorporator bridges the gap between Web APIs and
Enterprise Data Lakes. We will fetch deeply nested JSON, dynamically map it,
and instantly export it to both SQLite and Apache Avro.

The same `inc_code` vocabulary works across the web, SQLite, and Avro sources below.
"""

import asyncio
from pathlib import Path

from incorporator import FormatType, Incorporator

# Ensure you have installed the speedups: `pip install incorporator[avro]`

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


class User(Incorporator):
    pass


async def main() -> None:
    print("1. Fetching Nested JSON from Public API...")
    # The JSONPlaceholder API contains heavily nested 'address' and 'company' dicts.
    users = await User.incorp(
        inc_url="https://jsonplaceholder.typicode.com/users",
        inc_code="id",  # Maps JSON key to Memory Registry
        inc_name="name",  # Maps JSON key to human-readable label
    )
    print(f"   Mapped {len(users)} users into Python memory.")

    print("\n2. Pivoting to Local SQLite Database...")
    db_path = OUT / "users_warehouse.db"

    # Flattens nested address/company dicts for SQLite storage.
    await User.export(instance=users, file_path=str(db_path), sql_table="employees", if_exists="replace")
    print(f"   Exported natively to {db_path}")

    print("\n3. Pivoting to Apache Avro (Big Data Format)...")
    avro_path = OUT / "users_datalake.avro"

    # Converts the inferred schema into strict Avro types.
    await User.export(instance=users, file_path=str(avro_path), format_type=FormatType.AVRO)
    print(f"   Exported natively to {avro_path}")

    print("\n4. The Round Trip: Reading back from Binary Sources...")

    # A. Read directly from SQLite
    sql_users = await User.incorp(
        inc_file=str(db_path), sql_query="SELECT * FROM employees", inc_code="id", inc_name="name"
    )
    print(f"   Read {len(sql_users)} users from SQLite.")

    # B. Read directly from Avro
    avro_users = await User.incorp(inc_file=str(avro_path), format_type=FormatType.AVRO, inc_code="id", inc_name="name")
    print(f"   Read {len(avro_users)} users from Avro.")

    # Let's prove Incorporator flawlessly un-flattened the nested data AND
    # successfully built the O(1) inc_dict lookup registries for all three formats!
    print("\nVERIFICATION (Testing O(1) Lookups for User ID '1'):")

    target_id = 1

    print(f"Original JSON Name: {users.inc_dict[target_id].inc_name}")
    print(f"SQLite Read Name:   {sql_users.inc_dict[target_id].inc_name}")
    print(f"Avro Read Name:     {avro_users.inc_dict[target_id].inc_name}")

    # Prove that the nested Pydantic objects were reconstructed from the binary/SQL text
    print(f"\nOriginal JSON City: {users.inc_dict[target_id].address.city}")
    print(f"SQLite Read City:   {sql_users.inc_dict[target_id].address.city}")
    print(f"Avro Read City:     {avro_users.inc_dict[target_id].address.city}")

    # Outputs are deliberately *kept* in OUT/ so you can inspect them with
    # ``sqlite3 out/users_warehouse.db`` or ``head out/users_datalake.avro``.
    # The directory is gitignored — delete it manually to start fresh.


if __name__ == "__main__":
    asyncio.run(main())
