"""
Tutorial: The Data Lake Pivot (JSON ➡️ SQLite ➡️ Avro)
------------------------------------------------------
This example demonstrates how Incorporator bridges the gap between Web APIs and
Enterprise Data Lakes. We will fetch deeply nested JSON, dynamically map it,
and instantly export it to both SQLite and Apache Avro.

Crucially, it proves that the syntax for O(1) Memory Mapping (inc_code) is
100% universal across Web, Relational, and Binary data sources.
"""

import asyncio
from pathlib import Path

from incorporator import FormatType, Incorporator

# Ensure you have installed the speedups: `pip install incorporator[avro]`


class User(Incorporator):
    pass


async def main():
    print("🌐 1. Fetching Nested JSON from Public API...")
    # The JSONPlaceholder API contains heavily nested 'address' and 'company' dicts.
    users = await User.incorp(
        inc_url="https://jsonplaceholder.typicode.com/users",
        inc_code="id",  # Maps JSON key to Memory Registry
        inc_name="name",  # Maps JSON key to human-readable label
    )
    print(f"   ✅ Mapped {len(users)} users into Python memory.")

    print("\n🗄️ 2. Pivoting to Local SQLite Database...")
    db_path = "users_warehouse.db"

    # Flattens nested dictionaries into JSON strings and executes C-speed bulk inserts.
    await User.export(instance=users, file_path=db_path, sql_table="employees", if_exists="replace")
    print(f"   ✅ Exported natively to {db_path}")

    print("\n🐘 3. Pivoting to Apache Avro (Big Data Format)...")
    avro_path = "users_datalake.avro"

    # Translates the dynamic Pydantic schema into a strict Avro binary stream.
    await User.export(instance=users, file_path=avro_path, format_type=FormatType.AVRO)
    print(f"   ✅ Exported natively to {avro_path}")

    print("\n🔄 4. The Round Trip: Reading back from Binary Sources...")

    # A. Read directly from SQLite
    sql_users = await User.incorp(inc_file=db_path, sql_query="SELECT * FROM employees", inc_code="id", inc_name="name")
    print(f"   ✅ Read {len(sql_users)} users from SQLite.")

    # B. Read directly from Avro
    avro_users = await User.incorp(inc_file=avro_path, format_type=FormatType.AVRO, inc_code="id", inc_name="name")
    print(f"   ✅ Read {len(avro_users)} users from Avro.")

    # Let's prove Incorporator flawlessly un-flattened the nested data AND
    # successfully built the O(1) inc_dict lookup registries for all three formats!
    print("\n🏆 VERIFICATION (Testing O(1) Lookups for User ID '1'):")

    target_id = 1

    print(f"Original JSON Name: {users.inc_dict[target_id].inc_name}")
    print(f"SQLite Read Name:   {sql_users.inc_dict[target_id].inc_name}")
    print(f"Avro Read Name:     {avro_users.inc_dict[target_id].inc_name}")

    # Prove that the nested Pydantic objects were reconstructed from the binary/SQL text
    print(f"\nOriginal JSON City: {users.inc_dict[target_id].address.city}")
    print(f"SQLite Read City:   {sql_users.inc_dict[target_id].address.city}")
    print(f"Avro Read City:     {avro_users.inc_dict[target_id].address.city}")

    # Cleanup local files
    Path(db_path).unlink(missing_ok=True)
    Path(avro_path).unlink(missing_ok=True)


if __name__ == "__main__":
    asyncio.run(main())
