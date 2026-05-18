# 🐘 Tutorial 2 — Data Lake Pivot: From SaaS Roster to BI-Ready Columnar

**Prerequisites:** [Tutorial 1 — First Steps](../01-first-steps/README.md) (`incorp()`, `test()`, `inc_dict`).

**File:** [`data_lake_pivot.py`](./data_lake_pivot.py)

In T1 you loaded a single endpoint into a typed Pydantic registry.  Every real ETL job adds one more step: **pivot** those rows into a shape an analytical warehouse can ingest — flatten nested objects, infer column types, persist to columnar binary.  This is the arc that runs at 1,000 ops teams every morning at 1 a.m.: HR / SaaS user lists land in Auth0 or Okta, get pivoted into Avro + SQLite, and feed the BI layer by 7 a.m.

This tutorial walks that arc on a SaaS-style `/users` endpoint.  `jsonplaceholder.typicode.com/users` stands in for any rostered REST source (Auth0, Okta, Workday, BambooHR, internal LDAP exports).  The endpoint returns heavily nested `address` and `company` dictionaries — the kind of payload that usually requires manual schema definitions, type mapping, and flattening logic.

Incorporator eliminates this completely. This tutorial demonstrates how to pivot deeply nested JSON into SQL and Avro with zero boilerplate, while proving that Incorporator's **O(1) Memory Registry** works universally across all formats.

## 💡 What this tutorial proves
1. **Automatic Schema Translation:** Incorporator infers types from the JSON API and natively maps them to strict Avro types (e.g., `int` to `long`) and SQLite columns.
2. **Nested Data Flattening:** It safely flattens nested JSON objects (like an `address` dictionary) into strings for binary/SQL storage, preventing database crashes.
3. **Universal Memory Mapping:** The exact same `inc_code` syntax that maps a JSON key to the in-memory registry (`inc_dict`) also maps SQLite columns and Avro fields seamlessly.
4. **Flawless Reconstruction:** When reading back from SQL or Avro, Incorporator detects the flattened strings and magically reconstructs the full Pydantic object graph, allowing instant dot-notation (`.address.city`).

---

## The Code Walkthrough

### 1. Ingest the Nested JSON API
First, we fetch a list of users from a public API. Notice that this API returns heavily nested `address` and `company` dictionaries. Incorporator digests them effortlessly and registers them in memory by their `"id"`.

```python
from incorporator import Incorporator, FormatType

class User(Incorporator): pass

users = await User.incorp(
    inc_url="https://jsonplaceholder.typicode.com/users",
    inc_code="id",  # Maps the JSON key to the O(1) Memory Registry
    inc_name="name" # Maps the JSON key to a human-readable label
)
```

### 2. Export directly to Local SQLite
With a single command, Incorporator creates the `employees` table, infers the column types, flattens the nested dictionaries, and executes a C-speed bulk insert.

```python
await User.export(
    users, 
    file_path="users_warehouse.db", 
    sql_table="employees", 
    if_exists="replace"
)
```

### 3. Export directly to Apache Avro (Binary)
Avro requires a highly strict schema definition. Incorporator dynamically generates the Avro schema from the Pydantic models in memory, enforces type-casting, and writes the binary stream.

```python
await User.export(
    users, 
    file_path="users_datalake.avro", 
    format_type=FormatType.AVRO
)
```

### 4. The Round Trip (Reading from Binary)
The true magic of Incorporator is that **the syntax never changes**. To read the data back from SQLite or Avro, we just call `incorp()` again, passing `inc_code` to bind the Database columns and Binary fields directly into our memory registry.

```python
# Read from SQLite
sql_users = await User.incorp(
    inc_file="users_warehouse.db", 
    sql_query="SELECT * FROM employees",
    inc_code="id",    # 🛡️ THE MAGIC: Maps the SQLite Column to the Memory Registry!
    inc_name="name"
)

# Read from Avro
avro_users = await User.incorp(
    inc_file="users_datalake.avro", 
    format_type=FormatType.AVRO,
    inc_code="id",    # 🛡️ THE MAGIC: Maps the Avro Field to the Memory Registry!
    inc_name="name"
)
```

### 5. Verifying the O(1) Object Graph
Because we passed `inc_code="id"`, we don't have to write loops or slice arrays to find our data. We can instantly query user ID `1` from the JSON, SQL, and Avro datasets in exactly O(1) time. 

Furthermore, we can see that Incorporator flawlessly un-flattened the nested `address` data back into native Python objects.

```python
target_id = 1

print(f"Original JSON Name: {users.inc_dict[target_id].inc_name}")
print(f"SQLite Read Name:   {sql_users.inc_dict[target_id].inc_name}")
print(f"Avro Read Name:     {avro_users.inc_dict[target_id].inc_name}")

# Prove that the nested Pydantic objects were reconstructed from the binary/SQL text
print(f"Original JSON City: {users.inc_dict[target_id].address.city}")
print(f"SQLite Read City:   {sql_users.inc_dict[target_id].address.city}")
print(f"Avro Read City:     {avro_users.inc_dict[target_id].address.city}")

# Output:
# Original JSON City: Gwenborough
# SQLite Read City:   Gwenborough
# Avro Read City:     Gwenborough
```

### 🎯 The Enterprise Advantage
We went from a nested JSON Web Payload ➡️ Relational SQLite Database ➡️ Binary Hadoop Stream ➡️ and back to fully reconstructed Python Objects using exactly **two methods** (`incorp` and `export`). 

Because the data is safely parked in the `.inc_dict` registries, you can immediately use `link_to(sql_users)` or `link_to(avro_users)` to fuse this data with live REST API responses—without writing a single Database Mapper (ORM) or Hadoop Serializer.

---

## 🐳 Run it from the CLI

This is the simplest CLI case — fetch JSON, write to SQLite (or Avro, or Parquet, or any other supported format). Pure JSON, no sidecar file needed:

```json
{
  "incorp_params": {
    "inc_url": "https://jsonplaceholder.typicode.com/users",
    "inc_code": "id",
    "inc_name": "name"
  },
  "export_params": {"file_path": "data/users_warehouse.db"}
}
```

```bash
incorporator validate pipeline.json
incorporator stream pipeline.json
```

**Switch the export target by changing the file extension** — Incorporator infers the handler from the path:

* `users_warehouse.db` → SQLite
* `users_datalake.avro` → Apache Avro *(requires `pip install incorporator[avro]`)*
* `users.parquet` → Parquet *(requires `pip install incorporator[parquet]`)*
* `users.ndjson` / `.csv` / `.xlsx` → all native

For a Dockerised daemon that polls + refreshes on a schedule, see [`examples/cli-templates/daemon-mode.json`](../../examples/cli-templates/daemon-mode.json) and [the deployment guide](../../../docs/deployment.md).

---

## Where to Go Next

> 👉 **Up next: [Tutorial 3 — Universal Formats](../03-universal-formats/README.md).**  T2 showed the pivot arc on one source; T3 expands it into a five-format snapshot warehouse round-trip on a typed CoinGecko payload.

| Goal | Read |
|---|---|
| Build a per-tick snapshot warehouse across five formats | [Tutorial 3 — Universal Formats](../03-universal-formats/README.md) |
| Audit a warehouse against a federal source | [Tutorial 4 — XML Post Audit](../04-xml-post-audit/README.md) |
| Land Parquet artifacts at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](../appendix/tideweaver-parquet-snapshots/README.md) |
| Stream massive files through chunking + paginators | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |
| See the format kwarg reference | [Formats & Compression](../../docs/formats_and_compression.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/02-data-lake-pivot/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)