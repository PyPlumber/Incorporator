```markdown
# 📦 Incorporator: The Dynamic Class Building & Zero-Boilerplate Gateway

[![PyPI version](https://img.shields.io/pypi/v/incorporator.svg)](https://pypi.org/project/incorporator/)
[![Python versions](https://img.shields.io/pypi/pyversions/incorporator.svg)](https://pypi.org/project/incorporator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Incorporator** is a highly optimized, asynchronous Python micro-client designed to eliminate boilerplate when consuming unpredictable data sources.

It instantly converts JSON, CSV, or XML—whether from a REST API or a local file—into fully autocomplete-compatible Pydantic V2 objects. With a pristine "Holy Trinity" API (`incorp`, `refresh`, `export`), Incorporator hides all network resilience, schema generation, and format conversion logic, prioritizing an unparalleled Developer Experience (DX).

---

## ✨ Key Features
- **Dynamic Class Building:** No need to write manual Pydantic schemas. Incorporator infers types on the fly and generates strict runtime models via `create_model`.
- **Zero-Boilerplate Pagination:** Pass `paginate=True` to seamlessly stream and accumulate multi-page API responses using background-threaded connection pools.
- **Declarative ETL:** Clean, rename, and type-cast data *before* schema compilation using `static_dct`, `excl_lst`, `conv_dict`, and `name_chg`.
- **Native Format Parsers:** Ingest messy CSVs and nested XMLs without heavy dependencies. Features strict `FormatType` routing and nested-CSV protection.
- **Invisible Resilience:** Built-in connection pooling and jittered exponential backoff (via `tenacity`) to evade 429/503 rate limits gracefully.
- **Non-Blocking Observability:** A multi-threaded, background JSON-Lines logging engine that safely writes to disk without blocking the async event loop.

## ⚙️ Installation

```bash
pip install incorporator
```

---
## 🚀 Quickstart: The "Holy Trinity" API

Incorporator offers three factory methods to generate and manage dynamic schemas: **Extract** (`incorp`), **Load/Save** (`export`), and **Update** (`refresh`).

```python
import asyncio
from incorporator import Incorporator

async def main() -> None:
    print("🚀 Initiating Incorporator Gateway...")
    
    # 1. EXTRACT & TRANSFORM (Zero Boilerplate)
    # Fetch 10 nested users, set the primary key, drop fields, and clean data on the fly.
    users = await Incorporator.incorp(
        url="https://jsonplaceholder.typicode.com/users",
        code="id",                                  # Sets the primary key for our weakref registry
        excl_lst=["phone", "website", "company"],   # Drop irrelevant data
        name_chg=[("email", "contact_email")],      # Rename legacy keys
        conv_dict={"username": lambda x: str(x).lower()} # Clean data instantly
    )

    # 2. THE IN-MEMORY REGISTRY (Instant Lookups)
    # Because we mapped code="id", Incorporator automatically indexed all 10 users!
    # No "for loops" required. Just grab User #5 directly from the list's registry:
    target_user = users.codeDict[5]
    
    print(f"\nRetrieved User #5: {target_user.name}")
    print(f"Contact Email: {target_user.contact_email}")
    
    # Deep dot-notation access to nested JSON instantly works!
    print(f"City: {target_user.address.city} (Lat: {target_user.address.geo.lat})")

    # 3. LOAD (Cross-Format Export)
    # Save the complex Pydantic objects out as a clean, flat CSV.
    await Incorporator.export(users, file_path="cleaned_users.csv")
    print("\n✅ Pipeline Complete: Saved to 'cleaned_users.csv'")
    
    # 4. REFRESH (Stateful Updates)
    # Read the CSV back into memory, seamlessly rebuilding the complex Pydantic objects.
    restored = await Incorporator.refresh(users, new_file="cleaned_users.csv")
    print(f"🔄 Restored User #5 City from CSV: {restored.codeDict[5].address.city}")

if __name__ == "__main__":
    asyncio.run(main())
```


---

## 🧬 Declarative ETL Pipelines

Incorporator cleans data elegantly to intercept anomalies *before* it compiles the Pydantic object.

We provide built-in, "Null-Safe" functional wrappers (like `to_bool`, `to_date`, `to_int`) for a clearer syntax that safely traps empty strings (`""`) without crashing your pipeline.

```python
from incorporator import Incorporator, FormatType, to_date, to_float, to_bool

async def process_messy_csv() -> None:
    users = await Incorporator.incorp(
        file="messy_database_dump.csv",
        format_type=FormatType.CSV,     # Explicit routing, bypassing auto-inference
        code="user_id",

        excl_lst=["password_hash", "social_security"],  # Drop sensitive columns
        static_dct={"system_migrated": True},           # Inject static constants

        # Safely cast strings to native Python types!
        # If the CSV cell is empty, it safely assigns `None`.
        conv_dict={
            "user_id": int,
            "account_balance": to_float,
            "is_active": to_bool,
            "last_login": to_date
        }
    )
```

---

## 🔭 Non-Blocking Observability

If you are building high-throughput webhooks or API scrapers, standard logging will freeze your async event loop. Swap `Incorporator` for `LoggedIncorporator` to utilize our background C-Thread `QueueHandler` logger.

```python
from incorporator import LoggedIncorporator

class MyAPI(LoggedIncorporator): 
    pass

async def track_web_traffic() -> None:
    # Setting enable_logging=True automatically generates 3 JSONL files:
    # MyAPI_api.log, MyAPI_error.log, and MyAPI_debug.log
    data = await MyAPI.incorp(url="https://api.example.com", enable_logging=True)

    # Send traffic exclusively to api.log
    data[0].log_api("Successfully consumed webhook!")

    # Read the error.log back natively via an async thread!
    errors = await MyAPI.getError()
    for err in errors:
        print(f"[{err['level']}] {err['msg']}")
```

---
*Built with ❤️ for the Open-Source Python Community.*
```