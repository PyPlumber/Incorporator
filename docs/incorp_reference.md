***

# 📖 `incorp()` API Reference & ETL Guide

The `incorp()` classmethod is the beating heart of the Incorporator framework. It handles extraction, networking, decompression, pagination, data cleaning, and dynamic Pydantic schema compilation—all in a single asynchronous call.

Because Incorporator operates entirely on keyword arguments (`**kwargs`), understanding the execution order and available parameters is key to mastering the framework.

---

## 🕵️‍♂️ Before You Start: Use `.test()`
If you do not know the exact structure of the API you are querying, do not write an `.incorp()` call blindly! Instead, use the DX Inspector wrapper:

```python
await User.test(inc_url="https://api.unknown.com/users")
```

The `.test()` method takes the exact same kwargs as `.incorp()`. It safely fetches a single page, intercepts the payload, and prints a visual tree alongside exact `incorp()` kwarg recommendations (like `rec_path`, `inc_code`, and `conv_dict` timestamps) to your terminal.

---

## 🚦 The Execution Pipeline

When you call `await MyClass.incorp(...)`, the framework executes your parameters in a strict, highly optimized order:

1. **Fetch & Decompress:** Network/File I/O (`inc_url`, `inc_file`). Automatically intercepts and decompresses `.zip`, `.gz`, `.zst`, etc., in a background thread.
2. **Binary Bypass:** Instantly routes SQLite (`.db`) and Apache Avro (`.avro`) files directly to their binary parsers, bypassing text-decoding.
3. **Drill:** Intercept nested arrays or dictionaries (`rec_path`).
4. **Exclude:** Drop massive or unneeded keys before processing (`excl_lst`).
5. **Transform:** Mutate, cast, and calculate attributes (`conv_dict`, utilizing `inc()` and `calc()`).
6. **Rename:** Map messy API keys to clean Python attributes (`name_chg`).
7. **Identify:** Bind Primary Keys for the O(1) memory registry (`inc_code`, `inc_name`).
8. **Compile:** Dynamically build the Pydantic subclass and instantiate the objects.

---

## 🛠️ Core Routing Parameters

These kwargs tell Incorporator *where* to get the data and *what* to extract.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`inc_url`** | `str` \| `List[str]` | A single URL or a list of URLs to fetch concurrently. **(Natively auto-detects and decompresses `.gz`, `.zip`, `.zst`, etc.)** |
| **`inc_file`** | `str` \| `List[str]` | A local file path. Natively accepts flat files (`.json`, `.csv`), binary files (`.db`, `.avro`), and archives (`.tar.gz`, `.zip`). |
| **`rec_path`** | `str` | A dot-notation string (e.g., `"data.results"`) to drill past useless JSON/XML wrappers and target the core array. |
| **`inc_code`** | `str` | The JSON/CSV key to bind as the Primary Key for the O(1) memory registry (`inc_dict`). |
| **`inc_name`** | `str` | An optional key to bind as a human-readable label (`inc_name`). |
| **`inc_parent`** | `IncorporatorList` | A previously fetched dataset. Used to trigger HATEOAS deep-routing or Declarative Bulk POSTs. |
| **`inc_child`** | `str` | Used with `inc_parent`. The key to extract from the parent objects to build the child requests (e.g., `"profile_url"` or `"VIN"`). |

---

## 🗄️ Database & Format-Specific Parameters

When parsing binary files like SQLite, Incorporator uses *Convention over Configuration* to eliminate boilerplate. You only need these kwargs if you want to override the magic defaults.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`sql_query`** | `str` | The explicit SQL query to execute when reading from a local `.db` file (e.g., `"SELECT * FROM users WHERE active = 1"`). |
| **`sql_table`** | `str` | If `sql_query` is omitted, Incorporator magically infers the table name from your **Python Class Name** (e.g., `class User` generates `SELECT * FROM user`). Use this kwarg to explicitly override the target table. |

---

## 📡 Network & Pagination Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`inc_page`** | `AsyncPaginator` | An instance of a paginator (e.g., `NextUrlPaginator("next")` or `OffsetPaginator(limit=50)`). |
| **`call_lim`** | `int` | Hard limit on the number of pages to fetch (prevents infinite API loops). |
| **`concurrency_limit`**| `int` | Built-in `asyncio.Semaphore` limit for concurrent HTTPX connections (Default: `50`). |
| **`delay_between_batches`**| `float` | Seconds to pause between chunks to respect strict rate limits (Default: `0.0`). |
| **`http_method`**| `str` | `"GET"`, `"POST"`, `"PUT"`, etc. (Default: `"GET"`). |
| **`json_payload`**| `Dict` | The POST JSON body to send. Compatible with concurrent sentinels like `each()` and `join_all()`. |
| **`form_payload`**| `Dict` | The POST Form-Data body to send. |

---

## 🧪 The Declarative ETL Pipeline (`conv_dict`)

The `conv_dict` parameter is where Incorporator's true data-manipulation magic happens. Instead of writing custom Pydantic validators to handle dirty data, you map JSON keys to native conversion functions.

Incorporator provides several built-in functions in `incorporator.methods.converters`. The two most powerful are `inc()` and `calc()`.

### 🛡️ `inc()`: Null-Safe Type Casting
Real-world APIs are messy. They drop keys, send `"N/A"` for booleans, or format timestamps inconsistently. The `inc()` function leverages a Type-Ranked Conversion Engine to cast data safely without ever throwing a `ValidationError` or crashing your event loop.

**Signature:** `inc(target_type, default=None)`

**Example Usage:**
```python
from datetime import datetime
from incorporator.methods.converters import inc, new

dataset = await MyClass.incorp(
    inc_url="https://api.messy-data.com/v1/users",
    conv_dict={
        # 1. Messy Dates: Gracefully parses ISO-8601, Unix, or custom formats.
        "created_at": inc(datetime, default=datetime.utcnow()),

        # 2. Messy Booleans: Safely translates 1/0, "true"/"false", or None.
        "is_active": inc(bool, default=False),

        # 3. Messy Floats: Cleans up "$1,234.56" or "unknown".
        "balance": inc(float, default=0.0),

        # 4. Injecting New Keys: Use the `new` sentinel to create a key from scratch!
        "internal_system_tag": inc(new, default="processed_by_incorporator")
    }
)
```

---

### 🧮 `calc()`: Array Reduction & Row Math
When APIs return heavily nested arrays or you need to derive new columns based on existing data, `calc()` allows you to intercept the row before it compiles and run custom Python logic on it. 

**Signature:** `calc(func, *input_keys, default=None, target_type=None)`

#### Scenario A: Multi-Column Row Math
Calculate a new attribute based on multiple keys in the current JSON row.

```python
from incorporator.methods.converters import calc

def engagement_score(likes: int, shares: int) -> float:
    return float((likes * 1.5) + (shares * 3.0))

dataset = await MyClass.incorp(
    inc_url="https://api.domain.com/posts",
    conv_dict={
        # Passes the raw "likes" and "shares" JSON values into our custom function
        "engagement": calc(engagement_score, "likes", "shares", default=0.0, target_type=float)
    }
)
```

#### Scenario B: Nested Array Reduction
Stop building nested Pydantic models for useless sub-arrays. Use `calc()` to intercept a nested list of dictionaries and flatten it into a simple native type.

```python
from incorporator.methods.converters import calc

# The API returns: {"stats":[{"base_stat": 45}, {"base_stat": 65}]}
def calculate_bst(stats_array: list) -> int:
    """Calculates Base Stat Total by summing the 'base_stat' of all entries."""
    if not isinstance(stats_array, list): return 0
    return sum(stat_obj.get("base_stat", 0) for stat_obj in stats_array if isinstance(stat_obj, dict))

pokemon = await Pokemon.incorp(
    inc_url="https://pokeapi.co/api/v2/pokemon/1",
    conv_dict={
        # Intercepts the nested "stats" array, reduces it, and saves it as an integer!
        "stats": calc(calculate_bst, "stats", default=0, target_type=int)
    },
    # Optional: Rename the key to reflect the new, flattened value
    name_chg=[("stats", "base_stat_total")]
)

# You now have a clean integer instead of a list of objects!
print(pokemon.base_stat_total) # Output: 110
```

### Declarative POST Tokens
When passing `inc_parent` and performing a bulk `POST`, `PUT`, or `PATCH`, you can use these tokens inside your `json_payload` or `form_payload` to control how the parent data is mapped:

* **`each()`**: Triggers **N Concurrent Requests**. Maps the extracted parent list row-by-row into the payload.
  * *Example:* `json_payload={"user_id": each(), "status": "active"}`
* **`join_all(delimiter=",")`**: Triggers **1 Bulk Request**. Takes the extracted parent IDs and joins them into a single delimited string.
  * *Example:* `json_payload={"vehicle_vins": join_all(";")}`
* **`as_list()`**: Triggers **1 Bulk Request**. Injects the raw extracted parent list directly into a JSON Array.
  * *Example:* `json_payload={"target_ids": as_list()}`

---

## 🧹 Housekeeping: `excl_lst` and `name_chg`

Don't forget the other two pillars of the Declarative ETL pipeline!

*   **`excl_lst: List[str]`**: A list of JSON keys to completely drop *before* any transformations run. Use this to delete massive, unneeded Base64 strings or heavy arrays to save memory.
    *   *Example:* `excl_lst=["profile_image_base64", "historical_logs"]`
*   **`name_chg: List[Tuple[str, str]]`**: A list of tuples used to rename API keys to match PEP 8 standards or avoid Python reserved keywords. Runs *after* `conv_dict`.
    *   *Example:* `name_chg=[("desc", "description"), ("class", "class_name")]`