***

# 📖 `incorp()` Reference & ETL Guide

The `incorp()` classmethod is the beating heart of the Incorporator framework. It handles extraction, networking, pagination, data cleaning, and dynamic Pydantic schema compilation—all in a single asynchronous call.

Because Incorporator operates entirely on keyword arguments (`**kwargs`), understanding the execution order and available parameters is key to mastering the framework.

---

## 🚦 The Execution Pipeline

When you call `await MyClass.incorp(...)`, the framework executes your parameters in a strict, highly optimized order:

1. **Fetch:** Network/File I/O (`inc_url`, `inc_file`, `inc_parent`, `inc_page`).
2. **Drill:** Intercept nested JSON arrays (`rec_path`).
3. **Exclude:** Drop massive or unneeded keys before processing (`excl_lst`).
4. **Transform:** Mutate, cast, and calculate attributes (`conv_dict`, utilizing `inc()` and `calc()`).
5. **Rename:** Map messy API keys to clean Python attributes (`name_chg`).
6. **Identify:** Bind Primary Keys for the O(1) memory registry (`inc_code`, `inc_name`).
7. **Compile:** Dynamically build the Pydantic subclass and instantiate the objects.

---

## 🛠️ Core Routing Parameters

These kwargs tell Incorporator *where* to get the data and *what* to extract.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`inc_url`** | `str` \| `List[str]` | A single URL or a list of URLs to fetch concurrently. |
| **`inc_file`** | `str` \| `List[str]` | A local file path (JSON/CSV/XML). Natively offloaded to background threads. |
| **`rec_path`** | `str` | A dot-notation string (e.g., `"data.results"`) to drill past useless JSON wrappers and target the core array. |
| **`inc_code`** | `str` | The JSON key to bind as the Primary Key for the O(1) memory registry (`inc_dict`). |
| **`inc_name`** | `str` | An optional JSON key to bind as a human-readable label (`inc_name`). |
| **`inc_parent`** | `IncorporatorList` | A previously fetched dataset. Used to trigger HATEOAS deep-routing or Bulk POSTs. |
| **`inc_child`** | `str` | Used with `inc_parent`. The JSON key to extract from the parent objects to build the child requests (e.g., `"profile_url"` or `"user_id"`). |

---

## 📡 Network & Pagination Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`inc_page`** | `AsyncPaginator` | An instance of a paginator (e.g., `NextUrlPaginator("next")` or `OffsetPaginator(limit=50)`). |
| **`call_lim`** | `int` | Hard limit on the number of pages to fetch (prevents infinite API loops). |
| **`concurrency_limit`**| `int` | Built-in `asyncio.Semaphore` limit for concurrent HTTPX connections (Default: 50). |
| **`delay_between_batches`**| `float` | Seconds to pause between chunks to respect strict rate limits (Default: 0.0). |
| **`http_method`**| `str` | `"GET"`, `"POST"`, `"PUT"`, etc. (Default: `"GET"`). |
| **`json_payload`**| `Dict` | The POST body to send. Compatible with concurrent sentinels like `each()`. |

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
*Note: If an API sends complete garbage (like `"Apple"` for a float), `inc()` catches the exception, logs a helpful warning, and returns your `default` value gracefully.*

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

---

## 🧹 Housekeeping: `excl_lst` and `name_chg`

Don't forget the other two pillars of the Declarative ETL pipeline!

*   **`excl_lst: List[str]`**: A list of JSON keys to completely drop *before* any transformations run. Use this to delete massive, unneeded Base64 strings or heavy arrays to save memory.
    *   *Example:* `excl_lst=["profile_image_base64", "historical_logs"]`
*   **`name_chg: List[Tuple[str, str]]`**: A list of tuples used to rename API keys to match PEP 8 standards or avoid Python reserved keywords. Runs *after* `conv_dict`.
    *   *Example:* `name_chg=[("desc", "description"), ("class", "class_name")]`