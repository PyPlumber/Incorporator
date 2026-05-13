***

# 🕵️‍♂️ DX Inspector: Let the Framework Write Your Kwargs

You've found an unknown REST API. What's the schema? What's the
`inc_code`? Is there a `rec_path` wrapping the records? Are any fields
ISO-8601 strings that should be cast to `datetime`?

You could open Postman, eyeball the JSON, and write a half-dozen
hypothesis `incorp()` calls. Or you could let Incorporator do it for you.

`test()` is the **JIT API Profiler**. Hand it the URL of any endpoint
and it fetches one safe page, walks the payload tree, runs regex-based
value scoring to detect identity-shaped fields (UUIDs, timestamps,
slugs, etc.), and **prints the exact `incorp()` kwargs you'd write
yourself** — minus the trial and error.

---

## Step 1: Hit an Unknown API

```python
import asyncio
from incorporator import Incorporator


class User(Incorporator):
    pass


asyncio.run(User.test(inc_url="https://api.unknown.com/v1/users"))
```

That's it. **Swap `.incorp()` for `.test()`** to trigger the inspector.

---

## Step 2: Read the Report

Whatever the API looks like, `test()` prints a structured report:

```text
======================================================================
🕵️‍♂️  INCORPORATOR DX INSPECTOR
======================================================================

📦 1. PAYLOAD STRUCTURE:
   ├── metadata (dict)
   │   ├── count: int = 1500
   │   └── page: int = 1
   └── results (list, len=1500)
       ├── user_uuid: str = a1b2c3d4-e5f6...
       ├── full_name: str = Jimmy Jenkins
       ├── status: bool = True
       ├── created_at: str = 2026-05-12T14:32:00Z
       └── address (dict)

   ⚠️  WARNING: The root object is a dictionary, but it contains arrays.
   💡 SUGGESTION: You probably want to add `rec_path='results'` to your incorp() call.

🔑 2. IDENTITY MAPPING:
   Recommended kwargs for O(1) Memory Registry:
   ✅ inc_code='user_uuid'
   ✅ inc_name='full_name'

🛠️  3. ETL / TYPE CASTING SUGGESTIONS:
   💡 We detected string-based timestamps. Consider passing:
      conv_dict={
          'created_at': inc(datetime),
      }
======================================================================
```

Three sections, each actionable:

1. **Payload structure.** A tree-view of every key Python sees, with
   types and sample values. Warns when the root shape doesn't match
   common API conventions.
2. **Identity mapping.** Regex-scored candidates for `inc_code` (UUIDs,
   integer IDs, slugs) and `inc_name` (display strings). Picks the
   highest-confidence candidate.
3. **Type casting suggestions.** Detects ISO-8601 timestamps, numeric
   strings, and date patterns, and emits the exact `conv_dict` snippet
   you'd paste into a real call.

---

## Step 3: Turn the Report Into a Real Call

Copy the suggestions verbatim:

```python
from datetime import datetime
from incorporator import Incorporator
from incorporator.schema.converters import inc


class User(Incorporator):
    pass


async def main():
    users = await User.incorp(
        inc_url="https://api.unknown.com/v1/users",
        rec_path="results",                                # from inspector warning
        inc_code="user_uuid",                              # from identity mapping
        inc_name="full_name",
        conv_dict={"created_at": inc(datetime)},           # from type casting
    )
    print(f"Loaded {len(users)} users.")
    print(f"First user created: {users[0].created_at:%Y-%m-%d %H:%M %Z}")
```

You went from *"what does this API look like?"* to a fully-typed,
indexed, datetime-aware object graph **without writing or reading a
schema**.

---

## What `test()` does to stay safe

* **Single page only.** When you pass a paginator, `test()` forces
  `call_lim=1` so the inspector never paginates further during
  exploration.
* **Short timeout.** Defaults to `timeout=5.0` so an unresponsive
  endpoint fails fast.
* **Result preview cap.** Returns at most 3 records to avoid flooding
  the terminal — the return value is a real `IncorporatorList`, so
  `sample[0].whatever` works for poking at the shape.
* **Error analysis on failure.** If the fetch raises, `test()` routes
  the exception through the same inspector module to suggest
  diagnostics (auth headers missing, wrong content type, etc.).

---

## When to reach for `test()`

| Situation | Use `test()`? |
|---|---|
| New endpoint, no idea what shape returns | ✅ Yes — your first call should be `.test()` |
| Endpoint changed shape unexpectedly | ✅ Yes — diff the inspector output against your last known good config |
| Building a `pipeline.json` for the CLI | ✅ Yes — paste the inspector suggestions into the JSON |
| Production daemon | ❌ No — use `incorp()` / `stream()` / `fjord()` with the kwargs `test()` suggested |

For pagination + production patterns once you've explored, see
[Streaming Daemon](./6_streaming_daemon.md) and
[Multi-Source Fjord](./7_multi_source_fjord.md). For the full method
signature, see the [Library reference](./library_reference.md).
