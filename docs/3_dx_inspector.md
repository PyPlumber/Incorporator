***

# 🕵️‍♂️ DX Inspector: Let the Framework Write Your Kwargs

You've found an unknown REST API. What's the schema? What's the
`inc_code`? Is there a `rec_path` wrapping the records? Are any fields
ISO-8601 strings that should be cast to `datetime`?

You could open Postman, eyeball the JSON, and write a half-dozen
hypothesis `incorp()` calls. Or you could let Incorporator do it
for you.

`test()` is the **JIT API Profiler**. Hand it the URL of any endpoint
and it fetches one safe page, walks the payload tree, runs regex-based
value scoring to detect identity-shaped fields (UUIDs, timestamps,
slugs, etc.), and **prints the exact `incorp()` kwargs you'd write
yourself** — minus the trial and error.

---

## Step 1: Hit an Unknown Endpoint

```python
import asyncio
from incorporator import Incorporator


class Coin(Incorporator):
    pass


asyncio.run(Coin.test(inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"))
```

That's it. **Swap `.incorp()` for `.test()`** to trigger the inspector.

---

## Step 2: Read the Report

The inspector prints a five-section structured report:

```text
======================================================================
🕵️‍♂️  INCORPORATOR DX INSPECTOR
======================================================================

📦 1. PAYLOAD STRUCTURE:
   ├── id: str = bitcoin
   ├── symbol: str = btc
   ├── name: str = Bitcoin
   ├── current_price: float = 67234.51
   ├── market_cap: int = 1325000000000
   ├── last_updated: str = 2026-05-14T12:00:00.000Z
   └── ath_date: str = 2024-03-14T07:10:36.635Z

🔑 2. IDENTITY MAPPING:
   Recommended kwargs for O(1) Memory Registry:
   ✅ inc_code='id'
   ✅ inc_name='name'

🛠️  3. ETL / TYPE CASTING SUGGESTIONS:
   💡 The framework's runtime parsers would coerce these. Consider:
      conv_dict={
          'last_updated': inc(datetime),
          'ath_date': inc(datetime),
      }

📑 4. PAGINATION HINTS:
   (Skipped — this response has no pagination metadata.)

🗑️  5. HEAVY-FIELD HINTS:
   💡 Fields likely to bloat the payload — consider excluding:
      excl_lst=['image']
======================================================================
```

Each section is actionable:

1. **Payload structure.** Tree-view of every key Python sees, with
   types and sample values. Warns when the root shape doesn't match
   common API conventions (offers `rec_path=` for wrapper-shaped roots).
2. **Identity mapping.** Regex-scored candidates for `inc_code`
   (UUIDs, integer IDs, slugs) and `inc_name` (display strings).
3. **Type casting.** Routes detection through the framework's own
   `parses_as_datetime` / `parses_as_int` / `parses_as_float`
   predicates — every suggestion is structurally what `inc()` would
   accept at runtime.
4. **Pagination hints.** Detects `next` / `cursor` / `offset+limit`
   shapes and suggests the matching paginator.
5. **Heavy-field hints.** Flags asset URLs, base64 blobs, and
   oversized strings that should land in `excl_lst`.

---

## Step 3: Turn the Report Into a Real Call

Copy the suggestions verbatim. Same class, swap `.test()` for
`.incorp()`, paste the kwargs:

```python
from datetime import datetime
from incorporator import Incorporator
from incorporator.schema.converters import inc


class Coin(Incorporator):
    pass


async def main():
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
        inc_code="id",                                       # from identity mapping
        inc_name="name",
        conv_dict={                                          # from type casting
            "last_updated": inc(datetime),
            "ath_date": inc(datetime),
        },
        excl_lst=["image"],                                  # from heavy-field hints
    )
    print(f"Loaded {len(coins)} coins.")
    btc = coins.inc_dict["bitcoin"]
    print(f"BTC: ${btc.current_price:,.2f} (updated {btc.last_updated:%Y-%m-%d %H:%M})")
```

You went from *"what does this API look like?"* to a fully-typed,
indexed, datetime-aware object graph **without writing or reading a
schema**.

---

## Drilling Into Nested Arrays

When the inspector finds nested list-of-dicts inside the top-level
record (e.g. SpaceX `/launches/latest` has `cores: [...]`), it
surfaces them as a drill-down hint with a copy-pasteable command:

```text
⚠️  The root object also contains nested arrays:  cores (24), failures (3)
💡 To map one of those instead, add `rec_path` and re-run test():
   await YourClass.test(inc_url=..., rec_path='cores')
```

Identity + ETL still analyze the **top-level** record. The drill hint
just lets you opt into a different level if that's what you actually
want to map.

---

## What `test()` Does to Stay Safe

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

## When to Reach for `test()`

| Situation | Use `test()`? |
|---|---|
| New endpoint, no idea what shape returns | ✅ Yes — your first call should be `.test()` |
| Endpoint changed shape unexpectedly | ✅ Yes — diff the inspector output against your last known good config |
| Building a `pipeline.json` for the CLI | ✅ Yes — paste the inspector suggestions into the JSON |
| Production daemon | ❌ No — use `incorp()` / `stream()` / `fjord()` with the kwargs `test()` suggested |

---

## See Also

* **[Tutorial 4 — Parent-Child Drilling](./4_parent_child_drilling.md)** —
  use `test()` against an unknown nested API before drilling.
* **[Tutorial 5 — Stateful Refresh](./5_stateful_refresh.md)** — once
  the kwargs are right, keep the registry live.
* **[Library reference](./library_reference.md)** — full method
  signature for `test()`.
