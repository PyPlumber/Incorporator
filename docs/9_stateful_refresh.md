***

# 🔄 Stateful Refresh: Keeping Your Object Graph Live

`incorp()` builds an object graph. `refresh()` keeps it **synchronised
with the source** — without rebuilding from scratch, without breaking
existing Python references.

The distinction matters. A second `incorp()` call gives you a new list of
new instances; the old references are stale. `refresh()` re-fetches and
mutates the **existing** Pydantic models in place, so anything holding a
reference to `users[0]` automatically sees the latest field values.

By the end of this tutorial you'll know the three instance-resolution
modes, how HTTP deduplication keeps refresh cheap on large registries,
and the production patterns that distinguish `refresh()` from
`stream()` / `fjord()`.

---

## The Three Resolution Modes

`refresh()` chooses what to re-fetch from the shape of the `instance`
argument:

### 1. In-state — `refresh()` (no args)

Re-fetches every object currently in `cls.inc_dict`. Origin URLs/files
come from each instance's stored `inc_url` / `inc_file`. This is the
most common mode — "update everything I've loaded."

```python
users = await User.incorp(inc_url="https://api.example.com/users")
# ... 30 minutes pass ...
refreshed = await User.refresh()        # uses each instance's stored origin
print(users[0].email)                   # already updated — same Python object
```

### 2. Re-source — `refresh(new_url)` or `refresh(new_path)`

Re-fetches the registry from a brand-new source. If the string starts
with `http` it's a URL; otherwise it's a local file path.

```python
# Repoint at a v2 endpoint without rebuilding the registry:
await User.refresh("https://api.example.com/v2/users")
```

### 3. Targeted — `refresh(instance=[obj, obj, ...])`

Refresh only the listed instances. Useful for partial updates — e.g.
re-checking a handful of records flagged stale by your business logic.

```python
stale = [users.inc_dict[uid] for uid in flagged_user_ids]
await User.refresh(instance=stale)
```

---

## HTTP Deduplication: Cheap Refresh at Scale

If 1,000 instances were loaded from 20 distinct source URLs (e.g. each
URL returned 50 records), `refresh()` performs **20 HTTP calls, not
1,000**. Origin URLs are deduplicated across the resolved instance set
before the network engine ever runs.

This makes in-state refresh the right choice for nightly cron jobs,
manual triggers, and "user clicked refresh" UI flows — even on
mid-six-figure registries.

---

## Refresh vs. Incorp vs. Stream

| Need | Reach for |
|---|---|
| First-time load of an API or file | `incorp()` |
| One-shot "pull the latest" on an already-loaded graph | `refresh()` |
| Continuous polling on a fixed cadence | `stream()` |
| Multi-source fan-out + fused outflow | `fjord()` |

`refresh()` is **stateless on cadence** — it runs once when you call
it. `stream()` wraps `incorp()` + `refresh()` in a daemon with its own
refresh/export intervals. If you find yourself writing
`while True: await User.refresh(); await asyncio.sleep(60)` — switch to
`stream()`.

---

## Step 1: A Minimal Refresh Loop

```python
import asyncio
from incorporator import Incorporator


class User(Incorporator):
    pass


async def main():
    # 1. Initial load — populates User.inc_dict
    users = await User.incorp(
        inc_url="https://jsonplaceholder.typicode.com/users",
        inc_code="id",
        inc_name="name",
    )
    print(f"Loaded {len(users)} users; first email: {users[0].email}")

    # 2. Trigger an in-state refresh — same instances, latest values
    refreshed = await User.refresh()
    print(f"Refreshed {len(refreshed)} users; first email: {users[0].email}")

    # 3. The original `users` list still works — Python references survived.
    assert users[0] is refreshed.inc_dict[users[0].inc_code]


if __name__ == "__main__":
    asyncio.run(main())
```

That's it. Two verbs, one shared registry, zero stale references.

---

## When refresh() raises

* **No instances loaded.** Calling `refresh()` before any `incorp()`
  (and without a new URL/file) raises a `ValueError` — there's nothing
  to refresh and no origin to fetch from.
* **Origin missing on a targeted instance.** If you pass `instance=[obj]`
  but `obj.inc_url` / `obj.inc_file` is `None`, the framework logs a
  warning and skips it rather than crashing the batch.

Transient HTTP errors are handled by the same Tenacity retry policy
that `incorp()` uses; permanent failures surface via
`refreshed.failed_sources` for DLQ-style retry workflows.

---

## See Also

* [`stream()`](./6_streaming_daemon.md) — the daemon form of `refresh()`
  on a cadence.
* [Production debugging with `get_error()`](./10_debugging_get_error.md) —
  what to do when `failed_sources` is non-empty.
* [Library reference](./library_reference.md) — full signature, every
  kwarg.
