"""
Stateful Refresh Tutorial: Keeping the Object Graph Live
--------------------------------------------------------
Companion script for `docs/9_stateful_refresh.md`.

Demonstrates the three `refresh()` resolution modes:
  1. In-state    — refresh()                (re-fetches every object in inc_dict)
  2. Re-source   — refresh(new_url)         (repoint the registry at a new URL)
  3. Targeted    — refresh(instance=[obj])  (refresh only specific instances)

Key insight: `refresh()` mutates the existing Pydantic models in place,
so Python references survive across calls — no need to reassign your
local variables.

Run with:
    python examples/9_stateful_refresh.py
"""

import asyncio

from incorporator import Incorporator


class User(Incorporator):
    pass


async def main() -> None:
    # ------------------------------------------------------------------
    # 1. INITIAL LOAD — populates User.inc_dict via incorp()
    # ------------------------------------------------------------------
    users = await User.incorp(
        inc_url="https://jsonplaceholder.typicode.com/users",
        inc_code="id",
        inc_name="name",
    )
    print(f"✅ Loaded {len(users)} users.")
    first_id = users[0].inc_code
    original_email = users[0].email
    print(f"   First user [{first_id}]: {original_email}")

    # ------------------------------------------------------------------
    # 2. IN-STATE REFRESH — re-fetch every instance from its stored origin
    # ------------------------------------------------------------------
    # No args: refresh() reads `inc_url` off each instance and dedupes
    # the call set, so a registry sourced from 1 URL = 1 HTTP call.
    refreshed = await User.refresh()
    print(f"\n🔄 In-state refresh: {len(refreshed)} instances re-hydrated.")

    # The original `users` list still works — Python references survived.
    # `users[0]` and `refreshed.inc_dict[first_id]` point at the SAME object.
    assert users[0] is refreshed.inc_dict[first_id]
    print(f"   users[0] is refreshed.inc_dict[{first_id!r}] → True")

    # ------------------------------------------------------------------
    # 3. TARGETED REFRESH — only re-fetch a chosen subset
    # ------------------------------------------------------------------
    stale = [users.inc_dict[uid] for uid in (1, 2, 3) if uid in users.inc_dict]
    partial = await User.refresh(instance=stale)
    print(f"\n🎯 Targeted refresh: {len(partial)} instances updated.")

    # ------------------------------------------------------------------
    # 4. RE-SOURCE REFRESH — repoint the registry at a new URL
    # ------------------------------------------------------------------
    # (Uses the same endpoint here for demonstration — in production
    # this is how you migrate a live registry from v1 → v2 of an API.)
    resourced = await User.refresh("https://jsonplaceholder.typicode.com/users")
    print(f"\n🔁 Re-sourced refresh: {len(resourced)} instances reloaded.")

    # Failed origins (if any) surface on the result list for DLQ retry.
    if resourced.failed_sources:
        print(f"⚠️  Failed sources to retry: {resourced.failed_sources}")


if __name__ == "__main__":
    asyncio.run(main())
