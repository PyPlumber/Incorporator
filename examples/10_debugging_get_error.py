"""
Production Debugging Tutorial: get_error() + LoggedIncorporator
---------------------------------------------------------------
Companion script for `docs/10_debugging_get_error.md`.

Demonstrates the durable-error retrieval loop:

    1. Subclass LoggedIncorporator + set enable_logging = True
    2. Run incorp() against a mix of good and deliberately-broken URLs
    3. Read failed_sources off the live result (the per-tick view)
    4. Call await Class.get_error() for the durable, structured view
    5. Feed wave.failed_sources back into incorp() as a DLQ retry loop

Run with:
    python examples/10_debugging_get_error.py
"""

import asyncio

from incorporator import LoggedIncorporator


class Webhook(LoggedIncorporator):
    """Production-shaped ingester — every failure lands in logs/Webhook_error.log."""

    enable_logging = True


async def main() -> None:
    # ------------------------------------------------------------------
    # 1. Run with a deliberate mix of good and broken URLs.
    # ------------------------------------------------------------------
    sources = [
        "https://jsonplaceholder.typicode.com/users/1",
        "https://jsonplaceholder.typicode.com/users/2",
        "https://this-host-does-not-exist.example.invalid/data",
    ]
    webhooks = await Webhook.incorp(inc_url=sources, inc_code="id")
    print(f"✅ Loaded {len(webhooks)} records from {len(sources)} sources.")
    print(f"📋 failed_sources (live view): {webhooks.failed_sources}")

    # ------------------------------------------------------------------
    # 2. Query the durable error log — survives across processes.
    # ------------------------------------------------------------------
    errors = await Webhook.get_error()
    print(f"\n🩺 get_error() returned {len(errors)} log records.")
    for record in errors[:3]:                                  # peek at first 3
        ts = record.get("timestamp", "?")
        msg = record.get("msg", "?")
        print(f"   [{ts}] {msg}")

    # ------------------------------------------------------------------
    # 3. The production DLQ retry shape — drain + dedupe + reissue.
    # ------------------------------------------------------------------
    dlq_urls: list[str] = []
    for record in errors:
        wave = record.get("wave") or {}
        dlq_urls.extend(wave.get("failed_sources", []))

    dlq_urls = list(set(dlq_urls))                             # dedupe across ticks

    if dlq_urls:
        print(f"\n♻️  Retrying {len(dlq_urls)} previously-failed URLs...")
        # In production you'd reissue them through incorp():
        # retried = await Webhook.incorp(inc_url=dlq_urls, inc_code="id")
        # (Commented out so the example exits cleanly — the bad URL still fails.)
    else:
        print("\n✅ DLQ empty — nothing to retry.")


if __name__ == "__main__":
    asyncio.run(main())
