"""Cross-engine helpers shared by chunked / stateful / fjord engines."""

import asyncio
from typing import Any, Dict, Optional


async def _interruptible_sleep(event: asyncio.Event, timeout: Optional[float]) -> bool:
    """Sleeps for `timeout` seconds, returning True immediately if `event` fires first."""
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


async def _enrich_and_load(
    cls: Any,
    dataset: Any,
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    force_append: bool,
) -> None:
    """Atomic helper for the Enrich (Refresh) and Load (Export) phases."""
    # ``is not None`` rather than truthy: empty dict ``{}`` MUST opt into the
    # call ("run with default kwargs"); a truthy check treats ``{}`` as falsy
    # and silently skips, contradicting the documented contract.
    if refresh_params is not None:
        await cls.refresh(instance=dataset, **refresh_params)

    if export_params is not None:
        params = export_params.copy() if force_append else export_params
        if force_append:
            params["if_exists"] = "append"
        await cls.export(instance=dataset, **params)


def _row_count(dataset: Any) -> int:
    """Returns the number of rows in a dataset (list length, 1 for a single object, 0 for falsy)."""
    return len(dataset) if isinstance(dataset, list) else (1 if dataset else 0)
