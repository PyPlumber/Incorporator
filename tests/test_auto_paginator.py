"""Integration test for the Invisible Heuristic URL Paginator."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator


class AutoItem(Incorporator): pass


async def mock_auto_pagination_api(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks an API that relies strictly on URL counters and returns empty arrays when finished."""

    # 1. Simulate an API using page increments (e.g., page=1, page=2)
    if "page=" in url:
        page = int(url.split("page=")[-1])
        if page == 1:
            payload = {"data": [{"id": 1, "name": "Item A"}]}
        elif page == 2:
            payload = {"data": [{"id": 2, "name": "Item B"}]}
        else:
            # Over-paged: API returns an empty array. The heuristic MUST catch this to prevent infinite loops!
            payload = {"data": []}

    # 2. Simulate an API using offset/limit chunks (e.g., limit=5&offset=0)
    elif "offset=" in url:
        offset = int(url.split("offset=")[-1])
        if offset == 0:
            payload = {"data": [{"id": 101, "name": "Offset Item A"}]}
        elif offset == 5:
            payload = {"data": [{"id": 102, "name": "Offset Item B"}]}
        else:
            payload = {"data": []}

    else:
        payload = {"data": []}

    # CRITICAL: We return NO Link headers. The framework is entirely on its own!
    return httpx.Response(200, text=json.dumps(payload))


@pytest.mark.asyncio
async def test_invisible_url_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves the framework automatically increments `page` and `offset` queries silently."""

    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_auto_pagination_api)

    # ==========================================
    # TEST 1: The Standard `page=` Incrementer
    # ==========================================
    # We set paginate=True but provide NO next_url_extractor!
    page_items = await AutoItem.incorp(
        inc_url="https://api.com/items?page=1",
        rec_path="data",
        paginate=True
    )

    assert isinstance(page_items, list)
    assert len(page_items) == 2  # It fetched page 1 and page 2, and cleanly stopped at page 3's empty array!
    assert getattr(page_items[1], "name") == "Item B"

    # ==========================================
    # TEST 2: The Multiplier `offset=` Incrementer
    # ==========================================
    offset_items = await AutoItem.incorp(
        inc_url="https://api.com/items?limit=5&offset=0",
        rec_path="data",
        paginate=True
    )

    assert isinstance(offset_items, list)
    assert len(offset_items) == 2  # It fetched offset 0 and offset 5, and cleanly stopped!
    assert getattr(offset_items[1], "name") == "Offset Item B"

    print(
        "\n✅ Invisible heuristic URL incrementer successfully processed both page loops without developer intervention!")