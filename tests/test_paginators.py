"""Integration tests for the Explicit Asynchronous Pagination Engine."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.paginate import NextUrlPaginator, OffsetPaginator


class PaginatedItem(Incorporator): pass


async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks an API to test both NextUrl and Offset pagination strategies."""

    # httpx passes query string arguments via the 'params' kwarg
    params = kwargs.get("params", {})

    # ==========================================
    # MOCK 1: Offset/Limit API
    # ==========================================
    if "offset" in params:
        offset = int(params["offset"])
        if offset == 0:
            payload = {"results": [{"id": 101, "name": "Offset Item A"}]}
        elif offset == 5:
            payload = {"results": [{"id": 102, "name": "Offset Item B"}]}
        else:
            # Over-paged: API returns empty
            payload = {"results": []}

        return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))

    # ==========================================
    # MOCK 2: Next URL API (e.g. PokéAPI)
    # ==========================================
    if "page=2" in url:
        payload = {
            "results": [{"id": 2, "name": "NextUrl Item B"}],
            "next": None  # Stop condition
        }
    else:
        payload = {
            "results": [{"id": 1, "name": "NextUrl Item A"}],
            "next": "https://api.com/items?page=2"
        }

    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_explicit_offset_paginator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves OffsetPaginator safely injects mathematical offsets via httpx params."""
    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_request)

    offset_items = await PaginatedItem.incorp(
        inc_url="https://api.com/items",
        rec_path="results",
        # NEW SYNTAX: Explicit Strategy Pattern
        inc_page=OffsetPaginator(limit=5)
    )

    assert isinstance(offset_items, list)
    assert len(offset_items) == 2
    assert getattr(offset_items[0], "name") == "Offset Item A"
    assert getattr(offset_items[1], "name") == "Offset Item B"


@pytest.mark.asyncio
async def test_explicit_next_url_paginator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves NextUrlPaginator safely extracts subsequent URLs from the JSON body."""
    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_execute_request)

    page_items = await PaginatedItem.incorp(
        inc_url="https://api.com/items",
        rec_path="results",
        # NEW SYNTAX: Explicit Strategy Pattern
        inc_page=NextUrlPaginator()
    )

    assert isinstance(page_items, list)
    assert len(page_items) == 2
    assert getattr(page_items[0], "name") == "NextUrl Item A"
    assert getattr(page_items[1], "name") == "NextUrl Item B"