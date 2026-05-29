"""Unit tests for the web-API paginator subclasses.

Tests bind each paginator to a mock ``fetch_func`` and walk it through the
expected response sequence, verifying state progression, exhaustion
detection, infinite-loop defence, and reset semantics.
"""

import json
from typing import Any, Dict, List, Optional

import httpx
import pytest

from incorporator.io.pagination import (
    CursorPaginator,
    LinkHeaderPaginator,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)


def _make_response(url: str, body: Any, headers: Optional[Dict[str, str]] = None, status: int = 200) -> httpx.Response:
    """Build an httpx.Response with a real Request attached so .url works downstream."""
    req = httpx.Request("GET", url)
    return httpx.Response(status, text=json.dumps(body), headers=headers or {}, request=req)


# ==========================================
# 1. LinkHeaderPaginator
# ==========================================


@pytest.mark.asyncio
async def test_link_header_paginator_follows_rel_next() -> None:
    """LinkHeaderPaginator must follow rel='next' across two pages then exhaust."""
    pages = [
        _make_response(
            "https://api.example.com/items?page=1",
            [{"id": 1}],
            headers={"Link": '<https://api.example.com/items?page=2>; rel="next"'},
        ),
        _make_response(
            "https://api.example.com/items?page=2",
            [{"id": 2}],
            headers={},  # no next link → exhaust
        ),
    ]
    call_log: List[str] = []

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        call_log.append(url)
        return pages[len(call_log) - 1]

    p = LinkHeaderPaginator()
    p.fetch_func = mock_fetch
    yielded = [chunk async for chunk in p.paginate("https://api.example.com/items?page=1")]

    assert len(yielded) == 2
    assert len(call_log) == 2
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_link_header_paginator_exhausts_on_missing_header() -> None:
    """A single response with no Link header must yield once and mark exhausted."""

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, [{"id": 1}], headers={})

    p = LinkHeaderPaginator()
    p.fetch_func = mock_fetch
    pages = [chunk async for chunk in p.paginate("https://api.example.com/items")]
    assert len(pages) == 1
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_link_header_paginator_reset_restores_state() -> None:
    """reset() must clear is_exhausted / current_url / is_first_call for daemon reuse."""
    p = LinkHeaderPaginator()
    p.is_exhausted = True
    p.current_url = "https://stale"
    p.is_first_call = False

    p.reset()

    assert p.is_exhausted is False
    assert p.current_url is None
    assert p.is_first_call is True


# ==========================================
# 2. CursorPaginator
# ==========================================


@pytest.mark.asyncio
async def test_cursor_paginator_progresses_through_cursors() -> None:
    """CursorPaginator must send the cursor as a query param and follow next_cursor."""
    pages = [
        _make_response("https://api.example.com/c", {"data": [1], "next_cursor": "B"}),
        _make_response("https://api.example.com/c", {"data": [2], "next_cursor": "C"}),
        _make_response("https://api.example.com/c", {"data": [3]}),  # no next → exhaust
    ]
    seen_params: List[Optional[Dict[str, Any]]] = []
    iterator = iter(pages)

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        seen_params.append(request_params)
        return next(iterator)

    p = CursorPaginator(cursor_param="cursor")
    p.fetch_func = mock_fetch
    yielded = [chunk async for chunk in p.paginate("https://api.example.com/c")]

    assert len(yielded) == 3
    # The first call has no cursor, then cursor=B, then cursor=C
    assert seen_params[0] == {}
    assert seen_params[1] == {"cursor": "B"}
    assert seen_params[2] == {"cursor": "C"}
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_cursor_paginator_blocks_infinite_loop() -> None:
    """A repeating next_cursor MUST be treated as exhaustion to break the cycle."""
    # Both pages echo the same next_cursor — that's the loop trap
    pages = [
        _make_response("https://api.example.com/c", {"data": [1], "next_cursor": "X"}),
        _make_response("https://api.example.com/c", {"data": [2], "next_cursor": "X"}),
    ]
    iterator = iter(pages)

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return next(iterator)

    p = CursorPaginator()
    p.fetch_func = mock_fetch
    yielded = [chunk async for chunk in p.paginate("https://api.example.com/c")]

    # Two pages then stop — the third request would be the loop
    assert len(yielded) == 2
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_cursor_paginator_reset_clears_seen_cursors() -> None:
    """reset() must clear seen_cursors so the paginator can be reused safely."""
    p = CursorPaginator()
    p.seen_cursors.add("A")
    p.seen_cursors.add("B")
    p.current_cursor = "B"
    p.is_exhausted = True

    p.reset()

    assert p.seen_cursors == set()
    assert p.current_cursor is None
    assert p.is_exhausted is False


# ==========================================
# 3. PageNumberPaginator
# ==========================================


@pytest.mark.asyncio
async def test_page_number_paginator_increments_page() -> None:
    """PageNumberPaginator must send ?page=N and increment until the results list is empty."""
    pages = [
        _make_response("https://api.example.com/p", {"results": [{"id": 1}]}),
        _make_response("https://api.example.com/p", {"results": [{"id": 2}]}),
        _make_response("https://api.example.com/p", {"results": []}),  # exhaust
    ]
    seen_params: List[Optional[Dict[str, Any]]] = []
    iterator = iter(pages)

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        seen_params.append(request_params)
        return next(iterator)

    p = PageNumberPaginator(start_page=1)
    p.fetch_func = mock_fetch
    yielded = [chunk async for chunk in p.paginate("https://api.example.com/p")]

    # Three requests total — two with data, the third empty triggers exhaust
    assert seen_params == [{"page": 1}, {"page": 2}, {"page": 3}]
    assert len(yielded) == 3
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_page_number_paginator_call_lim_caps_pages() -> None:
    """call_lim must cap the number of pages even when more data is available."""

    # Endless mock — every page returns one record
    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, {"results": [{"id": 1}]})

    p = PageNumberPaginator()
    p.fetch_func = mock_fetch
    p.call_lim = 2  # Stop after 2 pages even though the mock would feed forever

    pages = [chunk async for chunk in p.paginate("https://api.example.com/p")]
    assert len(pages) == 2


# ==========================================
# 4. LinkHeaderPaginator — is_exhausted + error path
# ==========================================


@pytest.mark.asyncio
async def test_link_header_is_exhausted_guard_returns_immediately() -> None:
    """paginate() on an already-exhausted LinkHeaderPaginator must yield nothing."""
    p = LinkHeaderPaginator()
    p.is_exhausted = True
    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []


@pytest.mark.asyncio
async def test_link_header_error_non_strict_stops_gracefully() -> None:
    """An httpx error in non-strict mode must stop the paginator without propagating."""

    async def raising_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.HTTPStatusError("500 Server Error", request=req, response=httpx.Response(500, request=req))

    p = LinkHeaderPaginator()
    p.fetch_func = raising_fetch
    p.strict_mode = False

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_link_header_call_lim_caps_pages() -> None:
    """call_lim must stop the paginator after the specified number of pages."""

    async def endless_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, [{"id": 1}], headers={"Link": f'<{url}>; rel="next"'})

    p = LinkHeaderPaginator()
    p.fetch_func = endless_fetch
    p.call_lim = 2

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 2


# ==========================================
# 5. CursorPaginator — is_exhausted, call_lim, list response, error
# ==========================================


@pytest.mark.asyncio
async def test_cursor_is_exhausted_guard_returns_immediately() -> None:
    """paginate() on an already-exhausted CursorPaginator must yield nothing."""
    p = CursorPaginator()
    p.is_exhausted = True
    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []


@pytest.mark.asyncio
async def test_cursor_call_lim_caps_pages() -> None:
    """call_lim must cap CursorPaginator pages even when next_cursor is always present."""

    async def endless_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, {"data": [1], "next_cursor": "FOREVER"})

    p = CursorPaginator()
    p.fetch_func = endless_fetch
    p.call_lim = 2

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_cursor_list_response_exhausts_immediately() -> None:
    """A list (non-dict) response gives next_cursor=None → paginator exhausts after one page."""

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, [{"id": 1}, {"id": 2}])  # list, not dict

    p = CursorPaginator()
    p.fetch_func = mock_fetch

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 1
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_cursor_error_non_strict_stops_gracefully() -> None:
    """httpx error in non-strict CursorPaginator must stop without propagating."""

    async def raising_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.RequestError("timeout", request=req)

    p = CursorPaginator()
    p.fetch_func = raising_fetch
    p.strict_mode = False

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []
    assert p.is_exhausted is True


# ==========================================
# 6. OffsetPaginator — call_lim, list response, exhaustion, error
# ==========================================


@pytest.mark.asyncio
async def test_offset_paginator_call_lim_caps_pages() -> None:
    """call_lim must stop the OffsetPaginator after N fetches."""

    async def endless_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, {"results": [{"id": 1}]})

    p = OffsetPaginator(limit=10)
    p.fetch_func = endless_fetch
    p.call_lim = 2

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_offset_paginator_list_response_used_as_items() -> None:
    """A raw list response (instead of dict) is used directly as the items list."""
    pages = [
        _make_response("https://api.example.com", [{"id": 1}, {"id": 2}]),  # list
        _make_response("https://api.example.com", []),  # empty list → exhaust
    ]
    iterator = iter(pages)

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return next(iterator)

    p = OffsetPaginator()
    p.fetch_func = mock_fetch

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 2
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_offset_paginator_error_non_strict_stops_gracefully() -> None:
    """httpx error in non-strict OffsetPaginator must stop without propagating."""

    async def raising_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.RequestError("connection refused", request=req)

    p = OffsetPaginator()
    p.fetch_func = raising_fetch
    p.strict_mode = False

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []
    assert p.is_exhausted is True


# ==========================================
# 7. PageNumberPaginator — error path
# ==========================================


@pytest.mark.asyncio
async def test_page_number_error_non_strict_stops_gracefully() -> None:
    """httpx error in non-strict PageNumberPaginator must stop without propagating."""

    async def raising_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.HTTPStatusError("403 Forbidden", request=req, response=httpx.Response(403, request=req))

    p = PageNumberPaginator()
    p.fetch_func = raising_fetch
    p.strict_mode = False

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []
    assert p.is_exhausted is True


# ==========================================
# 8. NextUrlPaginator — is_exhausted, call_lim, error
# ==========================================


@pytest.mark.asyncio
async def test_next_url_is_exhausted_guard_returns_immediately() -> None:
    """paginate() on an already-exhausted NextUrlPaginator must yield nothing."""
    p = NextUrlPaginator("next")
    p.is_exhausted = True
    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []


@pytest.mark.asyncio
async def test_next_url_paginator_follows_next_key() -> None:
    """NextUrlPaginator must drill into JSON body and follow 'next' URL."""
    pages = [
        _make_response("https://api.example.com/p1", {"results": [1], "next": "https://api.example.com/p2"}),
        _make_response("https://api.example.com/p2", {"results": [2], "next": None}),
    ]
    iterator = iter(pages)

    async def mock_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return next(iterator)

    p = NextUrlPaginator("next")
    p.fetch_func = mock_fetch

    chunks = [chunk async for chunk in p.paginate("https://api.example.com/p1")]
    assert len(chunks) == 2
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_next_url_call_lim_caps_pages() -> None:
    """call_lim must stop NextUrlPaginator even when next URL is always present."""

    async def endless_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        return _make_response(url, {"results": [1], "next": "https://api.example.com/forever"})

    p = NextUrlPaginator("next")
    p.fetch_func = endless_fetch
    p.call_lim = 3

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert len(chunks) == 3


@pytest.mark.asyncio
async def test_next_url_error_non_strict_stops_gracefully() -> None:
    """httpx error in non-strict NextUrlPaginator must stop without propagating."""

    async def raising_fetch(url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        raise httpx.RequestError("DNS failure", request=req)

    p = NextUrlPaginator("next")
    p.fetch_func = raising_fetch
    p.strict_mode = False

    chunks = [chunk async for chunk in p.paginate("https://api.example.com")]
    assert chunks == []
    assert p.is_exhausted is True


@pytest.mark.asyncio
async def test_page_number_paginator_reset_restores_start_page() -> None:
    """reset() must restore current_page to start_page so daemon polling restarts cleanly."""
    p = PageNumberPaginator(start_page=5)
    p.current_page = 99
    p.is_exhausted = True

    p.reset()

    assert p.current_page == 5
    assert p.is_exhausted is False
