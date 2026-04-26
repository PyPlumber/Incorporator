"""
Advanced Asynchronous Pagination Engine for the Incorporator Framework.

Provides isolated OOP strategies to gracefully handle Next URL, Cursor, Offset,
and Metadata pagination patterns with built-in Exception logging.
"""

import logging
import re
from typing import Any, AsyncGenerator, Callable, Dict, Optional
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)


class AsyncPaginator:
    """Base paginator with shared request execution."""

    def __init__(self) -> None:
        self.call_lim: Optional[int] = None
        self.fetch_func: Optional[Callable[..., Any]] = None

    async def _fetch(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        """Executes the network request."""
        if not self.fetch_func:
            raise RuntimeError("Paginator must be bound to a network client before use.")
        # Errors will bubble up to the subclass to be handled gracefully!
        return await self.fetch_func(url=url, request_params=params)

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        """Yields raw text payloads. Must be overridden by subclasses."""
        if False:
            yield ""
        raise NotImplementedError


# --- 1. Link Header Pagination ---
class LinkHeaderPaginator(AsyncPaginator):
    """Example: GitHub API (Link header with rel="next")."""

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        url: Optional[str] = start_url
        calls = 0

        while url:
            if self.call_lim and calls >= self.call_lim:
                break

            try:
                # PROTECTED NETWORK CALL
                response = await self._fetch(url)
                yield response.text
                calls += 1

                next_link = None
                if "link" in response.headers:
                    links = response.headers["link"].split(",")
                    for link in links:
                        if 'rel="next"' in link:
                            match = re.search(r'<(.*?)>', link)
                            if match:
                                next_link = match.group(1)

                url = urljoin(str(response.url), next_link) if next_link else None
            except Exception as e:
                logger.warning(f"LinkHeader pagination stopped gracefully: {e}")
                break


# --- 2. Next URL in Body (Deep-Drill) ---
class NextUrlPaginator(AsyncPaginator):
    """Example: PokéAPI (returns 'next' URL in JSON body)."""

    def __init__(self, *path_keys: str) -> None:
        super().__init__()
        self.path_keys = path_keys if path_keys else ("next",)

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        url: Optional[str] = start_url
        calls = 0

        while url:
            if self.call_lim and calls >= self.call_lim:
                break

            try:
                # PROTECTED NETWORK CALL
                response = await self._fetch(url)
                yield response.text
                calls += 1

                data = response.json()
                for key in self.path_keys:
                    if isinstance(data, dict):
                        data = data.get(key)
                    else:
                        data = None
                        break

                url = str(data) if data else None
            except Exception as e:
                logger.warning(f"NextUrl pagination stopped gracefully: {e}")
                break


# --- 3. Cursor-Based Pagination ---
class CursorPaginator(AsyncPaginator):
    """Example: Twitter/X API v2 (uses 'next_token' cursor)."""

    def __init__(self, cursor_param: str = "cursor") -> None:
        super().__init__()
        self.cursor_param = cursor_param

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        cursor: Optional[str] = None
        calls = 0

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.cursor_param: cursor} if cursor else {}

            try:
                # PROTECTED NETWORK CALL
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                data = response.json()
                cursor = data.get("meta", {}).get("next_token") or data.get("next_cursor")
                if not cursor:
                    break
            except Exception as e:
                logger.warning(f"Cursor pagination stopped gracefully: {e}")
                break


# --- 4. Offset + Limit Pagination ---
class OffsetPaginator(AsyncPaginator):
    """Example: Open Library API (offset + limit)."""

    def __init__(self, limit: int = 50, offset_param: str = "offset", limit_param: str = "limit") -> None:
        super().__init__()
        self.limit = limit
        self.offset_param = offset_param
        self.limit_param = limit_param

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        offset = 0
        calls = 0

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.offset_param: offset, self.limit_param: self.limit}

            try:
                # PROTECTED NETWORK CALL
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                data = response.json()
                items = data.get("results") or data.get("docs", [])
                if not items:
                    break
                offset += self.limit
            except Exception as e:
                logger.warning(f"Offset pagination stopped gracefully at offset {offset}: {e}")
                break


# --- 5. Page Number Pagination ---
class PageNumberPaginator(AsyncPaginator):
    """Example: CoinGecko or ReqRes API (page=1, page=2, ...)."""

    def __init__(self, page_param: str = "page", start_page: int = 1, *items_keys: str) -> None:
        super().__init__()
        self.page_param = page_param
        self.start_page = start_page

    async def paginate(self, start_url: str) -> AsyncGenerator[str, None]:
        page = self.start_page
        calls = 0

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.page_param: page}

            try:
                # PROTECTED NETWORK CALL
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                data = response.json()
                if not data:
                    break
                page += 1
            except Exception as e:
                logger.warning(f"PageNumber pagination stopped gracefully at page {page}: {e}")
                break