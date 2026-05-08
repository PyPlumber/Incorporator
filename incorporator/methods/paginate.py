"""
Advanced Asynchronous Pagination Engine for the Incorporator Framework.

Provides isolated OOP strategies to gracefully handle Next URL, Cursor, Offset,
and Metadata pagination patterns with built-in Exception logging.
"""

import logging
import re
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Optional
from urllib.parse import urljoin

import httpx

# IMPORT OUR FORMAT ENGINE
from .format_parsers import infer_format, parse_source_data

logger = logging.getLogger(__name__)


class AsyncPaginator:
    def __init__(self) -> None:
        self.call_lim: Optional[int] = None
        self.fetch_func: Optional[Callable[..., Awaitable[httpx.Response]]] = None

    async def _fetch(
        self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> httpx.Response:
        """Executes the network request, allowing dynamic POST payload overrides via kwargs."""
        if not self.fetch_func:
            raise RuntimeError("Paginator must be bound to a network client before use.")
        return await self.fetch_func(url=url, request_params=params, **kwargs)

    async def _parse_response(self, response: httpx.Response) -> Any:
        """Format-Agnostic Parser: Gracefully handles JSON, XML, or CSV pagination logic."""
        fmt = infer_format(str(response.url))
        return await parse_source_data(response.text, fmt)

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
                response = await self._fetch(url)
                yield response.text
                calls += 1

                next_link = None
                if "link" in response.headers:
                    links = response.headers["link"].split(",")
                    for link in links:
                        if 'rel="next"' in link:
                            match = re.search(r"<(.*?)>", link)
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
                response = await self._fetch(url)
                yield response.text
                calls += 1

                # 🛡️ Format-Agnostic Parse
                data = await self._parse_response(response)

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
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                # 🛡️ Format-Agnostic Parse
                data = await self._parse_response(response)

                if isinstance(data, dict):
                    cursor = data.get("meta", {}).get("next_token") or data.get("next_cursor")
                else:
                    cursor = None

                if not cursor:
                    break
            except Exception as e:
                logger.warning(f"Cursor pagination stopped gracefully: {e}")
                break


# --- 4. Offset + Limit Pagination ---
class OffsetPaginator(AsyncPaginator):
    """Example: Open Library API (offset + limit)."""

    def __init__(
        self, limit: int = 50, offset_param: str = "offset", limit_param: str = "limit"
    ) -> None:
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
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                # 🛡️ Format-Agnostic Parse
                data = await self._parse_response(response)

                if isinstance(data, dict):
                    items = data.get("results") or data.get("docs", [])
                else:
                    items = data  # Failsafe for lists (like CSVs without wrappers)

                if not items:
                    break
                offset += self.limit
            except Exception as e:
                logger.warning(f"Offset pagination stopped gracefully at offset {offset}: {e}")
                break


# --- 5. Page Number Pagination ---
class PageNumberPaginator(AsyncPaginator):
    """Example: CoinGecko or ReqRes API (page=1, page=2, ...)."""

    def __init__(self, page_param: str = "page", start_page: int = 1) -> None:
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
                response = await self._fetch(start_url, params=params)
                yield response.text
                calls += 1

                # 🛡️ Format-Agnostic Parse
                data = await self._parse_response(response)

                if not data:
                    break
                page += 1
            except Exception as e:
                logger.warning(f"PageNumber pagination stopped gracefully at page {page}: {e}")
                break
