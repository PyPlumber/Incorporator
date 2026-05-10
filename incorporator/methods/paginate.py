"""
Advanced Asynchronous Pagination Engine for the Incorporator Framework.

Provides isolated OOP strategies to gracefully handle Next URL, Cursor, Offset,
and Metadata pagination patterns with built-in Exception logging.
"""

import logging
import re
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, Optional, Set, Union
from urllib.parse import urljoin

import httpx

# IMPORT OUR FORMAT ENGINE
from .format_parsers import infer_format, parse_source_data

logger = logging.getLogger(__name__)


class AsyncPaginator:
    def __init__(self) -> None:
        self.call_lim: Optional[int] = None
        self.fetch_func: Optional[Callable[..., Awaitable[httpx.Response]]] = None
        self.strict_mode: bool = False

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
        # Pass raw bytes to the parser so it handles decoding natively
        return await parse_source_data(response.read(), fmt)

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yields raw byte payloads to preserve binary/compression integrity."""
        if False:
            yield b""
        raise NotImplementedError


# --- 1. Link Header Pagination ---
class LinkHeaderPaginator(AsyncPaginator):
    """Example: GitHub API (Link header with rel="next")."""

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        url: Optional[str] = start_url
        calls = 0

        while url:
            if self.call_lim and calls >= self.call_lim:
                break

            try:
                response = await self._fetch(url)
                yield response.read()  # Yield raw bytes!
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
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"LinkHeaderPaginator stopped gracefully: {e}")
                break


# --- 2. Next URL in Body (Deep-Drill) ---
class NextUrlPaginator(AsyncPaginator):
    """Example: PokéAPI (returns 'next' URL in JSON body)."""

    def __init__(self, *path_keys: str) -> None:
        super().__init__()
        self.path_keys = path_keys if path_keys else ("next",)

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        url: Optional[str] = start_url
        calls = 0
        seen_urls: Set[str] = set()

        while url:
            if self.call_lim and calls >= self.call_lim:
                break

            # Infinite Loop Protection
            if url in seen_urls:
                logger.warning(f"Infinite loop detected! API returned previously visited URL: {url}")
                break
            seen_urls.add(url)

            try:
                response = await self._fetch(url)
                yield response.read()  # Yield raw bytes!
                calls += 1

                data = await self._parse_response(response)

                for key in self.path_keys:
                    if isinstance(data, dict):
                        data = data.get(key)
                    else:
                        data = None
                        break

                # Robustly join relative paths to the base domain
                url = urljoin(str(response.url), str(data)) if data else None
            except Exception as e:
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"NextUrlPaginator stopped gracefully: {e}")
                break


# --- 3. Cursor-Based Pagination ---
class CursorPaginator(AsyncPaginator):
    """Example: Twitter/X API v2 (uses 'next_token' cursor)."""

    def __init__(self, cursor_param: str = "cursor") -> None:
        super().__init__()
        self.cursor_param = cursor_param

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        cursor: Optional[str] = None
        calls = 0
        seen_cursors: Set[str] = set()

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            if cursor:
                # Infinite Loop Protection
                if cursor in seen_cursors:
                    logger.warning(f"Infinite loop detected! API returned duplicate cursor: {cursor}")
                    break
                seen_cursors.add(cursor)

            params = {self.cursor_param: cursor} if cursor else {}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()  # Yield raw bytes!
                calls += 1

                data = await self._parse_response(response)

                if isinstance(data, dict):
                    # Smart fallback list looking for common cursor names, including the user's custom param
                    cursor = data.get("meta", {}).get("next_token") or data.get("next_cursor") or data.get(
                        self.cursor_param)
                else:
                    cursor = None

                if not cursor:
                    break
            except Exception as e:
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"CursorPaginator stopped gracefully: {e}")
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

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        offset = 0
        calls = 0

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.offset_param: offset, self.limit_param: self.limit}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()  # Yield raw bytes!
                calls += 1

                data = await self._parse_response(response)

                if isinstance(data, dict):
                    items = data.get("results") or data.get("docs", [])
                else:
                    items = data

                if not items:
                    break
                offset += self.limit
            except Exception as e:
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"OffsetPaginator stopped gracefully: {e}")
                break


# --- 5. Page Number Pagination ---
class PageNumberPaginator(AsyncPaginator):
    """Example: CoinGecko or ReqRes API (page=1, page=2, ...)."""

    def __init__(self, page_param: str = "page", start_page: int = 1) -> None:
        super().__init__()
        self.page_param = page_param
        self.start_page = start_page

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        page = self.start_page
        calls = 0

        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.page_param: page}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()  # Yield raw bytes!
                calls += 1

                data = await self._parse_response(response)

                if not data:
                    break
                page += 1
            except Exception as e:
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"PageNumberPaginator stopped gracefully: {e}")
                break