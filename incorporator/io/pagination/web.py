"""Web API paginators: LinkHeader, Cursor, Offset, PageNumber, and NextUrl."""

import logging
import re
from typing import Any, AsyncGenerator, Dict, Optional, Set, Union
from urllib.parse import urljoin

import httpx

from .base import AsyncPaginator
from ...exceptions import IncorporatorFormatError

logger = logging.getLogger(__name__)


class LinkHeaderPaginator(AsyncPaginator):
    """Example: GitHub API (Link header with rel="next")."""

    def __init__(self) -> None:
        super().__init__()
        self.current_url: Optional[str] = None
        self.is_first_call: bool = True

    def reset(self) -> None:
        """Resets state for daemon polling loops."""
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        if self.is_exhausted:
            return

        # Initialize start_url on the very first execution
        if self.is_first_call:
            self.current_url = start_url
            self.is_first_call = False

        calls = 0

        # The while loop allows it to work natively for non-streamed `incorp()` calls
        while self.current_url:
            # The stream() controller passes call_lim=1 to force O(1) memory breaks
            if self.call_lim and calls >= self.call_lim:
                break

            try:
                response = await self._fetch(self.current_url)
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

                self.current_url = urljoin(str(response.url), next_link) if next_link else None

                # Flag exhaustion so stream() knows the pipeline is finished
                if not self.current_url:
                    self.is_exhausted = True

            except (httpx.HTTPStatusError, httpx.RequestError, IncorporatorFormatError) as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"LinkHeaderPaginator stopped on {type(e).__name__}: {e}")
                break


class CursorPaginator(AsyncPaginator):
    def __init__(self, cursor_param: str = "cursor") -> None:
        super().__init__()
        self.cursor_param = cursor_param
        self.current_cursor: Optional[str] = None
        self.seen_cursors: Set[str] = set()

    def reset(self) -> None:
        self.is_exhausted = False
        self.current_cursor = None
        self.seen_cursors.clear()

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        if self.is_exhausted:
            return

        calls = 0
        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.cursor_param: self.current_cursor} if self.current_cursor else {}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)
                if isinstance(data, dict):
                    next_cursor = (
                        data.get("meta", {}).get("next_token") or data.get("next_cursor") or data.get(self.cursor_param)
                    )
                else:
                    next_cursor = None

                if not next_cursor or next_cursor in self.seen_cursors:
                    self.is_exhausted = True
                    break
                else:
                    self.seen_cursors.add(next_cursor)
                    self.current_cursor = next_cursor

            except (httpx.HTTPStatusError, httpx.RequestError, IncorporatorFormatError) as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"CursorPaginator stopped on {type(e).__name__}: {e}")
                break


class OffsetPaginator(AsyncPaginator):
    def __init__(
        self,
        limit: int = 50,
        offset_param: str = "offset",
        limit_param: str = "limit",
        result_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.limit = limit
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.result_key = result_key
        self.current_offset = 0

    def reset(self) -> None:
        self.is_exhausted = False
        self.current_offset = 0

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        if self.is_exhausted:
            return

        calls = 0
        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.offset_param: self.current_offset, self.limit_param: self.limit}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)

                if isinstance(data, dict):
                    if self.result_key:
                        items = data.get(self.result_key, [])
                    else:
                        # Auto-detect from common conventions
                        items = (
                            data.get("results")
                            or data.get("data")
                            or data.get("items")
                            or data.get("docs")
                            or data.get("records")
                            or []
                        )
                else:
                    items = data if isinstance(data, list) else []

                if not items:
                    self.is_exhausted = True
                    break
                else:
                    self.current_offset += self.limit

            except (httpx.HTTPStatusError, httpx.RequestError, IncorporatorFormatError) as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"OffsetPaginator stopped on {type(e).__name__}: {e}")
                break


class PageNumberPaginator(AsyncPaginator):
    def __init__(
        self,
        page_param: str = "page",
        start_page: int = 1,
        result_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.page_param = page_param
        self.start_page = start_page
        self.current_page = start_page
        self.result_key = result_key

    def reset(self) -> None:
        self.is_exhausted = False
        self.current_page = self.start_page

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        if self.is_exhausted:
            return

        calls = 0
        while True:
            if self.call_lim and calls >= self.call_lim:
                break

            params = {self.page_param: self.current_page}

            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)

                if isinstance(data, dict):
                    if self.result_key:
                        items = data.get(self.result_key, [])
                    else:
                        items = (
                            data.get("results")
                            or data.get("data")
                            or data.get("items")
                            or data.get("docs")
                            or data.get("records")
                            or []
                        )
                else:
                    items = data if isinstance(data, list) else []

                if not items:
                    self.is_exhausted = True
                    break
                else:
                    self.current_page += 1

            except (httpx.HTTPStatusError, httpx.RequestError, IncorporatorFormatError) as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"PageNumberPaginator stopped on {type(e).__name__}: {e}")
                break


class NextUrlPaginator(AsyncPaginator):
    def __init__(self, *path_keys: str) -> None:
        super().__init__()
        self.path_keys = path_keys if path_keys else ("next",)
        self.current_url: Optional[str] = None
        self.is_first_call = True

    def reset(self) -> None:
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        if self.is_exhausted:
            return

        if self.is_first_call:
            self.current_url = start_url
            self.is_first_call = False

        calls = 0
        while self.current_url:
            if self.call_lim and calls >= self.call_lim:
                break

            try:
                response = await self._fetch(self.current_url)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)
                for key in self.path_keys:
                    if isinstance(data, dict):
                        data = data.get(key)
                    else:
                        data = None
                        break

                self.current_url = urljoin(str(response.url), str(data)) if data else None
                if not self.current_url:
                    self.is_exhausted = True
                    break

            except (httpx.HTTPStatusError, httpx.RequestError, IncorporatorFormatError) as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"NextUrlPaginator stopped on {type(e).__name__}: {e}")
                break
