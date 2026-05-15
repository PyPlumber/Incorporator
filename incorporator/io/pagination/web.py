"""Web API paginators: LinkHeader, Cursor, Offset, PageNumber, and NextUrl."""

import logging
import re
from typing import AsyncGenerator, Optional, Set, Union
from urllib.parse import urljoin

import httpx

from ...exceptions import IncorporatorFormatError
from .base import AsyncPaginator

logger = logging.getLogger(__name__)


class LinkHeaderPaginator(AsyncPaginator):
    """Paginator for RFC 5988 ``Link`` header–based APIs (GitHub, GitLab, etc.).

    Reads the response's ``Link`` header on every page and follows the URL
    in the entry tagged ``rel="next"``. Stops when no such entry is present.

    State: ``current_url`` advances through the chain; ``is_first_call``
    seeds it from the ``start_url`` argument exactly once.  ``reset()``
    restores both for daemon-polling reuse in :meth:`Incorporator.stream`.

    Memory: yields raw response bytes per page — the format handler
    parses them downstream so the paginator never materialises the full
    page list.
    """

    def __init__(self) -> None:
        super().__init__()
        self.current_url: Optional[str] = None
        self.is_first_call: bool = True

    def reset(self) -> None:
        """Reset state so the next ``paginate()`` starts from the first page."""
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yield one raw response body per page, following RFC 5988 ``Link: rel="next"`` headers.

        Stops when the response has no ``rel="next"`` entry.  Honours
        ``call_lim`` so ``stream()`` can force exactly one page per tick.

        Args:
            start_url: The URL of the first page.  Subsequent URLs are
                extracted from the ``Link`` header automatically.

        Yields:
            ``bytes`` — the raw HTTP response body for each page; the
            format handler downstream parses them into records.
        """
        if self.is_exhausted:
            return

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
                logger.warning("LinkHeaderPaginator stopped on %s: %s", type(e).__name__, e)
                break


class CursorPaginator(AsyncPaginator):
    """Paginator for cursor / continuation-token APIs (Twitter, Stripe, etc.).

    Reads the next cursor from one of the conventional response keys
    (``meta.next_token``, ``next_cursor``, or the configured ``cursor_param``)
    and forwards it as a query parameter on the next request.

    Infinite-loop defence: every cursor seen is recorded in ``seen_cursors``;
    if the API ever returns a cursor it has already issued, the paginator
    treats it as exhaustion and stops.  This is a real production hazard
    when upstream cursors are non-monotonic or buggy.

    State: ``current_cursor`` carries the in-flight token; ``seen_cursors``
    is a ``Set[str]`` of all tokens observed. ``reset()`` clears both for
    daemon-polling reuse.

    Args:
        cursor_param: Query-parameter name to send the cursor on
            (default ``"cursor"``). Set to ``"page_token"`` for Google APIs.
    """

    def __init__(self, cursor_param: str = "cursor") -> None:
        super().__init__()
        self.cursor_param = cursor_param
        self.current_cursor: Optional[str] = None
        self.seen_cursors: Set[str] = set()

    def reset(self) -> None:
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_cursor = None
        self.seen_cursors.clear()

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yield one raw response body per page, advancing via the cursor token.

        Reads the next cursor from ``meta.next_token``, ``next_cursor``, or
        the configured ``cursor_param`` key.  Stops when the cursor is absent
        or has already been seen (infinite-loop guard).  Honours ``call_lim``
        so ``stream()`` can force exactly one page per tick.

        Args:
            start_url: Base URL that receives the cursor as a query parameter
                on every request.

        Yields:
            ``bytes`` — the raw HTTP response body for each page; the
            format handler downstream parses them into records.
        """
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
                logger.warning("CursorPaginator stopped on %s: %s", type(e).__name__, e)
                break


class OffsetPaginator(AsyncPaginator):
    """Paginator for offset/limit APIs (most SQL-backed REST endpoints).

    Sends ``?offset=N&limit=M`` on every request and advances ``offset`` by
    ``limit`` between pages. Stops when the response's results list is empty.

    Results list is detected from ``result_key`` if provided; otherwise it
    falls back through the conventional keys ``results``, ``data``,
    ``items``, ``docs``, ``records`` in that order.

    Args:
        limit: Page size to request (default 50). Sent as ``?limit=...``.
        offset_param: Query-parameter name for the offset (default ``"offset"``).
        limit_param: Query-parameter name for the page size (default ``"limit"``).
        result_key: Explicit response-payload key holding the results array.
            ``None`` triggers auto-detection across common conventions.
    """

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
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_offset = 0

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yield one raw response body per page, advancing the offset by ``limit``.

        Stops when the results list is empty (auto-detected from common
        conventions: ``results``, ``data``, ``items``, ``docs``, ``records``
        or ``result_key`` if set).  Honours ``call_lim`` so ``stream()``
        can force exactly one page per tick.

        Args:
            start_url: Base URL that receives ``offset`` and ``limit`` as
                query parameters on every request.

        Yields:
            ``bytes`` — the raw HTTP response body for each page; the
            format handler downstream parses them into records.
        """
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
                logger.warning("OffsetPaginator stopped on %s: %s", type(e).__name__, e)
                break


class PageNumberPaginator(AsyncPaginator):
    """Paginator for ``?page=N`` APIs (WordPress, generic CMS, many REST APIs).

    Sends a single page-number query parameter and increments it after each
    response. Stops when the response's results list is empty (auto-detected
    the same way as :class:`OffsetPaginator`).

    Args:
        page_param: Query-parameter name for the page number (default ``"page"``).
        start_page: First page number to request (default ``1``; some APIs
            start at ``0``).
        result_key: Explicit response-payload key holding the results array.
            ``None`` triggers auto-detection.
    """

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
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_page = self.start_page

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yield one raw response body per page, incrementing the page number.

        Stops when the results list is empty (same auto-detection as
        :class:`OffsetPaginator`).  Honours ``call_lim`` so ``stream()``
        can force exactly one page per tick.

        Args:
            start_url: Base URL that receives the page number as a query
                parameter on every request.

        Yields:
            ``bytes`` — the raw HTTP response body for each page; the
            format handler downstream parses them into records.
        """
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
                logger.warning("PageNumberPaginator stopped on %s: %s", type(e).__name__, e)
                break


class NextUrlPaginator(AsyncPaginator):
    """Paginator for "next URL inside the JSON body" APIs (SpaceDevs, SWAPI, DRF, etc.).

    Drills into the response JSON using one or more dot-notation keys to
    find the absolute or relative URL of the next page, then re-fetches it
    until the value is ``None`` or absent.

    Example:
        Response body ``{"results": [...], "next": "https://api/page/2"}``::

            inc_page=NextUrlPaginator("next")        # single top-level key

        Nested ``{"meta": {"pagination": {"next": "..."}}}``::

            inc_page=NextUrlPaginator("meta", "pagination", "next")

    Args:
        *path_keys: One or more keys to drill through to find the next URL.
            Defaults to ``("next",)`` if none provided.
    """

    def __init__(self, *path_keys: str) -> None:
        super().__init__()
        self.path_keys = path_keys if path_keys else ("next",)
        self.current_url: Optional[str] = None
        self.is_first_call = True

    def reset(self) -> None:
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes], None]:
        """Yield one raw response body per page, following the next-URL embedded in the JSON.

        Drills into the response body via ``path_keys`` to extract the URL of
        the next page, then re-fetches it.  Stops when the value is absent or
        falsy.  Honours ``call_lim`` so ``stream()`` can force exactly one
        page per tick.

        Args:
            start_url: URL of the first page; subsequent URLs are extracted
                from the JSON body via ``path_keys``.

        Yields:
            ``bytes`` — the raw HTTP response body for each page; the
            format handler downstream parses them into records.
        """
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
                logger.warning("NextUrlPaginator stopped on %s: %s", type(e).__name__, e)
                break
