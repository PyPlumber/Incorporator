"""Web API paginators: LinkHeader, Cursor, Offset, PageNumber, and NextUrl."""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncGenerator
from typing import Any, cast
from urllib.parse import urljoin

import httpx

from ...exceptions import IncorporatorFormatError
from ..penstock import Penstock
from .base import AsyncPaginator

logger = logging.getLogger(__name__)

# Common response keys holding the results array.  Tried in priority order
# when a paginator's ``result_key`` is unset.  Adding a new convention
# means editing this tuple in one place rather than every paginator.
_RESULT_KEY_CONVENTIONS = ("results", "data", "items", "docs", "records")


def _extract_results_array(data: Any, result_key: str | None) -> list[Any]:
    """Pull the results array out of a paginated response body.

    Honours an explicit ``result_key`` when set; otherwise walks
    ``_RESULT_KEY_CONVENTIONS`` until a non-empty list is found.  Lists
    pass through; anything else resolves to ``[]``.
    """
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    if result_key:
        explicit = data.get(result_key, []) or []
        return cast(list[Any], explicit)
    for key in _RESULT_KEY_CONVENTIONS:
        items = data.get(key)
        if items:
            return cast(list[Any], items)
    return []


class LinkHeaderPaginator(AsyncPaginator):
    """Walk paginated REST responses that ship the next-page URL in the HTTP
    ``Link`` header — the GitHub / GitLab style, RFC 5988.

    Reach for this when the source advertises the next page by emitting a
    ``Link: <...>; rel="next"`` header on every response and keeps the JSON
    body free of pagination metadata.  This is the canonical pattern for
    GitHub (``/repos/{org}/{repo}/issues``, ``/users/{u}/repos``), GitLab
    projects, and most RFC-5988-compliant REST APIs.

    Example::

        async for wave in Issue.stream(
            incorp_params={
                "inc_url": "https://api.github.com/repos/python/cpython/issues",
                "inc_code": "number",
                "inc_page": LinkHeaderPaginator(),
            },
            export_params={"file_path": "issues.ndjson", "if_exists": "append"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    State: ``current_url`` advances through the chain; ``is_first_call``
    seeds it from the ``start_url`` argument exactly once.  ``reset()``
    restores both for daemon-polling reuse in :meth:`Incorporator.stream`.
    Yields raw response bytes per page so the format handler parses them
    downstream and the paginator never materialises the full page list.
    """

    def __init__(self, *, penstock: Penstock | None = None) -> None:
        super().__init__(penstock=penstock)
        self.current_url: str | None = None
        self.is_first_call: bool = True

    def reset(self) -> None:
        """Reset state so the next ``paginate()`` starts from the first page."""
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes, None]:
        """Yield one raw response body per page, following RFC 5988 ``Link: rel="next"`` headers.

        Stops when the response has no ``rel="next"`` entry.  Honours
        ``call_lim`` so ``stream()`` can force exactly one page per wave.

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

            # Per-paginator throttle (A-F-9) — runs BEFORE _fetch so it
            # composes additively with any host-level penstock registered
            # via register_host_penstock (host acquire runs inside
            # execute_request).  Both must permit before a page fires.
            await self._acquire_penstock()
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
    """Walk paginated APIs that return a continuation-token / cursor in the
    response body — Twitter, Stripe, Google Cloud APIs, most modern REST.

    Reach for this when each response carries the next page's identifier
    inside its JSON body (rather than in a ``Link`` header or as an offset),
    and the client is expected to echo that identifier back as a query
    parameter on the following request.  Canonical fits: Stripe customers
    / charges, Twitter timelines, Google Cloud list endpoints with
    ``page_token``.

    Example::

        async for wave in Customer.stream(
            incorp_params={
                "inc_url": "https://api.stripe.com/v1/customers",
                "inc_code": "id",
                "inc_page": CursorPaginator(cursor_param="starting_after"),
            },
            export_params={"file_path": "customers.ndjson", "if_exists": "append"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    The next-cursor field is auto-detected from the conventional response
    keys ``meta.next_token``, ``next_cursor``, and whatever ``cursor_param``
    is set to — so most endpoints work without further tuning.  Set
    ``cursor_param`` for the query-string name the API expects
    (``"page_token"`` for Google APIs, ``"starting_after"`` for Stripe,
    ``"next_token"`` for AWS).  Infinite-loop defence: every cursor seen is
    recorded in ``seen_cursors``; if the API ever returns one it has
    already issued, the paginator treats it as exhaustion and stops — a
    real production hazard when upstream cursors are non-monotonic.

    Args:
        cursor_param: Query-parameter name to send the cursor on
            (default ``"cursor"``). Set to ``"page_token"`` for Google APIs.
    """

    def __init__(self, cursor_param: str = "cursor", *, penstock: Penstock | None = None) -> None:
        super().__init__(penstock=penstock)
        self.cursor_param = cursor_param
        self.current_cursor: str | None = None
        self.seen_cursors: set[str] = set()

    def reset(self) -> None:
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_cursor = None
        self.seen_cursors.clear()

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes, None]:
        """Yield one raw response body per page, advancing via the cursor token.

        Reads the next cursor from ``meta.next_token``, ``next_cursor``, or
        the configured ``cursor_param`` key.  Stops when the cursor is absent
        or has already been seen (infinite-loop guard).  Honours ``call_lim``
        so ``stream()`` can force exactly one page per wave.

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

            # Per-paginator throttle (A-F-9) — runs BEFORE _fetch so it
            # composes additively with any host-level penstock.
            await self._acquire_penstock()
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
    """Walk classic SQL-backed REST endpoints with ``?offset=N&limit=M``
    semantics — the default style for DRF, FastAPI, and Flask-SQLAlchemy.

    Reach for this against any backend whose pagination is a thin wrapper
    over a SQL ``LIMIT ... OFFSET ...`` clause and whose responses look like
    ``{"data": [...], "total": 10000}``.  Canonical fits: enterprise
    back-office APIs, internal microservices, search endpoints that don't
    bother with cursors.

    Example::

        async for wave in Order.stream(
            incorp_params={
                "inc_url": "https://backoffice.example.com/api/orders",
                "inc_code": "order_id",
                "inc_page": OffsetPaginator(limit=500),
            },
            export_params={"file_path": "orders.ndjson", "if_exists": "append"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    ``limit`` controls per-chunk memory footprint — raise it to amortise
    HTTP round-trip overhead on a fat pipe, drop it when the source is
    rate-limited or each row is heavy.  Stops when the results list is
    empty, auto-detecting it from ``result_key`` if provided or falling
    back through the conventional keys ``results``, ``data``, ``items``,
    ``docs``, ``records`` in that order.

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
        result_key: str | None = None,
        *,
        penstock: Penstock | None = None,
    ) -> None:
        super().__init__(penstock=penstock)
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

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes, None]:
        """Yield one raw response body per page, advancing the offset by ``limit``.

        Stops when the results list is empty (auto-detected from common
        conventions: ``results``, ``data``, ``items``, ``docs``, ``records``
        or ``result_key`` if set).  Honours ``call_lim`` so ``stream()``
        can force exactly one page per wave.

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

            # Per-paginator throttle (A-F-9) — runs BEFORE _fetch so it
            # composes additively with any host-level penstock.
            await self._acquire_penstock()
            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)
                items = _extract_results_array(data, self.result_key)

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
    """Walk APIs paginated by a plain ``?page=N`` / ``?page_number=N`` query
    parameter — WordPress, Drupal, most CMS REST APIs, CoinGecko
    ``/coins/markets``.

    Reach for this when the source has no cursors, no offsets, and no
    next-URL — just a sequential integer page number that the client
    increments until results dry up.  This is the canonical T8
    bulk-drain pattern: paginated CoinGecko market data, WordPress posts,
    most public-data REST endpoints.

    Example::

        async for wave in Coin.stream(
            incorp_params={
                "inc_url": "https://api.coingecko.com/api/v3/coins/markets",
                "inc_code": "id",
                "request_params": {"vs_currency": "usd", "per_page": 250},
                "inc_page": PageNumberPaginator(page_param="page"),
            },
            export_params={"file_path": "coins.ndjson", "if_exists": "append"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    Set ``start_page=0`` for APIs that index from zero (a minority but
    a real one — some Elasticsearch wrappers, a few enterprise CMSs).
    Stops when the results list is empty, using the same auto-detection
    as :class:`OffsetPaginator` (``results`` / ``data`` / ``items`` /
    ``docs`` / ``records``, or ``result_key`` when set explicitly).

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
        result_key: str | None = None,
        *,
        penstock: Penstock | None = None,
    ) -> None:
        super().__init__(penstock=penstock)
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

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes, None]:
        """Yield one raw response body per page, incrementing the page number.

        Stops when the results list is empty (same auto-detection as
        :class:`OffsetPaginator`).  Honours ``call_lim`` so ``stream()``
        can force exactly one page per wave.

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

            # Per-paginator throttle (A-F-9) — runs BEFORE _fetch so it
            # composes additively with any host-level penstock.
            await self._acquire_penstock()
            try:
                response = await self._fetch(start_url, params=params)
                yield response.read()
                calls += 1

                data = await self._parse_response(response)
                items = _extract_results_array(data, self.result_key)

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
    """Walk APIs that embed the full next-page URL inside the JSON body —
    Django REST Framework's default style, SWAPI, SpaceDevs Launch
    Library, JSON:API.

    Reach for this when each response carries a ready-to-fetch URL like
    ``{"next": "https://.../page=2", "results": [...]}`` and the client's
    job is simply to follow it.  Canonical fits: SWAPI starships, the
    SpaceDevs Launch Library, any DRF-style enterprise endpoint that
    inherits ``PageNumberPagination`` or ``LimitOffsetPagination`` without
    customisation, JSON:API responses with ``links.next``.

    Example::

        async for wave in Starship.stream(
            incorp_params={
                "inc_url": "https://swapi.dev/api/starships/",
                "inc_code": "url",
                "inc_page": NextUrlPaginator(),  # auto-detects `next` key
            },
            export_params={"file_path": "starships.ndjson", "if_exists": "append"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    The default ``NextUrlPaginator()`` looks for a top-level ``"next"`` key
    (the DRF / SWAPI convention).  Pass one or more positional ``path_keys``
    to drill into nested envelopes — ``NextUrlPaginator("meta",
    "pagination", "next")`` for JSON:API-style ``{"meta": {"pagination":
    {"next": "..."}}}``.  Relative URLs are resolved against the response
    URL so APIs that emit bare paths still work.

    Args:
        *path_keys: One or more keys to drill through to find the next URL.
            Defaults to ``("next",)`` if none provided.
    """

    def __init__(self, *path_keys: str, penstock: Penstock | None = None) -> None:
        super().__init__(penstock=penstock)
        self.path_keys = path_keys if path_keys else ("next",)
        self.current_url: str | None = None
        self.is_first_call = True

    def reset(self) -> None:
        """Clear paginator state so the next ``paginate()`` starts from page 1.

        Called automatically by ``stream()`` between poll cycles so the
        daemon re-fetches from the source on every wake-up.
        """
        self.is_exhausted = False
        self.current_url = None
        self.is_first_call = True

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes, None]:
        """Yield one raw response body per page, following the next-URL embedded in the JSON.

        Drills into the response body via ``path_keys`` to extract the URL of
        the next page, then re-fetches it.  Stops when the value is absent or
        falsy.  Honours ``call_lim`` so ``stream()`` can force exactly one
        page per wave.

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

            # Per-paginator throttle (A-F-9) — runs BEFORE _fetch so it
            # composes additively with any host-level penstock registered
            # via register_host_penstock (host acquire runs inside
            # execute_request).  Both must permit before a page fires.
            await self._acquire_penstock()
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
