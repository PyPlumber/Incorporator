"""Base paginator class and shared utilities for the pagination engine."""

from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Union

import httpx

from ..formats import deserialize_nested, infer_format
from ..handlers import parse_source_data


def _deserialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {(str(k) if k is not None else "unknown_column"): deserialize_nested(v) for k, v in row.items()}


class AsyncPaginator:
    """Escape-hatch base for vendors whose pagination contract doesn't match
    any of the bundled paginators.

    Reach for a subclass only when the bundled paginators (LinkHeader,
    Cursor, Offset, PageNumber, NextUrl for web; SQLite, CSV, Avro for
    local files) can't model the source's shape — that's rare in practice
    because the bundled five cover Link-header, continuation-token, classic
    offset/limit, ``?page=N``, and DRF-style next-URL-in-body, which is
    the long tail of public REST APIs.  When you do subclass, override
    :meth:`paginate` and ``yield`` raw bytes / pre-parsed rows; override
    :meth:`reset` if your subclass holds persistent state (a cursor, file
    handle, sequence token) that needs clearing between daemon polls.

    Example::

        class MyVendorPaginator(AsyncPaginator):
            async def paginate(self, start_url):
                token = None
                while not self.is_exhausted:
                    response = await self._fetch(start_url, params={"t": token})
                    yield response.read()
                    token = response.json().get("continue")
                    if not token:
                        self.is_exhausted = True

        await Order.incorp(
            inc_url="https://vendor.example.com/orders",
            inc_page=MyVendorPaginator(),
        )

    The engine drives the loop, binds ``self.fetch_func`` to the network
    client, honours ``self.call_lim`` for O(1)-memory streaming, and
    propagates ``self.strict_mode`` so subclasses can re-raise instead of
    swallowing transport errors.
    """

    def __init__(self) -> None:
        self.call_lim: Optional[int] = None
        self.fetch_func: Optional[Callable[..., Awaitable[httpx.Response]]] = None
        self.strict_mode: bool = False
        self.is_exhausted: bool = False

    def reset(self) -> None:
        """Reset paginator state to allow another full pagination pass.

        Called automatically by :meth:`Incorporator.stream` between polls
        in chunking mode so the next cycle starts from page 1.  Subclasses
        with persistent state (cursors, offsets, file handles) should
        override this and clear that state before calling ``super().reset()``.
        """
        self.is_exhausted = False

    async def _fetch(self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        if not self.fetch_func:
            raise RuntimeError("Paginator must be bound to a network client before use.")
        return await self.fetch_func(url=url, request_params=params, **kwargs)

    async def _parse_response(self, response: httpx.Response) -> Any:
        fmt = infer_format(str(response.url))
        return await parse_source_data(response.read(), fmt)

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes, List[Any], Dict[str, Any]], None]:
        """Async-generate one chunk per yield until the source is exhausted.

        Subclasses implement source-specific traversal — Link headers,
        cursors, offset windows, file pointers, etc. The contract:

        * Yield raw bytes / strings / pre-parsed rows per page or chunk.
        * Respect ``self.call_lim`` if set (used by ``stream()`` to force
          O(1) memory by yielding exactly one chunk per wave).
        * Set ``self.is_exhausted = True`` when no more data is available
          so the orchestrator can flip to its idle / poll state.
        """
        if False:
            yield b""
        raise NotImplementedError
