"""Base paginator class and shared utilities for the pagination engine."""

from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Union

import httpx

from ..formats import deserialize_nested, infer_format
from ..handlers import parse_source_data


def _deserialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {(str(k) if k is not None else "unknown_column"): deserialize_nested(v) for k, v in row.items()}


class AsyncPaginator:
    """Base class for every paginator the framework ships.

    Pass any concrete subclass (``NextUrlPaginator``, ``CursorPaginator``,
    ``OffsetPaginator``, ``PageNumberPaginator``, ``LinkHeaderPaginator``,
    ``SQLitePaginator``, ``CSVPaginator``, ``AvroPaginator``) to
    :meth:`Incorporator.incorp` as ``inc_page=`` and the engine drives the
    pagination loop for you::

        from incorporator import Incorporator
        from incorporator.io.pagination import NextUrlPaginator

        launches = await Launch.incorp(
            inc_url="https://api.example.com/launches/",
            inc_page=NextUrlPaginator("next"),
            call_lim=5,          # cap at 5 pages while exploring
        )

    Implement a subclass when you need a strategy the bundled paginators
    don't cover — override :meth:`paginate` and ``yield`` parsed page
    payloads.  :meth:`reset` is called between daemon polls so any
    persistent state (cursor, offset, file handle) can be cleared.
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
