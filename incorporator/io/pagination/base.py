"""Base paginator class and shared utilities for the pagination engine."""

from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Union

import httpx

from ..formats import deserialize_nested, infer_format
from ..handlers import parse_source_data


def _deserialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {(str(k) if k is not None else "unknown_column"): deserialize_nested(v) for k, v in row.items()}


class AsyncPaginator:
    def __init__(self) -> None:
        self.call_lim: Optional[int] = None
        self.fetch_func: Optional[Callable[..., Awaitable[httpx.Response]]] = None
        self.strict_mode: bool = False
        self.is_exhausted: bool = False

    def reset(self) -> None:
        """Resets the paginator state for daemon polling loops."""
        self.is_exhausted = False

    async def _fetch(self, url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:
        if not self.fetch_func:
            raise RuntimeError("Paginator must be bound to a network client before use.")
        return await self.fetch_func(url=url, request_params=params, **kwargs)

    async def _parse_response(self, response: httpx.Response) -> Any:
        fmt = infer_format(str(response.url))
        return await parse_source_data(response.read(), fmt)

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes, List[Any], Dict[str, Any]], None]:
        if False:
            yield b""
        raise NotImplementedError
