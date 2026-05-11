"""
Advanced Stateful Pagination Engine for the Incorporator Framework.

Unifies REST API streaming and Local File/Database chunking under a single
O(1) Memory state tracker, perfectly integrated with the pipeline orchestrator.
"""

import asyncio
import csv
import itertools
import logging
import re
import sqlite3
from typing import IO, Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Set, Union
from urllib.parse import urljoin

import httpx

from .format_parsers import parse_source_data
from .format_utils import deserialize_nested, infer_format

logger = logging.getLogger(__name__)


def _deserialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {(str(k) if k is not None else "unknown_column"): deserialize_nested(v) for k, v in row.items()}


# ==========================================
# 1. BASE CLASS
# ==========================================
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


# ==========================================
# 2. LOCAL FILE & DATABASE PAGINATORS
# ==========================================
class SQLitePaginator(AsyncPaginator):
    """Yields O(1) chunks natively using SQL bounds."""

    def __init__(self, db_path: str, sql_query: str, chunk_size: int = 10000) -> None:
        super().__init__()
        self.db_path = db_path
        self.sql_query = sql_query
        self.chunk_size = chunk_size
        self._conn: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

    def reset(self) -> None:
        self.is_exhausted = False
        if self._conn is not None:
            self._conn.close()
        self._conn = None
        self._cursor = None

    def __del__(self) -> None:
        conn: Any = getattr(self, "_conn", None)
        if conn is not None:
            conn.close()

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes, List[Any], Dict[str, Any]], None]:
        if self.is_exhausted:
            return

        def _fetch_chunk() -> List[Dict[str, Any]]:
            if not self._conn:
                self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
                self._cursor = self._conn.execute(self.sql_query)

            if not self._cursor:
                return []

            rows = self._cursor.fetchmany(self.chunk_size)

            chunk: List[Dict[str, Any]] = [_deserialize_row(dict(row)) for row in rows]

            return chunk

        calls = 0
        while not self.is_exhausted:
            if self.call_lim and calls >= self.call_lim:
                break

            chunk_data = await asyncio.to_thread(_fetch_chunk)
            if not chunk_data:
                self.is_exhausted = True
                if self._conn:
                    self._conn.close()
                    self._conn = None
                break

            yield chunk_data
            calls += 1


class CSVPaginator(AsyncPaginator):
    """Yields O(1) chunks natively using a persistent csv.DictReader."""

    def __init__(self, file_path: str, chunk_size: int = 10000, delimiter: str = ",") -> None:
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.delimiter = delimiter
        self._file: Optional[IO[Any]] = None
        self._reader: Optional[Any] = None

    def reset(self) -> None:
        self.is_exhausted = False
        if self._file is not None:
            self._file.close()
        self._file = None
        self._reader = None

    def __del__(self) -> None:
        file_obj: Any = getattr(self, "_file", None)
        if file_obj is not None:
            file_obj.close()

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes, List[Any], Dict[str, Any]], None]:
        if self.is_exhausted:
            return

        def _fetch_chunk() -> List[Dict[str, Any]]:
            if not self._file:
                self._file = open(self.file_path, "rt", encoding="utf-8")
                self._reader = csv.DictReader(self._file, delimiter=self.delimiter)

            if self._reader is None:
                return []

            chunk: List[Dict[str, Any]] = [
                _deserialize_row(dict(row)) for row in itertools.islice(self._reader, self.chunk_size)
            ]
            return chunk

        calls = 0
        while not self.is_exhausted:
            if self.call_lim and calls >= self.call_lim:
                break

            chunk_data = await asyncio.to_thread(_fetch_chunk)
            if not chunk_data:
                self.is_exhausted = True
                if self._file:
                    self._file.close()
                    self._file = None
                break

            yield chunk_data
            calls += 1


class AvroPaginator(AsyncPaginator):
    """Yields O(1) chunks maintaining a persistent block reader."""

    def __init__(self, file_path: str, chunk_size: int = 10000) -> None:
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self._file: Optional[IO[Any]] = None
        self._reader: Optional[Any] = None

    def reset(self) -> None:
        self.is_exhausted = False
        if self._file is not None:
            self._file.close()
        self._file = None
        self._reader = None

    def __del__(self) -> None:
        file_obj: Any = getattr(self, "_file", None)
        if file_obj is not None:
            file_obj.close()

    async def paginate(self, start_url: str) -> AsyncGenerator[Union[str, bytes, List[Any], Dict[str, Any]], None]:
        if self.is_exhausted:
            return

        def _fetch_chunk() -> List[Dict[str, Any]]:
            try:
                import fastavro
            except ImportError:
                raise RuntimeError("fastavro not installed.") from None

            if not self._file:
                self._file = open(self.file_path, "rb")
                self._reader = fastavro.reader(self._file)

            if self._reader is None:
                return []

            chunk: List[Dict[str, Any]] = [
                _deserialize_row(raw_row)
                for raw_row in itertools.islice(self._reader, self.chunk_size)
                if isinstance(raw_row, dict)
            ]
            return chunk

        calls = 0
        while not self.is_exhausted:
            if self.call_lim and calls >= self.call_lim:
                break

            chunk_data = await asyncio.to_thread(_fetch_chunk)
            if not chunk_data:
                self.is_exhausted = True
                if self._file:
                    self._file.close()
                    self._file = None
                break

            yield chunk_data
            calls += 1


# ==========================================
# 3. WEB API PAGINATORS
# ==========================================
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

            except Exception as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise  # Let the DX Analyzer catch it!
                logger.warning(f"LinkHeaderPaginator stopped gracefully: {e}")
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

            except Exception as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"CursorPaginator stopped: {e}")
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

            except Exception as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"OffsetPaginator stopped: {e}")
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

            except Exception as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"PageNumberPaginator stopped: {e}")
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

            except Exception as e:
                self.is_exhausted = True
                if self.strict_mode:
                    raise
                logger.warning(f"NextUrlPaginator stopped: {e}")
                break
