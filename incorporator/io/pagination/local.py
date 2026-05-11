"""Local file and database paginators: SQLite, CSV, and Avro."""

import asyncio
import csv
import itertools
import logging
import sqlite3
from typing import IO, Any, AsyncGenerator, Dict, List, Optional, Union

from .base import AsyncPaginator, _deserialize_row

logger = logging.getLogger(__name__)


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
