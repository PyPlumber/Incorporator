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
    """Stream a SQLite query in O(1)-memory chunks via a persistent cursor.

    Opens one connection on first ``paginate()`` call, executes ``sql_query``
    once, then yields ``chunk_size`` rows per iteration via
    ``cursor.fetchmany()`` — the C driver streams rows lazily so peak memory
    stays bounded by ``chunk_size`` regardless of the total row count.

    Cleanup: connection is closed on exhaustion, ``reset()``, or ``__del__``.
    The connection uses ``check_same_thread=False`` so it survives the
    ``asyncio.to_thread`` round-trip.

    Args:
        db_path: Filesystem path to the SQLite database.
        sql_query: SQL ``SELECT`` to stream (typically ``"SELECT * FROM t"``).
        chunk_size: Rows per chunk (default 10 000).
    """

    def __init__(self, db_path: str, sql_query: str, chunk_size: int = 10000) -> None:
        super().__init__()
        self.db_path = db_path
        self.sql_query = sql_query
        self.chunk_size = chunk_size
        self._conn: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

    def reset(self) -> None:
        """Close any open file/cursor and clear state for daemon polling reuse."""
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
        """Yield ``chunk_size`` rows per iteration from the local source.

        ``start_url`` is unused — local paginators carry their own
        ``db_path`` / ``file_path`` state. Honours ``call_lim`` so
        ``stream()`` can force exactly one chunk per tick.

        Args:
            start_url: Unused; present for interface compatibility with web
                paginators.

        Yields:
            ``List[Dict[str, Any]]`` — one chunk of up to ``chunk_size``
            rows per iteration, consumed directly by the instantiation engine.
        """
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
    """Stream a CSV/TSV/PSV file in O(1)-memory chunks via ``csv.DictReader``.

    Opens the file once on first ``paginate()`` call, then yields chunks of
    ``chunk_size`` rows via ``itertools.islice``. Peak memory stays bounded
    by one chunk plus the DictReader's line buffer.

    Cleanup: file handle is closed on exhaustion, ``reset()``, or ``__del__``.
    All I/O runs inside ``asyncio.to_thread`` so the event loop never blocks
    on disk reads.

    Args:
        file_path: Filesystem path to the CSV file (must be UTF-8).
        chunk_size: Rows per chunk (default 10 000).
        delimiter: Field separator — ``","`` (default), ``"\\t"`` for TSV,
            ``"|"`` for PSV, etc.
    """

    def __init__(self, file_path: str, chunk_size: int = 10000, delimiter: str = ",") -> None:
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.delimiter = delimiter
        self._file: Optional[IO[Any]] = None
        self._reader: Optional[Any] = None

    def reset(self) -> None:
        """Close any open file/cursor and clear state for daemon polling reuse."""
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
        """Yield ``chunk_size`` rows per iteration from the local source.

        ``start_url`` is unused — local paginators carry their own
        ``db_path`` / ``file_path`` state. Honours ``call_lim`` so
        ``stream()`` can force exactly one chunk per tick.

        Args:
            start_url: Unused; present for interface compatibility with web
                paginators.

        Yields:
            ``List[Dict[str, Any]]`` — one chunk of up to ``chunk_size``
            rows per iteration, consumed directly by the instantiation engine.
        """
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
    """Stream an Apache Avro file in O(1)-memory chunks via ``fastavro.reader``.

    Opens the binary file once on first ``paginate()`` call and yields
    ``chunk_size`` decoded records per iteration. ``fastavro`` reads one
    Avro block at a time, so peak memory is bounded by block size +
    chunk size regardless of total file size.

    Requires the optional ``fastavro`` extra (``pip install incorporator[avro]``).
    A clear :class:`RuntimeError` is raised if it is missing.

    Cleanup: file handle is closed on exhaustion, ``reset()``, or ``__del__``.

    Args:
        file_path: Filesystem path to the Avro file.
        chunk_size: Records per chunk (default 10 000).
    """

    def __init__(self, file_path: str, chunk_size: int = 10000) -> None:
        super().__init__()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self._file: Optional[IO[Any]] = None
        self._reader: Optional[Any] = None

    def reset(self) -> None:
        """Close any open file/cursor and clear state for daemon polling reuse."""
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
        """Yield ``chunk_size`` rows per iteration from the local source.

        ``start_url`` is unused — local paginators carry their own
        ``db_path`` / ``file_path`` state. Honours ``call_lim`` so
        ``stream()`` can force exactly one chunk per tick.

        Args:
            start_url: Unused; present for interface compatibility with web
                paginators.

        Yields:
            ``List[Dict[str, Any]]`` — one chunk of up to ``chunk_size``
            rows per iteration, consumed directly by the instantiation engine.
        """
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
