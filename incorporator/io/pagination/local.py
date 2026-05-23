"""Local file and database paginators: SQLite, CSV, and Avro."""

from __future__ import annotations

import asyncio
import csv
import itertools
import logging
import sqlite3
from collections.abc import AsyncGenerator
from typing import IO, Any, ClassVar

from ..penstock import Penstock
from .base import AsyncPaginator, _deserialize_row

logger = logging.getLogger(__name__)


class _LocalChunkedPaginator(AsyncPaginator):
    """Template-method base for paginators that stream a local file/database.

    Owns the bits that were copy-pasted across :class:`SQLitePaginator`,
    :class:`CSVPaginator`, and :class:`AvroPaginator` before this base existed:
    the call-limit-aware yield loop, the on-exhaustion handle close, and the
    ``reset()`` / ``__del__()`` cleanup.  Concrete subclasses define only:

    - :attr:`_closeable_attrs` — names of instance attributes that hold a
      *primary* handle (the thing whose ``.close()`` must be called).
    - :attr:`_companion_attrs` — names of *secondary* attributes that
      piggyback on the primary handle (cursors, readers) and should be
      reset to ``None`` alongside the primary close.  These don't need
      their own ``.close()`` call — when the primary handle closes, the
      cursor/reader is released by the underlying driver.
    - :meth:`_fetch_chunk` — sync method returning one chunk of rows.  Lazy
      handle init lives here (``if not self._conn: open()``) so the first
      ``paginate()`` iteration pays the open cost exactly once.

    Subclasses must NOT override :meth:`reset`, :meth:`__del__`, or
    :meth:`paginate` — the whole point of this base is that the lifecycle
    is owned in one place.
    """

    _closeable_attrs: ClassVar[tuple[str, ...]] = ()
    _companion_attrs: ClassVar[tuple[str, ...]] = ()

    def reset(self) -> None:
        """Close any open handles and clear state for daemon-polling reuse."""
        self.is_exhausted = False
        for attr in self._closeable_attrs:
            handle = getattr(self, attr, None)
            if handle is not None:
                handle.close()
            setattr(self, attr, None)
        for attr in self._companion_attrs:
            setattr(self, attr, None)

    def __del__(self) -> None:
        # ``__del__`` must never raise — wrap every close in a swallow.
        for attr in self._closeable_attrs:
            handle: Any = getattr(self, attr, None)
            if handle is not None:
                try:
                    handle.close()
                except Exception:  # noqa: BLE001, S110 — finalisation, nothing to do
                    pass

    def _fetch_chunk(self) -> list[dict[str, Any]]:
        """Return the next ``chunk_size`` rows (sync).  Override in subclasses.

        Runs inside ``asyncio.to_thread`` so disk I/O never blocks the event
        loop.  The lazy-init pattern (``if not self._conn: open()``) belongs
        here, not in :meth:`paginate` — opens cost one syscall per pipeline,
        not one per chunk.
        """
        raise NotImplementedError

    async def paginate(self, start_url: str) -> AsyncGenerator[str | bytes | list[Any] | dict[str, Any], None]:
        """Yield ``chunk_size`` rows per iteration from the local source.

        ``start_url`` is unused — local paginators carry their own
        ``db_path`` / ``file_path`` state.  Honours ``call_lim`` so
        ``stream()`` can force exactly one chunk per wave.

        On exhaustion the primary handle is closed and cleared so the
        file descriptor isn't held while the daemon idles.

        Args:
            start_url: Unused; present for interface compatibility with web
                paginators.

        Yields:
            ``List[Dict[str, Any]]`` — one chunk of up to ``chunk_size``
            rows per iteration, consumed directly by the instantiation engine.
        """
        if self.is_exhausted:
            return

        calls = 0
        while not self.is_exhausted:
            if self.call_lim and calls >= self.call_lim:
                break

            chunk_data = await asyncio.to_thread(self._fetch_chunk)
            if not chunk_data:
                self.is_exhausted = True
                for attr in self._closeable_attrs:
                    handle = getattr(self, attr, None)
                    if handle is not None:
                        handle.close()
                        setattr(self, attr, None)
                break

            # Per-paginator throttle (A-F-9).  Local paginators don't go
            # through ``execute_request``, so the host-level penstock
            # registered via ``register_host_penstock`` cannot reach them
            # — this is the only throttle path.  Cheap no-op when the
            # default :class:`NullPenstock` is in place.
            await self._acquire_penstock()
            yield chunk_data
            calls += 1


class SQLitePaginator(_LocalChunkedPaginator):
    """Drain a SQLite database table or query in O(1)-memory chunks without
    slurping every row into RAM.

    Reach for this when the source is a SQLite file on disk — a
    data-warehouse dump, a logging archive, a local analytics replica —
    and the row count exceeds what comfortably fits in memory.  Canonical
    fit: process a 50M-row warehouse dump on a laptop, transform with
    ``conv_dict``, re-export to NDJSON / Parquet / a different SQLite,
    all without OOM.

    Example::

        async for wave in Trade.stream(
            incorp_params={
                "inc_file": "warehouse.sqlite",
                "sql_query": "SELECT * FROM trades WHERE date >= '2024-01-01'",
                "inc_code": "trade_id",
                "inc_page": SQLitePaginator(
                    db_path="warehouse.sqlite",
                    sql_query="SELECT * FROM trades WHERE date >= '2024-01-01'",
                    chunk_size=10_000,
                ),
            },
            export_params={"file_path": "trades_2024.parquet"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    Opens one connection on first ``paginate()`` call, executes
    ``sql_query`` once, then yields ``chunk_size`` rows per iteration via
    ``cursor.fetchmany()`` — the C driver streams lazily so peak memory
    stays bounded by ``chunk_size`` regardless of total row count.  The
    connection uses ``check_same_thread=False`` so it survives the
    ``asyncio.to_thread`` round-trip; cleanup (close on exhaustion,
    ``reset()``, or ``__del__``) is inherited from
    :class:`_LocalChunkedPaginator`.

    Args:
        db_path: Filesystem path to the SQLite database.
        sql_query: SQL ``SELECT`` to stream (typically ``"SELECT * FROM t"``).
        chunk_size: Rows per chunk (default 10 000).
    """

    _closeable_attrs: ClassVar[tuple[str, ...]] = ("_conn",)
    _companion_attrs: ClassVar[tuple[str, ...]] = ("_cursor",)

    def __init__(
        self,
        db_path: str,
        sql_query: str,
        chunk_size: int = 10000,
        *,
        penstock: Penstock | None = None,
    ) -> None:
        super().__init__(penstock=penstock)
        self.db_path = db_path
        self.sql_query = sql_query
        self.chunk_size = chunk_size
        self._conn: sqlite3.Connection | None = None
        self._cursor: sqlite3.Cursor | None = None

    def _fetch_chunk(self) -> list[dict[str, Any]]:
        if not self._conn:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._cursor = self._conn.execute(self.sql_query)
        if not self._cursor:
            return []
        rows = self._cursor.fetchmany(self.chunk_size)
        return [_deserialize_row(dict(row)) for row in rows]


class CSVPaginator(_LocalChunkedPaginator):
    """Drain a CSV / TSV / PSV file in O(1)-memory chunks without
    ``pandas.read_csv`` slurping the whole file at once.

    Reach for this when the source is a flat text file too large for an
    in-memory DataFrame — a multi-GB daily transaction export, a USPS-style
    pipe-delimited drop, a tail of access logs ingested as CSV.  Canonical
    fit: parse a vendor's daily transaction dump and route each chunk into
    an ``Incorporator`` subclass's registry for transform-and-re-export.

    Example::

        async for wave in Transaction.stream(
            incorp_params={
                "inc_file": "daily_transactions.csv",
                "inc_code": "txn_id",
                "inc_page": CSVPaginator(
                    file_path="daily_transactions.csv",
                    chunk_size=10_000,
                ),
            },
            export_params={"file_path": "transactions.parquet"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    Opens the file once on first ``paginate()`` call, then yields chunks
    of ``chunk_size`` rows via ``itertools.islice`` over a
    ``csv.DictReader`` — peak memory stays bounded by one chunk plus the
    reader's line buffer.  Switch ``delimiter`` for TSV (``"\\t"``), PSV
    (``"|"``), or any single-character separator.  All I/O runs inside
    ``asyncio.to_thread`` so the event loop never blocks on disk reads;
    cleanup is inherited from :class:`_LocalChunkedPaginator`.

    Args:
        file_path: Filesystem path to the CSV file (must be UTF-8).
        chunk_size: Rows per chunk (default 10 000).
        delimiter: Field separator — ``","`` (default), ``"\\t"`` for TSV,
            ``"|"`` for PSV, etc.
    """

    _closeable_attrs: ClassVar[tuple[str, ...]] = ("_file",)
    _companion_attrs: ClassVar[tuple[str, ...]] = ("_reader",)

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 10000,
        delimiter: str = ",",
        *,
        penstock: Penstock | None = None,
    ) -> None:
        super().__init__(penstock=penstock)
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.delimiter = delimiter
        self._file: IO[Any] | None = None
        self._reader: Any | None = None

    def _fetch_chunk(self) -> list[dict[str, Any]]:
        if not self._file:
            self._file = open(self.file_path, "rt", encoding="utf-8")
            self._reader = csv.DictReader(self._file, delimiter=self.delimiter)
        if self._reader is None:
            return []
        return [_deserialize_row(dict(row)) for row in itertools.islice(self._reader, self.chunk_size)]


class AvroPaginator(_LocalChunkedPaginator):
    """Drain a Kafka-archive / Hadoop-staged Apache Avro file chunk-by-chunk —
    the schema-evolution-aware binary format common in event-pipeline
    archives.

    Reach for this when the source is an Avro file dropped by an upstream
    streaming system: a daily-rotated dump from a Kafka topic, a Hadoop
    staging area, a data-lake landing zone.  Canonical fit: process a
    daily Avro export from a Kafka topic without loading the full file —
    decode rows with ``fastavro``, transform via ``conv_dict``, re-emit
    as NDJSON / Parquet / SQLite.

    Example::

        async for wave in Event.stream(
            incorp_params={
                "inc_file": "kafka_events_2026-05-18.avro",
                "inc_code": "event_id",
                "inc_page": AvroPaginator(
                    file_path="kafka_events_2026-05-18.avro",
                    chunk_size=10_000,
                ),
            },
            export_params={"file_path": "events.parquet"},
        ):
            print(wave.chunk_index, wave.rows_processed)

    Opens the binary file once on first ``paginate()`` call and yields
    ``chunk_size`` decoded records per iteration.  ``fastavro`` reads one
    Avro block at a time, so peak memory is bounded by block size +
    chunk size regardless of total file size.  Requires the optional
    ``fastavro`` extra (``pip install incorporator[avro]``); a clear
    :class:`RuntimeError` is raised if it is missing.  Cleanup is
    inherited from :class:`_LocalChunkedPaginator`.

    Args:
        file_path: Filesystem path to the Avro file.
        chunk_size: Records per chunk (default 10 000).
    """

    _closeable_attrs: ClassVar[tuple[str, ...]] = ("_file",)
    _companion_attrs: ClassVar[tuple[str, ...]] = ("_reader",)

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 10000,
        *,
        penstock: Penstock | None = None,
    ) -> None:
        super().__init__(penstock=penstock)
        self.file_path = file_path
        self.chunk_size = chunk_size
        self._file: IO[Any] | None = None
        self._reader: Any | None = None

    def _fetch_chunk(self) -> list[dict[str, Any]]:
        try:
            import fastavro
        except ImportError:
            raise RuntimeError("fastavro not installed.") from None

        if not self._file:
            self._file = open(self.file_path, "rb")
            self._reader = fastavro.reader(self._file)
        if self._reader is None:
            return []
        return [
            _deserialize_row(raw_row)
            for raw_row in itertools.islice(self._reader, self.chunk_size)
            if isinstance(raw_row, dict)
        ]
