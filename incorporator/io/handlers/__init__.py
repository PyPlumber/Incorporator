"""Native zero-bloat format I/O handlers and dispatch for Incorporator."""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, cast

from ...exceptions import IncorporatorFormatError
from ..formats import FormatType
from ._base import BaseFormatHandler
from .binary import AvroHandler, SQLiteHandler
from .columnar import FeatherHandler, OrcHandler, ParquetHandler
from .delimited import CSVHandler
from .markup import HTMLHandler
from .spreadsheet import ExcelHandler
from .text import JSONHandler, NDJSONHandler, XMLHandler

logger = logging.getLogger(__name__)

__all__ = ["parse_source_data", "write_destination_data"]

_HANDLERS: dict[FormatType, BaseFormatHandler] = {
    FormatType.JSON: JSONHandler(),
    FormatType.NDJSON: NDJSONHandler(),
    FormatType.CSV: CSVHandler(delimiter=","),
    FormatType.TSV: CSVHandler(delimiter="\t"),
    FormatType.PSV: CSVHandler(delimiter="|"),
    FormatType.XML: XMLHandler(),
    FormatType.SQLITE: SQLiteHandler(),
    FormatType.AVRO: AvroHandler(),
    FormatType.XLSX: ExcelHandler(),
    FormatType.PARQUET: ParquetHandler(),
    FormatType.FEATHER: FeatherHandler(),
    FormatType.ORC: OrcHandler(),
    FormatType.HTML: HTMLHandler(),
}


async def parse_source_data(
    source: str | bytes | Path | list[Any] | dict[str, Any], format_type: FormatType, **kwargs: Any
) -> dict[str, Any] | list[dict[str, Any]]:
    """Central parse dispatcher — routes the payload to the matching format handler.

    Pre-parsed ``list`` / ``dict`` sources pass through untouched. File / byte
    / string sources are dispatched to the registered :class:`BaseFormatHandler`
    for ``format_type`` and parsed inside ``asyncio.to_thread`` so disk I/O
    and CPU-heavy decoding never block the event loop.
    """
    if isinstance(source, list):
        return cast(list[dict[str, Any]], source)
    if isinstance(source, dict):
        return source

    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported format: '{format_type}'.")

    try:
        return await asyncio.to_thread(handler.parse, source, **kwargs)
    except IncorporatorFormatError:
        raise
    except Exception as e:
        snippet = str(source).strip()[:60].replace("\n", " ")
        raise IncorporatorFormatError(
            f"Parse failed for format '{format_type}'. "
            f"The payload may be malformed (e.g., corrupted file or HTML firewall). "
            f"\n   Error: {e}\n   Received snippet: {snippet!r}..."
        ) from e


def _peek_iterable(data: Iterable[Any]) -> tuple[bool, Iterator[Any]]:
    """Non-destructively probe any Iterable for emptiness.

    Consumes one item from the iterator and chains it back, so the caller
    receives a fully-intact iterator regardless of outcome.
    Returns (is_empty, reconstructed_iterator).

    .. warning::
        The returned iterator is **single-pass**.  Do not iterate it more than once —
        the peeked first element is stored in memory only for the chain and will not
        be re-emitted on a second iteration.
    """
    it = iter(data)
    try:
        first = next(it)
    except StopIteration:
        return True, iter([])
    return False, itertools.chain([first], it)


async def write_destination_data(
    data: Iterable[Any], file_path: str | Path, format_type: FormatType, **kwargs: Any
) -> None:
    """Central write dispatcher — routes the row stream to the matching format handler.

    Empty-input guard runs once here via ``_peek_iterable`` so individual
    handlers don't need to repeat it.  Parent-directory creation also runs
    once here so every handler gets the same "just works" behaviour when
    the user passes ``data/foo.ndjson`` without pre-creating ``data/``.
    The handler's ``write`` runs inside ``asyncio.to_thread`` so disk I/O
    and CPU-heavy encoding never block the event loop.

    Row type: ``Iterable[Any]`` rather than ``Iterable[Dict]`` because the
    upstream pipeline yields Pydantic ``BaseModel`` instances directly for
    text formats (JSON / NDJSON), letting the handler call
    ``model_dump_json()`` and skip the intermediate dict allocation.  Other
    handlers still receive plain dicts.
    """
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported export format: '{format_type}'.")

    # Resolve the destination path ONCE here so every downstream consumer
    # (mkdir, atomic_write_path inside the handler, the handler's own
    # Path.resolve()) reuses the same syscall.  Pre-fix, each handler
    # called Path(file_path).resolve() independently — a redundant stat
    # syscall per write that adds up under streaming daemons ticking
    # hundreds of times per minute.
    resolved_path = Path(file_path).resolve()

    # Auto-create the parent directory.  Streaming pipelines often target
    # paths like "data/<name>.ndjson" — failing every export tick because
    # the user didn't mkdir is hostile DX for zero benefit.  Run pre-write
    # so the empty-input case below short-circuits without a mkdir burn.
    parent = resolved_path.parent
    if parent and not parent.exists():
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise IncorporatorFormatError(f"Could not create parent directory {parent} for '{file_path}': {e}") from e

    is_empty, safe_iter = _peek_iterable(data)
    if is_empty:
        return

    # Pass the already-resolved path so the handler's internal Path.resolve()
    # is effectively a no-op (Path.resolve on an absolute path returns itself).
    await asyncio.to_thread(handler.write, safe_iter, resolved_path, **kwargs)
