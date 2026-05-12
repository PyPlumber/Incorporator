"""Native zero-bloat format I/O handlers and dispatch for Incorporator."""

import asyncio
import itertools
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple, Union, cast

from ...exceptions import IncorporatorFormatError
from ..formats import FormatType
from ._base import BaseFormatHandler
from .binary import AvroHandler, SQLiteHandler
from .delimited import CSVHandler
from .text import JSONHandler, NDJSONHandler, XMLHandler

logger = logging.getLogger(__name__)

__all__ = ["parse_source_data", "write_destination_data"]

_HANDLERS: Dict[FormatType, BaseFormatHandler] = {
    FormatType.JSON: JSONHandler(),
    FormatType.NDJSON: NDJSONHandler(),
    FormatType.CSV: CSVHandler(delimiter=","),
    FormatType.TSV: CSVHandler(delimiter="\t"),
    FormatType.PSV: CSVHandler(delimiter="|"),
    FormatType.XML: XMLHandler(),
    FormatType.SQLITE: SQLiteHandler(),
    FormatType.AVRO: AvroHandler(),
}


async def parse_source_data(
    source: Union[str, bytes, Path, List[Any], Dict[str, Any]], format_type: FormatType, **kwargs: Any
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    if isinstance(source, list):
        return cast(List[Dict[str, Any]], source)
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


def _peek_iterable(data: Iterable[Dict[str, Any]]) -> Tuple[bool, Iterator[Dict[str, Any]]]:
    """Non-destructively probe any Iterable for emptiness.

    Consumes one item from the iterator and chains it back, so the caller
    receives a fully-intact iterator regardless of outcome.
    Returns (is_empty, reconstructed_iterator).
    """
    it = iter(data)
    try:
        first = next(it)
    except StopIteration:
        return True, iter([])
    return False, itertools.chain([first], it)


async def write_destination_data(
    data: Iterable[Dict[str, Any]], file_path: Union[str, Path], format_type: FormatType, **kwargs: Any
) -> None:
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported export format: '{format_type}'.")

    is_empty, safe_iter = _peek_iterable(data)
    if is_empty:
        return

    await asyncio.to_thread(handler.write, safe_iter, file_path, **kwargs)
