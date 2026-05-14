"""Abstract base handler and shared utilities for format I/O."""

import os
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Union

from ...exceptions import IncorporatorFormatError
from ..formats import FormatType


@contextmanager
def atomic_write_path(target: Union[str, Path]) -> Iterator[Path]:
    """Yield a sibling tempfile path; rename atomically on success.

    Use for monolithic formats that write a single bulk file (Parquet,
    Excel, JSON, XML).  If the write completes successfully, the
    tempfile is renamed to ``target`` via ``os.replace()`` — an atomic
    operation on POSIX and Windows.  If the write raises, the tempfile
    is removed so we don't leave a half-written corrupt file behind.

    Pre-existing ``target`` files are left untouched until the rename
    succeeds, so an interrupted write never destroys the prior version.

    Streaming formats (NDJSON, CSV, SQLite, Avro) don't need this —
    their writes append line-by-line and partial output is recoverable.
    """
    target_path = Path(target).resolve()
    # Sibling tempfile in the same directory so os.replace stays atomic
    # (cross-device renames are not atomic on POSIX).
    tmp_path = target_path.with_name(f"{target_path.name}.tmp-{uuid.uuid4().hex[:8]}")
    try:
        yield tmp_path
    except BaseException:
        # Clean up the tempfile on any failure (including KeyboardInterrupt).
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise
    else:
        # Successful write — rename atomically.  os.replace overwrites the
        # destination on both POSIX and Windows (≥Vista), so we don't need
        # to special-case "destination exists".
        os.replace(tmp_path, target_path)


# Formats whose write handlers accept ``if_exists="append"``.  The pipeline
# engines consult this set to decide whether to inject append semantics on
# subsequent chunks/ticks or to fall back to "replace" so monolithic formats
# stay readable (the alternative would crash mid-pipeline or silently lose
# pre-tick data).  Source of truth for the append-fallback contract.
APPEND_FRIENDLY_FORMATS: set = {
    FormatType.NDJSON,
    FormatType.CSV,
    FormatType.TSV,
    FormatType.PSV,
    FormatType.SQLITE,
    FormatType.AVRO,
}


def supports_append(format_type: FormatType) -> bool:
    """Return True when the format's write handler supports ``if_exists="append"``.

    Used by the chunked / stateful / fjord engines to decide whether to
    request append on subsequent chunks (accumulate output) or to fall back
    to ``"replace"`` so each chunk overwrites the file with the latest
    snapshot — the only sensible behaviour for monolithic formats like
    Parquet / Excel / XML / JSON under a streaming daemon.
    """
    return format_type in APPEND_FRIENDLY_FORMATS


# Spreadsheet-aware CSV/XLSX formula-injection prefixes.  When a string cell
# value starts with any of these, Excel / LibreOffice Calc / Google Sheets
# interpret the cell as a formula and execute it on open.  Industry-standard
# mitigation (per OWASP "Formula Injection") is to prefix the cell with a
# single-quote so the spreadsheet renders the raw text instead of evaluating.
_FORMULA_INJECTION_PREFIXES: tuple = ("=", "@", "+", "-", "\t", "\r")


def _neutralise_formula_injection(value: Any) -> Any:
    """Single-quote-prefix any string starting with a formula-evaluating char.

    Non-string values pass through unchanged.  Empty strings pass through.
    The prefix `'` is the canonical mitigation — Excel renders the literal
    text and never evaluates.  Reversed automatically on re-import by every
    spreadsheet tool.
    """
    if not isinstance(value, str) or not value:
        return value
    if value.startswith(_FORMULA_INJECTION_PREFIXES):
        return "'" + value
    return value


def _raise_if_append_unsupported(kwargs: Dict[str, Any], format_name: str) -> None:
    if kwargs.get("if_exists") == "append":
        raise IncorporatorFormatError(
            f"Monolithic formats ({format_name}) do not support O(1) streaming appends. "
            "Please stream to NDJSON, CSV, SQLite, or Avro instead."
        )


class BaseFormatHandler(ABC):
    """Abstract Strategy for parsing and writing different data formats."""

    @abstractmethod
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Parse a byte buffer, string, or Path into a dict or list of dicts.

        Each subclass implements its own format-specific parsing (JSON, XML,
        CSV, Parquet, etc.). Failures must raise :class:`IncorporatorFormatError`
        so the central dispatch can surface a uniform error shape.
        """
        pass

    @abstractmethod
    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Stream rows from an iterable to a file in the subclass's format.

        Subclasses honour standard kwargs where applicable: ``if_exists``
        (``"replace"`` / ``"append"`` / ``"fail"``), ``all_field_names``
        (column order hint), and format-specific tuning kwargs. Failures
        must raise :class:`IncorporatorFormatError`.
        """
        pass
