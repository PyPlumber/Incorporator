"""Delimiter-separated format handlers: CSV, TSV, and PSV."""

import csv
import io
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, TextIO, Union

from ...exceptions import IncorporatorFormatError
from ..formats import deserialize_nested, ensure_string, serialize_nested
from ._base import BaseFormatHandler

logger = logging.getLogger(__name__)


class CSVHandler(BaseFormatHandler):
    """Parse and write delimiter-separated text files (CSV / TSV / PSV).

    A single handler covers all three families — the ``delimiter`` ctor
    arg is the only thing that varies. Reads use ``csv.DictReader``;
    writes use ``csv.DictWriter`` with ``extrasaction="ignore"`` so
    out-of-schema keys are silently dropped rather than raising.
    Append mode is supported natively: subsequent writes skip the header
    row when the target file already exists with non-zero size.
    """

    def __init__(self, delimiter: str = ",") -> None:
        self.delimiter = delimiter

    def _parse_stream(self, stream: Union[TextIO, io.StringIO], **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            reader = csv.DictReader(stream, delimiter=self.delimiter)
            rows: List[Dict[str, Any]] = []

            for row in reader:
                parsed_row: Dict[str, Any] = {}
                for k, v in row.items():
                    safe_k = str(k) if k is not None else "unknown_column"
                    parsed_row[safe_k] = deserialize_nested(v)
                rows.append(parsed_row)
            return rows
        except csv.Error as e:
            raise IncorporatorFormatError(f"Invalid Delimited Format: {e}") from e

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Read a delimited file or byte buffer and yield rows as dicts.

        Cells are passed through ``deserialize_nested`` so any JSON-encoded
        list/dict cells (written by ``serialize_nested``) round-trip back to
        native Python types.
        """
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f, **kwargs)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(io.StringIO(raw_data), **kwargs)

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Stream rows to a delimited file using a generator pipeline.

        Honours ``all_field_names`` (column order) and ``if_exists="append"``
        (skips the header row when the target file already exists with
        non-zero size). Nested dict/list values are JSON-encoded via
        ``serialize_nested``.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        try:
            path = Path(file_path).resolve()
            is_append = kwargs.get("if_exists") == "append"
            mode = "a" if is_append else "w"

            # Only write headers if we are creating a new file
            write_headers = not (is_append and path.exists() and path.stat().st_size > 0)

            explicit_fieldnames: List[str] = kwargs.get("all_field_names") or []
            data_iter: Iterable[Dict[str, Any]]

            if not explicit_fieldnames:
                # No schema hint available (e.g. called outside export()): must materialize
                # the full dataset to discover all column names before writing headers.
                rows: List[Dict[str, Any]] = list(data)
                explicit_fieldnames = list(dict.fromkeys(k for row in rows for k in row))
                data_iter = iter(rows)
            else:
                data_iter = data

            with open(path, mode, encoding="utf-8", newline="") as f:
                processed_gen = ({k: serialize_nested(v) for k, v in row.items()} for row in data_iter)
                writer = csv.DictWriter(
                    f, fieldnames=explicit_fieldnames, delimiter=self.delimiter, extrasaction="ignore"
                )

                if write_headers:
                    writer.writeheader()

                writer.writerows(processed_gen)
        except OSError as e:
            raise IncorporatorFormatError(f"Delimited File IO Error on {file_path}: {e}") from e
