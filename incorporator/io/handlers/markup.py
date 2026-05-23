"""HTML table handler — parse-only, closes the ``pandas.read_html`` gap.

HTML tables are the dominant format on the public web (Wikipedia, government
data portals, financial reports, scraped news sites). This handler extracts
every ``<table>`` element on a page and returns its rows as dicts, mirroring
``pandas.read_html`` semantics.

Why lxml: the stdlib ``html.parser`` is forgiving but lacks CSS selectors and
robust table-cell extraction. lxml is already an optional dep in
``[speedups]`` (shared with the XML handler) so adding HTML costs zero new
install footprint for anyone who already opted into speedups.

Scope:
* **Parse only.** Writing HTML tables is rarely useful and conflicts with the
  framework's structured-data focus.
* **Default behaviour:** extract the *first* table on the page (the most common
  scraping pattern).
* **Kwargs:** ``table_index=N`` selects the Nth table (0-indexed). ``table_index=-1``
  flattens *all* tables on the page into one stream.
* **Header detection:** uses the first ``<tr>`` containing ``<th>`` cells. Falls
  back to row 1 if no ``<th>`` is present.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Union

from ...exceptions import IncorporatorFormatError
from ..formats import ensure_string
from ._base import BaseFormatHandler, _require_optional

logger = logging.getLogger(__name__)


def _extract_rows_from_table(table_el: Any) -> list[dict[str, Any]]:
    """Extract a single <table> into a list of dicts keyed by header cells.

    Walks the table once: the first row containing <th> cells is taken as the
    header. Subsequent <tr> rows are zipped against those headers. Empty rows
    are skipped.
    """
    rows = table_el.xpath(".//tr")
    if not rows:
        return []

    headers: list[str] = []
    header_row_idx = 0

    # Find the first row with <th> cells. If none, fall back to row 0.
    for idx, tr in enumerate(rows):
        th_cells = tr.xpath("./th")
        if th_cells:
            headers = [(th.text_content() or "").strip() or f"col_{i}" for i, th in enumerate(th_cells)]
            header_row_idx = idx
            break

    if not headers:
        # No <th> anywhere — use the first row's <td> as headers (best-effort).
        first_row_cells = rows[0].xpath("./td")
        headers = [(td.text_content() or "").strip() or f"col_{i}" for i, td in enumerate(first_row_cells)]
        header_row_idx = 0

    parsed: list[dict[str, Any]] = []
    for tr in rows[header_row_idx + 1 :]:
        cells = tr.xpath("./td")
        if not cells:
            continue
        values = [(td.text_content() or "").strip() for td in cells]
        # Skip completely blank rows (formatting whitespace, etc.)
        if all(not v for v in values):
            continue
        row_dict: dict[str, Any] = {}
        for i, val in enumerate(values):
            key = headers[i] if i < len(headers) else f"col_{i}"
            row_dict[key] = val
        parsed.append(row_dict)
    return parsed


class HTMLHandler(BaseFormatHandler):
    """Parse-only HTML table handler. Requires lxml (ships in ``[speedups]``)."""

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> list[dict[str, Any]]:
        """Extract ``<table>`` rows from an HTML payload and return them as dicts.

        By default returns the first table on the page. Pass ``table_index=N``
        to select the Nth table, or ``table_index=-1`` to flatten every table
        on the page into one stream. Header cells are auto-detected from the
        first ``<tr>`` containing ``<th>`` elements; falls back to row 0 if no
        ``<th>`` is present.
        """
        lxml_html = _require_optional("lxml.html")

        try:
            raw_text = source.read_text(encoding="utf-8") if isinstance(source, Path) else ensure_string(source)

            # fromstring handles partial/fragment HTML gracefully; for full
            # documents it auto-detects the body. No external entity resolution.
            doc = lxml_html.fromstring(raw_text)
            all_tables = doc.xpath("//table")
            if not all_tables:
                raise IncorporatorFormatError("HTML payload contains no <table> elements to extract.")

            table_index: int = kwargs.get("table_index", 0)

            if table_index == -1:
                # Flatten ALL tables into one stream.
                flat: list[dict[str, Any]] = []
                for t in all_tables:
                    flat.extend(_extract_rows_from_table(t))
                return flat

            if table_index < 0 or table_index >= len(all_tables):
                raise IncorporatorFormatError(
                    f"table_index={table_index} out of range — page has {len(all_tables)} table(s)."
                )

            return _extract_rows_from_table(all_tables[table_index])
        except IncorporatorFormatError:
            raise
        except Exception as e:
            raise IncorporatorFormatError(f"HTML Parse Error: {e}") from e

    def write(self, data: Iterable[dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Always raises — HTML write is intentionally out of scope.

        See the module docstring for the design rationale. Export to JSON,
        CSV, or Parquet for structured output instead.
        """
        # HTML write is intentionally out of scope — see module docstring.
        raise IncorporatorFormatError(
            "HTML write is not supported. Export to JSON, CSV, or Parquet for structured output."
        )
