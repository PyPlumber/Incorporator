"""Unit tests for HTMLHandler — table extraction, header detection, multi-table
selection, and the missing-dep error path. lxml ships in ``[speedups]``."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import FormatType, infer_format
from incorporator.io.handlers import _HANDLERS
from incorporator.io.handlers.markup import HTMLHandler

pytest.importorskip("lxml")


SIMPLE_TABLE_HTML = """
<html><body>
  <table>
    <tr><th>id</th><th>name</th><th>score</th></tr>
    <tr><td>1</td><td>Alice</td><td>95.5</td></tr>
    <tr><td>2</td><td>Bob</td><td>87.0</td></tr>
    <tr><td>3</td><td>Carol</td><td>92.3</td></tr>
  </table>
</body></html>
"""

MULTI_TABLE_HTML = """
<html><body>
  <h2>Table 1</h2>
  <table>
    <tr><th>name</th><th>age</th></tr>
    <tr><td>Alice</td><td>30</td></tr>
  </table>
  <h2>Table 2</h2>
  <table>
    <tr><th>city</th><th>population</th></tr>
    <tr><td>NYC</td><td>8M</td></tr>
    <tr><td>LA</td><td>4M</td></tr>
  </table>
</body></html>
"""

NO_HEADER_TABLE_HTML = """
<html><body>
  <table>
    <tr><td>foo</td><td>bar</td></tr>
    <tr><td>baz</td><td>qux</td></tr>
  </table>
</body></html>
"""

EMPTY_ROW_TABLE_HTML = """
<html><body>
  <table>
    <tr><th>a</th><th>b</th></tr>
    <tr><td>1</td><td>2</td></tr>
    <tr><td></td><td></td></tr>
    <tr><td>3</td><td>4</td></tr>
  </table>
</body></html>
"""


def test_html_extracts_first_table_by_default() -> None:
    """Default behaviour: first <table> on the page."""
    rows = HTMLHandler().parse(SIMPLE_TABLE_HTML)
    assert len(rows) == 3
    assert rows[0] == {"id": "1", "name": "Alice", "score": "95.5"}
    assert rows[2]["name"] == "Carol"


def test_html_table_index_selects_specific_table() -> None:
    """table_index=1 must select the second table."""
    rows = HTMLHandler().parse(MULTI_TABLE_HTML, table_index=1)
    assert len(rows) == 2
    assert rows[0] == {"city": "NYC", "population": "8M"}
    assert rows[1]["city"] == "LA"


def test_html_table_index_minus_one_flattens_all() -> None:
    """table_index=-1 must flatten all tables into one stream."""
    rows = HTMLHandler().parse(MULTI_TABLE_HTML, table_index=-1)
    # Table 1 has 1 row, Table 2 has 2 rows → total 3
    assert len(rows) == 3


def test_html_no_header_fallback_uses_first_row() -> None:
    """When no <th> exists, the first <td> row becomes the header."""
    rows = HTMLHandler().parse(NO_HEADER_TABLE_HTML)
    # First row 'foo,bar' becomes headers; second row 'baz,qux' is the data.
    assert len(rows) == 1
    assert rows[0] == {"foo": "baz", "bar": "qux"}


def test_html_skips_fully_blank_rows() -> None:
    """Rows where every cell is empty must be skipped (formatting noise)."""
    rows = HTMLHandler().parse(EMPTY_ROW_TABLE_HTML)
    assert len(rows) == 2
    assert rows[0] == {"a": "1", "b": "2"}
    assert rows[1] == {"a": "3", "b": "4"}


def test_html_parse_from_path(tmp_path: Path) -> None:
    """Path input must work."""
    html_path = tmp_path / "page.html"
    html_path.write_text(SIMPLE_TABLE_HTML, encoding="utf-8")
    rows = HTMLHandler().parse(html_path)
    assert len(rows) == 3


def test_html_no_tables_on_page_raises() -> None:
    """Payload with no <table> must raise a clear error."""
    with pytest.raises(IncorporatorFormatError, match="no <table> elements"):
        HTMLHandler().parse("<html><body><p>no tables here</p></body></html>")


def test_html_table_index_out_of_range_raises() -> None:
    """Out-of-range table_index must raise."""
    with pytest.raises(IncorporatorFormatError, match="out of range"):
        HTMLHandler().parse(MULTI_TABLE_HTML, table_index=99)


def test_html_write_not_supported(tmp_path: Path) -> None:
    """HTMLHandler.write must reject — out of scope."""
    with pytest.raises(IncorporatorFormatError, match="HTML write is not supported"):
        HTMLHandler().write([{"a": 1}], tmp_path / "x.html")


def test_infer_format_html_extension() -> None:
    assert infer_format("page.html") == FormatType.HTML
    assert infer_format("page.htm") == FormatType.HTML
    assert infer_format("PAGE.HTML") == FormatType.HTML


def test_html_handler_registered_in_dispatch() -> None:
    assert FormatType.HTML in _HANDLERS
    assert isinstance(_HANDLERS[FormatType.HTML], HTMLHandler)


def test_html_missing_lxml_message(tmp_path: Path) -> None:
    """When lxml is missing the error must point to the [speedups] extras flag."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "lxml.html":
            raise ImportError("simulated missing lxml")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(IncorporatorFormatError, match=r"pip install incorporator\[speedups\]"):
            HTMLHandler().parse(SIMPLE_TABLE_HTML)
