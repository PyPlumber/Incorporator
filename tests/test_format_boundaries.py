"""Test skeleton for CSV and XML format boundary ingestion and transformation."""

from pathlib import Path
from typing import List

import pytest

from incorporator import FormatType, Incorporator


@pytest.mark.asyncio
async def test_csv_etl_type_conversions(csv_users_payload: str, tmp_path: Path) -> None:
    """Tests that Incorporator successfully parses CSV and applies conv_dict to string data."""
    mock_file = tmp_path / "users.csv"
    mock_file.write_text(csv_users_payload)

    users = await Incorporator.incorp(
        file=str(mock_file),
        format_type=FormatType.CSV,
        code='id',
        name='username',
        excl_lst=['account_balance'],  # Drop financial data
        conv_dict={
            # CSV readers return strings by default. conv_dict handles the type casting.
            'id': lambda x: int(x),
            'is_active': lambda x: str(x).lower() == 'true'
        }
    )

    assert isinstance(users, list)
    assert len(users) == 2
    alice = users[0]

    assert getattr(alice, "username") == "alice_smith"
    assert getattr(alice, "id") == 101
    assert getattr(alice, "is_active") is True
    assert not hasattr(alice, "account_balance")


@pytest.mark.asyncio
async def test_xml_etl_rpath_and_renaming(xml_catalog_payload: str, tmp_path: Path) -> None:
    """Tests that Incorporator parses nested XML, drills down via rPath, and renames tags."""
    mock_file = tmp_path / "catalog.xml"
    mock_file.write_text(xml_catalog_payload)

    books = await Incorporator.incorp(
        file=str(mock_file),
        format_type=FormatType.XML,
        # Drill straight through the catalog wrapper into the book array!
        rPath='catalog.book',
        code='id',
        name='title',
        name_chg=[('title', 'book_title'), ('price', 'cost_usd')],  # Rename XML tags
        conv_dict={
            'price': lambda x: float(x)  # Convert XML text to float
        }
    )

    assert isinstance(books, list)
    assert len(books) == 2

    book_1 = books[0]

    # Assert XML attributes (id='bk101') were extracted correctly
    assert getattr(book_1, "id") == 'bk101'
    assert getattr(book_1, "author") == "Gambardella, Matthew"

    # Assert name_chg successfully renamed the XML nodes
    assert not hasattr(book_1, "title")
    assert getattr(book_1, "book_title") == "XML Developer's Guide"

    # Assert combinations of name_chg and conv_dict work sequentially
    assert not hasattr(book_1, "price")
    assert getattr(book_1, "cost_usd") == 44.95