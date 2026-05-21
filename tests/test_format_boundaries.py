"""Test skeleton for CSV and XML format boundary ingestion and transformation."""

from pathlib import Path

import pytest

from incorporator import FormatType, Incorporator, inc


@pytest.mark.asyncio
async def test_csv_etl_type_conversions(csv_users_payload: str, tmp_path: Path) -> None:
    """Subclass parses CSV and applies conv_dict to coerce the string payload."""
    mock_file = tmp_path / "users.csv"
    mock_file.write_text(csv_users_payload)

    class User(Incorporator):
        pass

    users = await User.incorp(
        inc_file=str(mock_file),
        format_type=FormatType.CSV,
        inc_code="id",
        inc_name="username",
        excl_lst=["account_balance"],  # Drop financial data
        conv_dict={
            # CSV readers return strings by default. inc() handles the type
            # coercion with null-safe defaults — no defensive lambda needed.
            "id": inc(int),
            "is_active": inc(bool),
        },
    )

    assert isinstance(users, list)
    assert len(users) == 2
    alice = users[0]

    assert alice.username == "alice_smith"
    assert alice.id == 101
    assert alice.is_active is True
    assert not hasattr(alice, "account_balance")


@pytest.mark.asyncio
async def test_xml_etl_rpath_and_renaming(xml_catalog_payload: str, tmp_path: Path) -> None:
    """Subclass parses nested XML, drills via rec_path, and renames tags."""
    mock_file = tmp_path / "catalog.xml"
    mock_file.write_text(xml_catalog_payload)

    class Book(Incorporator):
        pass

    books = await Book.incorp(
        inc_file=str(mock_file),
        format_type=FormatType.XML,
        # Drill straight through the catalog wrapper into the book array!
        rec_path="catalog.book",
        inc_code="id",
        inc_name="title",
        name_chg=[("title", "book_title"), ("price", "cost_usd")],  # Rename XML tags
        conv_dict={
            "price": inc(float),  # Convert XML text to float (null-safe)
        },
    )

    assert isinstance(books, list)
    assert len(books) == 2

    book_1 = books[0]

    # Assert XML attributes (id='bk101') were extracted correctly
    assert book_1.id == "bk101"
    assert book_1.author == "Gambardella, Matthew"

    # Assert name_chg successfully renamed the XML nodes
    assert not hasattr(book_1, "title")
    assert book_1.book_title == "XML Developer's Guide"

    # Assert combinations of name_chg and conv_dict work sequentially
    assert not hasattr(book_1, "price")
    assert book_1.cost_usd == 44.95
