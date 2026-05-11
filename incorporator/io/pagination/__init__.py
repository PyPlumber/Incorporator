"""Pagination engine for Incorporator: web API and local file paginators."""

from .base import AsyncPaginator
from .local import AvroPaginator, CSVPaginator, SQLitePaginator
from .web import CursorPaginator, LinkHeaderPaginator, NextUrlPaginator, OffsetPaginator, PageNumberPaginator

__all__ = [
    "AsyncPaginator",
    "AvroPaginator",
    "CSVPaginator",
    "SQLitePaginator",
    "CursorPaginator",
    "LinkHeaderPaginator",
    "NextUrlPaginator",
    "OffsetPaginator",
    "PageNumberPaginator",
]
