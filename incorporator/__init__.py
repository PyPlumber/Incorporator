"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.8"

from .base import Incorporator
from .list import IncorporatorList
from .io.compression import CompressionType
from .schema.converters import (
    calc,
    calc_all,
    inc,
    new,
)
from .schema.extractors import (
    as_list,
    each,
    join_all,
    link_to,
    link_to_list,
    pluck,
    split_and_get,
    sum_attributes,
)
from .exceptions import (
    IncorporatorError,
    IncorporatorFormatError,
    IncorporatorNetworkError,
    IncorporatorSchemaError,
)
from .io.formats import FormatType
from .observability.logger import AuditResult, LoggedIncorporator, LoggingMixin, setup_class_logger
from .io.pagination import (
    AsyncPaginator,
    AvroPaginator,
    CSVPaginator,
    CursorPaginator,
    LinkHeaderPaginator,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    SQLitePaginator,
)

__all__ = [
    "__version__",
    "Incorporator",
    "IncorporatorList",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
    "CompressionType",
    "AuditResult",
    "inc",
    "calc",
    "calc_all",
    "each",
    "join_all",
    "as_list",
    "new",
    "split_and_get",
    "sum_attributes",
    "link_to",
    "link_to_list",
    "pluck",
    "AsyncPaginator",
    "CursorPaginator",
    "LinkHeaderPaginator",
    "NextUrlPaginator",
    "OffsetPaginator",
    "PageNumberPaginator",
    "SQLitePaginator",
    "CSVPaginator",
    "AvroPaginator",
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]
