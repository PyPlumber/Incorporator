"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.8"

from .base import Incorporator, IncorporatorList
from .methods.compression import CompressionType
from .methods.converters import (
    as_list,
    calc,
    calc_all,
    each,
    inc,
    join_all,
    link_to,
    link_to_list,
    new,
    pluck,
    split_and_get,
)
from .methods.exceptions import (
    IncorporatorError,
    IncorporatorFormatError,
    IncorporatorNetworkError,
    IncorporatorSchemaError,
)
from .methods.format_parsers import FormatType
from .methods.logger import AuditResult, LoggedIncorporator, LoggingMixin, setup_class_logger
from .methods.paginate import (
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
