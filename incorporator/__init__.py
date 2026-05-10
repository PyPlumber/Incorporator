"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.7"

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
from .methods.logger import LoggedIncorporator, LoggingMixin, setup_class_logger
from .methods.paginate import (
    AsyncPaginator,
    CursorPaginator,
    LinkHeaderPaginator,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)

__all__ =[
    "__version__",
    "Incorporator",
    "IncorporatorList",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
    "CompressionType",
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
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]