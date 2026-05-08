"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.6"

from .base import Incorporator
from .methods.converters import (
    calc,
    calc_all,
    each,  # Added POST token
    inc,
    join_all,  # Added POST token
    as_list,  # Added POST token
    link_to,
    link_to_list,
    new,  # Added Sentinel
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
    CursorPaginator,  # Added Paginator
    LinkHeaderPaginator,  # Added Paginator
    NextUrlPaginator,
    OffsetPaginator,  # Added Paginator
    PageNumberPaginator,  # Added Paginator
)

__all__ = [
    "__version__",
    "Incorporator",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
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
