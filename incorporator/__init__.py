"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.3"

from .base import Incorporator
from .methods.converters import (
    inc,
    calc,
    calc_all,
    extract_url_id,
    link_to,
    link_to_list,
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
from .methods.paginate import NextUrlPaginator

__all__ =[
    "__version__",
    "Incorporator",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
    "inc",
    "calc",
    "calc_all",
    "split_and_get",
    "link_to",
    "link_to_list",
    "extract_url_id",
    "pluck",
    "NextUrlPaginator",
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]