"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.0"

from .base import Incorporator
from .methods.converters import (
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

__all__ =[
    "__version__",
    "Incorporator",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
    "split_and_get",
    "link_to",
    "link_to_list",
    "extract_url_id",
    "pluck",
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]