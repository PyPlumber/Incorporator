"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

from .base import Incorporator
from .methods.logger import LoggedIncorporator, LoggingMixin, setup_class_logger
from .methods.format_parsers import FormatType
from .methods.converters import (
    to_bool,
    to_date,
    to_int,
    to_float,
    split_and_get,
    cast_list_items,
    default_if_null,
    link_to  # <--- ADDED HERE
)
from .methods.exceptions import (
    IncorporatorError,
    IncorporatorFormatError,
    IncorporatorNetworkError,
    IncorporatorSchemaError,
)

__all__ =[
    "Incorporator",
    "LoggedIncorporator",
    "LoggingMixin",
    "setup_class_logger",
    "FormatType",
    "to_bool",
    "to_date",
    "to_int",
    "to_float",
    "split_and_get",
    "cast_list_items",
    "default_if_null",
    "link_to",  # <--- ADDED HERE
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]