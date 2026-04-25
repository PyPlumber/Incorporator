"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway."""

__version__ = "1.0.0"

from .base import Incorporator
from .methods.converters import (
    cast_list_items,
    default_if_null,
    extract_url_id,
    json_path_extractor,
    link_to,
    link_to_list,
    pluck,
    split_and_get,
    to_bool,
    to_date,
    to_float,
    to_int,
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
    "to_bool",
    "to_date",
    "to_int",
    "to_float",
    "split_and_get",
    "cast_list_items",
    "default_if_null",
    "link_to",
    "link_to_list",
    "json_path_extractor",
    "extract_url_id",
    "pluck",
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
]