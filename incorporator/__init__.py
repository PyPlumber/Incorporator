"""Incorporator: The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway.

Key sentinels exported from this package
-----------------------------------------
``new``
    Pass ``new`` as a field type when the value does not come from the source data and
    must be generated entirely by a ``calc()`` expression or an ``outflow`` transform::

        class Order(Incorporator):
            total: float = inc(new)  # will be populated by calc()
            margin: float = calc(lambda price, cost: price - cost, "price", "cost",
                                 default=0.0, target_type=float)

    Using ``inc(new)`` tells Incorporator to accept any Python type for that field
    without coercion, delegating full control to the attached computation.
"""

from __future__ import annotations

__version__ = "1.3.4"

from ._deps import Category, DepInfo, install_hint, list_deps
from .base import Incorporator
from .exceptions import (
    IncorporatorError,
    IncorporatorFormatError,
    IncorporatorNetworkError,
    IncorporatorSchemaError,
)
from .io.compression import CompressionType
from .io.formats import FormatType
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
from .io.penstock import (
    BurstPenstock,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
    register_host_penstock,
)
from .list import IncorporatorList
from .observability.logger import LoggedIncorporator, LoggingMixin, setup_class_logger
from .observability.wave import Wave
from .rejects import RejectEntry
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
from .tideweaver import (
    Current,
    Export,
    Fjord,
    Stream,
    Tide,
    Tideweaver,
    Watershed,
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
    "register_host_penstock",
    # Penstock hierarchy (paginator-level + host-level throttling)
    "Penstock",
    "NullPenstock",
    "SustainedPenstock",
    "BurstPenstock",
    "WindowPenstock",
    "SignalPenstock",
    "Wave",
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
    "RejectEntry",
    "IncorporatorError",
    "IncorporatorFormatError",
    "IncorporatorNetworkError",
    "IncorporatorSchemaError",
    # Tideweaver orchestration layer
    "Tideweaver",
    "Watershed",
    "Current",
    "Stream",
    "Fjord",
    "Export",
    "Tide",
    # Optional-dependency introspection
    "Category",
    "DepInfo",
    "install_hint",
    "list_deps",
]
