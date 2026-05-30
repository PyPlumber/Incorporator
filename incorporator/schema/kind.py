"""DataKind enum — type-ladder for value classification.

Stdlib-only imports; ``classify()`` lives in ``converters.py`` to avoid a
circular import (converters imports kind, kind must not import converters).
"""

from __future__ import annotations

from enum import Enum


class DataKind(Enum):
    """Type-ladder the framework uses to classify values.

    Declaration order IS the ladder: ``classify()`` walks from most specific
    (``GARBAGE``) toward least specific (``OBJECT`` / ``STRING``) and returns
    the first kind that fits.

    Used by ``parses_as_*`` wrappers in
    :mod:`incorporator.schema.converters` and by the inspector cascade in
    :mod:`incorporator.tools.inspector`.
    """

    GARBAGE = "garbage"
    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    DATETIME = "datetime"
    STRING = "string"
    OBJECT = "object"
