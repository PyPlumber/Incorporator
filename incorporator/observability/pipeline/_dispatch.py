"""Front-door engine dispatcher for ``stream()`` and ``fjord()``.

The only configuration that loses data is **chunking + paginator +
monolithic export format**: every chunk would overwrite the previous file
because the format (Parquet / Feather / ORC / Excel / XML / JSON / HTML)
cannot append.  This module hoists the check to engine-selection time so
the traceback points at the user's call site, not at the async generator
mid-stream.

Stateful + monolithic is intentionally NOT rejected: stateful semantics
("file always holds the latest registry snapshot") map cleanly onto
``replace`` mode, which fjord's ``_resolve_if_exists_for_export`` already
coerces to.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from ...exceptions import IncorporatorFormatError
from ...io.formats import infer_format

__all__ = ["assert_engine_supported"]


def assert_engine_supported(
    *,
    file_path: Optional[Union[str, Any]],
    stateful_polling: bool,
    has_paginator: bool,
) -> None:
    """Raise :class:`IncorporatorFormatError` at call-site time if the
    requested engine cannot safely write the requested format.

    Decision matrix:

    +------------------+-----------------------+-----------+--------------------------+
    | stateful_polling | export format         | paginator | result                   |
    +==================+=======================+===========+==========================+
    | False            | append-friendly       | any       | OK (chunking)            |
    +------------------+-----------------------+-----------+--------------------------+
    | False            | monolithic            | no        | OK (single write)        |
    +------------------+-----------------------+-----------+--------------------------+
    | False            | monolithic            | yes       | **raise** — data-loss    |
    +------------------+-----------------------+-----------+--------------------------+
    | True             | any                   | n/a       | OK (shim → fjord)        |
    +------------------+-----------------------+-----------+--------------------------+

    Append-friendly formats: NDJSON, CSV, TSV, PSV, SQLite, Avro.
    Monolithic formats: Parquet, Feather, ORC, Excel, JSON, XML, HTML.
    """
    # Stateful paths are always safe — fjord coerces non-append exports to
    # "replace" on every tick.  No need to inspect the format here.
    if stateful_polling:
        return

    # Chunking without a paginator runs exactly once.  Monolithic formats
    # are fine because there's only one write.
    if not has_paginator:
        return

    # Chunking + paginator + no export target → in-memory only, no write
    # happens, nothing to validate.
    if file_path is None:
        return

    try:
        fmt = infer_format(str(file_path))
    except Exception:
        # Unknown extension → let the downstream handler raise its own
        # error with the format-specific message.
        return

    if fmt.is_append_safe:
        return

    raise IncorporatorFormatError(
        f"Chunked streaming to {fmt.value!r} would lose data — every chunk would "
        f"overwrite the prior chunk's output.  Switch the export target to an "
        f"append-friendly format (.ndjson / .csv / .sqlite / .avro), drop the "
        f"paginator for a single-shot write, or use stateful_polling=True if you "
        f"want the file to always hold the latest registry snapshot."
    )
