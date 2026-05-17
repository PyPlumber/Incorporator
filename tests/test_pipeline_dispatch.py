"""Tests for the front-door format dispatcher.

Validates ``assert_engine_supported`` — the single guard that catches the
``chunking + paginator + monolithic format`` data-loss case at engine-selection
time instead of letting the pipeline crash on the second chunk write.
"""

from typing import Any

import pytest

from incorporator import Incorporator
from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.pagination.base import AsyncPaginator
from incorporator.observability.pipeline import assert_engine_supported


class _DummyPaginator(AsyncPaginator):
    """Minimal AsyncPaginator stub for has_paginator detection."""

    def __init__(self) -> None:
        self.is_exhausted = False

    async def get_next_call_kwargs(self, **_: Any) -> dict:  # pragma: no cover - unused
        self.is_exhausted = True
        return {}


def test_dispatcher_rejects_paginator_to_parquet() -> None:
    """Chunking + paginator + Parquet → raise at call-site time, not mid-stream."""
    with pytest.raises(IncorporatorFormatError, match="overwrite"):
        assert_engine_supported(
            file_path="out.parquet",
            stateful_polling=False,
            has_paginator=True,
        )


def test_dispatcher_rejects_paginator_to_excel() -> None:
    """Same guard fires for every monolithic format (Excel example)."""
    with pytest.raises(IncorporatorFormatError):
        assert_engine_supported(
            file_path="out.xlsx",
            stateful_polling=False,
            has_paginator=True,
        )


def test_dispatcher_allows_paginator_to_ndjson() -> None:
    """Chunking + paginator + append-friendly format → no raise (the common path)."""
    assert_engine_supported(
        file_path="out.ndjson",
        stateful_polling=False,
        has_paginator=True,
    )


def test_dispatcher_allows_no_paginator_to_parquet() -> None:
    """Single-shot chunking writes one file once — monolithic targets are fine."""
    assert_engine_supported(
        file_path="out.parquet",
        stateful_polling=False,
        has_paginator=False,
    )


def test_dispatcher_allows_stateful_to_parquet() -> None:
    """Stateful streaming re-writes the same file each tick — monolithic OK."""
    assert_engine_supported(
        file_path="out.parquet",
        stateful_polling=True,
        has_paginator=False,
    )


def test_dispatcher_allows_no_file_path() -> None:
    """No export target → nothing to validate (in-memory only)."""
    assert_engine_supported(
        file_path=None,
        stateful_polling=False,
        has_paginator=True,
    )


@pytest.mark.asyncio
async def test_stream_raises_dispatcher_error_at_call_site() -> None:
    """stream() must hit the dispatcher BEFORE the engine starts.

    Pre-fix the dispatcher lived inside the chunking engine, so the error
    surfaced from inside the async generator (less helpful traceback).
    Post-fix it raises directly from the stream() entry point.
    """

    class StreamModel(Incorporator):
        inc_code: Any = None

    paginator = _DummyPaginator()
    gen = StreamModel.stream(
        incorp_params={"inc_url": "http://example.invalid", "inc_page": paginator},
        export_params={"file_path": "out.parquet"},
    )
    with pytest.raises(IncorporatorFormatError, match="overwrite"):
        async for _ in gen:
            pass  # pragma: no cover — guard should raise on first iteration
