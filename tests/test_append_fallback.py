"""Regression tests for the stream/fjord append-fallback contract.

The senior-level audit found that pre-fix, append-rejected formats
(Parquet / Excel / XML / JSON / Feather / ORC) either crashed mid-pipeline
in chunked mode OR silently clobbered in stateful / fjord modes.  The
fix introduces ``supports_append()`` + a per-tick resolver that:

  * On append-friendly formats (NDJSON / CSV / SQLite / Avro):
    chunk 1/tick 1 = handler default ('replace'), subsequent ticks =
    ``if_exists='append'`` so output accumulates.
  * On monolithic formats: every tick uses ``if_exists='replace'`` so
    the file always holds the latest snapshot.
  * On paginated CHUNKED mode with a monolithic target: fail loud
    before the pipeline starts, since chunks are NEW data and replace
    would lose every chunk but the last.

These tests pin all four behaviour branches.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.handlers._base import APPEND_FRIENDLY_FORMATS, supports_append
from incorporator.io.formats import FormatType
from incorporator.observability.pipeline._shared import (
    _enrich_and_load,
    _resolve_if_exists_for_export,
)


# ==========================================
# supports_append() contract
# ==========================================


def test_supports_append_includes_streaming_formats() -> None:
    """NDJSON / CSV / TSV / PSV / SQLite / Avro all support append."""
    for fmt in (
        FormatType.NDJSON,
        FormatType.CSV,
        FormatType.TSV,
        FormatType.PSV,
        FormatType.SQLITE,
        FormatType.AVRO,
    ):
        assert supports_append(fmt), f"{fmt} should be append-friendly"
        assert fmt in APPEND_FRIENDLY_FORMATS


def test_supports_append_rejects_monolithic_formats() -> None:
    """Parquet / Feather / ORC / Excel / XML / JSON / HTML do NOT support append."""
    for fmt in (
        FormatType.PARQUET,
        FormatType.FEATHER,
        FormatType.ORC,
        FormatType.XLSX,
        FormatType.XML,
        FormatType.JSON,
        FormatType.HTML,
    ):
        assert not supports_append(fmt), f"{fmt} should NOT be append-friendly"


# ==========================================
# _resolve_if_exists_for_export() — the per-tick decision
# ==========================================


def test_resolver_user_override_always_wins() -> None:
    """Explicit if_exists from the user beats every other branch."""
    # Monolithic format + user says append → user wins (caller will see
    # the handler's IncorporatorFormatError, that's fine — they asked for it).
    assert (
        _resolve_if_exists_for_export("out.parquet", force_append=True, user_override="append")
        == "append"
    )
    # Append-friendly + user says replace → user wins.
    assert (
        _resolve_if_exists_for_export("out.ndjson", force_append=True, user_override="replace")
        == "replace"
    )


def test_resolver_first_tick_returns_none() -> None:
    """First tick (force_append=False) leaves handler defaults in place."""
    assert _resolve_if_exists_for_export("out.parquet", force_append=False, user_override=None) is None
    assert _resolve_if_exists_for_export("out.ndjson", force_append=False, user_override=None) is None


def test_resolver_subsequent_tick_append_friendly_returns_append() -> None:
    """Subsequent ticks on NDJSON / CSV / SQLite / Avro inject 'append'."""
    for path in ("out.ndjson", "out.csv", "out.tsv", "out.psv", "out.sqlite", "out.avro"):
        assert (
            _resolve_if_exists_for_export(path, force_append=True, user_override=None) == "append"
        ), f"{path} should append on subsequent ticks"


def test_resolver_subsequent_tick_monolithic_returns_replace() -> None:
    """Subsequent ticks on monolithic formats downgrade to 'replace' so the
    file always holds the latest snapshot rather than crashing."""
    for path in ("out.parquet", "out.feather", "out.orc", "out.xlsx", "out.xml", "out.json"):
        assert (
            _resolve_if_exists_for_export(path, force_append=True, user_override=None) == "replace"
        ), f"{path} should replace on subsequent ticks"


# ==========================================
# Integration via _enrich_and_load (used by chunked engine)
# ==========================================


@pytest.mark.asyncio
async def test_enrich_and_load_append_friendly_format_appends() -> None:
    """NDJSON + force_append=True actually injects if_exists='append'."""
    cls = MagicMock()
    cls.export = AsyncMock()
    await _enrich_and_load(
        cls,
        [{"id": 1}],
        refresh_params=None,
        export_params={"file_path": "/tmp/out.ndjson"},
        force_append=True,
    )
    cls.export.assert_awaited_once_with(
        instance=[{"id": 1}], file_path="/tmp/out.ndjson", if_exists="append"
    )


@pytest.mark.asyncio
async def test_enrich_and_load_monolithic_format_replaces() -> None:
    """Parquet + force_append=True downgrades to if_exists='replace' so the
    pipeline doesn't crash on tick 2."""
    cls = MagicMock()
    cls.export = AsyncMock()
    await _enrich_and_load(
        cls,
        [{"id": 1}],
        refresh_params=None,
        export_params={"file_path": "/tmp/out.parquet"},
        force_append=True,
    )
    cls.export.assert_awaited_once_with(
        instance=[{"id": 1}], file_path="/tmp/out.parquet", if_exists="replace"
    )


# ==========================================
# Chunked engine pre-flight (the only mode that should fail-fast)
# ==========================================


@pytest.mark.asyncio
async def test_chunked_engine_rejects_paginated_monolithic_target() -> None:
    """Paginated chunked + Parquet target → fail at call site, not on chunk 2.

    Pre-collapse the guard fired from inside ``_run_chunking_engine`` (so the
    traceback pointed at the async generator).  Post-collapse it lives at the
    ``stream()`` entry point via ``assert_engine_supported`` — same error
    message, friendlier traceback.  See ``tests/test_pipeline_dispatch.py``
    for the full decision matrix.
    """
    from incorporator import Incorporator

    class _StreamModel(Incorporator):
        inc_code: Any = None

    paginator = MagicMock()
    paginator.is_exhausted = False

    with pytest.raises(IncorporatorFormatError, match="(?i)would lose data"):
        async for _ in _StreamModel.stream(
            incorp_params={"inc_url": "http://example.invalid", "inc_page": paginator},
            refresh_params=None,
            export_params={"file_path": "/tmp/out.parquet"},
        ):
            pass


@pytest.mark.asyncio
async def test_chunked_engine_allows_singleshot_monolithic_target() -> None:
    """Single-shot chunked (no paginator) + Parquet target = OK.  Only one
    chunk fires, so monolithic targets are safe."""
    from incorporator.observability.pipeline.chunked import _run_chunking_engine

    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])
    cls.export = AsyncMock()

    # No paginator → single-shot.  No raise expected.
    waves = []
    async for wave in _run_chunking_engine(
        cls=cls,
        incorp_params={},
        refresh_params=None,
        export_params={"file_path": "/tmp/out.parquet"},
        poll_interval=None,
        paginator=None,
    ):
        waves.append(wave)
    assert len(waves) == 1
    assert waves[0].failed_sources == []
