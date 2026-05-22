"""Tests for the paginator-level Penstock throttle (A-F-9).

The :class:`AsyncPaginator` base owns a ``penstock`` field that gates
every page-yield (web) or chunk-yield (local).  These tests pin the
contract by exercising one canonical case per shape:

* **Local throttle works at all** — pre-A-F-9, ``SQLitePaginator`` had
  zero throttle path.  This test confirms a paginator-level
  :class:`SustainedPenstock` actually slows chunk yields.
* **Web composition is additive** — paginator-level throttle stacks
  with the host-level :class:`BoundPenstock` registered via
  :func:`register_host_penstock`.  Both must permit before a page
  fires; the slower one wins.
* **Default behaviour is unchanged** — a paginator constructed without
  the ``penstock`` kwarg gets a :class:`NullPenstock` default with a
  zero-cost ``acquire()``.  Pre-A-F-9 tests must keep passing.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import httpx
import pytest

from incorporator import (
    NextUrlPaginator,
    NullPenstock,
    SQLitePaginator,
    SustainedPenstock,
    register_host_penstock,
)
from incorporator.io import fetch


# ---------------------------------------------------------------------------
# Test 1 — local paginator throttle works at all
# ---------------------------------------------------------------------------


def _build_test_sqlite(tmp_path: Path, row_count: int) -> Path:
    """Create a tiny SQLite db with ``row_count`` rows for the throttle test."""
    db_path = tmp_path / "throttle_test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t (id, name) VALUES (?, ?)", [(i, f"row_{i}") for i in range(row_count)])
    conn.commit()
    conn.close()
    return db_path


@pytest.mark.asyncio
async def test_sqlite_paginator_with_penstock_throttles_chunks(tmp_path: Path) -> None:
    """SQLitePaginator with a 4 r/s SustainedPenstock takes ≥ 0.5s to emit 4 chunks.

    400 rows / chunk_size=100 = 4 chunks.  At 4 chunks/sec the minimum
    gap between chunks is 0.25s.  The first chunk yields immediately
    (no last_consumed_at yet); chunks 2–4 each wait 0.25s.  Expected
    floor: ~0.75s elapsed (3 inter-chunk waits × 0.25s).  Floor at
    0.5s leaves headroom for OS scheduling jitter.

    Pre-A-F-9 this test would complete in milliseconds — the paginator
    iterated at disk speed regardless of the user's intent.
    """
    db_path = _build_test_sqlite(tmp_path, row_count=400)
    paginator = SQLitePaginator(
        db_path=str(db_path),
        sql_query="SELECT * FROM t",
        chunk_size=100,
        penstock=SustainedPenstock(rate_per_sec=4.0),
    )

    t0 = time.perf_counter()
    chunks: List[List[Dict[str, Any]]] = []
    async for chunk in paginator.paginate("unused"):
        # paginate() yields List[Dict] for the local subclasses, but its
        # type-union annotation includes str/bytes — cast for mypy peace.
        assert isinstance(chunk, list)
        chunks.append(chunk)
    elapsed = time.perf_counter() - t0

    assert len(chunks) == 4, f"expected 4 chunks of 100 rows each; got {len(chunks)}"
    assert sum(len(c) for c in chunks) == 400
    assert elapsed >= 0.5, (
        f"SQLitePaginator with SustainedPenstock(4.0) emitted 4 chunks "
        f"in {elapsed:.2f}s — should be ≥ 0.5s (3 inter-chunk waits at "
        "0.25s each).  Suggests _acquire_penstock isn't being called "
        "before yield, or local paginators bypass it."
    )


# ---------------------------------------------------------------------------
# Test 2 — web paginator composes additively with host-level throttle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_paginator_penstock_composes_with_host_throttle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-paginator SustainedPenstock(2.0) + host SustainedPenstock(10.0) → effective 2 r/s.

    The slower throttle wins.  We attach the paginator-level penstock
    at 2 r/s and the host-level at 10 r/s, then time 3 page fetches.
    At 2 r/s the minimum gap is 0.5s; the first page is permitted
    immediately and pages 2–3 wait 0.5s each.  Floor at 0.7s gives
    headroom for scheduler noise.

    Also asserts that ``execute_request``'s ``rate_limiter`` parameter
    was non-None on every call — verifies the host-level path is
    still wired and the paginator-level throttle didn't bypass it.
    """
    # 10 r/s host throttle — should be the looser of the two.
    register_host_penstock("test.example.com", SustainedPenstock(rate_per_sec=10.0))

    # Mock execute_request: return a tiny response with a "next" link
    # that points to the same path, then signal exhaustion after 3 calls.
    call_count = 0

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        """Return a 1-row response with a 'next' path pointer."""
        nonlocal call_count
        call_count += 1
        # NextUrlPaginator walks ``response.json()['next']`` by default;
        # exhaust after the third page by returning next=None.
        next_link = "https://test.example.com/page" if call_count < 3 else None
        # Validate the rate_limiter param threaded through every call.
        assert kwargs.get("rate_limiter") is not None, (
            f"call #{call_count}: rate_limiter must be threaded through "
            "even when the paginator has its own penstock — both throttles "
            "should apply."
        )
        payload = json.dumps({"items": [{"id": call_count}], "next": next_link})
        return httpx.Response(200, text=payload, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    paginator = NextUrlPaginator(
        "next",
        penstock=SustainedPenstock(rate_per_sec=2.0),
    )
    # Bind the paginator's fetch_func the same way Incorporator.stream would.
    async with httpx.AsyncClient() as client:
        async def bound_fetch(url: str, request_params: Any = None, **_: Any) -> httpx.Response:
            return await fetch.execute_request(
                url=url,
                client=client,
                rate_limiter=AsyncMock(acquire=AsyncMock()),  # simulate the host BoundPenstock
            )

        paginator.fetch_func = bound_fetch

        t0 = time.perf_counter()
        pages = [page async for page in paginator.paginate("https://test.example.com/page")]
        elapsed = time.perf_counter() - t0

    assert len(pages) == 3, f"expected 3 pages; got {len(pages)}"
    assert call_count == 3
    assert elapsed >= 0.7, (
        f"NextUrlPaginator with SustainedPenstock(2.0) emitted 3 pages "
        f"in {elapsed:.2f}s — should be ≥ 0.7s (2 inter-page waits at "
        f"0.5s each).  Either the paginator-level acquire isn't firing "
        "before _fetch, or it's being short-circuited."
    )


# ---------------------------------------------------------------------------
# Test 3 — default behaviour unchanged (NullPenstock is zero-cost)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_paginator_unchanged_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NextUrlPaginator() with no ``penstock=`` kwarg behaves as it did pre-A-F-9.

    5 mocked-instant pages should complete in well under 100ms — the
    :class:`NullPenstock` default's ``acquire()`` is a literal
    early-return.  If this test starts failing, the
    :func:`_acquire_penstock` helper grew non-zero overhead.
    """
    call_count = 0

    async def instant_mock(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        next_link = f"https://example.com/page{call_count + 1}" if call_count < 5 else None
        payload = json.dumps({"items": [{"id": call_count}], "next": next_link})
        return httpx.Response(200, text=payload, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", instant_mock)

    paginator = NextUrlPaginator("next")
    # Confirm the default penstock is NullPenstock — the field always
    # exists post-A-F-9; the question is just what's there by default.
    assert isinstance(paginator.penstock, NullPenstock)

    async with httpx.AsyncClient() as client:
        async def bound_fetch(url: str, request_params: Any = None, **_: Any) -> httpx.Response:
            return await fetch.execute_request(
                url=url,
                client=client,
                rate_limiter=None,
            )

        paginator.fetch_func = bound_fetch

        t0 = time.perf_counter()
        pages = [page async for page in paginator.paginate("https://example.com/page1")]
        elapsed = time.perf_counter() - t0

    assert len(pages) == 5
    assert elapsed < 0.1, (
        f"Default-penstock paginator took {elapsed:.3f}s for 5 mocked pages — "
        "should be < 100ms.  NullPenstock.acquire is supposed to be a no-op."
    )
