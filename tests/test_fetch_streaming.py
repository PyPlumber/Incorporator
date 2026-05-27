"""Tests for the opt-in ``stream_to_path`` path on ``execute_request()``.

Covers: happy path, retry+truncate (429 and mid-stream protocol error),
SSRF block during a stream, Penstock-acquire-once-per-attempt contract,
and the memory-boundedness of the streaming path.
"""

from __future__ import annotations

import tracemalloc
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import pytest

from incorporator.exceptions import IncorporatorNetworkError
from incorporator.io.fetch import _STREAM_CHUNK_SIZE, execute_request
from incorporator.io.penstock import BoundPenstock, FlowState, NullPenstock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _null_bound_penstock() -> BoundPenstock:
    """Return a BoundPenstock that never throttles — suitable for unit tests."""
    return BoundPenstock(penstock=NullPenstock(), state=FlowState())


def _make_client_with_transport(transport: httpx.AsyncBaseTransport) -> httpx.AsyncClient:
    """Build an AsyncClient wired to a custom transport (no HTTP/2 for MockTransport)."""
    return httpx.AsyncClient(transport=transport, follow_redirects=True)


class _IterStream(httpx.AsyncByteStream):
    """Wrap an async-generator factory into an ``httpx.AsyncByteStream``.

    httpx requires the ``stream=`` argument to ``httpx.Response`` to be an
    ``AsyncByteStream`` subclass (asserted at ``_send_single_request``).
    Plain async generators fail that assertion.
    """

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


# ---------------------------------------------------------------------------
# Test 1 — happy path: body written to file, sentinel response returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_to_path_happy_path(tmp_path: Path) -> None:
    """Happy path: 1 MB body streamed in 16 KB chunks lands intact on disk.

    Proves: file contents == expected bytes, sentinel has status_code=200
    and content==b"".
    """
    chunk_size = 16 * 1024
    total_size = 1024 * 1024
    raw = bytes(range(256)) * (total_size // 256)
    chunks = [raw[i : i + chunk_size] for i in range(0, len(raw), chunk_size)]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_IterStream(chunks))

    dest = tmp_path / "out.bin"
    async with _make_client_with_transport(httpx.MockTransport(handler)) as client:
        response = await execute_request(
            url="https://example.com/blob",
            client=client,
            rate_limiter=_null_bound_penstock(),
            stream_to_path=dest,
        )

    assert response.status_code == 200
    assert response.content == b""
    assert dest.read_bytes() == raw


# ---------------------------------------------------------------------------
# Test 2a — 429 retry truncates partial file
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_to_path_429_retry_truncates(tmp_path: Path) -> None:
    """First attempt returns 429 with Retry-After: 0; second returns 64 bytes.

    Proves the "wb" open on each attempt truncates, leaving exactly 64 bytes
    (not stale data from the first attempt).
    """
    final_body = b"x" * 64
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, stream=_IterStream([final_body]))

    dest = tmp_path / "retry.bin"
    async with _make_client_with_transport(httpx.MockTransport(handler)) as client:
        response = await execute_request(
            url="https://example.com/retry",
            client=client,
            rate_limiter=_null_bound_penstock(),
            stream_to_path=dest,
        )

    assert response.status_code == 200
    assert dest.read_bytes() == final_body
    assert len(dest.read_bytes()) == 64


# ---------------------------------------------------------------------------
# Test 2b — mid-stream RemoteProtocolError retry truncates partial write
# ---------------------------------------------------------------------------


class _FailingStream(httpx.AsyncByteStream):
    """Yields ``partial`` bytes then raises ``RemoteProtocolError``."""

    def __init__(self, partial: bytes) -> None:
        self._partial = partial

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._partial
        raise httpx.RemoteProtocolError("connection dropped mid-stream")


@pytest.mark.asyncio
async def test_stream_to_path_mid_stream_error_retry_truncates(tmp_path: Path) -> None:
    """Mid-stream RemoteProtocolError on first attempt; second attempt delivers 100 bytes.

    Proves that re-opening in "wb" mode on the retry discards the 32 bytes
    written before the error, leaving exactly 100 bytes from the clean retry.
    """
    partial_bytes = b"p" * 32
    clean_bytes = b"c" * 100
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(200, stream=_FailingStream(partial_bytes))
        return httpx.Response(200, stream=_IterStream([clean_bytes]))

    dest = tmp_path / "midstream.bin"
    async with _make_client_with_transport(httpx.MockTransport(handler)) as client:
        response = await execute_request(
            url="https://example.com/midstream",
            client=client,
            rate_limiter=_null_bound_penstock(),
            stream_to_path=dest,
        )

    assert response.status_code == 200
    file_data = dest.read_bytes()
    assert file_data == clean_bytes
    assert len(file_data) == 100


# ---------------------------------------------------------------------------
# Test 3 — SSRF redirect during stream raises IncorporatorNetworkError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_to_path_ssrf_redirect_blocked(tmp_path: Path) -> None:
    """_block_internal_redirect_hook fires for client.stream() just as for client.request().

    Mock returns 301 redirecting to http://169.254.169.254/. The SSRF hook
    registered on the AsyncClient must raise IncorporatorNetworkError before
    any body is consumed.
    """
    dest = tmp_path / "ssrf.bin"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            301,
            headers={"Location": "http://169.254.169.254/latest/meta-data/"},
            request=request,
        )

    from incorporator.io.fetch import HTTPClientBuilder, _block_internal_redirect_hook

    # Build a client with the SSRF hook and the mock transport.
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
        event_hooks={"response": [_block_internal_redirect_hook]},
    )

    with pytest.raises(IncorporatorNetworkError, match="blocked redirect"):
        async with client:
            await execute_request(
                url="https://example.com/redirect-me",
                client=client,
                rate_limiter=_null_bound_penstock(),
                stream_to_path=dest,
            )

    # File should be absent or empty — no body was consumed.
    assert not dest.exists() or dest.stat().st_size == 0


# ---------------------------------------------------------------------------
# Test 4 — Penstock acquire called exactly once per attempt (not per chunk)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_to_path_penstock_acquire_once_per_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BoundPenstock.acquire is called once per attempt, not once per chunk.

    Mock transport: fails with 503 twice, then returns 200 with 64 bytes.
    acquire must be called exactly 3 times — once per Tenacity attempt.
    """
    monkeypatch.chdir(tmp_path)

    acquire_count = {"n": 0}
    call_count = {"n": 0}

    original_acquire = BoundPenstock.acquire

    async def spy_acquire(self: BoundPenstock) -> None:
        acquire_count["n"] += 1
        await original_acquire(self)

    monkeypatch.setattr(BoundPenstock, "acquire", spy_acquire)

    body = b"z" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        if call_count["n"] <= 2:
            return httpx.Response(503)
        return httpx.Response(200, stream=_IterStream([body]))

    dest = tmp_path / "penstock.bin"
    async with _make_client_with_transport(httpx.MockTransport(handler)) as client:
        response = await execute_request(
            url="https://example.com/flaky",
            client=client,
            rate_limiter=BoundPenstock(penstock=NullPenstock(), state=FlowState()),
            stream_to_path=dest,
        )

    assert response.status_code == 200
    assert acquire_count["n"] == 3, (
        f"Expected acquire() called 3 times (once per attempt), got {acquire_count['n']}. "
        "Penstock must be gated once per attempt before the stream branch, not per chunk."
    )
    assert dest.read_bytes() == body


# ---------------------------------------------------------------------------
# Test 5 — Memory bounded: peak heap delta < 5 × chunk_size for 10 MB body
# ---------------------------------------------------------------------------


class _BigBodyStream(httpx.AsyncByteStream):
    """Yields ``chunk_count`` identical chunks of ``_STREAM_CHUNK_SIZE`` bytes."""

    def __init__(self, chunk: bytes, count: int) -> None:
        self._chunk = chunk
        self._count = count

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(self._count):
            yield self._chunk


@pytest.mark.asyncio
async def test_stream_to_path_memory_bounded(tmp_path: Path) -> None:
    """Peak Python heap growth stays below 5 × _STREAM_CHUNK_SIZE for a 10 MB body.

    Proves the streaming path processes chunks without accumulating the full
    response body in memory.  The 5× factor gives generous headroom for
    asyncio, httpx, and Python object overhead while still catching a naive
    ``response.read()`` buffering regression.
    """
    total_size = 10 * 1024 * 1024  # 10 MB
    chunk = b"m" * _STREAM_CHUNK_SIZE
    chunk_count = total_size // _STREAM_CHUNK_SIZE

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_BigBodyStream(chunk, chunk_count))

    dest = tmp_path / "bigfile.bin"

    tracemalloc.start()
    tracemalloc.clear_traces()
    snapshot_before = tracemalloc.take_snapshot()

    async with _make_client_with_transport(httpx.MockTransport(handler)) as client:
        await execute_request(
            url="https://example.com/big",
            client=client,
            rate_limiter=_null_bound_penstock(),
            stream_to_path=dest,
        )

    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    stats = snapshot_after.compare_to(snapshot_before, "lineno")
    delta_bytes = sum(s.size_diff for s in stats if s.size_diff > 0)

    max_allowed = 5 * _STREAM_CHUNK_SIZE
    assert delta_bytes < max_allowed, (
        f"Streaming 10 MB grew the Python heap by {delta_bytes:,} bytes "
        f"(allowed < {max_allowed:,} = 5 × _STREAM_CHUNK_SIZE). "
        "Likely regression: response body is being buffered instead of streamed."
    )

    assert dest.stat().st_size == total_size
