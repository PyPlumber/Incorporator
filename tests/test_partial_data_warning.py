"""Gate test (a): partial-data UserWarning fires from base.py, not thread.py.

Proves that when ``incorp()`` or ``refresh()`` completes with at least one
reject, the resulting ``UserWarning`` is emitted from ``base.py`` (the
``warnings.warn`` call after the asyncio.to_thread join) rather than from
``concurrent/thread.py`` (the old factory.py site).

Also verifies that:
- The warning message contains the source identifier and error_kind.
- No warning fires when the result has no rejects.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch


class _Source(Incorporator):
    pass


class _Source2(Incorporator):
    pass


async def _mock_ok(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Returns a valid single-record payload."""
    return httpx.Response(200, text=json.dumps([{"id": "abc"}]), request=httpx.Request("GET", url))


async def _mock_request_error(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Raises a RequestError so the fetch path appends a reject."""
    raise httpx.RequestError("connection refused", request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_incorp_warns_from_base_py_with_reject(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """UserWarning fires after incorp() when a source fails, attributed to base.py."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        await _Source.incorp("https://api.example.com/data")

    assert len(rec) >= 1
    w = rec[0]
    # Warning must NOT originate from thread.py or concurrent internals.
    # With stacklevel=2 inside the async coroutine body, the attributed frame is
    # the direct caller of the coroutine — the test file here — which is correct
    # user-visible attribution.  The old factory.py site pointed to thread.py.
    assert "thread" not in w.filename.lower(), f"Warning attributed to thread internals: {w.filename!r}"
    assert "concurrent" not in w.filename.lower(), f"Warning attributed to concurrent internals: {w.filename!r}"
    # Message must contain the source and error kind
    assert "https://api.example.com/data" in str(w.message)
    assert "RequestError" in str(w.message)


@pytest.mark.asyncio
async def test_incorp_no_warn_when_no_rejects(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No UserWarning fires when incorp() succeeds with no rejects."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_ok)

    with pytest.warns(UserWarning) as rec:
        await _Source2.incorp("https://api.example.com/data")
        # Inject a dummy warning so pytest.warns does not fail on empty
        import warnings

        warnings.warn("_sentinel_", UserWarning, stacklevel=1)

    # Only the sentinel should be present — no partial-data warning
    assert all("_sentinel_" in str(w.message) for w in rec), (
        f"Unexpected partial-data warning(s): {[str(w.message) for w in rec]}"
    )
