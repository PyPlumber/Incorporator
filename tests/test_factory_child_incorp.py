"""Tests for ``child_incorp`` in ``incorporator/schema/factory.py``.

Covers the empty-parent short-circuit: when ``inc_parent=[]`` (or produces
zero extracted IDs), ``child_incorp`` must return an empty ``IncorporatorList``
with zero HTTP calls, without constructing the bogus ``{}``-template URL.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from incorporator import Incorporator
from incorporator.list import IncorporatorList


# ---------------------------------------------------------------------------
# Module-level subclass so the test is repeatable without class re-definition
# ---------------------------------------------------------------------------


class _Rocket(Incorporator):
    """Thin Incorporator subclass for child-drill tests."""

    rocket_id: str = ""
    name: str = ""


# ---------------------------------------------------------------------------
# Empty-parent short-circuit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_child_incorp_empty_parent_returns_empty_list_no_http(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Empty inc_parent produces an empty IncorporatorList with zero HTTP calls.

    Proves the short-circuit added to ``child_incorp``: when ``extracted_data``
    is empty after BFS drill-down (here because ``inc_parent=[]``), the function
    returns immediately without calling ``cls.incorp()`` and therefore without
    issuing any HTTP request.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    mock_execute = AsyncMock(name="execute_request_should_not_be_called")
    monkeypatch.setattr(fetch, "execute_request", mock_execute)

    result = await _Rocket.incorp(
        inc_url="https://api.spacexdata.com/v4/rockets/{}",
        inc_parent=[],
        inc_child="rocket_id",
        inc_code="rocket_id",
    )

    assert isinstance(result, IncorporatorList), "Result must be an IncorporatorList"
    assert len(result) == 0, "Empty parent → zero child instances"
    assert result.rejects == [], "No rejects for an empty-parent short-circuit"
    mock_execute.assert_not_called()


@pytest.mark.asyncio
async def test_child_incorp_empty_parent_no_bogus_url(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """The literal ``{}`` template URL is never issued when the parent is empty.

    Regression guard: before the fix, ``child_incorp`` fell through to
    ``cls.incorp(**kwargs)`` with the raw template URL intact, producing
    requests to ``…/rockets/{}`` and misattributed warnings.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    called_urls: list[str] = []

    async def _spy(url: str, *args: Any, **kwargs: Any) -> Any:
        called_urls.append(url)
        raise AssertionError(f"execute_request must not be called; got URL={url!r}")

    monkeypatch.setattr(fetch, "execute_request", _spy)

    await _Rocket.incorp(
        inc_url="https://api.example.com/items/{}",
        inc_parent=[],
        inc_child="id",
        inc_code="id",
    )

    assert called_urls == [], f"Unexpected HTTP calls: {called_urls}"
