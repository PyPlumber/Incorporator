"""Tests for ``child_incorp`` in ``incorporator/schema/factory.py``.

Covers the empty-parent short-circuit: when ``inc_parent=[]`` (or produces
zero extracted IDs), ``child_incorp`` must return an empty ``IncorporatorList``
with zero HTTP calls, without constructing the bogus ``{}``-template URL.

Also covers the non-empty routing paths (D7-07): GET-template parent-id
dedupe, ``http_method`` alias normalization, ``each()`` payload fan-out, and
``None``-leaf filtering during BFS drill.  These use a ``FakeCls``-style
capture fixture that stubs ``cls.incorp`` to record the kwargs
``child_incorp`` builds, rather than driving a live HTTP call.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from incorporator import Incorporator
from incorporator.list import IncorporatorList
from incorporator.schema import factory
from incorporator.schema.extractors import each


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


# ---------------------------------------------------------------------------
# FakeCls-capture fixture — stub cls.incorp to record kwargs (D7-07)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_incorp(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Stub ``_Rocket.incorp`` to record kwargs instead of driving a live HTTP call.

    ``child_incorp`` builds its final kwargs shape (URL list / payload_list /
    http_method) BEFORE delegating to ``cls.incorp(**kwargs)`` — patching
    ``incorp`` itself lets these tests assert on the routing decision without
    a real network round-trip.
    """
    mock_incorp = AsyncMock(return_value=IncorporatorList(_Rocket, []))
    monkeypatch.setattr(_Rocket, "incorp", mock_incorp)
    return mock_incorp


# ---------------------------------------------------------------------------
# Non-empty routing paths (D7-07)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_child_incorp_get_template_dedupes_parent_ids(fake_incorp: AsyncMock) -> None:
    """GET {}-template: duplicate parent IDs are deduplicated before URL construction.

    Exercises ``_deduplicate_extracted`` (incorporator/list.py) and the
    ``{}``-template branch in ``router.resolve_declarative_routing``.
    """
    parents = [_Rocket(rocket_id="r1"), _Rocket(rocket_id="r1"), _Rocket(rocket_id="r2")]

    await factory.child_incorp(
        _Rocket,
        inc_parent=parents,
        inc_url="https://api.example.com/rockets/{}",
        inc_child="rocket_id",
        inc_code="rocket_id",
    )

    kwargs = fake_incorp.call_args.kwargs
    assert kwargs["inc_url"] == [
        "https://api.example.com/rockets/r1",
        "https://api.example.com/rockets/r2",
    ], "duplicate r1 parent must be deduped to a single URL"


@pytest.mark.asyncio
async def test_child_incorp_method_alias_lowercase_post_normalized(fake_incorp: AsyncMock) -> None:
    """A lowercase ``method="post"`` alias normalizes to ``http_method="POST"``.

    Exercises the ``kwargs.pop("method", kwargs.pop("http_method", "GET")).upper()``
    lines in ``child_incorp``.
    """
    parents = [_Rocket(rocket_id="r1")]

    await factory.child_incorp(
        _Rocket,
        inc_parent=parents,
        inc_url="https://api.example.com/rockets",
        inc_child="rocket_id",
        inc_code="rocket_id",
        method="post",
        json_payload={"x": 1},
    )

    kwargs = fake_incorp.call_args.kwargs
    assert kwargs["http_method"] == "POST"
    assert "method" not in kwargs, "the alias key itself must not leak through to cls.incorp"


@pytest.mark.asyncio
async def test_child_incorp_non_string_method_falls_back_to_get(fake_incorp: AsyncMock) -> None:
    """A non-string ``method`` value falls back to ``"GET"`` rather than raising.

    Exercises the ``isinstance(raw_method, str)`` guard in ``child_incorp``.
    """
    parents = [_Rocket(rocket_id="r1")]

    await factory.child_incorp(
        _Rocket,
        inc_parent=parents,
        inc_url="https://api.example.com/rockets/{}",
        inc_child="rocket_id",
        inc_code="rocket_id",
        method=123,
    )

    kwargs = fake_incorp.call_args.kwargs
    assert kwargs["http_method"] == "GET"


@pytest.mark.asyncio
async def test_child_incorp_each_multiplies_single_url_to_payload_count(fake_incorp: AsyncMock) -> None:
    """each() fans a single base URL out to one entry per extracted parent ID.

    Exercises the POST/``each()`` branch in ``router.resolve_declarative_routing``:
    ``len(source_urls) == 1`` multiplies the URL list to match ``payload_list``.
    """
    parents = [_Rocket(rocket_id="r1"), _Rocket(rocket_id="r2"), _Rocket(rocket_id="r3")]

    await factory.child_incorp(
        _Rocket,
        inc_parent=parents,
        inc_url="https://api.example.com/decode",
        inc_child="rocket_id",
        inc_code="rocket_id",
        http_method="POST",
        json_payload={"id": each()},
    )

    kwargs = fake_incorp.call_args.kwargs
    assert kwargs["inc_url"] == ["https://api.example.com/decode"] * 3
    assert kwargs["payload_list"] == [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}]


@pytest.mark.asyncio
async def test_child_incorp_drill_filters_none_leaves(fake_incorp: AsyncMock) -> None:
    """BFS drill drops ``None`` leaves rather than emitting a broken URL segment.

    Exercises ``router.extract_parent_data``'s ``if val is not None`` guards:
    a parent with a ``None`` value at the drilled path is skipped entirely,
    not turned into a literal ``"None"`` URL segment.
    """
    dict_parents = [
        {"rocket_id": "r1"},
        {"rocket_id": None},
        {"rocket_id": "r3"},
    ]

    await factory.child_incorp(
        _Rocket,
        inc_parent=dict_parents,
        inc_url="https://api.example.com/rockets/{}",
        inc_child="rocket_id",
        inc_code="rocket_id",
    )

    kwargs = fake_incorp.call_args.kwargs
    assert kwargs["inc_url"] == [
        "https://api.example.com/rockets/r1",
        "https://api.example.com/rockets/r3",
    ], "the None-leaf parent must be dropped, not rendered as a literal 'None' segment"
