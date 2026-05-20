"""Regression tests for ``Incorporator.refresh()`` — three resolution modes.

The motivating bug: ``base.py:645`` previously routed *every* refresh call
through ``router.resolve_declarative_routing()``, even when there was no
URL template or POST tokens to inject.  Pure in-state refresh
(``await Class.refresh()`` with no args) raised ``inc_parent extraction
yielded no valid URLs`` instead of falling through to the origin-URL
fallback that reads ``cls.inc_url``.

These tests pin the guard down structurally:

1. **In-state mode**     — ``refresh()`` no-args re-fetches the single
                          URL the class was loaded from.
2. **Re-source mode**    — ``refresh("https://new-url")`` re-points the
                          registry at a brand-new origin.
3. **Targeted mode**     — ``refresh(instance=[a, b])`` accepts the API
                          form and re-fetches without raising.
4. **Declarative mode**  — ``refresh(new_url="...{}", inc_child="id")``
                          template injection still routes through
                          ``resolve_declarative_routing``.

Framework note: ``Incorporator.inc_url`` is a ``ClassVar`` populated only
when the class was loaded from a **single** URL.  Multi-URL incorp and
per-instance origin tracking are out of scope for this regression suite
(see tutorial 5 for the documented limit).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch


@pytest.fixture(autouse=True)
def _clear_incorporator_registry() -> Any:
    """Belt-and-braces clear of the base Incorporator registry between tests.

    Per-subclass isolation is the production contract (see
    ``Incorporator.__init_subclass__``), so this fixture is normally a
    no-op — instances live in their own subclass ``inc_dict``.  The clear
    stays in case any test ever pokes ``Incorporator.inc_dict`` directly.
    """
    Incorporator.inc_dict.clear()
    yield
    Incorporator.inc_dict.clear()


def _stateful_mock(
    url_to_payloads: Dict[str, List[Any]],
) -> Callable[..., Any]:
    """Build a mock for ``fetch.execute_request`` that returns successive
    payloads per URL.

    ``url_to_payloads`` maps each URL to a list — every call to that URL
    pops the next payload off the front (last payload sticks for further
    calls).  Each entry is the raw body the framework will receive.

    Counter state lives inside the closure so tests stay isolated under
    pytest-randomly.
    """
    call_counts: Dict[str, int] = {u: 0 for u in url_to_payloads}

    async def mock_execute(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        if url not in url_to_payloads:
            raise AssertionError(f"Unexpected URL hit by mock: {url!r}")
        idx = min(call_counts[url], len(url_to_payloads[url]) - 1)
        call_counts[url] += 1
        payload = url_to_payloads[url][idx]
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    mock_execute.call_counts = call_counts  # type: ignore[attr-defined]
    return mock_execute


# ==========================================
# 1. IN-STATE MODE — the bug-fix regression
# ==========================================


@pytest.mark.asyncio
async def test_refresh_in_state_mode_rehits_class_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``await Cls.refresh()`` no-args must re-fetch from ``cls.inc_url``.

    Pre-fix behaviour: this raised
        [Cls] inc_parent extraction yielded no valid URLs

    because base.py:645 routed unconditionally through declarative routing,
    which tried to ``str(instance).startswith("http")`` each Pydantic
    model and failed.  This test confirms the guard now lets in-state
    refresh fall through to the origin-URL fallback at line 656-660.
    """

    class LiveStock(Incorporator):
        pass

    URL = "https://finance.api.com/ticker/all"

    mock = _stateful_mock(
        {
            URL: [
                # initial incorp returns two stocks
                [
                    {"symbol": "AAPL", "price": 150.0},
                    {"symbol": "MSFT", "price": 320.0},
                ],
                # refresh returns the same stocks with mutated prices
                [
                    {"symbol": "AAPL", "price": 165.5},
                    {"symbol": "MSFT", "price": 335.7},
                ],
            ],
        }
    )
    monkeypatch.setattr(fetch, "execute_request", mock)

    # Initial load — single URL, multi-record.  Hold the list to keep
    # the WeakValueDictionary entries alive across the refresh call.
    initial = await LiveStock.incorp(inc_url=URL, inc_code="symbol")
    assert len(initial) == 2
    assert LiveStock.inc_dict["AAPL"].price == 150.0
    assert LiveStock.inc_dict["MSFT"].price == 320.0

    # IN-STATE REFRESH — no args.  Must re-hit the URL stored on cls.
    refreshed = await LiveStock.refresh()

    # The framework hit the URL once for incorp and once for refresh.
    assert mock.call_counts[URL] == 2  # type: ignore[attr-defined]
    assert LiveStock.inc_dict["AAPL"].price == 165.5
    assert LiveStock.inc_dict["MSFT"].price == 335.7

    # Returned object is the IncorporatorList wrapper.
    assert len(refreshed) == 2


# ==========================================
# 2. RE-SOURCE MODE
# ==========================================


@pytest.mark.asyncio
async def test_refresh_re_source_mode_repoints_registry_at_new_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``refresh("https://new-url")`` must hit the new URL instead of the old."""

    class Coin(Incorporator):
        pass

    OLD_URL = "https://api.example.com/v1/coins"
    NEW_URL = "https://api.example.com/v2/coins"

    mock = _stateful_mock(
        {
            OLD_URL: [[{"id": "btc", "price": 30000.0}, {"id": "eth", "price": 2000.0}]],
            NEW_URL: [[{"id": "btc", "price": 31500.0}, {"id": "eth", "price": 2100.0}]],
        }
    )
    monkeypatch.setattr(fetch, "execute_request", mock)

    initial = await Coin.incorp(inc_url=OLD_URL, inc_code="id")
    assert len(initial) == 2
    assert Coin.inc_dict["btc"].price == 30000.0
    assert Coin.inc_dict["eth"].price == 2000.0
    assert Coin.inc_url == OLD_URL                         # baseline

    # RE-SOURCE — pass a brand-new URL string as the instance arg.
    refreshed = await Coin.refresh(NEW_URL)

    assert mock.call_counts[NEW_URL] == 1  # type: ignore[attr-defined]
    assert mock.call_counts[OLD_URL] == 1  # type: ignore[attr-defined]
    assert Coin.inc_dict["btc"].price == 31500.0
    assert Coin.inc_dict["eth"].price == 2100.0
    assert len(refreshed) == 2

    # Origin tracking must now reflect the new URL so subsequent in-state
    # refreshes hit the new source.  The bug this assertion guards: pre-fix,
    # cls.inc_url stayed pinned to OLD_URL even after a re-source, so a later
    # no-args refresh() would silently re-fetch the wrong endpoint.
    assert Coin.inc_url == NEW_URL


# ==========================================
# 3. TARGETED MODE — API shape accepts the call
# ==========================================


@pytest.mark.asyncio
async def test_refresh_targeted_mode_accepts_instance_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``refresh(instance=[a, b])`` must accept the API form and re-fetch.

    Note: on a single-URL registry the framework dedups to one HTTP call
    and re-applies to the full inc_dict (per-instance origin tracking
    isn't currently a framework feature).  This test verifies the API
    shape doesn't raise and that the registry reflects the mutation.
    """

    class Ticker(Incorporator):
        pass

    URL = "https://api.example.com/tickers"
    mock = _stateful_mock(
        {
            URL: [
                [
                    {"symbol": "AAPL", "price": 100.0},
                    {"symbol": "MSFT", "price": 200.0},
                    {"symbol": "GOOG", "price": 300.0},
                ],
                [
                    {"symbol": "AAPL", "price": 110.0},
                    {"symbol": "MSFT", "price": 220.0},
                    {"symbol": "GOOG", "price": 330.0},
                ],
            ],
        }
    )
    monkeypatch.setattr(fetch, "execute_request", mock)

    initial = await Ticker.incorp(inc_url=URL, inc_code="symbol")
    assert len(initial) == 3
    aapl = Ticker.inc_dict["AAPL"]
    msft = Ticker.inc_dict["MSFT"]

    # TARGETED — refresh AAPL + MSFT specifically.  The framework's
    # single-URL fallback re-hits the URL once; the inc_dict reflects
    # the mutation for every record on the second response.
    refreshed = await Ticker.refresh(instance=[aapl, msft])

    assert mock.call_counts[URL] == 2  # type: ignore[attr-defined]
    assert Ticker.inc_dict["AAPL"].price == 110.0
    assert Ticker.inc_dict["MSFT"].price == 220.0
    # Returned list reflects whatever the framework decided to refresh —
    # at minimum 2, and the API call didn't raise.
    assert len(refreshed) >= 2


# ==========================================
# 4. DECLARATIVE MODE — regression guard for the original guard fix
# ==========================================


@pytest.mark.asyncio
async def test_refresh_declarative_template_injection_still_works(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``refresh(new_url="...{}", inc_child="id")`` must still route through
    ``resolve_declarative_routing``.

    This is the regression guard: the in-state fix narrowed the guard on
    base.py:645 to require ``target_url or child_path``.  Both are
    present here, so the declarative router must still fire.
    """

    class Item(Incorporator):
        pass

    INITIAL_URL = "https://api.example.com/items-listing"
    DETAIL_TEMPLATE = "https://api.example.com/items/{}/detail"
    DETAIL_ALPHA = "https://api.example.com/items/alpha/detail"
    DETAIL_BETA = "https://api.example.com/items/beta/detail"

    mock = _stateful_mock(
        {
            INITIAL_URL: [
                [
                    {"id": "alpha", "name": "Alpha"},
                    {"id": "beta", "name": "Beta"},
                ],
            ],
            DETAIL_ALPHA: [[{"id": "alpha", "name": "Alpha", "detail": "DETAIL-ALPHA"}]],
            DETAIL_BETA: [[{"id": "beta", "name": "Beta", "detail": "DETAIL-BETA"}]],
        }
    )
    monkeypatch.setattr(fetch, "execute_request", mock)

    # Seed the registry from a listing endpoint.  Hold the result so
    # the WeakValueDictionary entries survive until refresh runs.
    initial = await Item.incorp(inc_url=INITIAL_URL, inc_code="id")
    assert len(initial) == 2
    assert "alpha" in Item.inc_dict
    assert "beta" in Item.inc_dict

    # DECLARATIVE REFRESH — URL template with `{}` + inc_child="id".
    # The router's GET branch sees "{}" and fans out one request per ID.
    refreshed = await Item.refresh(
        new_url=DETAIL_TEMPLATE,
        inc_child="id",
    )

    # Both detail URLs were hit exactly once.
    assert mock.call_counts[DETAIL_ALPHA] == 1  # type: ignore[attr-defined]
    assert mock.call_counts[DETAIL_BETA] == 1  # type: ignore[attr-defined]
    # The initial listing URL is NOT re-hit.
    assert mock.call_counts[INITIAL_URL] == 1  # type: ignore[attr-defined]
    assert len(refreshed) == 2
