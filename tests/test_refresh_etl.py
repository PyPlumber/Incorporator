"""Integration test for the refresh() Re-Hydration API."""

import json
from typing import Any, Callable
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from incorporator import Incorporator
from incorporator.base import IncorporatorList
from incorporator.io import fetch
from incorporator.schema.converters import calc


def _make_live_ticker_mock() -> Callable[..., Any]:
    """Builds a stateful per-test mock simulating two consecutive API responses.

    State isolation: the call counter lives inside this closure rather than at
    module scope, so each test invocation gets its own counter. A module-level
    ``call_counter`` would leak across tests under pytest-randomly.
    """
    call_counter = 0

    async def mock_live_ticker(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        nonlocal call_counter
        call_counter += 1

        if call_counter == 1:
            payload = {
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "current_price": "150.00",  # String that needs parsing
                "status": "Market Open",
            }
        else:
            payload = {
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "current_price": "165.50",
                "status": "Market Active",
            }

        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps([payload]), request=req)

    return mock_live_ticker


@pytest.mark.asyncio
async def test_stateful_refresh_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves refresh() re-fetches data and correctly applies the ETL pipeline again."""

    class LiveStock(Incorporator):
        pass

    monkeypatch.setattr(fetch, "execute_request", _make_live_ticker_mock())
    BASE_URL = "https://finance.api.com/ticker/aapl"

    # ==========================================
    # PHASE 1: INITIAL INCORP (State A)
    # ==========================================
    stock_a = await LiveStock.incorp(
        inc_url=BASE_URL,
        inc_code="symbol",
        inc_name="company_name",
        conv_dict={"current_price": calc(float, default=0.0, target_type=float)},
    )

    # Framework auto-unwraps single arrays!
    assert not isinstance(stock_a, list)

    assert stock_a.inc_code == "AAPL"
    assert stock_a.status == "Market Open"
    assert stock_a.current_price == 150.0  # Converted to float perfectly

    # ==========================================
    # PHASE 2: THE REFRESH (State B)
    # ==========================================
    stock_b = await LiveStock.refresh(
        instance=stock_a,
        new_url=BASE_URL,
        inc_code="symbol",
        inc_name="company_name",
        conv_dict={"current_price": calc(float, default=0.0, target_type=float)},
    )

    assert not isinstance(stock_b, list)

    # PROVE THE IDENTITY IS MAINTAINED
    assert stock_b.inc_code == "AAPL"

    # PROVE THE DATA UPDATED
    assert stock_b.status == "Market Active"

    # PROVE THE ETL PIPELINE FIRED AGAIN (It parsed the new string into a float)
    assert stock_b.current_price == 165.5


@pytest.mark.asyncio
async def test_incorporator_list_state_carrier() -> None:
    """Verifies that inc_child_path persists on the returned list wrapper."""

    class DummyModel(Incorporator):
        pass

    # We mock the network engine so we only test the framework's internal state mechanism
    with patch("incorporator.io.fetch.fetch_concurrent_payloads", new_callable=AsyncMock) as mock_fetch:
        # Mock returning 2 empty dictionaries from the network
        mock_fetch.return_value = ([{}, {}], list())

        # Execute incorp and explicitly pass our extraction path
        result = await DummyModel.incorp(inc_url="https://mock.api", inc_child="Vehicle.VIN")

        # Verify the wrapper caught and retained the state!
        assert isinstance(result, IncorporatorList)
        assert result.inc_child_path == "Vehicle.VIN"


@pytest.mark.asyncio
async def test_refresh_replays_persisted_incorp_kwargs() -> None:
    """Regression — refresh() must replay incorp()'s params / headers / rec_path.

    The user's bug report on Tutorial 7: fjord's daemon called
    ``CoinGecko.refresh()`` with no args, and the resulting fetch hit
    ``https://api.coingecko.com/api/v3/coins/markets`` WITHOUT the
    ``?vs_currency=usd&per_page=100&page=1`` query string the seed used.
    CoinGecko returned 422.  The fix persists incorp()'s network kwargs
    on the class as ``_incorp_kwargs`` and refresh() replays them.
    """

    class CoinGecko(Incorporator):
        pass

    with patch("incorporator.io.fetch.fetch_concurrent_payloads", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = (
            [{"id": "bitcoin", "current_price": 60000.0}],
            [],
        )

        # SEED — pass network kwargs the same way the user would.
        await CoinGecko.incorp(
            inc_url="https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "per_page": 100, "page": 1},
            headers={"X-Custom": "from-seed"},
            rec_path="results.items",
            inc_code="id",
        )

        # The seed call should have forwarded params / headers / rec_path.
        seed_call = mock_fetch.await_args_list[0]
        assert seed_call.kwargs.get("params") == {"vs_currency": "usd", "per_page": 100, "page": 1}
        assert seed_call.kwargs.get("headers") == {"X-Custom": "from-seed"}
        assert seed_call.kwargs.get("rec_path") == "results.items"

        # REFRESH — no kwargs.  Must replay the seed's params / headers / rec_path
        # via cls._incorp_kwargs.  Pre-fix this hit the bare URL → 422.
        await CoinGecko.refresh()

        refresh_call = mock_fetch.await_args_list[1]
        assert refresh_call.kwargs.get("params") == {"vs_currency": "usd", "per_page": 100, "page": 1}, (
            "refresh() must replay params= from the original incorp() call; "
            f"got kwargs={refresh_call.kwargs}"
        )
        assert refresh_call.kwargs.get("headers") == {"X-Custom": "from-seed"}
        assert refresh_call.kwargs.get("rec_path") == "results.items"


@pytest.mark.asyncio
async def test_refresh_caller_kwargs_win_over_persisted() -> None:
    """User-supplied refresh kwargs override the persisted incorp() context.

    Confirms the precedence order: explicit refresh args win on key
    conflicts.  This is what lets a user opt out of the seed's params
    or supply a different rec_path on a specific refresh tick.
    """

    class Sample(Incorporator):
        pass

    with patch("incorporator.io.fetch.fetch_concurrent_payloads", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = ([{"id": 1}], [])

        await Sample.incorp(
            inc_url="https://example.com/data",
            params={"q": "seed-value"},
            inc_code="id",
        )

        # Refresh with an explicit params override — should win.
        await Sample.refresh(params={"q": "refresh-override"})

        refresh_call = mock_fetch.await_args_list[1]
        assert refresh_call.kwargs.get("params") == {"q": "refresh-override"}
