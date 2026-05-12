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
