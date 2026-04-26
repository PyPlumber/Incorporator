"""Integration test for the refresh() Re-Hydration API."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.converters import calc, flt


class LiveStock(Incorporator): pass


# --- MOCK NETWORK SETUP ---
# We use a global counter to simulate an API where the data changes over time
call_counter = 0


async def mock_live_ticker(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks a stock market API where the price updates on the second call."""
    global call_counter
    call_counter += 1

    if call_counter == 1:
        # STATE A: Initial Market Open
        payload = {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "current_price": "150.00",  # String that needs parsing
            "status": "Market Open"
        }
    else:
        # STATE B: Market Update (Price surged!)
        payload = {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "current_price": "165.50",
            "status": "Market Active"
        }

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps([payload]), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_stateful_refresh_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves refresh() re-fetches data and correctly applies the ETL pipeline again."""
    global call_counter
    call_counter = 0  # Reset for test isolation

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_live_ticker)
    BASE_URL = "https://finance.api.com/ticker/aapl"

    # ==========================================
    # PHASE 1: INITIAL INCORP (State A)
    # ==========================================
    stock_a = await LiveStock.incorp(
        inc_url=BASE_URL,
        inc_code="symbol",
        inc_name="company_name",
        conv_dict={
            "current_price": calc(float, default=0.0, type=flt)
        }
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
        conv_dict={
            "current_price": calc(float, default=0.0, type=flt)
        }
    )

    assert not isinstance(stock_b, list)

    # PROVE THE IDENTITY IS MAINTAINED
    assert stock_b.inc_code == "AAPL"

    # PROVE THE DATA UPDATED
    assert stock_b.status == "Market Active"

    # PROVE THE ETL PIPELINE FIRED AGAIN (It parsed the new string into a float)
    assert stock_b.current_price == 165.5