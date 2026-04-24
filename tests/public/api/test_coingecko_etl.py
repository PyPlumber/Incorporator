"""Integration test for testing the call_lim boundary logic and auto-pagination (CoinGecko API style)."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator


# --- EXPLICIT SUBCLASSING ---
class Coin(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_infinite_coingecko_api(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks an infinitely paginating CoinGecko markets endpoint without Link headers."""

    # Extract the page number from the URL
    page_str = url.split("page=")[-1] if "page=" in url else "1"
    page = int(page_str)

    # Return exactly ONE coin per page ad infinitum
    payload = [{
        "id": f"coin_{page}",
        "symbol": f"C{page}",
        "name": f"Test Coin {page}",
        "current_price": 50000.0 / page
    }]

    # CRITICAL: We explicitly DO NOT return an RFC5988 Link header.
    # This forces the framework to rely entirely on the new _AutoURLPaginator heuristic!
    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---
@pytest.mark.asyncio
async def test_coingecko_call_lim_pagination_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests that a paginating API is forcefully halted exactly at `call_lim` pages."""

    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_infinite_coingecko_api)
    TARGET_LIMIT = 7

    # Fetch coins, utilizing the Zero-Boilerplate heuristic paginator and call_lim!
    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&page=1",
        inc_code="id",
        inc_name="name",
        paginate=True,
        call_lim=TARGET_LIMIT
    )

    # Validate the result list length exactly matches the call limit
    assert isinstance(coins, list)
    assert len(coins) == TARGET_LIMIT, f"Expected exactly {TARGET_LIMIT} coins, but got {len(coins)}!"

    # Validate the internal sorting logic
    coins.sort(key=lambda c: getattr(c, "current_price", 0.0), reverse=True)
    assert getattr(coins[0], "current_price") == 50000.0
    assert getattr(coins[0], "inc_name") == "Test Coin 1"

    # Print the Distinguishable Attribute Table
    print("\n\n" + "=" * 80)
    print(" 📈 TABLE 1: TOP CRYPTO ASSETS (Sorted by Current Price)")
    print("=" * 80)
    print(f"{'COIN NAME':<20} | {'SYMBOL':<10} | {'CURRENT PRICE (USD)'}")
    print("-" * 80)

    for c in coins:
        name = str(getattr(c, 'inc_name', 'Unknown'))
        symbol = str(getattr(c, 'symbol', 'N/A'))
        price = getattr(c, 'current_price', 0)

        print(f"{name:<20} | {symbol:<10} | ${price:,.2f}")

    print("=" * 80 + "\n")