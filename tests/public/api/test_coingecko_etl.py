"""Integration test for basic root-array API fetching (CoinGecko)."""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator


# --- EXPLICIT SUBCLASSING ---
class Coin(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_coingecko_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the CoinGecko /coins/markets endpoint."""
    if "coins/markets" in url:
        # Notice: CoinGecko returns a root-level JSON array, not an envelope like {"data":[]}
        payload =[
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 64000.00,
                "market_cap": 1250000000000,
                "total_volume": 35000000000
            },
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "current_price": 3500.00,
                "market_cap": 420000000000,
                "total_volume": 15000000000
            },
            {
                "id": "solana",
                "symbol": "sol",
                "name": "Solana",
                "current_price": 145.50,
                "market_cap": 65000000000,
                "total_volume": 3000000000
            }
        ]
    else:
        payload =[]

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# --- TESTS ---
@pytest.mark.asyncio
async def test_coingecko_zero_boilerplate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves root-level array parsing and dynamic typing with absolute minimum boilerplate."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_coingecko_execute_get)
    GECKO_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"

    # ==========================================
    # 1. FETCH DATA (The "Zero-Boilerplate" Call)
    # ==========================================
    coins = await Coin.incorp(
        inc_url=GECKO_URL,
        inc_code="id",
        inc_name="name",
    )

    # ==========================================
    # 2. ASSERTIONS
    # ==========================================
    assert isinstance(coins, list)
    assert len(coins) == 3

    # Grab the first coin (Bitcoin)
    btc = coins[0]

    # Verify standard mappings
    assert btc.inc_code == "bitcoin"
    assert btc.inc_name == "Bitcoin"

    # Verify Pydantic V2 dynamically typed the numbers as floats/ints
    assert isinstance(btc.current_price, float)
    assert btc.current_price == 64000.00
    assert isinstance(btc.market_cap, int)
    assert btc.market_cap == 1250000000000

    # Verify the `inc_dict` registry works flawlessly
    eth = coins.inc_dict.get("ethereum")
    assert eth is not None
    assert eth.inc_name == "Ethereum"
    assert eth.current_price == 3500.00

    sol = coins.inc_dict.get("solana")
    assert sol is not None
    assert sol.symbol == "sol"

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