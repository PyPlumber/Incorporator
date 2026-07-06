"""Mocked end-to-end smoke test for Tutorial 5 (parent-child drilling).

Covers the CoinGecko root-list parent + `/coins/{id}` child-drill path that
`test_coingecko_etl.py` does not exercise, and locks in the build-time
`conv_dict` refactor in `examples/05-parent-child-drilling/parent_child_drilling.py`
(``links_homepage`` / ``genesis_date`` lifted off the nested payload).
"""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.schema.converters import inc
from incorporator.schema.extractors import pluck

COINDETAIL_CONV_DICT = {
    "links_homepage": pluck("links.homepage"),
    "genesis_date": inc(str, default="-"),
}


class Coin(Incorporator):
    pass


class CoinDetail(Incorporator):
    pass


MARKETS_PAYLOAD = [
    {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "current_price": 67234.51},
    {"id": "shibby-memecoin", "symbol": "shib2", "name": "Shibby Memecoin", "current_price": 0.0001},
]

# Bitcoin detail: full `links` object + a real genesis_date — the happy path.
BITCOIN_DETAIL = {
    "id": "bitcoin",
    "genesis_date": "2009-01-03",
    "links": {"homepage": ["http://www.bitcoin.org"], "whitepaper": ""},
}

# Memecoin detail: no `links` key at all, `genesis_date` is null — proves the
# `or []` / `default="-"` fallback paths still fire post-refactor.
MEMECOIN_DETAIL = {
    "id": "shibby-memecoin",
    "genesis_date": None,
}


async def mock_coingecko_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks CoinGecko's /coins/markets root list and /coins/{id} detail drill."""
    if "coins/markets" in url:
        payload: Any = MARKETS_PAYLOAD
    elif "coins/bitcoin" in url:
        payload = BITCOIN_DETAIL
    elif "coins/shibby-memecoin" in url:
        payload = MEMECOIN_DETAIL
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


@pytest.mark.asyncio
async def test_parent_child_drill_conv_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves inc_parent/inc_child drill + conv_dict lift read as plain attrs, both coins."""
    monkeypatch.setattr(fetch, "execute_request", mock_coingecko_execute_get)

    coins = await Coin.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets",
        params={"vs_currency": "usd", "per_page": 10, "page": 1},
        inc_code="id",
        inc_name="name",
        excl_lst=["image"],
        requests_per_second=1000,
    )
    assert len(coins) == 2

    details = await CoinDetail.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/{}",
        inc_parent=coins,
        inc_child="id",
        inc_code="id",
        excl_lst=["image", "tickers", "community_data", "developer_data"],
        requests_per_second=1000,
        conv_dict=COINDETAIL_CONV_DICT,
    )
    assert len(details) == 2

    btc_detail = CoinDetail.inc_dict.get("bitcoin")
    assert btc_detail is not None
    assert btc_detail.genesis_date == "2009-01-03"
    assert btc_detail.links_homepage == ["http://www.bitcoin.org"]

    memecoin_detail = CoinDetail.inc_dict.get("shibby-memecoin")
    assert memecoin_detail is not None
    # Missing `links` key -> pluck() resolves to None -> read-time `or []` guard.
    assert memecoin_detail.links_homepage is None
    assert (memecoin_detail.links_homepage or []) == []
    # `genesis_date: null` is garbage -> inc(str, default="-") falls back to ASCII default.
    assert memecoin_detail.genesis_date == "-"

    # Full report-loop equivalence check, mirroring the tutorial script.
    rows = []
    for coin in coins:
        detail = CoinDetail.inc_dict.get(coin.id)
        assert detail is not None
        homepage_list = detail.links_homepage or []
        homepage = (homepage_list[0] if homepage_list else "")[:38]
        rows.append((coin.name, detail.genesis_date, homepage))

    assert rows == [
        ("Bitcoin", "2009-01-03", "http://www.bitcoin.org"),
        ("Shibby Memecoin", "-", ""),
    ]
