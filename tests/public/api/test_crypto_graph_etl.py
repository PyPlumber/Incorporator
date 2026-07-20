"""Pinning test for ``examples/appendix/crypto-graph-mapping/crypto_graph_mapping.py`` (T-Tutorial).

Locks the CURRENT observable behavior of the appendix's plain ``incorp()``
build-time ``link_to`` join (``main()``'s ``BinanceStat``/``BinanceBook``
fetches + the 7-entry inline ``conv_dict`` on ``CryptoAsset.incorp``) ahead
of an upcoming framework refactor program. No Watershed involved.

Calls the module's real ``main()`` directly (rather than reconstructing the
``incorp()`` calls by hand) so the pinned ``conv_dict`` -- including the 4
``link_to(..., extractor=make_linker(...))`` join entries -- is guaranteed
byte-identical to production code, not a test-side transcription that could
silently drift. ``main()`` never returns its ``assets``/``binance_stats``/
``binance_books`` locals (it only prints a dashboard), so assertions read
the post-``main()`` class-level graph maps (``CryptoAsset.inc_dict``,
``BinanceStat.inc_dict``, ``BinanceBook.inc_dict``) -- the same live
registries ``main()``'s own build-time join traverses.

Dogecoin is seeded with a Binance USDT pair but no USDC pair, pinning
``link_to()``'s documented null-on-miss contract for one quote currency
while the other resolves (mirrors ``test_parent_child_drilling_etl.py``'s
sparse-data memecoin pattern).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx
import pytest

from incorporator.io import fetch
from incorporator.io.penstock import _HOST_PENSTOCKS
from tests.helpers import load_sidecar

_HERE = Path(__file__).resolve()
_EXAMPLE_DIR = _HERE.parents[3] / "examples" / "appendix" / "crypto-graph-mapping"

_BINANCE_STATS_PAYLOAD = [
    {"symbol": "BTCUSDT", "quoteVolume": "123456.78"},
    {"symbol": "BTCUSDC", "quoteVolume": "98765.43"},
    {"symbol": "DOGEUSDT", "quoteVolume": "5555.55"},
    # No DOGEUSDC entry -- dogecoin's USDC pair is sparse data.
]

_BINANCE_BOOKS_PAYLOAD = [
    {"symbol": "BTCUSDT", "bidPrice": "67000.12"},
    {"symbol": "BTCUSDC", "bidPrice": "67010.34"},
    {"symbol": "DOGEUSDT", "bidPrice": "0.12345"},
    # No DOGEUSDC entry -- dogecoin's USDC pair is sparse data.
]

_COINGECKO_MARKETS_PAYLOAD = [
    {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "current_price": 67200.5, "market_cap_rank": 1},
    {"id": "dogecoin", "symbol": "doge", "name": "Dogecoin", "current_price": 0.15, "market_cap_rank": 8},
]


@pytest.fixture(autouse=True, scope="module")
def _restore_host_penstock_registry() -> Iterator[None]:
    """Snapshot/restore the process-global penstock registry around this module.

    Must run (and snapshot) BEFORE ``crypto_graph_mapping.py`` is loaded --
    see ``_crypto_mod``'s docstring for why the load is deferred into its own
    fixture instead of happening at this test module's import time. Mutates
    ``_HOST_PENSTOCKS`` in place; never reassigns -- every importer, including
    ``resolve_penstock``, holds a direct reference to this exact dict object.
    """
    snapshot = dict(_HOST_PENSTOCKS)
    yield
    _HOST_PENSTOCKS.clear()
    _HOST_PENSTOCKS.update(snapshot)


@pytest.fixture(scope="module")
def _crypto_mod(_restore_host_penstock_registry: None) -> ModuleType:
    """Load ``crypto_graph_mapping.py`` after the penstock-registry snapshot above.

    ``crypto_graph_mapping.py`` calls
    ``register_host_penstock("api.coingecko.com", ...)`` as a module-level
    import side effect. pytest imports every test module during its
    collection phase, before any fixture runs -- a module-level
    ``load_sidecar(...)`` call at this test file's top would therefore fire
    that side effect BEFORE ``_restore_host_penstock_registry`` gets a chance
    to snapshot a clean baseline, permanently leaking ``"api.coingecko.com"``
    into later-running test modules (e.g. ``test_penstock_registry.py``'s "a
    fresh process has an empty registry" assertion -- precedent commit
    ``c188fbd``). Deferring the load into this fixture (declared to depend on
    the snapshot fixture above) ensures the registration happens during the
    RUN phase, after the snapshot is already taken.
    """
    return load_sidecar(_EXAMPLE_DIR / "crypto_graph_mapping.py", "crypto_graph_mapping_appendix")


async def _mock_crypto_graph(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Return canned binance.us + api.coingecko.com responses keyed on URL."""
    if "ticker/24hr" in url:
        payload: Any = _BINANCE_STATS_PAYLOAD
    elif "ticker/bookTicker" in url:
        payload = _BINANCE_BOOKS_PAYLOAD
    elif "coins/markets" in url:
        payload = _COINGECKO_MARKETS_PAYLOAD
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


def _reset_all(*classes: type) -> None:
    """Wipe per-class inc_dict to prevent test cross-contamination."""
    for cls in classes:
        cls.inc_dict.clear()


@pytest.mark.asyncio
async def test_crypto_graph_mapping_main_builds_link_to_joins(
    monkeypatch: pytest.MonkeyPatch, _crypto_mod: ModuleType
) -> None:
    """Pins crypto-graph-mapping's ``main()`` build-time ``link_to`` join.

    Proves:
    - ``CryptoAsset.inc_dict`` has both ``bitcoin`` and ``dogecoin`` after
      ``main()`` runs.
    - Bitcoin's 4 linked sub-market objects (``stats_usdt``, ``book_usdt``,
      ``stats_usdc``, ``book_usdc``) all resolve to the exact seeded floats.
    - Dogecoin's USDC pair is real sparse data (``stats_usdc``/``book_usdc``
      are ``None``) while its USDT pair resolves.
    """
    BinanceStat = _crypto_mod.BinanceStat
    BinanceBook = _crypto_mod.BinanceBook
    CryptoAsset = _crypto_mod.CryptoAsset
    CryptoLiquidity = _crypto_mod.CryptoLiquidity

    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_crypto_graph)
    _reset_all(BinanceStat, BinanceBook, CryptoAsset, CryptoLiquidity)

    await _crypto_mod.main()

    bitcoin = CryptoAsset.inc_dict.get("bitcoin")
    dogecoin = CryptoAsset.inc_dict.get("dogecoin")
    assert bitcoin is not None
    assert dogecoin is not None

    assert bitcoin.stats_usdt is not None and bitcoin.stats_usdt.quoteVolume == 123456.78
    assert bitcoin.book_usdt is not None and bitcoin.book_usdt.bidPrice == 67000.12
    assert bitcoin.stats_usdc is not None and bitcoin.stats_usdc.quoteVolume == 98765.43
    assert bitcoin.book_usdc is not None and bitcoin.book_usdc.bidPrice == 67010.34

    assert dogecoin.stats_usdt is not None and dogecoin.stats_usdt.quoteVolume == 5555.55
    assert dogecoin.book_usdt is not None and dogecoin.book_usdt.bidPrice == 0.12345
    assert dogecoin.stats_usdc is None
    assert dogecoin.book_usdc is None
