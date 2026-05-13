"""Unit tests for Incorporator.fjord() — multi-source stateful streaming.

Each test seeds two mocked HTTP sources (Coin + BinanceFutures), exercises the
fjord engine with a user-supplied ``combine()`` function, and asserts the
joined output lands in the calling class's ``inc_dict`` plus the export file.

Mock pattern mirrors ``tests/public/api/test_coingecko_etl.py`` — we patch
``incorporator.io.fetch.execute_request`` with an async stub that returns
canned ``httpx.Response`` objects keyed off the URL.
"""

import json
from pathlib import Path
from typing import Any, AsyncGenerator

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch


# ----------------------------------------------------------------------
# User-defined classes (would normally live in the user's pipeline file)
# ----------------------------------------------------------------------
class Coin(Incorporator):
    pass


class BinanceFutures(Incorporator):
    pass


class CoinMarket(Incorporator):
    coin_name: str = ""
    spot_price: float = 0.0
    futures_price: float = 0.0
    spread: float = 0.0


# ----------------------------------------------------------------------
# Mock network — two endpoints, two payloads
# ----------------------------------------------------------------------
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/ticker/price"


async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Routes URL → canned response. Covers both mocked APIs."""
    if "coingecko" in url:
        payload = [
            {"id": "bitcoin", "name": "Bitcoin", "current_price": 64000.0},
            {"id": "ethereum", "name": "Ethereum", "current_price": 3500.0},
        ]
    elif "binance" in url:
        payload = [
            {"symbol": "bitcoin", "price": 64500.0},
            {"symbol": "ethereum", "price": 3520.0},
        ]
    else:
        payload = []
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


# ----------------------------------------------------------------------
# combine.py fixture writer
# ----------------------------------------------------------------------
COMBINE_SOURCE = '''
def combine(state):
    """Join Coin spot prices with BinanceFutures prices by symbol/id."""
    coins = state["Coin"]
    futures = state["BinanceFutures"]
    rows = []
    for c in coins:
        f = futures.inc_dict.get(c.inc_code)
        if f is None:
            continue
        rows.append({
            "inc_code": c.inc_code,
            "coin_name": getattr(c, "name", ""),
            "spot_price": getattr(c, "current_price", 0.0),
            "futures_price": getattr(f, "price", 0.0),
            "spread": getattr(f, "price", 0.0) - getattr(c, "current_price", 0.0),
        })
    return rows
'''

BROKEN_COMBINE_SOURCE = '''
def combine(state):
    raise ZeroDivisionError("simulated combine failure")
'''

BAD_ARITY_COMBINE = '''
def combine(state, extra):
    return []
'''

NO_COMBINE_SOURCE = '''
def transform(state):
    return []
'''


def _write_combine(tmp_path: Path, source: str = COMBINE_SOURCE) -> Path:
    p = tmp_path / "combine.py"
    p.write_text(source, encoding="utf-8")
    return p


@pytest.fixture(autouse=True)
def _clean_registries() -> Any:
    """Each test starts with empty inc_dicts to avoid cross-test contamination."""
    Coin.inc_dict.clear()
    BinanceFutures.inc_dict.clear()
    CoinMarket.inc_dict.clear()
    yield
    Coin.inc_dict.clear()
    BinanceFutures.inc_dict.clear()
    CoinMarket.inc_dict.clear()


async def _drain(gen: AsyncGenerator[Any, None]) -> list:
    """Collect every audit yielded by an async generator into a list."""
    out: list = []
    async for audit in gen:
        out.append(audit)
    return out


# ======================================================================
# TESTS
# ======================================================================


@pytest.mark.asyncio
async def test_fjord_one_shot_combines_two_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two sources + combine() → CoinMarket.inc_dict populated, export file written."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    combine_file = _write_combine(tmp_path)
    out_file = tmp_path / "markets.ndjson"

    audits = await _drain(
        CoinMarket.fjord(
            stream_params=[
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}},
            ],
            code_file=combine_file,
            export_params={"file_path": str(out_file)},
            # No intervals → one-shot tick then exit.
        )
    )

    # Two seed audits + one combine audit (no refresh daemons since refresh_params not set)
    operations = [a.operation for a in audits]
    assert "fjord_incorp:Coin" in operations
    assert "fjord_incorp:BinanceFutures" in operations
    assert "combine" in operations

    # Combined output landed
    combine_audit = next(a for a in audits if a.operation == "combine")
    assert combine_audit.rows_processed == 2
    assert not combine_audit.failed_sources

    # inc_dict populated with CoinMarket instances
    assert len(CoinMarket.inc_dict) == 2
    btc = CoinMarket.inc_dict["bitcoin"]
    assert btc.spread == pytest.approx(500.0)
    assert btc.coin_name == "Bitcoin"

    # Export file was written
    assert out_file.exists()
    lines = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 2
    assert {r["inc_code"] for r in lines} == {"bitcoin", "ethereum"}


@pytest.mark.asyncio
async def test_fjord_combine_error_yields_audit_with_failed_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """combine() raises → audit has failed_sources populated, no crash."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    combine_file = _write_combine(tmp_path, BROKEN_COMBINE_SOURCE)
    out_file = tmp_path / "won't-be-written.ndjson"

    audits = await _drain(
        CoinMarket.fjord(
            stream_params=[
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}},
            ],
            code_file=combine_file,
            export_params={"file_path": str(out_file)},
        )
    )

    combine_audit = next(a for a in audits if a.operation == "combine")
    assert combine_audit.rows_processed == 0
    assert combine_audit.failed_sources
    assert "Combine Error" in combine_audit.failed_sources[0]
    assert "simulated combine failure" in combine_audit.failed_sources[0]


@pytest.mark.asyncio
async def test_fjord_validates_stream_params(tmp_path: Path) -> None:
    """Missing keys / wrong types in stream_params must raise clearly."""
    combine_file = _write_combine(tmp_path)

    # Empty list
    with pytest.raises(ValueError, match="requires at least one stream"):
        async for _ in CoinMarket.fjord(
            stream_params=[],
            code_file=combine_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Missing 'cls'
    with pytest.raises(ValueError, match="missing required key 'cls'"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"incorp_params": {}}],
            code_file=combine_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Wrong type for 'cls'
    with pytest.raises(TypeError, match="must be an Incorporator subclass"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"cls": str, "incorp_params": {}}],
            code_file=combine_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Missing 'incorp_params'
    with pytest.raises(ValueError, match="missing required key 'incorp_params'"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"cls": Coin}],
            code_file=combine_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_combine_file_must_define_combine(tmp_path: Path) -> None:
    """code_file without a combine() function raises clearly."""
    bad_file = _write_combine(tmp_path, NO_COMBINE_SOURCE)

    with pytest.raises(ValueError, match="must define a top-level combine"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            code_file=bad_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_combine_arity_enforced(tmp_path: Path) -> None:
    """combine() with != 1 parameter raises ValueError."""
    bad_file = _write_combine(tmp_path, BAD_ARITY_COMBINE)

    with pytest.raises(ValueError, match="must accept exactly 1 parameter"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            code_file=bad_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_combine_file_missing_raises(tmp_path: Path) -> None:
    """Non-existent code_file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="code_file not found"):
        async for _ in CoinMarket.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            code_file=tmp_path / "does_not_exist.py",
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_per_stream_export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Per-stream export_params produces its own export file with class-tagged audit."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    combine_file = _write_combine(tmp_path)
    combined_out = tmp_path / "markets.ndjson"
    coin_out = tmp_path / "coins_only.ndjson"

    audits = await _drain(
        CoinMarket.fjord(
            stream_params=[
                {
                    "cls": Coin,
                    "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"},
                    "export_params": {"file_path": str(coin_out)},
                },
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}},
            ],
            code_file=combine_file,
            export_params={"file_path": str(combined_out)},
        )
    )

    operations = [a.operation for a in audits]
    assert "export:Coin" in operations
    assert "combine" in operations
    assert coin_out.exists()
    assert combined_out.exists()
