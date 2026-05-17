"""Unit tests for Incorporator.fjord() — multi-source stateful streaming.

The output class is **derived dynamically** from the ``outflow`` filename
(snake_case → PascalCase). The developer never declares it. Each test seeds
two mocked HTTP sources (``Coin`` + ``BinanceFutures``), drives the fjord
engine with a user-supplied ``outflow(state)`` function written to a tempfile,
and looks the resulting class up via :data:`SCHEMA_REGISTRY`.

Mock pattern mirrors ``tests/public/api/test_coingecko_etl.py`` — we patch
``incorporator.io.fetch.execute_request`` with an async stub that returns
canned ``httpx.Response`` objects keyed off the URL.
"""

import json
from pathlib import Path
from typing import Any, AsyncGenerator, Optional, Type

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.schema.builder import SCHEMA_REGISTRY


# ----------------------------------------------------------------------
# Source classes (developer-defined; legitimate)
# ----------------------------------------------------------------------
class Coin(Incorporator):
    pass


class BinanceFutures(Incorporator):
    pass


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
# outflow.py fixture writer
# ----------------------------------------------------------------------
OUTFLOW_SOURCE = '''
def outflow(state):
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

BROKEN_OUTFLOW_SOURCE = '''
def outflow(state):
    raise ZeroDivisionError("simulated outflow failure")
'''

BAD_ARITY_OUTFLOW = '''
def outflow(state, extra):
    return []
'''

NO_OUTFLOW_SOURCE = '''
def transform(state):
    return []
'''

EMPTY_OUTFLOW_SOURCE = '''
def outflow(state):
    return []
'''


def _write_outflow(tmp_path: Path, source: str = OUTFLOW_SOURCE, filename: str = "coin_market.py") -> Path:
    p = tmp_path / filename
    p.write_text(source, encoding="utf-8")
    return p


def _find_dynamic_class(class_name: str) -> Optional[Type[Any]]:
    """Look up a fjord-built dynamic class in SCHEMA_REGISTRY by name."""
    for (name, _keys, _base_id), cls in SCHEMA_REGISTRY.items():
        if name == class_name:
            return cls
    return None


_DYNAMIC_NAMES_TO_PURGE = {"CoinMarket", "CryptoSpread"}


@pytest.fixture(autouse=True)
def _clean_state() -> Any:
    """Each test starts with empty source registries and no leftover dynamic
    output classes from earlier tests. Other registry entries (real user
    classes) survive untouched.
    """
    Coin.inc_dict.clear()
    BinanceFutures.inc_dict.clear()
    for key in list(SCHEMA_REGISTRY.keys()):
        if key[0] in _DYNAMIC_NAMES_TO_PURGE:
            del SCHEMA_REGISTRY[key]
    yield
    Coin.inc_dict.clear()
    BinanceFutures.inc_dict.clear()
    for key in list(SCHEMA_REGISTRY.keys()):
        if key[0] in _DYNAMIC_NAMES_TO_PURGE:
            del SCHEMA_REGISTRY[key]


async def _drain(
    gen: AsyncGenerator[Any, None],
    until_ops: Optional[set[str]] = None,
) -> list:
    """Collect waves yielded by an async generator.

    When ``until_ops`` is provided, drain stops as soon as every operation
    name in the set has been seen at least once.  Useful for daemon-mode
    tests where the generator never exits on its own.  Without
    ``until_ops``, drain runs to completion (legacy one-shot tests).
    """
    out: list = []
    seen: set[str] = set()
    async for wave in gen:
        out.append(wave)
        if until_ops is not None:
            seen.add(wave.operation)
            if until_ops.issubset(seen):
                break
    return out


# ======================================================================
# TESTS
# ======================================================================


@pytest.mark.asyncio
async def test_fjord_one_shot_combines_two_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two sources + outflow() → dynamic CoinMarket class built, registry populated, export written."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    outflow_file = _write_outflow(tmp_path)  # coin_market.py → CoinMarket
    out_file = tmp_path / "markets.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                # refresh_params=None opts each source OUT of the refresh
                # daemon — tests want one-shot seed+outflow, not a running
                # daemon.  See plan: post-refactor, missing refresh_params
                # defaults to {} (refresh runs); tests using one-shot must
                # opt out explicitly.
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}, "refresh_params": None},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            export_params={"file_path": str(out_file)},
            # No intervals → one-shot tick then exit.
        )
    )

    # Two seed waves + one outflow wave.
    operations = [a.operation for a in waves]
    assert "fjord_incorp:Coin" in operations
    assert "fjord_incorp:BinanceFutures" in operations
    assert "outflow:CoinMarket" in operations

    outflow_wave = next(a for a in waves if a.operation == "outflow:CoinMarket")
    assert outflow_wave.rows_processed == 2
    assert not outflow_wave.failed_sources

    # Dynamic CoinMarket class was built and its registry populated.
    CoinMarket = _find_dynamic_class("CoinMarket")
    assert CoinMarket is not None
    assert len(CoinMarket.inc_dict) == 2
    btc = CoinMarket.inc_dict["bitcoin"]
    assert btc.spread == pytest.approx(500.0)
    assert btc.coin_name == "Bitcoin"

    # Export file was written.
    assert out_file.exists()
    lines = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 2
    assert {r["inc_code"] for r in lines} == {"bitcoin", "ethereum"}


@pytest.mark.asyncio
async def test_fjord_derives_class_name_from_stem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """outflow ``crypto_spread.py`` → dynamic class is named ``CryptoSpread``."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    outflow_file = _write_outflow(tmp_path, filename="crypto_spread.py")
    out_file = tmp_path / "out.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                # refresh_params=None opts each source OUT of the refresh
                # daemon — tests want one-shot seed+outflow, not a running
                # daemon.  See plan: post-refactor, missing refresh_params
                # defaults to {} (refresh runs); tests using one-shot must
                # opt out explicitly.
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}, "refresh_params": None},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            export_params={"file_path": str(out_file)},
        )
    )

    operations = [a.operation for a in waves]
    assert "outflow:CryptoSpread" in operations
    assert _find_dynamic_class("CryptoSpread") is not None
    assert _find_dynamic_class("CoinMarket") is None


@pytest.mark.asyncio
async def test_fjord_empty_outflow_emits_zero_row_wave(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """outflow() returning [] → zero-row wave, no export file written, no dynamic class built."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    outflow_file = _write_outflow(tmp_path, EMPTY_OUTFLOW_SOURCE)
    out_file = tmp_path / "should_not_exist.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                # refresh_params=None opts each source OUT of the refresh
                # daemon — tests want one-shot seed+outflow, not a running
                # daemon.  See plan: post-refactor, missing refresh_params
                # defaults to {} (refresh runs); tests using one-shot must
                # opt out explicitly.
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}, "refresh_params": None},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            export_params={"file_path": str(out_file)},
        )
    )

    outflow_wave = next(a for a in waves if a.operation == "outflow:CoinMarket")
    assert outflow_wave.rows_processed == 0
    assert not outflow_wave.failed_sources
    assert not out_file.exists()
    # No dynamic class is built on a zero-row tick.
    assert _find_dynamic_class("CoinMarket") is None


@pytest.mark.asyncio
async def test_fjord_outflow_error_yields_wave_with_failed_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """outflow() raises → wave has failed_sources populated, no crash."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    outflow_file = _write_outflow(tmp_path, BROKEN_OUTFLOW_SOURCE)
    out_file = tmp_path / "wont-be-written.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                # refresh_params=None opts each source OUT of the refresh
                # daemon — tests want one-shot seed+outflow, not a running
                # daemon.  See plan: post-refactor, missing refresh_params
                # defaults to {} (refresh runs); tests using one-shot must
                # opt out explicitly.
                {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"}, "refresh_params": None},
                {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            export_params={"file_path": str(out_file)},
        )
    )

    outflow_wave = next(a for a in waves if a.operation == "outflow:CoinMarket")
    assert outflow_wave.rows_processed == 0
    assert outflow_wave.failed_sources
    assert "Outflow Error" in outflow_wave.failed_sources[0]
    assert "simulated outflow failure" in outflow_wave.failed_sources[0]


@pytest.mark.asyncio
async def test_fjord_validates_stream_params(tmp_path: Path) -> None:
    """Missing keys / wrong types in stream_params must raise clearly."""
    outflow_file = _write_outflow(tmp_path)

    # Empty list
    with pytest.raises(ValueError, match="requires at least one stream"):
        async for _ in Incorporator.fjord(
            stream_params=[],
            outflow=outflow_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Missing 'cls'
    with pytest.raises(ValueError, match="missing required key 'cls'"):
        async for _ in Incorporator.fjord(
            stream_params=[{"incorp_params": {}}],
            outflow=outflow_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Wrong type for 'cls'
    with pytest.raises(TypeError, match="must be an Incorporator subclass"):
        async for _ in Incorporator.fjord(
            stream_params=[{"cls": str, "incorp_params": {}}],
            outflow=outflow_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass

    # Missing 'incorp_params'
    with pytest.raises(ValueError, match="missing required key 'incorp_params'"):
        async for _ in Incorporator.fjord(
            stream_params=[{"cls": Coin}],
            outflow=outflow_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_outflow_file_must_define_outflow(tmp_path: Path) -> None:
    """outflow file without a top-level outflow() function raises clearly."""
    bad_file = _write_outflow(tmp_path, NO_OUTFLOW_SOURCE)

    with pytest.raises(ValueError, match="must define a top-level outflow"):
        async for _ in Incorporator.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            outflow=bad_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_outflow_arity_enforced(tmp_path: Path) -> None:
    """outflow() with != 1 parameter raises ValueError."""
    bad_file = _write_outflow(tmp_path, BAD_ARITY_OUTFLOW)

    with pytest.raises(ValueError, match="must accept exactly 1 parameter"):
        async for _ in Incorporator.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            outflow=bad_file,
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_outflow_missing_raises(tmp_path: Path) -> None:
    """Non-existent outflow file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="not found"):
        async for _ in Incorporator.fjord(
            stream_params=[{"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL}}],
            outflow=tmp_path / "does_not_exist.py",
            export_params={"file_path": str(tmp_path / "x.ndjson")},
        ):
            pass


@pytest.mark.asyncio
async def test_fjord_per_stream_export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Per-stream export_params produces its own export file with class-tagged wave."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)

    outflow_file = _write_outflow(tmp_path)
    combined_out = tmp_path / "markets.ndjson"
    coin_out = tmp_path / "coins_only.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {
                    "cls": Coin,
                    "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"},
                    "export_params": {"file_path": str(coin_out)},
                    "refresh_params": None,                # opt out of refresh daemon
                    "export_interval": 0.01,               # tight tick — we break after first wave
                },
                {
                    "cls": BinanceFutures,
                    "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"},
                    "refresh_params": None,
                },
            ],
            outflow=outflow_file,
            export_params={"file_path": str(combined_out)},
        ),
        until_ops={"export:Coin", "outflow:CoinMarket"},      # break once both seen
    )

    operations = [a.operation for a in waves]
    assert "export:Coin" in operations
    assert "outflow:CoinMarket" in operations
    assert coin_out.exists()
    assert combined_out.exists()


def test_pascal_case_from_stem_basic() -> None:
    """Direct unit test for the filename → class name derivation."""
    from incorporator.usercode import pascal_case_from_stem

    assert pascal_case_from_stem(Path("coin_market.py")) == "CoinMarket"
    assert pascal_case_from_stem(Path("crypto-spread.py")) == "CryptoSpread"
    assert pascal_case_from_stem(Path("simple.py")) == "Simple"
    assert pascal_case_from_stem(Path("a_b_c_d.py")) == "ABCD"


def test_pascal_case_from_stem_rejects_invalid() -> None:
    """Stems that can't produce a valid Python identifier raise ValueError."""
    from incorporator.usercode import pascal_case_from_stem

    with pytest.raises(ValueError, match="Cannot derive a valid Python class name"):
        pascal_case_from_stem(Path("123.py"))
    with pytest.raises(ValueError, match="Cannot derive a valid Python class name"):
        pascal_case_from_stem(Path("_.py"))


# Per-source interval dict-shape


def test_resolve_per_source_interval_per_entry_override_wins() -> None:
    """Per-entry refresh_interval beats the top-level dict and top-level scalar."""
    from incorporator.observability.pipeline.fjord import _resolve_per_source_interval

    entry = {"cls": Coin, "refresh_interval": 5.0}
    assert _resolve_per_source_interval(60.0, entry, "refresh_interval") == 5.0
    assert _resolve_per_source_interval({"Coin": 30.0}, entry, "refresh_interval") == 5.0


def test_resolve_per_source_interval_top_level_dict_by_name() -> None:
    """Top-level dict keyed by class name resolves per-source."""
    from incorporator.observability.pipeline.fjord import _resolve_per_source_interval

    entry = {"cls": Coin}
    top = {"Coin": 30.0, "BinanceFutures": 5.0}
    assert _resolve_per_source_interval(top, entry, "refresh_interval") == 30.0


def test_resolve_per_source_interval_top_level_dict_by_class_object() -> None:
    """Top-level dict keyed by class object (Python ergonomics) also resolves."""
    from incorporator.observability.pipeline.fjord import _resolve_per_source_interval

    entry = {"cls": Coin}
    top = {Coin: 30.0}
    assert _resolve_per_source_interval(top, entry, "refresh_interval") == 30.0


def test_resolve_per_source_interval_top_level_scalar() -> None:
    """Scalar top-level applies to every entry."""
    from incorporator.observability.pipeline.fjord import _resolve_per_source_interval

    entry = {"cls": Coin}
    assert _resolve_per_source_interval(60.0, entry, "refresh_interval") == 60.0


def test_resolve_per_source_interval_dict_miss_returns_none() -> None:
    """Top-level dict missing the class name returns None (cascade falls to default)."""
    from incorporator.observability.pipeline.fjord import _resolve_per_source_interval

    entry = {"cls": Coin}
    top = {"BinanceFutures": 5.0}              # no entry for Coin
    assert _resolve_per_source_interval(top, entry, "refresh_interval") is None


# ----------------------------------------------------------------------
# depends_on opt-in tiered seed
# ----------------------------------------------------------------------


class Planet(Incorporator):
    pass


class Moon(Incorporator):
    pass


class Comet(Incorporator):
    pass


def test_has_any_depends_on_negative() -> None:
    from incorporator.observability.pipeline.fjord import _has_any_depends_on

    entries = [{"cls": Planet}, {"cls": Moon}]
    assert _has_any_depends_on(entries) is False


def test_has_any_depends_on_positive() -> None:
    from incorporator.observability.pipeline.fjord import _has_any_depends_on

    entries = [{"cls": Planet}, {"cls": Moon, "depends_on": ["Planet"]}]
    assert _has_any_depends_on(entries) is True


def test_validate_depends_on_typo_raises() -> None:
    """Unknown peer name in depends_on must fail fast with a clear message."""
    from incorporator.observability.pipeline.fjord import _validate_depends_on

    entries = [
        {"cls": Planet},
        {"cls": Moon, "depends_on": ["Plnet"]},  # typo: should be "Planet"
    ]
    with pytest.raises(ValueError, match="unknown peer class 'Plnet'"):
        _validate_depends_on(entries)


def test_validate_depends_on_clean_passes() -> None:
    """Well-formed graph: validator silently passes."""
    from incorporator.observability.pipeline.fjord import _validate_depends_on

    entries = [
        {"cls": Planet},
        {"cls": Moon, "depends_on": ["Planet"]},
        {"cls": Comet, "depends_on": ["Planet", "Moon"]},
    ]
    _validate_depends_on(entries)  # no raise


def test_tiered_seed_order_basic_chain() -> None:
    """Planet → Moon → Comet: three tiers, one entry each."""
    from incorporator.observability.pipeline.fjord import _tiered_seed_order

    entries = [
        {"cls": Planet},
        {"cls": Moon, "depends_on": ["Planet"]},
        {"cls": Comet, "depends_on": ["Moon"]},
    ]
    tiers = _tiered_seed_order(entries)
    assert len(tiers) == 3
    assert tiers[0][0]["cls"] is Planet
    assert tiers[1][0]["cls"] is Moon
    assert tiers[2][0]["cls"] is Comet


def test_tiered_seed_order_mixed_tier() -> None:
    """Two independents + one dependent: tier 0 has both, tier 1 has the dependent."""
    from incorporator.observability.pipeline.fjord import _tiered_seed_order

    entries = [
        {"cls": Planet},
        {"cls": Comet},  # independent peer alongside Planet
        {"cls": Moon, "depends_on": ["Planet"]},
    ]
    tiers = _tiered_seed_order(entries)
    assert len(tiers) == 2
    tier0_classes = {e["cls"] for e in tiers[0]}
    assert tier0_classes == {Planet, Comet}
    assert tiers[1][0]["cls"] is Moon


def test_tiered_seed_order_cycle_raises() -> None:
    """Cycle: A depends on B depends on A → ValueError listing the unresolved set."""
    from incorporator.observability.pipeline.fjord import _tiered_seed_order

    entries = [
        {"cls": Planet, "depends_on": ["Moon"]},
        {"cls": Moon, "depends_on": ["Planet"]},
    ]
    with pytest.raises(ValueError, match="depends_on cycle detected"):
        _tiered_seed_order(entries)
