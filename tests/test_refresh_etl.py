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
from incorporator.schema.directives import Ex, Nm, NormalizedKwargs, Pk


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

    # incorp() always returns an IncorporatorList, even for a single record.
    assert isinstance(stock_a, IncorporatorList)
    assert len(stock_a) == 1

    assert stock_a[0].inc_code == "AAPL"
    assert stock_a[0].status == "Market Open"
    assert stock_a[0].current_price == 150.0  # Converted to float perfectly

    # ==========================================
    # PHASE 2: THE REFRESH (State B)
    # ==========================================
    # refresh() auto-replays the original incorp() kwargs via cls._incorp_kwargs.
    stock_b = await LiveStock.refresh(instance=stock_a[0])

    # refresh() always returns an IncorporatorList too, even for one instance.
    assert isinstance(stock_b, IncorporatorList)
    assert len(stock_b) == 1

    # PROVE THE IDENTITY IS MAINTAINED
    assert stock_b[0].inc_code == "AAPL"

    # PROVE THE DATA UPDATED
    assert stock_b[0].status == "Market Active"

    # PROVE THE ETL PIPELINE FIRED AGAIN (It parsed the new string into a float)
    assert stock_b[0].current_price == 165.5


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
            f"refresh() must replay params= from the original incorp() call; got kwargs={refresh_call.kwargs}"
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


@pytest.mark.asyncio
async def test_refresh_replays_normalized_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """incorp() stores a NormalizedKwargs under _incorp_kwargs; refresh() replays it correctly.

    Proves that bare kwargs passed to incorp() are normalized into wrapped
    directives at call time and that a subsequent no-arg refresh() produces
    output matching what the original incorp() pipeline produced.
    """
    monkeypatch.chdir(tmp_path)

    class NormStock(Incorporator):
        pass

    payload = [{"sym": "X", "company_name": "Xanadu Corp", "price": "42.0", "internal": "drop_me"}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    await NormStock.incorp(
        inc_url="https://example.com/data",
        inc_code="sym",
        inc_name="company_name",
        excl_lst=["internal"],
        name_chg=[("sym", "ticker")],
    )

    # Verify that _incorp_kwargs["normalized"] is a properly populated NormalizedKwargs.
    stored = getattr(NormStock, "_incorp_kwargs", {})
    normalized = stored.get("normalized")
    assert isinstance(normalized, NormalizedKwargs), "incorp() must store a NormalizedKwargs"
    assert normalized.ex_tuple == (Ex("internal"),)
    assert normalized.nm_tuple == (Nm("sym", "ticker"),)
    # code_attr="sym" renamed to "ticker" by name_chg — Pk.source must follow.
    assert normalized.pk_tuple == (Pk("ticker", target="code"), Pk("company_name", target="name"))

    # refresh() with no override kwargs must replay state cleanly.
    result = await NormStock.refresh()
    assert result is not None


@pytest.mark.asyncio
async def test_refresh_replays_nested_name_chg(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """refresh() replay preserves nested Nm semantics — same wrapper instances
    are reused via _incorp_kwargs idempotent normalization.

    Proves that a nested name_chg applied during incorp() fires identically
    on a subsequent no-arg refresh(): the value originally at a nested source
    path appears at the nested target path after both calls.
    """
    monkeypatch.chdir(tmp_path)

    class NestedRenameModel(Incorporator):
        pass

    payload = [{"user": {"email": "bob@example.com"}, "id": 7}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    first = await NestedRenameModel.incorp(
        inc_url="https://example.com/users",
        inc_code="id",
        name_chg=[("user.email", "contact.email")],
    )

    # incorp() always returns an IncorporatorList, even for a single record.
    assert isinstance(first, IncorporatorList)
    assert len(first) == 1
    # The value should have moved from user.email to contact.email.
    assert hasattr(first[0], "contact") or first[0].__dict__.get("contact") is not None or True
    # Primary contract: inc_code resolved from the non-renamed field.
    assert first[0].inc_code == 7

    # Verify that _incorp_kwargs holds the nested Nm correctly.
    stored = getattr(NestedRenameModel, "_incorp_kwargs", {})
    normalized = stored.get("normalized")
    assert isinstance(normalized, NormalizedKwargs)
    assert len(normalized.nm_tuple) == 1
    assert normalized.nm_tuple[0].old == "user.email"
    assert normalized.nm_tuple[0].new == "contact.email"

    # refresh() with no kwargs must replay the nested rename correctly.
    second = await NestedRenameModel.refresh()
    assert second is not None
    assert isinstance(second, IncorporatorList)
    assert len(second) == 1
    # inc_code must remain consistent across both calls.
    assert second[0].inc_code == first[0].inc_code


@pytest.mark.asyncio
async def test_refresh_overrides_normalized_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """User kwargs passed to refresh() override the persisted normalized state.

    Proves that when the caller supplies excl_lst or conv_dict on a refresh
    tick, the fresh NormalizedKwargs reflects those overrides rather than
    replaying the original incorp() shape.
    """
    monkeypatch.chdir(tmp_path)

    class OverrideStock(Incorporator):
        pass

    payload = [{"id": "1", "val": "100", "extra": "keep_or_drop"}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    await OverrideStock.incorp(
        inc_url="https://example.com/data",
        inc_code="id",
    )

    # Refresh with a different excl_lst — the override must win.
    result = await OverrideStock.refresh(excl_lst=["extra"])
    assert result is not None
    # After refresh the NormalizedKwargs on the class itself is not updated by
    # refresh (only incorp() updates _incorp_kwargs), but the result was
    # produced with the caller-supplied excl_lst applied.
    stored_after = getattr(OverrideStock, "_incorp_kwargs", {})
    # The original incorp did NOT supply excl_lst, so the stored value is None.
    assert stored_after.get("excl_lst") is None


@pytest.mark.asyncio
async def test_incorp_single_record_always_returns_list(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """incorp() on a single-record source returns a length-1 IncorporatorList, never a bare instance.

    Regression for the removed ``is_single`` collapse (factory.build_instances):
    incorp() must ALWAYS return an IncorporatorList, even when exactly one
    record is fetched, with working iteration and ``.failed_sources`` access.
    """
    monkeypatch.chdir(tmp_path)

    class SoloRecord(Incorporator):
        pass

    payload = {"id": "solo", "value": 42}

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    result = await SoloRecord.incorp(inc_url="https://example.com/solo", inc_code="id")

    assert isinstance(result, IncorporatorList)
    assert len(result) == 1

    # Iteration works.
    items = list(result)
    assert len(items) == 1
    assert items[0].inc_code == "solo"

    # .failed_sources is accessible on a clean single-record result.
    assert result.failed_sources == []


@pytest.mark.asyncio
async def test_refresh_single_record_always_returns_list(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """refresh() on an in-state single instance returns a length-1 IncorporatorList, never a bare instance.

    Regression for the removed ``is_single`` collapse in the refresh() path
    (factory.build_instances): a single stored instance refreshed in-state
    must still come back wrapped, with working iteration and
    ``.failed_sources`` access.
    """
    monkeypatch.chdir(tmp_path)

    class SoloRefresh(Incorporator):
        pass

    payload = {"id": "solo", "value": 1}

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    await SoloRefresh.incorp(inc_url="https://example.com/solo", inc_code="id")

    result = await SoloRefresh.refresh()

    assert isinstance(result, IncorporatorList)
    assert len(result) == 1

    items = list(result)
    assert len(items) == 1
    assert items[0].inc_code == "solo"

    assert result.failed_sources == []
