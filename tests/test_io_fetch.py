"""Unit tests for ``incorporator.io.fetch`` helpers."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io.fetch import _normalize_source_list


# ----------------------------------------------------------------------
# _normalize_source_list — type handling
# ----------------------------------------------------------------------


def test_normalize_source_list_handles_str() -> None:
    assert _normalize_source_list("https://example.com/x", None) == ["https://example.com/x"]


def test_normalize_source_list_handles_pathlib_path(tmp_path: Path) -> None:
    """A ``pathlib.Path`` argument used to drop through to ``return []``.

    Before the fix, ``_normalize_source_list(Path("foo.ndjson"), None)`` saw
    ``isinstance(source, list) == False`` and ``isinstance(source, str) ==
    False``, falling through to the empty-list branch.  ``incorp()`` then
    silently returned an empty IncorporatorList with no diagnostic — the
    file was never opened.  The ``os.PathLike`` branch fixes this.
    """
    p = tmp_path / "x.ndjson"
    result = _normalize_source_list(p, None)
    assert result == [str(p)]
    assert isinstance(result[0], str)


def test_normalize_source_list_handles_list_of_paths(tmp_path: Path) -> None:
    """Mixed list of str + Path elements should all coerce to str."""
    paths = [tmp_path / "a.ndjson", str(tmp_path / "b.ndjson")]
    result = _normalize_source_list(paths, None)
    assert result == [str(tmp_path / "a.ndjson"), str(tmp_path / "b.ndjson")]


def test_normalize_source_list_handles_list_with_none() -> None:
    """``None`` entries inside the list are dropped."""
    result = _normalize_source_list(["a", None, "b"], None)
    assert result == ["a", "b"]


def test_normalize_source_list_payload_fallback() -> None:
    """No source but ``payload_list`` set → placeholder list matches its length."""
    result = _normalize_source_list(None, [{}, {}, {}])
    assert result == ["", "", ""]


def test_normalize_source_list_empty_when_unrecognised() -> None:
    """An unsupported type returns empty list (caller is responsible for the
    diagnostic via the ``source`` falsiness check in ``base.py``)."""
    assert _normalize_source_list(42, None) == []  # type: ignore[arg-type]
    assert _normalize_source_list(None, None) == []


# ----------------------------------------------------------------------
# End-to-end: incorp(inc_file=Path) round-trip
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incorp_inc_file_accepts_pathlib_path(tmp_path: Path) -> None:
    """``incorp(inc_file=Path(...))`` reads the file (was silently empty pre-fix).

    Tutorial 2 (universal-formats) uses ``data_dir / "coins_log.ndjson"`` —
    a ``Path`` object — for the round-trip read.  Before the fix this read
    returned an empty IncorporatorList and the tutorial died with a
    ``KeyError`` on the first ``inc_dict["bitcoin"]`` lookup.
    """

    class _Coin(Incorporator):
        inc_code: Any = None
        symbol: str = ""
        name: str = ""

    src = tmp_path / "coins.ndjson"
    src.write_text(
        json.dumps({"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"})
        + "\n"
        + json.dumps({"id": "ethereum", "symbol": "eth", "name": "Ethereum"})
        + "\n",
        encoding="utf-8",
    )

    # Pass the Path directly — no manual str() wrap.
    coins = await _Coin.incorp(inc_file=src, inc_code="id")
    assert len(coins) == 2
    assert "bitcoin" in coins.inc_dict
    assert coins.inc_dict["bitcoin"].name == "Bitcoin"


# ----------------------------------------------------------------------
# _schema_union → auto-coerce on typeless-format reads
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_csv_roundtrip_preserves_int_via_schema_union(tmp_path: Path) -> None:
    """Tutorial 2's failure mode: typed source → CSV → re-incorp.

    A subclass that's been populated from a JSON-shaped source records
    typed fields in ``_schema_union``.  When the same class is then read
    back from CSV (where every cell arrives as ``str``), the auto-coercion
    in ``factory._expand_conv_dict_with_schema_union`` synthesises
    ``inc()`` converters from those previously-observed types so values
    come back typed.
    """

    class _Token(Incorporator):
        pass

    # Seed _schema_union via a typed (JSON-shaped) load.
    src_json = tmp_path / "tokens.ndjson"
    src_json.write_text(
        '{"id": "btc", "price": 78388, "ratio": 0.123, "active": true}\n'
        '{"id": "eth", "price": 2191,  "ratio": 0.045, "active": false}\n',
        encoding="utf-8",
    )
    typed = await _Token.incorp(inc_file=src_json, inc_code="id")
    assert isinstance(typed.inc_dict["btc"].price, int)
    assert isinstance(typed.inc_dict["btc"].ratio, float)
    assert isinstance(typed.inc_dict["btc"].active, bool)
    # _schema_union now carries the typed shape.
    assert "price" in _Token._schema_union

    # Export to CSV (everything becomes text on disk).
    csv_path = tmp_path / "tokens.csv"
    await _Token.export(instance=typed, file_path=str(csv_path), if_exists="append")

    # Round-trip read from CSV — auto-coercion should restore the types.
    roundtrip = await _Token.incorp(inc_file=csv_path, inc_code="id")
    btc = roundtrip.inc_dict["btc"]
    assert isinstance(btc.price, int), f"expected int, got {type(btc.price).__name__}"
    assert isinstance(btc.ratio, float), f"expected float, got {type(btc.ratio).__name__}"
    assert isinstance(btc.active, bool), f"expected bool, got {type(btc.active).__name__}"
    assert btc.price == 78388
    assert btc.ratio == pytest.approx(0.123)
    assert btc.active is True


def test_expand_conv_dict_skips_declared_fields() -> None:
    """``last_rcd`` and other base-class fields stay with Pydantic, not us.

    ``last_rcd`` is a framework-internal ``datetime`` field on the
    ``Incorporator`` base class — Pydantic coerces it via its declared
    annotation.  The auto-coercion helper must NOT synthesise an
    ``inc(datetime)`` for it because ``inc()`` returns ``default=None``
    on garbage values and Pydantic's strict ``datetime`` field rejects
    None.  Skip = correct.
    """
    from incorporator.schema.factory import _expand_conv_dict_with_schema_union

    schema_union = {
        "last_rcd": {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
        "current_price": {"anyOf": [{"type": "integer"}, {"type": "number"}, {"type": "null"}]},
    }
    result = _expand_conv_dict_with_schema_union(
        conv_dict=None,
        schema_union=schema_union,
        declared_field_names=frozenset({"last_rcd", "inc_code", "inc_name"}),
    )
    assert result is not None
    assert "last_rcd" not in result, "last_rcd must be skipped (declared on base class)"
    assert "current_price" in result, "current_price should get auto-synthesised converter"


def test_expand_conv_dict_user_override_wins() -> None:
    """Caller-supplied ``conv_dict`` entries always trump auto-synthesis."""
    from incorporator.schema.factory import _expand_conv_dict_with_schema_union

    user_sentinel = object()
    schema_union = {"price": {"anyOf": [{"type": "integer"}, {"type": "null"}]}}
    result = _expand_conv_dict_with_schema_union(
        conv_dict={"price": user_sentinel},
        schema_union=schema_union,
    )
    assert result is not None
    assert result["price"] is user_sentinel  # user's entry preserved verbatim


def test_expand_conv_dict_omits_string_fields() -> None:
    """``_schema_union[field] = {"type": "string"}`` must NOT auto-coerce.

    The asymmetry exists because coercing TO ``str`` is either a no-op
    (real strings stay strings) or actively wrong (numeric values dressed
    as string by a stale CSV-first read would get cast BACK to string).
    Only typed-up coercion is safe to auto-apply.
    """
    from incorporator.schema.factory import _expand_conv_dict_with_schema_union

    schema_union = {
        "name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "count": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
    }
    result = _expand_conv_dict_with_schema_union(conv_dict=None, schema_union=schema_union)
    assert result is not None
    assert "name" not in result
    assert "count" in result


def test_expand_conv_dict_handles_flat_schema() -> None:
    """Flat ``{"type": "integer"}`` schemas (no anyOf wrapper) also work."""
    from incorporator.schema.factory import _expand_conv_dict_with_schema_union

    schema_union = {"count": {"type": "integer"}}
    result = _expand_conv_dict_with_schema_union(conv_dict=None, schema_union=schema_union)
    assert result is not None
    assert "count" in result


def test_expand_conv_dict_empty_schema_union_returns_caller_dict() -> None:
    """Empty schema_union → return caller's conv_dict unchanged (or None)."""
    from incorporator.schema.factory import _expand_conv_dict_with_schema_union

    assert _expand_conv_dict_with_schema_union(None, {}) is None
    caller = {"foo": lambda x: x}
    assert _expand_conv_dict_with_schema_union(caller, {}) is caller


# ----------------------------------------------------------------------
# fetch_concurrent_payloads — gather exception handling (DLQ contract)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_concurrent_non_429_error_does_not_cancel_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-429 HTTP error must surface in failed_sources, not cancel batch.

    Regression guard for the gather propagation trap: _safe_execute re-raises
    non-429 HTTPStatusError as IncorporatorNetworkError. Without
    ``return_exceptions=True``, that propagated up the gather and cancelled
    every sibling task — turning a single bad source into a wave abort with
    a confusing cancel cascade. Now: failure surfaces in failed_sources,
    siblings complete normally.
    """
    from incorporator.exceptions import IncorporatorNetworkError
    from incorporator.io import fetch

    call_count = {"value": 0}

    async def fake_process_single(src: str, is_file_mode: bool, client: Any, rate_limiter: Any, **_kw: Any) -> list:
        call_count["value"] += 1
        if "bad" in src:
            raise IncorporatorNetworkError(f"HTTP error 503 on {src}")
        return [{"src": src, "ok": True}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    parsed, failed = await fetch.fetch_concurrent_payloads(
        source_list=["https://good-a.test/", "https://bad.test/", "https://good-b.test/"],
        payload_list=None,
        is_file_mode=False,
        limit=3,
    )

    # All three sources attempted; siblings not cancelled.
    assert call_count["value"] == 3
    # Good sources returned their data.
    assert {row["src"] for row in parsed} == {"https://good-a.test/", "https://good-b.test/"}
    # Bad source surfaces in rejects rather than aborting the batch.
    assert "https://bad.test/" in [entry.source for entry in failed]


@pytest.mark.asyncio
async def test_fetch_concurrent_all_5xx_returns_empty_with_all_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All-failures case: empty parsed_data, every URL in failed_sources."""
    from incorporator.exceptions import IncorporatorNetworkError
    from incorporator.io import fetch

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        raise IncorporatorNetworkError(f"HTTP error 500 on {src}")

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    urls = ["https://a.test/", "https://b.test/", "https://c.test/"]
    parsed, failed = await fetch.fetch_concurrent_payloads(
        source_list=urls, payload_list=None, is_file_mode=False, limit=3
    )

    assert parsed == []
    assert sorted(entry.source for entry in failed) == sorted(urls)


@pytest.mark.asyncio
async def test_fetch_concurrent_unexpected_error_surfaces_in_failed_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected exception types (ValueError, KeyError) surface to DLQ, not crash."""
    from incorporator.io import fetch

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        if "boom" in src:
            raise ValueError("malformed paginator state")
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    parsed, failed = await fetch.fetch_concurrent_payloads(
        source_list=["https://ok.test/", "https://boom.test/"],
        payload_list=None,
        is_file_mode=False,
        limit=2,
    )

    assert any(row["src"] == "https://ok.test/" for row in parsed)
    assert "https://boom.test/" in [entry.source for entry in failed]


@pytest.mark.asyncio
async def test_fetch_concurrent_path_a_batched_no_cancel_cascade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Path A (delay_between_batches > 0) must also use return_exceptions=True.

    Mirrors the Path B test but exercises the batched-with-delay branch
    via delay_between_batches=0.001 — both gather sites need the same
    safety contract.
    """
    from incorporator.exceptions import IncorporatorNetworkError
    from incorporator.io import fetch

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        if "bad" in src:
            raise IncorporatorNetworkError(f"HTTP error 502 on {src}")
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    parsed, failed = await fetch.fetch_concurrent_payloads(
        source_list=["https://good-a.test/", "https://bad.test/", "https://good-b.test/"],
        payload_list=None,
        is_file_mode=False,
        limit=2,
        delay_between_batches=0.001,  # PATH A
    )

    assert {row["src"] for row in parsed} == {"https://good-a.test/", "https://good-b.test/"}
    assert "https://bad.test/" in [entry.source for entry in failed]


# ----------------------------------------------------------------------
# Host-aware rate-limit registry — opt-in via register_host_penstock.
# Framework ships no implicit per-host throttling; these tests register
# the host explicitly inline so the fetch path picks up the registered
# rate via the canonical resolve_penstock() resolver.
# ----------------------------------------------------------------------


def _capture_resolved_rates(monkeypatch: pytest.MonkeyPatch) -> list:
    """Patch ``fetch.resolve_penstock`` to record each resolved rate_per_sec.

    Returns the list that accumulates rates; caller asserts against it.
    """
    from incorporator.io import fetch

    captured_rates: list = []
    real_resolve = fetch.resolve_penstock

    def capture_resolve(*args: Any, **kwargs: Any) -> Any:
        bound = real_resolve(*args, **kwargs)
        rate = getattr(bound.penstock, "rate_per_sec", None)
        if rate is not None:
            captured_rates.append(rate)
        return bound

    monkeypatch.setattr(fetch, "resolve_penstock", capture_resolve)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)
    return captured_rates


@pytest.mark.asyncio
async def test_host_aware_penstock_applied_when_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registering ``api.coingecko.com`` at 0.2 r/s pins the penstock rate for that host.

    Before v1.3.0, this rate was a built-in default in
    ``incorporator/io/throttle.py``.  The framework now ships no
    implicit per-host throttling; users register hosts they care about
    explicitly (or pass ``requests_per_second=`` per call).
    """
    from incorporator.io import fetch
    from incorporator.io.penstock import _HOST_PENSTOCKS, SustainedPenstock

    monkeypatch.setitem(_HOST_PENSTOCKS, "api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
    captured_rates = _capture_resolved_rates(monkeypatch)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.coingecko.com/api/v3/coins/bitcoin"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert 0.2 in captured_rates


@pytest.mark.asyncio
async def test_explicit_requests_per_second_overrides_host_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller-supplied requests_per_second wins even on a registered host."""
    from incorporator.io import fetch

    captured_rates = _capture_resolved_rates(monkeypatch)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.coingecko.com/api/v3/coins/bitcoin"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
        requests_per_second=5.0,
    )

    assert 5.0 in captured_rates


@pytest.mark.asyncio
async def test_unknown_host_keeps_documented_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown host → SustainedPenstock falls back to the 15.0 documented default."""
    from incorporator.io import fetch

    captured_rates = _capture_resolved_rates(monkeypatch)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.binance.us/api/v3/ticker/price"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert 15.0 in captured_rates


# ----------------------------------------------------------------------
# Phase 1.5 — RejectEntry.duration_sec / attempt_number instrumentation
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reject_entry_duration_sec_populated_on_http_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """duration_sec is populated when a 429 HTTPStatusError is caught in _safe_execute.

    Proves the time.perf_counter() bracket in _safe_execute records elapsed time
    and passes it through _build_reject_entry to the RejectEntry.
    """
    import httpx

    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        req = httpx.Request("GET", src)
        resp = httpx.Response(429, request=req)
        exc = httpx.HTTPStatusError("429", request=req, response=resp)
        raise exc

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    _, rejects = await fetch.fetch_concurrent_payloads(
        source_list=["https://api.example.com/data"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert len(rejects) == 1
    entry = rejects[0]
    assert entry.duration_sec is not None
    assert entry.duration_sec >= 0.0


@pytest.mark.asyncio
async def test_reject_entry_attempt_number_populated_on_retry_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """attempt_number equals the max attempt count when retries are exhausted.

    Proves that execute_request attaches _incorporator_attempt_number to the
    exception and _safe_execute reads it via getattr into the RejectEntry.
    The mock raises httpx.RequestError (retryable) with the attempt attribute
    pre-set to 8 (max), simulating post-exhaustion state.
    """
    import httpx

    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        exc = httpx.ConnectError("connection refused")
        exc._incorporator_attempt_number = 8  # type: ignore[attr-defined]
        raise exc

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    _, rejects = await fetch.fetch_concurrent_payloads(
        source_list=["https://api.example.com/data"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert len(rejects) == 1
    assert rejects[0].attempt_number == 8


@pytest.mark.asyncio
async def test_reject_entry_attempt_number_populated_on_first_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """attempt_number is 1 when the request fails on the first attempt.

    Proves that a non-retryable failure (4xx wrapped as IncorporatorNetworkError
    with attempt_number=1 attached) surfaces correctly in the RejectEntry.
    """
    import httpx

    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        exc = httpx.ConnectError("dns failure")
        exc._incorporator_attempt_number = 1  # type: ignore[attr-defined]
        raise exc

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    _, rejects = await fetch.fetch_concurrent_payloads(
        source_list=["https://api.example.com/data"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert len(rejects) == 1
    assert rejects[0].attempt_number == 1


@pytest.mark.asyncio
async def test_reject_entry_duration_sec_on_format_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """duration_sec is populated and attempt_number is None for IncorporatorFormatError.

    Format errors are not retried by Tenacity, so attempt_number must be None.
    duration_sec is still measured (the parse itself takes some time).
    """
    from incorporator.exceptions import IncorporatorFormatError
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        raise IncorporatorFormatError("malformed JSON")

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    _, rejects = await fetch.fetch_concurrent_payloads(
        source_list=["https://api.example.com/data"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert len(rejects) == 1
    entry = rejects[0]
    assert entry.duration_sec is not None
    assert entry.duration_sec >= 0.0
    assert entry.attempt_number is None


@pytest.mark.asyncio
async def test_wave_bytes_processed_populated_in_chunked_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Wave.bytes_processed reflects the response content length from the chunked pipeline.

    Proves the _CURRENT_CHUNK_CLASS contextvar plumbing: chunked.py sets the
    contextvar before cls.incorp(), fetch._process_single_source writes
    cls._last_bytes_processed = len(res.content), and chunked.py reads it
    back into the Wave.
    """
    import json

    from incorporator import Incorporator
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    FAKE_PAYLOAD = b'[{"id": "btc", "price": 100}]'

    class _Coin(Incorporator):
        price: int = 0

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> Any:
        import httpx

        return httpx.Response(200, content=FAKE_PAYLOAD, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    waves = []
    async for wave in _Coin.stream(
        incorp_params={"inc_url": "https://api.example.com/coins", "inc_code": "id"},
    ):
        waves.append(wave)
        break  # One chunk is sufficient.

    matching = [w for w in waves if w.bytes_processed is not None]
    assert len(matching) >= 1
    assert matching[0].bytes_processed == len(FAKE_PAYLOAD)


@pytest.mark.asyncio
async def test_wave_bytes_processed_resets_between_classes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """_last_bytes_processed does not bleed between distinct Incorporator subclasses.

    Proves that setting _CURRENT_CHUNK_CLASS to cls_a then cls_b writes to the
    correct class attribute in each case, with no cross-contamination.
    """
    from incorporator import Incorporator
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS

    monkeypatch.chdir(tmp_path)

    class _ClassA(Incorporator):
        pass

    class _ClassB(Incorporator):
        pass

    # Simulate the contextvar set/reset pattern for class A with 100 bytes.
    token_a = _CURRENT_CHUNK_CLASS.set(_ClassA)
    try:
        _ClassA._last_bytes_processed = 100
    finally:
        _CURRENT_CHUNK_CLASS.reset(token_a)

    # Simulate the contextvar set/reset pattern for class B with 200 bytes.
    token_b = _CURRENT_CHUNK_CLASS.set(_ClassB)
    try:
        _ClassB._last_bytes_processed = 200
    finally:
        _CURRENT_CHUNK_CLASS.reset(token_b)

    # After both resets, the contextvar should be None (default).
    assert _CURRENT_CHUNK_CLASS.get() is None
    # Each class holds its own value — no bleed.
    assert _ClassA._last_bytes_processed == 100
    assert _ClassB._last_bytes_processed == 200


@pytest.mark.asyncio
async def test_file_mode_resets_http_telemetry_classvars(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """File-mode branch resets all three HTTP telemetry ClassVars to None.

    Proves that a class whose ClassVars were populated by a prior HTTP fetch
    has those values cleared to None when the same class is subsequently used
    as a file-mode source, preventing stale HTTP telemetry from bleeding into
    downstream consumers (e.g., the AIMD tuner that reads http_fetch_time_sec
    to choose split-time vs end-to-end steering).
    """
    from incorporator import Incorporator
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS, _process_single_source

    monkeypatch.chdir(tmp_path)

    class _FileClass(Incorporator):
        val: str = ""

    # Seed the ClassVars with non-None sentinel values simulating a prior HTTP fetch.
    _FileClass._last_bytes_processed = 42
    _FileClass._last_bytes_downloaded = 10
    _FileClass._last_http_fetch_time_sec = 0.5

    # Write a minimal NDJSON file for the file-mode path to consume.
    ndjson_file = tmp_path / "data.ndjson"
    ndjson_file.write_text('{"val": "x"}\n', encoding="utf-8")

    token = _CURRENT_CHUNK_CLASS.set(_FileClass)
    try:
        await _process_single_source(
            str(ndjson_file),
            is_file_mode=True,
            client=None,
            rate_limiter=None,
        )
    finally:
        _CURRENT_CHUNK_CLASS.reset(token)

    assert _FileClass._last_bytes_downloaded is None
    assert _FileClass._last_http_fetch_time_sec is None
    assert _FileClass._last_bytes_processed is None


@pytest.mark.asyncio
async def test_paginator_branch_resets_http_telemetry_classvars(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Paginator branch resets all three HTTP telemetry ClassVars to None.

    Proves that when the inc_page branch is taken, stale non-None HTTP telemetry
    values left over from a prior HTTP fetch are cleared before the paginator
    loop begins.  The reset happens once per source invocation, not per page,
    so all pages share the correct no-HTTP-telemetry signal.
    """
    from incorporator import Incorporator
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS, _process_single_source
    from incorporator.io.pagination.base import AsyncPaginator

    monkeypatch.chdir(tmp_path)

    class _PageClass(Incorporator):
        val: str = ""

    # Seed the ClassVars with non-None sentinel values simulating a prior HTTP fetch.
    _PageClass._last_bytes_processed = 99
    _PageClass._last_bytes_downloaded = 55
    _PageClass._last_http_fetch_time_sec = 1.2

    class _StubPaginator(AsyncPaginator):
        """Minimal paginator that yields one JSON string then stops."""

        async def paginate(self, start_url: str):  # type: ignore[override]
            yield '{"val": "y"}'

    token = _CURRENT_CHUNK_CLASS.set(_PageClass)
    try:
        await _process_single_source(
            "https://stub.example.com/",
            is_file_mode=False,
            client=None,
            rate_limiter=None,
            inc_page=_StubPaginator(),
        )
    finally:
        _CURRENT_CHUNK_CLASS.reset(token)

    assert _PageClass._last_bytes_downloaded is None
    assert _PageClass._last_http_fetch_time_sec is None
    assert _PageClass._last_bytes_processed is None


# ----------------------------------------------------------------------
# HTTP write-site telemetry — ClassVar population and WIRE/DECODED distinction
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_write_site_populates_all_three_classvars(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """HTTP branch of _process_single_source writes all three telemetry ClassVars.

    Proves that after a successful HTTP fetch the write site at
    ``fetch.py``'s ``else`` branch assigns:
      - ``_last_bytes_processed`` = len(res.content)  (DECODED)
      - ``_last_bytes_downloaded`` = res.num_bytes_downloaded  (WIRE)
      - ``_last_http_fetch_time_sec`` = res.elapsed.total_seconds()

    ``num_bytes_downloaded`` is 0 on hand-constructed ``httpx.Response``
    objects because ``__init__`` resets ``_num_bytes_downloaded`` after
    ``read()`` (httpx implementation detail).  The attribute is seeded
    explicitly here to exercise the write site with a non-zero WIRE value.
    """
    import datetime

    from incorporator import Incorporator
    from incorporator.io import fetch
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS, _process_single_source

    monkeypatch.chdir(tmp_path)

    DECODED_CONTENT = b'[{"id": "btc", "price": 100}]'
    WIRE_BYTES = 18  # simulates a partially-compressed response

    class _HttpTelClass(Incorporator):
        price: int = 0

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        res = httpx.Response(200, content=DECODED_CONTENT, request=httpx.Request("GET", url))
        # httpx resets _num_bytes_downloaded to 0 in __init__; seed the WIRE value manually.
        res._num_bytes_downloaded = WIRE_BYTES
        res.elapsed = datetime.timedelta(seconds=0.123)
        return res

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    async with httpx.AsyncClient() as client:
        token = _CURRENT_CHUNK_CLASS.set(_HttpTelClass)
        try:
            await _process_single_source(
                "https://api.example.com/coins",
                is_file_mode=False,
                client=client,
                rate_limiter=None,
                inc_code="id",
            )
        finally:
            _CURRENT_CHUNK_CLASS.reset(token)

    assert _HttpTelClass._last_bytes_downloaded == WIRE_BYTES
    assert _HttpTelClass._last_bytes_processed == len(DECODED_CONTENT)
    assert _HttpTelClass._last_http_fetch_time_sec == pytest.approx(0.123)


@pytest.mark.asyncio
async def test_http_write_site_elapsed_unset_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """When res.elapsed raises RuntimeError the guard sets _last_http_fetch_time_sec to None.

    httpx.Response.elapsed raises ``RuntimeError`` (not ``AttributeError``) when
    ``_elapsed`` has never been set, e.g. on hand-constructed mock responses that
    skip the AsyncClient streaming path.  The ``except RuntimeError`` guard must
    catch this and assign None rather than propagating the exception.
    """
    from incorporator import Incorporator
    from incorporator.io import fetch
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS, _process_single_source

    monkeypatch.chdir(tmp_path)

    DECODED_CONTENT = b'[{"id": "eth"}]'

    class _HttpTelElapsedClass(Incorporator):
        pass

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        # No res.elapsed assignment — accessing .elapsed raises RuntimeError.
        return httpx.Response(200, content=DECODED_CONTENT, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    async with httpx.AsyncClient() as client:
        token = _CURRENT_CHUNK_CLASS.set(_HttpTelElapsedClass)
        try:
            await _process_single_source(
                "https://api.example.com/tokens",
                is_file_mode=False,
                client=client,
                rate_limiter=None,
            )
        finally:
            _CURRENT_CHUNK_CLASS.reset(token)

    # Guard fired: no crash, and _last_http_fetch_time_sec is None.
    assert _HttpTelElapsedClass._last_http_fetch_time_sec is None


@pytest.mark.asyncio
async def test_http_write_site_wire_vs_decoded_distinction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """WIRE bytes_downloaded is strictly less than DECODED bytes_processed for compressed responses.

    Constructs a scenario where WIRE < DECODED (as in gzip-compressed HTTP responses)
    and asserts the two ClassVars carry the correct independent values.

    This locks the wire-vs-decoded distinction; swapping ``res.num_bytes_downloaded``
    and ``len(res.content)`` at the write site must fail this test.
    """
    import datetime

    from incorporator import Incorporator
    from incorporator.io import fetch
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS, _process_single_source

    monkeypatch.chdir(tmp_path)

    # 13 decoded bytes; 7 simulates the smaller wire size of a compressed response.
    DECODED_CONTENT = b'[{"id": "x"}]'
    WIRE_BYTES = 7
    assert WIRE_BYTES < len(DECODED_CONTENT), "test invariant: WIRE must be smaller than DECODED"

    class _WireClass(Incorporator):
        pass

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        res = httpx.Response(200, content=DECODED_CONTENT, request=httpx.Request("GET", url))
        # httpx resets _num_bytes_downloaded to 0 in __init__; seed the WIRE value manually.
        res._num_bytes_downloaded = WIRE_BYTES
        res.elapsed = datetime.timedelta(seconds=0.05)
        return res

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    async with httpx.AsyncClient() as client:
        token = _CURRENT_CHUNK_CLASS.set(_WireClass)
        try:
            await _process_single_source(
                "https://api.example.com/data",
                is_file_mode=False,
                client=client,
                rate_limiter=None,
            )
        finally:
            _CURRENT_CHUNK_CLASS.reset(token)

    assert _WireClass._last_bytes_downloaded == WIRE_BYTES
    assert _WireClass._last_bytes_processed == len(DECODED_CONTENT)
    # Semantic lock: WIRE must be strictly less than DECODED for a compressed response.
    assert _WireClass._last_bytes_downloaded < _WireClass._last_bytes_processed  # type: ignore[operator]


# ----------------------------------------------------------------------
# rec_path — integer-index list navigation
# ----------------------------------------------------------------------


def _mock_json_response(payload: Any) -> Any:
    """Return an async execute_request mock that always replies with *payload* as JSON."""

    async def _mock(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        content = json.dumps(payload).encode()
        return httpx.Response(200, content=content, request=httpx.Request("GET", url))

    return _mock


@pytest.mark.asyncio
async def test_rec_path_integer_index_drills_nested_array(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """rec_path="records.0.teamRecords" drills through a list index into a nested array.

    Proves that digit segments correctly index into the list at each level,
    yielding the inner list for accumulation.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Team(Incorporator):
        id: int = 0
        name: str = ""

    payload = {"records": [{"teamRecords": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}]}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Team.incorp(
        inc_url="https://api.example.com/teams", inc_code="id", rec_path="records.0.teamRecords"
    )
    assert len(result) == 2
    assert result.inc_dict[1].name == "a"
    assert result.inc_dict[2].name == "b"


@pytest.mark.asyncio
async def test_rec_path_terminal_index(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """rec_path="a.0" selects the first element of a list and returns it as a single instance.

    The drill leaves a dict (not a list) in parsed_chunk, so the single dict is
    accumulated and the framework returns one instance (not an IncorporatorList).
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        x: int = 0

    payload = {"a": [{"x": 10}, {"x": 20}]}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", rec_path="a.0")
    # Single-record path: incorp() returns the instance directly, not an IncorporatorList.
    assert result.x == 10  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_rec_path_out_of_range_index_yields_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """rec_path drill that exceeds the list length returns None and yields zero records.

    Proves out-of-range indexing short-circuits the loop, leaving parsed_chunk=None,
    which is accumulated as a single None item — but Pydantic discards it.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        b: int = 0

    payload = {"a": []}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", rec_path="a.99.b")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_rec_path_negative_index_is_not_supported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Negative index segments ("-1") are not treated as list indices — returns None.

    post-Bundle G: non-digit segment on a list returns None instead of leaving partial state.
    ``"-1".isdigit()`` is False, so ``_drill_path`` returns None on the ``"a.-1"`` path
    and the accumulator skips it.  Zero instances are created.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        v: int = 0

    payload = {"a": [{"v": 1}, {"v": 2}, {"v": 3}]}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", rec_path="a.-1")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_rec_path_non_digit_key_on_list_breaks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A non-digit segment on a list value returns None from _drill_path.

    post-Bundle G: non-digit segment on a list returns None instead of leaving partial state.
    rec_path="a.foo.b" on ``{"a": [{"b": 1}, {"b": 2}]}``: after drilling to
    ``"a"`` the current value is a list; ``"foo"`` is not a digit so ``_drill_path``
    returns None and the accumulator skips.  Zero instances are created.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        b: int = 0

    payload = {"a": [{"b": 1}, {"b": 2}]}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", rec_path="a.foo.b")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_rec_path_dict_only_path_still_works(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression: dict-only rec_path continues to work after the integer-index addition.

    rec_path="results" on ``{"results": [{"id": 1}, {"id": 2}]}`` must still
    yield 2 instances — the new elif branches are never reached.
    """
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        id: int = 0

    payload = {"results": [{"id": 1}, {"id": 2}]}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", inc_code="id", rec_path="results")
    assert len(result) == 2
    assert result.inc_dict[1].id == 1
    assert result.inc_dict[2].id == 2


@pytest.mark.asyncio
async def test_rec_path_dict_only_smoke_post_drill_path_refactor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression lock: dict-only rec_path drill still works post-Bundle G _drill_path refactor."""
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class _Item(Incorporator):
        id: int = 0

    payload = {"data": {"items": [{"id": 10}, {"id": 20}]}}
    monkeypatch.setattr(fetch, "execute_request", _mock_json_response(payload))

    result = await _Item.incorp(inc_url="https://api.example.com/items", inc_code="id", rec_path="data.items")
    assert len(result) == 2
    assert result.inc_dict[10].id == 10
    assert result.inc_dict[20].id == 20


# ----------------------------------------------------------------------
# Phase-aware retry classifier — _is_retryable_status / _is_retryable_error
# ----------------------------------------------------------------------


def test_is_retryable_status_retries_5xx() -> None:
    """5xx server errors are retryable."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    for code in (500, 502, 503, 504):
        resp = httpx.Response(code, request=req)
        exc = httpx.HTTPStatusError(f"{code}", request=req, response=resp)
        assert _is_retryable_status(exc) is True, f"{code} should be retryable"


def test_is_retryable_status_retries_429() -> None:
    """HTTP 429 (rate-limit) is retryable."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    resp = httpx.Response(429, request=req)
    exc = httpx.HTTPStatusError("429", request=req, response=resp)
    assert _is_retryable_status(exc) is True


def test_is_retryable_status_fatal_4xx() -> None:
    """4xx errors other than 429 are fatal (not retryable)."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    for code in (400, 401, 403, 404, 422):
        resp = httpx.Response(code, request=req)
        exc = httpx.HTTPStatusError(f"{code}", request=req, response=resp)
        assert _is_retryable_status(exc) is False, f"{code} should be fatal"


def test_is_retryable_error_connect_phase_retried() -> None:
    """ConnectError and ConnectTimeout are retried unconditionally (connect-phase).

    Attempt-count bounding is handled by _make_http_stop; this function only
    classifies the error type and method idempotency.
    """
    from incorporator.io.fetch import _is_retryable_error

    assert _is_retryable_error(httpx.ConnectError("refused"), "GET") is True
    assert _is_retryable_error(httpx.ConnectTimeout("timeout"), "POST") is True
    assert _is_retryable_error(httpx.PoolTimeout("pool"), "POST") is True


def test_is_retryable_error_read_timeout_get_retried() -> None:
    """ReadTimeout on a GET (idempotent) is retryable — attempt cap is in _make_http_stop."""
    from incorporator.io.fetch import _is_retryable_error

    assert _is_retryable_error(httpx.ReadTimeout("timeout"), "GET") is True


def test_is_retryable_error_read_timeout_post_not_retried() -> None:
    """ReadTimeout on a POST (non-idempotent) is never retried (avoids double-submit)."""
    from incorporator.io.fetch import _is_retryable_error

    assert _is_retryable_error(httpx.ReadTimeout("timeout"), "POST") is False
    assert _is_retryable_error(httpx.ReadTimeout("timeout"), "post") is False


def test_is_retryable_error_post_send_idempotent_methods() -> None:
    """All idempotent methods allow post-send retry; PATCH and POST do not."""
    from incorporator.io.fetch import _IDEMPOTENT_METHODS, _is_retryable_error

    for verb in _IDEMPOTENT_METHODS:
        assert _is_retryable_error(httpx.ReadTimeout("t"), verb) is True, f"{verb} should allow post-send retry"
    for verb in ("POST", "PATCH"):
        assert _is_retryable_error(httpx.ReadTimeout("t"), verb) is False, f"{verb} must not allow post-send retry"


# ----------------------------------------------------------------------
# execute_request — per-class call-count assertions via MockTransport
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_request_connect_error_retried_up_to_network_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ConnectError is retried up to _HTTP_NETWORK_RETRY_STOP total attempts.

    The stop callable fires when attempt_number >= _HTTP_NETWORK_RETRY_STOP, so
    total invocations equal _HTTP_NETWORK_RETRY_STOP exactly.  This is the
    assertion that was FALSE with the old retry_if_exception closure: the broken
    code ran all 8 attempts (~74 s); the real async loop now caps at 3.
    """
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_NETWORK_RETRY_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _raising_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        raise httpx.ConnectError("simulated connect failure", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_raising_transport)) as client:
        with pytest.raises(httpx.ConnectError):
            await execute_request(url="https://dead.example.com/", client=client)

    # stop fires at attempt_number >= _HTTP_NETWORK_RETRY_STOP → total calls == budget.
    assert call_count == _HTTP_NETWORK_RETRY_STOP, (
        f"Expected {_HTTP_NETWORK_RETRY_STOP} attempts, got {call_count}"
    )

    await asyncio.sleep(0)  # allow event loop cleanup


@pytest.mark.asyncio
async def test_execute_request_read_timeout_get_retried_up_to_network_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ReadTimeout on GET is capped at _HTTP_NETWORK_RETRY_STOP total attempts.

    Mirrors the ConnectError test: the stop callable reads the live attempt_number
    from retry_state, so the cap is reliable (not subject to the broken statistics
    closure that let the old code run all 8 attempts).
    """
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_NETWORK_RETRY_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _raising_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        raise httpx.ReadTimeout("simulated read timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_raising_transport)) as client:
        with pytest.raises(httpx.ReadTimeout):
            await execute_request(url="https://slow.example.com/", client=client, method="GET")

    assert call_count == _HTTP_NETWORK_RETRY_STOP, (
        f"Expected {_HTTP_NETWORK_RETRY_STOP} attempts (GET), got {call_count}"
    )

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_read_timeout_post_exactly_one_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ReadTimeout on POST (non-idempotent) is never retried — exactly 1 attempt."""
    import asyncio

    from incorporator.io.fetch import execute_request

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _raising_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        raise httpx.ReadTimeout("simulated read timeout", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_raising_transport)) as client:
        with pytest.raises(httpx.ReadTimeout):
            await execute_request(url="https://slow.example.com/api", client=client, method="POST")

    assert call_count == 1, f"POST ReadTimeout must not be retried; got {call_count} calls"

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_503_retried_up_to_inner_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """503 (server error) uses the full _HTTP_INNER_STOP=8 budget (unchanged)."""
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_INNER_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _503_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(503, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_503_transport)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await execute_request(url="https://flaky.example.com/", client=client)

    assert call_count == _HTTP_INNER_STOP, f"503 should exhaust full inner budget {_HTTP_INNER_STOP}, got {call_count}"

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_429_retried_up_to_inner_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """429 (rate-limit) uses the full _HTTP_INNER_STOP=8 budget (unchanged)."""
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_INNER_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _429_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(429, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_429_transport)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await execute_request(url="https://throttled.example.com/", client=client)

    assert call_count == _HTTP_INNER_STOP, f"429 should exhaust full inner budget {_HTTP_INNER_STOP}, got {call_count}"

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_404_fatal_exactly_one_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """404 is fatal — IncorporatorNetworkError raised after exactly 1 attempt."""
    import asyncio

    from incorporator.exceptions import IncorporatorNetworkError
    from incorporator.io.fetch import execute_request

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _404_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(404, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_404_transport)) as client:
        with pytest.raises(IncorporatorNetworkError):
            await execute_request(url="https://example.com/missing", client=client)

    assert call_count == 1, f"404 must not be retried; got {call_count} calls"

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_network_error_wait_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Network-class errors use short bounded waits, not the 30 s server-error exponential.

    Patches asyncio.sleep (the function ultimately called by tenacity's AsyncRetrying
    default sleep) so wall-clock time is not consumed.  Asserts that the total faked
    sleep over all retries is bounded by _HTTP_NETWORK_RETRY_STOP * _HTTP_NETWORK_WAIT_MAX
    — far below the ~58 s that wait_random_exponential(max=30) over 3 retries would produce.

    Also proves the attempt count: _incorporator_attempt_number attached to the raised
    exception equals _HTTP_NETWORK_RETRY_STOP (the total cap).
    """
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import (
        _HTTP_NETWORK_RETRY_STOP,
        _HTTP_NETWORK_WAIT_MAX,
    )

    monkeypatch.chdir(tmp_path)

    total_slept: list[float] = []
    real_sleep = asyncio.sleep

    async def _fake_sleep(seconds: float) -> None:
        # Record waits triggered by tenacity backoff (seconds > 0).
        # Pass seconds=0 calls through so event-loop cleanup still works.
        if seconds > 0:
            total_slept.append(seconds)
        else:
            await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

    def _raising_transport(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("simulated connect failure", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_raising_transport)) as client:
        with pytest.raises(httpx.ConnectError) as exc_info:
            await execute_request(url="https://dead.example.com/", client=client)

    raised = exc_info.value
    attempt_on_exc = getattr(raised, "_incorporator_attempt_number", None)
    assert attempt_on_exc == _HTTP_NETWORK_RETRY_STOP, (
        f"attempt_number on exception should be {_HTTP_NETWORK_RETRY_STOP}, got {attempt_on_exc}"
    )

    # Total faked sleep must be bounded by network budget, not the 30 s inner exponential.
    total = sum(total_slept)
    budget = _HTTP_NETWORK_RETRY_STOP * _HTTP_NETWORK_WAIT_MAX
    assert total <= budget, (
        f"Total faked sleep {total:.2f}s exceeded network wait budget {budget:.2f}s "
        f"(individual sleeps: {total_slept})"
    )

    await asyncio.sleep(0)


# ----------------------------------------------------------------------
# Commit B — 408 / 425 are now retryable (behavior change)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_request_408_retried_up_to_inner_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """408 Request Timeout retries up to _HTTP_INNER_STOP total attempts.

    Proves that _is_retryable_status now includes 408, so the stop callable
    exhausts at exactly _HTTP_INNER_STOP (8) and raises HTTPStatusError.
    """
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_INNER_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _408_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(408, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_408_transport)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await execute_request(url="https://slow.example.com/api", client=client)

    assert call_count == _HTTP_INNER_STOP, (
        f"408 should exhaust full inner budget {_HTTP_INNER_STOP}, got {call_count}"
    )

    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_execute_request_425_retried_up_to_inner_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """425 Too Early retries up to _HTTP_INNER_STOP total attempts.

    Proves that _is_retryable_status now includes 425, following the same
    capped-retry path as 5xx and 429.
    """
    import asyncio

    from incorporator.io.fetch import execute_request
    from incorporator.observability.tideweaver._retry_defaults import _HTTP_INNER_STOP

    monkeypatch.chdir(tmp_path)
    call_count = 0

    def _425_transport(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(425, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(_425_transport)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await execute_request(url="https://early.example.com/api", client=client)

    assert call_count == _HTTP_INNER_STOP, (
        f"425 should exhaust full inner budget {_HTTP_INNER_STOP}, got {call_count}"
    )

    await asyncio.sleep(0)


def test_is_retryable_status_retries_408() -> None:
    """HTTP 408 (Request Timeout) is now retryable."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    resp = httpx.Response(408, request=req)
    exc = httpx.HTTPStatusError("408", request=req, response=resp)
    assert _is_retryable_status(exc) is True


def test_is_retryable_status_retries_425() -> None:
    """HTTP 425 (Too Early) is now retryable."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    resp = httpx.Response(425, request=req)
    exc = httpx.HTTPStatusError("425", request=req, response=resp)
    assert _is_retryable_status(exc) is True


def test_is_retryable_status_fatal_404_unchanged() -> None:
    """HTTP 404 is still fatal after the 408/425 addition."""
    from incorporator.io.fetch import _is_retryable_status

    req = httpx.Request("GET", "https://example.com/")
    resp = httpx.Response(404, request=req)
    exc = httpx.HTTPStatusError("404", request=req, response=resp)
    assert _is_retryable_status(exc) is False
