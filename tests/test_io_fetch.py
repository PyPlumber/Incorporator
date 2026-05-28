"""Unit tests for ``incorporator.io.fetch`` helpers."""

import json
from pathlib import Path
from typing import Any

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
        json.dumps({"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}) + "\n"
        + json.dumps({"id": "ethereum", "symbol": "eth", "name": "Ethereum"}) + "\n",
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

    async def fake_process_single(
        src: str, is_file_mode: bool, client: Any, rate_limiter: Any, **_kw: Any
    ) -> list:
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

    result = await _Team.incorp(inc_url="https://api.example.com/teams", inc_code="id", rec_path="records.0.teamRecords")
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

    result = await _Item.incorp(
        inc_url="https://api.example.com/items", inc_code="id", rec_path="data.items"
    )
    assert len(result) == 2
    assert result.inc_dict[10].id == 10
    assert result.inc_dict[20].id == 20
