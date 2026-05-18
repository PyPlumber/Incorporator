"""Unit tests for ``incorporator.io.fetch`` helpers."""

import json
from pathlib import Path
from typing import Any

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
    # Bad source surfaces in failed_sources rather than aborting the batch.
    assert "https://bad.test/" in failed


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
    assert sorted(failed) == sorted(urls)


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
    assert "https://boom.test/" in failed


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
    assert "https://bad.test/" in failed


# ----------------------------------------------------------------------
# Host-aware rate-limit registry (_KNOWN_API_RATE_LIMITS / _resolve_host_safe_rate)
# ----------------------------------------------------------------------


def test_resolve_host_safe_rate_coingecko() -> None:
    from incorporator.io.fetch import _resolve_host_safe_rate

    assert _resolve_host_safe_rate(["https://api.coingecko.com/api/v3/coins/markets"]) == 0.2


def test_resolve_host_safe_rate_pokeapi() -> None:
    from incorporator.io.fetch import _resolve_host_safe_rate

    assert _resolve_host_safe_rate(["https://pokeapi.co/api/v2/pokemon/pikachu"]) == 1.5


def test_resolve_host_safe_rate_nhtsa_post_endpoint() -> None:
    """Registry is method-agnostic — POST URL against a registered host still throttles.

    NHTSA vPIC supports both GET and POST against the same hostname; the
    ``DecodeVINValuesBatch`` POST in the xml-post-audit appendix must hit
    the same registry entry as a hypothetical GET drill against any other
    vpic endpoint.
    """
    from incorporator.io.fetch import _resolve_host_safe_rate

    assert _resolve_host_safe_rate(["https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"]) == 1.5


def test_resolve_host_safe_rate_most_restrictive_wins() -> None:
    """Mixed-host call uses the smallest registered rate."""
    from incorporator.io.fetch import _resolve_host_safe_rate

    mixed = [
        "https://api.coingecko.com/api/v3/coins/bitcoin",  # 0.2
        "https://pokeapi.co/api/v2/pokemon/pikachu",       # 1.5
    ]
    assert _resolve_host_safe_rate(mixed) == 0.2


def test_resolve_host_safe_rate_unknown_host_returns_none() -> None:
    from incorporator.io.fetch import _resolve_host_safe_rate

    assert _resolve_host_safe_rate(["https://api.binance.us/api/v3/ticker/price"]) is None
    assert _resolve_host_safe_rate(["https://example.com/foo"]) is None


def test_resolve_host_safe_rate_non_string_entries_skipped() -> None:
    """Non-string entries (None, Path, etc.) don't blow up the resolver."""
    from incorporator.io.fetch import _resolve_host_safe_rate

    assert _resolve_host_safe_rate([None, 42, "https://api.coingecko.com/x"]) == 0.2  # type: ignore[list-item]


@pytest.mark.asyncio
async def test_host_aware_default_applied_for_coingecko(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No explicit requests_per_second + CoinGecko URL → RateLimiter pinned at 0.2."""
    from incorporator.io import fetch

    captured: dict = {}

    real_init = fetch.RateLimiter.__init__

    def capture_init(self: Any, rps: float) -> None:
        captured["rps"] = rps
        real_init(self, rps)

    monkeypatch.setattr(fetch.RateLimiter, "__init__", capture_init)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.coingecko.com/api/v3/coins/bitcoin"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert captured["rps"] == 0.2


@pytest.mark.asyncio
async def test_explicit_requests_per_second_overrides_host_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller-supplied requests_per_second wins even on a registered host."""
    from incorporator.io import fetch

    captured: dict = {}

    real_init = fetch.RateLimiter.__init__

    def capture_init(self: Any, rps: float) -> None:
        captured["rps"] = rps
        real_init(self, rps)

    monkeypatch.setattr(fetch.RateLimiter, "__init__", capture_init)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.coingecko.com/api/v3/coins/bitcoin"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
        requests_per_second=5.0,
    )

    assert captured["rps"] == 5.0


@pytest.mark.asyncio
async def test_unknown_host_keeps_documented_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown host → RateLimiter falls back to the 15.0 documented default."""
    from incorporator.io import fetch

    captured: dict = {}

    real_init = fetch.RateLimiter.__init__

    def capture_init(self: Any, rps: float) -> None:
        captured["rps"] = rps
        real_init(self, rps)

    monkeypatch.setattr(fetch.RateLimiter, "__init__", capture_init)

    async def fake_process_single(src: str, *_a: Any, **_kw: Any) -> list:
        return [{"src": src}]

    monkeypatch.setattr(fetch, "_process_single_source", fake_process_single)

    await fetch.fetch_concurrent_payloads(
        source_list=["https://api.binance.us/api/v3/ticker/price"],
        payload_list=None,
        is_file_mode=False,
        limit=1,
    )

    assert captured["rps"] == 15.0
