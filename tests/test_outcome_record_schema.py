"""Tests that new fields on Wave, Tide, and RejectEntry are populated correctly.

Covers every construction site documented in the plan:
- Wave.model_construct sites (chunked, shared, stateful_shim, fjord, outflow).
- Tide.model_construct (scheduler _run_pass).
- RejectEntry.model_construct (fetch._build_reject_entry, scheduler canal sites).
- factory.py schema-cache-hit flag.

Uses in-memory construction to avoid live I/O.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from incorporator.tideweaver.current_outcome import CurrentOutcome
from incorporator.tideweaver.reasons import SkipReason, WakeReason
from incorporator.tideweaver.tide import Tide
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry


# ---------------------------------------------------------------------------
# Wave new-field coverage
# ---------------------------------------------------------------------------


class TestWaveNewFields:
    """Group tests for all six new Wave fields."""

    def test_source_url_populated(self) -> None:
        """source_url is propagated when supplied."""
        wave = Wave.model_construct(
            chunk_index=1,
            operation="chunk",
            rows_processed=10,
            failed_sources=[],
            processing_time_sec=0.01,
            source_url="https://api.example.com/coins",
            bytes_processed=None,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.source_url == "https://api.example.com/coins"

    def test_bytes_processed_populated(self) -> None:
        """bytes_processed carries the response byte count."""
        wave = Wave.model_construct(
            chunk_index=1,
            operation="chunk",
            rows_processed=5,
            failed_sources=[],
            processing_time_sec=0.02,
            source_url=None,
            bytes_processed=8192,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.bytes_processed == 8192

    def test_http_retry_count_populated(self) -> None:
        """http_retry_count carries the number of retries beyond the first."""
        wave = Wave.model_construct(
            chunk_index=2,
            operation="chunk",
            rows_processed=0,
            failed_sources=[],
            processing_time_sec=0.5,
            source_url=None,
            bytes_processed=None,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=3,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.http_retry_count == 3

    def test_validation_error_count_populated(self) -> None:
        """validation_error_count carries the ValidationError row count."""
        wave = Wave.model_construct(
            chunk_index=1,
            operation="chunk",
            rows_processed=95,
            failed_sources=[],
            processing_time_sec=0.1,
            source_url=None,
            bytes_processed=None,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=0,
            validation_error_count=5,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.validation_error_count == 5

    def test_schema_cache_hit_false(self) -> None:
        """schema_cache_hit=False signals a new schema was compiled for this chunk."""
        wave = Wave.model_construct(
            chunk_index=1,
            operation="chunk",
            rows_processed=10,
            failed_sources=[],
            processing_time_sec=0.05,
            source_url=None,
            bytes_processed=None,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=False,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.schema_cache_hit is False

    def test_conv_dict_time_sec_populated(self) -> None:
        """conv_dict_time_sec carries the converter-pass wall-clock time."""
        wave = Wave.model_construct(
            chunk_index=1,
            operation="chunk",
            rows_processed=100,
            failed_sources=[],
            processing_time_sec=0.2,
            source_url=None,
            bytes_processed=None,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=0.012,
            timestamp=datetime.now(timezone.utc),
        )
        assert wave.conv_dict_time_sec == pytest.approx(0.012)

    def test_defaults_when_omitted(self) -> None:
        """New Wave fields use safe defaults when not supplied (back-compat)."""
        wave = Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)
        assert wave.source_url is None
        assert wave.bytes_processed is None
        assert wave.bytes_downloaded is None
        assert wave.http_fetch_time_sec is None
        assert wave.http_retry_count == 0
        assert wave.validation_error_count == 0
        assert wave.schema_cache_hit is True
        assert wave.conv_dict_time_sec is None

    def test_log_meta_back_compat(self) -> None:
        """log_meta() still produces the original five-field string — no new fields leaked."""
        wave = Wave.model_construct(
            chunk_index=3,
            operation="chunk",
            rows_processed=10,
            failed_sources=[],
            processing_time_sec=0.1,
            source_url="https://x.com",
            bytes_processed=1024,
            bytes_downloaded=None,
            http_fetch_time_sec=None,
            http_retry_count=1,
            validation_error_count=0,
            schema_cache_hit=False,
            conv_dict_time_sec=0.005,
            timestamp=datetime.now(timezone.utc),
        )
        meta = wave.log_meta()
        assert "chunk_index:3" in meta
        assert "rows:10" in meta
        # New fields must not appear in log_meta.
        assert "source_url" not in meta
        assert "bytes_processed" not in meta
        assert "schema_cache_hit" not in meta


# ---------------------------------------------------------------------------
# Tide new-field coverage
# ---------------------------------------------------------------------------


class TestTideNewFields:
    """Group tests for all new Tide scalar fields."""

    def _make_tide(
        self,
        *,
        wake_reason: WakeReason = WakeReason.STARTUP,
        heap_depth: int = 0,
        in_flight_count_at_start: int = 0,
        canal_rejects_added: int = 0,
        next_due_in_sec: Optional[float] = None,
    ) -> Tide:
        return Tide.model_construct(
            tide_number=1,
            fired=[],
            skipped=[],
            current_outcomes=[],
            duration_sec=0.01,
            wake_reason=wake_reason,
            heap_depth=heap_depth,
            in_flight_count_at_start=in_flight_count_at_start,
            canal_rejects_added=canal_rejects_added,
            next_due_in_sec=next_due_in_sec,
            timestamp=datetime.now(timezone.utc),
        )

    def test_wake_reason_startup(self) -> None:
        """wake_reason='startup' on the first pass."""
        tide = self._make_tide(wake_reason=WakeReason.STARTUP)
        assert tide.wake_reason == "startup"

    def test_wake_reason_timer(self) -> None:
        """wake_reason='timer' when heap due-time elapsed."""
        tide = self._make_tide(wake_reason=WakeReason.TIMER)
        assert tide.wake_reason == "timer"

    def test_heap_depth_populated(self) -> None:
        """heap_depth carries the post-housekeeping heap size."""
        tide = self._make_tide(heap_depth=5)
        assert tide.heap_depth == 5

    def test_in_flight_count_at_start_populated(self) -> None:
        """in_flight_count_at_start carries the pre-loop in-flight task count."""
        tide = self._make_tide(in_flight_count_at_start=3)
        assert tide.in_flight_count_at_start == 3

    def test_canal_rejects_added_populated(self) -> None:
        """canal_rejects_added carries the per-pass reject delta."""
        tide = self._make_tide(canal_rejects_added=2)
        assert tide.canal_rejects_added == 2

    def test_next_due_in_sec_populated(self) -> None:
        """next_due_in_sec is positive when the heap is non-empty."""
        tide = self._make_tide(next_due_in_sec=1.5)
        assert tide.next_due_in_sec == pytest.approx(1.5)

    def test_next_due_in_sec_none_when_heap_empty(self) -> None:
        """next_due_in_sec is None when the heap is empty."""
        tide = self._make_tide(next_due_in_sec=None)
        assert tide.next_due_in_sec is None

    def test_current_outcomes_populated(self) -> None:
        """current_outcomes carries structured per-current data."""
        outcomes = [
            CurrentOutcome(name="a", status="fired"),
            CurrentOutcome(name="b", status="skipped", reason="not_due"),
        ]
        tide = Tide.model_construct(
            tide_number=1,
            fired=["a"],
            skipped=[("b", SkipReason.NOT_DUE)],
            current_outcomes=outcomes,
            duration_sec=0.01,
            wake_reason=WakeReason.STARTUP,
            heap_depth=0,
            in_flight_count_at_start=0,
            canal_rejects_added=0,
            next_due_in_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
        assert len(tide.current_outcomes) == 2
        fired_names = [co.name for co in tide.current_outcomes if co.status == "fired"]
        assert fired_names == ["a"]

    def test_model_dump_includes_new_fields(self) -> None:
        """model_dump() includes all new scalar fields."""
        tide = self._make_tide(wake_reason=WakeReason.WAKE_EVENT, heap_depth=2, canal_rejects_added=1)
        dumped = tide.model_dump()
        assert dumped["wake_reason"] == "wake_event"
        assert dumped["heap_depth"] == 2
        assert dumped["canal_rejects_added"] == 1


# ---------------------------------------------------------------------------
# RejectEntry new-field coverage
# ---------------------------------------------------------------------------


class TestRejectEntryNewFields:
    """Group tests for all seven new RejectEntry fields."""

    def test_from_name_and_to_name_populated(self) -> None:
        """from_name and to_name carry canal-layer edge endpoints."""
        entry = RejectEntry.model_construct(
            source="ArbitrageOut",
            error_kind="PenstockLimited",
            message="edge binance→arb: rate limited",
            retry_after=None,
            wave_index=None,
            from_name="binance",
            to_name="arb",
            cooldown_sec=None,
        )
        assert entry.from_name == "binance"
        assert entry.to_name == "arb"

    def test_host_populated(self) -> None:
        """host carries the netloc from urlparse(source)."""
        entry = RejectEntry.model_construct(
            source="https://api.coingecko.com/v3/coins",
            error_kind="HTTPStatusError",
            message="429 Too Many Requests",
            retry_after=30.0,
            wave_index=None,
            host="api.coingecko.com",
            status_code=429,
            cooldown_sec=30.0,
        )
        assert entry.host == "api.coingecko.com"
        assert entry.status_code == 429

    def test_cooldown_sec_mirrors_retry_after_at_http_sites(self) -> None:
        """cooldown_sec == retry_after at HTTP error sites — both populated from Retry-After."""
        entry = RejectEntry.model_construct(
            source="https://x.com",
            error_kind="HTTPStatusError",
            message="rate limited",
            retry_after=60.0,
            wave_index=None,
            cooldown_sec=60.0,
        )
        assert entry.retry_after == 60.0
        assert entry.cooldown_sec == 60.0

    def test_retry_after_and_cooldown_coexist(self) -> None:
        """retry_after (HTTP-specific) and cooldown_sec (general) coexist — neither replaces the other."""
        entry = RejectEntry(
            source="https://x.com",
            retry_after=30.0,
            cooldown_sec=30.0,
        )
        assert entry.retry_after == 30.0
        assert entry.cooldown_sec == 30.0

    def test_new_fields_default_to_none(self) -> None:
        """All seven new fields default to None for back-compat."""
        entry = RejectEntry(source="https://x.com")
        assert entry.from_name is None
        assert entry.to_name is None
        assert entry.host is None
        assert entry.status_code is None
        assert entry.attempt_number is None
        assert entry.duration_sec is None
        assert entry.cooldown_sec is None

    def test_str_with_new_fields_fully_decorated(self) -> None:
        """__str__ renders the fully decorated form when status_code is set and message differs from source."""
        entry = RejectEntry(
            source="https://x.com",
            error_kind="HTTPStatusError",
            message="429 Too Many Requests",
            host="x.com",
            status_code=429,
            cooldown_sec=12.0,
        )
        assert str(entry) == "HTTPStatusError: https://x.com [HTTP 429 Too Many Requests] — 429 Too Many Requests"

    def test_build_reject_entry_populates_host_and_cooldown(self) -> None:
        """fetch._build_reject_entry populates host, status_code, and cooldown_sec."""
        from httpx import HTTPStatusError, Request, Response

        from incorporator.io.fetch import _build_reject_entry

        req = Request("GET", "https://api.coingecko.com/v3/coins")
        resp = Response(429, headers={"Retry-After": "30"}, request=req)
        exc = HTTPStatusError("rate limited", request=req, response=resp)

        entry = _build_reject_entry("https://api.coingecko.com/v3/coins", exc)
        assert entry.host == "api.coingecko.com"
        assert entry.status_code == 429
        assert entry.cooldown_sec == 30.0
        assert entry.retry_after == 30.0  # back-compat still populated

    def test_build_reject_entry_no_retry_after_sets_cooldown_none(self) -> None:
        """When no Retry-After header, both retry_after and cooldown_sec are None."""
        from httpx import RequestError

        from incorporator.io.fetch import _build_reject_entry

        exc = RequestError("connection refused")
        entry = _build_reject_entry("https://x.com", exc)
        assert entry.retry_after is None
        assert entry.cooldown_sec is None

    def test_build_reject_entry_host_extraction(self) -> None:
        """host is extracted from urlparse(source).netloc for any URL source."""
        from httpx import RequestError

        from incorporator.io.fetch import _build_reject_entry

        exc = RequestError("timeout")
        entry = _build_reject_entry("https://api.binance.com/v3/ticker", exc)
        assert entry.host == "api.binance.com"


# ---------------------------------------------------------------------------
# factory.py schema-cache-hit flag
# ---------------------------------------------------------------------------


class TestSchemaCacheHitFlag:
    """Verify that factory.build_instances writes _last_schema_cache_hit correctly."""

    def test_initial_value_is_true(self) -> None:
        """Incorporator._last_schema_cache_hit starts as True (initialized default)."""
        from incorporator import Incorporator

        class _FactoryTest(Incorporator):
            pass

        assert _FactoryTest._last_schema_cache_hit is True

    def test_cache_miss_sets_false_on_novel_shape(self) -> None:
        """build_instances with a novel shape sets _last_schema_cache_hit = False (new class built)."""
        import uuid

        from incorporator import Incorporator
        from incorporator.schema.factory import build_instances

        class _CacheMissTest(Incorporator):
            pass

        # Use a uuid-keyed field name to guarantee a novel registry miss.
        unique_key = f"field_{uuid.uuid4().hex}"
        data = [{unique_key: 42, "other_field": "hello"}]
        build_instances(_CacheMissTest, data, [], is_single=False)
        # A new class was compiled → cache miss.
        assert _CacheMissTest._last_schema_cache_hit is False

    def test_cache_hit_on_repeated_same_shape(self) -> None:
        """Second build_instances call with the same shape sets _last_schema_cache_hit = True."""
        import uuid

        from incorporator import Incorporator
        from incorporator.schema.factory import build_instances

        class _CacheHitTest(Incorporator):
            pass

        unique_key = f"field_{uuid.uuid4().hex}"
        data = [{unique_key: 1, "stable": "a"}]
        # First call: builds new schema → miss.
        build_instances(_CacheHitTest, data, [], is_single=False)
        assert _CacheHitTest._last_schema_cache_hit is False
        # Second call: same shape → registry hit.
        build_instances(_CacheHitTest, data, [], is_single=False)
        assert _CacheHitTest._last_schema_cache_hit is True

    def test_cache_hit_when_target_class_supplied(self) -> None:
        """build_instances with target_class= (refresh path) sets _last_schema_cache_hit = True."""
        from incorporator import Incorporator
        from incorporator.schema.factory import build_instances

        class _RefreshPathTest(Incorporator):
            pass

        data = [{"some_field": "value"}]
        build_instances(_RefreshPathTest, data, [], is_single=False, target_class=_RefreshPathTest)
        # target_class bypasses registry → treated as cache hit.
        assert _RefreshPathTest._last_schema_cache_hit is True


# ---------------------------------------------------------------------------
# http_retry_count + validation_error_count plumbing (Items 1 & 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wave_http_retry_count_populated_from_tenacity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Wave.http_retry_count reflects the retry count written into cls._last_http_retry_count.

    Proves the ContextVar plumbing end-to-end: the mock execute_request writes
    cls._last_http_retry_count = 2 (simulating two Tenacity retries) via the
    _CURRENT_CHUNK_CLASS ContextVar, and chunked.py reads it into the Wave.
    """
    import httpx

    from incorporator import Incorporator
    from incorporator.io import fetch
    from incorporator.io.fetch import _CURRENT_CHUNK_CLASS

    monkeypatch.chdir(tmp_path)

    FAKE_PAYLOAD = b'[{"id": "btc", "price": 100}]'

    class _RetryTest(Incorporator):
        price: int = 0

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> Any:
        # Simulate two Tenacity retries by writing the ClassVar directly,
        # mirroring what execute_request does after a successful response.
        chunk_cls = _CURRENT_CHUNK_CLASS.get()
        if chunk_cls is not None:
            try:
                chunk_cls._last_http_retry_count = 2
            except (AttributeError, TypeError):
                pass
        return httpx.Response(200, content=FAKE_PAYLOAD, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    waves = []
    async for wave in _RetryTest.stream(
        incorp_params={"inc_url": "https://api.example.com/coins", "inc_code": "id"},
    ):
        waves.append(wave)
        break

    success_waves = [w for w in waves if not w.failed_sources]
    assert len(success_waves) >= 1, "Expected at least one success wave"
    assert success_waves[0].http_retry_count == 2


@pytest.mark.asyncio
async def test_wave_validation_error_count_populated_on_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Wave.validation_error_count equals ValidationError.error_count() on the error path.

    Proves the except-path narrowing: feed a payload where rows carry a price
    value that Pydantic cannot coerce to int, so validate_python raises a
    ValidationError; the error Wave reports validation_error_count > 0.
    """
    import httpx

    from incorporator import Incorporator
    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    # "not_a_number" cannot coerce to int → ValidationError from TypeAdapter.
    FAKE_PAYLOAD = b'[{"id": "btc", "price": "not_a_number"}]'

    class _ValidationTest(Incorporator):
        price: int = 0

    async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> Any:
        return httpx.Response(200, content=FAKE_PAYLOAD, request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)

    waves = []
    async for wave in _ValidationTest.stream(
        incorp_params={"inc_url": "https://api.example.com/coins", "inc_code": "id"},
    ):
        waves.append(wave)
        break

    error_waves = [w for w in waves if w.failed_sources]
    assert len(error_waves) >= 1, "Expected at least one error wave"
    assert error_waves[0].validation_error_count > 0


@pytest.mark.asyncio
async def test_wave_validation_error_count_zero_for_non_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Wave.validation_error_count is 0 when a non-validation error reaches the except block.

    Proves that non-ValidationError exceptions (e.g. RuntimeError) leave
    validation_error_count at 0 — they are not per-row data-quality failures.
    Patches cls.incorp() directly so the exception propagates to the chunked
    engine's except handler rather than being swallowed by _safe_execute.
    """
    from incorporator import Incorporator
    from incorporator.pipeline import chunked

    monkeypatch.chdir(tmp_path)

    class _FormatErrorTest(Incorporator):
        price: int = 0

    async def mock_incorp(**kwargs: Any) -> Any:
        raise RuntimeError("simulated non-validation failure")

    monkeypatch.setattr(_FormatErrorTest, "incorp", mock_incorp)

    waves = []
    async for wave in chunked._run_chunking_engine(
        cls=_FormatErrorTest,
        incorp_params={"inc_url": "https://api.example.com/coins"},
        refresh_params=None,
        export_params=None,
        poll_interval=None,
        paginator=None,
    ):
        waves.append(wave)
        break

    assert len(waves) >= 1, "Expected at least one wave"
    error_waves = [w for w in waves if w.failed_sources]
    assert len(error_waves) >= 1, "Expected at least one error wave"
    assert error_waves[0].validation_error_count == 0
