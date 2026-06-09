"""Tests for architect.tune() and Tideweaver.summary().

Each test proves exactly one behaviour — the docstring states what behaviour
that is.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver.architect import (
    TuningHint,
    TuningReport,
    _tune_chunk_size,
    _tune_compound_budget,
    _tune_pass_interval,
    _tune_penstock_rate,
    _tune_retry_policy,
    _tune_surge_threshold,
    tune,
)
from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.tideweaver.scheduler import Tideweaver
from incorporator.observability.tideweaver.reasons import WakeReason
from incorporator.observability.tideweaver.tide import Tide
from incorporator.observability.tideweaver.watershed import Watershed
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry

_UTC = timezone.utc
_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=_UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wave(processing_time_sec: float, source_url: str = "https://api.example.com/data") -> Wave:
    """Build a minimal Wave with the given processing_time_sec (no HTTP telemetry fields)."""
    return Wave.model_construct(
        chunk_index=0,
        operation="stream",
        rows_processed=10,
        failed_sources=[],
        processing_time_sec=processing_time_sec,
        source_url=source_url,
        bytes_processed=None,
        bytes_downloaded=None,
        http_fetch_time_sec=None,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=None,
        timestamp=_NOW,
    )


def _wave_with_http(
    processing_time_sec: float,
    http_fetch_time_sec: float,
    source_url: str = "https://api.example.com/data",
) -> Wave:
    """Build a Wave with both processing_time_sec and http_fetch_time_sec set."""
    return Wave.model_construct(
        chunk_index=0,
        operation="stream",
        rows_processed=10,
        failed_sources=[],
        processing_time_sec=processing_time_sec,
        source_url=source_url,
        bytes_processed=None,
        bytes_downloaded=None,
        http_fetch_time_sec=http_fetch_time_sec,
        http_retry_count=0,
        validation_error_count=0,
        schema_cache_hit=True,
        conv_dict_time_sec=None,
        timestamp=_NOW,
    )


def _tide(
    duration_sec: float,
    wake_reason: WakeReason = WakeReason.TIMER,
    outcomes: List[CurrentOutcome] | None = None,
) -> Tide:
    """Build a minimal Tide with the given duration_sec and wake_reason."""
    return Tide.model_construct(
        tide_number=1,
        fired=[],
        skipped=[],
        current_outcomes=outcomes or [],
        duration_sec=duration_sec,
        wake_reason=wake_reason,
        heap_depth=0,
        in_flight_count_at_start=0,
        canal_rejects_added=0,
        next_due_in_sec=None,
        timestamp=_NOW,
    )


def _reject_http(
    host: str = "api.example.com",
    status_code: int = 429,
    cooldown_sec: float | None = None,
    attempt_number: int | None = None,
    duration_sec: float | None = None,
) -> RejectEntry:
    """Build a minimal HTTPStatusError RejectEntry."""
    return RejectEntry.model_construct(
        source=f"https://{host}/endpoint",
        error_kind="HTTPStatusError",
        message=f"HTTP {status_code}",
        retry_after=cooldown_sec,
        wave_index=None,
        from_name=None,
        to_name=None,
        host=host,
        status_code=status_code,
        attempt_number=attempt_number,
        duration_sec=duration_sec,
        cooldown_sec=cooldown_sec,
    )


def _reject_canal(
    error_kind: str,
    from_name: str = "upstream",
    to_name: str = "downstream",
    cooldown_sec: float | None = None,
) -> RejectEntry:
    """Build a minimal canal-layer RejectEntry."""
    return RejectEntry.model_construct(
        source="DownstreamClass",
        error_kind=error_kind,
        message=f"edge {from_name}->{to_name}: {error_kind}",
        retry_after=None,
        wave_index=None,
        from_name=from_name,
        to_name=to_name,
        host=None,
        status_code=None,
        attempt_number=None,
        duration_sec=None,
        cooldown_sec=cooldown_sec,
    )


# ---------------------------------------------------------------------------
# tune() entry point
# ---------------------------------------------------------------------------


def test_tune_empty_inputs_returns_empty_report() -> None:
    """tune() with no inputs returns a TuningReport with zero hints and correct summary."""
    report = tune()
    assert isinstance(report, TuningReport)
    assert report.hints == []
    assert report.summary["total_chunks"] == 0
    assert report.summary["total_passes"] == 0
    assert report.summary["total_rejects"] == 0
    assert report.summary["window_start"] is None
    assert report.summary["window_end"] is None


# ---------------------------------------------------------------------------
# _tune_chunk_size
# ---------------------------------------------------------------------------


def test_tune_chunk_size_high_recommends_raise() -> None:
    """30 waves with p50=5ms and p99<50ms triggers a HIGH hint to raise chunk_size."""
    waves = [_wave(0.005) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert h.knob == "chunk_size"
    assert "raise" in str(h.recommended_value).lower()
    assert h.sample_size == 30


def test_tune_chunk_size_med_recommends_lower() -> None:
    """30 waves where p99 > 500ms triggers a MED hint to lower chunk_size."""
    waves = [_wave(0.010) for _ in range(29)]
    # One very slow wave to push p99 above 500ms.
    waves.append(_wave(1.500))
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "med"
    assert h.knob == "chunk_size"
    assert "lower" in str(h.recommended_value).lower()
    assert h.sample_size == 30


def test_tune_chunk_size_insufficient_data() -> None:
    """Fewer than 20 waves per source emits an INFO hint about insufficient data."""
    waves = [_wave(0.005) for _ in range(5)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "info"
    assert "insufficient" in h.signal.lower()
    assert h.sample_size == 5


def test_tune_chunk_size_well_tuned_emits_info() -> None:
    """30 waves with p50 in the 10–50ms range and p99 < 500ms emits an INFO 'well-tuned' hint."""
    # p50 ~= 20ms, p99 well below 500ms
    waves = [_wave(0.020) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "info"
    assert "well-tuned" in h.signal


# ---------------------------------------------------------------------------
# _tune_chunk_size — split-time path (Commit E')
# ---------------------------------------------------------------------------


def test_tune_chunk_size_split_path_high_when_parse_too_fast() -> None:
    """Split-time path fires HIGH when p50_parse < 1 ms and p99_parse < 5 ms.

    30 waves with processing_time_sec=0.100 and http_fetch_time_sec=0.0995
    give parse remainder ~0.5 ms p50 and p99 — below the 1 ms / 5 ms thresholds.
    """
    # total=100ms, http=99.5ms → parse=0.5ms
    waves = [_wave_with_http(0.100, 0.0995) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert "raise" in str(h.recommended_value).lower()
    assert "parse" in h.signal.lower() or "p50_parse" in h.signal


def test_tune_chunk_size_split_path_med_when_parse_heavy() -> None:
    """Split-time path fires MED when p99_parse > 100 ms.

    30 waves with processing_time_sec=0.300 and http_fetch_time_sec=0.050
    give parse remainder p99 = 250 ms — above the 100 ms threshold.
    """
    # total=300ms, http=50ms → parse=250ms
    waves = [_wave_with_http(0.300, 0.050) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "med"
    assert "lower" in str(h.recommended_value).lower()


def test_tune_chunk_size_split_path_info_when_well_tuned() -> None:
    """Split-time path emits INFO when parse times are in the calibrated range.

    30 waves with total=110ms, http=100ms → parse=10ms p50; within range.
    """
    # parse=10ms, well inside [1ms, 100ms]
    waves = [_wave_with_http(0.110, 0.100) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "info"
    assert "well-tuned" in h.signal.lower()


def test_tune_chunk_size_fallback_when_http_fetch_time_sec_none() -> None:
    """End-to-end fallback used when http_fetch_time_sec is None for any wave in the group.

    Mixed group (some with, some without http_fetch_time_sec) falls back to
    the end-to-end path: p50=5ms, p99<50ms → HIGH under old thresholds.
    """
    # Half the waves have HTTP telemetry, half don't → fallback to end-to-end.
    waves = [_wave_with_http(0.005, 0.002) for _ in range(15)]
    waves += [_wave(0.005) for _ in range(15)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    # End-to-end path: p50=5ms, p99<50ms → HIGH
    assert h.severity == "high"
    assert "raise" in str(h.recommended_value).lower()
    # Signal uses end-to-end labels (no "parse" prefix).
    assert "p50=" in h.signal


def test_tune_chunk_size_all_none_http_uses_end_to_end_path() -> None:
    """All waves with http_fetch_time_sec=None use the original end-to-end thresholds.

    Regression guard: the None-fallback path must produce the same hint as
    the original _tune_chunk_size implementation.
    """
    waves = [_wave(0.005) for _ in range(30)]
    hints = _tune_chunk_size(waves)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    # Original end-to-end signal string preserved.
    assert "chunks finishing too fast" in h.signal


# ---------------------------------------------------------------------------
# _tune_penstock_rate
# ---------------------------------------------------------------------------


def test_tune_penstock_rate_429s_with_cooldown() -> None:
    """10 HTTPStatusError(429) rejects with cooldown_sec=10.0 triggers HIGH with rate≈0.1."""
    rejects = [_reject_http(cooldown_sec=10.0) for _ in range(10)]
    hints = _tune_penstock_rate(rejects)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert h.knob == "penstock.rate_per_sec"
    assert h.scope.get("host") == "api.example.com"
    # 1 / 10.0 = 0.1
    assert h.recommended_value == pytest.approx(0.1, abs=1e-4)
    assert h.sample_size == 10


def test_tune_penstock_rate_canal_limited_no_cooldown() -> None:
    """10 PenstockLimited canal rejects with no cooldown_sec triggers MED hint."""
    rejects = [_reject_canal("PenstockLimited", cooldown_sec=None) for _ in range(10)]
    hints = _tune_penstock_rate(rejects)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "med"
    assert h.knob == "penstock.rate_per_sec"
    assert "unavailable" in h.rationale.lower()
    assert h.recommended_value is None


def test_tune_penstock_rate_below_threshold_skipped() -> None:
    """5 or fewer rejects per group are skipped (below the >5 threshold)."""
    rejects = [_reject_http(cooldown_sec=5.0) for _ in range(5)]
    hints = _tune_penstock_rate(rejects)
    # 5 is not > 5, so no hint emitted.
    assert hints == []


# ---------------------------------------------------------------------------
# _tune_surge_threshold
# ---------------------------------------------------------------------------


def test_tune_surge_threshold_with_in_flight_data() -> None:
    """10 SkipAhead rejects with tides showing high in_flight_sec emits a MED hint."""
    rejects = [_reject_canal("SkipAhead", from_name="kraken", to_name="arb_fjord") for _ in range(10)]
    # Build tides with in_flight_sec data for "kraken".
    outcomes = [CurrentOutcome(name="kraken", status="still_running", in_flight_sec=3.2)]
    tides = [_tide(0.01, outcomes=outcomes) for _ in range(10)]
    hints = _tune_surge_threshold(rejects, tides)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "med"
    assert h.knob == "surge_barrier.threshold_multiple"
    assert h.scope == {"edge": "kraken->arb_fjord"}
    assert "3.20" in h.signal


def test_tune_surge_threshold_no_in_flight_data() -> None:
    """10 SkipAhead rejects with tides that have no matching in_flight_sec emits INFO."""
    rejects = [_reject_canal("SkipAhead", from_name="kraken", to_name="arb_fjord") for _ in range(10)]
    # Tides with outcomes for a DIFFERENT name — no in_flight_sec for "kraken".
    outcomes = [CurrentOutcome(name="other_source", status="still_running", in_flight_sec=3.2)]
    tides = [_tide(0.01, outcomes=outcomes) for _ in range(10)]
    hints = _tune_surge_threshold(rejects, tides)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "info"
    assert "insufficient" in h.rationale.lower()


def test_tune_surge_threshold_below_threshold_skipped() -> None:
    """5 or fewer surge rejects per edge are skipped."""
    rejects = [_reject_canal("SkipAhead") for _ in range(5)]
    hints = _tune_surge_threshold(rejects, [])
    assert hints == []


# ---------------------------------------------------------------------------
# _tune_pass_interval
# ---------------------------------------------------------------------------


def test_tune_pass_interval_saturated() -> None:
    """30 tides with p99 near 0.04s vs current_pass_interval=0.05 triggers HIGH."""
    # p99 of 0.04 > 0.8 * 0.05 = 0.04 — boundary case; use 0.041 to be safely over.
    tides = [_tide(0.041) for _ in range(30)]
    hints = _tune_pass_interval(tides, current_pass_interval=0.05)
    high_hints = [h for h in hints if h.severity == "high"]
    assert len(high_hints) >= 1
    h = high_hints[0]
    assert h.knob == "pass_interval"
    assert h.current_value == 0.05
    assert h.recommended_value is not None
    assert h.recommended_value > 0.05


def test_tune_pass_interval_fallback_heap_empty() -> None:
    """30 tides where 12 have wake_reason='pass_interval' (40%) triggers MED fallback hint."""
    tides = [_tide(0.005, wake_reason=WakeReason.TIMER) for _ in range(18)]
    tides += [_tide(0.005, wake_reason=WakeReason.PASS_INTERVAL) for _ in range(12)]
    hints = _tune_pass_interval(tides, current_pass_interval=1.0)
    med_hints = [h for h in hints if h.severity == "med"]
    assert len(med_hints) >= 1
    h = med_hints[0]
    assert "fallback" in h.rationale.lower() or "pass_interval" in h.rationale


def test_tune_pass_interval_well_sized() -> None:
    """30 tides with small duration and low fallback fraction emits INFO 'well-sized'."""
    tides = [_tide(0.005, wake_reason=WakeReason.TIMER) for _ in range(30)]
    hints = _tune_pass_interval(tides, current_pass_interval=1.0)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "info"
    assert "well-sized" in h.signal


def test_tune_pass_interval_insufficient_data() -> None:
    """Fewer than 20 tides emits INFO about insufficient data."""
    tides = [_tide(0.010) for _ in range(10)]
    hints = _tune_pass_interval(tides, current_pass_interval=0.05)
    assert len(hints) == 1
    assert hints[0].severity == "info"
    assert "insufficient" in hints[0].signal.lower()


# ---------------------------------------------------------------------------
# _tune_retry_policy
# ---------------------------------------------------------------------------


def test_tune_retry_policy_skipped_when_attempt_number_none() -> None:
    """HTTPStatusError rejects with attempt_number=None do not trigger a retry-ceiling hint."""
    rejects = [_reject_http(attempt_number=None) for _ in range(10)]
    hints = _tune_retry_policy(rejects)
    # No attempt_number data → no ceiling hint.  No duration/cooldown either → no wait hint.
    ceiling_hints = [h for h in hints if h.knob == "http.stop_after_attempt"]
    assert ceiling_hints == []


def test_tune_retry_policy_hits_ceiling() -> None:
    """Majority of attempt_number values equal max → MED hint to raise stop_after_attempt."""
    # 8 out of 10 rejects hit attempt_number=5 (the ceiling).
    rejects = [_reject_http(attempt_number=5) for _ in range(8)]
    rejects += [_reject_http(attempt_number=3) for _ in range(2)]
    hints = _tune_retry_policy(rejects)
    ceiling_hints = [h for h in hints if h.knob == "http.stop_after_attempt"]
    assert len(ceiling_hints) == 1
    h = ceiling_hints[0]
    assert h.severity == "med"
    assert h.current_value == 5
    assert "raise" in h.rationale.lower()


def test_tune_retry_policy_empty_when_no_http_errors() -> None:
    """PenstockLimited rejects with no attempt_number/duration data → empty list.

    The function now accepts canal kinds too (via _RETRY_POLICY_KINDS), but
    5 rejects with all-None attempt_number and timing data produce no hints.
    """
    rejects = [_reject_canal("PenstockLimited") for _ in range(5)]
    hints = _tune_retry_policy(rejects)
    assert hints == []


def test_tune_retry_policy_canal_only_corpus_knob_starts_with_canal() -> None:
    """_tune_retry_policy on canal-only corpus emits knobs starting with 'canal.'."""
    # 10 PenstockLimited rejects with attempt_number=3 (ceiling = 3 for all → majority at ceiling).
    rejects = [
        RejectEntry.model_construct(
            source="DownstreamCls",
            error_kind="PenstockLimited",
            message="edge up->down: penstock_limited",
            retry_after=None,
            wave_index=None,
            from_name="up",
            to_name="down",
            host=None,
            status_code=None,
            attempt_number=3,
            duration_sec=0.1,
            cooldown_sec=None,
        )
        for _ in range(10)
    ]
    hints = _tune_retry_policy(rejects)
    ceiling_hints = [h for h in hints if "stop_after_attempt" in h.knob]
    assert len(ceiling_hints) == 1
    h = ceiling_hints[0]
    assert h.knob.startswith("canal."), f"Expected canal. prefix; got {h.knob!r}"
    assert h.scope.get("edge") == "up->down"


def test_tune_retry_policy_mixed_corpus_no_cross_contamination() -> None:
    """Mixed HTTP+canal corpus produces both groups without cross-contamination.

    HTTP rejects should produce http.* knobs with host scope;
    canal rejects should produce canal.* knobs with edge scope.
    No HTTP hint should carry an edge scope, and no canal hint should carry a host scope.
    """
    http_rejects = [_reject_http(host="api.example.com", attempt_number=5) for _ in range(8)]
    http_rejects += [_reject_http(host="api.example.com", attempt_number=3) for _ in range(2)]
    canal_rejects = [
        RejectEntry.model_construct(
            source="Cls",
            error_kind="SurgeHalted",
            message="edge a->b: surge halted",
            retry_after=None,
            wave_index=None,
            from_name="a",
            to_name="b",
            host=None,
            status_code=None,
            attempt_number=5,
            duration_sec=0.2,
            cooldown_sec=None,
        )
        for _ in range(8)
    ]
    canal_rejects += [
        RejectEntry.model_construct(
            source="Cls",
            error_kind="SurgeHalted",
            message="edge a->b: surge halted",
            retry_after=None,
            wave_index=None,
            from_name="a",
            to_name="b",
            host=None,
            status_code=None,
            attempt_number=3,
            duration_sec=0.2,
            cooldown_sec=None,
        )
        for _ in range(2)
    ]
    hints = _tune_retry_policy(http_rejects + canal_rejects)

    http_hints = [h for h in hints if h.knob.startswith("http.")]
    canal_hints = [h for h in hints if h.knob.startswith("canal.")]

    assert len(http_hints) >= 1, f"Expected at least one http.* hint; got {hints}"
    assert len(canal_hints) >= 1, f"Expected at least one canal.* hint; got {hints}"

    for h in http_hints:
        assert "host" in h.scope, f"HTTP hint must have 'host' scope; got {h.scope}"
        assert "edge" not in h.scope, f"HTTP hint must not have 'edge' scope; got {h.scope}"

    for h in canal_hints:
        assert "edge" in h.scope, f"Canal hint must have 'edge' scope; got {h.scope}"
        assert "host" not in h.scope, f"Canal hint must not have 'host' scope; got {h.scope}"


def test_tune_retry_policy_http_error_shape_unchanged() -> None:
    """Existing HTTPStatusError-only corpus produces the same hint shape as before broadening.

    Regression guard: adding canal kinds must not change the knob/scope shape for HTTP errors.
    """
    rejects = [_reject_http(attempt_number=5) for _ in range(8)]
    rejects += [_reject_http(attempt_number=3) for _ in range(2)]
    hints = _tune_retry_policy(rejects)
    ceiling_hints = [h for h in hints if "stop_after_attempt" in h.knob]
    assert len(ceiling_hints) == 1
    h = ceiling_hints[0]
    assert h.knob == "http.stop_after_attempt"
    assert h.scope.get("host") == "api.example.com"
    assert h.severity == "med"


# ---------------------------------------------------------------------------
# Tideweaver.summary()
# ---------------------------------------------------------------------------


def test_tideweaver_summary_uses_self_rejects(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory
) -> None:
    """Tideweaver.summary() passes self.rejects to tune() so total_rejects matches."""
    monkeypatch.chdir(tmp_path)

    start = datetime.now(_UTC)
    end = start + timedelta(seconds=30)

    class _Src(Incorporator):
        pass

    from incorporator.observability.tideweaver import Stream

    stream = Stream(name="src", cls=_Src, interval=10)
    watershed = Watershed(window=(start, end), currents=[stream], edges=[])
    tw = Tideweaver(watershed)

    # Inject synthetic canal rejects directly into the accumulator.
    tw._canal_rejects = [_reject_canal("PenstockLimited") for _ in range(7)]

    report = tw.summary(tides=None, waves=None)
    assert report.summary["total_rejects"] == 7


# ---------------------------------------------------------------------------
# TuningReport.render()
# ---------------------------------------------------------------------------


def test_tuning_report_render_format() -> None:
    """TuningReport.render() produces a string containing severity labels and knob names."""
    hints = [
        TuningHint.model_construct(
            severity="high",
            knob="chunk_size",
            scope={"source": "https://api.example.com"},
            current_value=None,
            recommended_value="raise from current (target ~50 ms p50)",
            signal="p50=5.0ms — chunks finishing too fast",
            rationale="p50 and p99 are both well below the 50ms target.",
            sample_size=30,
        ),
        TuningHint.model_construct(
            severity="info",
            knob="pass_interval",
            scope={"global": "true"},
            current_value=0.05,
            recommended_value=None,
            signal="well-sized",
            rationale="No change needed.",
            sample_size=20,
        ),
    ]
    report = TuningReport.model_construct(
        hints=hints,
        summary={"total_chunks": 30, "total_passes": 20, "total_rejects": 0},
        analyzed_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=_UTC),
    )
    rendered = report.render()

    # HIGH hint should appear before INFO hint in sorted output.
    high_pos = rendered.find("[HIGH]")
    info_pos = rendered.find("[INFO]")
    assert high_pos != -1, "Expected [HIGH] label in render output"
    assert info_pos != -1, "Expected [INFO] label in render output"
    assert high_pos < info_pos, "HIGH hint should sort before INFO hint"

    # Both knob names should appear.
    assert "chunk_size" in rendered
    assert "pass_interval" in rendered

    # Summary footer should be present.
    assert "--- Summary ---" in rendered
    assert "total_chunks" in rendered


# ---------------------------------------------------------------------------
# _tune_compound_budget
# ---------------------------------------------------------------------------


def test_tune_compound_budget_fires_when_budget_exceeds_interval() -> None:
    """_tune_compound_budget emits a HIGH hint when compound budget (1200s) >= pass_interval."""
    hints = _tune_compound_budget(60.0)
    assert len(hints) == 1
    h = hints[0]
    assert h.severity == "high"
    assert h.knob == "compound_retry_budget"
    assert h.scope == {"global": "tideweaver"}
    assert h.current_value == pytest.approx(1200.0)
    assert "1200" in h.signal and "60" in h.signal


def test_tune_compound_budget_silent_when_budget_within_interval() -> None:
    """_tune_compound_budget returns empty list when compound budget (1200s) < pass_interval."""
    hints = _tune_compound_budget(2000.0)
    assert hints == []


def test_tune_compound_budget_fires_on_exact_equality() -> None:
    """_tune_compound_budget fires (>=) when pass_interval exactly equals the budget (1200s)."""
    hints = _tune_compound_budget(1200.0)
    assert len(hints) == 1
    assert hints[0].severity == "high"


def test_tune_compound_budget_skipped_via_tune_no_pass_interval() -> None:
    """tune() with no pass_interval does not produce a compound_retry_budget hint."""
    rejects = [_reject_http(attempt_number=5) for _ in range(8)]
    report = tune(rejects=rejects)
    compound_hints = [h for h in report.hints if h.knob == "compound_retry_budget"]
    assert compound_hints == []


def test_tune_compound_budget_skipped_via_tune_pass_interval_none() -> None:
    """tune(pass_interval=None) returns a report with no hints at all."""
    report = tune(pass_interval=None)
    assert report.hints == []
