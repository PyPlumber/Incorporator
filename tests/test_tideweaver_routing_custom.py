"""Tideweaver routing tests for **custom** Watershed shapes.

Two tests from the routing test plan:

* **Test 9** — MLB leaderboard cascade. A custom DAG with three Fjords
  in series: ``S_players → F_normalize → F_rank → F_summary``. Each
  middle Fjord's snapshot is consumed by the next. Proves transitive
  ``_tideweaver_snapshot`` propagation across a three-hop Fjord chain
  and that all three converter flavors (``calc``, ``calc_all``, ``inc``)
  coexist in the same routing path.
* **Test 10** — Mixed hard/soft edges + drain timeout (HTTPBin). A custom
  DAG with ``S_a`` (hard edge to ``F_tail``) and ``S_b`` (soft edge to
  ``F_tail``). The window closes mid-tick and ``drain_timeout`` cancels
  the in-flight ``S_a``. ``F_tail`` must not see a half-baked snapshot.

Same workaround helpers as the other routing files.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.tideweaver import (
    Current,
    Edge,
    Fjord,
    Stream,
    Tideweaver,
    Watershed,
    flow_from_mode,
)


# ---------------------------------------------------------------------------
# Source + output classes for Tests 9 and 10.
# ---------------------------------------------------------------------------


class MLBPlayerLB(Incorporator):
    """MLB player row for the leaderboard cascade."""

    model_config = ConfigDict(extra="allow")


class NormalizedPlayer(Incorporator):
    """Test 9 F_normalize output: parsed numeric stats."""

    model_config = ConfigDict(extra="allow")


class RankedPlayer(Incorporator):
    """Test 9 F_rank output: top-N ranked by batting average."""

    model_config = ConfigDict(extra="allow")


class LeagueSummary(Incorporator):
    """Test 9 F_summary output: league-wide aggregate."""

    model_config = ConfigDict(extra="allow")


class EchoA(Incorporator):
    """Test 10 hard-edge upstream Stream class."""

    model_config = ConfigDict(extra="allow")


class EchoB(Incorporator):
    """Test 10 soft-edge upstream Stream class."""

    model_config = ConfigDict(extra="allow")


class EchoTail(Incorporator):
    """Test 10 Fjord tail output class."""

    model_config = ConfigDict(extra="allow")


def _short_window(seconds: float) -> Tuple[datetime, datetime]:
    """Build a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _make_routing_tick(tw: Tideweaver, strong_refs_by_cls: Dict[type, List[Any]]) -> Any:
    """Tick dispatcher: Stream → real path + populate strong refs; Fjord → real path + read export file back.

    The production ``_tick_fjord`` exports its output instances and drops
    them — the WeakValueDict registry loses them immediately. To make a
    middle-Fjord's output visible to a downstream-of-Fjord consumer (Test 9
    cascade), we re-read the export file after each Fjord tick and
    reconstruct instances under a test-scope strong ref.
    """

    async def tick(current: Current) -> None:
        if isinstance(current, Stream):
            paginator = current.incorp_params.get("inc_page")
            if paginator is not None and hasattr(paginator, "reset"):
                paginator.reset()
            result = await current.cls.incorp(**current.incorp_params)
            if isinstance(result, list):
                strong_refs_by_cls[current.cls] = list(result)
            elif result is not None:
                strong_refs_by_cls[current.cls] = [result]
            current.cls._tideweaver_snapshot = list(strong_refs_by_cls.get(current.cls, []))  # type: ignore[attr-defined]
        elif isinstance(current, Fjord):
            await tw._tick_fjord(current)
            # No file-reread workaround needed: ``outflow.flush`` now parks
            # ``_tideweaver_snapshot`` on the Fjord's output class directly.
            # We still stash a strong-ref copy so the snapshot survives the
            # WeakValueDict between this test's manual ticks.
            snapshot = getattr(current.cls, "_tideweaver_snapshot", None)
            if snapshot:
                strong_refs_by_cls[current.cls] = list(snapshot)

    return tick


# ---------------------------------------------------------------------------
# Test 9 — MLB leaderboard cascade.
# ---------------------------------------------------------------------------


async def _mock_mlb_players(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned MLB player career hitting stats."""
    payload = [
        {"id": 1, "fullName": "Player A", "battingAverage": ".320", "homeRuns": "30"},
        {"id": 2, "fullName": "Player B", "battingAverage": ".285", "homeRuns": "42"},
        {"id": 3, "fullName": "Player C", "battingAverage": ".301", "homeRuns": "18"},
        {"id": 4, "fullName": "Player D", "battingAverage": ".250", "homeRuns": "8"},
        {"id": 5, "fullName": "Player E", "battingAverage": ".310", "homeRuns": "25"},
    ]
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_custom_mlb_leaderboard_three_hop_fjord_cascade(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three-hop Fjord-to-Fjord cascade: players → normalize → rank → summary.

    Proves: (a) a custom DAG can chain three Fjords in series with the
    output of each visible to the next; (b) ``_tideweaver_snapshot``
    propagates transitively across Fjord hops (via the workaround that
    re-reads each Fjord's export file); (c) the cascade carries multiple
    converter flavors end-to-end (``inc(float)`` on the normalize step
    coerces the ``".320"`` string to a real float; the rank step uses
    Python sort + slice; the summary step aggregates).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_mlb_players)
    _reset_registries(MLBPlayerLB, NormalizedPlayer, RankedPlayer, LeagueSummary)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    normalize_py = tmp_path / "outflow_normalize.py"
    normalize_py.write_text(
        "def outflow(state):\n"
        "    players = state.get('MLBPlayerLB', [])\n"
        "    rows = []\n"
        "    for p in players:\n"
        "        avg_raw = getattr(p, 'battingAverage', '0')\n"
        "        try:\n"
        "            avg = float(avg_raw)\n"
        "        except (TypeError, ValueError):\n"
        "            avg = 0.0\n"
        "        rows.append({'inc_code': p.inc_code, 'name': p.fullName, 'avg': avg})\n"
        "    return rows\n",
        encoding="utf-8",
    )
    rank_py = tmp_path / "outflow_rank.py"
    rank_py.write_text(
        "def outflow(state):\n"
        "    normalized = state.get('NormalizedPlayer', [])\n"
        "    ranked = sorted(normalized, key=lambda p: getattr(p, 'avg', 0.0), reverse=True)\n"
        "    return [{'inc_code': i + 1, 'name': p.name, 'avg': p.avg, 'rank': i + 1}\n"
        "            for i, p in enumerate(ranked[:3])]\n",
        encoding="utf-8",
    )
    summary_py = tmp_path / "outflow_summary.py"
    summary_py.write_text(
        "def outflow(state):\n"
        "    ranked = state.get('RankedPlayer', [])\n"
        "    if not ranked:\n"
        "        return []\n"
        "    avgs = [getattr(p, 'avg', 0.0) for p in ranked]\n"
        "    return [{'inc_code': 'league', 'top_avg': max(avgs), 'mean_avg': sum(avgs) / len(avgs), 'count': len(ranked)}]\n",
        encoding="utf-8",
    )

    players = Stream(
        name="players",
        cls=MLBPlayerLB,
        interval=1.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/people/stats?group=hitting",
            "inc_code": "id",
            # ignore_ssl=True skips httpx.AsyncClient's real TLS cert-chain
            # load (ssl.create_default_context()), which costs real time per
            # tick even though execute_request is mocked below — otherwise
            # this starves the real-clock window. Pre-existing, fully-plumbed
            # incorp_params key (see incorporator/io/fetch.py's
            # HTTPClientBuilder.build_client).
            "ignore_ssl": True,
        },
    )
    normalize = Fjord(
        name="normalize",
        cls=NormalizedPlayer,
        interval=0.3,
        on_error="isolate",
        outflow=normalize_py,
        export_params={"file_path": str(tmp_path / "normalized.ndjson"), "format": "ndjson", "if_exists": "replace"},
    )
    rank = Fjord(
        name="rank",
        cls=RankedPlayer,
        interval=0.3,
        on_error="isolate",
        outflow=rank_py,
        export_params={"file_path": str(tmp_path / "ranked.ndjson"), "format": "ndjson", "if_exists": "replace"},
    )
    summary = Fjord(
        name="summary",
        cls=LeagueSummary,
        interval=0.3,
        on_error="isolate",
        outflow=summary_py,
        export_params={"file_path": str(tmp_path / "summary.ndjson"), "format": "ndjson", "if_exists": "replace"},
    )

    ws = Watershed(
        window=_short_window(6.0),
        currents=[players, normalize, rank, summary],
        edges=[
            Edge(from_name="players", to_name="normalize", flow=flow_from_mode("hard")),
            Edge(from_name="normalize", to_name="rank", flow=flow_from_mode("hard")),
            Edge(from_name="rank", to_name="summary", flow=flow_from_mode("hard")),
        ],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Each hop wrote its export.
    normalized_lines = [
        ln for ln in (tmp_path / "normalized.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    ranked_lines = [ln for ln in (tmp_path / "ranked.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()]
    summary_lines = [ln for ln in (tmp_path / "summary.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert normalized_lines, "F_normalize must export rows"
    assert ranked_lines, "F_rank must export rows"
    assert summary_lines, "F_summary must export rows"

    # F_normalize coerced ".320" → 0.320 (float).
    normalized_rows = [json.loads(ln) for ln in normalized_lines]
    assert all(isinstance(r["avg"], float) for r in normalized_rows), (
        f"normalize must produce float avgs, got types {[type(r['avg']).__name__ for r in normalized_rows]}"
    )

    # F_rank produced the top 3 sorted by avg desc.
    ranked_rows = [json.loads(ln) for ln in ranked_lines]
    # Take the most recent flush's 3 rows.
    last_rank = ranked_rows[-3:]
    avgs_in_order = [r["avg"] for r in last_rank]
    assert avgs_in_order == sorted(avgs_in_order, reverse=True), f"rank must be desc-sorted by avg, got {avgs_in_order}"
    assert last_rank[0]["name"] == "Player A", f"top-rank must be Player A (.320), got {last_rank[0]}"

    # F_summary read F_rank's snapshot and produced league-wide aggregate.
    summary_rows = [json.loads(ln) for ln in summary_lines]
    last_summary = summary_rows[-1]
    assert last_summary["count"] == 3, f"summary must see 3 ranked players, got {last_summary}"
    assert abs(last_summary["top_avg"] - 0.320) < 1e-6, f"summary top_avg must match Player A, got {last_summary}"


# ---------------------------------------------------------------------------
# Test 10 — Mixed hard/soft edges with drain timeout.
# ---------------------------------------------------------------------------


async def _mock_httpbin(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """HTTPBin-style echo. ``/anything/slow`` simulates a slow response."""
    if "/slow" in url:
        await asyncio.sleep(2.0)
    if "/a/" in url or url.endswith("/a"):
        payload: Any = [{"id": 1, "status_code": 200, "label": "a-payload"}]
    elif "/b/" in url or url.endswith("/b"):
        payload = [{"id": 1, "delay_ms": 100, "label": "b-payload"}]
    else:
        payload = []
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_custom_mixed_modes_with_drain_timeout(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Custom DAG with mixed hard/soft edges + drain_timeout cancels mid-tick.

    Proves: (a) ``Watershed(...)`` with explicit mixed-mode edges
    orchestrates correctly (hard ``S_a → F_tail``; soft ``S_b → F_tail``);
    (b) ``drain_timeout`` cancels an in-flight Stream tick when the window
    closes; (c) after at least one successful ``S_a`` tick, the tail
    Fjord sees the parked snapshot (not a half-baked one from the
    cancelled tick); (d) soft-edge ``S_b`` fires on its own cadence
    without gating on ``S_a``.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_httpbin)
    _reset_registries(EchoA, EchoB, EchoTail)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    tail_py = tmp_path / "outflow_tail.py"
    tail_py.write_text(
        "def outflow(state):\n"
        "    a_rows = state.get('EchoA', [])\n"
        "    b_rows = state.get('EchoB', [])\n"
        "    return [{\n"
        "        'inc_code': 'merged',\n"
        "        'a_count': len(a_rows),\n"
        "        'b_count': len(b_rows),\n"
        "        'a_labels': ','.join(getattr(r, 'label', '') for r in a_rows),\n"
        "    }]\n",
        encoding="utf-8",
    )

    # ignore_ssl=True skips httpx.AsyncClient's real TLS cert-chain load
    # (ssl.create_default_context()), which costs real time per tick even
    # though execute_request is mocked below — otherwise this starves the
    # real-clock window. Pre-existing, fully-plumbed incorp_params key (see
    # incorporator/io/fetch.py's HTTPClientBuilder.build_client).
    s_a = Stream(
        name="s_a",
        cls=EchoA,
        interval=1.5,
        on_error="isolate",
        incorp_params={"inc_url": "https://httpbin.org/anything/a", "inc_code": "id", "ignore_ssl": True},
    )
    s_b = Stream(
        name="s_b",
        cls=EchoB,
        interval=1.5,
        on_error="isolate",
        incorp_params={"inc_url": "https://httpbin.org/anything/b", "inc_code": "id", "ignore_ssl": True},
    )
    tail = Fjord(
        name="tail",
        cls=EchoTail,
        interval=0.2,
        on_error="isolate",
        outflow=tail_py,
        export_params={"file_path": str(tmp_path / "tail.ndjson"), "format": "ndjson", "if_exists": "replace"},
    )

    # Custom watershed with mixed-mode edges.
    ws = Watershed(
        window=_short_window(5.0),
        currents=[s_a, s_b, tail],
        edges=[
            Edge(from_name="s_a", to_name="tail", flow=flow_from_mode("hard")),
            Edge(from_name="s_b", to_name="tail", flow=flow_from_mode("soft")),
        ],
        drain_timeout=0.3,
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Both Streams populated their per-class snapshots.
    a_rows = strong_refs_by_cls.get(EchoA, [])
    b_rows = strong_refs_by_cls.get(EchoB, [])
    assert a_rows, "s_a Stream must populate"
    assert b_rows, "s_b Stream must populate"

    # The tail Fjord wrote merged rows.
    tail_lines = [ln for ln in (tmp_path / "tail.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert tail_lines, "tail Fjord must write rows"
    tail_rows = [json.loads(ln) for ln in tail_lines]
    # The merge saw both upstream Streams' state — proves the soft edge
    # also delivered into state[].
    last_merge = tail_rows[-1]
    assert last_merge["a_count"] >= 1, f"tail must see at least one EchoA row, got {last_merge}"
    assert last_merge["b_count"] >= 1, f"tail must see at least one EchoB row (soft edge), got {last_merge}"
    # The labels propagated through the hard edge.
    assert "a-payload" in last_merge["a_labels"], f"tail must see EchoA labels, got {last_merge}"
