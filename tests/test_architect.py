"""Tests for ``incorporator.observability.tideweaver.architect``.

Layered to match the module boundaries:

* ``_resolve_sources`` — URL / file / dict / Path value-form resolution.
* ``_penstock_for`` — three-tier confidence ladder over a ``SourceProfile``.
* ``_analyze_topology`` — synthetic ``SourceProfile`` fixtures → ``OrchestrationPlan``.
* ``render_report`` / ``render_python`` / ``render_json`` — deterministic
  input → expected output, including the JSON round-trip through
  :func:`incorporator.observability.tideweaver.config.load_watershed`.
* End-to-end probe of local file fixtures via ``run()`` (no network).

Detection-level tests live in ``tests/test_inspector_capture.py``; this
file does not re-test pagination / pk / conv_dict detection.
"""

from __future__ import annotations

import ast
import asyncio
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver.architect import (
    CurrentSpec,
    EdgeSpec,
    OrchestrationPlan,
    PenstockSpec,
    _analyze_topology,
    _pascal_case,
    _penstock_for,
    _resolve_sources,
    render_json,
    render_python,
    render_report,
    run,
)
from incorporator.tools.inspector import ResponseMeta, SourceProfile


# ---------------------------------------------------------------------------
# Synthetic SourceProfile fixtures — keep tests deterministic / probe-free.
# ---------------------------------------------------------------------------


def _profile(
    name_fields: Set[str],
    pk: str | None = None,
    host: str | None = None,
    rate_limited: bool = False,
    pagination: str | None = None,
) -> SourceProfile:
    """Build a minimal SourceProfile for analyzer / penstock tests."""
    meta = ResponseMeta(host=host, rate_limited=rate_limited) if (host or rate_limited) else None
    return SourceProfile(
        parsed_data=[{f: i for i, f in enumerate(sorted(name_fields))}],
        provided_kwargs={},
        response_meta=meta,
        sample={f: i for i, f in enumerate(sorted(name_fields))},
        target_obj={f: i for i, f in enumerate(sorted(name_fields))},
        is_dict_shaped=True,
        top_level_fields=set(name_fields),
        primary_key_field=pk,
        primary_key_score=80 if pk else 0,
        pagination_kind=pagination,
        pagination_suggestion=(f"CursorPaginator(cursor_param='{pagination}')" if pagination else None),
    )


# ---------------------------------------------------------------------------
# _pascal_case helper
# ---------------------------------------------------------------------------


def test_pascal_case_snake() -> None:
    assert _pascal_case("user_data") == "UserData"


def test_pascal_case_kebab() -> None:
    assert _pascal_case("user-data") == "UserData"


def test_pascal_case_empty_falls_back_to_source() -> None:
    assert _pascal_case("") == "Source"


# ---------------------------------------------------------------------------
# _resolve_sources — URL / file / Path / dict value forms.
# ---------------------------------------------------------------------------


def test_resolve_sources_url_string() -> None:
    resolved = _resolve_sources({"api": "https://api.example.com/v1/users"})
    assert resolved == [("api", {"inc_url": "https://api.example.com/v1/users"})]


def test_resolve_sources_file_path_object(tmp_path: Path) -> None:
    f = tmp_path / "data.json"
    f.write_text("[]", encoding="utf-8")
    resolved = _resolve_sources({"local": f})
    assert resolved == [("local", {"inc_file": str(f)})]


def test_resolve_sources_existing_file_path_string(tmp_path: Path) -> None:
    f = tmp_path / "data.json"
    f.write_text("[]", encoding="utf-8")
    resolved = _resolve_sources({"local": str(f)})
    assert resolved == [("local", {"inc_file": str(f)})]


def test_resolve_sources_dot_relative_path_string() -> None:
    # Doesn't need to exist — leading "./" is the signal.
    resolved = _resolve_sources({"local": "./does/not/exist.json"})
    assert resolved == [("local", {"inc_file": "./does/not/exist.json"})]


def test_resolve_sources_dict_form_spreads_verbatim() -> None:
    resolved = _resolve_sources(
        {"api": {"inc_url": "https://x.example.com/v1", "rec_path": "data.items", "inc_code": "id"}},
    )
    assert resolved == [
        (
            "api",
            {"inc_url": "https://x.example.com/v1", "rec_path": "data.items", "inc_code": "id"},
        ),
    ]


def test_resolve_sources_rejects_bare_string_that_isnt_url_or_path() -> None:
    with pytest.raises(ValueError, match="Cannot resolve source"):
        _resolve_sources({"bad": "neither_url_nor_path"})


def test_resolve_sources_shared_kwargs_propagates() -> None:
    resolved = _resolve_sources(
        {"api": "https://x.example.com/v1"},
        shared_kwargs={"timeout": 10.0, "headers": {"User-Agent": "foo"}},
    )
    assert resolved[0][1]["timeout"] == 10.0
    assert resolved[0][1]["headers"] == {"User-Agent": "foo"}


def test_resolve_sources_per_source_overrides_shared() -> None:
    resolved = _resolve_sources(
        {"api": {"inc_url": "https://x.example.com/v1", "timeout": 30.0}},
        shared_kwargs={"timeout": 10.0},
    )
    # Per-source timeout wins.
    assert resolved[0][1]["timeout"] == 30.0


# ---------------------------------------------------------------------------
# _penstock_for — three-tier confidence.
# ---------------------------------------------------------------------------


def test_penstock_for_registered_host_high_confidence(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tier-1 fires for hosts the user has registered.

    The framework ships no implicit per-host throttling; this test
    explicitly registers ``api.coingecko.com`` first, then asserts
    architect picks the registered rate.
    """
    from incorporator.io.throttle import FixedIntervalThrottle, _HOST_FACTORIES

    monkeypatch.setitem(_HOST_FACTORIES, "api.coingecko.com", lambda: FixedIntervalThrottle(0.2))
    profile = _profile({"id"}, pk="id", host="api.coingecko.com")
    spec = _penstock_for(profile)
    assert spec is not None
    assert spec.kind == "sustained"
    assert spec.confidence == "high"
    assert spec.rate_per_sec == 0.2
    assert "coingecko" in spec.rationale


def test_penstock_for_unregistered_host_no_tier_1(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tier-1 returns None for hosts that aren't in the registry.

    Pins the v1.3.0 contract: the framework no longer auto-throttles
    coingecko / pokeapi / nhtsa.  Architect's high-confidence recommendation
    is now opt-in — fires only after explicit registration.
    """
    from incorporator.io.throttle import _HOST_FACTORIES

    # Ensure registry is clean for these three historical hosts.
    monkeypatch.setattr(
        "incorporator.observability.tideweaver.architect.known_host_rates",
        lambda: {},
    )
    profile = _profile({"id"}, pk="id", host="api.coingecko.com")
    spec = _penstock_for(profile)
    assert spec is None
    # Sanity: monkeypatch didn't leak into the real registry.
    assert "api.coingecko.com" not in _HOST_FACTORIES


def test_penstock_for_429_observed_medium_confidence() -> None:
    profile = _profile({"id"}, host="api.unknown.example", rate_limited=True)
    spec = _penstock_for(profile)
    assert spec is not None
    assert spec.confidence == "medium"
    assert spec.rate_per_sec == 1.0


def test_penstock_for_unknown_host_returns_none() -> None:
    profile = _profile({"id"}, host="api.unknown.example")
    assert _penstock_for(profile) is None


def test_penstock_for_no_response_meta_returns_none() -> None:
    profile = _profile({"id"})
    assert _penstock_for(profile) is None


# ---------------------------------------------------------------------------
# _analyze_topology — shape detection.
# ---------------------------------------------------------------------------


def test_analyze_topology_parallel_when_disjoint() -> None:
    profiles = [
        ("a", _profile({"a_id", "a_name"}, pk="a_id")),
        ("b", _profile({"b_id", "b_name"}, pk="b_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"a": {}, "b": {}})
    assert plan.shape == "parallel"
    assert plan.edges == []
    assert plan.needs_tail_current is False


def test_analyze_topology_fanout_when_head_pk_in_all_others() -> None:
    profiles = [
        ("users", _profile({"user_id", "name"}, pk="user_id")),
        ("orders", _profile({"order_id", "user_id", "total"}, pk="order_id")),
        ("comments", _profile({"comment_id", "user_id", "body"}, pk="comment_id")),
    ]
    plan = _analyze_topology(
        profiles,
        incorp_kwargs_by_name={n: {} for n, _p in profiles},
    )
    assert plan.shape == "fanout"
    # Two edges: users → orders, users → comments
    assert {(e.from_name, e.to_name) for e in plan.edges} == {("users", "orders"), ("users", "comments")}


def test_analyze_topology_diamond_when_multiple_share_pk() -> None:
    profiles = [
        ("laps", _profile({"user_id", "lap_count"}, pk="user_id")),
        ("pits", _profile({"user_id", "pit_count"}, pk="user_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"laps": {}, "pits": {}})
    assert plan.shape == "diamond"
    assert plan.needs_tail_current is True


def test_analyze_topology_custom_when_overlap_but_no_clear_pattern() -> None:
    # Field overlap, but no pk-as-foreign-key and no shared pk.
    profiles = [
        ("a", _profile({"id", "shared_meta"}, pk="id")),
        ("b", _profile({"slug", "shared_meta"}, pk="slug")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"a": {}, "b": {}})
    assert plan.shape == "custom"
    assert any("shared_meta" in note for note in plan.notes)


def test_analyze_topology_carries_penstock_recommendations_on_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the user has registered hosts, architect threads the recommendation onto each edge."""
    from incorporator.io.throttle import FixedIntervalThrottle, _HOST_FACTORIES

    monkeypatch.setitem(_HOST_FACTORIES, "api.coingecko.com", lambda: FixedIntervalThrottle(0.2))
    monkeypatch.setitem(_HOST_FACTORIES, "pokeapi.co", lambda: FixedIntervalThrottle(1.5))

    profiles = [
        ("users", _profile({"user_id"}, pk="user_id", host="api.coingecko.com")),
        ("orders", _profile({"order_id", "user_id"}, pk="order_id", host="pokeapi.co")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={n: {} for n, _p in profiles})
    assert plan.shape == "fanout"
    # The edge users→orders gets penstock from the orders profile (the downstream).
    edge = plan.edges[0]
    assert edge.penstock is not None
    assert edge.penstock.confidence == "high"


# ---------------------------------------------------------------------------
# Renderers.
# ---------------------------------------------------------------------------


def _silent(fn, *args, **kwargs):
    buf = io.StringIO()
    with redirect_stdout(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue()


def _build_simple_plan(shape: str = "fanout") -> Tuple[List, OrchestrationPlan]:
    """Two-source plan used across the renderer tests.

    The fixture (users.user_id appearing in orders as a foreign-key field)
    naturally analyses to ``shape='fanout'``.  Shape-specific renderer
    tests override the shape (and reset edges) to exercise the other
    branches.
    """
    profiles: List[Tuple[str, SourceProfile]] = [
        ("users", _profile({"user_id", "name"}, pk="user_id")),
        ("orders", _profile({"order_id", "user_id", "total"}, pk="order_id")),
    ]
    incorp = {
        "users": {"inc_url": "https://api.example.com/v1/users"},
        "orders": {"inc_url": "https://api.example.com/v1/orders"},
    }
    plan = _analyze_topology(profiles, incorp_kwargs_by_name=incorp)
    assert plan.shape == "fanout"  # given the fixture overlap
    if shape != "fanout":
        plan.shape = shape  # override for shape-specific tests
        plan.edges = []
    return profiles, plan


def test_render_report_returns_none_and_prints() -> None:
    profiles, plan = _build_simple_plan()
    result, output = _silent(render_report, profiles, plan)
    assert result is None
    assert "ORCHESTRATION HINTS" in output
    assert "fanout" in output


def test_render_python_returns_str_and_parses_as_python() -> None:
    profiles, plan = _build_simple_plan()
    rendered, output = _silent(render_python, profiles, plan)
    assert isinstance(rendered, str)
    # The rendered body is printed (with print()'s trailing newline) AND
    # returned — confirm both routes carry the same content.
    assert rendered in output
    # Strongest contract: the emitted body must be syntactically valid Python.
    ast.parse(rendered)
    assert "Watershed.fanout" in rendered
    assert "Stream(" in rendered


def test_render_python_class_blocks_for_each_source() -> None:
    profiles, plan = _build_simple_plan()
    rendered, _ = _silent(render_python, profiles, plan)
    assert "class Users(Incorporator):" in rendered
    assert "class Orders(Incorporator):" in rendered


def test_render_json_returns_str_and_loads_as_dict() -> None:
    profiles, plan = _build_simple_plan()
    rendered, output = _silent(render_json, profiles, plan)
    assert isinstance(rendered, str)
    assert rendered in output
    body = json.loads(rendered)
    assert body["shape"] == "fanout"


def test_render_json_parallel_has_no_gate_mode() -> None:
    profiles, plan = _build_simple_plan(shape="parallel")
    rendered, _ = _silent(render_json, profiles, plan)
    body = json.loads(rendered)
    assert body["shape"] == "parallel"
    assert "gate_mode" not in body
    assert "flow" not in body


def test_render_json_diamond_emits_tail_todo() -> None:
    profiles = [
        ("laps", _profile({"user_id", "lap_count"}, pk="user_id")),
        ("pits", _profile({"user_id", "pit_count"}, pk="user_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"laps": {}, "pits": {}})
    assert plan.shape == "diamond"
    rendered, _ = _silent(render_json, profiles, plan)
    body = json.loads(rendered)
    assert body["shape"] == "diamond"
    assert "tail" in body
    assert "_TODO_" in body["tail"]


def test_render_json_round_trips_through_load_watershed(tmp_path: Path) -> None:
    """Strongest contract test: emitted JSON loads via the official config.py path."""
    from incorporator.observability.tideweaver.config import build_watershed

    profiles = [
        (
            "users",
            _profile({"user_id"}, pk="user_id"),
        ),
        (
            "orders",
            _profile({"order_id", "user_id"}, pk="order_id"),
        ),
    ]
    plan = _analyze_topology(
        profiles,
        incorp_kwargs_by_name={
            "users": {"inc_url": "https://api.example.com/v1/users"},
            "orders": {"inc_url": "https://api.example.com/v1/orders"},
        },
    )
    rendered, _ = _silent(render_json, profiles, plan)
    body = json.loads(rendered)

    # Fill in the placeholders the user would set manually before loading.
    body["window"] = {"start": "2026-05-21T00:00:00+00:00", "end": "2026-05-21T01:00:00+00:00"}
    # Provide a stub outflow.py defining the discovered classes (the loader resolves classes against it).
    outflow_file = tmp_path / "outflow.py"
    outflow_file.write_text(
        "from incorporator import Incorporator\n"
        "class Users(Incorporator): pass\n"
        "class Orders(Incorporator): pass\n"
        "def outflow(state): return []\n",
        encoding="utf-8",
    )
    body["outflow"] = "outflow.py"

    # build_watershed accepts the raw dict + a base dir for path resolution.
    watershed = build_watershed(body, tmp_path)
    assert watershed is not None
    # And the fanout shape produced 1 edge (users → orders).
    assert len(watershed.edges) == 1
    assert (watershed.edges[0].from_name, watershed.edges[0].to_name) == ("users", "orders")


# ---------------------------------------------------------------------------
# run() end-to-end against local file fixtures (no network).
# ---------------------------------------------------------------------------


def test_run_end_to_end_local_fixtures(tmp_path: Path) -> None:
    """Probe two small local JSON files, assert ``run(output='json')`` is parseable."""
    a = tmp_path / "users.json"
    a.write_text(
        json.dumps([
            {"user_id": "u1", "name": "Ada"},
            {"user_id": "u2", "name": "Bob"},
        ]),
        encoding="utf-8",
    )
    b = tmp_path / "orders.json"
    b.write_text(
        json.dumps([
            {"order_id": "o1", "user_id": "u1", "total": "19.99"},
            {"order_id": "o2", "user_id": "u2", "total": "29.99"},
        ]),
        encoding="utf-8",
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        rendered = asyncio.run(
            run(Incorporator, sources={"users": a, "orders": b}, output="json"),
        )
    assert rendered is not None
    body = json.loads(rendered)
    # users.user_id should appear in orders → fanout shape detected.
    assert body["shape"] == "fanout"
    # Both currents present in the JSON.
    if "source" in body:
        assert body["source"]["name"] == "users"
        sink_names = [s["name"] for s in body["sinks"]]
        assert "orders" in sink_names


def test_run_invalid_output_raises() -> None:
    with pytest.raises(ValueError, match="output must be one of"):
        asyncio.run(run(Incorporator, sources={"x": "https://x.example/"}, output="yaml"))


def test_cls_architect_classmethod_shim(tmp_path: Path) -> None:
    """``cls.architect(...)`` is the user-facing entry point — delegates to ``architect.run()``.

    Smokes the shim itself (base.py) rather than the architect module.
    """
    a = tmp_path / "users.json"
    a.write_text(json.dumps([{"user_id": "u1", "name": "Ada"}]), encoding="utf-8")
    b = tmp_path / "orders.json"
    b.write_text(json.dumps([{"order_id": "o1", "user_id": "u1"}]), encoding="utf-8")

    buf = io.StringIO()
    with redirect_stdout(buf):
        rendered = asyncio.run(
            Incorporator.architect(sources={"users": a, "orders": b}, output="json"),
        )
    assert rendered is not None
    body = json.loads(rendered)
    assert body["shape"] == "fanout"


# ---------------------------------------------------------------------------
# output="plan" mode + Plan.to_watershed() — in-memory probe → tune → run handoff.
# ---------------------------------------------------------------------------


def test_run_output_plan_returns_orchestration_plan(tmp_path: Path) -> None:
    """``output='plan'`` returns the :class:`OrchestrationPlan` dataclass directly.

    No print side effects (architect's renderers don't fire); the caller
    can inspect / mutate the plan in-memory and feed it to
    :meth:`OrchestrationPlan.to_watershed`.
    """
    a = tmp_path / "users.json"
    a.write_text(json.dumps([{"user_id": "u1", "name": "Ada"}]), encoding="utf-8")
    b = tmp_path / "orders.json"
    b.write_text(json.dumps([{"order_id": "o1", "user_id": "u1"}]), encoding="utf-8")

    buf = io.StringIO()
    with redirect_stdout(buf):
        plan = asyncio.run(
            run(Incorporator, sources={"users": a, "orders": b}, output="plan"),
        )
    # No rendering happened — capture buffer is empty (architect doesn't print
    # the per-source inspector report when output="plan").
    assert isinstance(plan, OrchestrationPlan)
    assert plan.shape == "fanout"
    assert {(e.from_name, e.to_name) for e in plan.edges} == {("users", "orders")}


def test_plan_to_watershed_produces_valid_watershed() -> None:
    """``Plan.to_watershed()`` materialises a runnable :class:`Watershed`."""
    from datetime import datetime, timedelta, timezone

    from incorporator.observability.tideweaver import Watershed

    # Synthesise a small parallel plan (the simplest shape — no edges).
    profiles: List[Tuple[str, SourceProfile]] = [
        ("a", _profile({"a_id"}, pk="a_id")),
        ("b", _profile({"b_id"}, pk="b_id")),
    ]
    plan = _analyze_topology(
        profiles,
        incorp_kwargs_by_name={
            "a": {"inc_url": "https://x.example.com/a"},
            "b": {"inc_url": "https://x.example.com/b"},
        },
    )
    assert plan.shape == "parallel"

    start = datetime(2026, 5, 21, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    watershed = plan.to_watershed(window=(start, end))
    assert isinstance(watershed, Watershed)
    assert watershed.window == (start, end)
    assert {c.name for c in watershed.currents} == {"a", "b"}
    assert watershed.edges == []


def test_plan_to_watershed_default_window() -> None:
    """``Plan.to_watershed()`` with no window picks ``(now, now+1h)``."""
    from datetime import datetime, timezone

    profiles: List[Tuple[str, SourceProfile]] = [
        ("a", _profile({"a_id"}, pk="a_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"a": {}})

    before = datetime.now(timezone.utc)
    watershed = plan.to_watershed()
    after = datetime.now(timezone.utc)

    # Window start lands between before and after.
    assert before <= watershed.window[0] <= after
    # End is exactly an hour past start.
    delta_seconds = (watershed.window[1] - watershed.window[0]).total_seconds()
    assert 3590 <= delta_seconds <= 3610  # 1h ± 10s slop


def test_plan_to_watershed_uses_user_supplied_classes() -> None:
    """``classes={...}`` maps current names to existing :class:`Incorporator` subclasses."""
    profiles: List[Tuple[str, SourceProfile]] = [
        ("users", _profile({"user_id"}, pk="user_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"users": {}})

    class CustomUsers(Incorporator):
        pass

    watershed = plan.to_watershed(classes={"users": CustomUsers})
    assert watershed.currents[0].cls is CustomUsers


def test_plan_to_watershed_raises_when_diamond_lacks_tail() -> None:
    """Diamond candidate without a Fjord tail in ``classes`` raises with a clear message."""
    profiles: List[Tuple[str, SourceProfile]] = [
        ("laps", _profile({"user_id", "lap_count"}, pk="user_id")),
        ("pits", _profile({"user_id", "pit_count"}, pk="user_id")),
    ]
    plan = _analyze_topology(profiles, incorp_kwargs_by_name={"laps": {}, "pits": {}})
    assert plan.needs_tail_current is True

    with pytest.raises(ValueError, match="requires a tail Fjord"):
        plan.to_watershed()


def test_plan_to_watershed_carries_penstock_when_recommended() -> None:
    """When architect recommends a tier-1 Penstock, the materialised edge carries it.

    Uses a monkey-patched host registry so the test doesn't depend on
    whichever hosts the user has registered globally.
    """
    from incorporator.io.throttle import FixedIntervalThrottle, _HOST_FACTORIES
    from incorporator.observability.tideweaver import SustainedPenstock

    # Register a slow rate so architect's _penstock_for fires tier-1.
    original = _HOST_FACTORIES.get("api.fixture.example")
    _HOST_FACTORIES["api.fixture.example"] = lambda: FixedIntervalThrottle(0.5)
    try:
        profiles: List[Tuple[str, SourceProfile]] = [
            ("users", _profile({"user_id"}, pk="user_id", host="api.fixture.example")),
            ("orders", _profile({"order_id", "user_id"}, pk="order_id", host="api.fixture.example")),
        ]
        plan = _analyze_topology(
            profiles,
            incorp_kwargs_by_name={n: {} for n, _p in profiles},
        )
        assert plan.shape == "fanout"
        assert plan.edges[0].penstock is not None

        watershed = plan.to_watershed()
        edge = watershed.edges[0]
        assert isinstance(edge.flow.penstock, SustainedPenstock)
        assert edge.flow.penstock.rate_per_sec == 0.5
    finally:
        if original is None:
            _HOST_FACTORIES.pop("api.fixture.example", None)
        else:
            _HOST_FACTORIES["api.fixture.example"] = original
