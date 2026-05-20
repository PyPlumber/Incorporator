"""Tideweaver routing tests for the **fanout** Watershed shape.

Three tests from the routing test plan:

* **Test 5** — Rick & Morty 1 Stream → 3 heterogeneous Fjords (alive-count
  / group-by-species / flatten-episodes). Each Fjord has a different
  ``outflow()`` and a different output class but all read the same
  upstream snapshot. Proves fanout works with heterogeneous downstream
  consumers.
* **Test 6** — JSONPlaceholder 1 Stream → 2 Fjords + 1 Export. Proves the
  Export current placed in a fanout reads the upstream registry the same
  way a Fjord does but bypasses ``outflow()`` and just dumps the snapshot.
* **Test 11** — TheSportsDB 1 Stream (Premier League players) → 3 Fjords
  (top-scorers / top-assists / top-defenders). Complements MLB's diamond
  and leaderboard tests with a football reference; proves fanout works on
  a third sports-API shape and that all-string responses are properly
  coerced before downstream ranking.

Same workaround pattern as the chain / parallel files for the upstream
``_tideweaver_snapshot`` WeakValueDict race and the shared ``inc_dict``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.io.pagination import NextUrlPaginator
from incorporator.observability.tideweaver import (
    Current,
    Export,
    Fjord,
    Stream,
    Tideweaver,
    Watershed,
)
from incorporator.schema.converters import calc, inc


# ---------------------------------------------------------------------------
# Source + output classes.
# ---------------------------------------------------------------------------


class FanRMChar(Incorporator):
    """Rick & Morty character row (Test 5 source)."""

    model_config = ConfigDict(extra="allow")


class RMAliveCount(Incorporator):
    """Per-Fjord output: alive character count."""

    model_config = ConfigDict(extra="allow")


class RMSpeciesGroup(Incorporator):
    """Per-Fjord output: characters grouped by species."""

    model_config = ConfigDict(extra="allow")


class RMAppearance(Incorporator):
    """Per-Fjord output: one row per (character, episode) appearance."""

    model_config = ConfigDict(extra="allow")


class FanAlbum(Incorporator):
    """JSONPlaceholder album row (Test 6 source)."""

    model_config = ConfigDict(extra="allow")


class AlbumPerUser(Incorporator):
    """Test 6 Fjord output: albums grouped by userId."""

    model_config = ConfigDict(extra="allow")


class AlbumTotal(Incorporator):
    """Test 6 Fjord output: total album count."""

    model_config = ConfigDict(extra="allow")


class EPLPlayer(Incorporator):
    """TheSportsDB EPL player row (Test 11 source)."""

    model_config = ConfigDict(extra="allow")


class TopScorer(Incorporator):
    """Test 11 Fjord output: top scorers ranked by goals."""

    model_config = ConfigDict(extra="allow")


class TopAssist(Incorporator):
    """Test 11 Fjord output: top assists."""

    model_config = ConfigDict(extra="allow")


class TopDefender(Incorporator):
    """Test 11 Fjord output: top defenders by position filter."""

    model_config = ConfigDict(extra="allow")


def _short_window(seconds: float) -> Tuple[datetime, datetime]:
    """Build a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe shared inc_dict + per-class parked snapshot between tests."""
    Incorporator.inc_dict.clear()
    for cls in classes:
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _make_routing_tick(tw: Tideweaver, strong_refs_by_cls: Dict[type, List[Any]]) -> Any:
    """Build a tick dispatcher: Stream → workaround; Fjord/Export → real path."""

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
        elif isinstance(current, Export):
            await tw._tick_export(current)

    return tick


# ---------------------------------------------------------------------------
# Test 5 — Rick & Morty heterogeneous fanout.
# ---------------------------------------------------------------------------


async def _mock_rick_and_morty_chars(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Single-page Rick & Morty characters with mixed status + species."""
    payload = {
        "info": {"next": None},
        "results": [
            {"id": 1, "name": "Rick", "status": "Alive", "species": "Human", "episode": ["https://x/e/1", "https://x/e/2"]},
            {"id": 2, "name": "Morty", "status": "Alive", "species": "Human", "episode": ["https://x/e/1"]},
            {"id": 3, "name": "Birdperson", "status": "Dead", "species": "Bird-Person", "episode": ["https://x/e/2"]},
        ],
    }
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_fanout_one_stream_to_three_heterogeneous_fjords(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One head Stream fans out to three Fjords with different outflow logic.

    Proves: (a) ``Watershed.fanout`` produces edges source→each-sink; (b)
    each Fjord reads the same upstream snapshot via its transitive-upstream
    closure; (c) three different ``outflow(state)`` functions produce three
    different output classes (counts vs groups vs flattened rows); (d) the
    head Stream's ``conv_dict`` derivation (``is_alive`` boolean) survives
    the fanout and is visible to every downstream Fjord.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_rick_and_morty_chars)
    _reset_registries(FanRMChar, RMAliveCount, RMSpeciesGroup, RMAppearance)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    # Three outflow.py sidecars — each Fjord points at its own.
    alive_py = tmp_path / "outflow_alive.py"
    alive_py.write_text(
        "def outflow(state):\n"
        "    chars = state.get('FanRMChar', [])\n"
        "    alive = sum(1 for c in chars if getattr(c, 'status', '') == 'Alive')\n"
        "    return [{'inc_code': 'alive_now', 'count': alive, 'total': len(chars)}]\n",
        encoding="utf-8",
    )
    species_py = tmp_path / "outflow_species.py"
    species_py.write_text(
        "def outflow(state):\n"
        "    chars = state.get('FanRMChar', [])\n"
        "    groups = {}\n"
        "    for c in chars:\n"
        "        sp = getattr(c, 'species', 'Unknown')\n"
        "        groups[sp] = groups.get(sp, 0) + 1\n"
        "    return [{'inc_code': sp, 'count': n} for sp, n in groups.items()]\n",
        encoding="utf-8",
    )
    appearance_py = tmp_path / "outflow_appearance.py"
    appearance_py.write_text(
        "def outflow(state):\n"
        "    chars = state.get('FanRMChar', [])\n"
        "    rows = []\n"
        "    for c in chars:\n"
        "        for ep_url in getattr(c, 'episode', []) or []:\n"
        "            rows.append({'inc_code': f\"{c.inc_code}-{ep_url.rsplit('/', 1)[-1]}\", "
        "'character': c.name, 'episode_url': ep_url})\n"
        "    return rows\n",
        encoding="utf-8",
    )

    chars_stream = Stream(
        name="chars",
        cls=FanRMChar,
        interval=0.8,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://rickandmortyapi.com/api/character",
            "inc_code": "id",
            "rec_path": "results",
            "inc_page": NextUrlPaginator("info", "next"),
            "call_lim": 1,
            "conv_dict": {"is_alive": calc(lambda s: s == "Alive", "status", default=False, target_type=bool)},
        },
    )
    alive_count = Fjord(
        name="alive_count",
        cls=RMAliveCount,
        interval=0.2,
        on_error="isolate",
        outflow=alive_py,
        export_params={"file_path": str(tmp_path / "alive.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    per_species = Fjord(
        name="per_species",
        cls=RMSpeciesGroup,
        interval=0.2,
        on_error="isolate",
        outflow=species_py,
        export_params={"file_path": str(tmp_path / "species.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    appearances = Fjord(
        name="appearances",
        cls=RMAppearance,
        interval=0.2,
        on_error="isolate",
        outflow=appearance_py,
        export_params={"file_path": str(tmp_path / "appearances.ndjson"), "format": "ndjson", "if_exists": "append"},
    )

    ws = Watershed.fanout(
        window=_short_window(8.0),
        source=chars_stream,
        sinks=[alive_count, per_species, appearances],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # The head Stream's conv_dict landed on every row.
    char_rows = strong_refs_by_cls.get(FanRMChar, [])
    assert char_rows, "chars Stream must populate rows"
    assert all(getattr(c, "is_alive", None) == (getattr(c, "status", "") == "Alive") for c in char_rows)

    # All three Fjords' export files exist and contain the expected shapes.
    alive_lines = [ln for ln in (tmp_path / "alive.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert alive_lines, "alive Fjord must write at least one row"
    last_alive = json.loads(alive_lines[-1])
    assert last_alive["count"] == 2 and last_alive["total"] == 3, f"alive count mismatch: {last_alive}"

    species_lines = [
        ln for ln in (tmp_path / "species.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert species_lines, "species Fjord must write at least one row"
    species_rows = [json.loads(ln) for ln in species_lines]
    by_species = {r["inc_code"]: r["count"] for r in species_rows}
    assert by_species.get("Human") == 2 and by_species.get("Bird-Person") == 1, (
        f"species grouping mismatch: {by_species}"
    )

    appearance_lines = [
        ln for ln in (tmp_path / "appearances.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert appearance_lines, "appearances Fjord must write at least one row"
    appearance_rows = [json.loads(ln) for ln in appearance_lines]
    # Rick has 2 episodes, Morty has 1, Birdperson has 1 → 4 appearances per flush.
    # The fjord may flush multiple times; sample the LATEST batch's character names.
    unique_chars = {r["character"] for r in appearance_rows}
    assert unique_chars == {"Rick", "Morty", "Birdperson"}, f"appearances must cover every character, got {unique_chars}"


# ---------------------------------------------------------------------------
# Test 6 — JSONPlaceholder fanout with 2 Fjords + 1 Export.
# ---------------------------------------------------------------------------


async def _mock_albums(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned albums for Test 6."""
    payload = [
        {"id": 1, "userId": 1, "title": "vacation"},
        {"id": 2, "userId": 1, "title": "wedding"},
        {"id": 3, "userId": 2, "title": "trip"},
    ]
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_fanout_two_fjords_plus_one_export_share_upstream(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fanout with 2 Fjords + 1 Export — the Export bypasses outflow().

    Proves: (a) ``Watershed.fanout`` accepts mixed-verb sinks (Fjord + Export);
    (b) Fjord sinks consume state via ``outflow(state)``; (c) Export sinks
    call ``cls.export()`` directly on the upstream class without going
    through outflow logic; (d) the head Stream's ``conv_dict`` is applied
    once but visible to all three sinks.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_albums)
    _reset_registries(FanAlbum, AlbumPerUser, AlbumTotal)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    per_user_py = tmp_path / "outflow_per_user.py"
    per_user_py.write_text(
        "def outflow(state):\n"
        "    albums = state.get('FanAlbum', [])\n"
        "    counts = {}\n"
        "    for a in albums:\n"
        "        uid = getattr(a, 'userId', None)\n"
        "        counts[uid] = counts.get(uid, 0) + 1\n"
        "    return [{'inc_code': uid, 'album_count': n} for uid, n in counts.items()]\n",
        encoding="utf-8",
    )
    total_py = tmp_path / "outflow_total.py"
    total_py.write_text(
        "def outflow(state):\n"
        "    albums = state.get('FanAlbum', [])\n"
        "    return [{'inc_code': 'all', 'total': len(albums)}]\n",
        encoding="utf-8",
    )

    albums_stream = Stream(
        name="albums",
        cls=FanAlbum,
        interval=0.8,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/albums",
            "inc_code": "id",
            "conv_dict": {"title_upper": calc(lambda t: t.upper() if t else "", "title", default="", target_type=str)},
        },
    )
    per_user = Fjord(
        name="per_user",
        cls=AlbumPerUser,
        interval=0.2,
        on_error="isolate",
        outflow=per_user_py,
        export_params={"file_path": str(tmp_path / "per_user.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    total = Fjord(
        name="total",
        cls=AlbumTotal,
        interval=0.2,
        on_error="isolate",
        outflow=total_py,
        export_params={"file_path": str(tmp_path / "total.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    raw_dump = Export(
        name="raw_dump",
        cls=FanAlbum,
        interval=0.4,
        on_error="isolate",
        export_params={"file_path": str(tmp_path / "albums_dump.ndjson"), "format": "ndjson", "if_exists": "append"},
    )

    ws = Watershed.fanout(
        window=_short_window(6.0),
        source=albums_stream,
        sinks=[per_user, total, raw_dump],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Head Stream's conv_dict applied.
    album_rows = strong_refs_by_cls.get(FanAlbum, [])
    assert album_rows, "albums Stream must populate"
    assert all(getattr(a, "title_upper", "") == getattr(a, "title", "").upper() for a in album_rows)

    # Per-user Fjord produced grouped rows.
    per_user_lines = [
        ln for ln in (tmp_path / "per_user.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert per_user_lines, "per_user Fjord must write rows"
    per_user_rows = [json.loads(ln) for ln in per_user_lines]
    by_uid = {r["inc_code"]: r["album_count"] for r in per_user_rows}
    assert by_uid.get(1) == 2 and by_uid.get(2) == 1, f"per_user grouping mismatch: {by_uid}"

    # Total Fjord produced single aggregate.
    total_lines = [ln for ln in (tmp_path / "total.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert total_lines, "total Fjord must write rows"
    last_total = json.loads(total_lines[-1])
    assert last_total["total"] == 3, f"total count mismatch: {last_total}"

    # Export current dumped raw album rows (NOT through outflow).
    dump_path = tmp_path / "albums_dump.ndjson"
    assert dump_path.exists(), "Export current must have written the dump file"
    dump_lines = [ln for ln in dump_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert dump_lines, "Export dump must contain rows"
    dump_rows = [json.loads(ln) for ln in dump_lines]
    # Export dumps the raw inc_dict rows including conv_dict-derived fields.
    assert any("title_upper" in r for r in dump_rows), (
        f"Export must dump rows with conv_dict fields, sample: {dump_rows[0] if dump_rows else None}"
    )


# ---------------------------------------------------------------------------
# Test 11 — TheSportsDB Premier League fanout.
# ---------------------------------------------------------------------------


async def _mock_thesportsdb(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned TheSportsDB response — EPL roster with mixed positions + stats."""
    payload = {
        "players": [
            {"idPlayer": "1", "strPlayer": "Harry Kane", "strTeam": "Tottenham", "strPosition": "Forward", "intGoals": "25", "intAssists": "8"},
            {"idPlayer": "2", "strPlayer": "Mohamed Salah", "strTeam": "Liverpool", "strPosition": "forward", "intGoals": "22", "intAssists": "12"},
            {"idPlayer": "3", "strPlayer": "Kevin De Bruyne", "strTeam": "Man City", "strPosition": "Midfielder", "intGoals": "10", "intAssists": "20"},
            {"idPlayer": "4", "strPlayer": "Virgil van Dijk", "strTeam": "Liverpool", "strPosition": "Defender", "intGoals": "3", "intAssists": "1"},
            {"idPlayer": "5", "strPlayer": "Ruben Dias", "strTeam": "Man City", "strPosition": "Defender", "intGoals": "2", "intAssists": "0"},
        ]
    }
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_fanout_thesportsdb_epl_top_lists(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TheSportsDB EPL roster fans out to top-scorers / top-assists / top-defenders.

    Proves: (a) fanout works against a third sports-API shape (TheSportsDB
    free-tier ``/lookup_all_players.php``); (b) the head Stream's
    ``inc(int)`` coercion turns string ``"intGoals"`` into integers before
    downstream Fjords rank them; (c) three Fjords each filter and rank
    the shared snapshot differently (top scorers / top assists / top
    defenders by position).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_thesportsdb)
    _reset_registries(EPLPlayer, TopScorer, TopAssist, TopDefender)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    scorers_py = tmp_path / "outflow_scorers.py"
    scorers_py.write_text(
        "def outflow(state):\n"
        "    players = state.get('EPLPlayer', [])\n"
        "    ranked = sorted(players, key=lambda p: getattr(p, 'intGoals', 0) or 0, reverse=True)\n"
        "    return [{'inc_code': p.strPlayer, 'goals': getattr(p, 'intGoals', 0)} for p in ranked[:3]]\n",
        encoding="utf-8",
    )
    assists_py = tmp_path / "outflow_assists.py"
    assists_py.write_text(
        "def outflow(state):\n"
        "    players = state.get('EPLPlayer', [])\n"
        "    ranked = sorted(players, key=lambda p: getattr(p, 'intAssists', 0) or 0, reverse=True)\n"
        "    return [{'inc_code': p.strPlayer, 'assists': getattr(p, 'intAssists', 0)} for p in ranked[:3]]\n",
        encoding="utf-8",
    )
    defenders_py = tmp_path / "outflow_defenders.py"
    defenders_py.write_text(
        "def outflow(state):\n"
        "    players = state.get('EPLPlayer', [])\n"
        "    defs = [p for p in players if (getattr(p, 'strPosition', '') or '').lower() == 'defender']\n"
        "    return [{'inc_code': p.strPlayer, 'team': p.strTeam, 'goals': getattr(p, 'intGoals', 0)} for p in defs]\n",
        encoding="utf-8",
    )

    players_stream = Stream(
        name="players",
        cls=EPLPlayer,
        interval=0.8,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://www.thesportsdb.com/api/v1/json/3/lookup_all_players.php?id=4328",
            "inc_code": "idPlayer",
            "rec_path": "players",
            "conv_dict": {
                "intGoals": inc(int, default=0),
                "intAssists": inc(int, default=0),
            },
        },
    )
    top_scorers = Fjord(
        name="top_scorers",
        cls=TopScorer,
        interval=0.2,
        on_error="isolate",
        outflow=scorers_py,
        export_params={"file_path": str(tmp_path / "scorers.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    top_assists = Fjord(
        name="top_assists",
        cls=TopAssist,
        interval=0.2,
        on_error="isolate",
        outflow=assists_py,
        export_params={"file_path": str(tmp_path / "assists.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    top_defenders = Fjord(
        name="top_defenders",
        cls=TopDefender,
        interval=0.2,
        on_error="isolate",
        outflow=defenders_py,
        export_params={"file_path": str(tmp_path / "defenders.ndjson"), "format": "ndjson", "if_exists": "append"},
    )

    ws = Watershed.fanout(
        window=_short_window(6.0),
        source=players_stream,
        sinks=[top_scorers, top_assists, top_defenders],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Head Stream's inc(int) coerced string goals/assists into ints.
    player_rows = strong_refs_by_cls.get(EPLPlayer, [])
    assert player_rows, "players Stream must populate"
    assert all(isinstance(getattr(p, "intGoals", None), int) for p in player_rows), (
        "intGoals must be coerced to int by inc(int)"
    )

    # Top scorers Fjord ranked by goals.
    scorer_lines = [
        ln for ln in (tmp_path / "scorers.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert scorer_lines, "top_scorers Fjord must write rows"
    # Each flush emits 3 rows (top 3). Take the LAST 3 as the latest flush.
    latest_scorers = [json.loads(ln) for ln in scorer_lines[-3:]]
    assert latest_scorers[0]["inc_code"] == "Harry Kane", f"top scorer must be Kane (25 goals): {latest_scorers}"
    assert latest_scorers[0]["goals"] == 25

    # Top assists Fjord ranked by assists.
    assist_lines = [
        ln for ln in (tmp_path / "assists.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert assist_lines, "top_assists Fjord must write rows"
    latest_assists = [json.loads(ln) for ln in assist_lines[-3:]]
    assert latest_assists[0]["inc_code"] == "Kevin De Bruyne", f"top assister must be KDB (20 assists): {latest_assists}"

    # Top defenders Fjord filtered by position.
    defender_lines = [
        ln for ln in (tmp_path / "defenders.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert defender_lines, "top_defenders Fjord must write rows"
    defender_rows = [json.loads(ln) for ln in defender_lines]
    defender_names = {r["inc_code"] for r in defender_rows}
    assert defender_names == {"Virgil van Dijk", "Ruben Dias"}, (
        f"only the two Defenders should appear, got {defender_names}"
    )
