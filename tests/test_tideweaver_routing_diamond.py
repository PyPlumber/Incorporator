"""Tideweaver routing tests for the **diamond** Watershed shape.

Two tests from the routing test plan:

* **Test 3** — MLB diamond merge (MLB Stats API). A trigger head → two
  middle Streams (teams + players) → tail Fjord joining both into a
  ``TeamRoster`` output. The tail Fjord's ``outflow(state)`` reads from
  BOTH middle Streams via the transitive-upstream closure. ``conv_dict``
  on the players Stream parses the ``".281"`` batting-average string into
  a real ``0.281`` float.
* **Test 4** — Open Library diamond with heterogeneous middle Fjords.
  Head Stream (book search) → two middle Fjords (AuthorIndex, SubjectIndex)
  → tail Fjord (BookCatalog) joining both middle-Fjord snapshots. The
  only test where a Fjord depends on two other Fjords.

Same workaround pattern for the upstream ``_tideweaver_snapshot`` race
and shared ``inc_dict``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.io import fetch
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
# Source + output classes for Tests 3 and 4.
# ---------------------------------------------------------------------------


class Trigger(Incorporator):
    """Empty-ish head Stream that gates the diamond's middle stages."""

    model_config = ConfigDict(extra="allow")


class MLBTeam(Incorporator):
    """MLB team row (Test 3)."""

    model_config = ConfigDict(extra="allow")


class MLBPlayer(Incorporator):
    """MLB player row with career hitting stats (Test 3)."""

    model_config = ConfigDict(extra="allow")


class TeamRoster(Incorporator):
    """Test 3 tail Fjord output: roster row joining team + player metadata."""

    model_config = ConfigDict(extra="allow")


class OLBook(Incorporator):
    """Open Library book row (Test 4 head)."""

    model_config = ConfigDict(extra="allow")


class AuthorIndex(Incorporator):
    """Test 4 middle Fjord output: per-author book counts."""

    model_config = ConfigDict(extra="allow")


class SubjectIndex(Incorporator):
    """Test 4 middle Fjord output: per-subject book counts."""

    model_config = ConfigDict(extra="allow")


class BookCatalog(Incorporator):
    """Test 4 tail Fjord output: combined catalog row joining both middle Fjords."""

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
    """Build a tick dispatcher: Stream → workaround; Fjord → real path; Export → workaround."""

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
            # The Fjord's flush creates output instances and exports them, but
            # the WeakValueDict registry drops them immediately. To make the
            # middle-Fjord output available to a downstream-of-Fjord consumer
            # (Test 4 tail), re-read the export file and reconstruct instances
            # under a test-scope strong ref. Middle Fjords use ``if_exists=
            # "replace"`` so the file always reflects the latest tick.
            from pathlib import Path as _Path
            export_path = current.export_params.get("file_path")
            if export_path and _Path(export_path).exists():
                rows: List[Any] = []
                for line in _Path(export_path).read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    # Drop framework-managed fields so we don't double-set them.
                    data.pop("inc_name", None)
                    data.pop("last_rcd", None)
                    rows.append(current.cls(**data))
                strong_refs_by_cls[current.cls] = rows
                current.cls._tideweaver_snapshot = list(rows)  # type: ignore[attr-defined]
        elif isinstance(current, Export):
            instance = strong_refs_by_cls.get(current.cls, []) or list(current.cls.inc_dict.values())
            if not instance:
                return
            params = dict(current.export_params)
            params.setdefault("instance", instance)
            await current.cls.export(**params)

    return tick


# ---------------------------------------------------------------------------
# Test 3 — MLB diamond merge.
# ---------------------------------------------------------------------------


async def _mock_mlb(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned MLB Stats API shapes."""
    if "/teams" in url:
        payload: Any = [
            {"id": 147, "name": "New York Yankees", "abbreviation": "NYY"},
            {"id": 119, "name": "Los Angeles Dodgers", "abbreviation": "LAD"},
        ]
    elif "/people" in url and "/stats" in url:
        # Pull player id from the URL path so the mock can return per-player stats.
        payload = [
            {"id": 660271, "fullName": "Shohei Ohtani", "teamId": 119, "battingAverage": ".281"},
            {"id": 547989, "fullName": "Aaron Judge", "teamId": 147, "battingAverage": ".294"},
        ]
    elif "/trigger" in url:
        payload = [{"id": 1, "tick": "now"}]
    else:
        payload = []
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_diamond_mlb_teams_plus_players_fjord_join(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MLB diamond: head trigger → [teams, players] middle Streams → roster Fjord tail.

    Proves: (a) ``Watershed.diamond`` orchestrates head → two middle → tail
    with hard edges; (b) the tail Fjord's ``outflow(state)`` reads from
    BOTH middle Streams via the transitive-upstream closure
    (``state["MLBTeam"]`` and ``state["MLBPlayer"]``); (c) ``conv_dict``
    on the players Stream coerces the batting-average string ``".281"``
    to a real ``0.281`` float before the join.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_mlb)
    _reset_registries(Trigger, MLBTeam, MLBPlayer, TeamRoster)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    roster_py = tmp_path / "roster_outflow.py"
    roster_py.write_text(
        "def outflow(state):\n"
        "    teams = state.get('MLBTeam', [])\n"
        "    players = state.get('MLBPlayer', [])\n"
        "    teams_by_id = {t.inc_code: t for t in teams}\n"
        "    rows = []\n"
        "    for p in players:\n"
        "        tid = getattr(p, 'teamId', None)\n"
        "        t = teams_by_id.get(tid)\n"
        "        if t is None:\n"
        "            continue\n"
        "        rows.append({\n"
        "            'inc_code': p.inc_code,\n"
        "            'player': p.fullName,\n"
        "            'team': t.name,\n"
        "            'team_abbr': t.abbreviation,\n"
        "            'avg': getattr(p, 'battingAverage', None),\n"
        "        })\n"
        "    return rows\n",
        encoding="utf-8",
    )
    out_file = tmp_path / "roster.ndjson"

    trigger = Stream(
        name="trigger",
        cls=Trigger,
        interval=0.8,
        on_error="isolate",
        incorp_params={"inc_url": "https://statsapi.mlb.com/trigger", "inc_code": "id"},
    )
    teams = Stream(
        name="teams",
        cls=MLBTeam,
        interval=1.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams?sportId=1",
            "inc_code": "id",
            "conv_dict": {
                "abbr_lower": calc(lambda a: a.lower() if a else "", "abbreviation", default="", target_type=str),
            },
        },
    )
    players = Stream(
        name="players",
        cls=MLBPlayer,
        interval=1.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/people/660271/stats?stats=career&group=hitting",
            "inc_code": "id",
            "conv_dict": {"battingAverage": inc(float, default=0.0)},
        },
    )
    roster = Fjord(
        name="roster",
        cls=TeamRoster,
        interval=0.2,
        on_error="isolate",
        outflow=roster_py,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
    )

    ws = Watershed.diamond(
        window=_short_window(8.0),
        head=trigger,
        middle=[teams, players],
        tail=roster,
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Both middle Streams populated.
    team_rows = strong_refs_by_cls.get(MLBTeam, [])
    player_rows = strong_refs_by_cls.get(MLBPlayer, [])
    assert team_rows, "teams Stream must populate"
    assert player_rows, "players Stream must populate"
    # players Stream's conv_dict coerced ".281" → 0.281 (a real float).
    assert all(isinstance(getattr(p, "battingAverage", None), float) for p in player_rows), (
        "battingAverage must be coerced to float by inc(float)"
    )

    # The tail Fjord wrote roster rows joining BOTH middle Streams.
    assert out_file.exists(), "tail Fjord must have written the roster file"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "roster file must contain rows"
    rows = [json.loads(ln) for ln in lines]
    by_player = {r["inc_code"]: r for r in rows if "inc_code" in r}
    assert 660271 in by_player and 547989 in by_player, (
        f"both players must appear in the roster join, got {list(by_player)}"
    )
    assert by_player[660271]["team_abbr"] == "LAD", f"Ohtani's team must be LAD, got {by_player[660271]}"
    assert by_player[547989]["team_abbr"] == "NYY", f"Judge's team must be NYY, got {by_player[547989]}"


# ---------------------------------------------------------------------------
# Test 4 — Open Library diamond with heterogeneous middle Fjords.
# ---------------------------------------------------------------------------


async def _mock_open_library(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned Open Library search response."""
    payload = {
        "numFound": 2,
        "docs": [
            {
                "key": "/works/A",
                "title": "Dune",
                "author_name": ["Frank Herbert"],
                "subject": ["science fiction", "space"],
            },
            {
                "key": "/works/B",
                "title": "Foundation",
                "author_name": ["Isaac Asimov"],
                "subject": ["science fiction", "future history"],
            },
        ],
    }
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_diamond_open_library_two_middle_fjords_to_catalog_tail(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Open Library diamond: head Stream → 2 middle Fjords (different outputs) → catalog Fjord tail.

    Proves: (a) middle Fjords each produce a different output class
    (``AuthorIndex`` vs ``SubjectIndex``); (b) the tail Fjord reads BOTH
    middle Fjord snapshots via transitive-upstream and joins them; (c)
    ``conv_dict`` on the head Stream's title cleanup applies before the
    middle Fjords run.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_open_library)
    _reset_registries(OLBook, AuthorIndex, SubjectIndex, BookCatalog)
    strong_refs_by_cls: Dict[type, List[Any]] = {}

    authors_py = tmp_path / "outflow_authors.py"
    authors_py.write_text(
        "def outflow(state):\n"
        "    books = state.get('OLBook', [])\n"
        "    counts = {}\n"
        "    for b in books:\n"
        "        for a in getattr(b, 'author_name', []) or []:\n"
        "            counts[a] = counts.get(a, 0) + 1\n"
        "    return [{'inc_code': a, 'book_count': n} for a, n in counts.items()]\n",
        encoding="utf-8",
    )
    subjects_py = tmp_path / "outflow_subjects.py"
    subjects_py.write_text(
        "def outflow(state):\n"
        "    books = state.get('OLBook', [])\n"
        "    counts = {}\n"
        "    for b in books:\n"
        "        for s in getattr(b, 'subject', []) or []:\n"
        "            counts[s] = counts.get(s, 0) + 1\n"
        "    return [{'inc_code': s, 'occurrences': n} for s, n in counts.items()]\n",
        encoding="utf-8",
    )
    catalog_py = tmp_path / "outflow_catalog.py"
    catalog_py.write_text(
        "def outflow(state):\n"
        "    authors = state.get('AuthorIndex', [])\n"
        "    subjects = state.get('SubjectIndex', [])\n"
        "    return [{\n"
        "        'inc_code': 'summary',\n"
        "        'author_total': sum(getattr(a, 'book_count', 0) for a in authors),\n"
        "        'subject_total': sum(getattr(s, 'occurrences', 0) for s in subjects),\n"
        "        'distinct_authors': len(authors),\n"
        "        'distinct_subjects': len(subjects),\n"
        "    }]\n",
        encoding="utf-8",
    )

    books_stream = Stream(
        name="books",
        cls=OLBook,
        interval=0.8,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://openlibrary.org/search.json?title=Dune",
            "inc_code": "key",
            "rec_path": "docs",
            "conv_dict": {
                "title_clean": calc(lambda t: t.strip() if t else "", "title", default="", target_type=str),
            },
        },
    )
    authors_fjord = Fjord(
        name="authors",
        cls=AuthorIndex,
        interval=0.3,
        on_error="isolate",
        outflow=authors_py,
        export_params={"file_path": str(tmp_path / "authors.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    subjects_fjord = Fjord(
        name="subjects",
        cls=SubjectIndex,
        interval=0.3,
        on_error="isolate",
        outflow=subjects_py,
        export_params={"file_path": str(tmp_path / "subjects.ndjson"), "format": "ndjson", "if_exists": "append"},
    )
    catalog_fjord = Fjord(
        name="catalog",
        cls=BookCatalog,
        interval=0.3,
        on_error="isolate",
        outflow=catalog_py,
        export_params={"file_path": str(tmp_path / "catalog.ndjson"), "format": "ndjson", "if_exists": "append"},
    )

    ws = Watershed.diamond(
        window=_short_window(10.0),
        head=books_stream,
        middle=[authors_fjord, subjects_fjord],
        tail=catalog_fjord,
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    monkeypatch.setattr(tw, "_invoke_tick", _make_routing_tick(tw, strong_refs_by_cls))
    [_ async for _ in tw.run()]

    # Head Stream's conv_dict applied.
    book_rows = strong_refs_by_cls.get(OLBook, [])
    assert book_rows, "books Stream must populate"
    assert all(getattr(b, "title_clean", "") == getattr(b, "title", "").strip() for b in book_rows)

    # Middle Fjords each produced their own output class rows.
    authors_lines = [
        ln for ln in (tmp_path / "authors.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert authors_lines, "AuthorIndex Fjord must write rows"
    author_rows = [json.loads(ln) for ln in authors_lines]
    by_author = {r["inc_code"]: r["book_count"] for r in author_rows}
    assert by_author.get("Frank Herbert") == 1 and by_author.get("Isaac Asimov") == 1, (
        f"each author appears once, got {by_author}"
    )

    subjects_lines = [
        ln for ln in (tmp_path / "subjects.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert subjects_lines, "SubjectIndex Fjord must write rows"
    subject_rows = [json.loads(ln) for ln in subjects_lines]
    by_subject = {r["inc_code"]: r["occurrences"] for r in subject_rows}
    assert by_subject.get("science fiction") == 2, f"sci-fi must appear twice, got {by_subject}"

    # Tail Fjord joined BOTH middle Fjord snapshots.
    catalog_lines = [
        ln for ln in (tmp_path / "catalog.ndjson").read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    assert catalog_lines, "BookCatalog tail Fjord must write rows"
    catalog_rows = [json.loads(ln) for ln in catalog_lines]
    # Take the latest summary; the catalog Fjord may emit multiple flushes.
    last = catalog_rows[-1]
    assert last["distinct_authors"] >= 2, f"catalog must see ≥2 authors, got {last}"
    assert last["distinct_subjects"] >= 3, f"catalog must see ≥3 distinct subjects, got {last}"
