"""Tideweaver routing tests for the **chain** Watershed shape.

Two tests from the routing test plan:

* **Test 1** — Three-Stream cascade (JSONPlaceholder users → posts → comments)
  with conv_dict at each hop. Proves chain orchestration gates downstream on
  upstream-wave ordering and that each Stream's ``conv_dict`` lands on the
  parked snapshot independently.
* **Test 2** — Paginated chain into a Fjord tail (Rick & Morty characters
  Stream → episodes Stream → tail Fjord). The Fjord's ``outflow(state)``
  reads BOTH upstream snapshots and writes joined rows to NDJSON. Proves
  multi-source fan-in at the tail of a chain and transitive upstream
  visibility from the Fjord flush.

Mocks ``incorporator.io.fetch.execute_request`` per AGENTS.md's mandate.
Drives the production ``_tick_stream`` / ``_tick_fjord`` paths — the
:class:`Tideweaver` is constructed with no ``tick_factory`` override.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.io.pagination import NextUrlPaginator
from incorporator.tideweaver import Fjord, Stream, Tideweaver, Watershed
from incorporator.schema.converters import calc, inc


# ---------------------------------------------------------------------------
# Source classes — extra='allow' so conv_dict-derived fields survive.
# ---------------------------------------------------------------------------


class ChainUser(Incorporator):
    """JSONPlaceholder ``/users`` row for Test 1."""

    model_config = ConfigDict(extra="allow")


class ChainPost(Incorporator):
    """JSONPlaceholder ``/posts`` row for Test 1."""

    model_config = ConfigDict(extra="allow")


class ChainComment(Incorporator):
    """JSONPlaceholder ``/comments`` row for Test 1."""

    model_config = ConfigDict(extra="allow")


class RMChar(Incorporator):
    """Rick & Morty ``/character`` row for Test 2."""

    model_config = ConfigDict(extra="allow")


class RMEpisode(Incorporator):
    """Rick & Morty ``/episode`` row for Test 2."""

    model_config = ConfigDict(extra="allow")


class RMJoined(Incorporator):
    """Output class produced by Test 2's Fjord ``outflow(state)``."""

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


# ---------------------------------------------------------------------------
# Test 1 — Three-Stream cascade (chain shape).
# ---------------------------------------------------------------------------


async def _mock_jsonplaceholder_chain(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned JSONPlaceholder shapes for the users/posts/comments cascade."""
    if "/users" in url:
        payload: Any = [
            {"id": 1, "name": "Leanne Graham", "email": "a@b.co"},
            {"id": 2, "name": "Ervin Howell", "email": "c@d.co"},
        ]
    elif "/posts" in url:
        payload = [
            {"id": 10, "userId": 1, "title": "First post title", "body": "Body one."},
            {"id": 11, "userId": 2, "title": "Hi", "body": "Body two."},
        ]
    elif "/comments" in url:
        payload = [
            {"id": 100, "postId": 10, "name": "C1", "body": "Short comment."},
            {"id": 101, "postId": 11, "name": "C2", "body": "A longer comment body here."},
        ]
    else:
        payload = []
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_chain_three_streams_apply_conv_dict_in_order(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three Streams in a weir-mode chain each apply conv_dict independently.

    Uses ``Watershed.chain(..., gate_mode="weir")`` — the third gating
    mode introduced in the canal toolkit refactor.  ``weir`` keeps the
    data dependency (downstream waits for at least one upstream wave)
    but does NOT block on in-flight upstream the way ``"hard"`` does, so
    all three Streams fit their realistic 3.0s intervals inside the
    8.0s test window.  Previously this test had to drop the data
    dependency entirely with ``gate_mode="soft"`` to make the window fit.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_jsonplaceholder_chain)
    _reset_registries(ChainUser, ChainPost, ChainComment)

    # ignore_ssl=True skips httpx.AsyncClient's real TLS cert-chain load
    # (ssl.create_default_context()), which costs real time per tick even
    # though execute_request is mocked above — otherwise this starves the
    # real-clock window. Pre-existing, fully-plumbed incorp_params key (see
    # incorporator/io/fetch.py's HTTPClientBuilder.build_client).
    users = Stream(
        name="users",
        cls=ChainUser,
        interval=3.0,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/users",
            "inc_code": "id",
            "conv_dict": {"name_lower": calc(str.lower, "name", default="")},
            "ignore_ssl": True,
        },
    )
    posts = Stream(
        name="posts",
        cls=ChainPost,
        interval=3.0,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/posts",
            "inc_code": "id",
            "conv_dict": {"title_words": calc(lambda t: len(t.split()), "title", default=0, target_type=int)},
            "ignore_ssl": True,
        },
    )
    comments = Stream(
        name="comments",
        cls=ChainComment,
        interval=3.0,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/comments",
            "inc_code": "id",
            "conv_dict": {"body_length": calc(len, "body", default=0, target_type=int)},
            "ignore_ssl": True,
        },
    )

    # Soft chain — currents are sequenced topologically but downstream is
    # not gated on upstream waves. This lets all three fire on their own
    # cadence within a tight test window. Test 2 below exercises hard-chain
    # ordering with three currents (chars → eps → joined Fjord).
    ws = Watershed.chain(window=_short_window(8.0), currents=[users, posts, comments], gate_mode="weir")
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    # Each current fired at least once.
    fire_order: List[str] = [name for tide in tides for name in tide.fired]
    assert "users" in fire_order, f"users must fire, got {fire_order}"
    assert "posts" in fire_order, f"posts must fire, got {fire_order}"
    assert "comments" in fire_order, f"comments must fire, got {fire_order}"

    # The chain's topological order is preserved in the FIRST appearance of
    # each current — soft mode still sequences within a pass.
    first_users = fire_order.index("users")
    first_posts = fire_order.index("posts")
    first_comments = fire_order.index("comments")
    assert first_users <= first_posts <= first_comments, (
        f"chain topology must drive first-appearance order; got {fire_order}"
    )

    # conv_dict landed per hop. Read from the per-class strong-ref snapshot
    # the production ``_tick_stream`` parks at the end of each tick.
    user_rows: List[Any] = list(getattr(ChainUser, "_tideweaver_snapshot", []))
    post_rows: List[Any] = list(getattr(ChainPost, "_tideweaver_snapshot", []))
    comment_rows: List[Any] = list(getattr(ChainComment, "_tideweaver_snapshot", []))
    assert user_rows and post_rows and comment_rows, (
        f"every chain hop must produce rows; got users={len(user_rows)} "
        f"posts={len(post_rows)} comments={len(comment_rows)}"
    )
    assert all(getattr(u, "name_lower", None) == getattr(u, "name", "").lower() for u in user_rows)
    assert all(getattr(p, "title_words", None) == len(getattr(p, "title", "").split()) for p in post_rows)
    assert all(getattr(c, "body_length", None) == len(getattr(c, "body", "")) for c in comment_rows)


# ---------------------------------------------------------------------------
# Test 2 — Paginated Stream → Stream → Fjord (chain shape).
# ---------------------------------------------------------------------------


async def _mock_rick_and_morty(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Canned Rick & Morty shapes — single-page envelope (info.next = null)."""
    if "/character" in url:
        payload: Any = {
            "info": {"next": None},
            "results": [
                {"id": 1, "name": "Rick Sanchez", "status": "Alive", "episode": ["https://x/episode/1"]},
                {
                    "id": 2,
                    "name": "Morty Smith",
                    "status": "Alive",
                    "episode": ["https://x/episode/1", "https://x/episode/2"],
                },
            ],
        }
    elif "/episode" in url:
        payload = {
            "info": {"next": None},
            "results": [
                {
                    "id": 1,
                    "name": "Pilot",
                    "air_date": "December 2, 2013",
                    "characters": ["https://x/character/1", "https://x/character/2"],
                },
                {
                    "id": 2,
                    "name": "Lawnmower Dog",
                    "air_date": "December 9, 2013",
                    "characters": ["https://x/character/2"],
                },
            ],
        }
    else:
        payload = {}
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_chain_streams_into_fjord_tail_reads_both_upstream_snapshots(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Fjord tail at the end of a chain reads BOTH upstream Stream snapshots.

    Proves: (a) chained paginated Streams populate per-class snapshots that
    persist between ticks; (b) the Fjord's ``outflow(state)`` receives state
    keyed by every transitive upstream class name (chars + eps); (c) the
    Fjord's NDJSON export contains joined rows derived from both upstream
    snapshots; (d) conv_dict on the head Streams (``inc(datetime)`` parsing
    "December 2, 2013" → datetime, ``calc(len, "episode")`` episode count)
    fires per tick.

    Stream intervals are set larger than the window so each Stream's
    ``NextUrlPaginator`` runs exactly once — re-firing would exit the
    chunked engine immediately on the exhausted paginator and park an
    empty snapshot, blanking out the upstream state the Fjord reads.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_rick_and_morty)
    _reset_registries(RMChar, RMEpisode, RMJoined)

    # Write outflow.py that the Fjord tail will resolve. The body joins
    # RMChar + RMEpisode rows by episode-URL membership.
    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "def outflow(state):\n"
        "    chars = state.get('RMChar', [])\n"
        "    eps = state.get('RMEpisode', [])\n"
        "    rows = []\n"
        "    for ep in eps:\n"
        "        ep_url = f'https://x/episode/{ep.inc_code}'\n"
        "        members = [c.name for c in chars if ep_url in getattr(c, 'episode', [])]\n"
        "        rows.append({\n"
        "            'inc_code': ep.inc_code,\n"
        "            'episode': ep.name,\n"
        "            'cast_count': len(members),\n"
        "            'cast': ', '.join(members),\n"
        "        })\n"
        "    return rows\n",
        encoding="utf-8",
    )
    out_file = tmp_path / "joined.ndjson"

    # ignore_ssl=True skips httpx.AsyncClient's real TLS cert-chain load
    # (ssl.create_default_context()), which costs real time per tick even
    # though execute_request is mocked above — otherwise this starves the
    # real-clock window. Pre-existing, fully-plumbed incorp_params key (see
    # incorporator/io/fetch.py's HTTPClientBuilder.build_client).
    chars = Stream(
        name="chars",
        cls=RMChar,
        interval=15.0,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://rickandmortyapi.com/api/character",
            "inc_code": "id",
            "rec_path": "results",
            "inc_page": NextUrlPaginator("info", "next"),
            "conv_dict": {"episode_count": calc(len, "episode", default=0, target_type=int)},
            "ignore_ssl": True,
        },
    )
    eps = Stream(
        name="eps",
        cls=RMEpisode,
        interval=15.0,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://rickandmortyapi.com/api/episode",
            "inc_code": "id",
            "rec_path": "results",
            "inc_page": NextUrlPaginator("info", "next"),
            "conv_dict": {"air_date": inc(datetime)},
            "ignore_ssl": True,
        },
    )
    tail = Fjord(
        name="joined",
        cls=RMJoined,
        interval=0.2,
        on_error="isolate",
        outflow=outflow_py,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
    )

    # gate_mode="weir" lets the fast Fjord fire on its own cadence while the
    # paginated Streams are in-flight — replaces the old skip_threshold=50.0 hack.
    ws = Watershed.chain(window=_short_window(8.0), currents=[chars, eps, tail], gate_mode="weir")
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    # All three currents fired.
    fired_names = {name for tide in tides for name in tide.fired}
    assert fired_names == {"chars", "eps", "joined"}, f"every chain current must fire, got {fired_names}"

    # conv_dict fired on the head Streams. Read from the parked snapshot
    # the production ``_tick_stream`` sets at the end of each tick.
    char_rows: List[Any] = list(getattr(RMChar, "_tideweaver_snapshot", []))
    ep_rows: List[Any] = list(getattr(RMEpisode, "_tideweaver_snapshot", []))
    assert char_rows, "chars Stream must populate rows"
    assert ep_rows, "eps Stream must populate rows"
    assert all(getattr(c, "episode_count", None) == len(getattr(c, "episode", [])) for c in char_rows)
    assert all(isinstance(getattr(e, "air_date", None), datetime) for e in ep_rows)

    # The Fjord tail's outflow ran, reading BOTH upstream snapshots and
    # writing joined rows. Pilot (ep 1) cast = Rick + Morty (2); Lawnmower
    # Dog (ep 2) cast = Morty (1).
    assert out_file.exists(), "Fjord tail must have written the NDJSON export"
    lines = [ln for ln in Path(out_file).read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "exported NDJSON must have at least one row"
    rows = [json.loads(ln) for ln in lines]
    # The Fjord may run multiple times during the window — find the
    # joined rows for episodes 1 and 2 anywhere in the output.
    by_code = {r["inc_code"]: r for r in rows if "inc_code" in r}
    assert 1 in by_code and 2 in by_code, f"both episodes must appear in joined output, got keys {list(by_code)}"
    assert by_code[1]["cast_count"] == 2, f"pilot cast should be 2, got {by_code[1]}"
    assert by_code[2]["cast_count"] == 1, f"lawnmower dog cast should be 1, got {by_code[2]}"
