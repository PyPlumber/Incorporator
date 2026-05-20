"""Tideweaver routing tests for the **parallel** Watershed shape.

Two tests from the routing test plan:

* **Test 7** — Four independent parallel Streams (JSONPlaceholder) with a
  different ``conv_dict`` on each. Proves parallel mode runs all schedules
  concurrently without inter-current gating and that each Stream's
  ``conv_dict`` is applied per tick.
* **Test 8** — Parallel with **isolate-on-error**: one branch's mock raises;
  siblings keep firing and still apply their ``conv_dict``. Proves
  per-Current error isolation under routing.

Mocks ``incorporator.io.fetch.execute_request`` per AGENTS.md's mandate
(signature ``async def mock_fn(url, *args, **kwargs) -> httpx.Response``).
Drives the production ``_tick_stream`` path — the :class:`Tideweaver` is
constructed with no ``tick_factory`` override.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.observability.tideweaver import Stream, Tideweaver, Watershed
from incorporator.schema.converters import calc, inc


# ---------------------------------------------------------------------------
# Source classes — one per JSONPlaceholder endpoint.
#
# ``extra='allow'`` opts these classes out of Pydantic V2's default
# ``extra='ignore'`` so conv_dict-derived fields (``title_length``, etc.)
# survive instance construction instead of being silently dropped — see
# AGENTS.md "bare-class data-loss warning".
# ---------------------------------------------------------------------------


class Post(Incorporator):
    """JSONPlaceholder ``/posts`` row."""

    model_config = ConfigDict(extra="allow")


class Comment(Incorporator):
    """JSONPlaceholder ``/comments`` row."""

    model_config = ConfigDict(extra="allow")


class Album(Incorporator):
    """JSONPlaceholder ``/albums`` row."""

    model_config = ConfigDict(extra="allow")


class Photo(Incorporator):
    """JSONPlaceholder ``/photos`` row."""

    model_config = ConfigDict(extra="allow")


class User(Incorporator):
    """JSONPlaceholder ``/users`` row — used only in Test 8's failing branch."""

    model_config = ConfigDict(extra="allow")


def _short_window(seconds: float) -> Tuple[datetime, datetime]:
    """Build a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests so assertions are clean."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


# ---------------------------------------------------------------------------
# Mock — JSONPlaceholder canned JSON.
# ---------------------------------------------------------------------------


async def _mock_jsonplaceholder(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Return canned JSONPlaceholder-shaped payloads keyed by URL substring."""
    if "/posts" in url:
        payload: Any = [
            {"id": 1, "userId": 1, "title": "First Post", "body": "Hello world body content."},
            {"id": 2, "userId": 1, "title": "Second", "body": "Another body."},
        ]
    elif "/comments" in url:
        payload = [
            {"id": 10, "postId": 1, "name": "C1", "email": "a@b.co", "body": "Short."},
            {"id": 11, "postId": 1, "name": "C2", "email": "c@d.co", "body": "Longer comment body text."},
        ]
    elif "/albums" in url:
        payload = [
            {"id": 100, "userId": 1, "title": "vacation 2024"},
            {"id": 101, "userId": 2, "title": "wedding album"},
        ]
    elif "/photos" in url:
        payload = [
            {"id": 1000, "albumId": 100, "title": "BEACH SUNSET", "url": "https://x/p1.jpg"},
            {"id": 1001, "albumId": 100, "title": "OCEAN VIEW", "url": "https://x/p2.jpg"},
        ]
    else:
        payload = []
    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# ---------------------------------------------------------------------------
# Test 7 — Four independent parallel Streams.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parallel_four_independent_streams_apply_conv_dict(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Four parallel Streams each apply their own conv_dict per tick.

    Proves: (a) Watershed.parallel orchestrates four currents concurrently
    with zero edges and zero dependency gating; (b) each Stream's
    conv_dict-derived field lands on the populated registry and the parked
    ``_tideweaver_snapshot``; (c) different converter flavors (``calc``
    with different funcs, ``inc(int)``) all coexist in the same parallel
    watershed.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_jsonplaceholder)
    _reset_registries(Post, Comment, Album, Photo)

    posts = Stream(
        name="posts",
        cls=Post,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/posts",
            "inc_code": "id",
            "conv_dict": {
                "title_length": calc(lambda t: len(t) if t else 0, "title", default=0, target_type=int),
            },
        },
    )
    comments = Stream(
        name="comments",
        cls=Comment,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/comments",
            "inc_code": "id",
            "conv_dict": {
                "body_length": calc(lambda b: len(b) if b else 0, "body", default=0, target_type=int),
            },
        },
    )
    albums = Stream(
        name="albums",
        cls=Album,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/albums",
            "inc_code": "id",
            "conv_dict": {
                "title_upper": calc(lambda t: t.upper() if t else "", "title", default="", target_type=str),
            },
        },
    )
    photos = Stream(
        name="photos",
        cls=Photo,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/photos",
            "inc_code": "id",
            "conv_dict": {
                "album_id_int": inc(int, default=0),
            },
        },
    )

    ws = Watershed.parallel(
        window=_short_window(4.0),
        currents=[posts, comments, albums, photos],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    # Each of the four currents fired at least once.
    all_fired = {name for tide in tides for name in tide.fired}
    assert all_fired == {"posts", "comments", "albums", "photos"}, (
        f"every parallel current must fire at least once, got {all_fired}"
    )

    # Multiple passes happened — proves parallel mode actually drives the
    # scheduler forward (not just one fire and done).
    assert len(tides) >= 2, f"parallel mode should yield multiple tides, got {len(tides)}"

    # The snapshot parked by the production ``_tick_stream`` contains
    # conv_dict-derived fields on every row.
    post_snap: List[Any] = list(Post._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert post_snap, "Post snapshot is empty"
    post_pairs = [(getattr(p, "title", None), getattr(p, "title_length", None)) for p in post_snap]
    assert all(tl == len(t or "") for t, tl in post_pairs), f"Post conv_dict not applied: {post_pairs}"

    comment_snap: List[Any] = list(Comment._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert comment_snap, "Comment snapshot is empty"
    assert all(getattr(c, "body_length", None) == len(getattr(c, "body", "")) for c in comment_snap)

    album_snap: List[Any] = list(Album._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert album_snap, "Album snapshot is empty"
    assert all(getattr(a, "title_upper", "") == getattr(a, "title", "").upper() for a in album_snap)

    photo_snap: List[Any] = list(Photo._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert photo_snap, "Photo snapshot is empty"
    assert all(isinstance(getattr(p, "album_id_int", None), int) for p in photo_snap)


# ---------------------------------------------------------------------------
# Test 8 — Parallel with isolate-on-error.
# ---------------------------------------------------------------------------


async def _mock_jsonplaceholder_with_one_500(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Like ``_mock_jsonplaceholder`` but ``/users`` raises HTTP 500.

    Other endpoints return clean JSON so their Streams populate normally —
    that is what the test asserts.
    """
    if "/users" in url:
        req = httpx.Request("GET", url)
        return httpx.Response(500, text="internal server error", request=req)
    return await _mock_jsonplaceholder(url, *args, **kwargs)


@pytest.mark.asyncio
async def test_parallel_isolate_on_error_keeps_siblings_firing(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One failing parallel branch does not stop sibling Streams from firing.

    Proves: (a) ``on_error="isolate"`` traps an upstream HTTP failure without
    cancelling the watershed; (b) the surviving siblings continue ticking
    and applying their ``conv_dict``; (c) the failing branch's HTTP error
    produces no rows in its ``inc_dict`` while siblings populate normally.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_jsonplaceholder_with_one_500)
    _reset_registries(Post, Album, User)

    posts = Stream(
        name="posts",
        cls=Post,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/posts",
            "inc_code": "id",
            "conv_dict": {"title_length": calc(lambda t: len(t) if t else 0, "title", default=0, target_type=int)},
        },
    )
    albums = Stream(
        name="albums",
        cls=Album,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/albums",
            "inc_code": "id",
            "conv_dict": {"title_upper": calc(lambda t: t.upper() if t else "", "title", default="", target_type=str)},
        },
    )
    users = Stream(
        name="users",
        cls=User,
        interval=0.2,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://jsonplaceholder.typicode.com/users",
            "inc_code": "id",
            "conv_dict": {"name_lower": calc(lambda n: n.lower() if n else "", "name", default="")},
        },
    )

    ws = Watershed.parallel(
        window=_short_window(4.0),
        currents=[posts, albums, users],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    fired_counts = {
        "posts": sum(1 for t in tides for n in t.fired if n == "posts"),
        "albums": sum(1 for t in tides for n in t.fired if n == "albums"),
        "users": sum(1 for t in tides for n in t.fired if n == "users"),
    }

    # Healthy siblings keep firing across multiple passes.
    assert fired_counts["posts"] >= 1, f"healthy 'posts' must fire, got {fired_counts}"
    assert fired_counts["albums"] >= 1, f"healthy 'albums' must fire, got {fired_counts}"
    assert fired_counts["users"] >= 1, f"failing 'users' must attempt at least once, got {fired_counts}"

    # conv_dict on the healthy branches landed on their snapshots.
    post_snap: List[Any] = list(Post._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert post_snap, "Post snapshot must survive even after a sibling failed"
    assert all(getattr(p, "title_length", None) == len(getattr(p, "title", "")) for p in post_snap)

    album_snap: List[Any] = list(Album._tideweaver_snapshot)  # type: ignore[attr-defined]
    assert album_snap, "Album snapshot must survive even after a sibling failed"
    assert all(getattr(a, "title_upper", "") == getattr(a, "title", "").upper() for a in album_snap)

    # The failing User class produced no instances (the 500 short-circuited
    # parsing) — proves the failure path didn't leak rows.
    user_rows = list(User.inc_dict.values())
    assert not user_rows, f"failing branch must not populate any User rows, got {user_rows}"
