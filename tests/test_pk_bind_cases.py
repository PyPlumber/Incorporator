"""Regression tests for PK-bind dispatch order (Chain 3 fix).

Both cases failed silently before Chain 3: PK-binding ran inside the
conv_dict pass (pass 2), BEFORE ``name_chg`` (pass 3), so the source
field had already been renamed away (Case A) or the target field did not
yet exist (Case B) when PK-bind tried to resolve it.

Chain 3 fixes this by running PK-bind as pass 4 — after the Nm pass —
combined with ``_normalize_etl_kwargs``'s Case A source rewrite at
config time.
"""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch


@pytest.mark.asyncio
async def test_pk_bind_case_a_rename_source_away(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Case A: rename source field away.

    Pre-Chain 3 this failed silently because PK-bind ran before name_chg;
    Chain 3 fixes it via _normalize_etl_kwargs's Case A rewrite + post-Nm
    Pk pass.

    Proves that when ``inc_code`` names a field that ``name_chg`` renames
    away, the PK-bind source is rewritten at config time to follow the field
    to its new name and ``inc_code`` on each instance is non-None and NOT
    the auto-counter fallback.
    """
    monkeypatch.chdir(tmp_path)

    class TeamA(Incorporator):
        pass

    payload = [{"teamid": "BOS", "team_name": "Red Sox"}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    result = await TeamA.incorp(
        inc_url="https://stats.example.com/teams",
        inc_code="teamid",
        name_chg=[("teamid", "tid")],
    )

    # Auto-unwrap: single-element list → single instance.
    assert not isinstance(result, list)
    assert result.inc_code is not None, "inc_code must not be None after Case A rename"
    assert not str(result.inc_code).isdigit(), (
        f"inc_code must not be the auto-counter fallback; got {result.inc_code!r}"
    )
    assert result.inc_code == "BOS", f"inc_code must equal the source field value 'BOS'; got {result.inc_code!r}"


@pytest.mark.asyncio
async def test_pk_rewrite_through_nested_name_chg(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Stage 3: Pk.source is rewritten through name_chg even when both sides are dotted paths.

    Mirrors Case A but with nested paths: ``inc_code="user.email"`` and
    ``name_chg=[("user.email", "contact.email")]`` — Pk.source is rewritten
    to ``"contact.email"`` at normalization time so PK-bind runs after Nm
    and finds the value at its post-rename location.
    """
    monkeypatch.chdir(tmp_path)

    class UserNested(Incorporator):
        pass

    payload = [{"user": {"email": "alice@example.com"}, "name": "Alice"}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    result = await UserNested.incorp(
        inc_url="https://users.example.com/list",
        inc_code="user.email",
        name_chg=[("user.email", "contact.email")],
    )

    # Auto-unwrap: single-element list → single instance.
    assert not isinstance(result, list)
    assert result.inc_code is not None, "inc_code must not be None after nested-path rename"
    assert not str(result.inc_code).isdigit(), (
        f"inc_code must not be the auto-counter fallback; got {result.inc_code!r}"
    )
    assert result.inc_code == "alice@example.com", (
        f"inc_code must equal the email value; got {result.inc_code!r}"
    )


@pytest.mark.asyncio
async def test_pk_bind_case_b_rename_creates_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Case B: rename CREATES the PK-bind target.

    Pre-Chain 3 this failed silently because PK-bind ran before name_chg
    (target field didn't exist yet); Chain 3 fixes it by running Pk LAST
    after name_chg.

    Proves that when ``inc_code`` names a field that does not exist in the
    raw payload but IS created by a ``name_chg`` rename, the PK-bind
    resolves to the correct value and ``inc_code`` is non-None and NOT
    the auto-counter fallback.
    """
    monkeypatch.chdir(tmp_path)

    class UserB(Incorporator):
        pass

    payload = [{"user_id": "abc123", "name": "Alice"}]

    async def mock_fn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    monkeypatch.setattr(fetch, "execute_request", mock_fn)

    result = await UserB.incorp(
        inc_url="https://users.example.com/list",
        inc_code="id",
        name_chg=[("user_id", "id")],
    )

    # Auto-unwrap: single-element list → single instance.
    assert not isinstance(result, list)
    assert result.inc_code is not None, "inc_code must not be None after Case B rename"
    assert not str(result.inc_code).isdigit(), (
        f"inc_code must not be the auto-counter fallback; got {result.inc_code!r}"
    )
    assert result.inc_code == "abc123", (
        f"inc_code must equal 'abc123' (the value renamed into the target); got {result.inc_code!r}"
    )
