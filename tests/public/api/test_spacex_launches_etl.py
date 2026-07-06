"""Mocked smoke test for Tutorial 6's build-time link_to join (spacex_launches.py).

Proves the T5 parent-child drill still reads raw FK strings off `Launch`
(the child fan-out URL-templates the raw id, not a resolved instance), and
that the post-drill `link_to` re-coercion pass leaves `launch.rocket` /
`launch.launchpad` as the actual joined instances for every downstream reader.
"""

import json
from typing import Any

import httpx
import pytest

from incorporator import Incorporator, link_to
from incorporator.io import fetch


class Launch(Incorporator):
    pass


class Rocket(Incorporator):
    pass


class Pad(Incorporator):
    pass


LAUNCHES = [
    {"id": "L1", "name": "USSF-44", "rocket": "R1", "launchpad": "P1"},
    {"id": "L2", "name": "Starlink 4-36 (v1.5)", "rocket": "R2", "launchpad": "P2"},
    {"id": "L3", "name": "SWOT", "rocket": "R2", "launchpad": "P-missing"},
]

ROCKETS = [
    {"id": "R1", "name": "Falcon Heavy"},
    {"id": "R2", "name": "Falcon 9"},
]

PADS = [
    {"id": "P1", "name": "KSC LC 39A", "region": "Florida", "launch_successes": 55, "launch_attempts": 55},
    {"id": "P2", "name": "CCSFS SLC 40", "region": "Florida", "launch_successes": 97, "launch_attempts": 99},
]


async def mock_spacex_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Serves launches/upcoming, then per-id rocket/launchpad drills."""
    req = httpx.Request("GET", url)
    if "launches/upcoming" in url:
        return httpx.Response(200, text=json.dumps(LAUNCHES), request=req)
    if "/rockets/" in url:
        rid = url.rsplit("/", 1)[-1]
        match = [r for r in ROCKETS if r["id"] == rid]
        return httpx.Response(200, text=json.dumps(match), request=req)
    if "/launchpads/" in url:
        pid = url.rsplit("/", 1)[-1]
        match = [p for p in PADS if p["id"] == pid]
        return httpx.Response(200, text=json.dumps(match), request=req)
    return httpx.Response(200, text="[]", request=req)


@pytest.mark.asyncio
async def test_link_to_join_is_build_time_and_drill_still_gets_raw_fk(monkeypatch: pytest.MonkeyPatch) -> None:
    """The T5 drill must fan out on raw FK strings; link_to then resolves in place."""
    monkeypatch.setattr(fetch, "execute_request", mock_spacex_execute_request)

    launches = await Launch.incorp(
        inc_url="https://api.spacexdata.com/v4/launches/upcoming",
        inc_code="id",
        inc_name="name",
    )
    assert len(launches) == 3
    # Pre-join: the drill needs these as raw strings, not Rocket/Pad instances.
    assert isinstance(launches[0].rocket, str)
    assert isinstance(launches[0].launchpad, str)

    rockets = await Rocket.incorp(
        inc_url="https://api.spacexdata.com/v4/rockets/{}",
        inc_parent=launches,
        inc_child="rocket",
        inc_code="id",
    )
    pads = await Pad.incorp(
        inc_url="https://api.spacexdata.com/v4/launchpads/{}",
        inc_parent=launches,
        inc_child="launchpad",
        inc_code="id",
    )
    assert len(rockets) == 2  # deduped: 3 launches -> 2 unique rockets
    assert len(pads) == 2  # P-missing's drill returns an empty payload and is dropped

    resolve_rocket = link_to(rockets)
    resolve_pad = link_to(pads)
    for launch in launches:
        launch.rocket = resolve_rocket(launch.rocket)
        launch.launchpad = resolve_pad(launch.launchpad)

    l1 = launches.inc_dict["L1"]
    assert l1.rocket.name == "Falcon Heavy"
    assert l1.launchpad.name == "KSC LC 39A"
    assert l1.launchpad.region == "Florida"
    assert l1.launchpad.launch_successes == 55

    l2 = launches.inc_dict["L2"]
    assert l2.rocket.name == "Falcon 9"
    assert l2.launchpad.name == "CCSFS SLC 40"

    # Honest display-time boundary: a launch referencing an unfetched pad
    # resolves to None rather than raising — the read-time None-guard's job.
    l3 = launches.inc_dict["L3"]
    assert l3.rocket.name == "Falcon 9"
    assert l3.launchpad is None
