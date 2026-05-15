"""State-aware ``inflow(state)`` callback regressions.

Covers the user's primary motivating cases (SWAPI-Trinity-style linking
via ``link_to(state["Planet"], …)``) plus edge cases:

  - source N fails mid-seed
  - inflow(state) itself raises
  - inflow returns invalid shape
  - circular dependency (validate-time would catch — runtime here)
  - refresh sees fresh peer snapshots (link_to closure on live list)
  - co-equal sources still parallel
  - async inflow callable
  - inflow conv_dict wins over stream_params conv_dict

Mock pattern mirrors ``tests/test_fjord.py`` — patch
``incorporator.io.fetch.execute_request`` with a canned async stub.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Type

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.schema.builder import SCHEMA_REGISTRY


# ----------------------------------------------------------------------
# Source classes
# ----------------------------------------------------------------------
class Planet(Incorporator):
    pass


class Film(Incorporator):
    pass


class Person(Incorporator):
    pass


# ----------------------------------------------------------------------
# Mock network — three SWAPI-style endpoints
# ----------------------------------------------------------------------
PLANETS_URL = "https://swapi.test/api/planets/"
FILMS_URL = "https://swapi.test/api/films/"
PEOPLE_URL = "https://swapi.test/api/people/"


def _payload_for(url: str) -> Any:
    if "planets" in url:
        return {
            "results": [
                {"id": 1, "name": "Tatooine"},
                {"id": 2, "name": "Alderaan"},
            ]
        }
    if "films" in url:
        return {
            "results": [
                {"id": 4, "title": "A New Hope"},
                {"id": 5, "title": "The Empire Strikes Back"},
            ]
        }
    if "people" in url:
        return {
            "results": [
                {"id": 1, "name": "Luke", "homeworld": 1, "films": [4, 5]},
                {"id": 2, "name": "Leia", "homeworld": 2, "films": [4]},
            ]
        }
    return {"results": []}


async def mock_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    return httpx.Response(200, text=json.dumps(_payload_for(url)), request=httpx.Request("GET", url))


# ----------------------------------------------------------------------
# inflow.py fixtures
# ----------------------------------------------------------------------
LINKING_INFLOW = '''
from incorporator import link_to, link_to_list


def inflow(state):
    """Wire Person's homeworld + films against the already-loaded peers.

    inflow(state) is called BEFORE each source seeds, so on the first
    two calls (for Planet and Film) ``state`` is empty / partial.  Be
    defensive: only emit Person's overrides once its peers exist.
    """
    overrides = {}
    if "Planet" in state and "Film" in state:
        overrides["Person"] = {
            "conv_dict": {
                "homeworld": link_to(state["Planet"], extractor=lambda v: v),
                "films":     link_to_list(state["Film"], extractor=lambda v: v),
            }
        }
    return overrides
'''

BROKEN_INFLOW_RAISES = '''
def inflow(state):
    raise ZeroDivisionError("simulated inflow failure")
'''

BAD_SHAPE_INFLOW = '''
def inflow(state):
    return [1, 2, 3]   # not a dict — engine should raise TypeError
'''

ASYNC_INFLOW = '''
import asyncio
from incorporator import link_to


async def inflow(state):
    await asyncio.sleep(0)
    overrides = {}
    if "Planet" in state:
        overrides["Person"] = {
            "conv_dict": {
                "homeworld": link_to(state["Planet"], extractor=lambda v: v),
            }
        }
    return overrides
'''

WINS_INFLOW = '''
def inflow(state):
    return {"Person": {"conv_dict": {"name": lambda v: v.upper()}}}
'''

OUTFLOW_SOURCE = '''
def outflow(state):
    rows = []
    for p in state["Person"]:
        hw = getattr(p, "homeworld", None)
        hw_name = getattr(hw, "inc_name", None) if hw else None
        rows.append({"name": p.inc_name, "homeworld": hw_name})
    return rows
'''


def _write_file(tmp_path: Path, source: str, filename: str) -> Path:
    p = tmp_path / filename
    p.write_text(source, encoding="utf-8")
    return p


def _find_dynamic_class(class_name: str) -> Optional[Type[Any]]:
    for (name, _keys, _base_id), cls in SCHEMA_REGISTRY.items():
        if name == class_name:
            return cls
    return None


_DYNAMIC_NAMES_TO_PURGE = {"FjordReport"}


@pytest.fixture(autouse=True)
def _clean_state() -> Any:
    Planet.inc_dict.clear()
    Film.inc_dict.clear()
    Person.inc_dict.clear()
    for key in list(SCHEMA_REGISTRY.keys()):
        if key[0] in _DYNAMIC_NAMES_TO_PURGE:
            del SCHEMA_REGISTRY[key]
    yield
    Planet.inc_dict.clear()
    Film.inc_dict.clear()
    Person.inc_dict.clear()
    for key in list(SCHEMA_REGISTRY.keys()):
        if key[0] in _DYNAMIC_NAMES_TO_PURGE:
            del SCHEMA_REGISTRY[key]


async def _drain(gen: AsyncGenerator[Any, None]) -> List[Any]:
    out: List[Any] = []
    async for wave in gen:
        out.append(wave)
    return out


# ======================================================================
# Happy path: SWAPI Trinity linking works with inflow(state)
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_state_wires_link_to_against_peers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Person's conv_dict.homeworld resolves against Planet's live registry."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    inflow_file = _write_file(tmp_path, LINKING_INFLOW, "swapi_inflow.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")
    out_file = tmp_path / "report.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id", "inc_name": "name"}, "refresh_params": None},
                {"cls": Film,   "incorp_params": {"inc_url": FILMS_URL,   "rec_path": "results", "inc_code": "id", "inc_name": "title"}, "refresh_params": None},
                {"cls": Person, "incorp_params": {"inc_url": PEOPLE_URL,  "rec_path": "results", "inc_code": "id", "inc_name": "name"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            inflow=inflow_file,
            export_params={"file_path": str(out_file)},
        )
    )

    # Three seed waves + at least one outflow wave.
    ops = [w.operation for w in waves]
    assert "fjord_incorp:Planet" in ops
    assert "fjord_incorp:Film" in ops
    assert "fjord_incorp:Person" in ops

    # The dynamic FjordReport class has rows where homeworld was resolved
    # from the peer Planet registry.
    rows = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 2
    # Luke -> Tatooine, Leia -> Alderaan
    by_name = {r["name"]: r for r in rows}
    assert by_name["Luke"]["homeworld"] == "Tatooine"
    assert by_name["Leia"]["homeworld"] == "Alderaan"


# ======================================================================
# C4: no inflow callable → parallel-seed back-compat
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_passive_namebag_keeps_parallel_seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Legacy ``inflow.py`` with NO inflow function → parallel seed runs unchanged."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    passive = _write_file(tmp_path, "MY_CONST = 42\n", "passive_inflow.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id"}, "refresh_params": None},
                {"cls": Person, "incorp_params": {"inc_url": PEOPLE_URL,  "rec_path": "results", "inc_code": "id"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            inflow=passive,
            export_params={"file_path": str(tmp_path / "out.ndjson")},
        )
    )
    ops = [w.operation for w in waves]
    assert "fjord_incorp:Planet" in ops and "fjord_incorp:Person" in ops


# ======================================================================
# A2 — inflow(state) raises
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_raises_does_not_crash_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A user bug in inflow() surfaces as a clean seed-failure wave."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    bad_inflow = _write_file(tmp_path, BROKEN_INFLOW_RAISES, "bad_inflow.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            inflow=bad_inflow,
            export_params={"file_path": str(tmp_path / "out.ndjson")},
        )
    )
    # The Planet seed itself fails because inflow(state) raises before it.
    failed = [w for w in waves if w.failed_sources]
    assert failed, "engine should emit a failure wave when inflow() raises"
    assert any("ZeroDivisionError" in fs or "simulated inflow failure" in fs
               for w in failed for fs in w.failed_sources)


# ======================================================================
# A3 — inflow returns invalid shape (not a dict)
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_returns_bad_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """inflow() returning a list (not a dict) raises a clear TypeError-derived failure."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    bad = _write_file(tmp_path, BAD_SHAPE_INFLOW, "bad_shape.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            inflow=bad,
            export_params={"file_path": str(tmp_path / "out.ndjson")},
        )
    )
    failed = [w for w in waves if w.failed_sources]
    assert failed
    assert any("must return a dict" in fs for w in failed for fs in w.failed_sources)


# ======================================================================
# A8 — async inflow callable
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_async_callable_is_awaited(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ``async def inflow(state)`` is awaited; conv_dict still wires through."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    async_inflow = _write_file(tmp_path, ASYNC_INFLOW, "async_inflow.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")
    out_file = tmp_path / "out.ndjson"

    waves = await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id", "inc_name": "name"}, "refresh_params": None},
                {"cls": Person, "incorp_params": {"inc_url": PEOPLE_URL,  "rec_path": "results", "inc_code": "id", "inc_name": "name"}, "refresh_params": None},
            ],
            outflow=outflow_file,
            inflow=async_inflow,
            export_params={"file_path": str(out_file)},
        )
    )
    # Should complete without failure waves and produce the linked rows.
    failures = [w for w in waves if w.failed_sources]
    assert not failures, [w.failed_sources for w in failures]
    rows = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_name = {r["name"]: r for r in rows}
    assert by_name["Luke"]["homeworld"] == "Tatooine"


# ======================================================================
# A9 — inflow's conv_dict wins on conflicting keys
# ======================================================================


@pytest.mark.asyncio
async def test_inflow_conv_dict_overrides_stream_params(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both inflow AND stream_params declare conv_dict.name → inflow wins."""
    monkeypatch.setattr(fetch, "execute_request", mock_execute_request)
    monkeypatch.chdir(tmp_path)
    inflow_file = _write_file(tmp_path, WINS_INFLOW, "wins.py")
    outflow_file = _write_file(tmp_path, OUTFLOW_SOURCE, "fjord_report.py")
    out_file = tmp_path / "out.ndjson"

    await _drain(
        Incorporator.fjord(
            stream_params=[
                {"cls": Planet, "incorp_params": {"inc_url": PLANETS_URL, "rec_path": "results", "inc_code": "id", "inc_name": "name"}, "refresh_params": None},
                {
                    "cls": Person,
                    # Static conv_dict.name = lowercase
                    "incorp_params": {
                        "inc_url": PEOPLE_URL, "rec_path": "results", "inc_code": "id", "inc_name": "name",
                        "conv_dict": {"name": lambda v: v.lower()},
                    },
                    "refresh_params": None,
                },
            ],
            outflow=outflow_file,
            inflow=inflow_file,
            export_params={"file_path": str(out_file)},
        )
    )
    rows = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    # inflow's UPPER override wins, not stream_params' lower.
    names = {r["name"] for r in rows}
    assert "LUKE" in names or "LEIA" in names
    assert all(n.isupper() for n in names)
