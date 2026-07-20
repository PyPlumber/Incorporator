"""Pinning test for ``examples/appendix/pokeapi-etl/pokeapi_etl_calc.py`` (T-Tutorial).

Locks the CURRENT observable behavior of the appendix's plain ``incorp()``
T5 drill (``main()``'s shallow-discovery ``Nav.incorp`` + deep-drill
``Pokemon.incorp``) ahead of an upcoming framework refactor program. No
Watershed involved — the pinned path is ``main()``'s two linear ``incorp()``
calls, mirroring ``tests/public/api/test_pokemon_etl.py``'s structure but
importing the REAL ``Nav``/``Pokemon``/``calculate_bst``/``format_typing``
objects from the appendix module (via ``load_sidecar``) instead of
redefining local copies — that's what distinguishes this pin from the
sibling tutorial pin.

``main()`` itself never returns ``pokemon_nav``/``enriched_pokemon`` (it only
prints a table), so this test calls ``Nav.incorp``/``Pokemon.incorp``
directly with the exact kwargs ``main()`` uses, to capture the return values
needed for assertions — not a redesign, just direct invocation of the same
calls the entry makes inline.

Mock intentionally pins a 3-Pokemon subset (bulbasaur/ivysaur/mewtwo) via
``NextUrlPaginator``'s ``"next"`` field (page 1 -> 2 results + non-null
``next``, page 2 -> 1 result + ``next: null``) — a test-scoping choice, not a
fidelity gap; the real entry's ``call_lim=3`` would drill up to 150 Pokemon
against the live API.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import httpx
import pytest

from incorporator.io import fetch
from incorporator.io.pagination import NextUrlPaginator
from incorporator.io.penstock import _HOST_PENSTOCKS
from incorporator.schema.converters import calc
from tests.helpers import load_sidecar

_HERE = Path(__file__).resolve()
_EXAMPLE_DIR = _HERE.parents[3] / "examples" / "appendix" / "pokeapi-etl"

BASE_URL = "https://pokeapi.co/api/v2"


@pytest.fixture(autouse=True, scope="module")
def _restore_host_penstock_registry() -> Iterator[None]:
    """Snapshot/restore the process-global penstock registry around this module.

    Must run (and snapshot) BEFORE ``pokeapi_etl_calc.py`` is loaded — see
    ``_pokeapi_calc``'s docstring for why the load is deferred into its own
    fixture instead of happening at this test module's import time. Mutates
    ``_HOST_PENSTOCKS`` in place; never reassigns — every importer, including
    ``resolve_penstock``, holds a direct reference to this exact dict object.
    """
    snapshot = dict(_HOST_PENSTOCKS)
    yield
    _HOST_PENSTOCKS.clear()
    _HOST_PENSTOCKS.update(snapshot)


@pytest.fixture(scope="module")
def _pokeapi_calc(_restore_host_penstock_registry: None) -> ModuleType:
    """Load ``pokeapi_etl_calc.py`` after the penstock-registry snapshot above.

    ``pokeapi_etl_calc.py`` calls ``register_host_penstock("pokeapi.co", ...)``
    as a module-level import side effect. pytest imports every test module
    during its collection phase, before any fixture runs — a module-level
    ``load_sidecar(...)`` call at this test file's top would therefore fire
    that side effect BEFORE ``_restore_host_penstock_registry`` gets a chance
    to snapshot a clean baseline, permanently leaking ``"pokeapi.co"`` into
    later-running test modules (e.g. ``test_penstock_registry.py``'s "a fresh
    process has an empty registry" assertion — precedent commit ``c188fbd``).
    Deferring the load into this fixture (declared to depend on the snapshot
    fixture above) ensures the registration happens during the RUN phase,
    after the snapshot is already taken.
    """
    return load_sidecar(_EXAMPLE_DIR / "pokeapi_etl_calc.py", "pokeapi_etl_calc_appendix")


async def _mock_pokeapi(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Return canned PokeAPI responses: 2-page shallow discovery + 3 detail drills."""
    if "offset=0" in url:
        payload: Any = {
            "next": f"{BASE_URL}/pokemon/?limit=50&offset=50",
            "results": [
                {"name": "bulbasaur", "url": f"{BASE_URL}/pokemon/1/"},
                {"name": "ivysaur", "url": f"{BASE_URL}/pokemon/2/"},
            ],
        }
    elif "offset=50" in url:
        payload = {
            "next": None,
            "results": [{"name": "mewtwo", "url": f"{BASE_URL}/pokemon/150/"}],
        }
    elif "/pokemon/1/" in url:
        payload = {
            "id": 1,
            "name": "bulbasaur",
            "weight": 69,
            "types": [{"type": {"name": "grass"}}, {"type": {"name": "poison"}}],
            "stats": [
                {"base_stat": 45, "stat": {"name": "hp"}},
                {"base_stat": 49, "stat": {"name": "attack"}},
            ],  # BST = 94
        }
    elif "/pokemon/2/" in url:
        payload = {
            "id": 2,
            "name": "ivysaur",
            "weight": 130,
            "types": [{"type": {"name": "grass"}}, {"type": {"name": "poison"}}],
            "stats": [
                {"base_stat": 60, "stat": {"name": "hp"}},
                {"base_stat": 62, "stat": {"name": "attack"}},
            ],  # BST = 122
        }
    elif "/pokemon/150/" in url:
        payload = {
            "id": 150,
            "name": "mewtwo",
            "weight": 1220,
            "types": [{"type": {"name": "psychic"}}],
            "stats": [
                {"base_stat": 106, "stat": {"name": "hp"}},
                {"base_stat": 110, "stat": {"name": "attack"}},
                {"base_stat": 154, "stat": {"name": "special-attack"}},
            ],  # BST = 370
        }
    else:
        payload = {}

    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


def _reset_all(nav_cls: type, pokemon_cls: type) -> None:
    """Wipe per-class inc_dict to prevent test cross-contamination."""
    for cls in (nav_cls, pokemon_cls):
        cls.inc_dict.clear()


@pytest.mark.asyncio
async def test_pokeapi_appendix_shallow_discovery_and_deep_enrichment(
    monkeypatch: pytest.MonkeyPatch, _pokeapi_calc: ModuleType
) -> None:
    """Pins pokeapi-etl's shallow-discovery + inc_parent deep-drill + calc reducers.

    Proves:
    - Shallow discovery (``Nav.incorp`` + ``NextUrlPaginator``) yields 3 rows
      across 2 pages.
    - Deep enrichment (``Pokemon.incorp(inc_parent=...)``) yields 3 rows.
    - The real ``calculate_bst``/``format_typing`` reducers, invoked via the
      real appendix ``conv_dict``, compute the expected base_stat_total/types
      for all 3 Pokemon.
    """
    Nav = _pokeapi_calc.Nav
    Pokemon = _pokeapi_calc.Pokemon
    calculate_bst = _pokeapi_calc.calculate_bst
    format_typing = _pokeapi_calc.format_typing

    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_pokeapi)
    _reset_all(Nav, Pokemon)

    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        inc_child="url",
        inc_page=NextUrlPaginator("next"),
        call_lim=3,
        requests_per_second=1.5,
    )
    assert len(pokemon_nav) == 3

    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"],
        conv_dict={
            "stats": calc(calculate_bst, "stats", default=0, target_type=int),
            "types": calc(format_typing, "types", default="Unknown", target_type=str),
        },
        name_chg=[("stats", "base_stat_total")],
        requests_per_second=1.5,
    )
    assert len(enriched_pokemon) == 3

    bulbasaur = Pokemon.inc_dict.get(1)
    ivysaur = Pokemon.inc_dict.get(2)
    mewtwo = Pokemon.inc_dict.get(150)

    assert bulbasaur is not None and bulbasaur.base_stat_total == 94
    assert ivysaur is not None and ivysaur.base_stat_total == 122
    assert mewtwo is not None and mewtwo.base_stat_total == 370
    assert mewtwo.types == "Psychic"
    assert bulbasaur.types == "Grass / Poison"
    assert ivysaur.types == "Grass / Poison"
