"""Reducers and classes for the CLI form (``watershed.json``).

Same two-phase drill as ``pokeapi_etl_calc.py``: a shallow ``nav`` current
discovers 150 HATEOAS URLs, a ``pokemon`` current (``Stream(parent_current=
"nav")``) drills each one for real ``stats``/``types``.  ``calculate_bst`` /
``format_typing`` are kept in sync with ``pokeapi_etl_calc.py`` verbatim
(the two files can't import each other without an import-order risk, so this
is a deliberate copy, not DRY).

``incorporator tideweaver run watershed.json`` imports this module at
config-load time (unlike the Python entry, which is never imported by the
CLI path), so the host-throttle registration below is the ONLY thing that
paces the 150 concurrent detail requests on the CLI path.

``nav`` fetches ``?limit=150&offset=0`` in one call rather than 3 paginated
``?limit=50`` pages (the Python entry's approach). A Tideweaver ``Stream``
current with no ``parent_current`` always runs through ``cls.stream()``
(chunking mode), whose omitted-``refresh_params`` default silently retries
each chunk's rows via ``cls.refresh()`` -- harmless for a single in-place
upsert (hence ``inc_code="name"`` in ``watershed.json``, so the implicit
refresh re-fetch updates existing rows instead of re-inserting under new
auto-increment keys), but fatal for a *paginated* chunk: rows extracted via
``rec_path`` carry no per-instance origin URL, so ``refresh()`` raises and
the chunked engine aborts pagination after page 1. PokeAPI's list endpoint
accepts an arbitrary ``limit``, so fetching all 150 in one page sidesteps
the bug entirely instead of tripping it. See this appendix's README
"Suspected framework gap" note.
"""

from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, register_host_penstock

# Pace pokeapi.co at 1.5 req/sec (90/min -- under the documented 100/min
# ceiling).  MUST live here, not just in the Python entry -- this module is
# what the CLI path actually imports before firing 150 concurrent detail
# requests.
register_host_penstock("pokeapi.co", rate_per_sec=1.5)

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time, a few seconds before the config's window is parsed --
# see incorporator/tideweaver/config.py's build_watershed ordering.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)


class Nav(Incorporator):
    """Shallow discovery registry -- name + HATEOAS url, populated by the "nav" current."""


class Pokemon(Incorporator):
    """Enriched detail registry -- populated by the "pokemon" Stream(parent_current="nav") drill."""


def calculate_bst(stats_array):
    """Same reducer from the Python example, exposed for the CLI."""
    if not isinstance(stats_array, list):
        return 0
    return sum(s.get("base_stat", 0) for s in stats_array if isinstance(s, dict))


def format_typing(types_array):
    """Same reducer from the Python example, exposed for the CLI."""
    if not isinstance(types_array, list):
        return "Unknown"
    type_names = [t.get("type", {}).get("name", "").capitalize() for t in types_array if isinstance(t, dict)]
    return " / ".join(type_names)
