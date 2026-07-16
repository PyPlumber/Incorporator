"""Reducers and classes for the CLI form (``watershed.json``).

Same two-phase drill as ``pokeapi_etl_calc.py``: a shallow ``nav`` current
discovers 150 HATEOAS URLs, a ``pokemon`` current
(``Stream(parent_current="nav")``) drills each for real ``stats``/``types``.
``calculate_bst`` / ``format_typing`` are kept in sync with
``pokeapi_etl_calc.py`` verbatim (a deliberate copy, not DRY).

``incorporator tideweaver run watershed.json`` imports this module, not the
Python entry, so the host-throttle registration below is what paces the CLI
path's 150 concurrent detail requests.

``nav`` fetches all 150 rows in one ``?limit=150`` call rather than 3
paginated pages: a parent-less ``Stream``'s implicit post-chunk refresh
aborts pagination after page 1, so the single unpaginated call sidesteps it.
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
