"""Pure-store sidecar for the CLI form (``watershed.json``).

``Nav``/``Pokemon`` and the ``calculate_bst``/``format_typing`` reducers are
defined ONCE, in ``pokeapi_etl_calc.py``. This module only re-exports them
(via a plain import) plus the CLI-only tokens ``watershed.json``'s
``"window"`` needs (``window_start`` / ``window_end``). ``watershed.json``'s
``conv_dict`` entries reference ``calculate_bst``/``format_typing`` by name
(``"calc(calculate_bst, 'stats', ...)"``), so the reflective resolver needs
these names importable from this file even though the logic lives elsewhere.

``watershed.json``'s own ``host_penstocks`` block is what paces the CLI
path's 150 concurrent detail requests -- a declarative registration at
config-load time, no sidecar import side effect required. Importing
``pokeapi_etl_calc`` below still re-registers the same host at the same
rate as a side effect (it serves the Python entry, which never reads
``watershed.json``); registration is a plain dict overwrite, so the
duplicate is a harmless no-op, not a conflict.

``nav`` fetches all 150 rows in one ``?limit=150`` call rather than 3
paginated ``?limit=50`` pages (unlike the Python entry's ``NextUrlPaginator``
+ ``call_lim=3``), preserving the currently-verified single-tick behavior of
this watershed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pokeapi_etl_calc import Nav, Pokemon, calculate_bst, format_typing

__all__ = ["Nav", "Pokemon", "calculate_bst", "format_typing", "window_start", "window_end"]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil, evaluated once at import
# time (a 3-minute span from "now" -- ~150 drills at 1.5 req/s take ~100s).
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)
