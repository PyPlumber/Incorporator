"""Pure-store sidecar for the CLI form (``watershed.json``).

``Nav``/``Pokemon`` and the ``calculate_bst``/``format_typing`` reducers are
defined ONCE, in ``pokeapi_etl_calc.py``. This module only re-exports them
(via a plain import) plus the CLI-only tokens ``watershed.json``'s
``"window"`` needs (``window_start`` / ``window_end``). ``watershed.json``'s
``conv_dict`` entries reference ``calculate_bst``/``format_typing`` by name
(``"calc(calculate_bst, 'stats', ...)"``), so the reflective resolver needs
these names importable from this file even though the logic lives elsewhere.

``incorporator tideweaver run watershed.json`` imports this module at
config-load time (unlike the Python entry, which is never imported by the
CLI path), so the host-throttle registration in ``pokeapi_etl_calc.py`` --
which runs as an import side effect the moment the line below executes -- is
the ONLY thing that paces the CLI path's 150 concurrent detail requests;
this file must not register its own, redundant throttle.

``nav`` fetches all 150 rows in one ``?limit=150`` call rather than 3
paginated ``?limit=50`` pages (unlike the Python entry's ``NextUrlPaginator``
+ ``call_lim=3``), preserving the currently-verified single-tick behavior of
this watershed.

**Identity safety, and why this arrangement is required, not cosmetic.**
This file gets ``exec_module``'d under at least two distinct
``sys.modules`` cache keys within one ``tideweaver run`` invocation --
``load_watershed``'s config-load-time resolution and each ``Stream``
tick's own ``apply_inflow_resolution`` call use different ``name_hint``s
(see ``incorporator/usercode.py``). Because this file only IMPORTS
``pokeapi_etl_calc`` rather than redefining ``Nav``/``Pokemon``, Python's
own ``sys.modules['pokeapi_etl_calc']`` cache guarantees every such re-exec
binds the SAME canonical class objects, so a ``pokemon`` row seeded under
one exec is visible via ``Pokemon`` resolved under the other.

**The one gap this file works around.** ``load_user_module`` does not add
this file's own parent directory to ``sys.path`` before running it (unlike
``python <script>.py``, which auto-prepends the script's directory) -- the
``sys.path.insert`` below is required, guarded against a double insert.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from pokeapi_etl_calc import Nav, Pokemon, calculate_bst, format_typing  # noqa: E402

__all__ = ["Nav", "Pokemon", "calculate_bst", "format_typing", "window_start", "window_end"]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil, evaluated once at import
# time (a 3-minute span from "now" -- ~150 drills at 1.5 req/s take ~100s).
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)
