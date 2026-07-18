"""Pure-store sidecar for the CLI form (``watershed.json``).

``Coin``/``CoinDetail`` are defined ONCE, in ``parent_child_drilling.py``.
This module only re-exports them (via a plain import) plus the CLI-only
tokens ``watershed.json``'s ``"window"`` needs (``window_start`` /
``window_end``). ``watershed.json``'s ``conv_dict`` entries
(``pluck('links.homepage')``, ``inc(str, default='-')``) are all-literal
call-grammar tokens — no user-defined helper reference needed for them, so
this file carries no conv_dict logic of its own.

``incorporator tideweaver run watershed.json`` imports this module at
config-load time (unlike the Python entry, which is never imported by the
CLI path), so the host-throttle registration in
``parent_child_drilling.py`` — which runs as an import side effect the
moment the line below executes — is the ONLY thing that paces the 10
concurrent detail requests on the CLI path; this file must not register
its own, redundant throttle.

**Identity safety, and why this arrangement is required, not cosmetic.**
This file gets ``exec_module``'d under at least two distinct
``sys.modules`` cache keys within one ``tideweaver run`` invocation --
``load_watershed``'s config-load-time resolution and each ``Stream``
tick's own ``apply_inflow_resolution`` call use different ``name_hint``s
(see ``incorporator/usercode.py``). Because this file only IMPORTS
``parent_child_drilling`` rather than redefining ``Coin``/``CoinDetail``,
Python's own ``sys.modules['parent_child_drilling']`` cache guarantees
every such re-exec binds the SAME canonical class objects, so a
``coin_detail`` row seeded under one exec is visible via ``CoinDetail``
resolved under the other.

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

from parent_child_drilling import Coin, CoinDetail  # noqa: E402

__all__ = ["Coin", "CoinDetail", "window_start", "window_end"]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil, evaluated once at import
# time (a 3-minute span from "now").
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)
