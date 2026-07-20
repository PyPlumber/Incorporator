"""Sidecar for the NASCAR Tideweaver diamond -- a pure name-bag.

This file exists only because the CLI needs an importable module to point
``watershed.json``'s ``"outflow"`` key at; otherwise ``outflow(state)`` below
would just sit at the bottom of ``nascar_tideweaver.py``, as the plain
aggregation it already is.

``LapData``/``PitStops``/``FlagEvents``/``DriverState`` and ``outflow()``
are defined ONCE, in ``nascar_tideweaver.py``. This module only re-exports
them (via a plain ``import``) plus the CLI-only tokens the JSON config needs
(``window_start``/``window_end``).

**Why the ``sys.path.insert`` below is still here.** A real
``incorporator tideweaver run watershed.json`` load goes through
``load_user_module``, which since ``e6ab772`` caches purely on resolved
file path, short-circuits to an already-running ``__main__``, and
auto-inserts each sidecar's own directory onto ``sys.path`` -- no guard
needed there. This file's guard survives only because
``tests/public/api/test_nascar_tideweaver_etl.py`` loads it through
``tests/helpers.py``'s ``load_sidecar``, a separate, bespoke
``importlib`` loader that never got that fix.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from nascar_tideweaver import DriverState, FlagEvents, LapData, PitStops, outflow  # noqa: E402

__all__ = [
    "LapData",
    "PitStops",
    "FlagEvents",
    "DriverState",
    "window_start",
    "window_end",
    "outflow",
]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time, mirroring nascar_tideweaver.py's own main() window
# duration (15 seconds).
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=15)
