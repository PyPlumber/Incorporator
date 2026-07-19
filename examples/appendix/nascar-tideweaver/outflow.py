"""Sidecar for the NASCAR Tideweaver diamond -- a pure name-bag.

This file exists only because the CLI needs an importable module to point
``watershed.json``'s ``"outflow"`` key at; otherwise ``outflow(state)`` below
would just sit at the bottom of ``nascar_tideweaver.py``, as the plain
aggregation it already is.

``LapData``/``PitStops``/``FlagEvents``/``DriverState`` and ``outflow()``
are defined ONCE, in ``nascar_tideweaver.py``. This module only re-exports
them (via a plain ``import``) plus the CLI-only tokens the JSON config needs
(``window_start``/``window_end``).

**Identity safety, and why this arrangement is required, not cosmetic.**
This file gets ``exec_module``'d 2-3x under distinct ``sys.modules`` keys
(``usercode.py``'s ``load_user_module``, invoked once each for the class/
token resolver, and any fjord-outflow path). A class DEFINED here would
become a distinct class object on every such exec -- an ``issubclass``/
identity check spanning two of those execs could then silently disagree.
Because this file only IMPORTS ``nascar_tideweaver``, Python's own module
cache (``sys.modules['nascar_tideweaver']``, set on first import) guarantees
every re-exec of this sidecar binds the SAME canonical class objects.

**The one gap this file works around.** ``load_user_module`` does not add
this file's own parent directory to ``sys.path`` before running it (unlike
``python <script>.py``, which auto-prepends the script's directory), so the
guarded ``sys.path.insert`` below is required.
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
