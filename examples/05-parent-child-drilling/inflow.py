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
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from parent_child_drilling import Coin, CoinDetail

__all__ = ["Coin", "CoinDetail", "window_start", "window_end"]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil, evaluated once at import
# time (a 3-minute span from "now").
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)
