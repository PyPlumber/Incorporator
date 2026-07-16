"""Sidecar for the CLI form (``watershed.json``).

Same single T5 parent-child drill as ``parent_child_drilling.py``: a ``coin``
current loads the top-10 CoinGecko market rows, then a ``coin_detail``
current (``Stream(parent_current="coin")``) drills ``/coins/{id}`` for each
one -- the exact ``inc_parent``/``inc_child`` fan-out this tutorial teaches.
``conv_dict``'s ``pluck``/``inc`` entries are pure literal-arg tokens declared
directly in ``watershed.json`` -- no sidecar helper function needed for them.

``incorporator tideweaver run watershed.json`` imports this module at
config-load time (unlike the Python entry, which is never imported by the
CLI path), so the host-throttle registration below is the ONLY thing that
paces the 10 concurrent detail requests on the CLI path.
"""

from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, register_host_penstock

# Pace api.coingecko.com at 0.2 req/sec (12/min -- comfortably under the
# free tier's 5-15/min ceiling). MUST live here, not just in
# parent_child_drilling.py -- this module is what the CLI path actually
# imports before firing the 10 concurrent detail requests.
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time -- see incorporator/tideweaver/config.py's
# build_watershed ordering.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=3)


class Coin(Incorporator):
    """Lightweight market row from /coins/markets -- populated by the "coin" current."""


class CoinDetail(Incorporator):
    """Full per-coin detail record from /coins/{id}.

    Populated by the "coin_detail" Stream(parent_current="coin") drill.
    """
