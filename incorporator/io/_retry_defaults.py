"""HTTP-layer retry defaults consumed by ``fetch.py``.

The canal-outer and compound-budget constants live in
``incorporator/tideweaver/_retry_defaults.py``, which imports
``_HTTP_INNER_STOP`` and ``_HTTP_INNER_WAIT_MAX`` from here for the
compound-budget formula.
"""

from __future__ import annotations

_HTTP_INNER_STOP: int = 8
_HTTP_INNER_WAIT_MAX: float = 30.0
_HTTP_INNER_WAIT_MIN: float = 2.0
_HTTP_INNER_WAIT_MULTIPLIER: float = 1.5

# Sane ceiling on a live 429/503 wait even when the server's Retry-After hint
# is honored (see _make_http_wait) -- caps abusive or misconfigured hints.
_HTTP_RETRY_AFTER_CEILING: float = 120.0

# Separate attempt cap for network-layer errors (connect-phase and post-send
# on idempotent methods).  Kept low so a dead host stops quickly.
# Semantics: total invocations including the first — stop fires when
# attempt_number >= _HTTP_NETWORK_RETRY_STOP, yielding exactly this many calls.
_HTTP_NETWORK_RETRY_STOP: int = 3

# Short backoff bounds for network-class errors (dead host / transient timeout).
# Small ceiling prevents the ~58 s exponential sleep that a shared
# wait_random_exponential(max=30) would produce over _HTTP_NETWORK_RETRY_STOP attempts.
_HTTP_NETWORK_WAIT_MIN: float = 0.25
_HTTP_NETWORK_WAIT_MAX: float = 3.0
_HTTP_NETWORK_WAIT_MULTIPLIER: float = 1.0
