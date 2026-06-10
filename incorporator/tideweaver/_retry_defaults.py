"""Centralised retry-policy defaults consumed by scheduler.py
+ architect.tune()'s _tune_compound_budget rule.

The HTTP-layer group (``_HTTP_*``) now lives in
``incorporator.io._retry_defaults`` and is re-exported here for backward
compatibility of the compound-budget formula and existing import paths.

These literals previously lived inline at fetch.py:307-308 (HTTP inner)
and scheduler.py:716-717 (canal outer).  Centralising avoids silent
divergence between configured policy and what _tune_compound_budget
warns about.
"""

from __future__ import annotations

from ..io._retry_defaults import _HTTP_INNER_STOP as _HTTP_INNER_STOP
from ..io._retry_defaults import _HTTP_INNER_WAIT_MAX as _HTTP_INNER_WAIT_MAX
from ..io._retry_defaults import _HTTP_INNER_WAIT_MIN as _HTTP_INNER_WAIT_MIN
from ..io._retry_defaults import _HTTP_INNER_WAIT_MULTIPLIER as _HTTP_INNER_WAIT_MULTIPLIER
from ..io._retry_defaults import _HTTP_NETWORK_RETRY_STOP as _HTTP_NETWORK_RETRY_STOP
from ..io._retry_defaults import _HTTP_NETWORK_WAIT_MAX as _HTTP_NETWORK_WAIT_MAX
from ..io._retry_defaults import _HTTP_NETWORK_WAIT_MIN as _HTTP_NETWORK_WAIT_MIN
from ..io._retry_defaults import _HTTP_NETWORK_WAIT_MULTIPLIER as _HTTP_NETWORK_WAIT_MULTIPLIER

_CANAL_OUTER_STOP: int = 5
_CANAL_OUTER_WAIT_MAX: float = 8.0
_CANAL_OUTER_WAIT_MIN: float = 0.5
_CANAL_OUTER_WAIT_MULTIPLIER: float = 1.0

_COMPOUND_RETRY_BUDGET_SEC: float = _CANAL_OUTER_STOP * _HTTP_INNER_STOP * _HTTP_INNER_WAIT_MAX  # 5 × 8 × 30 = 1200.0
