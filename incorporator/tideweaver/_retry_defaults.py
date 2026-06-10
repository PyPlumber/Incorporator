"""Centralised retry-policy defaults consumed by ``scheduler.py`` and ``architect.tune()``'s compound-budget rule.

The HTTP-layer budget factors ``_HTTP_INNER_STOP`` and ``_HTTP_INNER_WAIT_MAX`` are imported from
``incorporator.io._retry_defaults`` for use in the ``_COMPOUND_RETRY_BUDGET_SEC`` formula.
Canal-outer constants (``_CANAL_OUTER_*``) are defined here.
"""

from __future__ import annotations

from ..io._retry_defaults import _HTTP_INNER_STOP, _HTTP_INNER_WAIT_MAX

_CANAL_OUTER_STOP: int = 5
_CANAL_OUTER_WAIT_MAX: float = 8.0
_CANAL_OUTER_WAIT_MIN: float = 0.5
_CANAL_OUTER_WAIT_MULTIPLIER: float = 1.0

_COMPOUND_RETRY_BUDGET_SEC: float = _CANAL_OUTER_STOP * _HTTP_INNER_STOP * _HTTP_INNER_WAIT_MAX  # 5 × 8 × 30 = 1200.0
