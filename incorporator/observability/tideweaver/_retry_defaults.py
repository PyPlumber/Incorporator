"""Centralised retry-policy defaults consumed by fetch.py + scheduler.py
+ architect.tune()'s _tune_compound_budget rule.

These literals previously lived inline at fetch.py:307-308 (HTTP inner)
and scheduler.py:716-717 (canal outer).  Centralising avoids silent
divergence between configured policy and what _tune_compound_budget
warns about.
"""

from __future__ import annotations

_HTTP_INNER_STOP: int = 8
_HTTP_INNER_WAIT_MAX: float = 30.0
_HTTP_INNER_WAIT_MIN: float = 2.0
_HTTP_INNER_WAIT_MULTIPLIER: float = 1.5

_CANAL_OUTER_STOP: int = 5
_CANAL_OUTER_WAIT_MAX: float = 8.0
_CANAL_OUTER_WAIT_MIN: float = 0.5
_CANAL_OUTER_WAIT_MULTIPLIER: float = 1.0

_COMPOUND_RETRY_BUDGET_SEC: float = _CANAL_OUTER_STOP * _HTTP_INNER_STOP * _HTTP_INNER_WAIT_MAX  # 5 × 8 × 30 = 1200.0
