"""Penstock — the canal-toolkit's rate-control primitive.

A *penstock* is a structural gate whose **throttle settings**
(``rate_per_sec``, ``burst``, ``window_sec``) determine the rate at
which flow passes through.  Used at the io layer to throttle outbound
HTTP requests, and at the Tideweaver edge layer to throttle wave
consumption between currents — the same primitive at both layers, so
``watershed.json`` and ``register_host_penstock`` describe rate control
with one vocabulary.

The penstock decides whether the next attempt is permitted; the caller
picks the semantics.  Two call styles share the same underlying
:meth:`Penstock.evaluate` / :meth:`Penstock.record` pair:

- :meth:`Penstock.acquire` — async; sleeps until permitted.  Used by
  the HTTP throttle wrapper (:class:`BoundPenstock`).
- :meth:`Penstock.consume_reason` — sync; returns a skip reason string
  if blocked, or ``None`` if permitted.  Used by the Tideweaver
  scheduler — the edge defers to the next tick instead of sleeping.

State lives outside the penstock so the same gate config can serve
multiple sources concurrently.  :class:`FlowState` is the canonical
dataclass at the io layer; the Tideweaver scheduler's ``_EdgeState``
satisfies the same structural shape and is passed in directly — no
copy needed.

Backpressure — the one penstock subclass that reads reservoir context
— lives in :mod:`incorporator.observability.tideweaver.flow` because
it has no meaningful HTTP-layer interpretation.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Union
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Mutable state (lives outside the frozen Pydantic config)
# ---------------------------------------------------------------------------


@dataclass
class FlowState:
    """Mutable counters tracked between consumption attempts.

    The canal-toolkit penstocks read and write these fields directly.
    At the io layer one :class:`FlowState` instance is bound to one
    source; at the Tideweaver edge layer the scheduler's ``_EdgeState``
    has the same field names and is passed in via duck typing — no
    conversion or copy.

    Attributes:
        last_consumed_at: Monotonic timestamp of the most recent
            successful consumption.  ``None`` before the first one.
            Read by sustained / burst / signal penstocks.
        bucket_tokens: Current token count for a :class:`BurstPenstock`.
            ``None`` until first touch (lazy init to the burst capacity).
        bucket_last_refill_at: Monotonic timestamp of the last token-bucket
            refill.  Paired with ``bucket_tokens``.
        window_log: Monotonic timestamps of consumptions within the
            current rolling window.  Used by :class:`WindowPenstock`.
    """

    last_consumed_at: Optional[float] = None
    bucket_tokens: Optional[float] = None
    bucket_last_refill_at: Optional[float] = None
    window_log: List[float] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Penstock base class — the unified rate-control primitive
# ---------------------------------------------------------------------------


class Penstock(BaseModel):
    """Canal-toolkit rate-control primitive shared by HTTP and edge layers.

    Subclasses implement :meth:`evaluate` (the pure decision: is this
    consumption permitted now, and if not how long until it is?) and
    :meth:`record` (post-consume bookkeeping: debit a token, append to
    the window log, advance the watermark).

    Callers pick one of two semantic wrappers built on top:

    - HTTP layer: ``await penstock.acquire(state, lock)`` — sleeps until
      permitted, then records.
    - Edge layer: ``penstock.consume_reason(state, flow, now)`` — returns
      ``"penstock_limited"`` if blocked, ``None`` if permitted; the
      scheduler defers instead of sleeping.

    The ``flow`` (edge layer) / unused (HTTP layer) third argument to
    ``consume_reason`` is plumbed through to :meth:`evaluate` as
    ``context=`` so :class:`Backpressure
    <incorporator.observability.tideweaver.flow.BackpressurePenstock>`
    can read reservoir depth.  All other penstocks ignore it.
    """

    model_config = ConfigDict(frozen=True)

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        """Decide whether the next consumption is permitted at ``now``.

        Args:
            state: Mutable counters object (:class:`FlowState` or a
                duck-typed equivalent like the Tideweaver scheduler's
                ``_EdgeState``).  ``evaluate`` may mutate refill / window
                bookkeeping but does not record a fresh consumption —
                that's :meth:`record`'s job.
            now: Monotonic clock reading from
                ``asyncio.get_running_loop().time()``.
            context: Optional layer-specific extension.  The edge layer
                passes the parent :class:`FlowControl` so
                :class:`BackpressurePenstock` can read reservoir depth.
                The io layer always passes ``None``.

        Returns:
            ``None`` if the consumption is permitted right now; else the
            number of seconds to wait until it would be permitted.  A
            return of :data:`math.inf` means "never under current
            conditions" — the HTTP wrapper sleeps the request out, the
            edge wrapper treats it as ``"penstock_limited"``.
        """
        raise NotImplementedError

    def record(self, state: Any, now: float) -> None:
        """Apply post-consume bookkeeping (debit a token, append to log).

        Args:
            state: The same mutable counters object passed to
                :meth:`evaluate`.
            now: Monotonic time at which the consumption occurred —
                possibly later than the ``now`` passed to
                :meth:`evaluate` if the caller slept in between.

        Default implementation is a no-op; subclasses override.
        """
        return None

    async def acquire(self, state: Any, lock: asyncio.Lock) -> None:
        """HTTP-style throttle: sleep under ``lock`` until permitted, then record.

        Args:
            state: Mutable counters for this source.
            lock: Per-source :class:`asyncio.Lock` serialising concurrent
                callers so the refill / debit pair stays atomic.
        """
        async with lock:
            now = asyncio.get_running_loop().time()
            wait = self.evaluate(state, now)
            if wait is not None:
                await asyncio.sleep(wait)
                now = asyncio.get_running_loop().time()
            self.record(state, now)

    def consume_reason(
        self,
        edge_state: Any,
        flow: Any,
        now: float,
    ) -> Optional[str]:
        """Edge-style throttle: return ``"penstock_limited"`` if blocked, ``None`` otherwise.

        Default impl reads the edge's :class:`FlowState` — either composed
        under ``edge_state.flow_state`` (Tideweaver scheduler path) or
        the ``edge_state`` itself when it already IS a FlowState (unit
        tests, direct callers) — and delegates to :meth:`evaluate`,
        translating the wait-seconds return into a skip reason.

        Subclasses with edge-specific needs (e.g.
        :class:`BackpressurePenstock` reading reservoir depth via
        ``flow`` AND wave count via ``edge_state.waves``) override
        directly and access the FlowState via the same fallback pattern.

        Args:
            edge_state: Tideweaver scheduler ``_EdgeState`` (with a
                composed ``flow_state: FlowState`` attribute) OR a bare
                :class:`FlowState` (or any duck-typed equivalent).
            flow: The parent :class:`FlowControl` for this edge.  Passed
                to :meth:`evaluate` as ``context``; ignored by every
                penstock except backpressure.
            now: Monotonic time.

        Returns:
            ``"penstock_limited"`` to skip this tick, or ``None`` to
            permit consumption.
        """
        state = getattr(edge_state, "flow_state", edge_state)
        wait = self.evaluate(state, now, context=flow)
        return "penstock_limited" if wait is not None else None

    def post_consume(self, edge_state: Any, now: float) -> None:
        """Edge-style post-consume hook (delegates to :meth:`record`).

        Accepts either an ``_EdgeState`` with composed ``flow_state`` or a
        bare :class:`FlowState` — matches :meth:`consume_reason`'s fallback.
        """
        state = getattr(edge_state, "flow_state", edge_state)
        self.record(state, now)


# ---------------------------------------------------------------------------
# Concrete penstock subclasses (shared between HTTP and edge layers)
# ---------------------------------------------------------------------------


class NullPenstock(Penstock):
    """No-op penstock — always permits, never records.

    The io-layer escape hatch (e.g. tests, trusted internal sources)
    and the edge-layer "explicitly no throttling here" marker.
    """

    type: Literal["null"] = "null"

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        return None

    def record(self, state: Any, now: float) -> None:
        return None


class SustainedPenstock(Penstock):
    """Leaky bucket: minimum gap of ``1 / rate_per_sec`` between consumptions.

    Equivalent to a single-token bucket refilled at ``rate_per_sec``.
    The simplest sustained rate limit, suitable as the default for an
    in-house API or a public host with a documented req/sec ceiling.
    """

    type: Literal["sustained"] = "sustained"
    rate_per_sec: float = Field(gt=0.0, description="Max sustained consumptions per second.")

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        if state.last_consumed_at is None:
            return None
        min_gap = 1.0 / self.rate_per_sec
        elapsed: float = now - state.last_consumed_at
        if elapsed >= min_gap:
            return None
        return min_gap - elapsed

    def record(self, state: Any, now: float) -> None:
        state.last_consumed_at = now


class BurstPenstock(Penstock):
    """Token bucket: initial burst of ``burst`` consumptions, then refills at ``rate_per_sec``.

    The right shape when an API publishes a documented burst window
    (e.g. "100 requests then 10/min") — the bucket starts full and
    drains under load.
    """

    type: Literal["burst"] = "burst"
    rate_per_sec: float = Field(gt=0.0, description="Refill rate (tokens per second).")
    burst: int = Field(ge=1, description="Bucket capacity — max tokens held.")

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        # First-touch initialization: bucket starts full.
        if state.bucket_tokens is None:
            state.bucket_tokens = float(self.burst)
            state.bucket_last_refill_at = now
        else:
            # Explicit None check, not ``or now`` — ``bucket_last_refill_at``
            # can legitimately be 0.0 in synthetic tests, and ``0.0 or now``
            # silently substitutes ``now`` and erases the refill window.
            last_refill = state.bucket_last_refill_at if state.bucket_last_refill_at is not None else now
            elapsed = now - last_refill
            state.bucket_tokens = min(
                float(self.burst),
                state.bucket_tokens + elapsed * self.rate_per_sec,
            )
            state.bucket_last_refill_at = now
        if state.bucket_tokens < 1.0:
            wait: float = (1.0 - state.bucket_tokens) / self.rate_per_sec
            return wait
        return None

    def record(self, state: Any, now: float) -> None:
        if state.bucket_tokens is not None:
            state.bucket_tokens = max(0.0, state.bucket_tokens - 1.0)
        state.last_consumed_at = now


class WindowPenstock(Penstock):
    """Rolling-window quota: at most ``cap`` consumptions per ``window_sec``.

    Suitable for APIs that publish a hard quota over a fixed window
    ("60 requests per minute, rolling") rather than a steady rate.
    """

    type: Literal["window"] = "window"
    window_sec: float = Field(gt=0.0, description="Rolling lookback window in seconds.")
    cap: int = Field(ge=1, description="Max consumptions within the window.")

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        cutoff = now - self.window_sec
        # Evict entries older than the window.  Mutation here keeps the
        # log bounded as the window slides forward.
        state.window_log = [t for t in state.window_log if t > cutoff]
        if len(state.window_log) >= self.cap:
            oldest: float = state.window_log[0]
            return oldest + self.window_sec - now
        return None

    def record(self, state: Any, now: float) -> None:
        state.window_log.append(now)
        state.last_consumed_at = now


class SignalPenstock(Penstock):
    """User callable returns the current allowed rate.

    ``rate_fn(state, now) -> float`` runs inside :meth:`evaluate`; a
    return ``<= 0`` blocks the consumption entirely (returns
    :data:`math.inf` from :meth:`evaluate`).  A positive return is
    treated as the sustained rate ceiling.

    Note: ``rate_fn`` signature changed in v1.3.0 — the legacy
    ``rate_fn(scheduler, edge_state, now)`` shape no longer receives
    the scheduler; the strategy hierarchy doesn't read scheduler
    privates anymore.  Migration: drop the first ``scheduler`` arg.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: Literal["signal"] = "signal"
    rate_fn: Callable[[Any, float], float] = Field(
        description="Callable returning the current allowed rate in consumptions/sec.",
    )

    def evaluate(
        self,
        state: Any,
        now: float,
        *,
        context: Any = None,
    ) -> Optional[float]:
        rate = self.rate_fn(state, now)
        if rate <= 0.0:
            return float("inf")
        if state.last_consumed_at is None:
            return None
        min_gap = 1.0 / rate
        elapsed: float = now - state.last_consumed_at
        if elapsed >= min_gap:
            return None
        return min_gap - elapsed

    def record(self, state: Any, now: float) -> None:
        state.last_consumed_at = now


# ---------------------------------------------------------------------------
# HTTP-layer binding — pairs a penstock with its state + lock
# ---------------------------------------------------------------------------


@dataclass
class BoundPenstock:
    """A :class:`Penstock` paired with its per-source state and lock.

    Returned by the HTTP-layer host-throttle registry (see
    :func:`resolve_penstock`) and consumed by :func:`make_request` —
    the binding owns the mutable :class:`FlowState` and the
    :class:`asyncio.Lock`, so the penstock config itself stays frozen
    and shareable.

    Callers use the parameterless :meth:`acquire` convenience method:

    .. code-block:: python

        bound = resolve_penstock(url)
        await bound.acquire()
        # ...issue the request...
    """

    penstock: Penstock
    state: FlowState
    lock: asyncio.Lock

    async def acquire(self) -> None:
        """Throttle one consumption — sleeps under the lock until permitted."""
        await self.penstock.acquire(self.state, self.lock)


# ---------------------------------------------------------------------------
# Per-host penstock registry — the HTTP-layer entry point
# ---------------------------------------------------------------------------


#: Per-host penstock registry — keyed by lowercase hostname.  **Empty by
#: default**: the framework ships with no implicit per-host throttling.
#: Use :func:`register_host_penstock` to attach a penstock to any host
#: that requires one; the penstock config is shared but each
#: :func:`resolve_penstock` call gets a fresh :class:`FlowState` + lock
#: pair so concurrent fan-out legs run independently.
#:
#: Migration from v1.2.0 implicit hosts — register these at startup if
#: you relied on them:
#:
#: .. code-block:: python
#:
#:     from incorporator import register_host_penstock
#:     from incorporator.io.penstock import SustainedPenstock
#:
#:     # CoinGecko public (anon): 5–15 req/min — 0.2 = 12 req/min, headroom.
#:     register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
#:     # PokeAPI: 100 req/min documented ceiling — 1.5 = 90 req/min.
#:     register_host_penstock("pokeapi.co", SustainedPenstock(rate_per_sec=1.5))
#:     # NHTSA vPIC: 100–200 req/min documented; method-agnostic.
#:     register_host_penstock("vpic.nhtsa.dot.gov", SustainedPenstock(rate_per_sec=1.5))
_HOST_PENSTOCKS: Dict[str, Penstock] = {}


DEFAULT_RPS: float = 15.0
"""Penstock rate used when no host match and no caller-supplied rate."""


_BYPASS_ENV_VAR: str = "INCORPORATOR_RATE_LIMIT_BYPASS"
"""Set to ``"1"`` to force :class:`NullPenstock` everywhere — test-only."""


def register_host_penstock(host: str, penstock: Union[Penstock, Callable[[], Penstock]]) -> None:
    """Register a per-host penstock for outbound HTTP throttling.

    Use this to attach a rate-limit policy to an in-house API or to
    override a built-in entry.  Each call to :func:`resolve_penstock`
    returns a fresh :class:`BoundPenstock` (sharing the registered
    penstock config but with its own :class:`FlowState` + lock) so
    each fan-out leg gets independent state.

    Args:
        host: Lowercase hostname (e.g. ``"api.internal.acme.com"``).
        penstock: Either a :class:`Penstock` instance (the canonical
            form) or a zero-arg callable returning a fresh
            :class:`Penstock` (the legacy factory form — accepted for
            back-compat with v1.2.0 ``register_host_throttle`` calls
            that pass ``lambda: FixedIntervalThrottle(rate)``).

    Example::

        from incorporator import register_host_penstock
        from incorporator.io.penstock import BurstPenstock

        register_host_penstock(
            "api.internal.acme.com",
            BurstPenstock(rate_per_sec=50.0, burst=200),
        )
    """
    if callable(penstock) and not isinstance(penstock, Penstock):
        _HOST_PENSTOCKS[host] = penstock()
    else:
        _HOST_PENSTOCKS[host] = penstock


def resolve_penstock(
    source: Any,
    *,
    requests_per_second: Optional[float] = None,
    burst: Optional[int] = None,
) -> BoundPenstock:
    """Pick a penstock for one source URL, bound with fresh state + lock.

    Precedence:

    1. ``INCORPORATOR_RATE_LIMIT_BYPASS=1`` env var → :class:`NullPenstock`
       (the global test-friendly escape hatch).
    2. Caller-supplied ``requests_per_second <= 0`` → :class:`NullPenstock`
       (the documented per-call escape hatch).
    3. Caller-supplied positive ``requests_per_second`` → either
       :class:`SustainedPenstock` or :class:`BurstPenstock` if ``burst``
       was also passed.
    4. Per-host registry match → the registered penstock config (with a
       fresh :class:`FlowState`).
    5. Fallback → ``SustainedPenstock(rate_per_sec=DEFAULT_RPS)``
       (15 req/sec).

    Args:
        source: A URL string (or any value with a hostname extractable
            via ``urllib.parse``).  Non-string sources skip the host
            lookup and use the default.
        requests_per_second: Caller override.  ``0`` or negative selects
            :class:`NullPenstock`.
        burst: Optional burst capacity; if supplied alongside a positive
            ``requests_per_second``, returns :class:`BurstPenstock`.

    Returns:
        A fresh :class:`BoundPenstock` ready for one source's
        :meth:`acquire` calls.
    """
    penstock: Penstock
    if os.environ.get(_BYPASS_ENV_VAR) == "1":
        penstock = NullPenstock()
    elif requests_per_second is not None:
        if requests_per_second <= 0:
            penstock = NullPenstock()
        elif burst is not None:
            penstock = BurstPenstock(rate_per_sec=requests_per_second, burst=burst)
        else:
            penstock = SustainedPenstock(rate_per_sec=requests_per_second)
    else:
        matched: Optional[Penstock] = None
        if isinstance(source, str):
            host = urlparse(source).hostname or ""
            matched = _HOST_PENSTOCKS.get(host)
        penstock = matched if matched is not None else SustainedPenstock(rate_per_sec=DEFAULT_RPS)
    return BoundPenstock(penstock=penstock, state=FlowState(), lock=asyncio.Lock())


def known_host_rates() -> Dict[str, float]:
    """Return ``host → rate_per_sec`` for every host in the penstock registry.

    Intended for diagnostics, logging, and the
    :func:`incorporator.observability.tideweaver.architect` probe that
    wants to suggest the user's already-registered rate without
    instantiating the penstock.  Only penstocks exposing a
    ``rate_per_sec`` field (Sustained, Burst, …) appear in the result;
    Window / Signal / Null / custom subclasses without a single rate
    figure are skipped.
    """
    rates: Dict[str, float] = {}
    for host, penstock in _HOST_PENSTOCKS.items():
        rate = getattr(penstock, "rate_per_sec", None)
        if isinstance(rate, (int, float)):
            rates[host] = float(rate)
    return rates
