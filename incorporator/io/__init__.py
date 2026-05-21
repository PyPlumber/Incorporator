"""I/O layer for Incorporator: format handling, compression, network, and pagination.

The penstock subsystem (the canal-toolkit's rate-control primitive) is
re-exported here so callers can register custom :class:`Penstock`
implementations for their in-house APIs without reaching into the
submodule:

.. code-block:: python

    from incorporator.io import BurstPenstock, register_host_throttle

    register_host_throttle(
        "api.internal.acme.com",
        lambda: BurstPenstock(rate_per_sec=50.0, burst=200),
    )
"""

from .penstock import (
    BoundPenstock,
    BurstPenstock,
    FlowState,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
)
from .throttle import (
    BurstThrottle,
    FixedIntervalThrottle,
    NullThrottle,
    ThrottleStrategy,
    register_host_throttle,
    resolve_throttle,
)

__all__ = [
    "BoundPenstock",
    "BurstPenstock",
    "BurstThrottle",
    "FixedIntervalThrottle",
    "FlowState",
    "NullPenstock",
    "NullThrottle",
    "Penstock",
    "SignalPenstock",
    "SustainedPenstock",
    "ThrottleStrategy",
    "WindowPenstock",
    "register_host_throttle",
    "resolve_throttle",
]
