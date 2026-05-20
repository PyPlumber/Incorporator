"""I/O layer for Incorporator: format handling, compression, network, and pagination.

The throttle subsystem is re-exported here so callers can register
custom :class:`ThrottleStrategy` implementations for their in-house APIs
without reaching into the submodule:

.. code-block:: python

    from incorporator.io import BurstThrottle, register_host_throttle

    register_host_throttle(
        "api.internal.acme.com",
        lambda: BurstThrottle(requests_per_second=50.0, burst=200),
    )
"""

from .throttle import (
    BurstThrottle,
    FixedIntervalThrottle,
    NullThrottle,
    ThrottleStrategy,
    register_host_throttle,
    resolve_throttle,
)

__all__ = [
    "BurstThrottle",
    "FixedIntervalThrottle",
    "NullThrottle",
    "ThrottleStrategy",
    "register_host_throttle",
    "resolve_throttle",
]
