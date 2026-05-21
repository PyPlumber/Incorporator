"""I/O layer for Incorporator: format handling, compression, network, and pagination.

The penstock subsystem (the canal-toolkit's rate-control primitive) is
re-exported here so callers can register custom :class:`Penstock`
implementations for their in-house APIs without reaching into the
submodule:

.. code-block:: python

    from incorporator.io import BurstPenstock, register_host_penstock

    register_host_penstock(
        "api.internal.acme.com",
        BurstPenstock(rate_per_sec=50.0, burst=200),
    )
"""

from .penstock import (
    DEFAULT_RPS,
    BoundPenstock,
    BurstPenstock,
    FlowState,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
    known_host_rates,
    register_host_penstock,
    resolve_penstock,
)

__all__ = [
    "DEFAULT_RPS",
    "BoundPenstock",
    "BurstPenstock",
    "FlowState",
    "NullPenstock",
    "Penstock",
    "SignalPenstock",
    "SustainedPenstock",
    "WindowPenstock",
    "known_host_rates",
    "register_host_penstock",
    "resolve_penstock",
]
