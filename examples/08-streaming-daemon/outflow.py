"""Outflow sidecar for `pipeline_stateful.json`, also imported directly by
`streaming_daemon.py`'s `stateful_demo()` -- defines the `BinancePair`
receiver class both paths share.

`stream(stateful_polling=True)` exports the same already-built row
instances `incorp()`/`refresh()` produced (an identity pass-through);
that only round-trips when the receiver class declares real fields -- a
bare class crashes at export time with `Outflow Error: 1 validation error
for BinancePair ... input_type=DynamicModel`.
"""

from incorporator import LoggedIncorporator


class BinancePair(LoggedIncorporator):
    """Live ticker registry -- auto-keyed by trading symbol.

    Declaring these fields explicitly is LOAD-BEARING (see module
    docstring), not cosmetic type-hinting.
    """

    symbol: str
    lastPrice: float
    priceChangePercent: float
    highPrice: float
    lowPrice: float
    volume: float
