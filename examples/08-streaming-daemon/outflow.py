"""Outflow sidecar for `examples/08-streaming-daemon/pipeline_stateful.json`
and (imported directly) for `streaming_daemon.py`'s `stateful_demo()`.

stream(stateful_polling=True) delegates to the fjord engine with a
synthesised IDENTITY outflow (incorporator/pipeline/_stateful_shim.py):
export receives the SAME already-built row instances incorp()/refresh()
produced, not freshly-shaped dicts. That pass-through only round-trips
cleanly when the receiver class declares real fields.

A bare `class BinancePair(LoggedIncorporator): pass` crashes here --
confirmed live: `Outflow Error: 1 validation error for BinancePair ...
input_type=DynamicModel`. Root cause (framework-side, tracked as a gap,
not fixed by this file): incorp()/refresh() always validate rows through
an auto-inferred class rooted at the receiver class actually used; but
flush()'s bare-class fallback (incorporator/pipeline/outflow.py:263)
re-infers a class rooted at the generic `Incorporator` base instead of
the receiver class, producing a sibling class the already-built rows
aren't instances of. Declaring real fields below sidesteps the buggy
branch entirely: flush() then reuses this class directly (no
re-inference), so ancestry matches and the pass-through succeeds.

Only 6 of Binance's ~21 ticker fields are typed here; the rest still
land in the output NDJSON as raw passthrough strings via the auto-
inferred schema's extra='allow' (confirmed live) -- this is a deliberate
minimal-but-representative field selection for the tutorial, not a
completeness bug.
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
