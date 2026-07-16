"""Shared sidecar for examples/appendix/nascar-tideweaver/.

Defines the four ``Incorporator`` subclasses and the ``outflow(state)``
function used by BOTH entry points in this directory:

* ``nascar_tideweaver.py`` — Python runner, imports classes directly.
* ``watershed.json`` — CLI entry, references this file via ``"outflow": "outflow.py"``.

Both entry points stay in lockstep because they load the same class
definitions and the same join logic from this single module.

The three head / middle streams read from local JSON fixtures, so the
example runs without any network access or credentials. See the README's
"Run it" section for the run commands (both entry forms).

Relative ``inc_file`` paths inside ``watershed.json`` resolve against the
config file's directory, so the CLI form works from any directory.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from incorporator import Incorporator
from incorporator.schema.converters import inc

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil. Fixtures are offline, so a
# 2-minute window gives the uniform 30s interval 4 ticks -- enough for the
# tail Fjord to flush its append-mode export more than once.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(minutes=2)


class LapData(Incorporator):
    """Head source — per-driver lap data."""


# Build-time coercion: the fixture already carries a native int, but a real
# telemetry feed may not — coercing here means outflow() reads ``lap.lap_number``
# as a plain int with no read-time int()/or-0 defensiveness. Source key ==
# output key, so ``inc`` (not ``calc``) is the shortest-correct primitive.
#
# ``inc`` is a base framework token always resolvable by watershed.json's
# string-form conv_dict, so no sidecar-allow-list plumbing is needed for the
# CLI form (unlike a user-defined helper such as mlb-pulse's
# ``flatten_team_records``).
LAPDATA_CONV_DICT = {
    "lap_number": inc(int, default=0),
}


class PitStops(Incorporator):
    """Middle source — per-driver pit-stop counts."""


class FlagEvents(Incorporator):
    """Middle source — race-flag colour timeline."""


class DriverState(Incorporator):
    """Derived output class for the tail Fjord flush.

    Declaring this class lets ``watershed.json`` name it via ``class`` and the
    Python runner pass it as ``cls=``.  The Fjord infers its fields from the
    rows ``outflow(state)`` returns and instantiates them as ``DriverState``
    records each tick — you never construct it yourself.
    """


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join laps + pits + flags into one row per driver.

    ``state`` is keyed by each upstream ``Incorporator`` subclass name
    and maps to a list of that class's current registry instances —
    populated by the upstream ``Stream`` currents' chunking drains and
    held alive between ticks via the strong-ref ``_tideweaver_snapshot``
    attribute the scheduler parks on each upstream class.

    The driver-or-inc_code fallback below (``getattr(lap, "driver", None) or
    getattr(lap, "inc_code", None)``) is field SELECTION, not coercion, and it
    stays read-time on purpose: ``inc_code`` is the framework-assigned PK
    bound from ``incorp_params={"inc_code": "driver"}`` AFTER conv_dict
    resolution, so no earlier build-time hook can read "what inc_code will
    resolve to" — see the README's honest-boundary note.
    """
    laps = state.get("LapData", [])
    pits = state.get("PitStops", [])
    flags = state.get("FlagEvents", [])

    by_driver: dict[str, dict[str, Any]] = {}
    for lap in laps:
        driver = getattr(lap, "driver", None) or getattr(lap, "inc_code", None)
        if driver is None:
            continue
        row = by_driver.setdefault(
            driver,
            {"driver": driver, "laps": 0, "pits": 0, "flag": None},
        )
        row["laps"] = max(row["laps"], lap.lap_number)

    for pit in pits:
        driver = getattr(pit, "driver", None) or getattr(pit, "inc_code", None)
        if driver is None:
            continue
        row = by_driver.setdefault(
            driver,
            {"driver": driver, "laps": 0, "pits": 0, "flag": None},
        )
        row["pits"] += 1

    if flags:
        latest_flag = flags[-1]
        for row in by_driver.values():
            row["flag"] = getattr(latest_flag, "color", None)

    return list(by_driver.values())
