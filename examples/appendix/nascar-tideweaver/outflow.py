"""Shared sidecar for examples/appendix/nascar-tideweaver/.

Defines the four ``Incorporator`` subclasses and the ``outflow(state)``
function used by BOTH entry points in this directory:

* ``nascar_tideweaver.py`` — Python runner, imports classes directly.
* ``watershed.json`` — CLI entry, references this file via ``"outflow": "outflow.py"``.

Both entry points stay in lockstep because they load the same class
definitions and the same join logic from this single module.

The three head / middle streams read from local JSON fixtures so the
example runs without any network access or credentials:

    python examples/appendix/nascar-tideweaver/nascar_tideweaver.py
    incorporator validate examples/appendix/nascar-tideweaver/watershed.json
    incorporator tideweaver run examples/appendix/nascar-tideweaver/watershed.json

Relative ``inc_file`` paths inside ``watershed.json`` resolve against the
config file's directory, so these commands work from any directory.
"""

from typing import Any

from incorporator import Incorporator


class LapData(Incorporator):
    """Head source — per-driver lap data."""


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
        row["laps"] = max(row["laps"], int(getattr(lap, "lap_number", 0) or 0))

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
