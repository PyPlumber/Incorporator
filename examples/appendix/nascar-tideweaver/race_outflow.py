"""Outflow logic for examples/appendix/nascar-tideweaver/watershed.json.

Defines the four ``Incorporator`` subclasses referenced from the
watershed config (head, two middles, and the tail's output class) plus
the ``outflow(state)`` function the Fjord current calls each tick.

The three middle / head streams read from local JSON files in this
directory so the CLI smoke-test runs without any network or
credentials:

    incorporator validate examples/appendix/nascar-tideweaver/watershed.json
    incorporator tideweaver run examples/appendix/nascar-tideweaver/watershed.json

Run paths inside the JSON config are relative to the *current working
directory* of the CLI process — invoke from the repo root.
"""

from typing import Any, Dict, List

from incorporator import Incorporator


class LapData(Incorporator):
    """Head source — per-driver lap data."""


class PitStops(Incorporator):
    """Middle source — per-driver pit-stop counts."""


class FlagEvents(Incorporator):
    """Middle source — race-flag colour timeline."""


class DriverState(Incorporator):
    """Derived output class for the tail Fjord flush.

    Because this name matches the tail current's ``class`` field in
    watershed.json, the shared ``flush()`` primitive uses *this* declared
    class verbatim each tick instead of inferring an anonymous schema
    from the row keys.
    """


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    by_driver: Dict[str, Dict[str, Any]] = {}
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
