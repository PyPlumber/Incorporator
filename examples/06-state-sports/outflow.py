"""Outflow sidecar for Tutorial 6's single-pass roster Watershed.

Reads the `roster_drill` `CustomCurrent`'s parked
`Player._tideweaver_snapshot` (the active-roster subset, already
league/team-name tagged -- see `state_sports.py`'s `RosterDrill.tick`) and
materializes one row per player into `Roster`.

`Roster` is pre-declared here with every field typed explicitly, not left
bare (`class Roster(Incorporator): pass`). A bare pre-declared class whose
`outflow(state)` rows carry extra keys gets silently swapped for a
dynamically-built subclass by `incorporator/pipeline/outflow.py::flush`
(and fires a one-time `logger.warning` that reaches stderr by default) --
reading `Roster._tideweaver_snapshot` after the run would then find
nothing, because the snapshot lands on that dynamic subclass, not this
one. Declaring every field up front keeps this exact class object as the
one the Fjord flush uses.
"""

from typing import Any

from incorporator import Incorporator


class Roster(Incorporator):
    league: str | None = None
    team_name: str | None = None
    salary: int | None = None
    tenure: int | None = None
    pos: str | None = None
    birth_city: str | None = None
    birth_state: str | None = None
    salary_per_year: float | None = None
    turned_pro_at: int | None = None


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join `state["Player"]` into per-player `Roster` rows.

    Single upstream (`roster_drill`), so no cross-source join logic is
    needed -- this Fjord's value is handing the noisy, weak-ref'd `Player`
    registry off to a stable, park-friendly output class ready for NDJSON
    export.
    """
    rows: list[dict[str, Any]] = []
    for p in state.get("Player", []):
        rows.append(
            {
                # League-qualified: ESPN athlete ids are only guaranteed
                # unique within one sport.
                "inc_code": f"{p.league}:{p.inc_code}",
                "inc_name": p.inc_name,
                "league": p.league,
                "team_name": p.team_name,
                "salary": p.salary,
                "tenure": p.tenure,
                "pos": p.pos,
                "birth_city": p.birth_city,
                "birth_state": p.birth_state,
                "salary_per_year": p.salary_per_year,
                "turned_pro_at": p.turned_pro_at,
            }
        )
    return rows
