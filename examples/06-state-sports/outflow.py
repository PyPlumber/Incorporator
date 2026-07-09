"""Outflow sidecar for Tutorial 6's single-pass roster Watershed.

Reads the `rosters` Stream's parked `TeamRoster._tideweaver_snapshot` (one
row per matched team, each carrying its own `team_name` and raw `athletes`
list -- `rec_path="team"` in `state_sports.py`'s parent-child drill keeps
team attribution and the roster array together, so no per-player tagging
pass is needed) and the transitive `MatchedTeam._tideweaver_snapshot` (for
the `league` lookup), joins the two on `inc_code` (ESPN's team `uid`),
flattens each team's active athletes into one row per player, and
materializes `Roster`.

`Roster` is pre-declared here with every field typed explicitly, not left
bare (`class Roster(Incorporator): pass`). A bare pre-declared class whose
`outflow(state)` rows carry extra keys gets silently swapped for a
dynamically-built subclass by `incorporator/pipeline/outflow.py::flush`
(and fires a one-time `logger.warning` that reaches stderr by default) --
reading `Roster._tideweaver_snapshot` after the run would then find
nothing, because the snapshot lands on that dynamic subclass, not this
one. Declaring every field up front keeps this exact class object as the
one the Fjord flush uses.

**`athlete` is a nested Pydantic sub-model, not a dict** (`conv_dict`'s
`pluck("athletes")` lifts ESPN's raw list through unchanged, and the
framework's own dynamic-schema inference turns each element into a
sub-model). Attribute access, not `.get()`, is required, and manual `is
not None` guards on `contract` / `experience` / `position` / `birthPlace`
are genuinely needed here -- this is plain user Python, not a `conv_dict`
pipeline, so `conv_dict`'s `is_garbage_value` null-safety guarantee
doesn't apply to this function.
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


def _salary(athlete: Any) -> int | None:
    return athlete.contract.salary if athlete.contract is not None else None


def _tenure(athlete: Any) -> int | None:
    return athlete.experience.years if athlete.experience is not None else None


def _pos(athlete: Any) -> str | None:
    return athlete.position.abbreviation if athlete.position is not None else None


def _birth_city(athlete: Any) -> str | None:
    return athlete.birthPlace.city if athlete.birthPlace is not None else None


def _birth_state(athlete: Any) -> str | None:
    return athlete.birthPlace.state if athlete.birthPlace is not None else None


def salary_per_year(salary: int | None, tenure: int | None) -> float | None:
    """None when salary is unpublished -- the common case for MLB/NHL in this feed."""
    if salary is None:
        return None
    return salary / max(tenure or 1, 1)


def turned_pro_at(age: int | None, tenure: int | None) -> int | None:
    """None when age is missing -- some NFL rookies omit it."""
    if age is None:
        return None
    return age - (tenure or 0)


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join `state["TeamRoster"]` against `state["MatchedTeam"]` and flatten active players.

    `TeamRoster` is `boards`'s direct upstream (named in `parent_currents`);
    `MatchedTeam` is transitive (two edges up the chain) but still visible
    in `state` -- the scheduler parks every non-direct ancestor's class
    snapshot unconditionally, no `parent_currents` listing required.
    """
    matched_by_code = {m.inc_code: m for m in state.get("MatchedTeam", [])}
    rows: list[dict[str, Any]] = []
    for roster in state.get("TeamRoster", []):
        match = matched_by_code.get(roster.inc_code)
        league = match.league if match is not None else None
        # MLB's org-list quirk: `team.athletes` is the whole ~250-person
        # organization, not the 26-man active roster -- filtering here (not
        # a separate pass) keeps the flatten and the active-only rule in
        # one place.
        for athlete in roster.athletes:
            if not athlete.active:
                continue
            salary = _salary(athlete)
            tenure = _tenure(athlete)
            rows.append(
                {
                    # League-qualified: ESPN athlete ids are only guaranteed
                    # unique within one sport.
                    "inc_code": f"{league}:{athlete.id}",
                    "inc_name": athlete.fullName,
                    "league": league,
                    "team_name": roster.team_name,
                    "salary": salary,
                    "tenure": tenure,
                    "pos": _pos(athlete),
                    "birth_city": _birth_city(athlete),
                    "birth_state": _birth_state(athlete),
                    "salary_per_year": salary_per_year(salary, tenure),
                    "turned_pro_at": turned_pro_at(athlete.age, tenure),
                }
            )
    return rows
