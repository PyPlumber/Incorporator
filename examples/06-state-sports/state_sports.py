"""
Tutorial 6 -- State Sports: Multi-League Team & Roster Drill (ESPN)
-------------------------------------------------------------------
Companion script for `examples/06-state-sports/README.md`.

Pick a US state (or Canadian province) code, discover every team whose venue
sits there across NFL / NBA / MLB / NHL via ESPN's public site API, drill
each roster through a single-pass Tideweaver Watershed, and rank players by
salary and tenure. Two phases:

Phase 1 (plain async, T5's own shape) -- fetch a live state/province
name -> code reference map from CountriesNow, discover every league's team
list, drill each team's venue detail (T5's whole-list `inc_parent` fan-out),
and filter to `region` by attribute equality.

Phase 2 (this tutorial's new ground) -- a 2-current Tideweaver `Watershed`:
a `CustomCurrent` (`roster_drill`) drills every matched team's roster and
tags league/team-name context, feeding a `Fjord` (`boards`) that joins the
active-player rows into a `Roster` output class and exports NDJSON. This is
the curriculum's first Watershed exposure -- T11 remains the capstone that
covers the full vocabulary (diamonds, penstocks, spillways).

Run with:
    python examples/06-state-sports/state_sports.py            # defaults to "CA"
    python examples/06-state-sports/state_sports.py ON
    python examples/06-state-sports/state_sports.py "California"
"""

import asyncio
import functools
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from pydantic import Field

from incorporator import Incorporator, calc, pluck
from incorporator.tideweaver import CustomCurrent, Fjord, Tideweaver, Watershed

HERE = Path(__file__).resolve().parent
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via `python -m` or from
# a working directory other than HERE. Python only auto-adds the script's own
# directory to sys.path for `python <script>` invocations.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from outflow import Roster  # noqa: E402

SPORTS = [
    ("NFL", "football/nfl"),
    ("NBA", "basketball/nba"),
    ("MLB", "baseball/mlb"),
    ("NHL", "hockey/nhl"),
]

# ESPN's `?enable=roster` feed publishes salaries for NFL/NBA only (verified
# live: MLB/NHL coverage is 0/N across every CA team) -- the paycheck board
# is scoped to those two leagues rather than pad the rest with "-" rows.
SALARY_LEAGUES = ("NFL", "NBA")

# CountriesNow's US-states feed has no District of Columbia entry (verified
# live 2026-07-08), but MLB's venue feed reports "District of Columbia" for
# the Nationals -- a one-entry supplement closes that one gap. Even a live
# reference API needs a hygiene check.
DC_SUPPLEMENT = {"District of Columbia": "DC"}

COUNTRIESNOW_URLS = [
    "https://countriesnow.space/api/v0.1/countries/states/q?country=United%20States",
    "https://countriesnow.space/api/v0.1/countries/states/q?country=Canada",
]

REFERENCE_API_ERROR = "ERROR: reference API unreachable - cannot normalize state names."


def to_state_code(mapping: dict[str, str], value: str) -> str:
    """Full state/DC/province name -> 2-letter code; already-abbreviated
    values pass through the dict.get(..., value) fallback unchanged.
    `pluck()`'s null-handling already skips this call on garbage input, so
    no `None`-guard belongs here."""
    return mapping.get(value, value)


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


class StateRef(Incorporator):
    pass


class Team(Incorporator):
    pass


class TeamDetail(Incorporator):
    pass


class Player(Incorporator):
    pass


async def fetch_state_code_map() -> dict[str, str]:
    """Build the full-name -> 2-letter-code map from CountriesNow (US + Canada).

    This reference map must exist before any venue-state normalization can
    run -- a silent empty map would produce a filter that matches nothing
    with no explanation why. Fail fast instead: one ASCII error line, exit
    non-zero.
    """
    mapping: dict[str, str] = dict(DC_SUPPLEMENT)
    for url in COUNTRIESNOW_URLS:
        states = await StateRef.incorp(
            inc_url=url,
            rec_path="data.states",
            inc_code="state_code",
            inc_name="name",
            timeout=8,
        )
        if not states:
            print(REFERENCE_API_ERROR)
            sys.exit(1)
        for state in states:
            mapping[state.inc_name] = state.inc_code
    return mapping


async def discover_state_teams(region: str, state_code_map: dict[str, str]) -> tuple[list[tuple[str, str, Team]], int]:
    """Fetch every league's team list, drill venue detail, filter to `region`.

    Returns the matched teams alongside the total no-venue count across all
    four leagues (teams with no reachable `franchise.venue.address`).
    """
    matched: list[tuple[str, str, Team]] = []
    no_venue_total = 0
    to_code = functools.partial(to_state_code, state_code_map)

    for league, sport in SPORTS:
        teams = await Team.incorp(
            inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams",
            rec_path="sports.0.leagues.0.teams",
            inc_code="team.uid",  # globally unique ("s:20~l:28~t:24") -- team.id collides across leagues
            inc_name="team.displayName",
            conv_dict={
                "location": pluck("team.location"),
                "team_id": pluck("team.id"),
                "abbreviation": pluck("team.abbreviation"),
            },
            timeout=8,
        )
        if not teams:
            print(f"WARN: {league} team list unreachable - skipping.")
            for entry in teams.rejects:
                print(f"   - {entry}")
            continue

        # T5's signature whole-list `inc_parent` fan-out: `teams` is the
        # whole IncorporatorList just built above, not a single instance --
        # every team in this league gets its own concurrent detail request.
        details = await TeamDetail.incorp(
            inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}",
            inc_parent=teams,
            inc_child="team.id",
            rec_path="team",
            inc_code="uid",  # top-level post-rec_path -- same string as Team's dotted "team.uid"
            inc_name="displayName",
            conv_dict={
                "venue_city": pluck("franchise.venue.address.city"),
                "venue_state": pluck("franchise.venue.address.state", chain=to_code),
                "venue_zip": pluck("franchise.venue.address.zipCode"),
            },
            timeout=8,
        )

        # Note: `conv_dict` above ADDS location/team_id/abbreviation to `Team`
        # and venue_city/venue_state/venue_zip to `TeamDetail` -- it doesn't
        # drop either raw envelope. drill_roster()'s `inc_child="team.id"`
        # depends on the *original* `Team` instance's envelope, which is why
        # the join below recovers `Team.inc_dict[...]` rather than reusing
        # the `TeamDetail` instance directly (its `rec_path="team"` means it
        # has no nested "team" key left to drill).
        for detail in details:
            if detail.venue_state is None:
                no_venue_total += 1
                continue
            if detail.venue_state != region:
                continue
            team = Team.inc_dict.get(detail.inc_code)
            if team is None:
                continue  # defensive; should not happen -- same-run build above
            matched.append((league, sport, team))

    return matched, no_venue_total


async def drill_roster(league: str, sport: str, team: Team) -> tuple[str, Team, list[Player], list[str]]:
    """Single-instance parent drill: `team.id` templates into the `{}` slot."""
    players = await Player.incorp(
        inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}?enable=roster",
        inc_parent=team,
        inc_child="team.id",
        rec_path="team.athletes",
        inc_code="id",
        inc_name="fullName",
        conv_dict={
            "salary": pluck("contract.salary"),
            "tenure": pluck("experience.years"),
            "pos": pluck("position.abbreviation"),
            "birth_city": pluck("birthPlace.city"),
            "birth_state": pluck("birthPlace.state"),
            # Read raw dotted paths, not the flattened salary/tenure fields
            # above -- keeps the derived fields independent of conv_dict
            # insertion order. No target_type=: the funcs already return
            # native float/int, and a bare `target_type=float` would try to
            # coerce their intentional `None` and log a spurious warning on
            # every no-salary row (~half of all players in this feed).
            "salary_per_year": calc(salary_per_year, "contract.salary", "experience.years"),
            "turned_pro_at": calc(turned_pro_at, "age", "experience.years"),
        },
        timeout=8,
    )
    # MLB's org-list quirk: team.athletes is the whole ~250-person organization,
    # not the 26-man active roster -- active must be filtered before board math.
    active = [p for p in players if p.active]
    for p in active:
        p.league = league
        p.team_name = team.inc_name
    return league, team, active, players.failed_sources


class RosterDrill(CustomCurrent):
    """Drills every matched team's roster and tags league/team-name context.

    ESPN's `team.athletes[i]` carries no back-reference to the parent
    team's `displayName`/`location` (confirmed live 2026-07-08) -- there is
    no declarative way to flatten a roster array while retaining a sibling
    field of the object being flattened, so a per-team `incorp()` call plus
    a post-hoc tagging loop is the genuine `CustomCurrent` escape hatch
    here, not a shortcut around a `Stream(parent_current=...)` whole-list
    fan-out (which would silently lose team attribution -- see the
    README's "Suspected framework gaps" note).
    """

    matched_teams: list[tuple[str, str, Any]] = Field(default_factory=list)
    roster_rejects: list[tuple[str, list[str]]] = Field(default_factory=list)

    async def tick(self, scheduler: Any) -> None:
        rosters = await asyncio.gather(
            *(drill_roster(league, sport, team) for league, sport, team in self.matched_teams)
        )
        active_players: list[Player] = []
        rejects: list[tuple[str, list[str]]] = []
        for _league, team, active, failed_sources in rosters:
            active_players.extend(active)
            if failed_sources:
                rejects.append((team.inc_name, failed_sources))
        self.roster_rejects = rejects
        # A NEW list object -- defeats CustomCurrent._run_tick's auto-park
        # identity check, which would otherwise snapshot the whole raw
        # Player.inc_dict (including MLB's ~250-player org roster this
        # active filter has already excluded).
        Player._tideweaver_snapshot = active_players


def print_league_summary(region: str, all_players: list[Any]) -> None:
    print(f"\n{region} across NFL / NBA / MLB / NHL")
    print("=" * 70)
    for league, _ in SPORTS:
        league_players = [p for p in all_players if p.league == league]
        if not league_players:
            continue
        teams_in_league = {p.team_name for p in league_players}
        with_salary = [p for p in league_players if p.salary is not None]
        payroll = sum(p.salary for p in with_salary)
        payroll_note = f", payroll ${payroll:,.0f}" if with_salary else ""
        print(
            f"{league:<5} {len(teams_in_league)} team(s), {len(league_players)} active players, "
            f"salary known {len(with_salary)}/{len(league_players)}{payroll_note}"
        )


def print_paycheck_board(all_players: list[Any]) -> None:
    pool = [p for p in all_players if p.league in SALARY_LEAGUES and p.salary is not None]
    pool.sort(key=lambda p: p.salary, reverse=True)

    print("\nPAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'POS':<5}{'TENURE':>7}{'SALARY':>14}{'$/YR-TENURE':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        spy = f"${p.salary_per_year:,.0f}" if p.salary_per_year is not None else "-"
        tenure = p.tenure if p.tenure is not None else "-"
        print(
            f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{(p.pos or '-'):<5}"
            f"{tenure!s:>7}{f'${p.salary:,.0f}':>14}{spy:>14}"
        )


def print_veterans_board(all_players: list[Any]) -> None:
    pool = [p for p in all_players if p.tenure is not None]
    pool.sort(key=lambda p: p.tenure, reverse=True)

    print("\nVETERANS BOARD (all four leagues)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'TENURE':>7}{'TURNED-PRO-AT':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        turned_pro = p.turned_pro_at if p.turned_pro_at is not None else "-"
        print(f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{turned_pro!s:>14}")


def print_homegrown_board(region: str, all_players: list[Any]) -> None:
    # Pure attribute equality -- no brand-string tables. `birthPlace.state` on
    # players uses 2-letter codes already (verified live), so it compares
    # directly against the normalized `region`.
    heroes = [p for p in all_players if p.birth_state == region]

    print(f"\nHOMEGROWN BOARD ({region}-born players on a {region} team)")
    if not heroes:
        print(f"   (none found -- no player in this pool was born in {region})")
        return
    header = f"{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'BORN':<28}"
    print(header)
    print("-" * len(header))
    for p in heroes:
        born = f"{p.birth_city}, {p.birth_state}"
        print(f"{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{born[:27]:<28}")


async def main() -> None:
    print("Fetching state/province reference data (CountriesNow)...")
    state_code_map = await fetch_state_code_map()

    region_arg = sys.argv[1] if len(sys.argv) > 1 else "CA"
    region = to_state_code(state_code_map, region_arg)
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    state_teams, no_venue_total = await discover_state_teams(region, state_code_map)
    if no_venue_total:
        print(f"WARN: {no_venue_total} team(s) had no reachable venue address - excluded from the region filter.")
    if not state_teams:
        print(f"\nNo {region} teams found. Try a 2-letter US state/DC code ('NY', 'TX') or a Canadian province ('ON').")
        print("See the README's 'brand labels vs data attributes' section for how this filter works.")
        return

    names = ", ".join(f"{league} {team.inc_name}" for league, _, team in state_teams)
    print(f"OK: Found {len(state_teams)} {region} team(s): {names}")

    out_file = OUT / "state_sports_roster.ndjson"
    roster_drill = RosterDrill(
        name="roster_drill",
        cls=Player,
        interval=60.0,
        on_error="isolate",
        matched_teams=state_teams,
    )
    boards = Fjord(
        name="boards",
        cls=Roster,
        interval=60.0,
        on_error="isolate",
        export_params={
            "file_path": str(out_file),
            "format": "ndjson",
            # "replace", not "append": this is a fresh snapshot every run,
            # not an accumulating log -- board-printing below reads this
            # same file back, so a stale prior run's rows must not linger.
            "if_exists": "replace",
        },
    )

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))
    watershed = Watershed.chain(
        window=window,
        currents=[roster_drill, boards],
        gate_mode="weir",
        outflow=str(OUTFLOW_PATH),
        drain_timeout=15.0,
    )

    print(f"\nRunning single-pass roster watershed for {region} ({len(state_teams)} teams)...")
    async for tide in Tideweaver(watershed).run():
        print(
            f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<24} | skipped: {len(tide.skipped):2d}"
        )

    # Read the exported NDJSON back, not `Roster._tideweaver_snapshot` --
    # the Fjord flush parks that snapshot on the `Roster` class object its
    # OWN outflow.py load resolves (`load_user_module`'s hashed
    # sys.modules cache key), which is a DIFFERENT Python class object
    # than the one this script imported above via a plain `sys.path`
    # import. Re-reading the just-written export file (the same pattern
    # `examples/11-tideweaver/arb_scanner.py` uses) sidesteps that
    # cross-module identity split entirely.
    roster_rows: list[Any] = []
    if out_file.exists():
        for line in out_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                roster_rows.append(SimpleNamespace(**json.loads(line)))
    print(f"OK: Loaded {len(roster_rows)} active players across {len(state_teams)} teams.")

    print_league_summary(region, roster_rows)
    print_paycheck_board(roster_rows)
    print_veterans_board(roster_rows)
    print_homegrown_board(region, roster_rows)

    # Structured-rejects walkthrough (mirrors T5/T6-precedent): each roster
    # drill returns its own failed_sources view even when other teams in
    # the same asyncio.gather() succeeded -- one team's rate-limit or
    # timeout doesn't sink the whole region.
    if roster_drill.roster_rejects:
        print("\nWARN: Some roster drills failed:")
        for team_name, failed_sources in roster_drill.roster_rejects:
            print(f"   - {team_name}: {failed_sources}")

    if roster_rows:
        print(f"\nWrote {len(roster_rows)} roster row(s) to {out_file}")

    print("\nGoing further: cross-sport tallest/heaviest splits and calc_all() dense-rank")
    print("leaderboards both live in the README.")


if __name__ == "__main__":
    asyncio.run(main())
