"""
Tutorial 6 -- State Sports: Multi-League Team & Roster Drill (ESPN)
-------------------------------------------------------------------
Companion script for `examples/06-state-sports/README.md`.

Pick a US state (or Canadian province) code, discover every team whose venue
sits there across NFL / NBA / MLB / NHL via ESPN's public site API, drill
each roster concurrently, and rank players by salary and tenure.  Builds on
Tutorial 5's `inc_parent` / `inc_child` shape twice over in one script: a
whole-list fan-out (T5's own signature shape) drills every league's team
detail records to read their venue state, then a single-instance fan-out
(today's T6 shape) drills each matching team's own roster.

One-shot script, same shape as T5 -- no Watershed, no daemon.  The old
streaming-daemon half of this tutorial slot moved to T8 (`stream()`) and
T10 (`fjord()`), which own recurring-refresh ground.

Run with:
    python examples/06-state-sports/state_sports.py            # defaults to "CA"
    python examples/06-state-sports/state_sports.py ON
    python examples/06-state-sports/state_sports.py "California"
"""

import asyncio
import sys

from incorporator import Incorporator, calc, pluck

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

# ESPN's `franchise.venue.address.state` is NOT normalized across leagues:
# NFL/NBA/NHL already report 2-letter codes ("CA", "ON"), but MLB reports
# full names -- the US state name ("California"), DC as "District of
# Columbia" (where the Wizards' NBA record already says "DC"), and -- a
# fact beyond the original live probe -- the Blue Jays' province as
# "Ontario" where every NHL/NBA Canadian team already reports "ON".
# Verified live 2026-07-08.  50 US states + DC + the 13 Canadian
# provinces/territories, closed vocabulary.
STATE_NAME_TO_CODE = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "Alberta": "AB",
    "British Columbia": "BC",
    "Manitoba": "MB",
    "New Brunswick": "NB",
    "Newfoundland and Labrador": "NL",
    "Northwest Territories": "NT",
    "Nova Scotia": "NS",
    "Nunavut": "NU",
    "Ontario": "ON",
    "Prince Edward Island": "PE",
    "Quebec": "QC",
    "Saskatchewan": "SK",
    "Yukon": "YT",
}


def to_state_code(value: str) -> str:
    """Full state/DC name -> 2-letter code; already-abbreviated values (incl.
    Canadian provinces, which ESPN already reports as 2-letter: ON, QC, ...)
    pass through unchanged.  `pluck()`'s null-handling already skips this
    call on garbage input, so no `None`-guard belongs here."""
    return STATE_NAME_TO_CODE.get(value, value)


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


class Team(Incorporator):
    pass


class TeamDetail(Incorporator):
    pass


class Player(Incorporator):
    pass


async def discover_state_teams(region: str) -> tuple[list[tuple[str, str, Team]], int]:
    """Fetch every league's team list, drill venue detail, filter to `region`.

    Returns the matched teams alongside the total no-venue count across all
    four leagues (teams with no reachable `franchise.venue.address`).
    """
    matched: list[tuple[str, str, Team]] = []
    no_venue_total = 0

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
                "venue_state": pluck("franchise.venue.address.state", chain=to_state_code),
                "venue_zip": pluck("franchise.venue.address.zipCode"),
            },
            timeout=8,
        )

        # Note: `conv_dict` above ADDS location/team_id/abbreviation to `Team`
        # and venue_city/venue_state/venue_zip to `TeamDetail` -- it doesn't
        # drop either raw envelope.  drill_roster()'s `inc_child="team.id"`
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
            # insertion order.  No target_type=: the funcs already return
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


def print_league_summary(region: str, all_players: list[Player]) -> None:
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


def print_paycheck_board(all_players: list[Player]) -> None:
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


def print_veterans_board(all_players: list[Player]) -> None:
    pool = [p for p in all_players if p.tenure is not None]
    pool.sort(key=lambda p: p.tenure, reverse=True)

    print("\nVETERANS BOARD (all four leagues)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'TENURE':>7}{'TURNED-PRO-AT':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        turned_pro = p.turned_pro_at if p.turned_pro_at is not None else "-"
        print(f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{turned_pro!s:>14}")


def print_homegrown_board(region: str, all_players: list[Player]) -> None:
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
    region = to_state_code(sys.argv[1]) if len(sys.argv) > 1 else "CA"
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    state_teams, no_venue_total = await discover_state_teams(region)
    if no_venue_total:
        print(f"WARN: {no_venue_total} team(s) had no reachable venue address - excluded from the region filter.")
    if not state_teams:
        print(f"\nNo {region} teams found. Try a 2-letter US state/DC code ('NY', 'TX') or a Canadian province ('ON').")
        print("See the README's 'brand labels vs data attributes' section for how this filter works.")
        return

    names = ", ".join(f"{league} {team.inc_name}" for league, _, team in state_teams)
    print(f"OK: Found {len(state_teams)} {region} team(s): {names}")

    rosters = await asyncio.gather(*(drill_roster(league, sport, team) for league, sport, team in state_teams))

    all_players: list[Player] = []
    roster_rejects: list[tuple[str, list[str]]] = []
    for _league, team, active, failed_sources in rosters:
        all_players.extend(active)
        if failed_sources:
            roster_rejects.append((team.inc_name, failed_sources))
    print(f"OK: Loaded {len(all_players)} active players across {len(state_teams)} teams.")

    print_league_summary(region, all_players)
    print_paycheck_board(all_players)
    print_veterans_board(all_players)
    print_homegrown_board(region, all_players)

    # Structured-rejects walkthrough (mirrors T5/T6-precedent): each roster
    # drill returns its own `failed_sources` view even when other teams in
    # the same asyncio.gather() succeeded -- one team's rate-limit or
    # timeout doesn't sink the whole region.
    if roster_rejects:
        print("\nWARN: Some roster drills failed:")
        for team_name, failed_sources in roster_rejects:
            print(f"   - {team_name}: {failed_sources}")

    print("\nGoing further: cross-sport tallest/heaviest splits, calc_all() dense-rank")
    print("leaderboards, and exporting these boards to NDJSON/CSV all live in the README.")


if __name__ == "__main__":
    asyncio.run(main())
