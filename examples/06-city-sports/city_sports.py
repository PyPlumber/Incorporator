"""
Tutorial 6 -- City Sports: Multi-League Team & Roster Drill (ESPN)
-------------------------------------------------------------------
Companion script for `examples/06-city-sports/README.md`.

Pick a city, discover every team it fields across NFL / NBA / MLB / NHL via
ESPN's public site API, drill each roster concurrently, and rank players by
salary and tenure.  Builds on Tutorial 5's `inc_parent` / `inc_child` shape:
here the parent is a single already-built `Team` instance (not a whole
`IncorporatorList`) and the child fan-out fires once per city team rather
than once per top-N parent row.

One-shot script, same shape as T5 -- no Watershed, no daemon.  The old
streaming-daemon half of this tutorial slot moved to T8 (`stream()`) and
T10 (`fjord()`), which own recurring-refresh ground.

Run with:
    python examples/06-city-sports/city_sports.py
    python examples/06-city-sports/city_sports.py "New York"
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
# live: MLB/NHL coverage is 0/N across every LA team) -- the paycheck board
# is scoped to those two leagues rather than pad the rest with "-" rows.
SALARY_LEAGUES = ("NFL", "NBA")

# ESPN's team `location` strings are its own metro labels, not the reader's
# expectation -- Brooklyn != New York, New Jersey Devils, Golden State,
# Vegas, state-named teams (Arizona, Minnesota, ...).  This small table only
# needs to cover cities this README's "try another city" section names;
# an unlisted city still runs, its hometown board just comes up empty.
CITY_STATE = {
    "Los Angeles": "CA",
    "New York": "NY",
    "Chicago": "IL",
    "Boston": "MA",
}

# Metro birthplace-city aliases for the hometown-heroes board.  Only the
# default city gets a hand-tuned metro set; every other city falls back to
# an exact city-name match against CITY_STATE.  Note "Glendale": a real
# LA-metro city, and exactly why the birth_state guard below exists.
HOMETOWN_METRO = {
    "Los Angeles": {
        "Los Angeles",
        "Hawthorne",
        "Inglewood",
        "Long Beach",
        "Compton",
        "Northridge",
        "Glendale",
        "Pasadena",
        "Carson",
        "Irvine",
        "Newport Beach",
        "Anaheim",
    },
}


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


class Player(Incorporator):
    pass


async def discover_city_teams(city: str) -> list[tuple[str, str, Team]]:
    """Fetch every league's team list, filter to `city`, report unreachable leagues."""
    city_teams: list[tuple[str, str, Team]] = []

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

        # Note: `conv_dict` above ADDS location/team_id/abbreviation, it doesn't
        # drop the raw `team` envelope -- drill_roster()'s `inc_child="team.id"`
        # depends on that envelope still being present on the built instance.
        city_teams.extend((league, sport, team) for team in teams if team.location == city)

    return city_teams


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


def print_league_summary(city: str, all_players: list[Player]) -> None:
    print(f"\n{city} across NFL / NBA / MLB / NHL")
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


def print_hometown_board(city: str, all_players: list[Player]) -> None:
    state = CITY_STATE.get(city, "")
    metro = HOMETOWN_METRO.get(city, {city})
    # birth_state guard is mandatory: without it, same-named-but-wrong-state
    # cities sneak in.  Verified live: Chargers RB Jaret Patterson was born in
    # Glendale, MO -- a city-only match against the LA metro set (which
    # rightly contains Glendale, CA) would wrongly crown him a hometown hero.
    heroes = [p for p in all_players if p.birth_city in metro and p.birth_state == state]

    print(f"\nHOMETOWN HEROES ({city} metro, state-matched)")
    if not heroes:
        print(f"   (none found -- {city} may not be in this script's hometown metro table; see README)")
        return
    header = f"{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'BORN':<28}"
    print(header)
    print("-" * len(header))
    for p in heroes:
        born = f"{p.birth_city}, {p.birth_state}"
        print(f"{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{born[:27]:<28}")


async def main() -> None:
    city = sys.argv[1] if len(sys.argv) > 1 else "Los Angeles"
    print(f"Discovering {city}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    city_teams = await discover_city_teams(city)
    if not city_teams:
        print(f"\nNo {city} teams found. ESPN uses its own metro labels -- try 'New York', 'Chicago', or 'Boston'.")
        print("See the README's city-string caveat table before assuming the feed is broken.")
        return

    names = ", ".join(f"{league} {team.inc_name}" for league, _, team in city_teams)
    print(f"OK: Found {len(city_teams)} {city} team(s): {names}")

    rosters = await asyncio.gather(*(drill_roster(league, sport, team) for league, sport, team in city_teams))

    all_players: list[Player] = []
    roster_rejects: list[tuple[str, list[str]]] = []
    for _league, team, active, failed_sources in rosters:
        all_players.extend(active)
        if failed_sources:
            roster_rejects.append((team.inc_name, failed_sources))
    print(f"OK: Loaded {len(all_players)} active players across {len(city_teams)} teams.")

    print_league_summary(city, all_players)
    print_paycheck_board(all_players)
    print_veterans_board(all_players)
    print_hometown_board(city, all_players)

    # Structured-rejects walkthrough (mirrors T5/T6-precedent): each roster
    # drill returns its own `failed_sources` view even when other teams in
    # the same asyncio.gather() succeeded -- one team's rate-limit or
    # timeout doesn't sink the whole city.
    if roster_rejects:
        print("\nWARN: Some roster drills failed:")
        for team_name, failed_sources in roster_rejects:
            print(f"   - {team_name}: {failed_sources}")

    print("\nGoing further: cross-sport tallest/heaviest splits, calc_all() dense-rank")
    print("leaderboards, and exporting these boards to NDJSON/CSV all live in the README.")


if __name__ == "__main__":
    asyncio.run(main())
