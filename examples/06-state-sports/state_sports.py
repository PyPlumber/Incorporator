"""Tutorial 6 -- State Sports: Two Chained Parent-Child Drills (ESPN).

Companion script for `examples/06-state-sports/README.md`.

Pick a US state / Canadian province code, discover every NFL/NBA/MLB/NHL team
whose venue sits there via ESPN's public site API, then drill each matched
team's roster straight into `Player` rows. A pure one-shot script: no
Watershed, no files read or written at runtime, ASCII-only stdout, and
`main()` is the only function (inline, top-to-bottom in dependency order).

Run with:
    python examples/06-state-sports/state_sports.py      # defaults to "CA"
"""

import asyncio
import functools
import operator
import sys

from incorporator import Incorporator, calc, inc, pluck

BASE = "https://site.api.espn.com/apis/site/v2/sports"

SPORTS = [
    ("NFL", "football/nfl"),
    ("NBA", "basketball/nba"),
    ("MLB", "baseball/mlb"),
    ("NHL", "hockey/nhl"),
]

# ESPN's ?enable=roster feed publishes salaries for NFL/NBA only, so the
# paycheck board is scoped to those two leagues.
SALARY_LEAGUES = ("NFL", "NBA")

# CountriesNow omits DC, but the Wizards' venue reports the bare code "DC";
# patch both directions so the .get chain (no identity passthrough) resolves it.
DC_SUPPLEMENT = {"District of Columbia": "DC", "DC": "DC"}

COUNTRIESNOW_URLS = [
    "https://countriesnow.space/api/v0.1/countries/states/q?country=United%20States",
    "https://countriesnow.space/api/v0.1/countries/states/q?country=Canada",
]

REFERENCE_API_ERROR = "ERROR: reference API unreachable - cannot normalize state names."


class StateRef(Incorporator):
    pass


class League(Incorporator):
    pass


class Team(Incorporator):
    pass


class Player(Incorporator):
    pass


async def main(region: str = "CA") -> None:
    """Discover ``region``'s teams across four leagues, then rank their rosters.

    Reuses T5's ``inc_parent`` drill twice, one parent at a time in a plain
    ``for`` loop: once per League (venue detail, Drill 1), once per matched
    Team (roster -> ``Player`` rows, Drill 2). ESPN's site API team objects
    carry no API self-href, so each ``{sport}/{league}`` URL is templated from
    the parent row's own attributes rather than a single whole-list drill.
    Every athlete builds (active and inactive alike); the boards filter
    ``if p.active`` at report time. ``pluck``, ``calc``, and ``inc`` each
    appear across the two ``conv_dict``s.
    """
    print("Fetching state/province reference data (CountriesNow)...")
    states = await StateRef.incorp(
        inc_url=COUNTRIESNOW_URLS,
        rec_path="data.states",
        inc_code="state_code",
        inc_name="name",
        timeout=8,
    )
    # Map codes to themselves and full names to their code (both directions),
    # plus the DC patch, so the .get chain normalizes either spelling.
    state_code_map: dict[str, str] = (
        {s.inc_code: s.inc_code for s in states} | {s.inc_name: s.inc_code for s in states} | DC_SUPPLEMENT
    )
    # Partial-failure check: one country 500ing still leaves `states` non-empty,
    # so probe an entry from each country before trusting the map.
    if "California" not in state_code_map or "Ontario" not in state_code_map:
        sys.exit(REFERENCE_API_ERROR)

    region = state_code_map.get(region, region)
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    league_urls = [f"{BASE}/{sport}/teams" for _, sport in SPORTS]
    leagues = await League.incorp(inc_url=league_urls, rec_path="sports.0", timeout=8)

    # Drill 1: T5's inc_parent shape, once per league -- each `{sport}/{league}`
    # URL is templated off `lg`'s own attributes, no composite-path reducer.
    teams: list[Team] = []
    for lg in leagues:
        part = await Team.incorp(
            inc_parent=lg,
            inc_child="leagues.teams.team.id",
            inc_url=f"{BASE}/{lg.slug}/{lg.leagues[0].slug}/teams/{{}}",
            rec_path="team",
            inc_code="uid",  # globally unique ("s:20~l:28~t:24"); team.id collides across leagues
            inc_name="displayName",
            conv_dict={
                "venue_city": pluck("franchise.venue.address.city"),
                "venue_state": pluck("franchise.venue.address.state", chain=state_code_map.get),
            },
            timeout=8,
        )
        for t in part:
            t.league = lg.leagues[0].abbreviation
        teams.extend(part)

    # Attribute equality -- ESPN has no server-side filter for this.
    matched = [t for t in teams if t.venue_state == region]
    if not matched:
        sys.exit(f"No {region} teams found - try 'NY', 'TX', or 'ON'.")

    names = ", ".join(f"{t.league} {t.inc_name}" for t in matched)
    print(f"OK: Found {len(matched)} {region} team(s): {names}")

    # Drill 2: T5's shape again, once per matched team -- rosters drill straight
    # into `Player` rows. `slugs` is a runtime dict from the League rows, keyed
    # by each Team's stamped abbreviation, holding the "{sport}/{league}" path.
    slugs: dict[str, str] = {lg.leagues[0].abbreviation: f"{lg.slug}/{lg.leagues[0].slug}" for lg in leagues}

    players: list[Player] = []
    for team in matched:
        roster = await Player.incorp(
            inc_parent=team,
            inc_child="id",
            inc_url=f"{BASE}/{slugs[team.league]}/teams/{{}}?enable=roster",
            rec_path="team.athletes",
            inc_code="uid",  # globally unique across leagues (see Drill 1)
            inc_name="fullName",
            conv_dict={
                # Output key == source key, so inc(TYPE) coercion (not calc());
                # the boards filter on p.active.
                "active": inc(bool, default=False),
                "salary": calc(int, "contract.salary", default=0, target_type=int),
                "tenure": calc(functools.partial(max, 1), "experience.years", default=1, target_type=int),
                "age": calc(int, "age", default=0, target_type=int),
                "pos": calc(str, "position.abbreviation", default="-", target_type=str),
                "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
                "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
                # Derived from the coerced fields above (insertion order);
                # salary_per_year is zero-safe since tenure floors to 1.
                "turned_pro_at": calc(operator.sub, "age", "tenure", default=0, target_type=int),
                "salary_per_year": calc(operator.truediv, "salary", "tenure", default=0.0, target_type=float),
            },
            timeout=10,
        )
        for p in roster:
            p.league, p.team_name = team.league, team.inc_name
        players.extend(roster)

    active_count = sum(1 for p in players if p.active)
    print(f"OK: Loaded {len(players)} players ({active_count} active) across {len(matched)} teams.")

    print(f"\n{region} across NFL / NBA / MLB / NHL")
    print("=" * 70)
    for league, _ in SPORTS:
        league_players = [p for p in players if p.league == league]
        if not league_players:
            continue
        team_count = len({p.team_name for p in league_players})
        league_active_count = sum(1 for p in league_players if p.active)
        salary_known_total = sum(1 for p in league_players if p.active and p.salary > 0)
        payroll_total = sum(p.salary for p in league_players if p.active)
        # Only show payroll when the league publishes salaries at all.
        payroll_note = f", payroll ${payroll_total:,.0f}" if salary_known_total else ""
        print(
            f"{league:<5} {team_count} team(s), {len(league_players)} players ({league_active_count} active), "
            f"salary known {salary_known_total}/{league_active_count}{payroll_note}"
        )

    # Active players only -- the report-time gate (inactive org rows carry
    # stats but don't belong in a top-10).
    pool = sorted(
        (p for p in players if p.league in SALARY_LEAGUES and p.active and p.salary > 0),
        key=lambda p: p.salary,
        reverse=True,
    )
    print("\nPAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'POS':<5}{'TENURE':>7}{'SALARY':>14}{'$/YR-TENURE':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        print(
            f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.pos:<5}"
            f"{p.tenure!s:>7}{f'${p.salary:,.0f}':>14}{f'${p.salary_per_year:,.0f}':>14}"
        )

    pool = sorted((p for p in players if p.active), key=lambda p: p.tenure, reverse=True)
    print("\nVETERANS BOARD (all four leagues)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'TENURE':>7}{'TURNED-PRO-AT':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        print(f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{p.turned_pro_at:>14}")

    # birthPlace.state is already a 2-letter code, so it compares directly
    # against the normalized `region` (active players only).
    heroes = [p for p in players if p.active and p.birth_state == region]
    print(f"\nHOMEGROWN BOARD ({region}-born players on a {region} team)")
    if not heroes:
        print(f"   (none found -- no player in this pool was born in {region})")
    else:
        header = f"{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'BORN':<28}"
        print(header)
        print("-" * len(header))
        for p in heroes:
            born = f"{p.birth_city}, {p.birth_state}"
            print(f"{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{born[:27]:<28}")


if __name__ == "__main__":
    asyncio.run(main("CA"))  # change the region here -- e.g. "NY", "TX", "ON", "California"
