"""
Tutorial 6 -- State Sports: Two Chained Parent-Child Drills (ESPN)
------------------------------------------------------------------
Companion script for `examples/06-state-sports/README.md`.

Pick a US state (or Canadian province) code, discover every team whose
venue sits there across NFL / NBA / MLB / NHL via ESPN's public site API,
then drill every matched team's roster **directly into `Player` rows** --
one `Player.incorp(inc_parent=team, rec_path="team.athletes", ...)` call
per team, no intermediate roster class, no in-memory hand-off. A pure
one-shot script: no time-windowed orchestration, no files read or written
at runtime, ASCII-only stdout. Modeled directly on
`examples/appendix/pokeapi-etl/pokeapi_etl_calc.py`'s inline shape --
discovery -> `inc_parent` drill -> another drill -> print tables reading
precomputed attributes, all in one linear `main()`.

**A plain series of `incorp()` calls, T5's shape reused once per vertical.**
ESPN's team detail/roster payloads only ever live at a fixed
`{sport}/{league}/teams/{id}` URL, and that `{sport}/{league}` pair can't
be recovered from a fetched row -- `conv_dict` only ever sees the response,
never the request that produced it. Rather than build a composite path
string per team (a `calc()` reducer working around a whole-list drill),
this tutorial drills `Team.incorp()` **once per `League` row** (Drill 1)
and `Player.incorp()` **once per matched `Team` row** (Drill 2) in a plain
`for` loop, reading each parent's own attributes straight into an `inc_url`
f-string template, then stamping the parent's `league`/`team_name` context
onto the freshly built children. `inc_parent` accepts a single
`Incorporator` instance just as readily as a whole `IncorporatorList` (see
`incorporator/base.py`'s `incorp()` signature) -- the *same* primitive T5
introduced, applied to one parent at a time.

**One `active` flag, filtered in the reports.** `rec_path="team.athletes"`
drills straight past ESPN's roster envelope into every athlete row -- active
AND inactive (MLB's `athletes` array is the whole organization, not the
26-man roster) -- and `players` holds all of them. Each board filters
`if p.active` before it sorts or compares, so inactive org players (who
carry real tenure/birthplace data but no current-roster relevance) never
surface in a top-10.

`conv_dict` exercises three converters across the two calls:
  * `pluck()`  -- nested lifts (`franchise.venue.address.*`) and the
                  reference-map normalization.
  * `calc()`   -- every per-player derived field.
  * `inc()`    -- `"active": inc(bool, default=False)` -- the one case in
                  this tutorial where the output key equals the source key,
                  so a plain TYPE coercion is the right primitive.

Every printed field carries a build-time `calc(..., default=...)`, so the
board print code formats values without a single missing-data conditional.

Run with:
    python examples/06-state-sports/state_sports.py      # defaults to "CA" -- edit main("CA") in the
                                                           # entry block below to try another region
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

# ESPN's `?enable=roster` feed publishes salaries for NFL/NBA only (verified
# live: MLB/NHL coverage is 0/N across every CA team) -- the paycheck board
# is scoped to those two leagues rather than pad the rest with "-" rows.
SALARY_LEAGUES = ("NFL", "NBA")

# CountriesNow's US-states feed has no District of Columbia entry at all
# (verified live 2026-07-09), but the NBA Wizards' own venue record reports
# the already-abbreviated "DC" -- `chain=state_code_map.get` has no
# passthrough fallback (unlike the old `mapping.get(value, value)`), so both
# directions need an explicit identity entry or "DC" resolves to `None` and
# falls into the no-venue bucket instead of matching a `DC` region query.
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
    print("Fetching state/province reference data (CountriesNow)...")
    states = await StateRef.incorp(
        inc_url=COUNTRIESNOW_URLS,
        rec_path="data.states",
        inc_code="state_code",
        inc_name="name",
        timeout=8,
    )
    # Identity-augmented map: codes map to themselves, full names map to
    # their code, and the DC gap (above) is patched in both directions.
    # `chain=state_code_map.get` (a bound method, lambda-free-legal) reads
    # this map below -- a miss returns `None`, same as pluck's own
    # missing-path handling, not a `mapping.get(value, value)` passthrough.
    state_code_map: dict[str, str] = (
        {s.inc_code: s.inc_code for s in states} | {s.inc_name: s.inc_code for s in states} | DC_SUPPLEMENT
    )
    # A single multi-URL call needs a PARTIAL-failure check, not just an
    # empty-list check -- one country's request 500ing still leaves `states`
    # non-empty. Checking for a representative entry from BOTH countries
    # catches that case too. Fail fast: one ASCII line to stderr, exit 1.
    if "California" not in state_code_map or "Ontario" not in state_code_map:
        sys.exit(REFERENCE_API_ERROR)

    region = state_code_map.get(region, region)
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    league_urls = [f"{BASE}/{sport}/teams" for _, sport in SPORTS]
    leagues = await League.incorp(inc_url=league_urls, rec_path="sports.0", timeout=8)

    # Drill 1: T5's `inc_parent`/`inc_child` shape, reused once per league in a
    # plain loop -- `lg` is a single `League` instance per call, so the
    # `{sport}/{league}` URL segments come straight off its own attributes as an
    # f-string template instead of a build-time composite-path reducer.
    teams: list[Team] = []
    for lg in leagues:
        part = await Team.incorp(
            inc_parent=lg,
            inc_child="leagues.teams.team.id",
            inc_url=f"{BASE}/{lg.slug}/{lg.leagues[0].slug}/teams/{{}}",
            rec_path="team",
            inc_code="uid",  # globally unique ("s:20~l:28~t:24") -- team.id collides across leagues
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

    # Attribute equality, zero brand strings -- ESPN has no server-side filter for this.
    matched = [t for t in teams if t.venue_state == region]
    if not matched:
        sys.exit(f"No {region} teams found - try 'NY', 'TX', or 'ON'.")

    names = ", ".join(f"{t.league} {t.inc_name}" for t in matched)
    print(f"OK: Found {len(matched)} {region} team(s): {names}")

    # Drill 2: T5's shape again, once per matched TEAM in a plain loop -- every
    # matched team's roster drills straight into `Player` rows. `slugs` is a
    # small runtime dict built from the `League` rows themselves (never
    # hardcoded), keyed by the abbreviation each `Team` was stamped with in
    # Drill 1, combined into the `"{sport}/{league}"` path the URL template needs.
    slugs: dict[str, str] = {lg.leagues[0].abbreviation: f"{lg.slug}/{lg.leagues[0].slug}" for lg in leagues}

    players: list[Player] = []
    for team in matched:
        roster = await Player.incorp(
            inc_parent=team,
            inc_child="id",
            inc_url=f"{BASE}/{slugs[team.league]}/teams/{{}}?enable=roster",
            rec_path="team.athletes",
            inc_code="uid",  # globally unique across leagues (verified live)
            inc_name="fullName",
            conv_dict={
                # One `active` flag -- the reports below filter on it. Output
                # key == source key, so `inc(bool, ...)` (a TYPE coercion, not a
                # `calc()` transform) is the right primitive here.
                "active": inc(bool, default=False),
                "salary": calc(int, "contract.salary", default=0, target_type=int),
                "tenure": calc(functools.partial(max, 1), "experience.years", default=1, target_type=int),
                "age": calc(int, "age", default=0, target_type=int),
                "pos": calc(str, "position.abbreviation", default="-", target_type=str),
                "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
                "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
                # Derived from the pre-coerced fields above (insertion order).
                # "turned_pro_at" reads "age"/"tenure"; "salary_per_year" reads
                # "salary"/"tenure" and is zero-safe since tenure >= 1.
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
        # Data-semantics branch (does this league publish salaries at all),
        # not a per-row missing-value guard -- kept.
        payroll_note = f", payroll ${payroll_total:,.0f}" if salary_known_total else ""
        print(
            f"{league:<5} {team_count} team(s), {len(league_players)} players ({league_active_count} active), "
            f"salary known {salary_known_total}/{league_active_count}{payroll_note}"
        )

    # Filter to active players first, then sort/compare on the raw fields -- the
    # one `active` flag is the report-time gate (inactive org players carry
    # stats but never belong in a top-10).
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

    # Attribute equality -- no brand-string tables. `birthPlace.state` on
    # players uses 2-letter codes already (verified live), so it compares
    # directly against the normalized `region` (active players only).
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
