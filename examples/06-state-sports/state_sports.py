"""
Tutorial 6 -- State Sports: Per-League Drills, Then a Third In-Memory `incorp()` (ESPN)
----------------------------------------------------------------------------------------
Companion script for `examples/06-state-sports/README.md`.

Pick a US state (or Canadian province) code, discover every team whose
venue sits there across NFL / NBA / MLB / NHL via ESPN's public site API,
drill every matched team's roster, then hand the active-player rows off to
a THIRD `incorp()` call -- `Player.incorp(payload_list=...)`, a network-free
passthrough over data that's already sitting in memory. A pure one-shot
script: no time-windowed orchestration, no files read or written at
runtime, ASCII-only stdout. Modeled directly on
`examples/appendix/pokeapi-etl/pokeapi_etl_calc.py`'s inline shape --
discovery -> `inc_parent` drill -> another drill -> print tables reading
precomputed attributes, all in one linear `main()`.

**Per-league drills, T5's shape reused once per vertical.** ESPN's team
detail/roster payloads only ever live at a fixed `{sport}/{league}/teams/{id}`
URL, and that `{sport}/{league}` pair can't be recovered from a fetched row
-- `conv_dict` only ever sees the response, never the request that produced
it. Rather than build a composite path string per team (a `calc()` reducer
working around a whole-list drill), this tutorial drills `Team.incorp()`
**once per `League` row**, in a plain loop, reading `lg.slug` /
`lg.leagues[0].slug` straight off that single parent to build the `inc_url`
f-string template. `inc_parent` accepts a single `Incorporator` instance
just as readily as a whole `IncorporatorList` (see
`incorporator/base.py`'s `incorp()` signature) -- this is the *same*
primitive T5 introduced, applied to one parent at a time instead of a whole
list, and it structurally sidesteps the BFS-fanout quirk a single-parent,
list-valued leaf would otherwise hit (see the README's "full dotted
`inc_child` path" section).

`conv_dict` still exercises all four converters across the three calls:
  * `pluck()`    -- nested lifts (`franchise.venue.address.*`, `displayName`).
  * `calc()`     -- the reference-map normalization, the roster-join
                    read-through, and every per-player field on the
                    in-memory `Player` passthrough.
  * `inc()`      -- not used in this revision; every remaining coercion is a
                    dotted/renamed path, which is `calc(TYPE, ...)` territory
                    (`pluck()` has no `default=`, and `inc()` can't drill a
                    nested key -- see `docs/api_atlas.md`'s converter table).
  * `link_to()`  -- a build-time join from every roster row back to its own
                    `Team` instance (`team_ref`), wrapped in `calc()` so the
                    output key differs from the join's source key (`"uid"`).

Player rows are built via `Player.incorp(payload_list=roster_payload)` --
the "Build rows from memory" recipe in `docs/api_atlas.md`. `roster_payload`
is a plain Python comprehension: active-only filter, parent (`league`/
`team_name`) stamp, and `athlete.model_dump()` to flatten each re-inferred
Pydantic sub-model back to a dict, all in one pass. Every board below reads
`Player` attributes directly -- zero missing-data conditionals; every
printed field carries a build-time `calc(..., default=...)`.

Run with:
    python examples/06-state-sports/state_sports.py      # defaults to "CA" -- edit main("CA") in the
                                                           # entry block below to try another region
"""

import asyncio
import functools
import operator
import sys

from incorporator import Incorporator, calc, link_to, pluck

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


class TeamRoster(Incorporator):
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

    # Drill 1: T5's `inc_parent`/`inc_child` shape, reused once per league --
    # `lg` is a single `League` instance per call, so the `{sport}/{league}`
    # URL segments come straight off its own attributes as an f-string
    # template instead of a build-time composite-path reducer.
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

    # Drill 2: T5's shape again, once per league-GROUP of matched teams --
    # `league_slugs` is a small runtime dict built from the `League` rows
    # themselves (never hardcoded), keyed by the abbreviation each `Team`
    # was stamped with in Drill 1.
    league_slugs = {lg.leagues[0].abbreviation: (lg.slug, lg.leagues[0].slug) for lg in leagues}
    groups: dict[str, list[Team]] = {}
    for t in matched:
        groups.setdefault(t.league, []).append(t)

    # `matched` must stay a strong local reference through this whole loop --
    # `link_to(matched)` builds its registry off `matched`'s items' own
    # `inc_code`, and `Team.inc_dict` is a `WeakValueDictionary`.
    rosters: list[TeamRoster] = []
    for lg_abbr, group in groups.items():
        sport_slug, league_slug = league_slugs[lg_abbr]
        part = await TeamRoster.incorp(
            inc_parent=group,
            inc_child="id",
            inc_url=f"{BASE}/{sport_slug}/{league_slug}/teams/{{}}?enable=roster",
            rec_path="team",
            inc_code="uid",
            inc_name="displayName",
            conv_dict={
                # link_to(): build-time join back to Drill 1's `Team`
                # instances on the shared "uid" field, wrapped in calc() so
                # the output key ("team_ref") differs from the join's
                # source key ("uid").
                "team_ref": calc(link_to(matched), "uid"),
                # calc(): reads "league" off the already-linked Team
                # instance -- the entire payoff of the join above.
                "league": calc(operator.attrgetter("league"), "team_ref", default=None, target_type=str),
                "team_name": pluck("displayName"),
            },
            excl_lst=["record", "logos", "nextEvent", "standingSummary"],
            timeout=10,
        )
        rosters.extend(part)

    # Flatten each roster's active athletes, stamping team context, for the
    # in-memory build below.
    roster_payload = [
        {**athlete.model_dump(), "league": team.league, "team_name": team.team_name}
        for team in rosters
        for athlete in team.athletes
        if athlete.active  # MLB's `athletes` array is the whole organization, not the active roster
    ]
    players = await Player.incorp(
        payload_list=roster_payload,
        inc_code="uid",  # globally unique across leagues (verified live) -- no league-qualifying calc needed
        inc_name="fullName",
        conv_dict={
            # Coercions first -- every derived field below reads these
            # already-mutated, never-None values (insertion order). "tenure"
            # floors both a missing value and a genuine zero to 1 -- a player
            # who's on a roster has been there at least one year.
            "salary": calc(int, "contract.salary", default=0, target_type=int),
            "tenure": calc(functools.partial(max, 1), "experience.years", default=1, target_type=int),
            "age": calc(int, "age", default=0, target_type=int),
            "pos": calc(str, "position.abbreviation", default="-", target_type=str),
            "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
            "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
            # Derived from the pre-coerced fields above. "turned_pro_at" reads
            # "age"/"tenure" (a missing age now surfaces as a negative sentinel,
            # since tenure is never zero); "salary_per_year" reads "salary"/
            # "tenure" and is zero-safe by construction since tenure >= 1.
            "turned_pro_at": calc(operator.sub, "age", "tenure", default=0, target_type=int),
            "salary_per_year": calc(operator.truediv, "salary", "tenure", default=0.0, target_type=float),
        },
    )

    print(f"OK: Loaded {len(players)} active players across {len(rosters)} teams.")

    print(f"\n{region} across NFL / NBA / MLB / NHL")
    print("=" * 70)
    for league, _ in SPORTS:
        league_players = [p for p in players if p.league == league]
        if not league_players:
            continue
        team_count = len({p.team_name for p in league_players})
        salary_known_total = sum(1 for p in league_players if p.salary > 0)
        payroll_total = sum(p.salary for p in league_players)
        # Data-semantics branch (does this league publish salaries at all),
        # not a per-row missing-value guard -- kept.
        payroll_note = f", payroll ${payroll_total:,.0f}" if salary_known_total else ""
        print(
            f"{league:<5} {team_count} team(s), {len(league_players)} active players, "
            f"salary known {salary_known_total}/{len(league_players)}{payroll_note}"
        )

    pool = sorted(
        (p for p in players if p.league in SALARY_LEAGUES and p.salary > 0), key=lambda p: p.salary, reverse=True
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

    pool = sorted(players, key=lambda p: p.tenure, reverse=True)
    print("\nVETERANS BOARD (all four leagues)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'TENURE':>7}{'TURNED-PRO-AT':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        print(f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{p.turned_pro_at:>14}")

    # Pure attribute equality -- no brand-string tables. `birthPlace.state` on
    # players uses 2-letter codes already (verified live), so it compares
    # directly against the normalized `region`.
    heroes = [p for p in players if p.birth_state == region]
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
