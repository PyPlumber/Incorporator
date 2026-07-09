"""
Tutorial 6 -- State Sports: Two Drills, Then a Third In-Memory `incorp()` (ESPN)
---------------------------------------------------------------------------------
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

`conv_dict` still exercises all four converters across the three calls:
  * `pluck()`    -- nested lifts (`franchise.venue.address.*`, `displayName`).
  * `calc()`     -- URL-path array reduction (`build_team_paths`), league
                    derivation, the roster-join read-through, and every
                    per-player field on the in-memory `Player` passthrough.
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

**A list-valued `inc_child` leaf does not auto-flatten across parents in
one BFS segment.** `build_team_paths` returns `list[dict]`
(`[{"path": ...}, ...]`) instead of bare strings so the drill can use a
SECOND dotted segment (`inc_child="team_paths.path"`) -- that second
segment's list-of-lists intermediate DOES fan out correctly.

**URL taxonomy (which of ESPN's 4 fixed `sport`/`league` path segments to
hit) cannot be recovered from a fetched row** -- `conv_dict` only ever
sees the response, never the request that produced it.
`LEAGUE_SPORT_SLUGS` is a small, honest, closed-vocabulary constant for
ESPN's own fixed URL scheme -- not a reintroduction of the
brand-string/city-alias tables an earlier version of this tutorial
deleted (those were about team *identity/location* labels; this is only
about which of 4 fixed URL path segments to request).

Run with:
    python examples/06-state-sports/state_sports.py            # defaults to "CA"
    python examples/06-state-sports/state_sports.py ON
    python examples/06-state-sports/state_sports.py "California"
"""

import asyncio
import functools
import operator
import sys
from typing import Any

from incorporator import Incorporator, calc, link_to, pluck

SPORTS = [
    ("NFL", "football/nfl"),
    ("NBA", "basketball/nba"),
    ("MLB", "baseball/mlb"),
    ("NHL", "hockey/nhl"),
]

# ESPN's team detail/roster payloads only ever live at a fixed
# {sport}/{league}/teams/{id} path -- a row's own `links` array reveals the
# LEAGUE label (`league_from_links`) but has no inverse back to the SPORT
# path segment ESPN's URL scheme demands. Four entries, one per fixed
# league -- URL-taxonomy plumbing, not a brand/location alias table.
LEAGUE_SPORT_SLUGS = {"NFL": "football", "NBA": "basketball", "MLB": "baseball", "NHL": "hockey"}

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


def build_team_paths(sport_slug: str, leagues_array: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Reduce a league row's own nested `leagues[0].teams` array into a list
    of single-key `{"path": ...}` dicts (see the module docstring's
    BFS-flatten note). No isinstance ladder -- a malformed `leagues_array`
    (a missing `leagues[0]`, a non-list `teams`) raises, and `calc()`'s own
    exception-fallback resolves the WHOLE league's `team_paths` to
    `default=[]` (no per-team salvage). Acceptable for a tutorial."""
    league = leagues_array[0]
    return [{"path": f"{sport_slug}/{league['slug']}/teams/{t['team']['id']}"} for t in league["teams"]]


def league_from_links(links: list[dict[str, str]]) -> str:
    """Every team detail/roster row's own `links[0]['href']` embeds the
    league slug (`espn.com/{slug}/team/...`), stable across all four
    leagues (verified live) -- derived from the row's own data, no lookup
    table. Malformed `links` raises; `calc()`'s exception-fallback resolves
    to `default=None`."""
    return links[0]["href"].split("/")[3].upper()


def build_roster_path(league: str, team_id: str) -> str:
    """`league` is THIS row's own, already-mutated `league` conv_dict entry
    -- insertion order guarantees it ran first -- resolved back to ESPN's
    fixed URL sport segment via the closed `LEAGUE_SPORT_SLUGS` map."""
    return f"{LEAGUE_SPORT_SLUGS.get(league, '')}/{str(league).lower()}/teams/{team_id}?enable=roster"


def salary_per_year(salary: float | None, tenure: int) -> str:
    """Preformatted display string -- never sorted or aggregated. `tenure`
    defaults to `0` upstream (a real, non-garbage value), so `calc()`'s own
    all-inputs-garbage short-circuit never fires on the common
    salary-missing case (MLB/NHL publish no salaries at all) -- the explicit
    `None` guard, not `calc()`'s default, is what keeps that the expected
    common case quiet instead of a per-row exception-fallback warning."""
    if salary is None:
        return "-"
    return f"${salary / (tenure or 1):,.0f}"


def turned_pro_at(age: int | None, tenure: int) -> str:
    """Preformatted display string, same reasoning as `salary_per_year` --
    a missing `age` is common enough (any row ESPN doesn't publish it for)
    that leaning on `calc()`'s exception-fallback would warn on every one."""
    if age is None:
        return "-"
    return str(age - (tenure or 0))


def print_league_summary(region: str, all_players: list[Any]) -> None:
    print(f"\n{region} across NFL / NBA / MLB / NHL")
    print("=" * 70)
    for league, _ in SPORTS:
        league_players = [p for p in all_players if p.league == league]
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


def print_paycheck_board(all_players: list[Any]) -> None:
    pool = sorted(
        (p for p in all_players if p.league in SALARY_LEAGUES and p.salary > 0), key=lambda p: p.salary, reverse=True
    )

    print("\nPAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'POS':<5}{'TENURE':>7}{'SALARY':>14}{'$/YR-TENURE':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        print(
            f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.pos:<5}"
            f"{p.tenure!s:>7}{f'${p.salary:,.0f}':>14}{p.salary_per_year:>14}"
        )


def print_veterans_board(all_players: list[Any]) -> None:
    pool = sorted(all_players, key=lambda p: p.tenure, reverse=True)

    print("\nVETERANS BOARD (all four leagues)")
    header = f"{'RANK':<5}{'PLAYER':<24}{'LG':<5}{'TEAM':<22}{'TENURE':>7}{'TURNED-PRO-AT':>14}"
    print(header)
    print("-" * len(header))
    for i, p in enumerate(pool[:10], start=1):
        print(f"{i:<5}{p.inc_name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{p.turned_pro_at!s:>14}")


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
    states = await StateRef.incorp(
        inc_url=COUNTRIESNOW_URLS,
        rec_path="data.states",
        inc_code="state_code",
        inc_name="name",
        timeout=8,
    )
    state_code_map: dict[str, str] = dict(DC_SUPPLEMENT, **{s.inc_name: s.inc_code for s in states})
    # A single multi-URL call needs a PARTIAL-failure check, not just an
    # empty-list check -- one country's request 500ing still leaves `states`
    # non-empty. Checking for a representative entry from BOTH countries
    # catches that case too. Fail fast: one ASCII line to stderr, exit 1.
    if "California" not in state_code_map or "Ontario" not in state_code_map:
        sys.exit(REFERENCE_API_ERROR)

    region_arg = sys.argv[1] if len(sys.argv) > 1 else "CA"
    region = to_state_code(state_code_map, region_arg)
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    to_code = functools.partial(to_state_code, state_code_map)
    league_urls = [f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams" for _, sport in SPORTS]

    leagues = await League.incorp(
        inc_url=league_urls,
        rec_path="sports.0",
        conv_dict={"team_paths": calc(build_team_paths, "slug", "leagues", default=[])},
        timeout=8,
    )

    teams = await Team.incorp(
        inc_parent=leagues,
        inc_child="team_paths.path",
        inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
        rec_path="team",
        inc_code="uid",  # globally unique ("s:20~l:28~t:24") -- team.id collides across leagues
        inc_name="displayName",
        conv_dict={
            "venue_city": pluck("franchise.venue.address.city"),
            "venue_state": pluck("franchise.venue.address.state", chain=to_code),
            "league": calc(league_from_links, "links", default=None, target_type=str),
            "roster_path": calc(build_roster_path, "league", "id", default="", target_type=str),
        },
        timeout=8,
    )

    no_venue_total = sum(1 for t in teams if t.venue_state is None)
    if no_venue_total:
        print(f"WARN: {no_venue_total} team(s) had no reachable venue address - excluded from the region filter.")

    # The filter: attribute equality, zero brand strings. There is no
    # `state=` query parameter on ESPN's detail endpoint and no bulk
    # "every team whose venue is in state X" endpoint -- this genuinely
    # can't be pushed server-side, so an app-level comprehension over the
    # already-built `Team` list is the correct (and only) option here.
    matched = [t for t in teams if t.venue_state == region]
    if not matched:
        print(f"\nNo {region} teams found. Try a 2-letter US state/DC code ('NY', 'TX') or a Canadian province ('ON').")
        print("See the README's 'brand labels vs data attributes' section for how this filter works.")
        return

    names = ", ".join(f"{t.league} {t.inc_name}" for t in matched)
    print(f"OK: Found {len(matched)} {region} team(s): {names}")

    # Drill 2: team -> roster, T5's whole-list `inc_parent` fan-out reused a
    # second time. `matched` must stay a strong local reference for this
    # entire call -- `link_to(matched)` builds its registry off `matched`'s
    # own `inc_dict`, and `inc_dict` is a `WeakValueDictionary`.
    rosters = await TeamRoster.incorp(
        inc_parent=matched,
        inc_child="roster_path",
        inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            # link_to(): build-time join back to Drill 1's `Team` instances
            # on the shared "uid" field, wrapped in calc() so the output key
            # ("team_ref") differs from the join's source key ("uid").
            "team_ref": calc(link_to(matched), "uid"),
            # calc(): reads "league" off the already-linked Team instance --
            # the entire payoff of the join above.
            "league": calc(operator.attrgetter("league"), "team_ref", default=None, target_type=str),
            "team_name": pluck("displayName"),
        },
        excl_lst=["record", "logos", "nextEvent", "standingSummary"],
        timeout=10,
    )

    # Third incorp() call: rows already sit in memory (each roster's own
    # `athletes` array), so no network call belongs here. `rosters` stays a
    # strong local reference until this comprehension finishes reading
    # `team.athletes` off each row -- same WeakValueDictionary lifetime rule
    # `matched` needed for `link_to(matched)` above.
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
            "salary": calc(float, "contract.salary", default=0.0, target_type=float),
            "tenure": calc(int, "experience.years", default=0, target_type=int),
            "pos": calc(str, "position.abbreviation", default="-", target_type=str),
            "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
            "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
            "salary_per_year": calc(
                salary_per_year, "contract.salary", "experience.years", default="-", target_type=str
            ),
            "turned_pro_at": calc(turned_pro_at, "age", "experience.years", default="-", target_type=str),
        },
    )
    print(f"OK: Loaded {len(players)} active players across {len(rosters)} teams.")

    print_league_summary(region, players)
    print_paycheck_board(players)
    print_veterans_board(players)
    print_homegrown_board(region, players)

    print("\nGoing further: cross-sport tallest/heaviest splits and calc_all() dense-rank")
    print("leaderboards both live in the README.")


if __name__ == "__main__":
    asyncio.run(main())
