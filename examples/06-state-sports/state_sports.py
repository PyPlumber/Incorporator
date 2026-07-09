"""
Tutorial 6 -- State Sports: Two Chained Parent-Child Drills (ESPN)
-------------------------------------------------------------------
Companion script for `examples/06-state-sports/README.md`.

Pick a US state (or Canadian province) code, discover every team whose
venue sits there across NFL / NBA / MLB / NHL via ESPN's public site API,
then drill every matched team's roster -- two chained whole-list
`inc_parent` drills (league -> team, team -> roster), the exact shape
Tutorial 5 introduced, reused twice over. A pure one-shot script: no
time-windowed orchestration, no files read or written at runtime,
ASCII-only stdout.
Modeled directly on `examples/appendix/pokeapi-etl/pokeapi_etl_calc.py`'s
shape -- shallow discovery -> `inc_parent` deep enrichment -> `calc()`
array reductions -> `name_chg` hygiene -> plain print tables reading
precomputed attributes.

`conv_dict` exercises all four converters:
  * `pluck()`    -- nested lifts (`franchise.venue.address.*`,
                    `franchise.venue.fullName`).
  * `calc()`     -- URL-path array reductions (`build_team_paths`), league
                    derivation (`league_from_links`), per-team roster
                    reductions (`extract_active_players`, `team_payroll`,
                    ...).
  * `inc()`      -- pure type coercion (`id` -> `int`), no transform.
  * `link_to()`  -- a build-time join from every roster row back to its
                    own `Team` instance (`team_ref`), wrapped in `calc()`
                    so the output key differs from the join's source key
                    (`"uid"`, which the PK bind still needs untouched).

**A list-valued `inc_child` leaf does not auto-flatten across parents in
one BFS segment.** `extract_parent_data(leagues, "team_paths")` where
`team_paths` is `list[str]` returns a list-of-lists (one list per league),
which would corrupt the `{}`-template URL step. `build_team_paths` returns
`list[dict]` (`[{"path": ...}, ...]`) instead of bare strings so the drill
can use a SECOND dotted segment (`inc_child="team_paths.path"`) -- that
second segment's list-of-lists intermediate DOES fan out correctly.

**A conv_dict-computed nested list is re-inferred into Pydantic
sub-models, same as a raw one.** `extract_active_players`'s returned
list-of-dicts becomes `roster.players[0].salary` (attribute access), never
`roster.players[0]["salary"]` -- the framework's own dynamic-schema
inference runs on every conv_dict output, not just raw API fields.

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
import sys
from typing import Any

from incorporator import Incorporator, calc, inc, link_to, pluck

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


# ==========================================
# DECLARATIVE ETL FUNCTIONS
# ==========================================
def build_team_paths(sport_slug: Any, leagues_array: Any) -> list[dict[str, str]]:
    """Reduce a league row's own nested `leagues[0].teams` array into a list
    of single-key `{"path": ...}` dicts -- see the module docstring's
    BFS-flatten note for why a list of bare strings would silently corrupt
    the next drill's URL-templating step."""
    if not isinstance(leagues_array, list) or not leagues_array:
        return []
    league = leagues_array[0]
    if not isinstance(league, dict):
        return []
    league_slug = league.get("slug", "")
    teams = league.get("teams", [])
    if not isinstance(teams, list):
        return []
    paths = []
    for entry in teams:
        team = entry.get("team") if isinstance(entry, dict) else None
        team_id = team.get("id") if isinstance(team, dict) else None
        if team_id is None:
            continue
        paths.append({"path": f"{sport_slug}/{league_slug}/teams/{team_id}"})
    return paths


def league_from_links(links: Any) -> str | None:
    """Every team detail/roster row's own `links[0]['href']` embeds the
    league slug (`espn.com/{slug}/team/...`), stable across all four
    leagues (verified live) -- derived from the row's own data, no lookup
    table."""
    if not isinstance(links, list) or not links:
        return None
    href = links[0].get("href") if isinstance(links[0], dict) else None
    if not isinstance(href, str):
        return None
    parts = href.split("/")
    return parts[3].upper() if len(parts) > 3 else None


def build_roster_path(league: Any, team_id: Any) -> str:
    """`league` is THIS row's own, already-mutated `league` conv_dict entry
    -- insertion order guarantees it ran first -- resolved back to ESPN's
    fixed URL sport segment via the closed `LEAGUE_SPORT_SLUGS` map."""
    sport = LEAGUE_SPORT_SLUGS.get(league, "")
    return f"{sport}/{str(league).lower()}/teams/{team_id}?enable=roster"


def league_from_team_ref(team_ref: Any) -> str | None:
    """Reads `league` off the already-built `Team` instance the `team_ref`
    join landed on -- avoiding a second `league_from_links` derivation
    against the roster payload; this is the entire payoff of the
    `link_to(matched)` join."""
    return getattr(team_ref, "league", None)


def team_payroll(athletes: Any) -> float:
    """Sum published salaries across active players -- MLB/NHL contribute
    0.0 (no published salaries in this feed), not a crash."""
    if not isinstance(athletes, list):
        return 0.0
    total = 0.0
    for athlete in athletes:
        if not isinstance(athlete, dict) or not athlete.get("active"):
            continue
        salary = (athlete.get("contract") or {}).get("salary")
        if salary is not None:
            total += salary
    return total


def team_salary_known_count(athletes: Any) -> int:
    """Count active players whose salary is published -- pairs with
    `team_payroll` for the league-summary board's coverage fraction."""
    if not isinstance(athletes, list):
        return 0
    count = 0
    for athlete in athletes:
        if not isinstance(athlete, dict) or not athlete.get("active"):
            continue
        if (athlete.get("contract") or {}).get("salary") is not None:
            count += 1
    return count


def team_active_count(athletes: Any) -> int:
    """Count active roster players -- MLB's `athletes` array is the whole
    ~250-person organization, not the 26-man active roster; `active_count`
    already reflects the active-only filter this board needs."""
    if not isinstance(athletes, list):
        return 0
    return sum(1 for athlete in athletes if isinstance(athlete, dict) and athlete.get("active"))


def extract_active_players(athletes: Any, league: Any, team_name: Any) -> list[dict[str, Any]]:
    """Array-reduction workhorse: one precomputed dict per ACTIVE athlete,
    filtering MLB's whole-organization roster quirk down to the active
    roster in the same pass. `league` / `team_name` are this row's OWN
    already-computed conv_dict values (read via insertion order, single-
    segment keys), embedded per player so the boards below need zero
    further derivation or team-level join."""
    if not isinstance(athletes, list):
        return []
    players: list[dict[str, Any]] = []
    for athlete in athletes:
        if not isinstance(athlete, dict) or not athlete.get("active"):
            continue
        contract = athlete.get("contract") or {}
        salary = contract.get("salary")
        experience = athlete.get("experience") or {}
        tenure = experience.get("years")
        position = athlete.get("position") or {}
        birth_place = athlete.get("birthPlace") or {}
        age = athlete.get("age")
        salary_per_year = salary / max(tenure or 1, 1) if salary is not None else None
        turned_pro_at = age - (tenure or 0) if age is not None else None
        birth_city = birth_place.get("city")
        birth_state = birth_place.get("state")
        players.append(
            {
                "id": athlete.get("id"),
                "name": athlete.get("fullName"),
                "league": league,
                "team_name": team_name,
                "pos": position.get("abbreviation"),
                "salary": salary,
                "tenure": tenure,
                "birth_city": birth_city,
                "birth_state": birth_state,
                "salary_per_year": salary_per_year,
                "turned_pro_at": turned_pro_at,
            }
        )
    return players


async def fetch_state_code_map() -> dict[str, str]:
    """Build the full-name -> 2-letter-code map from CountriesNow (US + Canada).

    One multi-URL `incorp()` call replaces a per-country loop -- `inc_url`
    accepts `str | list[str]` and fans both requests out under a single
    `IncorporatorList`. This reference map must exist before any
    venue-state normalization can run -- a silent empty map would produce a
    filter that matches nothing with no explanation why. Fail fast instead:
    one ASCII error line, exit non-zero. A single multi-URL call also means
    a PARTIAL failure (one country's request 500s, the other succeeds)
    still leaves `states` non-empty -- checking for a representative entry
    from *both* countries catches that case, not just an empty list.
    """
    states = await StateRef.incorp(
        inc_url=COUNTRIESNOW_URLS,
        rec_path="data.states",
        inc_code="state_code",
        inc_name="name",
        timeout=8,
    )
    mapping: dict[str, str] = dict(DC_SUPPLEMENT)
    for state in states:
        mapping[state.inc_name] = state.inc_code
    if not states or "California" not in mapping or "Ontario" not in mapping:
        print(REFERENCE_API_ERROR)
        sys.exit(1)
    return mapping


async def discover_teams(state_code_map: dict[str, str]) -> Any:
    """Drill 1: league discovery -> whole-list team-detail fan-out.

    One multi-URL `League.incorp()` call fetches all four leagues' team
    lists in a single request set; `Team.incorp(inc_parent=leagues, ...)`
    is T5's canonical whole-list `inc_parent`/`inc_child` fan-out, reused
    verbatim, drilling every team across all four leagues concurrently in
    one call. Returns the built `Team` `IncorporatorList`, unfiltered --
    the region filter runs in `main()`, a plain Python comprehension, since
    ESPN's detail endpoint has no server-side `state=` filter to push it
    into.
    """
    to_code = functools.partial(to_state_code, state_code_map)
    league_urls = [f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams" for _, sport in SPORTS]

    leagues = await League.incorp(
        inc_url=league_urls,
        rec_path="sports.0",
        conv_dict={"team_paths": calc(build_team_paths, "slug", "leagues", default=[])},
        timeout=8,
    )
    if not leagues:
        print("ERROR: no league team-lists reachable - aborting.")
        for entry in leagues.rejects:
            print(f"   - {entry}")
        sys.exit(1)
    if leagues.rejects:
        print(f"WARN: {len(leagues.rejects)} league team-list request(s) failed:")
        for entry in leagues.rejects:
            print(f"   - {entry}")

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
    if teams.rejects:
        print(f"WARN: {len(teams.rejects)} team-detail request(s) failed:")
        for entry in teams.rejects:
            print(f"   - {entry}")
    return teams


def print_league_summary(region: str, rosters: list[Any]) -> None:
    print(f"\n{region} across NFL / NBA / MLB / NHL")
    print("=" * 70)
    for league, _ in SPORTS:
        league_rosters = [r for r in rosters if r.league == league]
        if not league_rosters:
            continue
        active_total = sum(r.active_count for r in league_rosters)
        salary_known_total = sum(r.salary_known for r in league_rosters)
        payroll_total = sum(r.payroll for r in league_rosters)
        payroll_note = f", payroll ${payroll_total:,.0f}" if salary_known_total else ""
        print(
            f"{league:<5} {len(league_rosters)} team(s), {active_total} active players, "
            f"salary known {salary_known_total}/{active_total}{payroll_note}"
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
            f"{i:<5}{p.name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{(p.pos or '-'):<5}"
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
        print(f"{i:<5}{p.name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{p.tenure:>7}{turned_pro!s:>14}")


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
        print(f"{p.name[:23]:<24}{p.league:<5}{p.team_name[:21]:<22}{born[:27]:<28}")


async def main() -> None:
    print("Fetching state/province reference data (CountriesNow)...")
    state_code_map = await fetch_state_code_map()

    region_arg = sys.argv[1] if len(sys.argv) > 1 else "CA"
    region = to_state_code(state_code_map, region_arg)
    print(f"Discovering {region}'s teams across NFL / NBA / MLB / NHL (ESPN site API)...")

    teams = await discover_teams(state_code_map)

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

    # Drill 2: team -> roster, T5's whole-list `inc_parent` fan-out reused
    # a second time. `matched` must stay a strong local reference for the
    # entire call below -- `link_to(matched)` builds its registry off
    # `matched`'s own `inc_dict`, and `inc_dict` is a `WeakValueDictionary`.
    rosters = await TeamRoster.incorp(
        inc_parent=matched,
        inc_child="roster_path",
        inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            # link_to(): build-time join back to Drill 1's `Team` instances
            # on the SHARED "uid" field. Wrapped in calc() so the output key
            # ("team_ref") differs from the join's SOURCE key ("uid") --
            # calc() reads via DataPath, not d.get(output_key), so the raw
            # "uid" field PK-binding (inc_code="uid") still needs stays
            # untouched.
            "team_ref": calc(link_to(matched), "uid"),
            # calc(): reads "league" off the already-linked Team instance --
            # the entire payoff of the join above.
            "league": calc(league_from_team_ref, "team_ref", default=None, target_type=str),
            # pluck(): a genuinely nested lift, distinct from anything
            # `team_ref` already carries -- the venue's own display name.
            "venue_name": pluck("franchise.venue.fullName"),
            # inc(): pure type coercion, no transform -- ESPN's numeric "id"
            # arrives as a JSON string; coerce it to a real int at build
            # time so nothing downstream ever isinstance()-checks it.
            "id": inc(int, default=0),
            # calc(): per-team summary aggregates the league-summary board
            # reads directly -- computed BEFORE "athletes" is overwritten
            # in place below (insertion order).
            "payroll": calc(team_payroll, "athletes", default=0.0, target_type=float),
            "salary_known": calc(team_salary_known_count, "athletes", default=0, target_type=int),
            "active_count": calc(team_active_count, "athletes", default=0, target_type=int),
            # calc(): the array-reduction workhorse, computed in place under
            # the original "athletes" key (pokeapi's own "stats" precedent),
            # then renamed via name_chg below -- avoids ever needing to
            # excl_lst "athletes" (excl_lst runs BEFORE conv_dict, so it
            # can't drop a key this same pass still needs to read).
            "athletes": calc(extract_active_players, "athletes", "league", "displayName", default=[]),
        },
        name_chg=[("athletes", "players")],
        excl_lst=["record", "logos", "nextEvent", "standingSummary"],
        timeout=10,
    )
    if rosters.rejects:
        print(f"WARN: {len(rosters.rejects)} roster request(s) failed:")
        for entry in rosters.rejects:
            print(f"   - {entry}")

    total_players = sum(len(r.players) for r in rosters)
    print(f"OK: Loaded {total_players} active players across {len(rosters)} teams.")

    print_league_summary(region, rosters)
    all_players = [p for team in rosters for p in team.players]
    print_paycheck_board(all_players)
    print_veterans_board(all_players)
    print_homegrown_board(region, all_players)

    print("\nGoing further: cross-sport tallest/heaviest splits and calc_all() dense-rank")
    print("leaderboards both live in the README.")


if __name__ == "__main__":
    asyncio.run(main())
