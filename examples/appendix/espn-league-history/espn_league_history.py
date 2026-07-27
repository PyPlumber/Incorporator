"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

A one-shot script (no Watershed) -- every ESPN season is fetched exactly
once, ever. `Owner`/`Standing`/`Matchup`/`DraftPick` are each built with
ONE `incorp(payload_list=...)` call spanning *every* fetched season (never
inside the season-discovery loop), so `calc_all` can roll up all-time owner
aggregates, franchise-season all-play win shares, and franchise-history win/
loss streaks declaratively, in `conv_dict`, instead of hand-written Python
folds. Composite `"{season}:{team_id}"` join keys (`team_key()`) plus
`link_to()` build-time joins replace every hand-built lookup dict; a single
`TeamGame` reshape (one row per team per decided matchup -- the one
unavoidable de-nesting, since ESPN ships matchups as home/away pairs and
every cross-row stat here is team-scoped) unlocks `calc_all` for the
all-play and win/loss-streak computations. Only three genuine N:M
cardinality reductions stay in plain Python -- the pairwise rivalry matrix,
the draft-tendency groupbys, and Franchise Cards' own playoff-appearances
cross-class merge (`Standing` owners x `Matchup` bracket appearances, a
join `calc_all` can't cross) -- everything else is
`inc`/`calc`/`calc_all`/`link_to`, called inline in `main()` right before
each view's own `Cls.incorp(payload_list=...)` + `Cls.export(...)` -- the
return-twin of a Fjord's `outflow(state)`, without a Fjord.

Two auth modes, one pipeline:
- PUBLIC (default): no cookies, demo league 899513, unauthenticated floor
  season 2020 (earlier seasons 401 without cookies and are skipped with a
  printed note).
- PRIVATE: set `ESPN_S2` / `ESPN_SWID` (browser cookies) to unlock a
  private league and the cookie-gated pre-2018 `leagueHistory` endpoint.

Season discovery is server-declared, not brute-forced: one bootstrap fetch
of the current calendar-year season reads `status.previousSeasons` off
the response and unions it with the current year -- that IS the season
list, no floor/ceiling guessing.

Run with:
    python examples/appendix/espn-league-history/espn_league_history.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import operator
import os
import warnings
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from incorporator import Incorporator, IncorporatorList, calc, calc_all, inc, link_to, register_host_penstock

# lm-api-reads.fantasy.espn.com has no known_host_rates() entry -- register
# a polite 1 req/sec throttle for the ~15-20 calls this walkthrough makes.
register_host_penstock("lm-api-reads.fantasy.espn.com", rate_per_sec=1.0)

BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
MODERN_URL = BASE + "/seasons/{season}/segments/0/leagues/{league_id}"
HISTORY_URL = BASE + "/leagueHistory/{league_id}"
PLAYERS_URL = BASE + "/seasons/{season}/players"
VIEWS = ["mTeam", "mMatchupScore", "mSettings", "mDraftDetail"]

DEMO_LEAGUE_ID = "899513"

# Standard ESPN fantasy-football defaultPositionId enumeration (live-verified
# against Davante Adams=3=WR and a D/ST row=16); unknown ids fall back to a
# labelled placeholder rather than crashing.
POSITION_MAP = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}

TOP_N_MOST_DRAFTED = 15


# ---------------------------------------------------------------------------
# Domain-calc helpers -- every one of these is a `conv_dict` callable, not a
# row-building function; the framework calls each once per row (`calc`) or
# once total with whole-column lists (`calc_all`).
# ---------------------------------------------------------------------------


def position_name(position_id: int) -> str:
    return POSITION_MAP.get(position_id, f"POS_{position_id}")


def team_key(season: int, team_id: int | None) -> str | None:
    """Composite join key -- a bare ESPN `team.id` repeats every season, so
    once `Standing` batches across all seasons in one `incorp()` call a bare
    `id` would collide in `inc_dict`. `None` (a playoff bye's missing `away`
    side) stays `None` rather than stringifying into a bogus key."""
    if team_id is None:
        return None
    return f"{season}:{team_id}"


def round2(x: float) -> float:
    return round(x, 2)


def round3(x: float) -> float:
    return round(x, 3)


def abs_diff(a: float, b: float) -> float:
    return abs(a - b)


def win_pct_equiv(wins: int, losses: int, ties: int) -> float:
    """Per-SEASON win rate, ties counted as half a win -- the definition
    Records Book's best/worst-season kinds use. Distinct from
    `win_pct_from_totals` (the all-time, ties-excluded rate Franchise Cards
    sorts by) so the two views never publish different numbers under the
    same field name (fixes bug #1)."""
    games = wins + losses + ties
    return (wins + 0.5 * ties) / games if games else 0.0


def win_pct_from_totals(wins: int, losses: int, ties: int) -> float:
    played = wins + losses + ties
    return wins / played if played else 0.0


def is_champion(final_rank: int) -> bool:
    return final_rank == 1


def is_runner_up(final_rank: int) -> bool:
    return final_rank == 2


def sum_by_group(groups: list[str], values: list[float]) -> list[float]:
    totals: dict[str, float] = defaultdict(float)
    for g, v in zip(groups, values, strict=True):
        totals[g] += v
    return [totals[g] for g in groups]


def count_distinct_by_group(groups: list[str], values: list[int]) -> list[int]:
    seen: dict[str, set[int]] = defaultdict(set)
    for g, v in zip(groups, values, strict=True):
        seen[g].add(v)
    return [len(seen[g]) for g in groups]


def count_true_by_group(groups: list[str], flags: list[bool]) -> list[int]:
    totals: dict[str, int] = defaultdict(int)
    for g, f in zip(groups, flags, strict=True):
        if f:
            totals[g] += 1
    return [totals[g] for g in groups]


def mean_positive_by_group(groups: list[str], values: list[int]) -> list[float]:
    totals: dict[str, list[int]] = defaultdict(list)
    for g, v in zip(groups, values, strict=True):
        if v > 0:
            totals[g].append(v)
    means = {g: sum(vs) / len(vs) for g, vs in totals.items()}
    return [means.get(g, 0.0) for g in groups]


def is_group_max_positive(groups: list[int], values: list[int]) -> list[bool]:
    """True on the row(s) whose `values` entry is the max POSITIVE value
    within its `groups` bucket -- the broadcast this franchise's last-place
    finish flag needs, per season."""
    positive_max: dict[int, int] = {}
    for g, v in zip(groups, values, strict=True):
        if v > 0:
            positive_max[g] = max(positive_max.get(g, 0), v)
    return [bool(v > 0 and v == positive_max.get(g)) for g, v in zip(groups, values, strict=True)]


def all_play_broadcast(
    seasons: list[int], team_keys: list[str], weeks: list[int], scores: list[float], tiers: list[str]
) -> list[float]:
    """One `calc_all` pass, computed once, replacing the O(72 x 556)
    per-standing rescan (bug #2): every regular-season week's full field of
    scores is grouped once, then each team-week's win-share against that
    field is summed into a running per-(season, team) total broadcast onto
    every row of that team-season."""
    week_scores: dict[tuple[int, int], dict[str, float]] = defaultdict(dict)
    for season, tk, week, score, tier in zip(seasons, team_keys, weeks, scores, tiers, strict=True):
        if tier == "NONE":
            week_scores[(season, week)][tk] = score

    season_totals: dict[tuple[int, str], float] = defaultdict(float)
    for season, tk, week, score, tier in zip(seasons, team_keys, weeks, scores, tiers, strict=True):
        if tier != "NONE":
            continue
        others = [v for other_tk, v in week_scores[(season, week)].items() if other_tk != tk]
        season_totals[(season, tk)] += sum(1 for v in others if score > v) + 0.5 * sum(1 for v in others if score == v)

    return [season_totals.get((season, tk), 0.0) for season, tk in zip(seasons, team_keys, strict=True)]


def make_streak_broadcast(target_result: str) -> Any:
    """Factory: returns a `calc_all` reducer computing, for `target_result`
    ("W" or "L"), the RUNNING consecutive-result count for each owner up
    through that row -- chronological across every season. The row where
    the count peaks IS the record; `records_book_rows` selects it with one
    `max()`, recovering that row's season/week for free."""

    def broadcast(standings: list[Any], seasons: list[int], weeks: list[int], results: list[str]) -> list[int]:
        order = [i for i, _ in sorted(enumerate(zip(seasons, weeks, strict=True)), key=operator.itemgetter(1))]
        run_by_owner: dict[str, int] = defaultdict(int)
        streaks = [0] * len(standings)
        for i in order:
            standing = standings[i]
            owner_id = standing.owner.id if standing and standing.owner else None
            if owner_id is None:
                continue
            run_by_owner[owner_id] = run_by_owner[owner_id] + 1 if results[i] == target_result else 0
            streaks[i] = run_by_owner[owner_id]
        return streaks

    return broadcast


def all_play_wins_for(team_key_value: str) -> float:
    tg = TeamGame.inc_dict.get(team_key_value)
    return tg.all_play_expected_wins if tg else 0.0


def luck_delta(wins: int, ties: int, all_play_wins: float) -> float:
    return round2(wins + 0.5 * ties - all_play_wins)


def ppr_points_from_scoring(scoring_items: list[dict[str, Any]]) -> float:
    """`pointsOverrides` may be null OR the key entirely absent -- both
    guarded, plus the third branch (present WITH an override)."""
    ppr_item = next((item for item in scoring_items if item.get("statId") == 53), None)
    if ppr_item is None:
        return 0.0
    overrides = ppr_item.get("pointsOverrides") or {}
    return overrides.get("16", ppr_item.get("points", 0.0))


def division_names_from_schedule(divisions: list[dict[str, Any]]) -> list[str]:
    return [d.get("name", "") for d in divisions]


def drop_raw_settings_blob(_value: Any) -> None:
    """Nulls out a raw ESPN settings sub-dict once every `calc()` entry that
    drills into it has already run (declaration order matters -- this must
    be the LAST conv_dict entry reading that key). `excl_lst` can't do this
    job: it runs in Pass 1, before `conv_dict`'s Pass 2, so it would strip
    `scheduleSettings`/`rosterSettings`/`scoringSettings` before the `calc()`
    entries above ever got to drill into them."""
    return None


def playoff_appearances_by_owner(all_matchups: list[Any]) -> dict[str, set[int]]:
    """The one genuine cross-class merge Franchise Cards needs: which
    seasons each owner appeared in a WINNERS_BRACKET matchup -- `Standing`
    owners joined against `Matchup` bracket appearances, a join `calc_all`
    can't express since it never crosses `incorp()` calls."""
    result: dict[str, set[int]] = defaultdict(set)
    for m in all_matchups:
        if m.playoffTierType != "WINNERS_BRACKET":
            continue
        for standing in (m.home_standing, m.away_standing):
            if standing and standing.owner:
                result[standing.owner.id].add(m.season)
    return result


def cookie_headers(espn_s2: str | None, espn_swid: str | None) -> dict[str, str]:
    """Hand-rolled Cookie header -- `incorp()` has no native `cookies=` kwarg."""
    if espn_s2 and espn_swid:
        return {"Cookie": f"espn_s2={espn_s2}; SWID={espn_swid}"}
    return {}


@contextlib.contextmanager
def quiet_expected_reject():
    """Suppress the framework's per-call reject `UserWarning` + WARNING-level
    log line for a season-discovery probe this loop already handles via its
    own `if not rows:` skip/fallback branch -- narrowly scoped to ONE
    incorp() call, never a blanket warnings.filterwarnings()."""
    fetch_logger = logging.getLogger("incorporator")
    prev_level = fetch_logger.level
    fetch_logger.setLevel(logging.ERROR)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            yield
    finally:
        fetch_logger.setLevel(prev_level)


class Season(Incorporator):
    """One ESPN league-season response -- modern dict-root or historical
    list-root (`rec_path="0"`), same conv_dict either way. Discarded after
    its four sub-collections (teams/schedule/members/draft_picks) and its
    settings dict are read off; never looked up via `inc_dict`."""


class Owner(Incorporator):
    """One league member (GUID + display name), built network-free off every
    season's `members` in ONE batched `payload_list=` call -- `inc_code="id"`
    makes `Owner.inc_dict`/`link_to(Owner)` the graph-map join every other
    class uses instead of a hand-built display-name dict."""


class Standing(Incorporator):
    """One team's season-long record, built network-free off every season's
    `teams` in ONE batched `payload_list=` call so `calc_all` can roll up
    all-time owner aggregates across the whole history in one declarative
    pass. `inc_code="team_key"` (a `"{season}:{team_id}"` composite -- a
    bare team id repeats every season once seasons are batched together)."""


class Matchup(Incorporator):
    """One scheduled/played game, built network-free off every season's
    `schedule` in ONE batched `payload_list=` call. `home`/`away` are left
    nested (never flattened into parallel scalars) -- the framework's
    dynamic schema builder auto-promotes each into an Optional submodel, so
    a playoff bye (`home`-only, no `away` key) surfaces as `m.away is None`
    directly, not an erased `0` sentinel."""


class DraftPick(Incorporator):
    """One draft pick, built network-free off every season's `draft_picks`
    in ONE batched `payload_list=` call, after `Standing`/`PlayerName`."""


class PlayerName(Incorporator):
    """Resolved player name + position, batched per season via ESPN's
    `players_wl` endpoint (season-matched calls are required -- old ids
    don't resolve against modern player universes)."""


class TeamGame(Incorporator):
    """One row per team per DECIDED matchup (home perspective + away
    perspective) -- the one unavoidable reshape, since ESPN ships matchups
    as home/away pairs and every cross-row stat here (all-play expected
    wins, win/loss streaks, single-week/margin records) is team-scoped.
    Built network-free off already-linked `Matchup` rows; `inc_code=
    "team_key"` lets Season Timeline read each team-season's broadcast
    all-play total back via `TeamGame.inc_dict.get(...)`."""


class FranchiseCard(Incorporator):
    """All-time per-franchise rollup (view 1). Bare -- the payload dict's
    keys are its export shape."""


class SeasonTimelineRow(Incorporator):
    """One franchise-season (view 2). Bare, no `inc_code` -- owner+season is
    a composite with no consumer that needs a lookup key."""


class RivalryRow(Incorporator):
    """One franchise pair, all-time (view 3). Bare, no `inc_code` -- the pair
    key is a plain Python tuple used only for aggregation."""


class RecordRow(Incorporator):
    """One records-book entry, one of ten kinds (view 4). Bare except for
    `value`: the ten kinds mix point/margin/ratio floats with win/loss-
    streak game counts under one column, and the schema inferencer types a
    bare field from its FIRST sampled row only (`highest_single_week_score`'s
    float, here) -- silently widening every later int (the streak kinds) to
    float. An explicit `int | float` annotation opts `value` out of that
    single-sample inference so each row keeps its own natural type. No
    `inc_code` -- `kind` is a plain field, not a synthesized PK."""

    value: int | float | None = None


class DraftTendencyRow(Incorporator):
    """One draft-tendency entry, one of three kinds (view 5). Bare, no
    `inc_code` -- heterogeneous per-kind shape, list-scanned only."""


class SettingsRow(Incorporator):
    """One season's league settings snapshot (view 6). `inc_code="season"`
    -- an existing, naturally-unique field."""


# ---------------------------------------------------------------------------
# Genuine N:M reductions -- these three cannot be expressed as a `calc_all`
# broadcast because they REDUCE row count (pair-keyed rivalries, top-N draft
# counts) rather than annotate every input row. Joins inside them read the
# linked graph map directly (`m.home_standing.owner`, `p.standing.owner`,
# `p.player`) -- zero hand-built lookup dicts.
# ---------------------------------------------------------------------------


def rivalry_matrix_rows(all_matchups: list[Any]) -> list[dict[str, Any]]:
    """One row per franchise pair, all-time: W-L, meetings, playoff
    meetings, biggest blowout, closest game. Pair key is sorted
    (owner_id_a < owner_id_b) so each pair is counted once."""
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for m in all_matchups:
        if m.winner == "UNDECIDED" or not m.home_standing or not m.away_standing:
            continue
        owner_home = m.home_standing.owner
        owner_away = m.away_standing.owner
        if not owner_home or not owner_away or owner_home.id == owner_away.id:
            continue

        if owner_home.id < owner_away.id:
            owner_a, owner_b, score_a, score_b, a_won = (
                owner_home,
                owner_away,
                m.home.totalPoints,
                m.away.totalPoints,
                (m.winner == "HOME"),
            )
        else:
            owner_a, owner_b, score_a, score_b, a_won = (
                owner_away,
                owner_home,
                m.away.totalPoints,
                m.home.totalPoints,
                (m.winner == "AWAY"),
            )

        stat = pairs.setdefault(
            (owner_a.id, owner_b.id),
            {
                "display_name_a": owner_a.display_name,
                "display_name_b": owner_b.display_name,
                "meetings": 0,
                "wins_a": 0,
                "wins_b": 0,
                "playoff_meetings": 0,
                "biggest_blowout_margin": -1.0,
                "biggest_blowout_season": None,
                "biggest_blowout_week": None,
                "closest_game_margin": None,
                "closest_game_season": None,
                "closest_game_week": None,
            },
        )
        stat["meetings"] += 1
        stat["wins_a" if a_won else "wins_b"] += 1
        if m.playoffTierType != "NONE":
            stat["playoff_meetings"] += 1

        margin = abs(score_a - score_b)
        if margin > stat["biggest_blowout_margin"]:
            stat["biggest_blowout_margin"] = margin
            stat["biggest_blowout_season"] = m.season
            stat["biggest_blowout_week"] = m.matchupPeriodId
        if stat["closest_game_margin"] is None or margin < stat["closest_game_margin"]:
            stat["closest_game_margin"] = margin
            stat["closest_game_season"] = m.season
            stat["closest_game_week"] = m.matchupPeriodId

    return [{"owner_guid_a": owner_a, "owner_guid_b": owner_b, **stat} for (owner_a, owner_b), stat in pairs.items()]


def records_book_rows(all_standings: list[Any], all_team_games: list[Any]) -> list[dict[str, Any]]:
    """Ten all-time record kinds. `all_team_games` is already filtered to
    DECIDED matchups (byes and in-progress weeks never became `TeamGame`
    rows at all -- see `TeamGame`'s own docstring); a genuine 0-point week
    and a tiebreak-decided tie (margin legitimately `0.0`) both register."""
    rows: list[dict[str, Any]] = []

    def record(kind: str, value: Any, owner: Any, season: int | None, week: int | None, detail: str) -> None:
        rows.append(
            {
                "kind": kind,
                "value": value,
                "owner_guid": owner.id if owner else None,
                "display_name": owner.display_name if owner else "Unknown",
                "season": season,
                "week": week,
                "detail": detail,
            }
        )

    def opponent_name(tg: Any) -> str:
        return (
            tg.opponent_standing.owner.display_name
            if tg.opponent_standing and tg.opponent_standing.owner
            else "Unknown"
        )

    def owner_of(tg: Any) -> Any:
        return tg.standing.owner if tg.standing else None

    # 1-2: highest/lowest single-week score.
    if all_team_games:
        highest = max(all_team_games, key=operator.attrgetter("score"))
        record(
            "highest_single_week_score",
            highest.score,
            owner_of(highest),
            highest.season,
            highest.week,
            f"vs {opponent_name(highest)}",
        )
        lowest = min(all_team_games, key=operator.attrgetter("score"))
        record(
            "lowest_single_week_score",
            lowest.score,
            owner_of(lowest),
            lowest.season,
            lowest.week,
            f"vs {opponent_name(lowest)}",
        )

    # 3-4: largest/narrowest margin of victory, recorded from the winner's side.
    winner_games = [tg for tg in all_team_games if tg.result == "W"]
    if winner_games:
        biggest = max(winner_games, key=operator.attrgetter("margin"))
        record(
            "largest_margin_of_victory",
            biggest.margin,
            owner_of(biggest),
            biggest.season,
            biggest.week,
            f"beat {opponent_name(biggest)} by {round2(biggest.margin)}",
        )
        narrowest = min(winner_games, key=operator.attrgetter("margin"))
        record(
            "narrowest_margin_of_victory",
            narrowest.margin,
            owner_of(narrowest),
            narrowest.season,
            narrowest.week,
            f"beat {opponent_name(narrowest)} by {round2(narrowest.margin)}",
        )

    # 5-8: best/worst season record, highest/lowest season points-for --
    # only seasons a franchise actually played at least one decided game.
    played_standings = [s for s in all_standings if (s.wins + s.losses + s.ties) > 0]
    if played_standings:
        best = max(played_standings, key=operator.attrgetter("win_pct_equiv"))
        record(
            "best_season_record",
            round2(best.win_pct_equiv),
            best.owner,
            best.season,
            None,
            f"{best.wins}-{best.losses}-{best.ties}",
        )
        worst = min(played_standings, key=operator.attrgetter("win_pct_equiv"))
        record(
            "worst_season_record",
            round2(worst.win_pct_equiv),
            worst.owner,
            worst.season,
            None,
            f"{worst.wins}-{worst.losses}-{worst.ties}",
        )

        highest_pf = max(played_standings, key=operator.attrgetter("points_for"))
        record(
            "highest_season_points_for",
            round2(highest_pf.points_for),
            highest_pf.owner,
            highest_pf.season,
            None,
            f"{highest_pf.points_for:.2f} points",
        )
        lowest_pf = min(played_standings, key=operator.attrgetter("points_for"))
        record(
            "lowest_season_points_for",
            round2(lowest_pf.points_for),
            lowest_pf.owner,
            lowest_pf.season,
            None,
            f"{lowest_pf.points_for:.2f} points",
        )

    # 9-10: longest win/loss streak -- a franchise-history streak,
    # chronological across ALL seasons; the running-count broadcast peaks at
    # exactly the row `max()` selects, so season/week come along for free.
    if all_team_games:
        best_win = max(all_team_games, key=operator.attrgetter("longest_win_streak"))
        if best_win.longest_win_streak:
            record(
                "longest_win_streak",
                best_win.longest_win_streak,
                owner_of(best_win),
                best_win.season,
                best_win.week,
                f"{best_win.longest_win_streak} straight wins",
            )
        best_loss = max(all_team_games, key=operator.attrgetter("longest_loss_streak"))
        if best_loss.longest_loss_streak:
            record(
                "longest_loss_streak",
                best_loss.longest_loss_streak,
                owner_of(best_loss),
                best_loss.season,
                best_loss.week,
                f"{best_loss.longest_loss_streak} straight losses",
            )

    return rows


def draft_tendency_rows(all_draft_picks: list[Any], top_drafted: list[tuple[int, int]]) -> list[dict[str, Any]]:
    """Three draft-tendency kinds: round-1 position mix per franchise,
    all-time most-drafted players (`top_drafted`, computed once in `main()`
    and shared with the player-name-resolution wanted-ID set -- fixes
    bug #3), and the first-overall honor roll."""
    rows: list[dict[str, Any]] = []

    position_counts: dict[tuple[str, str], int] = defaultdict(int)
    owner_by_id: dict[str, Any] = {}
    for p in all_draft_picks:
        if p.roundId != 1 or not p.standing or not p.standing.owner:
            continue
        owner_by_id[p.standing.owner.id] = p.standing.owner
        position = p.player.position if p.player else "UNKNOWN"
        position_counts[(p.standing.owner.id, position)] += 1

    for (owner_id, position), count in position_counts.items():
        owner = owner_by_id[owner_id]
        rows.append(
            {
                "kind": "round1_position_mix",
                "owner_guid": owner_id,
                "display_name": owner.display_name,
                "position": position,
                "count": count,
            }
        )

    for rank, (player_id, times_drafted) in enumerate(top_drafted, start=1):
        player = PlayerName.inc_dict.get(player_id)
        rows.append(
            {
                "kind": "most_drafted",
                "rank": rank,
                "player_id": player_id,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
                "times_drafted": times_drafted,
            }
        )

    for p in all_draft_picks:
        if p.overallPickNumber != 1:
            continue
        owner = p.standing.owner if p.standing else None
        rows.append(
            {
                "kind": "first_overall",
                "season": p.season,
                "owner_guid": owner.id if owner else None,
                "display_name": owner.display_name if owner else "Unknown",
                "player_id": p.playerId,
                "player_name": p.player.fullName if p.player else "Unknown",
                "position": p.player.position if p.player else "UNKNOWN",
            }
        )

    return rows


def ascii_safe(text: str) -> str:
    return text.encode("ascii", errors="replace").decode("ascii")


def print_franchise_board(franchise_cards: IncorporatorList) -> None:
    ranked = sorted(franchise_cards, key=operator.attrgetter("win_pct"), reverse=True)
    print("\nFRANCHISE CARDS (all-time, sorted by win%)")
    header = f"{'FRANCHISE':<24}{'W-L-T':<12}{'WIN%':>7}{'SEASONS':>9}{'TITLES':>8}{'PLAYOFF%':>10}"
    print(header)
    print("-" * len(header))
    for row in ranked:
        name = ascii_safe(str(row.display_name))[:23]
        record = f"{row.wins}-{row.losses}-{row.ties}"
        print(
            f"{name:<24}{record:<12}{row.win_pct:>7.3f}{row.seasons_played:>9}"
            f"{row.championships:>8}{row.playoff_rate:>10.1%}"
        )


def print_records_book(records_book: IncorporatorList) -> None:
    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}  {'FRANCHISE':<20}{'SEASON':>8}  {'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in records_book:
        name = ascii_safe(str(row.display_name))[:19]
        print(f"{row.kind:<28}{row.value:>10}  {name:<20}{row.season!s:>8}  {ascii_safe(str(row.detail))}")


def print_honor_roll(draft_tendencies: IncorporatorList) -> None:
    honor_roll = sorted((r for r in draft_tendencies if r.kind == "first_overall"), key=operator.attrgetter("season"))
    print("\nFIRST-OVERALL DRAFT HONOR ROLL")
    header = f"{'SEASON':<8}{'FRANCHISE':<22}{'PLAYER':<24}{'POS'}"
    print(header)
    print("-" * len(header))
    for row in honor_roll:
        franchise = ascii_safe(str(row.display_name))[:21]
        player = ascii_safe(str(row.player_name))[:23]
        print(f"{row.season:<8}{franchise:<22}{player:<24}{row.position}")


async def main() -> None:
    league_id = os.environ.get("ESPN_LEAGUE_ID", DEMO_LEAGUE_ID)
    espn_s2 = os.environ.get("ESPN_S2")
    espn_swid = os.environ.get("ESPN_SWID")
    auth_headers = cookie_headers(espn_s2, espn_swid)
    has_cookies = bool(auth_headers)

    out_dir = Path(__file__).resolve().parent / "out"
    out_dir.mkdir(exist_ok=True)

    print(f"League {league_id} -- {'private (cookies present)' if has_cookies else 'public (no cookies)'}")
    print("Discovering reachable seasons via status.previousSeasons ...")

    current_year = date.today().year
    pending = [current_year]
    seen_years: set[int] = set()
    all_seasons: list[Any] = []

    season_conv_dict = {
        "teams": calc(list, "teams", default=[], target_type=list),
        "schedule": calc(list, "schedule", default=[], target_type=list),
        "members": calc(list, "members", default=[], target_type=list),
        "draft_picks": calc(list, "draftDetail.picks", default=[], target_type=list),
        "settings": calc(dict, "settings", default={}, target_type=dict),
        "previous_seasons": calc(list, "status.previousSeasons", default=[], target_type=list),
        # seasonId is present in the payload root but was previously never
        # read -- season was re-derived from the loop variable instead
        # (bug #5). calc(), not inc(): the output key ("season") differs
        # from the source key ("seasonId").
        "season": calc(int, "seasonId", default=0, target_type=int),
    }

    while pending:
        year = pending.pop(0)
        if year in seen_years:
            continue
        seen_years.add(year)

        with quiet_expected_reject():
            rows = await Season.incorp(
                inc_url=MODERN_URL.format(season=year, league_id=league_id),
                headers=auth_headers,
                params={"view": VIEWS},
                conv_dict=season_conv_dict,
            )
        if not rows:
            # A failed fetch surfaces as an empty IncorporatorList carrying
            # structured RejectEntry records, not a raised exception. With
            # cookies present, a failed modern fetch retries against the
            # cookie-gated leagueHistory endpoint regardless of status code --
            # ESPN returns 404 (not 401) for a season outside the modern
            # endpoint's own coverage window, so the status code alone can't
            # gate the retry.
            if has_cookies:
                with quiet_expected_reject():
                    rows = await Season.incorp(
                        inc_url=HISTORY_URL.format(league_id=league_id),
                        headers=auth_headers,
                        params={"view": VIEWS, "seasonId": year},
                        rec_path="0",
                        conv_dict=season_conv_dict,
                    )
                if not rows:
                    print(f"  season {year}: unavailable via historical endpoint too -- skipping")
                    continue
            else:
                print(f"  season {year}: unavailable (no cookies) -- skipping")
                continue

        season_row = rows[0]
        all_seasons.append(season_row)
        print(f"  season {year}: OK ({len(season_row.teams)} teams, {len(season_row.schedule)} matchups)")

        if year == current_year:
            pending.extend(y for y in season_row.previous_seasons if y not in seen_years)

    all_seasons.sort(key=operator.attrgetter("season"))
    print(f"\nFetched {len(all_seasons)} season(s): {[s.season for s in all_seasons]}")

    # --- Owner: must build first -- everything downstream links to it.
    # ESPN repeats every member row per season (bug #4: ~73 rows for ~13
    # people); dedupe by id BEFORE the one incorp() call.
    all_owner_rows = [o.model_dump(by_alias=True) for s in all_seasons for o in s.members]
    deduped_owner_rows = list({o["id"]: o for o in all_owner_rows}.values())
    all_owners = await Owner.incorp(
        payload_list=deduped_owner_rows,
        inc_code="id",
        inc_name="display_name",
        conv_dict={
            "id": inc(str, default=""),
            "display_name": calc(str, "displayName", default="Unknown", target_type=str),
        },
    )

    # --- Standing: needs Owner built for its build-time `owner` join.
    all_team_rows = [{**t.model_dump(by_alias=True), "season": s.season} for s in all_seasons for t in s.teams]
    all_standings = await Standing.incorp(
        payload_list=all_team_rows,
        inc_code="team_key",
        conv_dict={
            "id": inc(int, default=0),
            "primaryOwner": inc(str, default=""),
            "name": inc(str, default="Unknown"),
            # season==output key matches the stamped source key exactly, so
            # inc() (not calc()) is correct here -- contrast Season's own
            # "season" entry above, which drills a differently-named source.
            "season": inc(int, default=0),
            "team_key": calc(team_key, "season", "id", target_type=str),
            "owner": calc(link_to(all_owners), "primaryOwner"),
            "division_id": calc(int, "divisionId", default=0, target_type=int),
            "wins": calc(int, "record.overall.wins", default=0, target_type=int),
            "losses": calc(int, "record.overall.losses", default=0, target_type=int),
            "ties": calc(int, "record.overall.ties", default=0, target_type=int),
            "points_for": calc(float, "record.overall.pointsFor", default=0.0, target_type=float),
            "points_against": calc(float, "record.overall.pointsAgainst", default=0.0, target_type=float),
            "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
            "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
            "win_pct_equiv": calc(win_pct_equiv, "wins", "losses", "ties", target_type=float),
            "is_champion": calc(is_champion, "final_rank", default=False, target_type=bool),
            "is_runner_up": calc(is_runner_up, "final_rank", default=False, target_type=bool),
            # --- calc_all broadcasts: whole-column pass, once, across every
            # fetched season -- the fix for rule 13's structural root cause.
            "owner_wins_total": calc_all(sum_by_group, "primaryOwner", "wins", target_type=int),
            "owner_losses_total": calc_all(sum_by_group, "primaryOwner", "losses", target_type=int),
            "owner_ties_total": calc_all(sum_by_group, "primaryOwner", "ties", target_type=int),
            "owner_points_for_total": calc_all(sum_by_group, "primaryOwner", "points_for", target_type=float),
            "owner_points_against_total": calc_all(sum_by_group, "primaryOwner", "points_against", target_type=float),
            "owner_seasons_played": calc_all(count_distinct_by_group, "primaryOwner", "season", target_type=int),
            "owner_championships": calc_all(count_true_by_group, "primaryOwner", "is_champion", target_type=int),
            "owner_runner_ups": calc_all(count_true_by_group, "primaryOwner", "is_runner_up", target_type=int),
            "owner_average_finish": calc_all(mean_positive_by_group, "primaryOwner", "final_rank", target_type=float),
            "season_is_last_place": calc_all(is_group_max_positive, "season", "final_rank", target_type=bool),
            "owner_last_places_total": calc_all(
                count_true_by_group, "primaryOwner", "season_is_last_place", target_type=int
            ),
            "owner_win_pct": calc(
                win_pct_from_totals, "owner_wins_total", "owner_losses_total", "owner_ties_total", target_type=float
            ),
        },
    )

    # --- Matchup: needs Standing built for its build-time home/away joins.
    # `home`/`away` stay nested -- never flattened into parallel scalars.
    all_schedule_rows = [{**m.model_dump(by_alias=True), "season": s.season} for s in all_seasons for m in s.schedule]
    all_matchups = await Matchup.incorp(
        payload_list=all_schedule_rows,
        conv_dict={
            "id": inc(int, default=0),
            "matchupPeriodId": inc(int, default=0),
            "playoffTierType": inc(str, default="NONE"),
            "winner": inc(str, default="UNDECIDED"),
            "season": inc(int, default=0),
            "home_team_key": calc(team_key, "season", "home.teamId", default=None, target_type=str),
            "away_team_key": calc(team_key, "season", "away.teamId", default=None, target_type=str),
            "home_standing": calc(link_to(all_standings), "home_team_key"),
            "away_standing": calc(link_to(all_standings), "away_team_key"),
        },
    )

    print(f"Loaded {len(all_standings)} standings, {len(all_matchups)} matchups, {len(all_owners)} owner records.")

    # --- Player names: round-1 per-season pass, then a targeted top-N pass.
    # The wanted-ID set is computed ONCE, off the flattened raw pick payload
    # (bug #3 -- the top-15 Counter was previously computed twice).
    all_pick_rows = [{**p.model_dump(by_alias=True), "season": s.season} for s in all_seasons for p in s.draft_picks]
    round1_ids_by_season: dict[int, set[int]] = defaultdict(set)
    for p in all_pick_rows:
        if p["roundId"] == 1:
            round1_ids_by_season[p["season"]].add(p["playerId"])
    draft_counts = Counter(p["playerId"] for p in all_pick_rows)
    top_drafted = draft_counts.most_common(TOP_N_MOST_DRAFTED)

    player_name_conv_dict = {
        "defaultPositionId": inc(int, default=0),
        # Computed once per player, at the source -- every read site then
        # takes `p.player.position if p.player else "UNKNOWN"` instead of
        # calling position_name() itself with its own guard (fixes bug #6).
        "position": calc(position_name, "defaultPositionId", default="UNKNOWN", target_type=str),
    }

    print("Resolving player names for round-1 picks + top drafted players ...")
    all_player_names: list[Any] = []
    for season in all_seasons:
        missing_ids = sorted(
            pid for pid in round1_ids_by_season.get(season.season, set()) if pid not in PlayerName.inc_dict
        )
        if not missing_ids:
            continue
        names = await PlayerName.incorp(
            inc_url=PLAYERS_URL.format(season=season.season),
            headers={**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": missing_ids}})},
            params={"view": "players_wl"},
            inc_code="id",
            inc_name="fullName",
            conv_dict=player_name_conv_dict,
        )
        all_player_names.extend(names)

    most_recent_season = {p["playerId"]: p["season"] for p in all_pick_rows}
    missing_by_season: dict[int, list[int]] = defaultdict(list)
    for player_id, _times_drafted in top_drafted:
        if player_id not in PlayerName.inc_dict:
            missing_by_season[most_recent_season[player_id]].append(player_id)

    for season_year, player_ids in missing_by_season.items():
        names = await PlayerName.incorp(
            inc_url=PLAYERS_URL.format(season=season_year),
            headers={**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": player_ids}})},
            params={"view": "players_wl"},
            inc_code="id",
            inc_name="fullName",
            conv_dict=player_name_conv_dict,
        )
        all_player_names.extend(names)

    print(f"Resolved {len(all_player_names)} player names.")

    # --- DraftPick: needs Standing + PlayerName built for its build-time joins.
    all_draft_picks = await DraftPick.incorp(
        payload_list=all_pick_rows,
        conv_dict={
            "roundId": inc(int, default=0),
            "roundPickNumber": inc(int, default=0),
            "overallPickNumber": inc(int, default=0),
            "playerId": inc(int, default=0),
            "teamId": inc(int, default=0),
            "keeper": inc(bool, default=False),
            "season": inc(int, default=0),
            "team_key": calc(team_key, "season", "teamId", target_type=str),
            "standing": calc(link_to(all_standings), "team_key"),
            # PlayerName is built via SEVERAL incorp() calls (one per
            # season/batch) accumulated into a plain list, which has no
            # `inc_dict` of its own -- link_to(PlayerName) (the class)
            # reads the bubble-up registry every one of those calls feeds.
            "player": calc(link_to(PlayerName), "playerId"),
        },
    )
    print(f"Loaded {len(all_draft_picks)} draft picks.")

    # --- TeamGame: the one reshape -- one row per team per DECIDED matchup.
    team_game_rows = [
        {
            "season": m.season,
            "week": m.matchupPeriodId,
            "tier": m.playoffTierType,
            "team_key": m.home_team_key,
            "standing": m.home_standing,
            "opponent_team_key": m.away_team_key,
            "opponent_standing": m.away_standing,
            "score": m.home.totalPoints if m.home else 0.0,
            "opponent_score": m.away.totalPoints if m.away else 0.0,
            "result": "W" if m.winner == "HOME" else ("L" if m.winner == "AWAY" else "T"),
        }
        for m in all_matchups
        if m.winner != "UNDECIDED"
    ] + [
        {
            "season": m.season,
            "week": m.matchupPeriodId,
            "tier": m.playoffTierType,
            "team_key": m.away_team_key,
            "standing": m.away_standing,
            "opponent_team_key": m.home_team_key,
            "opponent_standing": m.home_standing,
            "score": m.away.totalPoints if m.away else 0.0,
            "opponent_score": m.home.totalPoints if m.home else 0.0,
            "result": "W" if m.winner == "AWAY" else ("L" if m.winner == "HOME" else "T"),
        }
        for m in all_matchups
        if m.winner != "UNDECIDED"
    ]
    all_team_games = await TeamGame.incorp(
        payload_list=team_game_rows,
        inc_code="team_key",
        conv_dict={
            "margin": calc(abs_diff, "score", "opponent_score", target_type=float),
            "all_play_expected_wins": calc_all(
                all_play_broadcast, "season", "team_key", "week", "score", "tier", target_type=float
            ),
            "longest_win_streak": calc_all(
                make_streak_broadcast("W"), "standing", "season", "week", "result", target_type=int
            ),
            "longest_loss_streak": calc_all(
                make_streak_broadcast("L"), "standing", "season", "week", "result", target_type=int
            ),
        },
    )

    # --- View 1: Franchise Cards ---
    playoff_by_owner = playoff_appearances_by_owner(all_matchups)
    franchise_cards = await FranchiseCard.incorp(
        payload_list=[
            {
                "owner_guid": s.primaryOwner,
                "display_name": s.owner.display_name if s.owner else "Unknown",
                "wins": s.owner_wins_total,
                "losses": s.owner_losses_total,
                "ties": s.owner_ties_total,
                "win_pct": s.owner_win_pct,
                "points_for": s.owner_points_for_total,
                "points_against": s.owner_points_against_total,
                "seasons_played": s.owner_seasons_played,
                "average_finish": s.owner_average_finish,
                "championships": s.owner_championships,
                "runner_ups": s.owner_runner_ups,
                "last_places": s.owner_last_places_total,
                "playoff_appearances": len(playoff_by_owner.get(s.primaryOwner, set())),
                "playoff_rate": (
                    len(playoff_by_owner.get(s.primaryOwner, set())) / s.owner_seasons_played
                    if s.owner_seasons_played
                    else 0.0
                ),
            }
            for s in {st.primaryOwner: st for st in all_standings if st.primaryOwner}.values()
        ],
        inc_code="owner_guid",
        inc_name="display_name",
        conv_dict={
            "win_pct": calc(round3, "win_pct", target_type=float),
            "points_for": calc(round2, "points_for", target_type=float),
            "points_against": calc(round2, "points_against", target_type=float),
            "average_finish": calc(round2, "average_finish", target_type=float),
            "playoff_rate": calc(round3, "playoff_rate", target_type=float),
        },
    )
    await FranchiseCard.export(
        instance=franchise_cards, file_path=out_dir / "franchise_cards.ndjson", if_exists="replace"
    )

    # --- View 2: Season Timeline ---
    season_timeline = await SeasonTimelineRow.incorp(
        payload_list=[
            {
                "owner_guid": s.primaryOwner,
                "display_name": s.owner.display_name if s.owner else "Unknown",
                "season": s.season,
                "team_id": s.id,
                "team_name": s.name,
                "division_id": s.division_id,
                "seed": s.playoff_seed,
                "final_rank": s.final_rank,
                "wins": s.wins,
                "losses": s.losses,
                "ties": s.ties,
                "points_for": s.points_for,
                "points_against": s.points_against,
                "all_play_expected_wins": all_play_wins_for(s.team_key),
            }
            for s in all_standings
        ],
        conv_dict={
            "luck_delta": calc(luck_delta, "wins", "ties", "all_play_expected_wins", target_type=float),
            "all_play_expected_wins": calc(round2, "all_play_expected_wins", target_type=float),
        },
    )
    await SeasonTimelineRow.export(
        instance=season_timeline, file_path=out_dir / "season_timeline.ndjson", if_exists="replace"
    )

    # --- View 3: Rivalry Matrix ---
    rivalry_matrix = await RivalryRow.incorp(
        payload_list=rivalry_matrix_rows(all_matchups),
        conv_dict={
            "biggest_blowout_margin": calc(round2, "biggest_blowout_margin", target_type=float),
            "closest_game_margin": calc(round2, "closest_game_margin", target_type=float),
        },
    )
    await RivalryRow.export(instance=rivalry_matrix, file_path=out_dir / "rivalry_matrix.ndjson", if_exists="replace")

    # --- View 4: Records Book ---
    records_book = await RecordRow.incorp(
        payload_list=records_book_rows(all_standings, all_team_games),
        conv_dict={"value": calc(round2, "value")},
    )
    await RecordRow.export(instance=records_book, file_path=out_dir / "records_book.ndjson", if_exists="replace")

    # --- View 5: Draft Tendencies ---
    draft_tendencies = await DraftTendencyRow.incorp(payload_list=draft_tendency_rows(all_draft_picks, top_drafted))
    await DraftTendencyRow.export(
        instance=draft_tendencies, file_path=out_dir / "draft_tendencies.ndjson", if_exists="replace"
    )

    # --- View 6: Settings Evolution ---
    # Only the three raw sub-dicts the conv_dict below actually drills into
    # are carried onto the payload row -- ESPN's `settings` blob has ~10
    # other top-level keys (acquisitionSettings, draftSettings, ...) no view
    # reads; spreading the FULL `model_dump()` would leak them into export.
    settings_evolution = await SettingsRow.incorp(
        payload_list=[
            {
                "season": s.season,
                "league_size": len(s.teams),
                "scheduleSettings": s.settings.model_dump(by_alias=True).get("scheduleSettings"),
                "rosterSettings": s.settings.model_dump(by_alias=True).get("rosterSettings"),
                "scoringSettings": s.settings.model_dump(by_alias=True).get("scoringSettings"),
            }
            for s in all_seasons
        ],
        inc_code="season",
        conv_dict={
            "playoff_team_count": calc(int, "scheduleSettings.playoffTeamCount", default=0, target_type=int),
            "playoff_seeding_rule": calc(str, "scheduleSettings.playoffSeedingRule", default="", target_type=str),
            "ppr_points": calc(ppr_points_from_scoring, "scoringSettings.scoringItems", default=0.0, target_type=float),
            "division_names": calc(
                division_names_from_schedule, "scheduleSettings.divisions", default=[], target_type=list
            ),
            "division_count": calc(len, "division_names", default=0, target_type=int),
            "roster_slots": calc(dict, "rosterSettings.lineupSlotCounts", default={}, target_type=dict),
            # Drop the raw ESPN sub-dicts LAST, now every extraction above has
            # already drilled into them -- keeps the export to the 8 clean
            # fields instead of leaking ESPN's full settings blob.
            "scheduleSettings": calc(drop_raw_settings_blob, "scheduleSettings"),
            "rosterSettings": calc(drop_raw_settings_blob, "rosterSettings"),
            "scoringSettings": calc(drop_raw_settings_blob, "scoringSettings"),
        },
    )
    await SettingsRow.export(
        instance=settings_evolution, file_path=out_dir / "settings_evolution.ndjson", if_exists="replace"
    )

    print(f"\nWrote 6 views to {out_dir}:")
    print(f"  franchise_cards.ndjson    {len(franchise_cards)} rows")
    print(f"  season_timeline.ndjson    {len(season_timeline)} rows")
    print(f"  rivalry_matrix.ndjson     {len(rivalry_matrix)} rows")
    print(f"  records_book.ndjson       {len(records_book)} rows")
    print(f"  draft_tendencies.ndjson   {len(draft_tendencies)} rows")
    print(f"  settings_evolution.ndjson {len(settings_evolution)} rows")

    print_franchise_board(franchise_cards)
    print_records_book(records_book)
    print_honor_roll(draft_tendencies)


if __name__ == "__main__":
    asyncio.run(main())
