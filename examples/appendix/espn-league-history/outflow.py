"""Outflow sidecar for the ESPN league-history one-shot fjord pipeline.

Defines the seven source classes (``Season``, ``Owner``, ``Standing``,
``Matchup``, ``DraftPick``, ``PlayerName``, ``TeamGame``) plus the six
derived view classes and the ``outflow(state)`` function that fuses them
into one six-key dict -- fjord's multi-output contract writes each key to
its own file (see ``export_params`` in ``espn_league_history.py``).

Every cross-class join (owner display name, team-key -> standing, player-id
-> name/position) resolves HERE, read-time, against the live snapshot
``fjord()`` hands ``outflow(state)`` each wave -- ``state["Peer"].inc_dict.get(key)``
(the ``cls.fjord()`` daemon path: ``state`` values are live ``IncorporatorList``s
with a real ``inc_dict``, not the Tideweaver-Fjord-current plain-list form).
No build-time ``link_to`` anywhere: ``Standing``/``Matchup``/``DraftPick``/
``TeamGame`` are sibling ``stream_params`` entries seeded with no ordering
guarantee between them, so a build-time join would be unreliable even before
considering that read-time is the doctrine default.

Every source's own static coercion (int/float/bool casts, the composite
``team_key`` join key, the per-owner ``calc_all`` rollups) lives in its own
``conv_dict``, written inline in ``espn_league_history.py``'s ``stream_params``
entries -- the domain-calc functions those ``conv_dict`` entries call are
defined here and imported into the entry file, same split as
``examples/09-nascar-fantasy-fjord/outflow.py``.
"""

from __future__ import annotations

import operator
from collections import Counter, defaultdict
from typing import Any

from pydantic import ConfigDict

from incorporator import Incorporator

# Standard ESPN fantasy-football defaultPositionId enumeration (live-verified
# against Davante Adams=3=WR and a D/ST row=16); unknown ids fall back to a
# labelled placeholder rather than crashing.
POSITION_MAP = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}

TOP_N_MOST_DRAFTED = 15


# ---------------------------------------------------------------------------
# Domain-calc helpers -- conv_dict callables (inline in the entry file's
# stream_params) plus the row-shaping helpers outflow(state) itself calls.
# ---------------------------------------------------------------------------


def position_name(position_id: int) -> str:
    return POSITION_MAP.get(position_id, f"POS_{position_id}")


def team_key(season: int, team_id: int | None) -> str | None:
    """Composite join key -- a bare ESPN `team.id` repeats every season, so
    once `Standing` batches across all seasons a bare `id` would collide in
    `inc_dict`. `None` (a playoff bye's missing `away` side) stays `None`
    rather than stringifying into a bogus key."""
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
    same field name."""
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
    """One `calc_all` pass, computed once: every regular-season week's full
    field of scores is grouped once, then each team-week's win-share against
    that field is summed into a running per-(season, team) total broadcast
    onto every row of that team-season."""
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
    through that row -- chronological across every season. `owner_guids` is
    TeamGame's own raw ``owner_guid`` string field (or `None`), stamped in
    `espn_league_history.py`'s `main()` from a plain `team_key -> primaryOwner`
    dict since `Standing` and `TeamGame` are sibling fjord sources with no
    build-time ordering guarantee -- no `standing.owner` object to guard."""

    def broadcast(owner_guids: list[Any], seasons: list[int], weeks: list[int], results: list[str]) -> list[int]:
        order = [i for i, _ in sorted(enumerate(zip(seasons, weeks, strict=True)), key=operator.itemgetter(1))]
        run_by_owner: dict[str, int] = defaultdict(int)
        streaks = [0] * len(owner_guids)
        for i in order:
            owner_id = owner_guids[i]
            if owner_id is None:
                continue
            run_by_owner[owner_id] = run_by_owner[owner_id] + 1 if results[i] == target_result else 0
            streaks[i] = run_by_owner[owner_id]
        return streaks

    return broadcast


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


def cookie_headers(espn_s2: str | None, espn_swid: str | None) -> dict[str, str]:
    """Hand-rolled Cookie header -- `incorp()` has no native `cookies=` kwarg."""
    if espn_s2 and espn_swid:
        return {"Cookie": f"espn_s2={espn_s2}; SWID={espn_swid}"}
    return {}


# ---------------------------------------------------------------------------
# Source classes -- each fjord source needs its own subclass; none carries a
# build-time link_to now (every cross-class join is read-time, in outflow()
# below). Field declarations here would silently coerce types, so every
# source's own conv_dict (in espn_league_history.py) owns coercion instead.
# ---------------------------------------------------------------------------


class Season(Incorporator):
    """One ESPN league-season response -- modern dict-root or historical
    list-root (`rec_path="0"`), same `conv_dict` either way. Re-registered a
    fourth time as a `stream_params` entry (`payload_list=[s.model_dump(...)
    for s in all_seasons]`) purely so `outflow(state)` can read
    `state["Season"]` -- the pre-fjord discovery loop's own built rows aren't
    otherwise visible inside the sidecar."""


class Owner(Incorporator):
    """One league member (GUID + display name), built network-free off every
    season's `members` in ONE batched `payload_list=` call -- `inc_code="id"`
    makes `Owner.inc_dict` the join every view resolves display names
    against."""


class Standing(Incorporator):
    """One team's season-long record, built network-free off every season's
    `teams` in ONE batched `payload_list=` call so `calc_all` can roll up
    all-time owner aggregates across the whole history in one declarative
    pass. `inc_code="team_key"` (a `"{season}:{team_id}"` composite -- a bare
    team id repeats every season once seasons are batched together)."""


class Matchup(Incorporator):
    """One scheduled/played game, built network-free off every season's
    `schedule` in ONE batched `payload_list=` call. `home`/`away` stay
    nested (the framework's dynamic schema builder auto-promotes each into
    an Optional submodel) -- a playoff bye (`home`-only, no `away` key)
    surfaces as `m.away is None` directly, not an erased `0` sentinel."""


class DraftPick(Incorporator):
    """One draft pick, built network-free off every season's `draft_picks`
    in ONE batched `payload_list=` call."""


class PlayerName(Incorporator):
    """Resolved player name + position. The ONE genuinely-networked fjord
    source -- fanned out over every discovered season's `players_wl`
    endpoint in ONE `incorp(inc_url=[...])` call, sharing one
    `X-Fantasy-Filter` header carrying the union of every wanted player id
    (round-1 picks + all-time top-N most-drafted); each season endpoint
    resolves only the ids it recognises, `inc_code="id"` dedups the rest."""


class TeamGame(Incorporator):
    """One row per team per DECIDED matchup (home perspective + away
    perspective) -- the one unavoidable reshape, since ESPN ships matchups
    as home/away pairs and every cross-row stat here (all-play expected
    wins, win/loss streaks, single-week/margin records) is team-scoped.
    Built network-free in `main()` off the RAW schedule dicts (never off
    built `Matchup` instances -- `Matchup` is a sibling `stream_params`
    entry with no ordering guarantee). `owner_guid`/`opponent_owner_guid`
    are raw strings stamped from a `team_key -> primaryOwner` dict built off
    `Standing`'s own raw payload rows in `main()`. `inc_code="team_key"`
    lets Season Timeline read each team-season's broadcast all-play total
    back via `TeamGame.inc_dict.get(...)`."""


class FranchiseCard(Incorporator):
    """All-time per-franchise rollup (view 1). Bare -- the returned dict's
    keys are its export shape. `extra="allow"` is declared explicitly
    (run-verified 2026-07-27): the fjord daemon's `flush()` calls
    `derived_cls.model_validate(row)` directly on a pre-declared class with
    no dynamic per-row schema union, so a bare class with pydantic's
    default `extra="ignore"` silently drops every undeclared key -- unlike
    the Tideweaver-Fjord-current path's dynamically-unioned schema."""

    model_config = ConfigDict(extra="allow")


class SeasonTimelineRow(Incorporator):
    """One franchise-season (view 2). Bare, no `inc_code` -- owner+season is
    a composite with no consumer that needs a lookup key. See
    `FranchiseCard`'s docstring for why `extra="allow"` is declared here."""

    model_config = ConfigDict(extra="allow")


class RivalryRow(Incorporator):
    """One franchise pair, all-time (view 3). Bare, no `inc_code` -- the pair
    key is a plain Python tuple used only for aggregation. See
    `FranchiseCard`'s docstring for why `extra="allow"` is declared here."""

    model_config = ConfigDict(extra="allow")


class RecordRow(Incorporator):
    """One records-book entry, one of ten kinds (view 4). `value` is
    declared explicitly: the ten kinds mix point/margin/ratio floats with
    win/loss-streak game counts under one column, and the schema inferencer
    types a bare field from its FIRST sampled row only
    (`highest_single_week_score`'s float, here) -- silently widening every
    later int (the streak kinds) to float. An explicit `int | float`
    annotation opts `value` out of that single-sample inference so each row
    keeps its own natural type. `extra="allow"` preserves every OTHER
    field alongside it -- see `FranchiseCard`'s docstring. No `inc_code` --
    `kind` is a plain field, not a synthesized PK."""

    model_config = ConfigDict(extra="allow")
    value: int | float | None = None


class DraftTendencyRow(Incorporator):
    """One draft-tendency entry, one of three kinds (view 5). Bare, no
    `inc_code` -- heterogeneous per-kind shape, list-scanned only. See
    `FranchiseCard`'s docstring for why `extra="allow"` is declared here."""

    model_config = ConfigDict(extra="allow")


class SettingsRow(Incorporator):
    """One season's league settings snapshot (view 6). Bare -- built fresh
    from only the 8 wanted fields inside `outflow()`, so there's no raw ESPN
    settings blob to drop before export (unlike the pre-fjord `conv_dict`
    version, which had to null it out after drilling into it). See
    `FranchiseCard`'s docstring for why `extra="allow"` is declared here."""

    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Outflow -- six derived views from one fused, read-time-joined state.
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return-twin of the original pipeline's six view-building blocks --
    reads `state["Season"|"Owner"|"Standing"|"Matchup"|"DraftPick"|
    "PlayerName"|"TeamGame"]` directly instead of closed-over `all_*`
    variables. Every cross-class join is `PeerClass.inc_dict.get(key)`,
    read-time, against the live snapshot `fjord()` hands this function."""
    seasons = state.get("Season")
    owners = state.get("Owner")
    standings = state.get("Standing")
    matchups = state.get("Matchup")
    draft_picks = state.get("DraftPick")
    player_names = state.get("PlayerName")
    team_games = state.get("TeamGame")
    if seasons is None or owners is None or standings is None:
        return {}

    # ════════════════════════════════════════════════════════════════
    # View 1 — Franchise Cards
    # ════════════════════════════════════════════════════════════════
    playoff_by_owner: dict[str, set[int]] = defaultdict(set)
    for m in matchups or []:
        if m.playoffTierType != "WINNERS_BRACKET":
            continue
        for tk in (m.home_team_key, m.away_team_key):
            st = standings.inc_dict.get(tk) if tk else None
            if st and st.primaryOwner:
                playoff_by_owner[st.primaryOwner].add(m.season)

    last_standing_by_owner = {s.primaryOwner: s for s in standings if s.primaryOwner}
    franchise_cards: list[dict[str, Any]] = []
    for owner_guid, s in last_standing_by_owner.items():
        owner_obj = owners.inc_dict.get(owner_guid)
        appearances = len(playoff_by_owner.get(owner_guid, set()))
        franchise_cards.append(
            {
                "owner_guid": owner_guid,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
                "wins": s.owner_wins_total,
                "losses": s.owner_losses_total,
                "ties": s.owner_ties_total,
                "win_pct": round3(s.owner_win_pct),
                "points_for": round2(s.owner_points_for_total),
                "points_against": round2(s.owner_points_against_total),
                "seasons_played": s.owner_seasons_played,
                "average_finish": round2(s.owner_average_finish),
                "championships": s.owner_championships,
                "runner_ups": s.owner_runner_ups,
                "last_places": s.owner_last_places_total,
                "playoff_appearances": appearances,
                "playoff_rate": round3(appearances / s.owner_seasons_played if s.owner_seasons_played else 0.0),
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 2 — Season Timeline
    # ════════════════════════════════════════════════════════════════
    season_timeline: list[dict[str, Any]] = []
    for s in standings:
        owner_obj = owners.inc_dict.get(s.primaryOwner) if s.primaryOwner else None
        tg = team_games.inc_dict.get(s.team_key) if team_games else None
        all_play = tg.all_play_expected_wins if tg else 0.0
        season_timeline.append(
            {
                "owner_guid": s.primaryOwner,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
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
                "all_play_expected_wins": round2(all_play),
                "luck_delta": luck_delta(s.wins, s.ties, all_play),
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 3 — Rivalry Matrix
    # ════════════════════════════════════════════════════════════════
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for m in matchups or []:
        if m.winner == "UNDECIDED":
            continue
        home_standing = standings.inc_dict.get(m.home_team_key) if m.home_team_key else None
        away_standing = standings.inc_dict.get(m.away_team_key) if m.away_team_key else None
        if not home_standing or not away_standing:
            continue
        owner_home = owners.inc_dict.get(home_standing.primaryOwner) if home_standing.primaryOwner else None
        owner_away = owners.inc_dict.get(away_standing.primaryOwner) if away_standing.primaryOwner else None
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

    rivalry_matrix = [
        {
            "owner_guid_a": a,
            "owner_guid_b": b,
            "display_name_a": stat["display_name_a"],
            "display_name_b": stat["display_name_b"],
            "meetings": stat["meetings"],
            "wins_a": stat["wins_a"],
            "wins_b": stat["wins_b"],
            "playoff_meetings": stat["playoff_meetings"],
            "biggest_blowout_margin": round2(stat["biggest_blowout_margin"]),
            "biggest_blowout_season": stat["biggest_blowout_season"],
            "biggest_blowout_week": stat["biggest_blowout_week"],
            "closest_game_margin": (
                round2(stat["closest_game_margin"]) if stat["closest_game_margin"] is not None else None
            ),
            "closest_game_season": stat["closest_game_season"],
            "closest_game_week": stat["closest_game_week"],
        }
        for (a, b), stat in pairs.items()
    ]

    # ════════════════════════════════════════════════════════════════
    # View 4 — Records Book (ten all-time kinds)
    # ════════════════════════════════════════════════════════════════
    records_book: list[dict[str, Any]] = []

    def _record(kind: str, value: Any, owner_guid: Any, season: int | None, week: int | None, detail: str) -> None:
        owner_obj = owners.inc_dict.get(owner_guid) if owner_guid else None
        records_book.append(
            {
                "kind": kind,
                "value": round2(value),
                "owner_guid": owner_guid,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
                "season": season,
                "week": week,
                "detail": detail,
            }
        )

    def _opponent_name(tg: Any) -> str:
        opp = owners.inc_dict.get(tg.opponent_owner_guid) if tg.opponent_owner_guid else None
        return opp.display_name if opp else "Unknown"

    all_team_games = list(team_games) if team_games else []

    if all_team_games:
        highest = max(all_team_games, key=operator.attrgetter("score"))
        _record(
            "highest_single_week_score",
            highest.score,
            highest.owner_guid,
            highest.season,
            highest.week,
            f"vs {_opponent_name(highest)}",
        )
        lowest = min(all_team_games, key=operator.attrgetter("score"))
        _record(
            "lowest_single_week_score",
            lowest.score,
            lowest.owner_guid,
            lowest.season,
            lowest.week,
            f"vs {_opponent_name(lowest)}",
        )

    winner_games = [tg for tg in all_team_games if tg.result == "W"]
    if winner_games:
        biggest = max(winner_games, key=operator.attrgetter("margin"))
        _record(
            "largest_margin_of_victory",
            biggest.margin,
            biggest.owner_guid,
            biggest.season,
            biggest.week,
            f"beat {_opponent_name(biggest)} by {round2(biggest.margin)}",
        )
        narrowest = min(winner_games, key=operator.attrgetter("margin"))
        _record(
            "narrowest_margin_of_victory",
            narrowest.margin,
            narrowest.owner_guid,
            narrowest.season,
            narrowest.week,
            f"beat {_opponent_name(narrowest)} by {round2(narrowest.margin)}",
        )

    played_standings = [s for s in standings if (s.wins + s.losses + s.ties) > 0]
    if played_standings:
        best = max(played_standings, key=operator.attrgetter("win_pct_equiv"))
        _record(
            "best_season_record",
            best.win_pct_equiv,
            best.primaryOwner,
            best.season,
            None,
            f"{best.wins}-{best.losses}-{best.ties}",
        )
        worst = min(played_standings, key=operator.attrgetter("win_pct_equiv"))
        _record(
            "worst_season_record",
            worst.win_pct_equiv,
            worst.primaryOwner,
            worst.season,
            None,
            f"{worst.wins}-{worst.losses}-{worst.ties}",
        )

        highest_pf = max(played_standings, key=operator.attrgetter("points_for"))
        _record(
            "highest_season_points_for",
            highest_pf.points_for,
            highest_pf.primaryOwner,
            highest_pf.season,
            None,
            f"{highest_pf.points_for:.2f} points",
        )
        lowest_pf = min(played_standings, key=operator.attrgetter("points_for"))
        _record(
            "lowest_season_points_for",
            lowest_pf.points_for,
            lowest_pf.primaryOwner,
            lowest_pf.season,
            None,
            f"{lowest_pf.points_for:.2f} points",
        )

    if all_team_games:
        best_win = max(all_team_games, key=operator.attrgetter("longest_win_streak"))
        if best_win.longest_win_streak:
            _record(
                "longest_win_streak",
                best_win.longest_win_streak,
                best_win.owner_guid,
                best_win.season,
                best_win.week,
                f"{best_win.longest_win_streak} straight wins",
            )
        best_loss = max(all_team_games, key=operator.attrgetter("longest_loss_streak"))
        if best_loss.longest_loss_streak:
            _record(
                "longest_loss_streak",
                best_loss.longest_loss_streak,
                best_loss.owner_guid,
                best_loss.season,
                best_loss.week,
                f"{best_loss.longest_loss_streak} straight losses",
            )

    # ════════════════════════════════════════════════════════════════
    # View 5 — Draft Tendencies (three kinds)
    # ════════════════════════════════════════════════════════════════
    draft_tendencies: list[dict[str, Any]] = []
    position_counts: dict[tuple[str, str], int] = defaultdict(int)
    owner_by_pair: dict[str, Any] = {}
    for p in draft_picks or []:
        if p.roundId != 1:
            continue
        standing = standings.inc_dict.get(p.team_key) if p.team_key else None
        if not standing or not standing.primaryOwner:
            continue
        owner_by_pair[standing.primaryOwner] = owners.inc_dict.get(standing.primaryOwner)
        player = player_names.inc_dict.get(p.playerId) if player_names else None
        position = player.position if player else "UNKNOWN"
        position_counts[(standing.primaryOwner, position)] += 1

    for (owner_guid, position), count in position_counts.items():
        owner_obj = owner_by_pair.get(owner_guid)
        draft_tendencies.append(
            {
                "kind": "round1_position_mix",
                "owner_guid": owner_guid,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
                "position": position,
                "count": count,
            }
        )

    draft_counts = Counter(p.playerId for p in draft_picks) if draft_picks else Counter()
    for rank, (player_id, times_drafted) in enumerate(draft_counts.most_common(TOP_N_MOST_DRAFTED), start=1):
        player = player_names.inc_dict.get(player_id) if player_names else None
        draft_tendencies.append(
            {
                "kind": "most_drafted",
                "rank": rank,
                "player_id": player_id,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
                "times_drafted": times_drafted,
            }
        )

    for p in draft_picks or []:
        if p.overallPickNumber != 1:
            continue
        standing = standings.inc_dict.get(p.team_key) if p.team_key else None
        owner_obj = owners.inc_dict.get(standing.primaryOwner) if standing and standing.primaryOwner else None
        player = player_names.inc_dict.get(p.playerId) if player_names else None
        draft_tendencies.append(
            {
                "kind": "first_overall",
                "season": p.season,
                "owner_guid": standing.primaryOwner if standing else None,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
                "player_id": p.playerId,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 6 — Settings Evolution
    # ════════════════════════════════════════════════════════════════
    settings_evolution: list[dict[str, Any]] = []
    for s in seasons:
        raw_settings = s.settings.model_dump(by_alias=True)
        schedule_settings = raw_settings.get("scheduleSettings") or {}
        roster_settings = raw_settings.get("rosterSettings") or {}
        scoring_settings = raw_settings.get("scoringSettings") or {}
        division_names = division_names_from_schedule(schedule_settings.get("divisions", []))
        settings_evolution.append(
            {
                "season": s.season,
                "league_size": len(s.teams),
                "playoff_team_count": schedule_settings.get("playoffTeamCount", 0),
                "playoff_seeding_rule": schedule_settings.get("playoffSeedingRule", ""),
                "ppr_points": ppr_points_from_scoring(scoring_settings.get("scoringItems", [])),
                "division_names": division_names,
                "division_count": len(division_names),
                "roster_slots": roster_settings.get("lineupSlotCounts", {}),
            }
        )

    return {
        "FranchiseCard": franchise_cards,
        "SeasonTimelineRow": season_timeline,
        "RivalryRow": rivalry_matrix,
        "RecordRow": records_book,
        "DraftTendencyRow": draft_tendencies,
        "SettingsRow": settings_evolution,
    }
