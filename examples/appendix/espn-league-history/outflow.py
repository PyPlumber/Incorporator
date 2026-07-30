"""Outflow sidecar for the ESPN league-history one-shot fjord pipeline.

Defines the seven source classes (``Season``, ``Owner``, ``Standing``,
``Matchup``, ``DraftPick``, ``PlayerName``, ``TeamGame``) and the
``outflow(state)`` function that fuses them into a six-key dict -- fjord's
multi-output contract writes each key to its own file (see
``export_params`` in ``espn_league_history.py``). Five of the six derived
view classes are NOT pre-declared here; fjord builds a dynamic class per
returned dict key and infers its schema from the emitted rows. ``RecordRow``
is the one exception -- see its own docstring below.

Every value a source's OWN ``conv_dict`` can already compute -- canonical
rivalry a/b orientation on ``Matchup``, export-precision rounding, ``Season``'s
settings-evolution fields, ``TeamGame.luck_delta`` -- is computed there, at
``incorp()`` time, not here. What's left for ``outflow(state)`` are the joins
that genuinely need read-time resolution (an owner GUID resolved to
``Owner.display_name`` -- ``Owner`` is a sibling ``stream_params`` entry with
no seeding-order guarantee relative to the sources that reference its GUIDs,
so no build-time ``link_to`` is used anywhere) and the per-row-count-changing
folds a ``conv_dict`` cannot express at all: rivalry-pair accumulation, the
records-book max/min selections, and draft position-mix counts. Every
cross-class join here reads ``state["Peer"].inc_dict.get(key)`` (the
``cls.fjord()`` daemon path).

Every source's own static coercion lives in its own ``conv_dict``, written
inline in ``espn_league_history.py``'s ``stream_params`` entries -- the
domain-calc functions those ``conv_dict`` entries call are defined here and
imported into the entry file, same split as
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


def round2(value: Any) -> float:
    """`target_type=` accepts any 1-arg callable, not just a `type` --
    export precision belongs where a value is COMPUTED, not where it's read."""
    return round(float(value), 2)


def round3(value: Any) -> float:
    return round(float(value), 3)


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


def perspective_result(winner: str, side: str) -> str:
    if winner not in ("HOME", "AWAY"):
        return "T"
    return "W" if winner == side.upper() else "L"


def abs_diff(a: float, b: float) -> float:
    return abs(a - b)


def _away_is_a(home_owner_guid: str | None, away_owner_guid: str | None) -> bool:
    """Canonical rivalry orientation: the alphabetically-lower owner GUID is
    always side "a" -- `owner_home.id < owner_away.id`'s old read-time
    comparison, moved to build time. `away_owner_guid` is `None` on a
    playoff bye, which always sorts "a" to home."""
    return away_owner_guid is not None and away_owner_guid < home_owner_guid


def canonical_owner_a(home_owner_guid: str | None, away_owner_guid: str | None) -> str | None:
    return away_owner_guid if _away_is_a(home_owner_guid, away_owner_guid) else home_owner_guid


def canonical_owner_b(home_owner_guid: str | None, away_owner_guid: str | None) -> str | None:
    return home_owner_guid if _away_is_a(home_owner_guid, away_owner_guid) else away_owner_guid


def canonical_score_a(
    home_owner_guid: str | None, away_owner_guid: str | None, home_points: float | None, away_points: float | None
) -> float:
    points = away_points if _away_is_a(home_owner_guid, away_owner_guid) else home_points
    return points if points is not None else 0.0


def canonical_score_b(
    home_owner_guid: str | None, away_owner_guid: str | None, home_points: float | None, away_points: float | None
) -> float:
    """Always returns a real float, never `None` -- a playoff bye's missing
    away side must not reach `abs_diff` as `None`: `calc()`'s garbage
    short-circuit only fires when EVERY input is garbage, and here
    `home_owner_guid`/`home_points` are real, so `func` still runs and a
    `None` return would raise `TypeError` inside `margin`'s `abs_diff`."""
    points = home_points if _away_is_a(home_owner_guid, away_owner_guid) else away_points
    return points if points is not None else 0.0


def canonical_a_won(home_owner_guid: str | None, away_owner_guid: str | None, winner: str) -> bool:
    return winner == "AWAY" if _away_is_a(home_owner_guid, away_owner_guid) else winner == "HOME"


def luck_delta_fn(wins: int, ties: int, all_play_expected_wins: float) -> float:
    return wins + 0.5 * ties - all_play_expected_wins


def win_pct_equiv(wins: int, losses: int, ties: int) -> float:
    """Per-SEASON win rate (ties = half a win) -- Records Book's best/worst-
    season kinds. Distinct from `win_pct_from_totals` (all-time, franchise cards)."""
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
    """Factory: a `calc_all` reducer computing each owner's RUNNING
    consecutive-`target_result` count, chronological across every season."""

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


def ppr_points_from_scoring(scoring_items: list[dict[str, Any]]) -> float:
    """`pointsOverrides` may be null OR the key entirely absent -- both
    guarded, plus the third branch (present WITH an override)."""
    ppr_item = next((item for item in scoring_items if item.get("statId") == 53), None)
    if ppr_item is None:
        return 0.0
    overrides = ppr_item.get("pointsOverrides") or {}
    return overrides.get("16", ppr_item.get("points", 0.0))


def division_names_from_raw(divisions: list[dict[str, Any]]) -> list[str]:
    return [d.get("name", "") for d in divisions]


def roster_slots_from_raw(lineup_slot_counts: dict[str, Any]) -> dict[str, int]:
    """`None`-filtered passthrough, not just `dict(...)`.

    This conv_dict entry re-runs a SECOND time when `Season` reseeds the
    fjord from `payload_list=[s.model_dump(...) for s in all_seasons]` --
    at that point its source, `settings.rosterSettings.lineupSlotCounts`, no
    longer comes from raw ESPN JSON but from a Season instance's own
    reconstructed `settings` field (still schema-inferred, since only
    `roster_slots` itself is declared, not `settings`). When two seasons in
    the SAME `incorp()` batch have different key sets at that nested path,
    `infer_dynamic_schema`'s cross-record sample union
    (`incorporator/schema/builder.py:395-427`) builds ONE shared schema from
    whichever record's keys got sampled first, and back-fills the other
    season's missing keys with `None` on `model_dump()` (run-verified
    2026-07-30: a 3-key season sharing a batch with an 8-key season came
    back with the 5 extra keys present as `None`, not absent). Declaring
    `Season.roster_slots: dict[str, int]` (not `int | None`) then rejects
    those `None`s outright. Filtering here keeps every season's true key
    set intact through both passes."""
    return {k: v for k, v in lineup_slot_counts.items() if v is not None}


# ---------------------------------------------------------------------------
# Source classes -- each fjord source needs its own subclass; none carries a
# build-time link_to (every cross-class join is read-time, in outflow()
# below -- Owner is a sibling stream_params entry with no seeding-order
# guarantee). Field declarations here would silently coerce types, so every
# source's own conv_dict (in espn_league_history.py) owns coercion instead --
# `Season.roster_slots` below is the one deliberate exception.
# ---------------------------------------------------------------------------


class Season(Incorporator):
    """One ESPN league-season response -- modern dict-root or historical
    list-root (`rec_path="0"`), same `conv_dict` either way.

    `roster_slots` is declared (not left to schema inference) because its
    source, `settings.rosterSettings.lineupSlotCounts`, is a dict keyed by
    digit strings ("0", "2", "20", ...). `infer_dynamic_schema` promotes any
    undeclared dict-valued field into a nested submodel
    (`incorporator/schema/builder.py:453-455`), and that submodel's field
    names run through `sanitize_json_key`'s digit-prefix rescue, mangling
    "0" into "field_0" (run-verified 2026-07-30). Declaring the field here
    skips inference entirely (`builder.py:450-451`'s `base_class.model_fields`
    check) so the dict's keys survive untouched -- same escape hatch as
    `RecordRow.value` below."""

    roster_slots: dict[str, int] | None = None


class Owner(Incorporator):
    """One league member (GUID + display name), built network-free off every
    season's `members` in ONE batched `payload_list=` call."""


class Standing(Incorporator):
    """One team's season-long record, batched network-free off every
    season's `teams`. `inc_code="team_key"` (a `"{season}:{team_id}"` composite)."""


class Matchup(Incorporator):
    """One scheduled/played game, batched network-free off every season's
    `schedule`. A playoff bye surfaces as `m.away is None`, not an erased
    sentinel. `owner_a`/`owner_b`/`score_a`/`score_b`/`a_won`/`margin` are the
    rivalry view's canonical home/away-independent orientation, built once
    here instead of per-row inside outflow()'s rivalry fold."""


class DraftPick(Incorporator):
    """One draft pick, batched network-free off every season's `draft_picks`."""


class PlayerName(Incorporator):
    """Resolved player name + position -- the ONE genuinely-networked fjord
    source, fanned out over every season's `players_wl` endpoint."""


class TeamGame(Incorporator):
    """One row per team per DECIDED matchup (home + away perspective).
    `inc_code="team_key"` lets Season Timeline read each team-season's
    broadcast all-play total and luck delta back via
    `TeamGame.inc_dict.get(...)`."""


class RecordRow(Incorporator):
    """One records-book entry (view 4) -- the one derived view pre-declared,
    since its ten kinds share one `value` key across float and int kinds and
    a bare class's schema inference types that key from its first row only
    (run-verified 2026-07-29: `longest_win_streak` exported as `10.0`).
    `extra="allow"` is required here (not inherited) -- declaring `value`
    alone, without it, silently drops every other emitted field on
    `model_validate` (run-verified 2026-07-29: `display_name`/`owner_guid`/
    etc. vanished from the exported row)."""

    model_config = ConfigDict(extra="allow")
    value: int | float | None = None


# ---------------------------------------------------------------------------
# Outflow -- six derived views from one fused, read-time-joined state.
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return-twin of the original pipeline's six view-building blocks --
    reads `state["Season"|"Owner"|"Standing"|"Matchup"|"DraftPick"|
    "PlayerName"|"TeamGame"]` directly. Every cross-class join is
    `PeerClass.inc_dict.get(key)`, read-time, against the live snapshot
    `fjord()` hands this function."""
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
    # Playoff-appearance count-distinct: Matchup already carries owner_a/
    # owner_b (build-time, canonical orientation) so this needs no Standing
    # lookup at all -- and reuses count_distinct_by_group (the same calc_all
    # broadcast standing_conv_dict already uses for owner_seasons_played)
    # as a plain function call instead of a hand-rolled defaultdict(set) fold.
    playoff_owner_seasons: list[str] = []
    playoff_season_values: list[int] = []
    for m in matchups or []:
        if m.playoffTierType != "WINNERS_BRACKET":
            continue
        for owner_guid in (m.home_owner_guid, m.away_owner_guid):
            if owner_guid:
                playoff_owner_seasons.append(owner_guid)
                playoff_season_values.append(m.season)
    playoff_by_owner_count = dict(
        zip(playoff_owner_seasons, count_distinct_by_group(playoff_owner_seasons, playoff_season_values), strict=True)
    )

    # owner_wins_total/losses/ties/points_for/points_against/seasons_played/
    # average_finish/championships/runner_ups/last_places are all verbatim
    # calc_all outputs already computed by standing_conv_dict -- this view
    # only reads them, no re-derivation and no read-time rounding (win_pct
    # and points already carry their export precision from the source
    # conv_dict; playoff_rate is the one ratio that genuinely can't move
    # upstream, since its two inputs -- the Matchup fold above and Standing's
    # own calc_all -- live on different sources with no safe one-directional
    # build-time ordering between them).
    last_standing_by_owner = {s.primaryOwner: s for s in standings if s.primaryOwner}
    franchise_cards: list[dict[str, Any]] = []
    for owner_guid, s in last_standing_by_owner.items():
        owner_obj = owners.inc_dict.get(owner_guid)
        appearances = playoff_by_owner_count.get(owner_guid, 0)
        franchise_cards.append(
            {
                "owner_guid": owner_guid,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
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
                "playoff_appearances": appearances,
                "playoff_rate": round(appearances / s.owner_seasons_played if s.owner_seasons_played else 0.0, 3),
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 2 — Season Timeline
    # ════════════════════════════════════════════════════════════════
    season_timeline: list[dict[str, Any]] = []
    for s in standings:
        owner_obj = owners.inc_dict.get(s.primaryOwner) if s.primaryOwner else None
        tg = team_games.inc_dict.get(s.team_key) if team_games else None
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
                "all_play_expected_wins": tg.all_play_expected_wins if tg else 0.0,
                "luck_delta": tg.luck_delta if tg else 0.0,
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 3 — Rivalry Matrix
    # ════════════════════════════════════════════════════════════════
    # Matchup already carries the canonical a/b orientation (owner_a/owner_b/
    # score_a/score_b/a_won/margin -- build-time, home/away-independent), so
    # this fold is now a plain reduction over already-rounded fields; no
    # per-row home/away swap. max/min of pre-rounded (2dp) values equals
    # round(max/min(raw), 2) since rounding is monotonic.
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for m in matchups or []:
        if m.winner == "UNDECIDED" or m.owner_a is None or m.owner_b is None or m.owner_a == m.owner_b:
            continue
        owner_a_obj = owners.inc_dict.get(m.owner_a)
        owner_b_obj = owners.inc_dict.get(m.owner_b)
        stat = pairs.setdefault(
            (m.owner_a, m.owner_b),
            {
                "display_name_a": owner_a_obj.display_name if owner_a_obj else "Unknown",
                "display_name_b": owner_b_obj.display_name if owner_b_obj else "Unknown",
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
        stat["wins_a" if m.a_won else "wins_b"] += 1
        if m.playoffTierType != "NONE":
            stat["playoff_meetings"] += 1
        if m.margin > stat["biggest_blowout_margin"]:
            stat["biggest_blowout_margin"] = m.margin
            stat["biggest_blowout_season"] = m.season
            stat["biggest_blowout_week"] = m.matchupPeriodId
        if stat["closest_game_margin"] is None or m.margin < stat["closest_game_margin"]:
            stat["closest_game_margin"] = m.margin
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
            "biggest_blowout_margin": stat["biggest_blowout_margin"],
            "biggest_blowout_season": stat["biggest_blowout_season"],
            "biggest_blowout_week": stat["biggest_blowout_week"],
            "closest_game_margin": stat["closest_game_margin"],
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
        # No round() here: every value passed in already carries its export
        # precision from the source conv_dict that produced it (score/margin/
        # win_pct_equiv/points_for are all round2 at build time; streak
        # counts are plain ints).
        owner_obj = owners.inc_dict.get(owner_guid) if owner_guid else None
        records_book.append(
            {
                "kind": kind,
                "value": value,
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
            f"beat {_opponent_name(biggest)} by {biggest.margin}",
        )
        narrowest = min(winner_games, key=operator.attrgetter("margin"))
        _record(
            "narrowest_margin_of_victory",
            narrowest.margin,
            narrowest.owner_guid,
            narrowest.season,
            narrowest.week,
            f"beat {_opponent_name(narrowest)} by {narrowest.margin}",
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
    # p.owner_guid is threaded onto the pick row upstream (same
    # owner_by_team_key_raw pattern as Matchup's home/away_owner_guid), so
    # this needs no Standing lookup for the join -- only the read-time Owner
    # display-name resolution remains, since Owner is a sibling
    # stream_params entry with no seeding-order guarantee.
    draft_tendencies: list[dict[str, Any]] = []
    position_counts: dict[tuple[str, str], int] = defaultdict(int)
    for p in draft_picks or []:
        if p.roundId != 1 or not p.owner_guid:
            continue
        player = player_names.inc_dict.get(p.playerId) if player_names else None
        position = player.position if player else "UNKNOWN"
        position_counts[(p.owner_guid, position)] += 1

    for (owner_guid, position), count in position_counts.items():
        owner_obj = owners.inc_dict.get(owner_guid)
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
        owner_obj = owners.inc_dict.get(p.owner_guid) if p.owner_guid else None
        player = player_names.inc_dict.get(p.playerId) if player_names else None
        draft_tendencies.append(
            {
                "kind": "first_overall",
                "season": p.season,
                "owner_guid": p.owner_guid,
                "display_name": owner_obj.display_name if owner_obj else "Unknown",
                "player_id": p.playerId,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
            }
        )

    # ════════════════════════════════════════════════════════════════
    # View 6 — Settings Evolution
    # ════════════════════════════════════════════════════════════════
    # Every field here is now a build-time Season conv_dict output -- no
    # model_dump()/raw-dict digging, just a dict-literal projection (the same
    # shape as every other view's row-building).
    settings_evolution = [
        {
            "season": s.season,
            "league_size": s.league_size,
            "playoff_team_count": s.playoff_team_count,
            "playoff_seeding_rule": s.playoff_seeding_rule,
            "ppr_points": s.ppr_points,
            "division_names": s.division_names,
            "division_count": s.division_count,
            "roster_slots": s.roster_slots,
        }
        for s in seasons
    ]

    return {
        "FranchiseCard": franchise_cards,
        "SeasonTimelineRow": season_timeline,
        "RivalryRow": rivalry_matrix,
        "RecordRow": records_book,
        "DraftTendencyRow": draft_tendencies,
        "SettingsRow": settings_evolution,
    }
