***

> 📎 **Appendix — Multi-season one-shot ETL.** Fetches every reachable
> season of an ESPN Fantasy Football league exactly once (no Watershed --
> see [Section 5](#5-why-a-one-shot-script-not-a-watershed) for why) and
> fuses standings, matchups, members, and drafts into six all-time
> analytical views. If you're new to the `payload_list=` in-memory
> passthrough, read
> [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md)
> first.

***

# ESPN Fantasy Football League History: Six-View Franchise Almanac

A research walkthrough over an entire league's history: standings, matchups,
members, drafts, and settings across every reachable season, fused into
per-franchise all-time rollups, a season-by-season timeline, a rivalry
matrix, a ten-kind records book, three-kind draft tendencies, and a
settings-evolution log -- six NDJSON files plus a console report.

```
League 899513 -- public (no cookies)
Discovering reachable seasons via status.previousSeasons ...
  season 2026: OK (10 teams, 70 matchups)
  season 2018: unavailable (no cookies) -- skipping
  season 2019: unavailable (no cookies) -- skipping
  season 2020: OK (10 teams, 70 matchups)
  ...

Fetched 7 season(s): [2020, 2021, 2022, 2023, 2024, 2025, 2026]
Loaded 72 standings, 556 matchups, 13 owner records.
Resolving player names for round-1 picks + top drafted players ...
Resolved 49 player names.
Loaded 1270 draft picks.

Wrote 6 views to .../out:
  franchise_cards.ndjson    13 rows
  season_timeline.ndjson    72 rows
  rivalry_matrix.ndjson     75 rows
  records_book.ndjson       10 rows
  draft_tendencies.ndjson   54 rows
  settings_evolution.ndjson 7 rows

FRANCHISE CARDS (all-time, sorted by win%)
FRANCHISE               W-L-T          WIN%  SEASONS  TITLES  PLAYOFF%
----------------------------------------------------------------------
longhorn0010            53-29-0       0.646        7       1     71.4%
...
```

## 1. Two auth modes, one pipeline

| Env var | Required? | Effect |
|---|---|---|
| `ESPN_LEAGUE_ID` | No -- defaults to `899513` | The league to fetch. `899513` is a third-party public demo league; swap in your own league's numeric ID (visible in the ESPN Fantasy web app URL: `.../football/league?leagueId=<ID>`). |
| `ESPN_S2` | No | Browser cookie value. Unlocks a private league and the cookie-gated `leagueHistory` endpoint (seasons ESPN's `status.previousSeasons` reports but that 401 through the modern endpoint without cookies). |
| `ESPN_SWID` | No | Browser cookie value, paired with `ESPN_S2`. Both must be set together -- setting only one leaves the pipeline in public (no-cookie) mode. |

**Extracting `ESPN_S2` / `ESPN_SWID` from your browser** (Chrome/Edge DevTools):
1. Log into [fantasy.espn.com](https://fantasy.espn.com) and open your league.
2. Open DevTools (F12) -> **Application** tab -> **Cookies** -> `https://fantasy.espn.com`.
3. Copy the `espn_s2` value (long alphanumeric string) and the `SWID` value (a brace-wrapped GUID, e.g. `{ABCD1234-...}`) into `ESPN_S2` / `ESPN_SWID`.

The demo league `899513`'s own unauthenticated floor is season 2020 -- seasons
2018/2019 exist (ESPN's `status.previousSeasons` reports them) but 401
through the modern endpoint without cookies, so the public run skips them
with a printed note. This is a fact about the demo league's own history, not
a hardcoded floor in the code -- **season discovery is entirely
server-declared** (Section 4).

## 2. File layout

```
examples/appendix/espn-league-history/
  espn_league_history.py   # the whole pipeline -- one file, no sidecars
  README.md                # this file
  out/                      # gitignored -- six NDJSON views land here
```

No `watershed.json`, `outflow.py`, or `inflow.py` -- see Section 5 for why
this appendix is a one-shot script rather than a Watershed.

## 3. The six views

| View | File | Shape |
|---|---|---|
| 1. Franchise Cards | `franchise_cards.ndjson` | One row per owner, all-time: W-L-T, win%, PF/PA, seasons played, average finish, championships/runner-ups/last-places, playoff appearances + rate. |
| 2. Season Timeline | `season_timeline.ndjson` | One row per franchise-season: seed -> final rank, record, PF/PA, division, all-play expected wins, luck delta. |
| 3. Rivalry Matrix | `rivalry_matrix.ndjson` | One row per franchise pair, all-time: meetings, W-L, playoff meetings, biggest blowout, closest game. |
| 4. Records Book | `records_book.ndjson` | Ten all-time record kinds (Section 3a). |
| 5. Draft Tendencies | `draft_tendencies.ndjson` | Three kinds: round-1 position mix per franchise, all-time most-drafted players (top 15), first-overall honor roll. |
| 6. Settings Evolution | `settings_evolution.ndjson` | One row per season: league size, playoff format, PPR adoption, roster slots, division eras. |

### 3a. The ten records-book kinds

`highest_single_week_score`, `lowest_single_week_score`,
`largest_margin_of_victory`, `narrowest_margin_of_victory`,
`best_season_record`, `worst_season_record`, `highest_season_points_for`,
`lowest_season_points_for`, `longest_win_streak`, `longest_loss_streak`.

Every kind filters to **decided** matchups first (`winner != "UNDECIDED"`):
a playoff bye (`home`-only, no `away` key, permanently `UNDECIDED`) and an
in-progress week (both sides `0.0`, `UNDECIDED`) never register. A genuine
0-point week and a tiebreak-decided tie (`winner` still resolves to a side;
margin can legitimately be `0.0`) both DO register -- that distinction is
the whole reason the filter is on `winner`, not on a `totalPoints > 0`
check. Win/loss streaks are franchise-history streaks, chronological across
every fetched season (`(season, matchupPeriodId)` order), not reset at a
season boundary.

## 4. Season discovery is server-declared

No brute-force floor/ceiling guessing. One bootstrap fetch -- the current
calendar-year season -- reads `status.previousSeasons` off the response and
unions it with the current year; that union IS the season list. For each
season in that list, the script tries the modern per-season endpoint first;
any failed fetch (commonly a 404 for a season outside the modern endpoint's
own coverage window, sometimes a 401) retries the same season against the
cookie-gated `leagueHistory` endpoint (list-root response, `rec_path="0"`)
whenever cookies are present; a failed fetch with no cookies present is
skipped immediately with a printed note. League size is read from each
season's own `len(season.teams)` -- never hardcoded, since a league's team
count can and does drift across seasons.

A fetch failure surfaces through `IncorporatorList.rejects` (structured
`RejectEntry` records with `.status_code`), not a raised exception -- the
season-discovery loop checks `if not rows:` and retries against
`leagueHistory` whenever cookies are present, regardless of which status
code came back. The retry decision never inspects `.status_code` at all;
it depends solely on whether cookies are present.

## 5. Why a one-shot script, not a Watershed

Every season is fetched exactly once, ever -- there is no polling axis. A
Watershed's entire value proposition is repeated ticks against a moving
window, and nothing here moves: this is static historical data.
[Tutorial 6 -- State Sports](../../06-state-sports/README.md) is the shipped
precedent for this shape: a pure one-shot script, no Watershed, classes
defined once, network calls made exactly as many times as the data
genuinely requires.

The six views are cardinality-reducing group-bys (per-owner rollups across
N seasons, per-pair rivalry aggregation, top-N draft counts). `calc_all`
*can* roll up all of it declaratively -- but only within the scope of ONE
`incorp()` call, and only that far: it runs once, against the full column
lists of whatever rows THAT call built. `Owner`/`Standing`/`Matchup`/
`DraftPick` are each built with exactly ONE batched `incorp(payload_list=
...)` call spanning every fetched season (never inside the season-discovery
loop), so every `calc_all` entry in `Standing`'s own `conv_dict` sees the
WHOLE history in one column-wide pass -- all-time win/loss totals,
championships, average finish, and win% all broadcast onto every row
declaratively, no hand-rolled fold. A `TeamGame` reshape (one row per team
per decided matchup -- the one unavoidable de-nesting, since ESPN ships
matchups as home/away pairs and every cross-row stat here is team-scoped)
does the same for all-play expected wins and franchise-history win/loss
streaks. What `calc_all` genuinely cannot do is REDUCE row count across a
join that spans two DIFFERENT `incorp()` calls -- Franchise Cards'
playoff-appearances count needs `Standing` owners joined against `Matchup`
bracket appearances from a separate call, and the pairwise rivalry matrix /
draft-tendency counts collapse many rows into fewer. Those three stay
plain Python, called inline in `main()` right before each view's own
`Cls.incorp(payload_list=...)` + `Cls.export(...)` -- the return-twin of a
Fjord's `outflow(state)`, without a Fjord's tick machinery.

## 6. `payload_list=` and `model_dump(by_alias=True)`

Every sub-collection (`teams`, `schedule`, `members`, `draftDetail.picks`)
is pulled off EVERY fetched `Season` row, flattened into one list spanning
every season (each row stamped with `"season": s.season` inline via dict-
comprehension unpacking -- `{**t.model_dump(by_alias=True), "season":
s.season}`), and handed to a sibling class's `incorp(payload_list=...)` --
a network-free, in-memory passthrough (`incorporator/base.py`'s
payload-only mode): one dict entry in, one row out, through the full
`conv_dict` pipeline, called exactly ONCE per class rather than once per
season. Because `Season`'s own `conv_dict` runs `calc(list, ...)` /
`calc(dict, ...)` against nested JSON, the framework's dynamic schema
builder auto-promotes each nested dict into its own typed submodel (dotted
attribute access) -- so `season.teams` is a list of submodel instances, not
plain dicts, by the time the loop reads it. `t.model_dump(by_alias=True)`
flattens each back to a plain dict before the `payload_list=` call.
`by_alias=True` matters specifically for `rosterSettings.lineupSlotCounts`:
its keys are numeric STRINGS (`"0"`, `"2"`, ..., `"23"`), and the
auto-promoted submodel sanitizes those into Python attribute names
(`field_0`, `field_2`, ...) -- `model_dump(by_alias=True)` exports the
ORIGINAL string keys instead of the sanitized attribute names.
`SettingsRow.roster_slots` stays a plain dict read directly off
`season.settings.model_dump(by_alias=True)` via `calc(dict, "rosterSettings
.lineupSlotCounts", ...)`, never its own `Incorporator` row class (a row
class with numeric-string field names would crash schema inference).

A bare ESPN `team.id` repeats every season -- once every season's `Standing`
rows share one `incorp()` call, a bare `id` would collide in `inc_dict`.
`team_key(season, team_id)` synthesizes a `"{season}:{team_id}"` composite
join key via `calc()`, and `Standing`'s `inc_code="team_key"` makes that
composite the registry key every downstream `link_to()` resolves against
(`Matchup.home_standing`/`away_standing`, `DraftPick.standing`).

## 7. Player names: season-matched, batched, targeted

Draft picks carry only a numeric `playerId` (negative for D/ST, e.g.
`-16003` for a Bears defense) -- resolving a name requires a season-matched
call to ESPN's `players_wl` endpoint (`GET
.../seasons/{S}/players?view=players_wl` with an `X-Fantasy-Filter` header
listing the wanted IDs); old IDs don't resolve against a newer season's
player universe. The pipeline makes two passes:

1. **Per-season, round-1 only.** For each fetched season, batch every
   round-1 `playerId` not already resolved into one `PlayerName.incorp()`
   call for that season.
2. **Targeted top-N pass.** The all-time top 15 most-drafted `playerId`s
   (by pick count, any round) that round-1 didn't already resolve are
   grouped by each player's own most-recently-drafted season and fetched
   in a second, smaller batch per season.

Both passes genuinely need one HTTP call per season -- ESPN's player
universe is season-scoped, so this is the one part of the pipeline that
can't batch down to a single `incorp()` call. `DraftPick.player` is a
build-time `link_to(PlayerName)` join (`calc(link_to(PlayerName),
"playerId")`) -- since `PlayerName` is built via several `incorp()` calls
(one per season/batch) rather than one, the join reads the CLASS
(`link_to(PlayerName)`), not a single call's returned list: `model_post_init`
bubbles every built instance's registration up to the base class's own
`inc_dict`, so `link_to(PlayerName)` sees every batch's results regardless
of which call built them.

`defaultPositionId` (not `lineupSlotId`) is the verified position source --
`1=QB`, `2=RB`, `3=WR`, `4=TE`, `5=K`, `16=D/ST`, computed ONCE per player at
`PlayerName`'s own build time via `calc(position_name, "defaultPositionId",
...)`; an unmapped id falls back to a labelled placeholder (`POS_<id>`)
rather than crashing. Every read site then takes `p.player.position if
p.player else "UNKNOWN"` -- a single conditional-dot guard, not a repeated
`position_name(...)` call. Some very old or vacated draft slots resolve to
a sentinel `playerId` ESPN doesn't map to a real player at all -- that
row's name/position fall back to `"Unknown"`/`"UNKNOWN"` gracefully, the
same fallback used for any unresolved lookup.

## Run it

```bash
# Public demo league, no setup required
python examples/appendix/espn-league-history/espn_league_history.py

# Your own league, or a private one
ESPN_LEAGUE_ID=1234567 ESPN_S2=... ESPN_SWID='{...}' \
  python examples/appendix/espn-league-history/espn_league_history.py
```

Also runs in Docker via the
[central mount pattern](../../README.md#running-a-tutorial-in-docker) (not
run or verified) -- pass the three env vars through with `-e`.

## Where to Go Next

| Goal | Read |
|---|---|
| The one-shot, no-Watershed shape this appendix follows | [Tutorial 6 -- State Sports](../../06-state-sports/README.md) |
| Build rows from data already in memory | [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Universal export formats (`Cls.export(...)`) | [Tutorial 3 -- Universal Formats](../../03-universal-formats/README.md) |
| A read-time multi-source join, as a Fjord instead | [`crypto-graph-mapping`](../crypto-graph-mapping/README.md) |

***

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/espn-league-history/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
