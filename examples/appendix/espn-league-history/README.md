***

> 📎 **Appendix — Multi-season one-shot fjord ETL.** Fetches every reachable
> season of an ESPN Fantasy Football league exactly once and fuses
> standings, matchups, members, and drafts into six all-time analytical
> views via a one-shot `Incorporator.fjord()` (no Watershed -- see
> [Section 5](#5-why-a-one-shot-fjord-not-a-watershed) for why). If you're
> new to the `payload_list=` in-memory passthrough, read
> [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md)
> first; if you're new to `fjord()` itself, read
> [Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
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

Fetched 7 season(s): [2020, 2021, 2022, 2023, 2024, 2025, 2026]
  unresolved seasons (no data available): [2018, 2019]
Running one-shot fjord: Owner/Standing/Matchup/DraftPick network-free, PlayerName fanned out ...

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
longhorn0010            53-29-0       0.646        6       1     83.3%
...
```

(`draft_tendencies.ndjson`'s row count drifts season to season as a season
enters or leaves the `first_overall` honor roll while its draft completes --
see Section 7 for the vacant-pick sentinel behind that.)

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
through the modern endpoint without cookies, so the public run leaves them
out of the final season list (printed once, as a set difference at the end
of discovery). Season discovery is entirely server-declared (Section 4).

## 2. File layout

```
examples/appendix/espn-league-history/
  espn_league_history.py   # season discovery (plain Python) + the fjord() call + the console report
  outflow.py                # 6 source classes + 6 bare view classes + outflow(state) -- the
                             # one declared field in the whole file is Season.roster_slots
                             # (Section 6), a source-side concern, not a view-class one
  README.md                # this file
  out/                      # gitignored -- six NDJSON views land here
```

No `watershed.json` -- see Section 5 for why this appendix is a one-shot
`Incorporator.fjord()` rather than a Watershed. Unlike a Watershed example,
where every `Incorporator` subclass lives in the main entry file and a
sidecar is a pure re-import store, a `cls.fjord()` daemon example (this one)
defines ALL its classes in `outflow.py` -- both the six source classes AND
the six derived view classes the fjord builds (see
`examples/09-nascar-fantasy-fjord/`, the template this rewrite follows).

`espn_league_history.py`'s own console report reads three of those view
classes' `inc_dict` back AFTER the fjord loop ends, so the entry file loads
`outflow.py` via one explicit
`incorporator.usercode.load_outflow_module(...)` call rather than a bare
`from outflow import (...)` -- a bare import registers a second,
disconnected copy of the file under Python's own `sys.modules` cache,
distinct from the path-keyed cache `fjord()`'s internal loader uses to
build and park the view classes' rows, so a bare-imported class's
`inc_dict` would come back silently empty post-loop.
`load_outflow_module` shares that same path-keyed cache, so calling it
explicitly here, once, before `fjord()` runs, guarantees both loads
resolve to the identical module and class objects.

## 3. The six views

| View | File | Shape |
|---|---|---|
| 1. Franchise Cards | `franchise_cards.ndjson` | One row per owner, all-time: W-L-T, win%, PF/PA, seasons played (completed only, Section 3b), average finish, championships/runner-ups/last-places, playoff appearances + rate, `has_current_season`. |
| 2. Season Timeline | `season_timeline.ndjson` | One row per franchise-season: seed -> final rank, record, PF/PA, division, all-play expected wins, luck delta, `is_complete`. |
| 3. Rivalry Matrix | `rivalry_matrix.ndjson` | One row per franchise pair, all-time: meetings, W-L, playoff meetings, biggest blowout, closest game. |
| 4. Records Book | `records_book.ndjson` | Ten all-time record kinds (Section 3a). |
| 5. Draft Tendencies | `draft_tendencies.ndjson` | Three kinds: round-1 position mix per franchise, all-time most-drafted players (top 15), first-overall honor roll. |
| 6. Settings Evolution | `settings_evolution.ndjson` | One row per season: league size, playoff format, PPR adoption, roster slots, division eras, `is_complete`. |

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

`RecordRow` is bare, like all six of `outflow.py`'s derived view classes
(Section 2). All ten kinds share one `value` column, but the column's
native type differs by kind: point/margin/ratio floats for seven kinds,
plain win/loss-streak game counts (ints) for the other two. Since
`outflow(state)` appends the float-valued kinds first and the two streak
kinds last, fjord's own dynamic-schema inference locks `value`'s type from
the first-sampled (float) row and coerces the trailing int streak values to
match -- `longest_win_streak` renders as `10.0`, not `10`, in the exported
NDJSON.

### 3b. Completed vs. in-progress seasons

Every row from every season stays in every view, including the current,
still-in-progress year -- nothing is dropped. `Season.is_complete`
(`status.finalScoringPeriod < status.latestScoringPeriod`) flags each
season, and only the three season-COUNTING `FranchiseCard` fields narrow
their denominator to completed seasons: `seasons_played`, `average_finish`,
and `playoff_rate`'s denominator. A franchise whose only season on record is
the in-progress one reads `seasons_played: 0, average_finish: 0.0,
playoff_rate: 0.0` rather than dividing by zero.
`FranchiseCard.has_current_season` marks a franchise that fielded a team in
the still-in-progress year, and `SeasonTimelineRow.is_complete` /
`SettingsRow.is_complete` carry the same per-season flag through for
downstream filtering. Game-level records, streaks, the rivalry matrix,
all-play/luck, and every draft-tendency fold stay fully inclusive -- a
decided game this season is a real result.

## 4. Season discovery is exactly two `Season.incorp` calls

No brute-force floor/ceiling guessing, and no per-year fan-out machinery.
Season discovery is exactly two `Season.incorp` call sites, both shared
identically across auth modes -- mode differences are argument ternaries on
a single fan-out call, never duplicated call sites:

1. **`current_season`** -- one call for the in-progress current-year season
   (the modern endpoint). Reads `status.previousSeasons` off the response;
   that's the candidate-years list, load-bearing for the public branch's
   drill below only (harmless, unused, in private mode).
2. **History/fan-out** -- one call, one statement, mode decided ONCE up
   front from whether cookies are present, no probe, no retry:
   - **With cookies**, ONE `Season.incorp(inc_url=<leagueHistory URL>)`
     call against the cookie-gated `leagueHistory/{league_id}` endpoint
     with NO `seasonId` query param -- its top-level JSON response IS the
     list of every OTHER completed season (excluding the in-progress
     current year already covered by `current_season`), so `incorp()`
     treats the array itself as the record set with no `rec_path`
     drilling at all.
   - **With no cookies**, `leagueHistory` 401s uncookied, so a declarative
     `inc_parent=current_season, inc_child="previous_seasons"` drill fans
     the modern per-season endpoint out instead -- one GET per entry of
     `current_season`'s own `previous_seasons` field, the same
     `inc_parent`/`inc_child` shape as
     [Tutorial 6 -- State Sports](../../06-state-sports/README.md)'s
     per-team drill. No `y != current_year` filter is needed: ESPN's own
     `status.previousSeasons` never includes the in-progress year, and the
     framework does not post-filter parent rows. A modern-endpoint miss on
     an old season is terminal: ESPN gives back the successful years and
     each failed year's URL lands in `.failed_sources`, which the pipeline
     reports (via a plain set-difference against the candidate list) but
     never retries.

League size is read from each season's own `len(season.teams)` -- never
hardcoded, since a league's team count can and does drift across seasons.

A fetch failure surfaces through `IncorporatorList.rejects` /
`.failed_sources` (structured `RejectEntry` records with `.status_code`)
plus the framework's own WARNING-level log line -- never a raised exception.

## 5. Why a one-shot fjord, not a Watershed

Every season is fetched exactly once, ever -- there's no polling axis, and a
Watershed's value proposition is repeated ticks against a moving window. Six
cardinality-reducing views fused from six fjord sources is exactly what
`Incorporator.fjord()` is for, without a Watershed's tick/wave scheduler:
give every `stream_params` entry `refresh_params=None` and omit the
top-level `export_interval`, and `fjord()` seeds every source once, flushes
`outflow(state)` once, and exits.
[Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
is the shipped precedent for this one-shot shape.

The six views (per-owner rollups, rivalry aggregation, records-book
extremes, top-N draft counts) are plain Python inside `outflow.py`'s
`outflow(state)` -- see Section 6 for the read-time joins involved and
Section 2 for why the console report loads `outflow.py` via
`load_outflow_module` rather than a bare top-level import.

## 6. `payload_list=` and read-time joins

Every sub-collection (`teams`, `schedule`, `members`, `draftDetail.picks`)
is pulled off EVERY fetched `Season` row and flattened into one list
spanning every season in `main()`, and handed to a sibling fjord source's
`payload_list=` -- a network-free, in-memory passthrough
(`incorporator/base.py`'s payload-only mode): one dict entry in, one row
out, through the full `conv_dict` pipeline, seeded exactly ONCE per class
by `fjord()` rather than once per season. Each row is an explicit dict
literal built by attribute access, carrying only the fields something
downstream actually consumes:

- **Owner rows** -- 2 keys: `id`, `displayName` -- the pre-rename camelCase
  key `name_chg=[("displayName", "display_name")]` expects; renaming it in
  the literal would silently no-op `name_chg`.
- **Standing rows** -- 12 keys, built in a plain `for` loop (not a
  comprehension) since flattening the nested `record.overall.*` needs a
  local: `rec = t.record.overall if t.record else None`, then
  `rec.wins if rec else 0` (and so on for `losses`/`ties`/`pointsFor`/
  `pointsAgainst`) directly in the literal. `Standing`'s own `conv_dict`
  (below) drills the already-flat `wins`/`losses`/`ties`/`pointsFor`/
  `pointsAgainst` keys, two path tokens shorter than the raw wire shape.
- **Matchup rows** -- flattens `home`/`away` to scalar `home_points`/
  `away_points`, preserving the playoff-bye tri-state: `away_points` is
  `None` for a bye (no `away` key) and `0.0` for a scoreless-but-played
  game (Section 3a). `home.teamId`/`away.teamId` are consumed only at the
  pre-fjord guid-threading step below and never reach the row.
- **DraftPick rows** -- 5 keys: `playerId`, `roundId`, `overallPickNumber`,
  `season`, `owner_guid` (`teamId` is read for the guid lookup but not
  emitted; `keeper`/`roundPickNumber` are unused).

Every unmentioned field left IN a literal (or auto-inferred on a source
with no `conv_dict` at all, like `Matchup`/`DraftPick`) auto-coerces
through schema inference -- ESPN's own JSON is clean-typed, so
`season_conv_dict` lists only the fields that need a real transform
(`pluck()` for the two nested lists, a handful of settings-evolution
`calc()`s). `rosterSettings.lineupSlotCounts` needs different handling: its
keys are numeric STRINGS (`"0"`, `"2"`, ..., `"23"`), and an UNDECLARED
dict-valued field's auto-promoted submodel sanitizes those into Python
attribute names (`field_0`, `field_2`, ...). `Season.roster_slots`
sidesteps this by being a DECLARED field (`dict[str, int] | None`) on the
`Season` class itself -- `infer_dynamic_schema` skips inference for any
field already present on the base class, so the dict's original string
keys survive untouched. Its `season_conv_dict` entry drills
`settings.rosterSettings.lineupSlotCounts` directly (`calc(dict, ...)`, a
plain shallow-copy coercion); that entry alone never touches the `settings`
field's own (still-inferred, still key-mangled) submodel -- the fjord's own
reseed literal below does, separately, for `division_names`.
`Season.roster_slots` stays a declared field through the reseed too: its
literal copies `s.roster_slots` straight off the already-built first-pass
instance rather than re-drilling it. That same reseed literal also emits
`division_names` (`[d.name for d in s.settings.scheduleSettings.divisions]`)
so `outflow.py`'s `SettingsRow` build can read `s.division_names` /
`len(s.division_names)` directly instead of re-drilling
`s.settings.scheduleSettings.divisions` three levels deep on every wave;
`division_names` exists only on the RESEEDED `Season` instances
`outflow(state)` sees, not on the first-pass network-fetched ones
`main()` holds in `all_seasons`.

`outflow.py`'s `SettingsRow` -- View 6's derived row class -- does NOT need
the same declared-field treatment: it's bare, like all six derived view
classes (Section 2). Its bare inference does reintroduce one observable
effect at read time: a season whose `lineupSlotCounts` key set is smaller
than another season sharing the SAME flush wave gets its missing keys
null-padded in the exported `settings_evolution.ndjson` -- fjord's own
dynamic-schema inference union-merges an undeclared dict-valued field's keys
across every row `outflow(state)` returns in one wave, the derived-row-level
counterpart of the fan-out-batch union `Season.roster_slots`'s own
declaration sidesteps at the source level. The smaller season's own real
values stay present and correct; only the extra keys another row in the
wave carries come back `null`. Every real season in the live public-899513
run shares the same key
set, so the padding never surfaces there; the appendix's own private-mode
test (`OLD_YEAR`, a 3-key roster sharing a flush wave with an 8-key season)
asserts exactly that shape.

A bare ESPN `team.id` repeats every season -- once every season's `Standing`
rows share one fjord source, a bare `id` would collide in `inc_dict`.
`Standing`'s own `conv_dict` synthesizes a `"{season}:{id}"` composite join
key inline -- `calc("{}:{}".format, "season", "id", target_type=str)`, the
bound-method idiom `docs/api_atlas.md` documents for `calc()` -- and
`Standing`'s `inc_code="team_key"` makes that composite the registry key.
`main()`'s own pre-fjord Python builds the SAME composite as a plain
f-string (`f"{r['season']}:{r['id']}"`) to thread each team's owner GUID
directly onto its sibling schedule/pick rows -- `home_owner_guid`/
`away_owner_guid` on every schedule row, `owner_guid` on every pick row --
BEFORE `Matchup.incorp()`/`DraftPick.incorp()` ever run. Neither source
needs a `conv_dict` entry for that FK at all: both stay empty, letting
every other field auto-infer. What's left for `outflow.py`'s
`outflow(state)` is the join that genuinely can't move upstream -- an
owner GUID resolved to `Owner.display_name` -- since `Owner` is
a sibling `stream_params` entry with no seeding-order guarantee relative to
the sources that reference its GUIDs, so no build-time `link_to` is used
anywhere; see that file's own module docstring.

**`Matchup` is a fjord source exactly once** -- no pre-fjord typed build, no
second group-collapsing registration. `outflow(state)` melts
`state["Matchup"]` into everything team/rivalry-related in a single pass:
for each decided game (`winner != "UNDECIDED"`), it appends one row per
perspective to a local `team_games` list (`m.home_points` /
`m.away_points if m.away_points is not None else 0.0`, the flattened fields
from Section 6 -- `m.away_points is None` on a playoff bye), a canonicalized
rivalry-candidate row (the lower owner GUID is always side "a"), and, on a
`WINNERS_BRACKET` game, both owners' season into a `defaultdict[str, set]`
that answers `playoff_appearances` directly. Everything else -- Franchise
Cards, the all-play expected-wins column, win/loss streaks, all ten
records-book kinds, the rivalry matrix -- is a plain `defaultdict` bucket or
a `max()`/`min(key=operator.itemgetter(...))` pick over that one
`team_games` list.

## 7. Player names: one shared fan-out, not two passes

Draft picks carry only a numeric `playerId` (negative for D/ST, e.g.
`-16003` for a Bears defense) -- resolving a name requires a season-matched
call to ESPN's `players_wl` endpoint (`GET
.../seasons/{S}/players?view=players_wl` with an `X-Fantasy-Filter` header
listing the wanted IDs); old IDs don't resolve against a newer season's
player universe. The pipeline makes exactly ONE `PlayerName.incorp` call,
a declarative `inc_parent=all_seasons, inc_child="season"` drill (the same
`inc_parent`/`inc_child` shape as
[Tutorial 6 -- State Sports](../../06-state-sports/README.md)) fanning out
over EVERY discovered season's `players_wl` URL concurrently on one
client, sharing ONE `X-Fantasy-Filter` header whose value is the UNION of
every wanted id -- round-1 picks from every season, plus the all-time
top-15 most-drafted `playerId`s (by pick count, any round), computed once
via a single `collections.Counter` pass over every fetched draft pick.
Each season endpoint resolves only the ids it recognises out of that
shared union; `inc_code="id"` dedups the rest across every URL's response
into one `PlayerName.inc_dict`.

Season-matched calls are still genuinely required -- ESPN's player universe
is season-scoped, so this is the one part of the pipeline that can't batch
below one HTTP request per season -- but every season shares the SAME
filter in ONE `incorp()` call. `DraftPick.player` resolves as a read-time
join inside `outflow.py`'s `outflow(state)`
(`player_names.inc_dict.get(p.playerId)`), not a build-time `link_to`,
since `DraftPick` and `PlayerName` are sibling fjord sources with no
ordering guarantee between them (Section 6).

`defaultPositionId` (not `lineupSlotId`) is the verified position source --
`1=QB`, `2=RB`, `3=WR`, `4=TE`, `5=K`, `16=D/ST`, computed ONCE per player at
`PlayerName`'s own build time via `calc(position_name, "defaultPositionId",
...)`; an unmapped id falls back to a labelled placeholder (`POS_<id>`)
rather than crashing. Every read site inside `outflow(state)` then takes
`player.position if player else "UNKNOWN"` against the read-time
`player_names.inc_dict.get(playerId)` lookup -- a single conditional-dot
guard, not a repeated `position_name(...)` call. Two distinct cases can leave
a pick's name unresolved: a genuinely unresolved `playerId` -- one
`players_wl` just doesn't recognise -- falls back to
`"Unknown"`/`"UNKNOWN"` gracefully, the same fallback used for any
unresolved lookup; and ESPN's own vacant/placeholder-pick sentinel
(`playerId == -1, teamId == -1`, an undrafted or not-yet-reached slot ESPN
returns fully formed rather than omitting), which never reaches that
fallback path at all -- it's filtered out of `main()`'s `all_pick_rows`
flatten before it ever becomes a `DraftPick` row (Section 6).

## Run it

```bash
# Public demo league, no setup required
python examples/appendix/espn-league-history/espn_league_history.py

# Your own league, or a private one
ESPN_LEAGUE_ID=1234567 ESPN_S2=... ESPN_SWID='{...}' \
  python examples/appendix/espn-league-history/espn_league_history.py
```

Also runs in Docker via the
[central mount pattern](../../README.md#running-a-tutorial-in-docker) --
pass the three env vars through with `-e`.

## Where to Go Next

| Goal | Read |
|---|---|
| The one-shot `fjord()` shape this appendix follows | [Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md) |
| A plain one-shot script, no fjord, no Watershed | [Tutorial 6 -- State Sports](../../06-state-sports/README.md) |
| Build rows from data already in memory | [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Universal export formats (`Cls.export(...)`) | [Tutorial 3 -- Universal Formats](../../03-universal-formats/README.md) |
| A read-time multi-source join, as a Tideweaver Fjord current instead | [`crypto-graph-mapping`](../crypto-graph-mapping/README.md) |

***

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/espn-league-history/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
