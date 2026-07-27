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
Running one-shot fjord: Owner/Standing/Matchup/DraftPick/TeamGame network-free, PlayerName fanned out ...

Wrote 6 views to .../out:
  franchise_cards.ndjson    13 rows
  season_timeline.ndjson    72 rows
  rivalry_matrix.ndjson     75 rows
  records_book.ndjson       10 rows
  draft_tendencies.ndjson   55 rows
  settings_evolution.ndjson 7 rows

FRANCHISE CARDS (all-time, sorted by win%)
FRANCHISE               W-L-T          WIN%  SEASONS  TITLES  PLAYOFF%
----------------------------------------------------------------------
longhorn0010            53-29-0       0.646        7       1     71.4%
...
```

(`draft_tendencies.ndjson`'s row count drifts slightly season to season --
2026's draft was still in progress at last run, so its `first_overall` pick
resolved to `Unknown`/`UNKNOWN` rather than a real name; this is expected,
not a bug.)

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
out of the final season list (printed once, as a set difference, at the end
of discovery -- never per-item). This is a fact about the demo league's own
history, not a hardcoded floor in the code -- **season discovery is entirely
server-declared** (Section 4).

## 2. File layout

```
examples/appendix/espn-league-history/
  espn_league_history.py   # season discovery (plain Python) + the fjord() call
  outflow.py                # 7 source classes + 6 derived view classes + outflow(state)
  README.md                # this file
  out/                      # gitignored -- six NDJSON views land here
```

No `watershed.json` -- see Section 5 for why this appendix is a one-shot
`Incorporator.fjord()` rather than a Watershed. Unlike a Watershed example,
where every `Incorporator` subclass lives in the main entry file and a
sidecar is a pure re-import store, a `cls.fjord()` daemon example (this one)
defines its classes in `outflow.py` and the entry file imports the SOURCE
classes back out of it -- the two-path split the framework's own CLI class
resolution depends on (see `examples/09-nascar-fantasy-fjord/`, the template
this rewrite follows).

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
calendar-year season -- reads `status.previousSeasons` off the response;
that IS the candidate season list. Every remaining year fans out
CONCURRENTLY in one `Season.incorp(inc_url=[...])` call on one client with
shared headers -- ESPN gives back the successful years and each failed
year's URL lands in `.failed_sources`, never a raised exception. With
cookies present, whichever years failed that first fan-out retry as ONE
more fan-out against the cookie-gated `leagueHistory` endpoint (list-root
response, `rec_path="0"`); each retried year's `seasonId` is embedded
directly in ITS OWN URL string (`?seasonId=<year>`), since `params=`/
`headers=` are shared across every URL in a fan-out call, not per-URL. With
no cookies present, whichever years failed the first fan-out are simply
left out of the final season list. League size is read from each season's
own `len(season.teams)` -- never hardcoded, since a league's team count can
and does drift across seasons.

A fetch failure surfaces through `IncorporatorList.rejects` /
`.failed_sources` (structured `RejectEntry` records with `.status_code`)
plus the framework's own WARNING-level log line -- never a raised exception,
and never suppressed. The retry decision (private mode only) never inspects
`.status_code` at all; it depends solely on whether cookies are present.

## 5. Why a one-shot fjord, not a Watershed

Every season is fetched exactly once, ever -- there is no polling axis. A
Watershed's entire value proposition is repeated ticks against a moving
window, and nothing here moves: this is static historical data. But six
cardinality-reducing views fused from seven sources IS exactly what
`Incorporator.fjord()` is for, without needing a Watershed's tick/wave
scheduler at all: give every `stream_params` entry `refresh_params=None`
and omit the top-level `export_interval`, and `fjord()` seeds every source
once, flushes `outflow(state)` once, and exits.
[Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
is the shipped precedent this appendix follows for that exact one-shot
shape; [Tutorial 6 -- State Sports](../../06-state-sports/README.md) remains
the precedent for a *plain script*, no-fjord one-shot when there's no
multi-source fusion to do.

The six views are cardinality-reducing group-bys (per-owner rollups across
N seasons, per-pair rivalry aggregation, top-N draft counts). `calc_all`
*can* roll up all of it declaratively -- but only within the scope of ONE
`incorp()` call, and only that far: it runs once, against the full column
lists of whatever rows THAT call built. `Owner`/`Standing`/`Matchup`/
`DraftPick`/`TeamGame` are each ONE network-free `payload_list=` fjord
source (built once in `main()`, never inside the season-discovery loop), so
every `calc_all` entry in `Standing`'s own `conv_dict` sees the WHOLE
history in one column-wide pass -- all-time win/loss totals, championships,
average finish, and win% all broadcast onto every row declaratively, no
hand-rolled fold. A `TeamGame` reshape (one row per team per decided
matchup -- the one unavoidable de-nesting, since ESPN ships matchups as
home/away pairs and every cross-row stat here is team-scoped) does the same
for all-play expected wins and franchise-history win/loss streaks. What
`calc_all` genuinely cannot do is REDUCE row count across a join that spans
two DIFFERENT sources -- Franchise Cards' playoff-appearances count needs
`Standing` owners joined against `Matchup` bracket appearances, and the
pairwise rivalry matrix / draft-tendency counts collapse many rows into
fewer. Those three stay plain Python, inside `outflow.py`'s single
`outflow(state)` function -- the return-twin of the six view-building
blocks a plain script would otherwise write inline in `main()`.

## 6. `payload_list=` and read-time joins

Every sub-collection (`teams`, `schedule`, `members`, `draftDetail.picks`)
is pulled off EVERY fetched `Season` row, flattened into one list spanning
every season in `main()` (each row stamped with `"season": s.season` inline
via dict-comprehension unpacking -- `{**t.model_dump(by_alias=True),
"season": s.season}`), and handed to a sibling fjord source's
`payload_list=` -- a network-free, in-memory passthrough
(`incorporator/base.py`'s payload-only mode): one dict entry in, one row
out, through the full `conv_dict` pipeline, seeded exactly ONCE per class
by `fjord()` rather than once per season. Because `Season`'s own
`conv_dict` runs `calc(list, ...)` / `calc(dict, ...)` against nested JSON,
the framework's dynamic schema builder auto-promotes each nested dict into
its own typed submodel (dotted attribute access) -- so `season.teams` is a
list of submodel instances, not plain dicts, by the time `main()` reads it.
`t.model_dump(by_alias=True)` flattens each back to a plain dict before the
`payload_list=` handoff. `by_alias=True` matters specifically for
`rosterSettings.lineupSlotCounts`: its keys are numeric STRINGS (`"0"`,
`"2"`, ..., `"23"`), and the auto-promoted submodel sanitizes those into
Python attribute names (`field_0`, `field_2`, ...) -- `model_dump(by_alias=
True)` exports the ORIGINAL string keys instead of the sanitized attribute
names. `outflow.py`'s `SettingsRow` builder reads `roster_slots` straight
off `season.settings.model_dump(by_alias=True)["rosterSettings"]
["lineupSlotCounts"]`, never its own `Incorporator` row class (a row class
with numeric-string field names would crash schema inference).

A bare ESPN `team.id` repeats every season -- once every season's `Standing`
rows share one fjord source, a bare `id` would collide in `inc_dict`.
`team_key(season, team_id)` synthesizes a `"{season}:{team_id}"` composite
join key via `calc()`, and `Standing`'s `inc_code="team_key"` makes that
composite the registry key every downstream READ-TIME join resolves
against, inside `outflow.py`'s `outflow(state)`
(`standings.inc_dict.get(m.home_team_key)` for a matchup's home side,
`standings.inc_dict.get(p.team_key)` for a draft pick's team, and so on).
`Standing`/`Matchup`/`DraftPick`/`TeamGame` are sibling `stream_params`
entries seeded with no ordering guarantee between them, so every
cross-class join happens read-time in `outflow(state)`, not build-time via
`link_to` -- see that file's own module docstring.

## 7. Player names: one shared fan-out, not two passes

Draft picks carry only a numeric `playerId` (negative for D/ST, e.g.
`-16003` for a Bears defense) -- resolving a name requires a season-matched
call to ESPN's `players_wl` endpoint (`GET
.../seasons/{S}/players?view=players_wl` with an `X-Fantasy-Filter` header
listing the wanted IDs); old IDs don't resolve against a newer season's
player universe. The pipeline makes exactly ONE `PlayerName.incorp(inc_url=
[...])` call, fanning out over EVERY discovered season's `players_wl` URL
concurrently on one client, sharing ONE `X-Fantasy-Filter` header whose
value is the UNION of every wanted id -- round-1 picks from every season,
plus the all-time top-15 most-drafted `playerId`s (by pick count, any
round), computed once via a single `collections.Counter` pass over every
fetched draft pick. Each season endpoint resolves only the ids it
recognises out of that shared union; `inc_code="id"` dedups the rest across
every URL's response into one `PlayerName.inc_dict`.

Season-matched calls are still genuinely required -- ESPN's player universe
is season-scoped, so this is the one part of the pipeline that can't batch
down below one HTTP request per season -- but sending every season the
SAME shared filter, in ONE `incorp()` call, replaces what used to be up to
two loops of per-season calls. `DraftPick.player` is now a READ-TIME join
inside `outflow.py`'s `outflow(state)` (`player_names.inc_dict.get(p.
playerId)`), not a build-time `link_to`, since `DraftPick` and `PlayerName`
are sibling fjord sources with no ordering guarantee between them (Section
6).

`defaultPositionId` (not `lineupSlotId`) is the verified position source --
`1=QB`, `2=RB`, `3=WR`, `4=TE`, `5=K`, `16=D/ST`, computed ONCE per player at
`PlayerName`'s own build time via `calc(position_name, "defaultPositionId",
...)`; an unmapped id falls back to a labelled placeholder (`POS_<id>`)
rather than crashing. Every read site inside `outflow(state)` then takes
`player.position if player else "UNKNOWN"` against the read-time
`player_names.inc_dict.get(playerId)` lookup -- a single conditional-dot
guard, not a repeated `position_name(...)` call. Some very old or vacated
draft slots resolve to a sentinel `playerId` ESPN doesn't map to a real
player at all -- that row's name/position fall back to
`"Unknown"`/`"UNKNOWN"` gracefully, the same fallback used for any
unresolved lookup.

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
