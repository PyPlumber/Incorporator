***

> 📎 **Appendix — Parquet snapshots inside a Tideweaver window.**
> NDJSON / CSV / SQLite are append-friendly per tick; columnar
> formats (Parquet, Feather, ORC) are not. This appendix shows two
> safe patterns for landing Parquet from a `Tideweaver` run. Requires
> `pip install incorporator[parquet]`. Read
> [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) first.

***

# 🧵 Parquet Snapshots in a Tideweaver Window

Your Tideweaver run accumulates a window of data into NDJSON, but downstream (Athena, DuckDB, Spark) wants columnar Parquet. Add a tail `Export` current that fires once at window close, draws from the in-memory class registry, and writes the Parquet artifact — one file per window, atomic via `os.replace()`.

The reason you can't just set `export_params={"file_path": "...parquet"}` on a regular `Stream` or `Fjord`: those currents flush **once per tick**. That's fine for NDJSON (one line per record, append the file), CSV (header once, append rows), or SQLite (transactional `INSERT OR REPLACE`). It is **not fine** for Parquet, Feather, or ORC: those formats write a column-statistics footer at the **end** of the file, recomputed from the full row set. Appending a second chunk requires reading every existing row group back, merging, re-encoding, and rewriting the whole file — a Parquet "append" is really a rebuild.

The framework refuses to silently rebuild because the bigger the file gets, the longer each tick blocks the event loop. The check is `FormatType.is_append_safe` ([api_atlas](../../../docs/api_atlas.md)) — `True` for NDJSON, CSV, TSV, PSV, SQLite, Avro; `False` for Parquet, Feather, ORC, JSON, XML, XLSX, HTML. Two patterns get you a clean Parquet artifact without per-tick rewrites.

---

## Pattern 1: `Export` current at window close

Inside a `Watershed`, add an `Export` current after the head stream and push its single tick to the end of the window with `phase_offset_sec`. The first tick of any current fires on the first pass it is gate-eligible (scheduler skips the interval check while `last_tick_started is None`); a long `interval` then blocks any re-fire inside the window. So with `phase_offset_sec` set near the window length, that first — and only — `Export` tick lands just before window close, calling `cls.export()` against the fully accumulated registry in one shot. (`phase_offset_sec` is the green-wave delay knob documented in [Tutorial 11](../../11-tideweaver/README.md#green-wave-coordination-with-phase_offset_sec).)

```python
from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, Tideweaver, Watershed, Stream, Export


class Lap(Incorporator):
    pass


start = datetime.now(timezone.utc)
end = start + timedelta(hours=1)

watershed = Watershed.chain(
    window=(start, end),
    gate_mode="hard",  # wait for the upstream Stream's data each tick
    currents=[
        Stream(
            name="laps",
            cls=Lap,
            interval=30,
            incorp_params={
                "inc_url": "https://cf.nascar.com/.../laps.json",
                "inc_code": "lap_id",
            },
        ),
        Export(
            name="laps_snapshot",
            cls=Lap,
            interval=3600,                                  # long interval — never re-fires inside the window
            phase_offset_sec=3500,                          # hold the single tick until just before window close
            export_params={"file_path": "laps_snapshot.parquet"},
        ),
    ],
)

async for tide in Tideweaver(watershed).run():
    print(tide.tide_number, tide.fired, tide.skipped)
```

Pyarrow writes the file via the same atomic `os.replace()` path `incorp().export()` uses, so a crash mid-write leaves the previous snapshot (or nothing) — never a half-written Parquet.

For production runs that should leave a disk trail of every pass,
swap `Tideweaver(watershed)` for `LoggedTideweaver(watershed,
enable_logging=True, logger_name="LapsSnapshot")` — same constructor
surface, but every yielded `Tide` and every canal-layer `RejectEntry`
lands on disk via a non-blocking `QueueHandler`: every `Tide` in
`logs/LapsSnapshot_tide.log` (the single file `get_tides()` reads) and
every canal-layer `RejectEntry` in `logs/LapsSnapshot_error.log`.
Import path:
`from incorporator.observability.tideweaver import LoggedTideweaver`.

---

## Pattern 2: Post-run export

`Tideweaver.run()` drains all in-flight currents before returning. After the loop exits, the source class registries are quiescent and you can call `export()` against them directly:

```python
async for tide in Tideweaver(watershed).run():
    ...                                                     # tick-level work
await Lap.export(file_path="laps_final.parquet")            # one-shot, no Tideweaver
```

This is the right shape when you want exactly one artifact at the end and don't need an `Export` node in the graph for ordering or dependency reasons.

---

## Post-window observability (v1.2.1+)

A Parquet snapshot is only complete if every upstream wave actually
landed in the registry.  After the loop exits, three Tideweaver-side
surfaces tell you what to check before treating the artifact as final:

**`tw.rejects` — what didn't make it in.**  Canal-layer skips
(`PenstockLimited`, `SurgeHalted`, `SkipAhead`, `GateBlocked`) surface
as `RejectEntry` records on the `Tideweaver` instance; filter by
`error_kind` to audit which upstream waves were dropped before the
Export current ran.

```python
tw = Tideweaver(watershed)
tides = [tide async for tide in tw.run()]
await Lap.export(file_path="laps_final.parquet")

canal_drops = [r for r in tw.rejects
               if r.error_kind in {"PenstockLimited", "SurgeHalted",
                                   "SkipAhead", "GateBlocked"}]
if canal_drops:
    print(f"⚠ {len(canal_drops)} upstream waves skipped by canal control")
```

**`tune()` — what to adjust next window.**  Feed the accumulated
records back in and get severity-sorted hints across `chunk_size`,
penstock rate, surge threshold, `pass_interval`, and retry policy:

```python
from incorporator.observability.tideweaver import tune

report = tune(rejects=tw.rejects, tides=tides,
              pass_interval=tw.pass_interval)
print(report.render())
```

`Tideweaver.summary(tides=tides)` returns the same `TuningReport` as
an instance-method convenience.

**`backlog_backoff_factor` — for saturated runs near window close.**
If `tide.next_due_in_sec` is consistently negative in the final
quarter of the window, the scheduler is behind.  Set
`Tideweaver(watershed, backlog_backoff_factor=2.0)` next run to
multiplicatively extend the next-pass wait until the heap drains.
Default `1.0` is disabled.

---

## Format support matrix

| Format       | Per-tick append? | Use inside Tideweaver as |
|--------------|------------------|---------------------------|
| `.ndjson`    | ✅ append        | `Stream.export_params` / `Fjord.export_params` |
| `.csv`       | ✅ append        | `Stream.export_params` / `Fjord.export_params` |
| `.sqlite`    | ✅ upsert        | `Stream.export_params` / `Fjord.export_params` |
| `.parquet`   | ❌ rebuilds full file | `Export` at window close (Pattern 1) or post-run (Pattern 2) |
| `.feather`   | ❌ rebuilds full file | same as Parquet |
| `.orc`       | ❌ rebuilds full file | same as Parquet |
| `.xlsx`      | ❌ rebuilds full file | same as Parquet (avoid in streaming pipelines) |

Pick NDJSON or CSV when the analytics tier downstream is happy with row-shaped storage; pick Parquet at window close when the downstream is Athena / DuckDB / Spark and you want column statistics.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Master the Tideweaver orchestrator patterns | [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) |
| Pick append-friendly vs columnar formats | [Formats & Compression](../../../docs/formats_and_compression.md) |
| See the data-lake round-trip patterns this builds on | [Tutorial 2 — Data Lake Pivot](../../02-data-lake-pivot/README.md) |
| Run the same Tideweaver against a different domain | [Appendix — NASCAR Tideweaver](../nascar-tideweaver/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/tideweaver-parquet-snapshots/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
