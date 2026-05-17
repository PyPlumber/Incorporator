***

> 📎 **Appendix — Parquet snapshots inside a Tideweaver window.**
> NDJSON / CSV / SQLite are append-friendly per tick; columnar
> formats (Parquet, Feather, ORC) are not.  This appendix shows two
> safe patterns for landing Parquet from a `Tideweaver` run.  Read
> [Tutorial 7 — Tideweaver](../7_tideweaver.md) first.

***

# 🧵 Parquet Snapshots in a Tideweaver Window

`stream()` and the `Fjord` current's flush both export **once per
tick**.  That's fine for NDJSON (one line per record, append the
file), CSV (header once, append rows), or SQLite (transactional
`INSERT OR REPLACE`).  It is **not fine** for Parquet, Feather, or
ORC: those formats write a column-statistics footer at the **end**
of the file, recomputed from the full row set.  Appending a second
chunk requires reading every existing row group back, merging,
re-encoding, and rewriting the whole file — a Parquet "append" is
really a rebuild.

The framework refuses to silently rebuild because the bigger the
file gets, the longer each tick blocks the event loop.  Two
patterns get you a clean Parquet artifact without per-tick
rewrites.

---

## Pattern 1: `Export` current at window close

Inside a `Watershed`, add an `Export` current after the head stream
with a much longer `interval` than the head — typically the full
window length.  `Export` calls `cls.export()` against the live
registry once per tick; with `interval=window_duration` and
`dependency_mode="hard"`, it fires exactly once, right before
window close.

```python
from datetime import datetime, timedelta, timezone

from incorporator import Incorporator, Tideweaver, Watershed, Stream, Export


class Lap(Incorporator):
    pass


start = datetime.now(timezone.utc)
end = start + timedelta(hours=1)

watershed = Watershed.chain(
    window=(start, end),
    nodes=[
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
            interval=3600,                                  # one tick: at the very end
            dependency_mode="hard",                         # wait for the upstream Stream
            export_params={"file_path": "laps_snapshot.parquet"},
        ),
    ],
)

async for tide in Tideweaver(watershed).run():
    print(tide.tide_number, tide.fired, tide.skipped)
```

Pyarrow writes the file via the same atomic `os.replace()` path
`incorp().export()` uses, so a crash mid-write leaves the previous
snapshot (or nothing) — never a half-written Parquet.

---

## Pattern 2: Post-run export

`Tideweaver.run()` drains all in-flight currents before returning.
After the loop exits, the source class registries are quiescent
and you can call `export()` against them directly:

```python
async for tide in Tideweaver(watershed).run():
    ...                                                     # tick-level work
await Lap.export(file_path="laps_final.parquet")            # one-shot, no Tideweaver
```

This is the right shape when you want exactly one artifact at the
end and don't need an `Export` node in the graph for ordering or
dependency reasons.

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

Pick NDJSON or CSV when the analytics tier downstream is happy with
row-shaped storage; pick Parquet at window close when the
downstream is Athena / DuckDB / Spark and you want column statistics.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Master the Tideweaver orchestrator patterns | [Tutorial 7 — Tideweaver](../7_tideweaver.md) |
| Pick append-friendly vs columnar formats | [Formats & Compression](../formats_and_compression.md) |
| See the data-lake round-trip patterns this builds on | [Appendix — Data Lake Pivot](./data_lake_pivot.md) |
| Run the same Tideweaver against a different domain | [Appendix — NASCAR Tideweaver](./nascar_tideweaver.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/appendix/tideweaver_parquet_snapshots.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
