# Appendix — same patterns, different domains

Each appendix entry mirrors a pattern from the [seven-tutorial curriculum](../README.md) in a different domain.  Same self-contained-folder shape as the main tutorials: `README.md` + runnable code + sidecars + fixtures.

| Folder | Mirrors | What's different |
|---|---|---|
| [`spacex-launches/`](./spacex-launches/) | T3 + T5 | Non-crypto domain (SpaceX v4 API); slow-cadence daemon. |
| [`nascar-tideweaver/`](./nascar-tideweaver/) | T7 | Diamond shape on race telemetry; pre-recorded JSON fixtures keep it offline-runnable. |
| [`pokeapi-etl/`](./pokeapi-etl/) | T3 | Paginated HATEOAS drill + `calc()` array reductions. |
| [`xml-post-audit/`](./xml-post-audit/) | T3 | XML ingestion + declarative bulk POST + fraud-audit join. |
| [`crypto-graph-mapping/`](./crypto-graph-mapping/) | T6 | `link_to`-based static join (Tutorial 6 as a one-shot, not a daemon). |
| [`nascar-fantasy-fjord/`](./nascar-fantasy-fjord/) | T6 | Six-source fjord with state-aware `inflow(state)` + multi-output `outflow(state)`. |
| [`tideweaver-parquet-snapshots/`](./tideweaver-parquet-snapshots/) | T7 | Doc-only — safe Parquet write patterns inside a Tideweaver window. |
| [`tideweaver-vs-prefect/`](./tideweaver-vs-prefect/) | T7 | Doc-only — in-process vs cloud orchestration decision matrix. |
| [`data-lake-pivot/`](./data-lake-pivot/) | T2 (legacy) | JSON ↔ Avro/SQLite round-trip; superseded by Tutorial 2 but kept as a reference. |
