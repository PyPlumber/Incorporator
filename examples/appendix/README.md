# Appendix — optional side-quests

The eleven-tutorial curriculum at [`examples/README.md`](../README.md) is the canonical learning path.  Each appendix entry below is an optional side-quest that mirrors one of those tutorials in a different domain or adds a niche pattern not covered in the main path.

| Folder | Mirrors | What's different |
|---|---|---|
| [`pokeapi-etl/`](./pokeapi-etl/) | T5 | Paginated HATEOAS drill + `calc()` array reductions. |
| [`crypto-graph-mapping/`](./crypto-graph-mapping/) | T10 | `link_to`-based live in-memory join (T10's fjord as a one-shot, not a daemon). |
| [`nascar-tideweaver/`](./nascar-tideweaver/) | T11 | Diamond shape on race telemetry; pre-recorded JSON fixtures keep it offline-runnable. |
| [`mlb-pulse/`](./mlb-pulse/) | T11 + T5 | Live sports analytics: four MLB Stats API endpoints joined inside a Tideweaver window. |
| [`espn-league-history/`](./espn-league-history/) | T6 | One-shot multi-season ETL: every reachable ESPN Fantasy Football season fused into six all-time analytical views. |
