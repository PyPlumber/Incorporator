# CLI templates

Generic `pipeline.json` / sidecar shapes used by [`incorporator stream`](../../docs/cli_and_configuration.md) and [`incorporator fjord`](../../docs/cli_and_configuration.md).  Copy any of these as the starting point for your own pipeline config; edit the URL / paths / intervals to match.

| File | Pattern |
|---|---|
| [`stream-basic.json`](./stream-basic.json) | Minimal one-shot stream: pulls a URL once, exports to CSV. |
| [`fjord-basic.json`](./fjord-basic.json) | Two-source fjord skeleton paired with [`outflow_example.py`](./outflow_example.py). |
| [`daemon-mode.json`](./daemon-mode.json) | Long-running stateful daemon — `stateful_polling: true`, decoupled refresh / export intervals.  For multi-source live registries reach for the `fjord-basic.json` shape instead; the single-source `stateful_polling` path is a compatibility shim (see [Tutorial 8](../08-streaming-daemon/README.md)). |
| [`with-auth.json`](./with-auth.json) | Stream with `${API_KEY}` env-var auth header. |
| [`outflow_example.py`](./outflow_example.py) | Canonical `outflow(state) -> list[dict]` skeleton — paired with `fjord-basic.json`, also referenced from the [crypto-graph-mapping appendix](../appendix/crypto-graph-mapping/README.md) and [Tutorial 4 — XML Post Audit](../04-xml-post-audit/README.md).  The dynamic output class name comes from this file's stem in PascalCase (`outflow_example` → `OutflowExample`).  Don't pre-declare the output class — let the engine build it. |

Run any template from the repo root so relative paths resolve:

```bash
incorporator validate examples/cli-templates/stream-basic.json
incorporator stream   examples/cli-templates/stream-basic.json
```
