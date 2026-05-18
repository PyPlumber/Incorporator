# CLI templates

Generic `pipeline.json` / sidecar shapes used by [`incorporator stream`](../../docs/cli_and_configuration.md) and [`incorporator fjord`](../../docs/cli_and_configuration.md).  Copy any of these as the starting point for your own pipeline config; edit the URL / paths / intervals to match.

| File | Pattern |
|---|---|
| [`stream-basic.json`](./stream-basic.json) | Minimal one-shot stream: pulls a URL once, exports to CSV. |
| [`fjord-basic.json`](./fjord-basic.json) | Two-source fjord skeleton paired with [`outflow_example.py`](./outflow_example.py). |
| [`daemon-mode.json`](./daemon-mode.json) | Long-running stateful daemon — `stateful_polling: true`, decoupled refresh / export intervals. |
| [`with-auth.json`](./with-auth.json) | Stream with `${API_KEY}` env-var auth header. |
| [`outflow_example.py`](./outflow_example.py) | Canonical `outflow(state)` skeleton — paired with `fjord-basic.json`, referenced by the [crypto-graph-mapping](../appendix/crypto-graph-mapping/) and [xml-post-audit](../04-xml-post-audit/) appendices. |

Run any template from the repo root so relative paths resolve:

```bash
incorporator validate examples/cli-templates/stream-basic.json
incorporator stream   examples/cli-templates/stream-basic.json
```
