# Deployment (Docker & Prefect)

Incorporator pipelines are stateless by design — Docker Compose, a systemd
service, or a Kubernetes pod are all viable deployment targets without
framework changes. The same `incorp()` / `stream()` / `fjord()` vocabulary
you used in development runs unchanged inside a container; only the secrets
and log paths change.

---

## 1. Containerisation (Docker)

Schema-free ingestion means there are no class definition files or schema
registries to bundle — only your `pipeline.json`, an optional `outflow.py`,
and the package itself. The framework handles local data and configuration
via volume mounts.

### 5-Minute quickstart with `docker compose`

The repository ships with a working `docker-compose.yml` and an
`.env.example`. End-to-end first run:

```bash
# 1. Copy the secrets template and fill in any required values.
cp .env.example .env

# 2. Wire up the three host folders compose mounts into the container.
mkdir -p config data logs

# 3. Pick a starter pipeline.json. examples/ has four ready-to-edit configs.
cp examples/cli-templates/stream-basic.json config/pipeline.json
# Or generate one from scratch:
#   incorporator init --type stream --output-dir config

# 4. Validate before you ship.
incorporator validate config/pipeline.json

# 5. Build + run.
docker compose up -d
docker compose logs -f
```

Volumes mounted by `docker-compose.yml`:

| Host path | Container path | Purpose |
| :--- | :--- | :--- |
| `./config` | `/app/config` (read-only) | `pipeline.json` and any `outflow.py` files |
| `./data` | `/app/data` | Exported output files (CSV / NDJSON / Parquet / …) |
| `./logs` | `/app/logs` | Rotating JSON log files (when `--logs` is set) |

### Secrets — local vs. production

Three options, increasing isolation:

1. **`.env` file (compose default).** Convenient for local dev.
   References in `pipeline.json` like
   `"Authorization": "Bearer ${BEARER_TOKEN}"` are expanded at JSON
   load time from environment. Visible via `docker inspect`, so don't
   use in production.
2. **Docker Swarm Secrets / Kubernetes Secrets.** Mount the secret as
   a tmpfs file (e.g. at `/run/secrets/bearer_token`) and reference it
   in JSON with `"Authorization": "Bearer ${file:/run/secrets/bearer_token}"`.
   Not visible to `docker inspect`; survives a leaky `env` dump.
3. **External secret manager** (Vault, AWS Secrets Manager, GCP Secret
   Manager). Pull secrets into env vars or sidecar-mounted files before
   the container starts; the JSON references the same `${VAR}` or
   `${file:...}` form.

`.env` is gitignored. `pipeline.json` is also gitignored by default
since most teams keep environment-specific configs out of source
control — copy yours into `config/` per environment.

> ### Sandboxing `${file:...}` with `INCORPORATOR_SECRETS_ROOT`
>
> Without a sandbox, a hostile or typo'd `pipeline.json` containing
> `${file:/etc/passwd}` would silently exfiltrate host files at
> expansion time. Set `INCORPORATOR_SECRETS_ROOT` to an absolute
> directory and **any `${file:...}` reference resolving outside
> that root is rejected** with a clear diagnostic before any file
> is opened (`incorporator/config/envexpand.py:126-184`).
>
> The canonical `docker-compose.yml` sets it to `/run/secrets` to
> match Docker Swarm / Kubernetes Secrets mount conventions:
>
> ```yaml
> services:
>   incorporator:
>     environment:
>       INCORPORATOR_SECRETS_ROOT: /run/secrets
> ```
>
> For Kubernetes pods, set the same env-var via the Pod spec's
> `env:` block. When the var is unset (local dev), the framework
> falls back to permissive behaviour — `${file:...}` can read any
> readable host file. **Always set the var in production.**

### Log directory

By default, `LoggedIncorporator` writes rotating JSON logs to
`./logs/<ClassName>_{api,error,debug}.log` relative to the process
CWD. `LoggedTideweaver` adds a fourth file: `<logger_name>_tide.log`.
In a containerised environment the CWD is `/app` (set by the
Dockerfile's `WORKDIR`), so logs land at `/app/logs/...` — and
`docker-compose.yml` bind-mounts `./logs` there for host visibility.

The `_api.log` file receives URL/internet-traffic errors
(`is_url_traffic_error=True`); `_error.log` receives all other records
at INFO and above; `_debug.log` is the superset of both. This routing
is enforced at the Python `logging.Filter` level — no post-processing
needed to classify failures by origin.

Override with `INCORPORATOR_LOG_DIR` when the default doesn't match
your environment — e.g. ECS / CloudRun / Kubernetes patterns that
expect logs under a specific path:

```yaml
services:
  incorporator:
    environment:
      INCORPORATOR_LOG_DIR: /var/log/incorporator
    volumes:
      - /var/log/incorporator:/var/log/incorporator
```

The directory is created lazily on first log write. When the env-var
is unset, the default `./logs` behaviour is preserved.

### Healthcheck

The Dockerfile and `docker-compose.yml` both declare a `HEALTHCHECK`
that watches `/tmp/incorporator.heartbeat`. The CLI's
`--heartbeat-file` flag (already baked into the default `CMD`)
`touch`es that file after every wave. If no waves land for 2
minutes, the container is reported unhealthy — your orchestrator
(compose / swarm / k8s) can then restart it automatically.

### Custom Dockerfile

If you prefer a standalone Dockerfile, the repo's blueprint runs as a
non-root user and installs `.[all]` to include the optional orjson /
cramjam native-code accelerators:

```dockerfile
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd -m -s /bin/bash appuser
WORKDIR /app

RUN mkdir -p /app/config /app/data /app/logs && \
    chown -R appuser:appuser /app

COPY pyproject.toml README.md ./
COPY incorporator/ ./incorporator/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir .[all]

USER appuser
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD test -f /tmp/incorporator.heartbeat \
        && test $(( $(date +%s) - $(stat -c %Y /tmp/incorporator.heartbeat) )) -lt 120 \
        || exit 1
ENTRYPOINT ["incorporator"]
CMD ["stream", "/app/config/pipeline.json", \
     "--poll", "60.0", \
     "--logs", \
     "--heartbeat-file", "/tmp/incorporator.heartbeat"]
```

### Production HTTP throttling — `register_host_penstock`

The framework ships with **no implicit per-host throttling**;
unregistered hosts fall back to the 15 req/sec default
(`incorporator/io/penstock.py:498`). For any API with a tighter rate
ceiling, register the throttle once at process start and every
subsequent `incorp()` / `stream()` / `fjord()` against that host
respects it — the same Penstock primitive used at the HTTP layer also
governs Tideweaver edge flow, so one vocabulary covers both:

```python
from incorporator import register_host_penstock

# Pace api.coingecko.com at 0.2 r/s (12 r/min — under the free-tier ceiling).
register_host_penstock("api.coingecko.com", rate_per_sec=0.2)

# Token-bucket for an in-house API that allows bursts of 200 at 50 r/s sustained.
register_host_penstock("api.internal.acme.com", rate_per_sec=50.0, burst=200)
```

Put these calls in a module imported before any pipeline runs — the
top of your container's entrypoint script, your `outflow.py`, or a
dedicated `throttle.py` imported by both. The registration is
process-global; registering twice is safe (second call replaces the
first).

### Graceful shutdown

The CLI installs a SIGTERM handler (`incorporator/cli/runners.py:184-192`)
that sets a shutdown event, triggering the same exit path as Ctrl+C.
`docker stop`, `docker compose down`, and `kubectl delete pod` all
send SIGTERM by default — no extra config is required when using the
CLI runner. Bare `asyncio.run(main())` scripts need a user-installed
signal handler.

### Deploying a Tideweaver Watershed

The same Docker pattern works for `incorporator tideweaver run` —
swap the entrypoint args:

```dockerfile
CMD ["tideweaver", "run", "/app/config/watershed.json", \
     "--logs", \
     "--heartbeat-file", "/tmp/incorporator.heartbeat"]
```

Tideweaver runs to its declared window-end and then drains in-flight
ticks for up to `drain_timeout` seconds before exiting (default 30s).
For long-running or restart-on-exit shapes, use the same `restart:
unless-stopped` policy a stream daemon would use; for one-shot
window runs, omit `restart` and the container stops when the window
closes. `--json-output` and `--heartbeat-file` behave as they do for
`stream` / `fjord`, except the heartbeat is touched after every
*Tide* rather than every Wave — size your HEALTHCHECK window against
`pass_interval` accordingly.

> ### Coupling `drain_timeout` with `stop_grace_period`
>
> Docker / Compose / Kubernetes send SIGTERM to the container then
> wait **`--stop-timeout` / `stop_grace_period`** before sending
> SIGKILL. The platform default is **10 seconds** — shorter than
> Tideweaver's default `drain_timeout` of 30s. Without matching
> them, every `docker stop` truncates the drain and loses
> in-flight ticks silently.
>
> Three knobs, in precedence order:
>
> 1. `incorporator tideweaver run --drain-timeout=N` — CLI flag.
> 2. `INCORPORATOR_DRAIN_TIMEOUT=N` — env-var (preferred for
>    containers; the canonical `docker-compose.yml` sets it).
> 3. `drain_timeout: N` inside `watershed.json` — last-resort
>    fallback.
>
> Pair whichever you pick with a matching `stop_grace_period: <N+5>s`
> in your `docker-compose.yml` (or `terminationGracePeriodSeconds`
> in your K8s Pod spec) — a few seconds of slack covers the drain
> cleanup itself.

See [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) for the watershed.json
shape.

### Production logging for Tideweaver — `LoggedTideweaver`

`LoggedTideweaver` is the orchestration-side equivalent of
`LoggedIncorporator`: a drop-in for `Tideweaver` that routes every
yielded `Tide` and every accumulated `RejectEntry` through the same
`QueueHandler` pipeline that backs `LoggedIncorporator`, so disk I/O
never blocks the event loop. Import path matters — it is **not**
top-level exported:

```python
from incorporator.tideweaver import LoggedTideweaver

tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="ArbSession")
async for tide in tw.run():
    ...
# Post-run, replay from disk in any other process:
tides   = await LoggedTideweaver.get_tides(logger_name="ArbSession")
rejects = await LoggedTideweaver.get_rejects(logger_name="ArbSession")
events  = await LoggedTideweaver.get_scheduler_events(logger_name="ArbSession")
```

**`logger_name` resolution.** The log file prefix resolves in order:
explicit `logger_name` kwarg → `watershed.name` → `"Tideweaver"`. Set
`watershed.name` on the `Watershed` to get a stable, meaningful prefix
without repeating the name at every `LoggedTideweaver` construction site:

```python
ws = Watershed(name="ArbSession", window=(...), currents=[...])
tw = LoggedTideweaver(ws, enable_logging=True)
# → logs/ArbSession_tide.log, ArbSession_error.log, ArbSession_api.log, ArbSession_debug.log
```

**Per-current session routing.** When `log_currents=True` (default),
each `Stream` current's yielded waves and their `wave.rejects` are
written to the session log tagged with `current`, `class`, and `code`
meta fields. No separate per-class `<Class>_*.log` files are created
during a watershed run — all records share the session log. Set
`log_currents=False` to suppress per-current routing entirely.

**Watershed lifecycle events.** The scheduler emits `watershed_started`
and `watershed_completed` records to `_error.log` at the beginning and
end of every `tw.run()` call. Retrieve them via
`get_scheduler_events()` alongside diagnostic event types
(`isolated_tick_failure`, `tick_parked`, `empty_output`,
`empty_parent_snapshot`, `fjord_flush_failure`). Each event record
includes `event_type`, `current_name`, `cls_name`, `tide_number`,
`session`, and `detail`.

**Reject routing.** `get_rejects()` unions `_error.log` + `_api.log`
so canal-layer rejects and verb-layer HTTP failures are both covered.
`entry["reject"]["is_url_traffic_error"]` distinguishes the two classes
within the union.

In containers, prefer `LoggedTideweaver` over inline `print(tide)`
loops so stdout stays clean for `--json-output` and the structured
records still hit `/app/logs/` for the log shipper.

### Backlog short-circuit — `backlog_backoff_factor`

When a Tideweaver scheduler is consistently saturated (every pass runs
over `pass_interval` because in-flight ticks haven't completed), set
`backlog_backoff_factor=2.0` on the constructor to multiplicatively
extend the next-pass wait until the heap drains. Default is `1.0`
(disabled — identical behaviour to v1.2.0). Pair with
`tide.next_due_in_sec` and `tide.heap_depth` for diagnosis.

---

## 2. Prefect integration

If you need state tracking, retries, and a dashboard UI alongside your
Incorporator pipelines, the `[orchestrate]` extra bundles a Prefect
`@flow` / `@task` wrapper. Install it first:

```bash
pip install "incorporator[orchestrate]"
```

Note: `[orchestrate]` pulls in `prefect>=2.10.0` AND `typer>=0.9.0` as
hard dependencies — `typer` is not in the base package; it ships only
with `[orchestrate]` and `[all]`. `pip install incorporator[orchestrate]`
is the smallest install that gets you the `incorporator` CLI entry point.

### Deploying a flow

The pre-built `run_incorporator_flow` entry point loads your
`pipeline.json` through the same config pipeline the CLI uses —
`${VAR}` / `${file:...}` env expansion, config-dir input-path rebasing,
and inflow/outflow sidecar + token resolution all run before the stream
starts, so a config that works under `incorporator stream` behaves
identically here. It then executes the stream as a Prefect task. Each
Wave's `chunk_index`, `rows_processed`, and `processing_time_sec` are
logged through Prefect's run logger as they arrive, making them visible
in the Prefect UI run log. Waves with `failed_sources` emit as warnings.

`run_incorporator_flow` (and the underlying `run_incorporator_stream`
task wrapper) don't accumulate every `Wave` in memory — they return a
bounded summary dict once the stream completes:

```python
import asyncio
from incorporator.integrations.prefect import run_incorporator_flow

async def deploy():
    summary = await run_incorporator_flow(
        config_path="pipeline.json",
        poll_interval=600.0
    )
    print(f"Flow completed. Processed {summary['chunks']} chunks, "
          f"{summary['rows_processed']} rows.")

if __name__ == "__main__":
    asyncio.run(deploy())
```

`summary` carries `chunks`, `rows_processed`, `failed_chunks`, a capped
sample of `failed_sources`, and `elapsed_sec`.

`run_incorporator_stream` also accepts `enable_logging: bool = False`
and `retries` / `retry_delay_seconds` passthrough for Prefect's task
retry machinery (both default to no-retry, matching today's behaviour).
The integration already routes Wave telemetry through Prefect's run
logger; setting `enable_logging=True` ALSO wires up Incorporator's own
JSON-line disk logger — a genuine double-write you're opting into, not
a bug. Leave it `False` (the default) unless you specifically want both.

---

## Where to go next

| Goal | Read |
|---|---|
| Pick the right CLI command for your pipeline shape | [CLI & Configuration Guide](./cli_and_configuration.md) |
| Coordinate multiple pipelines on independent cadences | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) |
| Decide between in-process and cloud orchestration | [Appendix — Tideweaver vs. Prefect](../examples/appendix/tideweaver-vs-prefect/README.md) |
| Get structured error logs flowing to a log aggregator | [Production Debugging](./debugging.md) |
| Tune performance per format / payload size | [Performance Guide](./performance.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/deployment.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
