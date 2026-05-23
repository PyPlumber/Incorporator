# Enterprise Deployment (Docker & Prefect)

The Incorporator Orchestration Platform is designed to run anywhere. Because the core engine strictly enforces O(1) Memory constraints, you can deploy infinite data streams on the smallest Docker containers or monitor them in real-time via Prefect Cloud.

---

## 1. Containerization (Docker)

Incorporator pipelines are stateless by design, making them perfect for Docker. The framework safely handles local data and configuration files via volume mounts.

### 5-Minute Quickstart with `docker compose`

The repository ships with a working `docker-compose.yml` and an
`.env.example`. End-to-end first run:

```bash
# 1. Copy the secrets template and fill in any required values.
cp .env.example .env

# 2. Wire up the three host folders compose mounts into the container.
mkdir -p config data logs

# 3. Pick a starter pipeline.json. examples/ has four ready-to-edit configs.
cp examples/cli-templates/stream-basic.json config/pipeline.json
# Or, generate one from scratch:
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
| `./config` | `/app/config` (read-only) | `pipeline.json` and any user `outflow.py` files |
| `./data` | `/app/data` | Exported output files (CSV / NDJSON / Parquet / …) |
| `./logs` | `/app/logs` | Rotating JSON log files (when `--logs` is set) |

### Secrets — Local vs. Production

Three options, increasing isolation:

1. **`.env` file (compose default).** Convenient for local dev.
   References from `pipeline.json` like
   `"Authorization": "Bearer ${BEARER_TOKEN}"` are expanded at JSON
   load time from environment. Visible via `docker inspect`, so don't
   use in production.
2. **Docker Swarm Secrets / Kubernetes Secrets.** Mount the secret as
   a tmpfs file (e.g. at `/run/secrets/bearer_token`) and reference it
   in JSON with `"Authorization": "Bearer ${file:/run/secrets/bearer_token}"`.
   Not visible to `docker inspect`; survives a leaky `env` dump.
3. **External secret manager** (Vault, AWS Secrets Manager, GCP Secret
   Manager). Out of scope for this CLI — pull secrets into env vars
   or sidecar-mounted files before the container starts; the JSON
   references the same `${VAR}` or `${file:...}` form.

`.env` is gitignored. `pipeline.json` is also gitignored by default
since most teams keep environment-specific configs out of source
control — copy yours into `config/` per environment.

> ### Sandboxing `${file:...}` with `INCORPORATOR_SECRETS_ROOT`
>
> Without a sandbox, a hostile or typo'd `pipeline.json` containing
> `${file:/etc/passwd}` would silently exfiltrate host files at
> expansion time.  The framework's defence is the
> `INCORPORATOR_SECRETS_ROOT` env-var — set it to an absolute
> directory, and **any `${file:...}` reference resolving outside
> that root is rejected** with a clear diagnostic before any file
> is opened.
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
> `env:` block.  When the var is unset (local dev), the framework
> falls back to permissive behaviour — `${file:...}` can read any
> readable host file.  **Always set the var in production.**

### Log directory

By default, `LoggedIncorporator` writes rotating JSON logs to
`./logs/<ClassName>_{api,error,debug}.log` relative to the process
CWD.  In a containerised environment the CWD is `/app` (set by the
Dockerfile's `WORKDIR`), so logs land at `/app/logs/...` — and
`docker-compose.yml` bind-mounts `./logs` there for host visibility.

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

The directory is created lazily on first log write.  When the env-var
is unset, the default `./logs` behaviour is preserved.

### Healthcheck

The Dockerfile and `docker-compose.yml` both declare a `HEALTHCHECK`
that watches `/tmp/incorporator.heartbeat`. The CLI's
`--heartbeat-file` flag (already baked into the default `CMD`)
`touch`es that file after every wave. If no waves land for 2
minutes, the container is reported unhealthy — your orchestrator
(compose / swarm / k8s) can then restart it automatically.

### Custom Dockerfile

If you don't want to use compose, the repo's `Dockerfile` works
standalone. Build:

```bash
docker build -t incorporator:v2 .
```

Run as a one-shot:

```bash
docker run --rm \
  -v $(pwd)/my_pipeline.json:/app/config/pipeline.json \
  -v $(pwd)/output_data:/app/data \
  -v $(pwd)/output_logs:/app/logs \
  incorporator:v2 stream /app/config/pipeline.json --logs
```

Watch the chunking telemetry:

```bash
docker logs -f <container-name>
```

### The Zero-Bloat Dockerfile
If you are building a custom container for your pipeline, use this optimized blueprint. It runs securely as a non-root user and automatically bakes in the Rust/C speedups for maximum OS performance.

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
unregistered hosts fall back to the 15 req/sec default.  For any
production API with a tighter rate ceiling (CoinGecko's 5–15 r/min,
Binance's 1200 r/min, your in-house service's bespoke limit),
register the throttle once at process start and every subsequent
`incorp()` / `stream()` / `fjord()` against that host respects it:

```python
from incorporator import register_host_penstock
from incorporator.io.penstock import SustainedPenstock, BurstPenstock

# Pace api.coingecko.com at 0.2 r/s (12 r/min — under the free-tier ceiling).
register_host_penstock("api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))

# Token-bucket for an in-house API that allows bursts of 200 at 50 r/s sustained.
register_host_penstock("api.internal.acme.com", BurstPenstock(rate_per_sec=50.0, burst=200))
```

Put these calls in a module that's imported before any pipeline
runs — the top of your container's entrypoint script, the top of
your `outflow.py`, or a dedicated `throttle.py` imported by both.
The registration is process-global, so registering twice is safe
(the second call replaces the first).

### Graceful Shutdown

The CLI installs a SIGTERM handler that triggers the same shutdown
path as Ctrl+C: the engine drains in-flight refresh/export daemons,
flushes waves, and exits cleanly. `docker stop`, `docker compose
down`, and `kubectl delete pod` all send SIGTERM by default — no
extra config required.

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
closes.  `--json-output` and `--heartbeat-file` work the same as
they do for `stream` / `fjord`.

> ### Coupling `drain_timeout` with `stop_grace_period`
>
> Docker / Compose / Kubernetes send SIGTERM to the container then
> wait **`--stop-timeout` / `stop_grace_period`** before sending
> SIGKILL.  The platform default is **10 seconds** — shorter than
> Tideweaver's default `drain_timeout` of 30s.  Without matching
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
never blocks the event loop.  Import path matters — it is **not**
top-level exported:

```python
from incorporator.observability.tideweaver import LoggedTideweaver

tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="ArbSession")
async for tide in tw.run():
    ...
# Post-run, replay from disk in any other process:
tides   = await LoggedTideweaver.get_tides(logger_name="ArbSession")
rejects = await LoggedTideweaver.get_rejects(logger_name="ArbSession")
```

In containers, prefer `LoggedTideweaver` over inline `print(tide)`
loops so stdout stays clean for `--json-output` and the structured
records still hit `/app/logs/` for the log shipper.

### Backlog short-circuit — `backlog_backoff_factor`

When a Tideweaver scheduler is consistently saturated (every pass runs
over `pass_interval` because in-flight ticks haven't completed), set
`backlog_backoff_factor=2.0` on the constructor to multiplicatively
extend the next-pass wait until the heap drains.  Default is `1.0`
(disabled — identical behaviour to v1.2.0).  Pair with
`tide.next_due_in_sec` and `tide.heap_depth` for diagnosis.

---

## 2. Cloud Orchestration (Prefect)

If you need enterprise-grade state tracking, retries, and dashboard UI, Incorporator integrates natively with **Prefect**.

Ensure you have the orchestration dependencies installed:
```bash
pip install "incorporator[orchestrate]"
```

### Deploying a Flow
Incorporator includes a pre-built `@flow` wrapper that automatically pipes our `Wave` telemetry metrics directly into the Prefect Cloud UI.

Create a tiny deployment script (`deploy.py`):
```python
import asyncio
from incorporator.integrations.prefect import run_incorporator_flow

async def deploy():
    # Automatically loads your pipeline.json and executes it as a Prefect Flow
    results = await run_incorporator_flow(
        config_path="pipeline.json",
        poll_interval=600.0
    )
    print(f"Flow completed. Processed {len(results)} chunks.")

if __name__ == "__main__":
    asyncio.run(deploy())
```

When you run this script, the Incorporator Engine bypasses its internal disk-logging queues and instead streams the chunk progress (e.g., `✅ Chunk 1 | 10000 rows in 1.4s`) directly to your active Prefect Server or Prefect Cloud dashboard!

---

## Where to Go Next

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