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
cp examples/pipeline_stream.json config/pipeline.json
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

### Graceful Shutdown

The CLI installs a SIGTERM handler that triggers the same shutdown
path as Ctrl+C: the engine drains in-flight refresh/export daemons,
flushes waves, and exits cleanly. `docker stop`, `docker compose
down`, and `kubectl delete pod` all send SIGTERM by default — no
extra config required.

---

## 2. Cloud Orchestration (Prefect)

If you need enterprise-grade state tracking, retries, and dashboard UI, Incorporator integrates natively with **Prefect**.

Ensure you have the orchestration dependencies installed:
```bash
pip install "incorporator[orchestrate]"
```

### Deploying a Flow
Incorporator includes a pre-built `@flow` wrapper that automatically pipes our `AuditResult` telemetry metrics directly into the Prefect Cloud UI.

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