# Enterprise Deployment (Docker & Prefect)

The Incorporator Orchestration Platform is designed to run anywhere. Because the core engine strictly enforces O(1) Memory constraints, you can deploy infinite data streams on the smallest Docker containers or monitor them in real-time via Prefect Cloud.

---

## 1. Containerization (Docker)

Incorporator pipelines are stateless by design, making them perfect for Docker. The framework safely handles local data and configuration files via volume mounts.

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
ENTRYPOINT ["incorporator"]
CMD ["stream", "/app/config/pipeline.json", "--poll", "60.0", "--logs"]
```

### Running the Daemon
Once built (`docker build -t incorporator:v2 .`), run the daemon by mounting your local `pipeline.json` and a data folder directly into the container:

```bash
docker run -d \
  --name my_pipeline_stream \
  -v $(pwd)/my_pipeline.json:/app/config/pipeline.json \
  -v $(pwd)/output_data:/app/data \
  incorporator:v2
```

You can watch the chunking telemetry in real-time natively via Docker:
```bash
docker logs -f my_pipeline_stream
```

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
from incorporator.prefect_nodes import run_incorporator_flow

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