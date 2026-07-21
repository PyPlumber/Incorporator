# Use a lightweight, modern Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and force unbuffered stdout for real-time logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a non-root user for security
RUN useradd -m -s /bin/bash appuser

# Set the working directory
WORKDIR /app

# Create directories for mounted configurations, logs, exported data, and
# fjord/window-close Parquet exports (the `/app/out` mount pattern in
# examples/README.md). We do this first so we can cleanly assign ownership
# to the appuser.
RUN mkdir -p /app/config /app/data /app/logs /app/out && \
    chown -R appuser:appuser /app

# Copy project metadata and the real source, then install in one step.
# (An earlier stub-package layer-split — priming the dependency layer, then
# reinstalling the real source — proved unreliable in CI: the same-version
# reinstall did not consistently replace the stub's __init__.py. A single
# straightforward install is used instead; the dependency layer is rebuilt
# when incorporator/ changes, which is acceptable since CI builds cleanly and
# the image is not iterated on locally.)
COPY pyproject.toml README.md ./
COPY incorporator/ ./incorporator/

# Install [speedups]+[avro]+[xlsx]+[cli] — the Rust/C accelerators (orjson,
# cramjam, lxml), Avro/xlsx format support, and the Typer CLI entry point.
# This is equivalent to [all] minus Prefect: Prefect is intentionally NOT
# baked into this image (see docs/deployment.md's Prefect-integration
# section) — users who want incorporator.integrations.prefect available in
# their own image should swap this extras set for [orchestrate].
RUN pip install --upgrade pip && \
    pip install --no-cache-dir .[speedups,avro,xlsx,cli]

# Switch to the secure non-root user
USER appuser

# The CLI writes a heartbeat file every audit; HEALTHCHECK monitors its mtime
# so Docker/Kubernetes can detect a stalled daemon (no audits in 2 min).
# For `tideweaver run`: the heartbeat fires once per Tide (not per Wave).
# Set --interval here to at least 2 × pass_interval + start_period so a
# slow watershed does not appear unhealthy during a normally-long pass.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD test -f /tmp/incorporator.heartbeat \
        && test $(( $(date +%s) - $(stat -c %Y /tmp/incorporator.heartbeat) )) -lt 120 \
        || exit 1

# Expose the Typer CLI as the container's native entrypoint
ENTRYPOINT ["incorporator"]

# Default: daemon-mode stream with 60-second polling, structured disk logs,
# and a heartbeat file the HEALTHCHECK above can stat.
# Tideweaver users: set --stop-timeout to INCORPORATOR_DRAIN_TIMEOUT + 5s
# (e.g. `docker run --stop-timeout=50 ...` when INCORPORATOR_DRAIN_TIMEOUT=45)
# so SIGTERM drains complete before SIGKILL truncates in-flight ticks.
CMD ["stream", "/app/config/pipeline.json", \
     "--poll", "60.0", \
     "--logs", \
     "--heartbeat-file", "/tmp/incorporator.heartbeat"]
