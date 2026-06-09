"""The ``Wave`` telemetry record yielded by ``stream()`` / ``fjord()``.

Kept in its own module so the dataclass is importable independently of the
logging machinery in ``observability/logger.py``.  ``logger.py`` re-exports
``Wave`` so ``from incorporator.observability.logger import Wave`` still works.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from ..rejects import RejectEntry


class Wave(BaseModel):
    """Per-chunk telemetry record yielded by a running pipeline — what a DX inspects to watch progress.

    Use it for real-time progress monitoring, failed-source detection
    (route into a retry loop via :attr:`Wave.failed_sources`), and feeding
    downstream dashboards as the chunked drain advances across an overnight
    window:

    .. code-block:: python

        async for wave in Coin.stream(...):
            if wave.failed_sources:
                enqueue_retries(wave.failed_sources)
            if wave.rows_processed > 1000:
                notify_slack(f"Wave {wave.chunk_index}: {wave.rows_processed} rows")

    Attributes:
        chunk_index: Sequential 0-indexed position of this chunk in the
            stream.
        operation: Pipeline phase that produced the wave.  In stream
            mode: ``"incorp"``, ``"refresh"``, ``"export"``, or
            ``"chunk"``.  In fjord mode: ``"fjord_incorp:<ClassName>"``,
            ``"fjord_refresh:<ClassName>"``, ``"export:<ClassName>"``,
            or ``"outflow:<DynamicClassName>"``.
        rows_processed: Count of rows successfully processed by this
            chunk.
        failed_sources: Source URIs that errored during the chunk —
            non-empty means partial-failure semantics kicked in.
        rejects: Structured :class:`~incorporator.rejects.RejectEntry`
            records accumulated during the chunk.  Populated from
            ``IncorporatorList.rejects`` at every incorp / seed call
            that completes (success with partial failures or empty
            result).  Empty list when no structured rejects are
            available — exception-path waves where no
            ``IncorporatorList`` was returned always carry ``[]``.
            Additive field: callers that only inspect
            :attr:`failed_sources` are unaffected.
        processing_time_sec: Wall-clock duration of the chunk in
            seconds, useful for live mark-to-market latency tracking.
        source_url: Origin URL or file path the chunk was fetched from.
            Populated from the class-level ``inc_url`` or ``inc_file``
            at chunk close.  ``None`` for one-shot or non-URL sources.
        bytes_processed: Decoded byte count of the response body
            (``len(response.content)``) — post-decompression size, not
            the wire bytes; see ``bytes_downloaded`` for the wire count.
            Populated after a successful fetch; ``None`` for file-mode
            and error chunks.
        bytes_downloaded: Wire byte count transferred over the network
            (``response.num_bytes_downloaded``).  Smaller than
            ``bytes_processed`` when the response was compressed
            (gzip/br/zstd); equal when the body was uncompressed.
            ``None`` for file-mode, paginator-driven, and error chunks.
        http_fetch_time_sec: Wall-clock seconds from the moment the HTTP
            request was issued until the full response body was received
            (``response.elapsed.total_seconds()``).  Isolates the network
            round-trip from the parse/validate remainder so
            ``_tune_chunk_size`` can reason on parse latency separately.
            ``None`` when no HTTP response was available (file-mode,
            paginator-driven, error chunks, or servers that omit the
            timing header).
        http_retry_count: Number of Tenacity retry attempts beyond the
            first for this chunk.  Zero when the request succeeded on
            the first attempt.
        validation_error_count: Count of Pydantic ``ValidationError``
            rows caught during this chunk's build phase.  Zero when all
            rows validated cleanly.
        schema_cache_hit: Whether the schema registry returned an
            existing compiled class (``True``) or built a new one
            (``False``) for this chunk's payload shape.  A persistent
            ``False`` signals a shape-cycling source that may need an
            explicit schema declaration.
        conv_dict_time_sec: Wall-clock seconds spent running the
            converter pass (``conv_dict`` expansion + ETL
            transformations) for this chunk.  Measured at chunk
            boundary, never per-row.  ``None`` when no converter pass
            ran.
        parent_snapshot_size: Row count of the upstream
            ``_tideweaver_snapshot`` consumed by a parent-child
            tick (Stream with ``parent_current`` set or Fjord with
            ``parent_currents`` populated).  ``None`` for ticks
            without parent-child semantics.
        timestamp: UTC timestamp at which the wave was emitted.

    Frozen Pydantic model so instances can be passed around (and
    cached in dashboards) without worrying about mutation.
    """

    model_config = ConfigDict(frozen=True)

    chunk_index: int = Field(..., description="Sequential index of the current chunk.")
    operation: str = Field(
        "stream",
        description=(
            "Pipeline phase. Stream mode: 'incorp', 'refresh', 'export', or 'chunk'. "
            "Fjord mode: 'fjord_incorp:<ClassName>', 'fjord_refresh:<ClassName>', "
            "'export:<ClassName>', or 'outflow:<DynamicClassName>'."
        ),
    )
    rows_processed: int = Field(..., description="Number of rows successfully processed.")
    failed_sources: list[str] = Field(default_factory=list, description="Failed source URIs.")
    rejects: list[RejectEntry] = Field(
        default_factory=list,
        description=(
            "Structured RejectEntry records from the chunk's incorp / seed call. "
            "Empty when no IncorporatorList was available (exception-path waves). "
            "Additive: callers that inspect only failed_sources are unaffected."
        ),
    )
    processing_time_sec: float = Field(..., description="Chunk processing duration in seconds.")
    source_url: str | None = Field(default=None, description="Origin URL or file path for the chunk.")
    bytes_processed: int | None = Field(
        default=None,
        description="Decoded byte count of the response body (len(response.content)) — post-decompression.",
    )
    bytes_downloaded: int | None = Field(
        default=None,
        description="Wire byte count transferred (response.num_bytes_downloaded); None for non-HTTP chunks.",
    )
    http_fetch_time_sec: float | None = Field(
        default=None,
        description="HTTP round-trip latency in seconds (response.elapsed); None for non-HTTP chunks.",
    )
    http_retry_count: int = Field(default=0, description="Tenacity retry attempts beyond the first.")
    validation_error_count: int = Field(default=0, description="Pydantic ValidationError rows caught.")
    schema_cache_hit: bool = Field(
        default=True, description="True when the schema registry reused an existing compiled class."
    )
    conv_dict_time_sec: float | None = Field(
        default=None,
        description=(
            "Wall-clock seconds spent inside the wrapped ``cls.incorp(...)`` "
            "call for this chunk — covers fetch + parse + validate + "
            "converter expansion together.  Use it as a proxy for total "
            "per-chunk ETL work; isolating the converter-only slice "
            "requires future per-stage instrumentation."
        ),
    )
    parent_snapshot_size: int | None = Field(
        default=None,
        description="Upstream snapshot row count consumed by a parent-child tick; None when not applicable.",
    )
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def log_meta(self) -> str:
        """Compact, single-line meta string mirroring :meth:`LoggingMixin.log_meta`.

        Used by :func:`_route_wave_to_log` so Wave records share the
        flat ``meta`` shape with instance-level log records. The full
        Pydantic dump is also attached as a structured ``wave`` field
        on every record (see :class:`JSONFormatter`).
        """
        return (
            f'operation:"{self.operation}", chunk_index:{self.chunk_index}, '
            f"rows:{self.rows_processed}, time_sec:{self.processing_time_sec:.3f}, "
            f"failed:{len(self.failed_sources)}"
        )
