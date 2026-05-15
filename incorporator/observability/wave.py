"""The ``Wave`` telemetry record yielded by ``stream()`` / ``fjord()``.

Kept in its own module so the dataclass is importable independently of the
logging machinery in ``observability/logger.py``.  ``logger.py`` re-exports
``Wave`` so ``from incorporator.observability.logger import Wave`` still works.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, ConfigDict, Field


class Wave(BaseModel):
    """A single tick of pipeline telemetry yielded by ``stream()`` / ``fjord()``.

    Each ``Wave`` reports one cycle of work in the engine — a chunk in
    the chunking pipeline, or one refresh / export tick in the stateful
    daemons.  Frozen Pydantic model so callers can pass instances
    around without worrying about mutation.
    """

    model_config = ConfigDict(frozen=True)

    chunk_index: int = Field(..., description="Sequential index of the current chunk.")
    operation: str = Field("stream", description="The phase: 'incorp', 'refresh', or 'export'.")
    rows_processed: int = Field(..., description="Number of rows successfully processed.")
    failed_sources: List[str] = Field(default_factory=list, description="Failed source URIs.")
    processing_time_sec: float = Field(..., description="Chunk processing duration in seconds.")
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
