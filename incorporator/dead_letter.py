"""DeadLetterEntry — structured failure record for ``IncorporatorList``.

Replaces the prior flat ``failed_sources: List[str]`` representation
while preserving the legacy attribute as a derived ``@property``
returning ``[entry.source for entry in self._dead_letter_queue]``.
Users opting into structured access read
:attr:`IncorporatorList.dead_letter_queue` directly.

Fields mirror the audit's M6 specification:
``source``, ``error_kind``, ``message``, ``retry_after``, ``wave_index``.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class DeadLetterEntry(BaseModel):
    """One source's failure record — Pydantic-serialisable for durable logs.

    Constructed at the framework's failure points (HTTP errors in
    :mod:`incorporator.io.fetch`, fjord seed errors in
    :mod:`incorporator.observability.pipeline.fjord`, generic pipeline
    failures in :mod:`incorporator.observability.pipeline._shared`)
    and accumulated on the :attr:`IncorporatorList.dead_letter_queue`.

    The legacy string list
    (:attr:`IncorporatorList.failed_sources`) becomes a derived view
    over the entries' ``source`` fields, fully back-compat for all
    existing read sites in user code, tests, examples, and durable
    logs.

    Attributes:
        source: URL, file path, or source identifier that failed.  For
            HTTP errors this is the request URL; for fjord seed errors
            this is the source class name; for pipeline errors this is
            the error-prefix label (e.g. ``"Outflow Error"``).
        error_kind: Exception type name (e.g.
            ``"HTTPStatusError"``, ``"RequestError"``, ``"KeyError"``,
            ``"Unknown"``).  Defaults to ``"Unknown"`` when the
            framework was given only a legacy string and inferred no
            exception context.
        message: Human-readable error detail.  Typically ``str(exc)``
            of the originating exception.  Empty string when no detail
            beyond the kind is available.
        retry_after: Seconds-to-wait hint, populated from the HTTP
            ``Retry-After`` response header when the upstream supplies
            one.  ``None`` otherwise.  Future retry-loop logic can use
            this without re-parsing the original exception.
        wave_index: ``chunk_index`` of the parent :class:`Wave`, if any
            (set when the failure was captured during a streaming /
            fjord tick that emitted a wave).  ``None`` for one-shot
            ``incorp()`` calls.

    Frozen — assigning to any field after construction raises.
    """

    model_config = ConfigDict(frozen=True)

    source: str = Field(..., description="URL, file path, or source identifier that failed.")
    error_kind: str = Field(default="Unknown", description="Exception type name.")
    message: str = Field(default="", description="Human-readable error detail.")
    retry_after: Optional[float] = Field(
        default=None,
        description="Seconds to wait before retry, when supplied by the server.",
    )
    wave_index: Optional[int] = Field(
        default=None,
        description="``chunk_index`` of the parent :class:`Wave`, if any.",
    )

    def __str__(self) -> str:
        """Back-compat string form for callers that log the entry directly.

        Reproduces the old hand-formatted shapes when context is
        available; falls back to the source identifier when only the
        legacy string was supplied.
        """
        if self.error_kind and self.error_kind != "Unknown":
            if self.message:
                return f"{self.error_kind}: {self.message}"
            return self.error_kind
        return self.message or self.source
