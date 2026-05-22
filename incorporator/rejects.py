"""RejectEntry — structured failure record for ``IncorporatorList``.

The in-memory counterpart of :attr:`IncorporatorList.failed_sources` —
a flat ``List[str]`` of URLs / file paths / source identifiers — but
carrying the exception type, message, ``Retry-After`` hint, and parent
wave index so retry orchestrators can act on structured data without
re-parsing strings.

ETL vocabulary (Snowflake / Redshift COPY, Informatica, Talend, SSIS)
calls failed-load rows *rejects* or *rejected rows*.  Incorporator
follows that idiom: this surface is **not** a messaging-system
dead-letter queue (no redelivery semantics, no consumer) — the
framework captures each failure once and hands the structured list to
the caller.

This surface is **parallel to, not part of**, the disk-based logging
layer (:class:`LoggedIncorporator` + :meth:`get_error`).  Logging is
opt-in and retroactive (reads JSONL files via ``asyncio.to_thread``);
rejects are always-on and immediate (available the moment
:meth:`Incorporator.incorp` returns, regardless of subclass).
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class RejectEntry(BaseModel):
    """One source's failure record — Pydantic-serialisable for durable logs.

    Constructed at the framework's failure points (HTTP errors in
    :mod:`incorporator.io.fetch`, fjord seed errors in
    :mod:`incorporator.observability.pipeline.fjord`, and canal-layer
    skips in :mod:`incorporator.observability.tideweaver.scheduler`)
    and accumulated on :attr:`IncorporatorList.rejects` or
    :attr:`incorporator.observability.tideweaver.Tideweaver.rejects`
    depending on the surface — verb-layer failures land on the former,
    scheduler-level skips that never reach a tick body land on the
    latter.

    The legacy string list :attr:`IncorporatorList.failed_sources` is a
    derived view over the entries' ``source`` fields, fully back-compat
    for all existing read sites in user code, tests, examples, and
    durable logs.

    Attributes:
        source: URL, file path, or source identifier that failed.  For
            HTTP errors this is the request URL; for fjord seed errors
            this is the source class name; for pipeline errors this is
            the error-prefix label (e.g. ``"Outflow Error"``).
        error_kind: Exception type name (e.g.
            ``"HTTPStatusError"``, ``"RequestError"``, ``"KeyError"``,
            ``"Unknown"``), or a canal-layer skip kind
            (``"PenstockLimited"``, ``"SurgeHalted"``, ``"SkipAhead"``,
            ``"GateBlocked"``) emitted from the Tideweaver scheduler.
            Defaults to ``"Unknown"`` when the framework was given only
            a legacy string and inferred no exception context.
        message: Human-readable error detail.  Typically ``str(exc)``
            of the originating exception.  Empty string when no detail
            beyond the kind is available.
        retry_after: Seconds-to-wait hint, populated from the HTTP
            ``Retry-After`` response header when the upstream supplies
            one.  ``None`` otherwise.  Retry-loop logic can use this
            without re-parsing the original exception.
        wave_index: ``chunk_index`` of the parent :class:`Wave`, if any
            (set when the failure was captured during a streaming /
            fjord tick that emitted a wave).  ``None`` for one-shot
            ``incorp()`` calls.

    Frozen — assigning to any field after construction raises.

    **Durability.**  Only the ``source`` field survives an
    :meth:`Incorporator.export` → :meth:`Incorporator.incorp`
    round-trip (via the derived :attr:`IncorporatorList.failed_sources`
    string view).  ``error_kind`` / ``message`` / ``retry_after`` /
    ``wave_index`` are **in-memory only** — they're populated at the
    HTTP / parse failure points and consumed by the caller before the
    next ``export``.  Retry orchestrators that need durable structured
    rejects should serialise the queue themselves
    (``json.dumps([e.model_dump() for e in lst.rejects])``) before
    discarding the :class:`IncorporatorList`.
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
