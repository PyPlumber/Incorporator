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
        from_name: Tideweaver edge source name (upstream current name).
            Populated at canal-layer skip sites so per-edge penstock
            recommendations can key on ``(from_name, to_name)`` without
            re-parsing the ``message`` field.  ``None`` for HTTP-layer
            and fjord-seed rejects.
        to_name: Tideweaver edge destination name (downstream current
            name).  Same origin as ``from_name``; ``None`` elsewhere.
        host: Network host extracted from the ``source`` URL via
            ``urlparse(source).netloc``.  Populated on the HTTP-error
            path so per-host failure clustering needs no string parsing
            in the consumer.  ``None`` for file-mode and canal-layer
            rejects.
        status_code: HTTP response status code (e.g. ``429``, ``500``).
            Extracted from ``exc.response.status_code`` when available.
            ``None`` for non-HTTP failures.
        attempt_number: Tenacity retry attempt number at the point of
            final failure.  ``None`` when the failure did not go through
            the Tenacity retry wrapper.
        duration_sec: Wall-clock seconds from the start of the failing
            call to the exception.  Populated where a timing bracket is
            cheaply available; ``None`` otherwise.
        cooldown_sec: Unified "try again after N seconds" hint across
            HTTP and canal sites.  At HTTP error sites this mirrors
            ``retry_after`` (populated from the ``Retry-After`` header);
            at canal sites this carries the penstock-state cooldown.
            Coexists with ``retry_after`` — ``retry_after`` is
            HTTP-specific and kept for back-compat; ``cooldown_sec`` is
            the general cross-site hint.

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
    from_name: Optional[str] = Field(
        default=None,
        description="Tideweaver upstream current name for canal-layer rejects.",
    )
    to_name: Optional[str] = Field(
        default=None,
        description="Tideweaver downstream current name for canal-layer rejects.",
    )
    host: Optional[str] = Field(
        default=None,
        description="Network host from ``urlparse(source).netloc``, for HTTP-layer rejects.",
    )
    status_code: Optional[int] = Field(
        default=None,
        description="HTTP response status code (e.g. 429, 500).",
    )
    attempt_number: Optional[int] = Field(
        default=None,
        description="Tenacity retry attempt number at final failure.",
    )
    duration_sec: Optional[float] = Field(
        default=None,
        description="Wall-clock seconds from call start to exception.",
    )
    cooldown_sec: Optional[float] = Field(
        default=None,
        description=(
            "Unified try-again hint in seconds — mirrors retry_after at HTTP sites, "
            "carries penstock-state cooldown at canal sites."
        ),
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
