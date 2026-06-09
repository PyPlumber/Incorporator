"""RejectEntry — structured failure record for ``IncorporatorList``.

The in-memory counterpart of :attr:`IncorporatorList.failed_sources` —
a flat ``list[str]`` of URLs / file paths / source identifiers — but
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

import httpx
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
            Defaults to ``"Unknown"`` when no exception context is available.
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
            final failure, or ``1`` for canal-layer skip sites (which
            have no retry loop — the first skip is always attempt 1).
            ``None`` when no attempt context is available.
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
        is_url_traffic_error: ``True`` when the reject originates from
            an httpx HTTP-status (4xx/5xx) or transport
            (:class:`httpx.RequestError`) failure — i.e. the HTTP/network
            layer failed talking to a URL.  ``False`` for parse errors
            (:class:`~incorporator.exceptions.IncorporatorFormatError`),
            file-mode errors, fjord seed errors, and canal-layer skips.
            Used by :func:`~incorporator.observability.logger._route_reject_to_log`
            to route URL-traffic rejects to ``api.log`` and all others
            to ``error.log``.

    Frozen — assigning to any field after construction raises.

    **Durability.**  Only the ``source`` field survives an
    :meth:`Incorporator.export` → :meth:`Incorporator.incorp`
    round-trip (via the derived :attr:`IncorporatorList.failed_sources`
    string view).  ``error_kind`` / ``message`` / ``retry_after`` /
    ``wave_index`` / ``host`` / ``status_code`` / ``cooldown_sec`` /
    ``is_url_traffic_error`` are **in-memory only** — they're populated
    at the HTTP / parse failure points and consumed by the caller before
    the next ``export``.  Retry orchestrators that need durable structured
    rejects should serialise the queue themselves
    (``json.dumps([e.model_dump() for e in lst.rejects])``) before
    discarding the :class:`IncorporatorList`.
    """

    model_config = ConfigDict(frozen=True)

    source: str = Field(..., description="URL, file path, or source identifier that failed.")
    error_kind: str = Field(default="Unknown", description="Exception type name.")
    message: str = Field(default="", description="Human-readable error detail.")
    retry_after: float | None = Field(
        default=None,
        description="Seconds to wait before retry, when supplied by the server.",
    )
    wave_index: int | None = Field(
        default=None,
        description="``chunk_index`` of the parent :class:`Wave`, if any.",
    )
    from_name: str | None = Field(
        default=None,
        description="Tideweaver upstream current name for canal-layer rejects.",
    )
    to_name: str | None = Field(
        default=None,
        description="Tideweaver downstream current name for canal-layer rejects.",
    )
    host: str | None = Field(
        default=None,
        description="Network host from ``urlparse(source).netloc``, for HTTP-layer rejects.",
    )
    status_code: int | None = Field(
        default=None,
        description="HTTP response status code (e.g. 429, 500).",
    )
    attempt_number: int | None = Field(
        default=None,
        description="Tenacity retry attempt number at final failure.",
    )
    duration_sec: float | None = Field(
        default=None,
        description="Wall-clock seconds from call start to exception.",
    )
    cooldown_sec: float | None = Field(
        default=None,
        description=(
            "Unified try-again hint in seconds — mirrors retry_after at HTTP sites, "
            "carries penstock-state cooldown at canal sites."
        ),
    )
    session: str | None = Field(
        default=None,
        description=(
            "Logger name for the Tideweaver session that produced this reject; None for "
            "HTTP-layer and non-session rejects."
        ),
    )
    is_url_traffic_error: bool = Field(
        default=False,
        description=(
            "True when the reject originates from an httpx HTTP-status (4xx/5xx) or transport "
            "(RequestError) failure — the HTTP/network layer failed talking to a URL. "
            "False for parse errors (IncorporatorFormatError), file-mode errors, fjord seed "
            "errors, and canal-layer skips."
        ),
    )

    def __str__(self) -> str:
        """Canonical human renderer — ASCII-safe, cp1252-compatible.

        Format: ``"{error_kind}: {source}"``
        + `` ({from_name}->{to_name})`` when ``from_name`` is set
        + `` [HTTP {status_code}]`` when ``status_code`` is set
        + `` — {message[:120]}`` when ``message`` is non-empty and distinct
          from ``source`` (avoids repeating the source as both position and
          detail).
        """
        parts = [f"{self.error_kind}: {self.source}"]
        if self.from_name is not None:
            parts.append(f" ({self.from_name}->{self.to_name})")
        if self.status_code is not None:
            phrase = httpx.codes.get_reason_phrase(self.status_code)
            if phrase:
                parts.append(f" [HTTP {self.status_code} {phrase}]")
            else:
                parts.append(f" [HTTP {self.status_code}]")
        if self.message and self.message != self.source:
            parts.append(f" — {self.message[:120]}")
        return "".join(parts)


def _format_reject_warning(rejects: list[RejectEntry], cap: int = 5) -> str:
    """Format a structured reject list into a multi-line ``warnings.warn`` message.

    Args:
        rejects: Non-empty list of :class:`RejectEntry` instances to summarise.
        cap: Maximum number of individual entries to render before emitting
            an overflow line.  Defaults to 5.

    Returns:
        A newline-separated string with a count headline, up to ``cap`` rendered
        entries (via :meth:`RejectEntry.__str__`), and an optional overflow line.
    """
    lines = [f"{len(rejects)} source(s) returned partial data."]
    for r in rejects[:cap]:
        lines.append(str(r))
    if len(rejects) > cap:
        lines.append(f"... and {len(rejects) - cap} more.")
    return "\n".join(lines)
