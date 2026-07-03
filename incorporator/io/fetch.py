"""HTTP client builder and request dispatcher."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import socket
import time
from collections.abc import Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, wait_random_exponential

from ..exceptions import IncorporatorFormatError, IncorporatorNetworkError
from ..rejects import RejectEntry
from ..schema.path import DataPath
from . import handlers as format_parsers
from ._retry_defaults import (
    _HTTP_INNER_STOP,
    _HTTP_INNER_WAIT_MAX,
    _HTTP_INNER_WAIT_MIN,
    _HTTP_INNER_WAIT_MULTIPLIER,
    _HTTP_NETWORK_RETRY_STOP,
    _HTTP_NETWORK_WAIT_MAX,
    _HTTP_NETWORK_WAIT_MIN,
    _HTTP_NETWORK_WAIT_MULTIPLIER,
)
from .compression import decompress_data, infer_compression
from .formats import FormatType, infer_format
from .pagination.base import AsyncPaginator
from .penstock import (
    BoundPenstock,
    resolve_penstock,
)
from .source_ref import SourceRef

# Scoped to the chunked-pipeline call so the fetch path can attribute
# response.content size back to the originating Incorporator subclass
# without threading a class reference through every helper signature.
# Set by ``pipeline/chunked.py`` before ``cls.incorp(...)``
# and reset by the same try/finally afterward.  Default ``None`` covers
# all non-chunked call sites (incorp(), refresh(), inspector probes).
_CURRENT_CHUNK_CLASS: ContextVar[type[Any] | None] = ContextVar("_CURRENT_CHUNK_CLASS", default=None)


def _extract_retry_after(exc: Exception) -> float | None:
    """Pull a ``Retry-After`` hint from an HTTPStatusError if the server sent one.

    The header value is interpreted as seconds (the HTTP/1.1 spec also
    allows an HTTP-date form; we treat that as ``None`` to avoid the
    parsing edge cases for a hint that's already advisory).
    """
    if isinstance(exc, httpx.HTTPStatusError):
        header = exc.response.headers.get("Retry-After")
        if header:
            try:
                return float(header)
            except ValueError:
                return None
    return None


def _build_reject_entry(
    source: str,
    exc: Exception,
    *,
    attempt_number: int | None = None,
    duration_sec: float | None = None,
) -> RejectEntry:
    """Construct a :class:`RejectEntry` for one source's network failure.

    Args:
        source: URL, file path, or source identifier that failed.
        exc: The originating exception.
        attempt_number: Tenacity retry attempt count at final failure, when
            available.  ``None`` for format errors and other non-retried paths.
        duration_sec: Wall-clock seconds from call start to exception, when a
            timing bracket is in scope.  ``None`` at call sites without one.

    Returns:
        A frozen :class:`RejectEntry` ready for ``IncorporatorList.rejects``.
    """
    retry_after = _extract_retry_after(exc)
    # URL internet-traffic error = the HTTP/network layer failed talking to a URL:
    # a direct httpx HTTP-status (4xx/5xx) or transport (RequestError) failure, OR an
    # IncorporatorNetworkError that *wraps* one (non-429 5xx is re-raised as
    # `IncorporatorNetworkError(...) from e` at the gather layer, so the httpx origin
    # survives on __cause__).  IncorporatorNetworkErrors raised for file/path, config,
    # or SSRF reasons have no httpx __cause__ and stay False (-> error.log).
    _httpx_traffic = (httpx.HTTPStatusError, httpx.RequestError)
    is_url_traffic = isinstance(exc, _httpx_traffic) or isinstance(getattr(exc, "__cause__", None), _httpx_traffic)
    return RejectEntry.model_construct(
        source=source,
        error_kind=type(exc).__name__,
        message=str(exc),
        retry_after=retry_after,
        wave_index=None,
        host=urlparse(source).netloc if source else None,
        status_code=getattr(getattr(exc, "response", None), "status_code", None),
        cooldown_sec=retry_after,
        attempt_number=attempt_number,
        duration_sec=duration_sec,
        is_url_traffic_error=is_url_traffic,
    )


logger = logging.getLogger(__name__)

# 64 KB — consistent with decompress_data buffer discipline.
_STREAM_CHUNK_SIZE: int = 65_536

# Mirrors urllib3's DEFAULT_ALLOWED_METHODS — inlined so we don't depend on
# urllib3 being importable (it is only a transitive dep via httpx).
_IDEMPOTENT_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})


def _is_retryable_status(exc: httpx.HTTPStatusError) -> bool:
    """Return True when the HTTP status warrants a retry.

    Retries 5xx server errors, 429 rate-limiting, 408 Request Timeout, and
    425 Too Early.  All other 4xx client errors are immediately fatal —
    retrying them cannot succeed and burns the budget.

    408 is included because it is the HTTP-layer equivalent of a network
    ReadTimeout: the server acknowledges the connection but did not receive the
    full request in time; the condition is transient and idempotent-safe.

    425 is included because it signals that the server is not yet ready to
    process the request (TLS early-data / replay-protection); a brief backoff
    and retry resolves the condition.

    Exhausted 408/425 retries raise as :class:`IncorporatorNetworkError` via
    the same non-429 path at ``_safe_execute`` — consistent with exhausted 5xx.

    Args:
        exc: The :class:`httpx.HTTPStatusError` whose response status is tested.

    Returns:
        ``True`` for 5xx, HTTP 429, HTTP 408, and HTTP 425;
        ``False`` for all other 4xx.
    """
    return exc.response.is_server_error or exc.response.status_code in (
        httpx.codes.TOO_MANY_REQUESTS,
        httpx.codes.REQUEST_TIMEOUT,
        httpx.codes.TOO_EARLY,
    )


def _is_network_class_error(exc: BaseException, method: str) -> bool:
    """Return True for connect-phase errors and idempotent-method post-send errors.

    Used by both ``_is_retryable_error`` (type gate) and the retry_state-aware
    stop/wait callables to dispatch per-class attempt caps and backoff bounds.

    Args:
        exc: The exception to classify.
        method: HTTP verb string.  Case-insensitive.

    Returns:
        ``True`` for connect-phase errors (unconditional) and post-send errors
        on idempotent methods; ``False`` otherwise.
    """
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
        return True
    if isinstance(
        exc, (httpx.ReadTimeout, httpx.ReadError, httpx.WriteTimeout, httpx.WriteError, httpx.RemoteProtocolError)
    ):
        return method.upper() in _IDEMPOTENT_METHODS
    return False


def _is_retryable_error(exc: BaseException, method: str) -> bool:
    """Classify a request-layer exception as retryable given the HTTP method.

    Mirrors httpx/httpcore's own stance: connect-establishment failures are
    safe to retry unconditionally; post-send failures are only safe when the
    method is idempotent (avoids double-submitting non-idempotent requests).
    Attempt-count bounding is handled separately by ``_make_http_stop``.

    Args:
        exc: The exception raised inside the tenacity ``with attempt:`` block.
        method: HTTP verb string (e.g. ``"GET"``, ``"POST"``).  Case-insensitive.

    Returns:
        ``True`` if the error type and method allow a retry; ``False`` to
        stop and re-raise immediately (non-idempotent post-send, unknown type).
    """
    if isinstance(exc, httpx.HTTPStatusError):
        # Fatal 4xx already converted to IncorporatorNetworkError before this
        # predicate fires, so in practice this branch sees 429 / 5xx only.
        return _is_retryable_status(exc)
    return _is_network_class_error(exc, method)


def _make_http_stop(method: str) -> Callable[[RetryCallState], bool]:
    """Return a tenacity stop callable that dispatches per-class attempt caps.

    Reads ``retry_state.attempt_number`` and ``retry_state.outcome.exception()``
    directly from the live retry state — reliable unlike a closure over
    ``retrying.statistics`` which is not live when ``retry_if_exception``
    fires.

    Network-class errors (connect-phase + idempotent post-send) stop after
    :data:`io._retry_defaults._HTTP_NETWORK_RETRY_STOP` total attempts;
    server-responded errors (5xx / 429) stop after
    :data:`io._retry_defaults._HTTP_INNER_STOP` total attempts.

    Args:
        method: HTTP verb string.  Case-insensitive.  Forwarded to
            :func:`_is_network_class_error` to distinguish post-send phase.

    Returns:
        A callable ``(retry_state: RetryCallState) -> bool`` suitable for
        ``AsyncRetrying(stop=...)``.
    """

    def _stop(retry_state: RetryCallState) -> bool:
        exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
        if exc is None:
            # Success path — tenacity calls stop after a successful attempt
            # only to confirm; returning False lets the result propagate.
            return False
        n = retry_state.attempt_number
        if _is_network_class_error(exc, method):
            return n >= _HTTP_NETWORK_RETRY_STOP
        return n >= _HTTP_INNER_STOP

    return _stop


def _make_http_wait(method: str) -> Callable[[RetryCallState], float]:
    """Return a tenacity wait callable that dispatches per-class backoff bounds.

    Network-class errors use a short bounded backoff
    (:data:`io._retry_defaults._HTTP_NETWORK_WAIT_MIN` ..
    :data:`io._retry_defaults._HTTP_NETWORK_WAIT_MAX`) so a dead host fails
    quickly.  Server-responded errors (5xx / 429) keep the existing slower
    exponential to respect back-pressure signals.

    Args:
        method: HTTP verb string.  Case-insensitive.  Forwarded to
            :func:`_is_network_class_error`.

    Returns:
        A callable ``(retry_state: RetryCallState) -> float`` suitable for
        ``AsyncRetrying(wait=...)``.
    """
    _network_wait = wait_random_exponential(
        multiplier=_HTTP_NETWORK_WAIT_MULTIPLIER, min=_HTTP_NETWORK_WAIT_MIN, max=_HTTP_NETWORK_WAIT_MAX
    )
    _inner_wait = wait_random_exponential(
        multiplier=_HTTP_INNER_WAIT_MULTIPLIER, min=_HTTP_INNER_WAIT_MIN, max=_HTTP_INNER_WAIT_MAX
    )

    def _wait(retry_state: RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
        if exc is not None and _is_network_class_error(exc, method):
            return _network_wait(retry_state)
        return _inner_wait(retry_state)

    return _wait


# ==========================================
# 1. THROTTLING & RESILIENCE
# ==========================================


# ==========================================
# 2. HTTPX CONFIGURATION & FACTORY
# ==========================================
class HTTPClientBuilder:
    """Centralizes httpx client configuration limits and parameters."""

    @staticmethod
    def build_client(
        concurrency_limit: int = 50,
        ignore_ssl: bool = False,
        timeout: float = 15.0,
        headers: dict[str, str] | None = None,
        block_internal_redirects: bool = False,
    ) -> httpx.AsyncClient:
        """Construct the shared ``httpx.AsyncClient`` used by every fetch.

        HTTP/2 multiplexing is enabled so one TCP/TLS connection carries every
        concurrent request, eliminating per-batch handshake overhead. The
        keepalive pool is decoupled from ``concurrency_limit``: a small pool
        of persistent connections is reused by all workers, while
        ``max_connections`` caps total sockets to prevent runaway exhaustion.

        Pass ``block_internal_redirects=True`` for an opt-in SSRF guard: any
        3xx redirect whose Location header resolves to an RFC1918 / loopback /
        link-local / metadata-endpoint IP is rejected before httpx follows it.
        Default is False to preserve the existing behaviour for pipelines
        that legitimately call internal services.
        """
        # Decouple keepalive pool from worker count: a small pool of persistent
        # connections is reused by all concurrent workers, amortising TCP/TLS
        # handshakes across the session.  max_connections is still capped at
        # concurrency_limit to prevent runaway socket exhaustion.
        client_limits = httpx.Limits(
            max_keepalive_connections=10,
            max_connections=concurrency_limit,
        )
        event_hooks: dict[str, list[Any]] = {}
        if block_internal_redirects:
            event_hooks["response"] = [_block_internal_redirect_hook]
        return httpx.AsyncClient(
            http2=True,  # HTTP/2 multiplexing (pip install httpx[http2])
            follow_redirects=True,
            timeout=timeout,
            limits=client_limits,
            verify=not ignore_ssl,
            headers=headers,
            event_hooks=event_hooks if event_hooks else None,
        )


def _validate_url(url: str) -> None:
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https"):
        raise IncorporatorNetworkError(f"Security Policy Violation: Unsupported scheme '{parsed.scheme}'.")


# Hosts that always resolve to instance/cloud metadata endpoints — flagged
# even when the IP-based check might not match (e.g. DNS rebinding tricks).
_METADATA_HOSTS = frozenset(
    {
        "169.254.169.254",  # AWS/Azure/OpenStack IMDS, GCP metadata.google.internal
        "metadata.google.internal",
        "metadata",
        "fd00:ec2::254",  # AWS IMDS over IPv6
    }
)


def _host_is_internal_fast(host: str) -> bool | None:
    """Cheap pre-DNS check.

    Returns:
        ``True`` if ``host`` is a known-internal metadata host or an IP
        literal that's internal; ``False`` if it's an IP literal that's
        external; ``None`` when DNS resolution is needed to decide.
    """
    if not host:
        return False
    if host.lower() in _METADATA_HOSTS:
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    return _ip_is_internal(ip)


def _addrinfos_have_internal(infos: list[Any]) -> bool:
    """Walk a ``getaddrinfo`` result list; True if any resolved IP is internal."""
    for info in infos:
        addr_raw = info[4][0]
        # ``info[4]`` is ``(host, port)`` for AF_INET and ``(host, port, flowinfo, scopeid)``
        # for AF_INET6; ``host`` is always a str — coerce defensively so the
        # IPv6 scope-id suffix split below is well-typed under mypy.
        addr = str(addr_raw).split("%", 1)[0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _ip_is_internal(ip):
            return True
    return False


async def _host_is_internal(host: str) -> bool:
    """Return True when ``host`` resolves to an RFC1918 / loopback / link-local IP.

    The lookup is performed via ``loop.getaddrinfo`` (asyncio's async wrapper
    around ``socket.getaddrinfo``) so DNS-resolved hostnames (``localhost``,
    ``my-internal.local``) are caught alongside bare IP literals **without
    blocking the event loop**.  Each returned address is checked against the
    standard ``ipaddress`` private/loopback/link-local properties plus an
    explicit cloud-metadata blocklist (the ``169.254.169.254`` family).

    Failure to resolve (NXDOMAIN, transient DNS error) is treated as
    **non-internal** — we don't want a DNS hiccup to make a legitimate
    redirect look malicious.  The subsequent HTTP request will fail
    naturally if the host genuinely doesn't exist.
    """
    fast = _host_is_internal_fast(host)
    if fast is not None:
        return fast
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None)
    except (socket.gaierror, OSError):
        return False
    return _addrinfos_have_internal(infos)


def _ip_is_internal(ip: Any) -> bool:
    """Return True for any IP that should be treated as ``not safe to follow``."""
    return bool(
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
    )


async def _block_internal_redirect_hook(response: httpx.Response) -> None:
    """httpx response hook — reject any 3xx whose Location points at an internal host.

    Fires on every response.  For non-redirect status codes it's a single
    integer check and an immediate return, so the per-request overhead is
    negligible.  On a redirect we parse the Location header and run the
    DNS-aware ``_host_is_internal`` check; if internal, we raise
    :class:`IncorporatorNetworkError` BEFORE httpx itself follows the
    redirect — that's the security guarantee.

    Relative redirects (no scheme) are resolved against the original
    request URL so a ``Location: /admin`` from an external host still
    targets the external host, not an internal one.
    """
    if not response.is_redirect:
        return
    location = response.headers.get("Location") or response.headers.get("location")
    if not location:
        return
    # httpx resolves redirects against the request URL; mirror that resolution
    # here so relative Locations are checked against the correct authority.
    try:
        target = response.request.url.join(location)
    except Exception:
        # If the URL is malformed enough that httpx can't resolve it, let httpx
        # surface the parse error rather than hiding it behind our SSRF check.
        return
    host = target.host
    if await _host_is_internal(host):
        raise IncorporatorNetworkError(
            f"Security Policy Violation: blocked redirect to internal host '{host}' "
            f"(full URL: {target}). Disable with block_internal_redirects=False if "
            f"the destination is intentional."
        )


# ==========================================
# 3. CORE HTTP EXECUTION WORKERS
# ==========================================


async def _stream_to_path_request(
    client: httpx.AsyncClient,
    method: str,
    url: str | httpx.URL,
    req_kwargs: dict[str, Any],
    dest: Path,
) -> httpx.Response:
    """Stream a response body to ``dest`` and return a sentinel Response.

    Opens ``dest`` in "wb" mode at the top of each call, so a retry by
    AsyncRetrying naturally truncates the partial write from the prior
    attempt.  Penstock acquire and SSRF event hooks fire identically to
    the non-streaming path because the AsyncClient owns those concerns,
    not the call style.

    ``dest.parent`` must exist before this is called; a missing parent
    directory raises ``FileNotFoundError`` which is NOT retried by
    tenacity (correct — bad path is a permanent caller error).
    """
    fh = await asyncio.to_thread(dest.open, "wb")
    try:
        async with client.stream(method.upper(), url, **req_kwargs) as response:
            response.raise_for_status()  # mid-stream 429 / 5xx surface here, before body
            async for chunk in response.aiter_bytes(chunk_size=_STREAM_CHUNK_SIZE):
                await asyncio.to_thread(fh.write, chunk)
            return httpx.Response(
                status_code=response.status_code,
                headers=response.headers,
                content=b"",
            )
    finally:
        await asyncio.to_thread(fh.close)


async def execute_request(
    url: str,
    client: httpx.AsyncClient,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    json_payload: dict[str, Any] | None = None,
    form_payload: dict[str, Any] | None = None,
    rate_limiter: BoundPenstock | None = None,
    stream_to_path: Path | None = None,
) -> httpx.Response:
    """Execute a resilient, jittered HTTP request supporting GET/POST and query strings.

    Uses an ``AsyncRetrying`` loop so that ``retrying.statistics["attempt_number"]``
    is readable after the loop, enabling downstream consumers to populate
    ``RejectEntry.attempt_number``.

    Args:
        url: Absolute HTTP/HTTPS URL.
        client: Shared ``httpx.AsyncClient`` managed by the caller.
        method: HTTP verb (``"GET"``, ``"POST"``, etc.).  Case-insensitive.
        params: Query parameters merged onto ``url``'s existing query string
            (via ``httpx.URL.copy_merge_params``), overriding any same-named
            key already present in ``url`` — this preserves pagination state
            embedded in follow-up URLs instead of replacing it.
        json_payload: Body serialised as JSON (``Content-Type: application/json``).
        form_payload: Body serialised as form data (``Content-Type: application/x-www-form-urlencoded``).
        rate_limiter: Optional per-host :class:`BoundPenstock`; acquired once per attempt.
        stream_to_path: When set, streams the response body to this :class:`~pathlib.Path`
            instead of buffering it in memory.  Returns a sentinel
            ``httpx.Response`` with ``content=b""``; callers detect the streaming
            path via ``len(response.content) == 0``.  The file is opened in
            ``"wb"`` mode on each attempt so retries automatically truncate any
            partial write from the previous attempt.

    Returns:
        The successful ``httpx.Response``.  When ``stream_to_path`` is set,
        this is a sentinel with ``content=b""`` and the actual body is on disk.

    Raises:
        IncorporatorNetworkError: For permanent 4xx client errors (excluding 429).
        httpx.HTTPStatusError: For 429 / 5xx errors after all retries are exhausted.
        httpx.RequestError: For network-layer failures after all retries are exhausted.
    """
    retrying = AsyncRetrying(
        stop=_make_http_stop(method),
        wait=_make_http_wait(method),
        retry=retry_if_exception(lambda exc: _is_retryable_error(exc, method)),
        reraise=True,
    )
    try:
        async for attempt in retrying:
            with attempt:
                _validate_url(url)
                if rate_limiter is not None:
                    await rate_limiter.acquire()

                # httpx's request-level params= REPLACES the URL's existing
                # query string rather than merging into it.  Paginator
                # follow-up URLs (NextUrlPaginator, LinkHeaderPaginator) and
                # single inc_url values with an embedded query rely on that
                # query surviving — so merge params onto the URL itself via
                # copy_merge_params() instead of passing params= to the
                # request call.  copy_merge_params applies the given dict
                # ON TOP OF the existing query, so params (already correctly
                # request_params-over-base_params ordered by bound_fetch)
                # wins any key collision with the URL's own query — matching
                # existing base/request_params precedence semantics.
                request_url: str | httpx.URL = httpx.URL(url).copy_merge_params(params) if params else url

                req_kwargs: dict[str, Any] = {}
                if json_payload:
                    req_kwargs["json"] = json_payload
                if form_payload:
                    req_kwargs["data"] = form_payload

                if stream_to_path is not None:
                    try:
                        return await _stream_to_path_request(client, method, request_url, req_kwargs, stream_to_path)
                    except httpx.HTTPStatusError as exc:
                        if not _is_retryable_status(exc):
                            raise IncorporatorNetworkError(
                                f"Fatal client error {exc.response.status_code} for URL {url}: {exc}"
                            ) from exc
                        raise

                # method.upper() natively supports 'POST', 'PUT', etc.
                response = await client.request(method.upper(), request_url, **req_kwargs)

                # Intercept Post-to-Get Downgrades
                if response.history and method.upper() in ["POST", "PUT", "PATCH"]:
                    logger.warning(
                        f"NETWORK REDIRECT DETECTED: Your {method.upper()} request to '{url}' "
                        f"was redirected (HTTP {response.history[0].status_code}). "
                        f"Most servers drop the payload and downgrade to GET during a redirect. "
                        f"If you receive empty data, verify your URL exactness "
                        f"(e.g., check for a missing trailing slash '/' !)."
                    )

                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    if not _is_retryable_status(e):
                        raise IncorporatorNetworkError(
                            f"Fatal client error {e.response.status_code} for URL {url}: {e}"
                        ) from e
                    raise

                # Capture retry count for Wave.http_retry_count on the success
                # path (mirrors the exception-attribute pattern below for
                # RejectEntry.attempt_number).  Gated on the chunked-pipeline
                # contextvar so non-chunked callers don't clobber unrelated
                # class-level state.
                chunk_cls = _CURRENT_CHUNK_CLASS.get()
                if chunk_cls is not None:
                    try:
                        chunk_cls._last_http_retry_count = retrying.statistics.get("attempt_number", 1) - 1
                    except (AttributeError, TypeError):
                        pass  # Class doesn't have the ClassVar (non-Incorporator); ignore.
                return response
        # Unreachable: AsyncRetrying with reraise=True exits via exception or return.
        raise RuntimeError("execute_request: AsyncRetrying loop terminated without return or exception")
    except Exception as e:
        # Attach the final attempt count so _safe_execute / _build_reject_entry
        # can populate RejectEntry.attempt_number without re-instrumenting Tenacity.
        attempt_number = retrying.statistics.get("attempt_number", 1)
        try:
            e._incorporator_attempt_number = attempt_number  # type: ignore[attr-defined]
        except AttributeError:
            pass  # Built-in exception types may not allow arbitrary attribute assignment.
        raise


# ==========================================
# 4. I/O HELPER (Module Level)
# ==========================================
async def resolve_source_payload(
    source_val: str,
    is_file_mode: bool,
    active_format: FormatType,
    response: httpx.Response | None = None,
    archive_target: str | None = None,
) -> str | bytes | Path:
    """Decoupled helper to resolve text, bytes, or physical paths."""

    # 1. LOCAL FILE MODE: Return Path directly to preserve O(1) memory streaming
    if is_file_mode:
        is_compressed = infer_compression(source_val) is not None
        if is_compressed:
            # 🛡️ Compressed files must be unpacked to RAM/Disk via the Decompression Engine
            return await asyncio.to_thread(decompress_data, source_val, source_val, active_format, archive_target)

        # 🛡️ THE FIX: Do not read the entire file into RAM! Pass the physical Path down!
        path = Path(source_val).resolve()
        if not path.is_file():
            raise IncorporatorNetworkError(f"Security/IO Error: Path is not a valid file: {path}")
        return path

    # 2. HTTP NETWORK MODE
    if response is not None:
        # BINARY BYPASS: Skip text decoding for Databases and Avro
        if active_format in (FormatType.SQLITE, FormatType.AVRO):
            return response.read()

        is_compressed = infer_compression(source_val) is not None
        if is_compressed:
            # Pass strict format and target rules to the Decompression Engine
            return await asyncio.to_thread(decompress_data, response.read(), source_val, active_format, archive_target)
        return response.text

    raise IncorporatorNetworkError("No valid response or file path provided.")


# ==========================================
# 4b. MAIN STREAM PROCESSOR
# ==========================================


async def _process_single_source(
    source_val: str,
    is_file_mode: bool,
    client: httpx.AsyncClient | None,
    rate_limiter: BoundPenstock | None,
    dynamic_payload: dict[str, Any] | None = None,
    **kwargs: Any,
) -> list[Any]:
    """Isolates stream processing, dynamic Paginator routing, and rec_path drill-down."""
    format_type = kwargs.pop("format_type", None)
    inc_page: AsyncPaginator | None = kwargs.pop("inc_page", None)
    call_lim = kwargs.pop("call_lim", None)
    rec_path = kwargs.pop("rec_path", None)
    archive_target = kwargs.pop("archive_target", None)
    stream_to_path: Path | None = kwargs.pop("stream_to_path", None)

    method = kwargs.pop("http_method", kwargs.pop("method", "GET"))
    base_params = kwargs.pop("params", {})

    active_format = format_type or infer_format(source_val)
    accumulated: list[Any] = []

    # 1. Setup the Injection Wrapper
    async def bound_fetch(
        url: str, request_params: dict[str, Any] | None = None, **kwargs_override: Any
    ) -> httpx.Response:

        if client is None:
            raise IncorporatorNetworkError("HTTP client is uninitialized during pagination.")

        merged_params = {**base_params, **(request_params or {})}
        payload_type = kwargs.get("payload_type", "json")

        j_override = kwargs_override.get("json_payload")
        f_override = kwargs_override.get("form_payload")

        j_payload = j_override or (
            dynamic_payload if dynamic_payload is not None and payload_type == "json" else kwargs.get("json_payload")
        )
        f_payload = f_override or (
            dynamic_payload if dynamic_payload is not None and payload_type == "form" else kwargs.get("form_payload")
        )

        return await execute_request(
            url=url,
            client=client,
            method=method,
            params=merged_params,
            json_payload=j_payload,
            form_payload=f_payload,
            rate_limiter=rate_limiter,
            stream_to_path=stream_to_path,
        )

    _rec_path = DataPath.parse(rec_path) if rec_path else None

    # 2. Pure Data Processing (Accepts Polymorphic Inputs)
    async def _process_payload(raw_payload: str | bytes | Path | list[Any] | dict[str, Any]) -> None:
        # Pass **kwargs down so 'sql_query' reaches the database handler!
        # Typed as Any so the drill loop below can assign None on out-of-range.
        parsed_chunk: Any = await format_parsers.parse_source_data(raw_payload, active_format, **kwargs)

        if _rec_path is not None:
            parsed_chunk = _rec_path.resolve(parsed_chunk)

        if isinstance(parsed_chunk, list):
            accumulated.extend(parsed_chunk)
        elif parsed_chunk is not None:
            accumulated.append(parsed_chunk)

    # 3. Execution Routing
    #
    # Payload-as-data passthrough: ``dynamic_payload`` is not None and
    # ``source_val`` is the empty-string placeholder that
    # ``SourceRef.from_payload(...).as_str()`` emits for payload-only
    # dispatch (source=None + payload_list=[...]).  Route the payload
    # straight through the same parse pipeline real fetched bodies use —
    # no network, no client, no SSRF check, no penstock acquire.
    # ``http_method`` / ``payload_type`` were already popped above; they
    # are read but never reach an HTTP call in this branch.
    is_passthrough = not is_file_mode and inc_page is None and dynamic_payload is not None and not source_val
    if is_passthrough and dynamic_payload is not None:
        await _process_payload(dynamic_payload)
    elif is_file_mode:
        chunk_cls = _CURRENT_CHUNK_CLASS.get()
        if chunk_cls is not None:
            try:
                chunk_cls._last_bytes_downloaded = None
                chunk_cls._last_http_fetch_time_sec = None
                chunk_cls._last_bytes_processed = None
            except (AttributeError, TypeError):
                pass  # Class doesn't carry the ClassVar (non-Incorporator caller); ignore.
        payload = await resolve_source_payload(
            source_val,
            is_file_mode=True,
            active_format=active_format,
            archive_target=archive_target,
        )
        await _process_payload(payload)

    elif inc_page:
        chunk_cls = _CURRENT_CHUNK_CLASS.get()
        if chunk_cls is not None:
            try:
                chunk_cls._last_bytes_downloaded = None
                chunk_cls._last_http_fetch_time_sec = None
                chunk_cls._last_bytes_processed = None
            except (AttributeError, TypeError):
                pass  # Class doesn't carry the ClassVar (non-Incorporator caller); ignore.
        inc_page.fetch_func = bound_fetch
        inc_page.call_lim = call_lim
        if kwargs.get("__inspect"):
            inc_page.strict_mode = True

        async for text in inc_page.paginate(start_url=source_val):
            # Paginators currently still yield text natively
            await _process_payload(text)

    else:
        res = await bound_fetch(source_val)
        # Attribute response size to the originating class for Wave.bytes_processed.
        chunk_cls = _CURRENT_CHUNK_CLASS.get()
        if chunk_cls is not None:
            try:
                chunk_cls._last_bytes_processed = len(res.content)
                chunk_cls._last_bytes_downloaded = res.num_bytes_downloaded
                # res.elapsed raises RuntimeError when not set (e.g. mock responses).
                try:
                    chunk_cls._last_http_fetch_time_sec = res.elapsed.total_seconds()
                except RuntimeError:
                    chunk_cls._last_http_fetch_time_sec = None
            except (AttributeError, TypeError):
                pass  # Class doesn't carry the ClassVar (non-Incorporator caller); ignore.
        payload = await resolve_source_payload(
            source_val,
            is_file_mode=False,
            active_format=active_format,
            response=res,
            archive_target=archive_target,
        )
        await _process_payload(payload)

    return accumulated


# ==========================================
# 5. SOURCE PREPARATION HELPERS
# ==========================================


def _inject_sqlite_query(source: Any, table_name: str, kwargs: dict[str, Any]) -> None:
    """Auto-injects a default SELECT query for SQLite sources when sql_query is not provided.

    Accepts any source shape ``incorp()`` accepts (``str``, ``PathLike``, or
    a list of either) — values are str-coerced via ``infer_format``'s own
    ``str()`` call before format detection.
    """
    sample = source[0] if isinstance(source, list) else source
    if infer_format(str(sample)) == FormatType.SQLITE and not kwargs.get("sql_query"):
        safe_table = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
        kwargs["sql_query"] = f'SELECT * FROM "{safe_table}"'  # noqa: S608


def _normalize_source_list(
    source: Any,
    payload_list: list[Any] | None,
) -> list[str]:
    """Normalises any single-source-or-list input into a flat ``list[str]``.

    Accepts:
      * ``str`` (URL or local path) → single-element list.
      * ``os.PathLike`` (``pathlib.Path`` and friends) → str-coerced via
        ``os.fspath``, single-element list.  Without this branch a Path
        argument silently dropped through to ``return []`` — the file was
        never read and ``incorp()`` returned an empty IncorporatorList with
        no diagnostic.
      * ``list`` of any of the above → str-coerced, ``None``-filtered list.
      * ``None`` with ``payload_list`` set → placeholder list matching
        ``payload_list``'s length (the payload-driven flow doesn't need real
        source URLs).
      * Anything else → empty list (defers to the caller's source-required
        check at ``base.py:438`` for the diagnostic).

    Internal: routes every kind through :class:`SourceRef` — URL / file /
    kwargs via :meth:`SourceRef.parse`, payload-list via
    :meth:`SourceRef.from_payload`.  The flat ``list[str]`` return
    contract is preserved by :meth:`SourceRef.as_str` (URL / file kinds
    return their string form; payload / kwargs / parent kinds return
    ``""`` so the fetch dispatcher's per-source loop reads
    ``payload_list[i]`` independently).
    """
    if source is None:
        if not payload_list:
            return []
        # Payload-driven dispatch: every entry shares an empty source
        # placeholder.  The SourceRef construction validates shape and
        # gives future payload-aware diagnostics one place to hang.
        payload_ref = SourceRef.from_payload(payload_list)
        return [payload_ref.as_str()] * len(payload_list)
    items = source if isinstance(source, list) else [source]
    out: list[str] = []
    for item in items:
        if item is None:
            continue
        try:
            ref = SourceRef.parse(item)
        except ValueError:
            continue
        out.append(ref.as_str())
    return out


# ==========================================
# 6. CONCURRENT ORCHESTRATOR
# ==========================================


async def fetch_concurrent_payloads(
    source_list: list[str],
    is_file_mode: bool,
    payload_list: list[dict[str, Any] | None] | None = None,
    **kwargs: Any,
) -> tuple[list[Any], list[RejectEntry]]:
    """Unified Orchestrator: Exclusively manages sliding windows and concurrent batching.

    Returns ``(all_parsed_data, rejects)`` — the second element is a
    list of structured :class:`RejectEntry` records (one per failed
    source) with ``error_kind`` populated from the underlying exception
    type and ``retry_after`` populated from any HTTP ``Retry-After``
    header.
    """

    rejects: list[RejectEntry] = []

    limit = kwargs.pop("concurrency_limit", 50)
    delay_between_batches = kwargs.pop("delay_between_batches", 0.0)

    _client = kwargs.pop("_client", None)
    _rate_limiter = kwargs.pop("_rate_limiter", None)
    _ignore_ssl = kwargs.pop("ignore_ssl", False)
    _timeout = kwargs.pop("timeout", 15.0)
    _headers = kwargs.pop("headers", None)
    # Detect whether the caller passed an explicit rate — explicit always
    # wins over the per-host registry default below.
    _user_provided_rps = "requests_per_second" in kwargs
    _requests_per_second: float | None = kwargs.pop("requests_per_second", None)
    _user_burst: int | None = kwargs.pop("burst", None)
    # SSRF defence: opt-in.  When True, redirects whose Location header
    # resolves to a private / loopback / link-local / metadata IP are
    # rejected before httpx follows them.  Pipelines that legitimately
    # call internal services keep the default False.
    _block_internal_redirects = kwargs.pop("block_internal_redirects", False)
    should_close = False

    # Per-source throttle resolution: caller-supplied ``requests_per_second``
    # applies as a single global cap; without it, each distinct host gets its own penstock.
    throttle_for_source: dict[str, BoundPenstock] = {}
    if _rate_limiter is not None:
        # Explicit caller injection (test hook / advanced usage) — honour it.
        for src in source_list:
            throttle_for_source[src] = _rate_limiter
    elif _user_provided_rps:
        # Caller rate is a global cap — one penstock shared across every source.
        shared = resolve_penstock("", requests_per_second=_requests_per_second, burst=_user_burst)
        for src in source_list:
            throttle_for_source[src] = shared
    else:
        # Per-host resolution: one penstock per distinct host.  Empty-string
        # placeholder sources (payload-only passthrough rows — see
        # SourceRef.as_str()) are skipped entirely: they never reach
        # execute_request/rate_limiter.acquire(), so resolving a penstock for
        # them would only register a spurious empty-host entry.
        by_host: dict[str, BoundPenstock] = {}
        for src in source_list:
            if not src:
                continue
            host = urlparse(src).hostname or "" if isinstance(src, str) else ""
            if host not in by_host:
                by_host[host] = resolve_penstock(src)
            throttle_for_source[src] = by_host[host]
        if not is_file_mode and by_host:
            applied = {h: getattr(t.penstock, "rate_per_sec", "n/a") for h, t in by_host.items() if h}
            if applied:
                logger.info(
                    "Per-host penstocks applied: %s.  Pass requests_per_second=N for a global cap.",
                    ", ".join(f"{h}={r}" for h, r in applied.items()),
                )

    if not is_file_mode and _client is None:
        _client = HTTPClientBuilder.build_client(
            concurrency_limit=limit,
            ignore_ssl=_ignore_ssl,
            timeout=_timeout,
            headers=_headers,
            block_internal_redirects=_block_internal_redirects,
        )
        should_close = True

    async def _safe_execute(src: str, payload: Any) -> list[Any]:
        start = time.perf_counter()
        try:
            # ``.get()`` (not ``[]``): empty-string placeholder sources (payload-only
            # passthrough) are deliberately absent from throttle_for_source — they
            # never reach the HTTP path that would call rate_limiter.acquire().
            return await _process_single_source(
                src, is_file_mode, _client, throttle_for_source.get(src), dynamic_payload=payload, **dict(kwargs)
            )
        except httpx.HTTPStatusError as e:
            duration = time.perf_counter() - start
            attempt = getattr(e, "_incorporator_attempt_number", None)
            if e.response.status_code == httpx.codes.TOO_MANY_REQUESTS:
                reject = _build_reject_entry(src, e, attempt_number=attempt, duration_sec=duration)
                logger.warning(str(reject))
                logger.info(
                    "Tip: Lower requests_per_second (e.g. 0.2 for ~12 req/min);"
                    " check the host's free-tier docs for the correct ceiling."
                )
                rejects.append(reject)
                return []
            raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e
        except httpx.RequestError as e:
            duration = time.perf_counter() - start
            attempt = getattr(e, "_incorporator_attempt_number", None)
            reject = _build_reject_entry(src, e, attempt_number=attempt, duration_sec=duration)
            logger.warning(str(reject))
            rejects.append(reject)
            return []
        except IncorporatorFormatError as e:
            duration = time.perf_counter() - start
            # Format errors are not retried by Tenacity, so attempt_number is unavailable.
            reject = _build_reject_entry(src, e, duration_sec=duration)
            logger.warning(str(reject))
            rejects.append(reject)
            return []

    try:
        if payload_list is not None and len(payload_list) != len(source_list):
            raise ValueError(
                f"payload_list has {len(payload_list)} entries but {len(source_list)} source(s) "
                f"were resolved from inc_url — lengths must match exactly (no auto-expansion). Fix with one of: "
                f"(1) pass inc_url as a list of {len(payload_list)} URLs to pair 1:1 with payload_list; "
                f"(2) use the declarative each() token via inc_parent routing, which auto-expands inc_url to "
                f"match; or (3) omit inc_url entirely (source=None) for payload-only mode, which auto-matches "
                f"placeholder length to payload_list."
            )
        p_list = payload_list if payload_list else [None] * len(source_list)
        all_parsed_data: list[Any] = []

        # ========================================================
        # PATH A: Strict Batching (The Convoy Effect is desired)
        # ========================================================
        if delay_between_batches > 0.0:
            for i in range(0, len(source_list), limit):
                if i > 0:
                    await asyncio.sleep(delay_between_batches)

                s_batch = source_list[i : i + limit]
                p_batch = p_list[i : i + limit]

                # asyncio.gather natively preserves array ordering!
                # ``return_exceptions=True``: any exception that escapes
                # _safe_execute (e.g. the non-429 IncorporatorNetworkError
                # re-raise at line 519) must not cancel sibling tasks.
                # Failures surface in failed_sources just like the 429 path.
                tasks = [_safe_execute(str(s), p) for s, p in zip(s_batch, p_batch, strict=False)]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for src, res in zip(s_batch, results, strict=False):
                    if isinstance(res, BaseException) and not isinstance(res, Exception):
                        # CancelledError / SystemExit / KeyboardInterrupt — propagate.
                        raise res
                    if isinstance(res, Exception):
                        reject = _build_reject_entry(str(src), res)
                        logger.warning(str(reject))
                        rejects.append(reject)
                    elif res:
                        all_parsed_data.extend(res)

        # ========================================================
        # PATH B: O(1) Memory Sliding Window (Ordered Workers)
        # ========================================================
        else:
            # Add enumerate to track the exact index
            task_iterator = iter(enumerate(zip(source_list, p_list, strict=False)))

            # Pre-allocate an empty array to maintain strict ordering
            ordered_results: list[list[Any]] = [[] for _ in range(len(source_list))]

            async def _sliding_worker() -> None:
                """Consume items from the shared task_iterator and write results into ordered_results.

                Thread-safety note: asyncio runs on a single thread, so the ``for`` loop over
                ``task_iterator`` (a plain ``enumerate`` iterator) is safe without a lock —
                cooperative multitasking guarantees no two coroutines interleave inside a
                synchronous ``for`` body.  If this is ever migrated to a true multi-threaded
                executor, replace ``task_iterator`` with an ``asyncio.Queue`` to restore safety.

                Exception handling: catch ``Exception`` (not ``BaseException``)
                so cancellation still propagates cleanly while non-429 fetch
                errors degrade to a per-source failure rather than cancelling
                every sibling worker.
                """
                for idx, (src, p) in task_iterator:
                    try:
                        res = await _safe_execute(str(src), p)
                    except Exception as exc:
                        reject = _build_reject_entry(str(src), exc)
                        logger.warning(str(reject))
                        rejects.append(reject)
                        continue
                    if res:
                        ordered_results[idx] = res

            workers = [asyncio.create_task(_sliding_worker()) for _ in range(limit)]
            await asyncio.gather(*workers, return_exceptions=True)

            # Flatten the perfectly ordered results
            for res in ordered_results:
                all_parsed_data.extend(res)

        return all_parsed_data, rejects

    finally:
        if should_close and _client is not None:
            await _client.aclose()
