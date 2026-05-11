"""
Modular Network Engine for the Incorporator Framework.
Handles HTTPX client generation, dynamic request execution, resilience,
and asynchronous connection-pooling for maximum throughput.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from . import format_parsers
from .compression import decompress_data, infer_compression
from .exceptions import IncorporatorNetworkError
from .format_parsers import FormatType, infer_format
from .paginate import AsyncPaginator

logger = logging.getLogger(__name__)


# ==========================================
# 1. THROTTLING & RESILIENCE
# ==========================================
class RateLimiter:
    """Standard token-bucket lock for requests-per-second throttling to prevent 429 bans."""

    def __init__(self, requests_per_second: float) -> None:
        self.rate = requests_per_second
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.lock = asyncio.Lock()
        self.last_call = 0.0

    async def wait(self) -> None:
        """Yields execution only for the exact delta needed to maintain the rate limit."""
        if self.rate <= 0:
            return

        async with self.lock:
            now = asyncio.get_running_loop().time()
            elapsed = now - self.last_call

            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)

            self.last_call = asyncio.get_running_loop().time()


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
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.AsyncClient:
        client_limits = httpx.Limits(max_keepalive_connections=concurrency_limit, max_connections=concurrency_limit)
        return httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            limits=client_limits,
            verify=not ignore_ssl,
            headers=headers,
        )


def _validate_url(url: str) -> None:
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https"):
        raise IncorporatorNetworkError(f"Security Policy Violation: Unsupported scheme '{parsed.scheme}'.")


# ==========================================
# 3. CORE HTTP EXECUTION WORKERS
# ==========================================
@retry(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True,
)
async def execute_request(
    url: str,
    client: httpx.AsyncClient,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    form_payload: Optional[Dict[str, Any]] = None,
    rate_limiter: Optional[RateLimiter] = None,
) -> httpx.Response:
    """Executes a resilient, jittered HTTP request supporting GET/POST and query strings."""
    _validate_url(url)
    if rate_limiter:
        await rate_limiter.wait()

    req_kwargs: Dict[str, Any] = {}
    if params:
        req_kwargs["params"] = params
    if json_payload:
        req_kwargs["json"] = json_payload
    if form_payload:
        req_kwargs["data"] = form_payload

    # method.upper() natively supports 'POST', 'PUT', etc.
    response = await client.request(method.upper(), url, **req_kwargs)

    # Intercept Post-to-Get Downgrades
    if response.history and method.upper() in ["POST", "PUT", "PATCH"]:
        logger.warning(
            f"⚠️ NETWORK REDIRECT DETECTED: Your {method.upper()} request to '{url}' "
            f"was redirected (HTTP {response.history[0].status_code}). "
            f"Most servers drop the payload and downgrade to GET during a redirect. "
            f"If you receive empty data, verify your URL exactness (e.g., check for a missing trailing slash '/' !)."
        )

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        # Immediately break Tenacity retry loop for permanent Client Errors (except 429 Throttling)
        if status < 500 and status != 429:
            raise IncorporatorNetworkError(f"Fatal client error {status} for URL {url}: {e}") from e
        raise e

    return response


# ==========================================
# I/O HELPER (Module Level)
# ==========================================
async def resolve_source_payload(
    source_val: str,
    is_file_mode: bool,
    active_format: FormatType,
    response: Optional[httpx.Response] = None,
    archive_target: Optional[str] = None,
) -> Union[str, bytes, Path]:
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
# MAIN STREAM PROCESSOR
# ==========================================


async def _process_single_source(
    source_val: str,
    is_file_mode: bool,
    client: Optional[httpx.AsyncClient],
    rate_limiter: Optional[RateLimiter],
    dynamic_payload: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> List[Any]:
    """Isolates stream processing, dynamic Paginator routing, and rec_path drill-down."""
    format_type = kwargs.pop("format_type", None)
    inc_page: Optional[AsyncPaginator] = kwargs.pop("inc_page", None)
    call_lim = kwargs.pop("call_lim", None)
    rec_path = kwargs.pop("rec_path", None)
    archive_target = kwargs.pop("archive_target", None)

    method = kwargs.pop("http_method", kwargs.pop("method", "GET"))
    base_params = kwargs.pop("params", {})

    active_format = format_type or infer_format(source_val)
    accumulated: List[Any] = []

    # 1. Setup the Injection Wrapper
    async def bound_fetch(
        url: str, request_params: Optional[Dict[str, Any]] = None, **kwargs_override: Any
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
        )

    # 2. Pure Data Processing (Accepts Polymorphic Inputs)
    async def _process_payload(raw_payload: Union[str, bytes, Path, List[Any], Dict[str, Any]]) -> None:
        # Pass **kwargs down so 'sql_query' reaches the database handler!
        parsed_chunk = await format_parsers.parse_source_data(raw_payload, active_format, **kwargs)

        if rec_path:
            for part in rec_path.split("."):
                if isinstance(parsed_chunk, dict) and part in parsed_chunk:
                    parsed_chunk = parsed_chunk[part]
                else:
                    break

        if isinstance(parsed_chunk, list):
            accumulated.extend(parsed_chunk)
        else:
            accumulated.append(parsed_chunk)

    # 3. Execution Routing
    if is_file_mode:
        payload = await resolve_source_payload(
            source_val,
            is_file_mode=True,
            active_format=active_format,
            archive_target=archive_target,
        )
        await _process_payload(payload)

    elif inc_page:
        inc_page.fetch_func = bound_fetch
        inc_page.call_lim = call_lim
        if kwargs.get("__inspect"):
            inc_page.strict_mode = True

        async for text in inc_page.paginate(start_url=source_val):
            # Paginators currently still yield text natively
            await _process_payload(text)

    else:
        res = await bound_fetch(source_val)
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
# 5. CONCURRENT ORCHESTRATOR
# ==========================================


async def fetch_concurrent_payloads(
    source_list: List[str],
    is_file_mode: bool,
    payload_list: Optional[List[Optional[Dict[str, Any]]]] = None,
    **kwargs: Any,
) -> Tuple[List[Any], List[str]]:
    """Unified Orchestrator: Exclusively manages sliding windows and concurrent batching."""

    failed_sources: List[str] = []

    limit = kwargs.pop("concurrency_limit", 50)
    delay_between_batches = kwargs.pop("delay_between_batches", 0.0)

    _client = kwargs.pop("_client", None)
    _rate_limiter = kwargs.pop("_rate_limiter", None)
    _ignore_ssl = kwargs.pop("ignore_ssl", False)
    _timeout = kwargs.pop("timeout", 15.0)
    _headers = kwargs.pop("headers", None)
    _requests_per_second = kwargs.pop("requests_per_second", 15.0)
    should_close = False

    if not is_file_mode and _client is None:
        _client = HTTPClientBuilder.build_client(
            concurrency_limit=limit,
            ignore_ssl=_ignore_ssl,
            timeout=_timeout,
            headers=_headers,
        )
        _rate_limiter = RateLimiter(_requests_per_second)
        should_close = True

    async def _safe_execute(src: str, payload: Any) -> List[Any]:
        try:
            return await _process_single_source(
                src, is_file_mode, _client, _rate_limiter, dynamic_payload=payload, **dict(kwargs)
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning(
                    f"🚦 RATE LIMITED (HTTP 429) on '{src}'. Skipping. "
                    f"Tip: Try lowering `concurrency_limit` or adding `delay_between_batches`."
                )
                failed_sources.append(src)
                return []
            raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e
        except httpx.RequestError as e:
            logger.warning(f"Network Connection Error for '{src}': {e.__class__.__name__}. Skipping.")
            failed_sources.append(src)
            return []

    try:
        p_list = payload_list if payload_list else [None] * len(source_list)
        all_parsed_data: List[Any] = []

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
                tasks = [_safe_execute(str(s), p) for s, p in zip(s_batch, p_batch)]
                results = await asyncio.gather(*tasks)

                for res in results:
                    if res:
                        all_parsed_data.extend(res)

        # ========================================================
        # PATH B: O(1) Memory Sliding Window (Ordered Workers)
        # ========================================================
        else:
            # Add enumerate to track the exact index
            task_iterator = iter(enumerate(zip(source_list, p_list)))

            # Pre-allocate an empty array to maintain strict ordering
            ordered_results: List[List[Any]] = [[] for _ in range(len(source_list))]

            async def _sliding_worker() -> None:
                # Worker grabs the index and places the result in the exact slot
                for idx, (src, p) in task_iterator:
                    res = await _safe_execute(str(src), p)
                    if res:
                        ordered_results[idx] = res

            workers = [asyncio.create_task(_sliding_worker()) for _ in range(limit)]
            await asyncio.gather(*workers)

            # Flatten the perfectly ordered results
            for res in ordered_results:
                all_parsed_data.extend(res)

        return all_parsed_data, failed_sources

    finally:
        if should_close and _client is not None:
            await _client.aclose()
