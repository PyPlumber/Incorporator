"""
Modular Network Engine for the Incorporator Framework.
Handles HTTPX client generation, dynamic request execution, resilience,
and asynchronous connection-pooling for maximum throughput.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from . import format_parsers
from .exceptions import IncorporatorNetworkError
from .format_parsers import infer_format
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
            headers: Optional[Dict[str, str]] = None
    ) -> httpx.AsyncClient:
        client_limits = httpx.Limits(max_keepalive_connections=concurrency_limit, max_connections=concurrency_limit)
        return httpx.AsyncClient(
            follow_redirects=True, timeout=timeout, limits=client_limits,
            verify=not ignore_ssl, headers=headers
        )


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise IncorporatorNetworkError(f"Security Policy Violation: Unsupported scheme '{parsed.scheme}'.")


# ==========================================
# 3. LOCAL FILE I/O
# ==========================================
def _sync_read(file_path: str) -> str:
    try:
        path = Path(file_path).resolve()
        if not path.is_file():
            raise IncorporatorNetworkError(f"Security/IO Error: Path is not a valid file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as e:
        raise IncorporatorNetworkError(f"Failed to read file {file_path}: {e}") from e

async def _read_file(file_path: str) -> str:
    return await asyncio.to_thread(_sync_read, file_path)


# ==========================================
# 4. CORE HTTP EXECUTION WORKERS
# ==========================================
@retry(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True
)
async def execute_request(
    url: str,
    client: httpx.AsyncClient,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    form_payload: Optional[Dict[str, Any]] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> httpx.Response:
    """Executes a resilient, jittered HTTP request supporting GET/POST and query strings."""
    _validate_url(url)
    if rate_limiter: await rate_limiter.wait()

    req_kwargs: Dict[str, Any] = {}
    if params: req_kwargs["params"] = params
    if json_payload: req_kwargs["json"] = json_payload
    if form_payload: req_kwargs["data"] = form_payload

    response = await client.request(method.upper(), url, **req_kwargs)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status < 500 and status != 429:
            raise IncorporatorNetworkError(f"Fatal client error {status} for URL {url}: {e}") from e
        raise e

    return response


async def _process_single_source(
        source_val: str,
        is_file_mode: bool,
        client: Optional[httpx.AsyncClient],
        rate_limiter: Optional[RateLimiter],
        **kwargs: Any
) -> List[Any]:
    """Isolates stream processing, dynamic Paginator routing, and rec_path drill-down."""
    format_type = kwargs.get('format_type')
    inc_page: Optional[AsyncPaginator] = kwargs.get('inc_page')
    call_lim = kwargs.get('call_lim')
    rec_path = kwargs.get('rec_path')

    method = kwargs.get('method', 'GET')
    base_params = kwargs.get('params', {})

    active_format = format_type or infer_format(source_val)
    accumulated: List[Any] = []

    # 1. Setup the Injection Wrapper
    async def bound_fetch(url: str, request_params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        if client is None:
            raise IncorporatorNetworkError("HTTP client is uninitialized during pagination.")

        merged_params = {**base_params, **(request_params or {})}
        return await execute_request(
            url=url, client=client, method=method, params=merged_params,
            json_payload=kwargs.get('json_payload'), form_payload=kwargs.get('form_payload'),
            rate_limiter=rate_limiter
        )

    # 2. Standard Parse & Drill Action
    async def _accumulate_text(raw_text: str) -> None:
        parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)
        if rec_path:
            for part in rec_path.split('.'):
                if isinstance(parsed_chunk, dict) and part in parsed_chunk:
                    parsed_chunk = parsed_chunk[part]
                else:
                    break

        if isinstance(parsed_chunk, list):
            accumulated.extend(parsed_chunk)
        else:
            accumulated.append(parsed_chunk)

    # 3. Execute Route
    if is_file_mode:
        await _accumulate_text(await _read_file(source_val))
    elif inc_page:
        inc_page.fetch_func = bound_fetch
        inc_page.call_lim = call_lim
        async for text in inc_page.paginate(start_url=source_val):
            await _accumulate_text(text)
    else:
        res = await bound_fetch(source_val)
        await _accumulate_text(res.text)

    return accumulated


# ==========================================
# 5. CONCURRENT ORCHESTRATOR
# ==========================================
async def fetch_concurrent_payloads(
    source_list: List[str],
    is_file_mode: bool,
    **kwargs: Any
) -> Tuple[List[Any], List[str]]:
    """Unified Orchestrator: Exclusively manages semaphores, chunks, and concurrent batching."""
    failed_sources: List[str] = []
    all_parsed_data: List[Any] =[]

    limit = kwargs.get('concurrency_limit', 50)
    delay_between_batches = kwargs.get('delay_between_batches', 0.0)
    semaphore = asyncio.Semaphore(limit)

    _client = kwargs.get('_client')
    _rate_limiter = kwargs.get('_rate_limiter')
    should_close = False

    if not is_file_mode and _client is None:
        _client = HTTPClientBuilder.build_client(
            concurrency_limit=limit, ignore_ssl=kwargs.get('ignore_ssl', False),
            timeout=kwargs.get('timeout', 15.0), headers=kwargs.get('headers')
        )
        _rate_limiter = RateLimiter(kwargs.get('requests_per_second', 15.0))
        should_close = True

    async def _task_wrapper(src: str) -> List[Any]:
        async with semaphore:
            try:
                return await _process_single_source(src, is_file_mode, _client, _rate_limiter, **kwargs)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(f"Request failed with status 429. Skipping: {src}")
                    failed_sources.append(src)
                    return[]
                raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e

    try:
        chunks =[source_list[i:i + limit] for i in range(0, len(source_list), limit)]
        for i, chunk in enumerate(chunks):
            tasks =[_task_wrapper(str(src)) for src in chunk]
            chunk_results = await asyncio.gather(*tasks)

            for res in chunk_results:
                if res: all_parsed_data.extend(res)

            if delay_between_batches > 0.0 and i < len(chunks) - 1:
                await asyncio.sleep(delay_between_batches)

        return all_parsed_data, failed_sources
    finally:
        if should_close and _client is not None:
            await _client.aclose()