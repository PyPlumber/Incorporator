"""Network and file I/O operations for Incorporator.

Manages scoped HTTP connection pooling, transparent retry resilience via tenacity,
and zero-boilerplate API pagination with dynamic rate limiting.
"""

import asyncio
import re
from typing import AsyncGenerator, Callable, Optional

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from .exceptions import IncorporatorNetworkError


# ==========================================
# 1. DYNAMIC RATE LIMITER
# ==========================================
class RateLimiter:
    """Provides precise, context-aware requests-per-second throttling."""

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
# 2. LOCAL FILE I/O
# ==========================================
def _sync_read(file_path: str) -> str:
    """Synchronous file reader."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        raise IncorporatorNetworkError(f"Failed to read file {file_path}: {e}")


async def _read_file(file_path: str) -> str:
    """Reads raw text from a local file without blocking the async event loop."""
    return await asyncio.to_thread(_sync_read, file_path)


# ==========================================
# 3. LIVE NETWORK ENGINE (With Resilience)
# ==========================================
@retry(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True
)
async def _execute_get(url: str, client: httpx.AsyncClient,
                       rate_limiter: Optional[RateLimiter] = None) -> httpx.Response:
    """Executes a single GET request using the provided client, with jittered retries."""
    if rate_limiter:
        await rate_limiter.wait()

    response = await client.get(url)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status < 500 and status != 429:
            raise IncorporatorNetworkError(f"Fatal client error {status} for URL {url}: {e}") from e
        raise e

    return response


def _extract_rfc5988_next_link(link_header: str) -> Optional[str]:
    """Parses standard API Link headers to find the 'next' page URL."""
    links = link_header.split(",")
    for link in links:
        if 'rel="next"' in link:
            match = re.search(r'<(.*?)>', link)
            if match:
                return match.group(1)
    return None


async def stream_raw_data(
        source: str,
        is_file: bool = False,
        paginate: bool = False,
        next_url_extractor: Optional[Callable[[str], Optional[str]]] = None,
        call_lim: Optional[int] = None,  # <--- NEW: Added call_lim parameter
        client: Optional[httpx.AsyncClient] = None,
        rate_limiter: Optional[RateLimiter] = None,
        ignore_ssl: bool = False
) -> AsyncGenerator[str, None]:
    """Advanced router that yields data payloads and natively handles connection scopes."""
    if is_file:
        yield await _read_file(source)
        return

    async def _run_stream(c: httpx.AsyncClient) -> AsyncGenerator[str, None]:
        current_url: Optional[str] = source
        calls_made = 0  # <--- NEW: Track execution count

        while current_url:
            # <--- NEW: Enforce pagination limit
            if call_lim is not None and calls_made >= call_lim:
                break

            try:
                response = await _execute_get(current_url, c, rate_limiter)
            except httpx.HTTPStatusError as e:
                raise e
            except Exception as e:
                raise IncorporatorNetworkError(f"Failed to fetch data from {current_url} after all retries: {e}") from e

            yield response.text
            calls_made += 1  # <--- NEW: Increment execution count

            if not paginate:
                break

            next_url: Optional[str] = None
            if "link" in response.headers:
                next_url = _extract_rfc5988_next_link(response.headers["link"])

            if not next_url and next_url_extractor:
                try:
                    next_url = next_url_extractor(response.text)
                except Exception as e:
                    raise IncorporatorNetworkError(f"Pagination extractor failed on {current_url}: {e}") from e

            current_url = next_url

    if client:
        async for data in _run_stream(client):
            yield data
    else:
        limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0, limits=limits, verify=not ignore_ssl) as c:
            async for data in _run_stream(c):
                yield data