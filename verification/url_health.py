"""Async URL health checker with Wayback Machine classification.

Classifies URLs as LIVE, DEAD, LIKELY_HALLUCINATED, or UNKNOWN using
HTTP HEAD requests and the Wayback Machine CDX API.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any

import aiohttp
from loguru import logger as log


class URLClassification(str, Enum):
    LIVE = "LIVE"
    DEAD = "DEAD"
    LIKELY_HALLUCINATED = "LIKELY_HALLUCINATED"
    UNKNOWN = "UNKNOWN"
    BOT_BLOCKED = "BOT_BLOCKED"  # 403 from known-real domains


# Major domains that return 403 to HEAD requests (bot protection) but are real
_BOT_BLOCKED_DOMAINS = frozenset({
    "reuters.com", "wsj.com", "bloomberg.com", "ft.com",
    "nasdaq.com", "spglobal.com", "cnbc.com", "marketwatch.com",
    "nytimes.com", "washingtonpost.com", "economist.com",
    "bls.gov", "sec.gov", "treasury.gov",
})


@dataclass(frozen=True)
class URLCheckResult:
    """Result of a single URL health check."""

    url: str
    classification: URLClassification
    http_status: int | None
    wayback_url: str | None
    latency_ms: int
    error: str | None = None


# ── Wayback CDX API ─────────────────────────────────────────────────────────

_WAYBACK_CDX = "http://web.archive.org/cdx/search/cdx"


async def _check_wayback(
    session: aiohttp.ClientSession, url: str, timeout_s: float = 5.0,
) -> str | None:
    """Query Wayback Machine CDX API. Returns archived URL or None."""
    try:
        params = {"url": url, "output": "json", "limit": "1"}
        async with session.get(
            _WAYBACK_CDX, params=params, timeout=aiohttp.ClientTimeout(total=timeout_s),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            # CDX returns [[header], [row]] — row has timestamp at index 1
            if len(data) >= 2 and len(data[1]) >= 2:
                timestamp = data[1][1]
                return f"https://web.archive.org/web/{timestamp}/{url}"
            return None
    except Exception:
        return None


# ── Constants ───────────────────────────────────────────────────────────────

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── URL Check ───────────────────────────────────────────────────────────────

async def _fetch_status(
    session: aiohttp.ClientSession,
    url: str,
    timeout: aiohttp.ClientTimeout,
) -> int:
    """Try HEAD, fall back to GET on 403/405. Returns HTTP status code."""
    try:
        async with session.head(
            url, timeout=timeout, allow_redirects=True,
            max_redirects=3, headers=_HEADERS,
        ) as resp:
            if resp.status not in (403, 405):
                return resp.status
    except Exception:
        pass

    # HEAD blocked or failed — try GET
    async with session.get(
        url, timeout=timeout, allow_redirects=True,
        max_redirects=3, headers=_HEADERS,
    ) as resp:
        await resp.read()  # Consume body to release connection
        return resp.status


async def check_url(
    session: aiohttp.ClientSession,
    url: str,
    timeout_s: float = 5.0,
    wayback_enabled: bool = True,
) -> URLCheckResult:
    """Check a single URL via HTTP HEAD (+ GET fallback), classify via Wayback."""
    t0 = time.monotonic()

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        status = await _fetch_status(session, url, timeout)
        latency = int((time.monotonic() - t0) * 1000)

        # 2xx/3xx → LIVE
        if 200 <= status < 400:
            return URLCheckResult(
                url=url, classification=URLClassification.LIVE,
                http_status=status, wayback_url=None, latency_ms=latency,
            )

        # 404/410 → check Wayback to distinguish DEAD from HALLUCINATED
        if status in (404, 410):
            wayback_url = None
            if wayback_enabled:
                wayback_url = await _check_wayback(session, url, timeout_s)

            if wayback_url:
                return URLCheckResult(
                    url=url, classification=URLClassification.DEAD,
                    http_status=status, wayback_url=wayback_url,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                )
            return URLCheckResult(
                url=url, classification=URLClassification.LIKELY_HALLUCINATED,
                http_status=status, wayback_url=None,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )

        # 403 from known major domains → BOT_BLOCKED (real site, bot protection)
        if status == 403:
            from urllib.parse import urlparse
            domain = urlparse(url).hostname or ""
            for bd in _BOT_BLOCKED_DOMAINS:
                if domain == bd or domain.endswith("." + bd):
                    return URLCheckResult(
                        url=url, classification=URLClassification.BOT_BLOCKED,
                        http_status=status, wayback_url=None,
                        latency_ms=int((time.monotonic() - t0) * 1000),
                    )

        # Everything else → UNKNOWN
        return URLCheckResult(
            url=url, classification=URLClassification.UNKNOWN,
            http_status=status, wayback_url=None,
            latency_ms=int((time.monotonic() - t0) * 1000),
        )

    except Exception as exc:
        latency = int((time.monotonic() - t0) * 1000)
        return URLCheckResult(
            url=url, classification=URLClassification.UNKNOWN,
            http_status=None, wayback_url=None,
            latency_ms=latency, error=str(exc),
        )


# ── Batch Check ─────────────────────────────────────────────────────────────

async def check_urls(
    urls: list[str],
    *,
    max_concurrent: int = 10,
    rate_limit: float = 10.0,
    timeout_s: float = 5.0,
    wayback_enabled: bool = True,
) -> list[URLCheckResult]:
    """Batch-check URLs with concurrency limit and rate limiting.

    Args:
        urls: List of URLs to check.
        max_concurrent: Max parallel requests.
        rate_limit: Max requests per second.
        timeout_s: Per-URL timeout.
        wayback_enabled: Whether to check Wayback Machine for 404s.

    Returns:
        List of URLCheckResult in same order as input URLs.
    """
    if not urls:
        return []

    semaphore = asyncio.Semaphore(max_concurrent)
    delay = 1.0 / rate_limit if rate_limit > 0 else 0

    async def _check_with_limit(
        session: aiohttp.ClientSession, url: str, idx: int,
    ) -> tuple[int, URLCheckResult]:
        async with semaphore:
            if delay > 0 and idx > 0:
                await asyncio.sleep(delay * (idx % max_concurrent == 0))
            result = await check_url(session, url, timeout_s, wayback_enabled)
            return idx, result

    connector = aiohttp.TCPConnector(limit_per_host=2, limit=max_concurrent)
    async with aiohttp.ClientSession(connector=connector, headers=_HEADERS) as session:
        tasks = [
            _check_with_limit(session, url, i)
            for i, url in enumerate(urls)
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Reconstruct in order
    results: list[URLCheckResult | None] = [None] * len(urls)
    for item in raw_results:
        if isinstance(item, Exception):
            continue
        idx, result = item
        results[idx] = result

    # Fill any gaps from exceptions
    return [
        r if r is not None else URLCheckResult(
            url=urls[i], classification=URLClassification.UNKNOWN,
            http_status=None, wayback_url=None, latency_ms=0,
            error="gather exception",
        )
        for i, r in enumerate(results)
    ]
