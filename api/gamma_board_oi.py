"""Private SPX board: current Massive contract OI, keyed by exact OCC identity.

The returned UTC timestamp records retrieval of the complete snapshot. The
provider does not date each OI field. An absent key means unavailable and an
explicit 0 means reported zero. No prior clearing date is inferred.
"""
import asyncio
import json
import math
import re
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from threading import Lock
from time import monotonic
from urllib.parse import parse_qsl, unquote, urlsplit
from zoneinfo import ZoneInfo

import aiohttp

from config import settings

_HOST = "api.polygon.io"
_PATH = "/v3/snapshot/options/I:SPX"
_URL = "https://" + _HOST + _PATH
_DEADLINE = 12.0
_MAX_PAGES = 50
_LIMIT = 250
_PAGE_BYTES = 2 * 1024 * 1024
_TTL = 3600
_COOLDOWN = 60
_OCC = re.compile(r"O:((SPXW|SPXO|SPX)([0-9]{6})([CP])([0-9]{8}))\Z")
_build_lock = Lock()
_cache = None
_failed_until = 0.0


class OpenInterestUnavailable(RuntimeError):
    """Safe reason, without request URLs, credentials, cursors or payloads."""


def _cursor(url):
    if not isinstance(url, str) or len(url) > 8192:
        raise OpenInterestUnavailable("Invalid continuation")
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.netloc != _HOST
            or unquote(parsed.path) != _PATH or parsed.fragment):
        raise OpenInterestUnavailable("Invalid continuation")
    query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    if (len(query) != 1 or query[0][0] != "cursor"
            or not re.fullmatch(r"[a-zA-Z0-9_+=/.-]{1,4096}", query[0][1])):
        raise OpenInterestUnavailable("Invalid continuation")
    return query[0][1]


def _accumulate(rows, seen, oi, day, end, low, high):
    for row in rows:
        details = row["details"]
        ticker = details["ticker"]
        match = _OCC.fullmatch(ticker)
        if not match:
            # An adjusted/nonstandard series has no matching standard board row.
            if isinstance(ticker, str) and re.fullmatch(r"O:SPX[A-Z0-9]{1,4}[0-9]{6}[CP][0-9]{8}", ticker):
                continue
            raise OpenInterestUnavailable("Invalid contract identity")
        key, _root, expiry_code, side, strike_code = match.groups()
        if key in seen:
            raise OpenInterestUnavailable("Duplicate contract identity")
        seen.add(key)
        expiry = date.fromisoformat(details["expiration_date"])
        strike = int(strike_code) / 1000
        if (expiry.strftime("%y%m%d") != expiry_code or not day <= expiry <= end
                or details["strike_price"] != strike or not low <= strike <= high
                or details["contract_type"] != ("call" if side == "C" else "put")):
            raise OpenInterestUnavailable("Inconsistent contract metadata")
        if details.get("shares_per_contract", 100) != 100:
            continue
        value = row.get("open_interest")
        if value is None:
            continue
        if type(value) is not int or value < 0:
            raise OpenInterestUnavailable("Invalid open-interest field")
        oi[key] = value


async def _download(secret, day, low, high):
    end = day + timedelta(days=30)
    params = {"expiration_date.gte": day.isoformat(), "expiration_date.lte": end.isoformat(),
              "strike_price.gte": str(low), "strike_price.lte": str(high), "limit": str(_LIMIT)}
    seen, cursors, oi = set(), set(), {}
    async with aiohttp.ClientSession(
        headers={"Authorization": "Bearer " + secret, "Accept": "application/json"},
        timeout=aiohttp.ClientTimeout(total=6, sock_connect=3, sock_read=3),
        trust_env=False,
    ) as client:
        for _ in range(_MAX_PAGES):
            async with client.get(_URL, params=params, allow_redirects=False) as response:
                if response.status != 200:
                    raise OpenInterestUnavailable("OI provider unavailable")
                body = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    if len(body) + len(chunk) > _PAGE_BYTES:
                        raise OpenInterestUnavailable("OI response too large")
                    body.extend(chunk)
            doc = json.loads(body)
            rows = doc.get("results")
            if doc.get("status") not in ("OK", "DELAYED") or not isinstance(rows, list) or not 0 < len(rows) <= _LIMIT:
                raise OpenInterestUnavailable("Invalid OI result page")
            _accumulate(rows, seen, oi, day, end, low, high)
            next_url = doc.get("next_url")
            if next_url is None:
                if len(rows) == _LIMIT or not oi:
                    raise OpenInterestUnavailable("OI coverage could not be confirmed")
                return oi
            cursor = _cursor(next_url)
            if cursor in cursors:
                raise OpenInterestUnavailable("Repeated continuation")
            cursors.add(cursor)
            # Send only the cursor to our fixed endpoint, never the supplied URL.
            params = {"cursor": cursor}
    raise OpenInterestUnavailable("OI pagination limit exceeded")


def read_spx_oi(*, day: date, low: float, high: float, now: datetime) -> tuple[dict[str, int], datetime]:
    """Fetch using a bounded sync route worker. Coalesce requests into a 100-point band."""
    global _cache, _failed_until
    secret = settings.POLYGON_API_KEY
    if not secret:
        raise OpenInterestUnavailable("OI provider disabled")
    if (now.tzinfo is None or now.utcoffset() is None or day != now.astimezone(ZoneInfo("America/New_York")).date()
            or not (math.isfinite(low) and math.isfinite(high) and 0 < low < high)):
        raise OpenInterestUnavailable("Invalid OI window")
    lo, hi = math.floor(low / 100) * 100, math.ceil(high / 100) * 100
    key = (day, lo, hi, sha256(secret.encode()).digest())
    if not _build_lock.acquire(blocking=False):
        raise OpenInterestUnavailable("OI refresh in progress")
    try:
        if _cache is not None and _cache[0] == key and monotonic() < _cache[1]:
            return _cache[2].copy(), _cache[3]
        if monotonic() < _failed_until:
            raise OpenInterestUnavailable("OI provider cooling down")
        try:
            values = asyncio.run(asyncio.wait_for(_download(secret, day, lo, hi), timeout=_DEADLINE))
        except Exception:
            _failed_until = monotonic() + _COOLDOWN
            # Exceptions from network/JSON libraries may contain external data.
            raise OpenInterestUnavailable("SPX contract OI unavailable") from None
        receipt = datetime.now(timezone.utc)
        _cache = (key, monotonic() + _TTL, values, receipt)
        return values.copy(), receipt
    finally:
        _build_lock.release()
