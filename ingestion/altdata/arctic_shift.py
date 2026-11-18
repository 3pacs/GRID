"""Reddit posts and comments from the Arctic Shift archive API (no login, no key).

Why
---
Since about 17:50Z on 2026-11-17 Reddit answers every logged-out read with
403 (``.json``) or 404 (``.rss``), which stopped ``smart_money``,
``reddit_options_pulse`` and the Reddit part of ``social_sentiment``. The
Data API needs an owner-registered app (``reddit_oauth.py``) and is paid at
scale. Arctic Shift (https://github.com/ArthurHeitmann/arctic_shift, a
community-run successor to Pushshift) ingests Reddit continuously and serves
it over a free JSON API without credentials. Probed from grid-svr on
2026-11-18: newest r/wallstreetbets post 7 min old, newest comment 3 min old;
r/stocks, r/investing, r/options, r/economics and r/CryptoCurrency within
25 to 140 minutes.

What it is not
--------------
- Only an HTTP data source. Nothing from the project is installed or run.
- Not an authority on votes: scores and comment counts are re-read about
  36 hours after a post (before that they are typically 0 or 1, per the API
  README), so ``post()`` reports ``score``/``num_comments`` as None for a
  younger post instead of a misleading near-zero.
- The aggregate endpoints lag by hours to days (every 24 h hourly bucket
  read back 0 on 2026-11-18), so callers count records themselves.
- Full-text title search on r/wallstreetbets times out ("Timeout. Maybe slow
  down a bit", HTTP 422) more often than not; callers page the newest posts
  and match titles locally.

Rate limits and safety
----------------------
Limits are dynamic (``X-RateLimit-Reset`` seconds on every answer, 429 when
exceeded) and the README asks users to be considerate. Every GRID process
shares one lock file that spaces requests ``MIN_INTERVAL_S`` apart. A 429
waits for the advertised reset when it fits the caller's budget, and the
"slow down" 422 is retried once after ``SLOWDOWN_WAIT_S``.

Responses are untrusted: the host is fixed, redirects are refused, bodies
are capped at ``MAX_RESPONSE_BYTES``, JSON must be ``{"data": [...]}``, and
``post()`` / ``comment()`` keep only typed fields with bounded text. Text is
data for counting and FinBERT only; no URL is ever taken from it.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import stat
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger as log

BASE_URL: str = "https://arctic-shift.photon-reddit.com"
USER_AGENT: str = "GRID-market-research/1.0 (low-volume sentiment counts; python-requests)"
MIN_INTERVAL_S: float = 2.0
DEFAULT_MAX_WAIT_S: float = 90.0
MAX_RESET_WAIT_S: float = 60.0
SLOWDOWN_WAIT_S: float = 5.0
REQUEST_TIMEOUT_S: tuple[float, float] = (10.0, 40.0)
MAX_RESPONSE_BYTES: int = 4_000_000
PAGE_LIMIT: int = 100
SCORE_SETTLED_H: float = 36.0
MAX_TEXT_CHARS: int = 4000
LOCK_PATH_ENV: str = "GRID_ARCTIC_SHIFT_LOCK"

_KINDS = frozenset({"posts", "comments"})
_PARAMS = frozenset({"subreddit", "author", "after", "before", "limit", "sort",
                     "fields", "link_id"})
_SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")
_ID_RE = re.compile(r"^[a-z0-9]{1,13}$")
_AUTHOR_RE = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
# 2005-06-01 .. 2100-01-01: anything else is not a Reddit creation time.
_TS_MIN, _TS_MAX = 1_117_584_000, 4_102_444_800


class ArcticShiftError(RuntimeError):
    """The API answered with something other than usable data."""


class ArcticShiftBusy(ArcticShiftError):
    """No request slot (or rate-limit reset) within the caller's budget."""


# ── cross-process spacing ────────────────────────────────────────────


def _lock_path() -> str:
    return os.environ.get(LOCK_PATH_ENV) or os.path.join(
        tempfile.gettempdir(), "grid-arctic-shift.lock")


def _open_lock() -> int | None:
    """The shared spacing file, or None (uncoordinated) when it is unusable."""
    path = _lock_path()
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        log.warning("arctic_shift: lock {p} unusable ({e}); requests are not "
                    "spaced across processes", p=path, e=str(exc))
        return None
    st = os.fstat(fd)
    # Shared /tmp: only a private regular file of ours may hold the slot.
    if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid()
            or stat.S_IMODE(st.st_mode) & 0o022):
        os.close(fd)
        log.warning("arctic_shift: lock {p} is not a private regular file owned "
                    "by this user; requests are not spaced across processes", p=path)
        return None
    return fd


def _read_last(fd: int) -> float:
    os.lseek(fd, 0, os.SEEK_SET)
    try:
        last = float(os.read(fd, 64).decode().strip() or 0.0)
    except ValueError:
        return 0.0
    return min(last, time.time())


@contextmanager
def _request_slot(deadline: float) -> Iterator[None]:
    """Hold the shared slot for one request, ``MIN_INTERVAL_S`` after the last."""
    fd = _open_lock()
    if fd is None:
        yield
        return
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= deadline:
                    raise ArcticShiftBusy("another process held the Arctic Shift slot "
                                          "past the wait budget") from None
                time.sleep(0.2)
        try:
            wait = _read_last(fd) + MIN_INTERVAL_S - time.time()
            if wait > 0:
                if time.time() + wait > deadline:
                    raise ArcticShiftBusy(f"next Arctic Shift slot is {wait:.1f}s away, "
                                          "past the wait budget")
                time.sleep(wait)
            try:
                yield
            finally:
                os.ftruncate(fd, 0)
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, f"{time.time():.3f}\n".encode())
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


# ── HTTP ─────────────────────────────────────────────────────────────


def read_json_bounded(resp: Any, max_bytes: int | None = None) -> Any:
    """Decode a streamed response body, refusing anything over ``max_bytes``."""
    if max_bytes is None:
        max_bytes = MAX_RESPONSE_BYTES
    declared = (getattr(resp, "headers", None) or {}).get("Content-Length")
    try:
        if declared is not None and int(declared) > max_bytes:
            raise ArcticShiftError(f"response declares {declared} bytes (cap {max_bytes})")
    except ValueError:
        pass
    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=65536):
        total += len(chunk)
        if total > max_bytes:
            raise ArcticShiftError(f"response over {max_bytes} bytes")
        chunks.append(chunk)
    try:
        return json.loads(b"".join(chunks))
    except ValueError as exc:
        raise ArcticShiftError(f"response is not JSON: {exc}") from None


def _close(resp: Any) -> None:
    close = getattr(resp, "close", None)
    if callable(close):
        close()


def _reset_wait(resp: Any) -> float:
    try:
        return max(0.0, float((resp.headers or {}).get("X-RateLimit-Reset", "")))
    except (TypeError, ValueError):
        return MAX_RESET_WAIT_S


def search(
    kind: str,
    params: dict[str, Any],
    *,
    session: Any = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> list[dict[str, Any]]:
    """GET ``/api/<kind>/search`` and return its ``data`` records (dicts only).

    Raises ``ArcticShiftBusy`` when no slot or rate-limit reset fits
    ``max_wait_s``, ``ArcticShiftError`` for any other non-data answer, and
    ``requests.RequestException`` for network failures.
    """
    if kind not in _KINDS:
        raise ValueError(f"unknown Arctic Shift kind {kind!r}")
    unknown = set(params) - _PARAMS
    if unknown:
        raise ValueError(f"unsupported Arctic Shift parameters {sorted(unknown)}")
    sub = params.get("subreddit")
    if sub is not None and not _SUBREDDIT_RE.match(str(sub)):
        raise ValueError(f"invalid subreddit {sub!r}")
    link = params.get("link_id")
    if link is not None and not _ID_RE.match(str(link).removeprefix("t3_")):
        raise ValueError(f"invalid link id {link!r}")
    http = session if session is not None else requests
    url = f"{BASE_URL}/api/{kind}/search"
    deadline = time.time() + max_wait_s
    slowdowns = rate_limits = 0
    while True:
        with _request_slot(deadline):
            resp = http.get(url, params=dict(params), timeout=REQUEST_TIMEOUT_S,
                            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                            allow_redirects=False, stream=True)
        try:
            status = int(resp.status_code)
            if status == 200:
                payload = read_json_bounded(resp)
                data = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(data, list):
                    error = payload.get("error") if isinstance(payload, dict) else None
                    raise ArcticShiftError(f"no data list in answer: {str(error)[:120]}")
                return [d for d in data if isinstance(d, dict)]
            if 300 <= status < 400:
                raise ArcticShiftError(f"HTTP {status} redirect refused")
            if status == 429 and rate_limits == 0:
                wait = min(_reset_wait(resp), MAX_RESET_WAIT_S) + 0.5
                if time.time() + wait > deadline:
                    raise ArcticShiftBusy(f"HTTP 429; reset in {wait:.0f}s is past the wait budget")
                rate_limits += 1
                log.info("arctic_shift: 429 on {k}; waiting {w:.0f}s", k=kind, w=wait)
                time.sleep(wait)
                continue
            if status == 422 and slowdowns == 0 and time.time() + SLOWDOWN_WAIT_S < deadline:
                slowdowns += 1
                log.info("arctic_shift: 422 (query timeout) on {k}; retrying once", k=kind)
                time.sleep(SLOWDOWN_WAIT_S)
                continue
            raise ArcticShiftError(f"HTTP {status} from Arctic Shift {kind} search")
        finally:
            _close(resp)


# ── record normalisation ─────────────────────────────────────────────


def _ts(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    v = float(value)
    return v if math.isfinite(v) and _TS_MIN <= v <= _TS_MAX else None


def _text(value: Any, limit: int = MAX_TEXT_CHARS) -> str:
    if not isinstance(value, str):
        return ""
    if value in ("[deleted]", "[removed]"):
        return ""
    return " ".join(value.split())[:limit]


def _count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _author(value: Any) -> str:
    return value if isinstance(value, str) and _AUTHOR_RE.match(value) else ""


def post(raw: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any] | None:
    """A typed post, or None when the record lacks a valid id or creation time.

    ``score`` and ``num_comments`` are None until ``SCORE_SETTLED_H`` after
    the post was created (the archive re-reads them then).
    """
    pid, created = raw.get("id"), _ts(raw.get("created_utc"))
    if not isinstance(pid, str) or not _ID_RE.match(pid) or created is None:
        return None
    sub = raw.get("subreddit")
    ref = (now or datetime.now(timezone.utc)).timestamp()
    settled = ref - created >= SCORE_SETTLED_H * 3600
    return {
        "id": pid,
        "subreddit": sub if isinstance(sub, str) and _SUBREDDIT_RE.match(sub) else "",
        "author": _author(raw.get("author")),
        "title": _text(raw.get("title"), 300),
        "selftext": _text(raw.get("selftext")),
        "created_utc": created,
        "score": _count(raw.get("score")) if settled else None,
        "num_comments": _count(raw.get("num_comments")) if settled else None,
        # Reddit's mod/admin marker ("moderator", "admin") or None.
        "distinguished": raw.get("distinguished") if raw.get("distinguished") in (
            "moderator", "admin") else None,
    }


def comment(raw: dict[str, Any]) -> dict[str, Any] | None:
    """A typed comment, or None when the record lacks a valid id or creation time."""
    cid, created = raw.get("id"), _ts(raw.get("created_utc"))
    if not isinstance(cid, str) or not _ID_RE.match(cid) or created is None:
        return None
    link = raw.get("link_id")
    link = link.removeprefix("t3_") if isinstance(link, str) else ""
    return {
        "id": cid,
        "author": _author(raw.get("author")),
        "body": _text(raw.get("body")),
        "created_utc": created,
        "link_id": link if _ID_RE.match(link) else "",
    }


# ── paged reads ──────────────────────────────────────────────────────


def recent_posts(
    subreddit: str,
    *,
    since: datetime,
    until: datetime,
    max_pages: int,
    page_limit: int = PAGE_LIMIT,
    session: Any = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> tuple[list[dict[str, Any]], bool]:
    """Posts on one board created in ``[since, until]``, newest first.

    Pages back with ``before=<oldest seen>``. The flag is True when the pages
    reached past ``since`` (or ran dry), i.e. the list is the whole window;
    False when ``max_pages`` ran out first.
    """
    lo, hi = int(since.timestamp()), int(until.timestamp())
    found: dict[str, dict[str, Any]] = {}
    before = hi + 1
    for _ in range(max(1, max_pages)):
        batch = search("posts", {
            "subreddit": subreddit, "after": lo, "before": before,
            "limit": max(1, min(page_limit, PAGE_LIMIT)),
            "sort": "desc", "fields": ("id,subreddit,author,title,selftext,created_utc,score,"
                                     "num_comments,distinguished"),
        }, session=session, max_wait_s=max_wait_s)
        oldest: float | None = None
        for raw in batch:
            p = post(raw, now=until)
            if p is None:
                continue
            if p["subreddit"] and p["subreddit"].lower() != subreddit.lower():
                continue  # never count another board's post
            oldest = p["created_utc"] if oldest is None else min(oldest, p["created_utc"])
            if lo <= p["created_utc"] <= hi:
                found.setdefault(p["id"], p)
        if len(batch) < page_limit or oldest is None or oldest <= lo:
            return sorted(found.values(), key=lambda x: -x["created_utc"]), True
        before = int(oldest)  # strictly older next; same-second ties are rare
    return sorted(found.values(), key=lambda x: -x["created_utc"]), False


def newest_comments(
    subreddit: str,
    *,
    since: datetime,
    until: datetime,
    session: Any = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> tuple[list[dict[str, Any]], bool]:
    """Up to one page of the newest comments on a board within ``[since, until]``.

    The flag is True when the page held the whole window (fewer than a page).
    """
    lo, hi = int(since.timestamp()), int(until.timestamp())
    batch = search("comments", {
        "subreddit": subreddit, "after": lo, "before": hi + 1, "limit": PAGE_LIMIT,
        "sort": "desc", "fields": "id,author,body,created_utc,link_id",
    }, session=session, max_wait_s=max_wait_s)
    out = [c for c in (comment(r) for r in batch) if c and lo <= c["created_utc"] <= hi]
    return out, len(batch) < PAGE_LIMIT


def thread_comments(
    link_id: str,
    *,
    max_comments: int,
    session: Any = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> list[dict[str, Any]]:
    """The newest ``max_comments`` comments of one thread, newest first."""
    link = link_id.removeprefix("t3_")
    found: dict[str, dict[str, Any]] = {}
    before: int | None = None
    while len(found) < max_comments:
        params: dict[str, Any] = {"link_id": link, "limit": PAGE_LIMIT, "sort": "desc",
                                  "fields": "id,author,body,created_utc,link_id"}
        if before is not None:
            params["before"] = before
        batch = search("comments", params, session=session, max_wait_s=max_wait_s)
        new = 0
        oldest: float | None = None
        for raw in batch:
            c = comment(raw)
            if c is None or (c["link_id"] and c["link_id"] != link):
                continue
            oldest = c["created_utc"] if oldest is None else min(oldest, c["created_utc"])
            if c["id"] not in found:
                found[c["id"]] = c
                new += 1
        if len(batch) < PAGE_LIMIT or oldest is None or new == 0:
            break
        before = int(oldest)
    return sorted(found.values(), key=lambda x: -x["created_utc"])[:max_comments]


__all__ = [
    "BASE_URL",
    "LOCK_PATH_ENV",
    "MIN_INTERVAL_S",
    "PAGE_LIMIT",
    "SCORE_SETTLED_H",
    "USER_AGENT",
    "ArcticShiftBusy",
    "ArcticShiftError",
    "comment",
    "newest_comments",
    "post",
    "read_json_bounded",
    "recent_posts",
    "search",
    "thread_comments",
]
