"""Unauthenticated Reddit reads through RSS/Atom feeds, shared by every puller.

Why
---
grid-svr gets ``403 Blocked`` on ``www.reddit.com/*.json`` and no Data API
app was ever registered (``ingestion/altdata/reddit_oauth.py``), so every
Reddit puller has been failing. Reddit's Atom feeds answer without
credentials (probed from grid-svr 2026-11-05):

- listings: ``/r/<a+b+c>/hot.rss?limit=100`` (one feed for many boards;
  pinned posts come first)
- comment threads: ``/r/<sub>/comments/<id>.rss?limit=500`` (about 470
  comments from a WSB daily thread; the first entry is the post itself)

Feeds carry id, title, author, body and timestamps, but no vote counts,
comment counts or upvote ratios. Search feeds work too but lag by weeks, so
pullers should find current threads in listings.

Rate limit
----------
Unauthenticated RSS allows about one request per minute per IP: after one
call the response says ``x-ratelimit-remaining: 0.0`` and further calls get
429 until ``x-ratelimit-reset``. grid-scheduler and grid-intelligence both
read Reddit, so every request goes through one file lock that also records
the time of the last request. Callers queue on the lock, and each request
starts at least ``MIN_INTERVAL_S`` after the previous one. A caller that
cannot get its turn within ``max_wait_s`` gets ``RedditRateLimited`` without
sending anything. A 429 (another client on the IP) is retried once, at the
next free slot, when that still fits the budget.
"""

from __future__ import annotations

import fcntl
import html
import os
import re
import stat
import tempfile
import time
import xml.etree.ElementTree as ET  # stdlib expat caps entity expansion (3.14)
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from loguru import logger as log

RSS_BASE: str = "https://www.reddit.com"
USER_AGENT: str = "linux:grid-intelligence:1.0 (research; grid.stepdad.finance)"
MIN_INTERVAL_S: float = 61.0
DEFAULT_MAX_WAIT_S: float = 150.0
REQUEST_TIMEOUT_S: int = 20
LOCK_PATH_ENV: str = "GRID_REDDIT_RSS_LOCK"

_ATOM = "{http://www.w3.org/2005/Atom}"
_MD_BODY = re.compile(r'<div class="md">(.*?)</div>', re.DOTALL)
_TAG = re.compile(r"<[^>]+>")


class RedditRateLimited(RuntimeError):
    """No request slot within the caller's wait budget; nothing was sent."""


@dataclass(frozen=True)
class RssEntry:
    """One Atom entry: a post (``t3_``) or a comment (``t1_``)."""

    id: str
    title: str
    author: str
    subreddit: str
    published: datetime | None
    body: str
    link: str

    @property
    def kind(self) -> str:
        return self.id[:2]

    @property
    def short_id(self) -> str:
        return self.id[3:] if self.id[2:3] == "_" else self.id


def body_text(content_html: str, max_chars: int = 4000) -> str:
    """Post or comment text from an entry's HTML; '' for link posts.

    Reddit wraps the text in ``<div class="md">``; the rest of an entry is the
    "submitted by /u/... [link] [comments]" footer, which is not post text.
    """
    match = _MD_BODY.search(content_html or "")
    if not match:
        return ""
    text = html.unescape(_TAG.sub(" ", match.group(1)))
    return " ".join(text.split())[:max_chars]


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_feed(content: bytes | str) -> list[RssEntry]:
    """Parse an Atom feed from Reddit into entries, in feed order."""
    root = ET.fromstring(content)
    entries: list[RssEntry] = []
    for entry in root.iter(f"{_ATOM}entry"):
        category = entry.find(f"{_ATOM}category")
        link = entry.find(f"{_ATOM}link")
        author = (entry.findtext(f"{_ATOM}author/{_ATOM}name") or "").strip()
        entries.append(RssEntry(
            id=entry.findtext(f"{_ATOM}id", ""),
            title=entry.findtext(f"{_ATOM}title", ""),
            author=author.removeprefix("/u/"),
            subreddit=category.get("term", "") if category is not None else "",
            published=_parse_time(entry.findtext(f"{_ATOM}published")
                                  or entry.findtext(f"{_ATOM}updated")),
            body=body_text(entry.findtext(f"{_ATOM}content", "")),
            link=link.get("href", "") if link is not None else "",
        ))
    return entries


def _lock_path() -> str:
    return os.environ.get(LOCK_PATH_ENV) or os.path.join(
        tempfile.gettempdir(), "grid-reddit-rss.lock")


def _open_lock() -> int | None:
    """Open the shared lock file, or None (uncoordinated) when it is unusable."""
    path = _lock_path()
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        log.warning("reddit_rss: lock {p} unusable ({e}); requests are not "
                    "spaced across processes", p=path, e=str(exc))
        return None
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        log.warning("reddit_rss: lock {p} is not a regular file; ignoring it", p=path)
        return None
    return fd


def _read_last(fd: int) -> float:
    os.lseek(fd, 0, os.SEEK_SET)
    try:
        return float(os.read(fd, 64).decode().strip() or 0.0)
    except ValueError:
        return 0.0


def _write_last(fd: int, when: float) -> None:
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, f"{when:.3f}\n".encode())


@contextmanager
def _request_slot(deadline: float) -> Iterator[None]:
    """Hold the cross-process Reddit slot for one request.

    Waits for the lock and for ``MIN_INTERVAL_S`` since the last recorded
    request, then yields; the request time is recorded when the block exits.
    Raises ``RedditRateLimited`` (nothing recorded) if the turn would come
    after ``deadline``.
    """
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
                    raise RedditRateLimited(
                        "another process held the Reddit request slot past the wait budget"
                    ) from None
                time.sleep(0.5)
        try:
            wait = _read_last(fd) + MIN_INTERVAL_S - time.time()
            if wait > 0:
                if time.time() + wait > deadline:
                    raise RedditRateLimited(
                        f"next Reddit request slot is {wait:.0f}s away, past the wait budget")
                time.sleep(wait)
            try:
                yield
            finally:
                _write_last(fd, time.time())
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def fetch_feed(
    path: str,
    params: dict[str, Any] | None = None,
    *,
    session: Any = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> list[RssEntry]:
    """GET one Reddit feed (``path`` like ``/r/options/hot.rss``) and parse it.

    Raises ``RedditRateLimited`` when no request slot comes up within
    ``max_wait_s``, and ``requests.HTTPError`` on any other HTTP failure
    (including a second 429).
    """
    http = session if session is not None else requests
    deadline = time.time() + max_wait_s
    for attempt in (1, 2):
        with _request_slot(deadline):
            resp = http.get(f"{RSS_BASE}{path}", params=params,
                            headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT_S)
        if resp.status_code == 429 and attempt == 1:
            log.info("reddit_rss: 429 on {p}; retrying at the next slot", p=path)
            continue
        resp.raise_for_status()
        return parse_feed(resp.content)
    raise AssertionError("unreachable")  # the loop returns or raises


__all__ = [
    "DEFAULT_MAX_WAIT_S",
    "LOCK_PATH_ENV",
    "MIN_INTERVAL_S",
    "RSS_BASE",
    "USER_AGENT",
    "RedditRateLimited",
    "RssEntry",
    "body_text",
    "fetch_feed",
    "parse_feed",
]
