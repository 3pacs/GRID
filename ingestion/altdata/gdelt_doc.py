"""One request gate for the GDELT DOC 2.0 API, shared by GRID's callers.

Why
---
Four GRID components call ``api.gdeltproject.org/api/v2/doc/doc`` from
grid-svr's one public IP, each with its own private spacing:

- ``intelligence/breaking_news.py`` (grid-breaking-news, nonstop): 12 queries
  per cycle, a cycle every ~5 min, about 3,500 requests a day;
- ``ingestion/altdata/gdelt.py`` (SmartScheduler hourly, grid-scheduler
  daily, hermes_fixers repairs);
- ``intelligence/spider/sources/news_cooccurrence.py`` (grid-spider, hourly);
- ``ingestion/altdata/actor_news_puller.py`` (grid-intelligence, daily batch
  at 2 s spacing). It stops its batch on the first 429 and is not on this
  gate yet (its batch-budget contract has its own tests).

None of the first three knew about the others or stopped after a 429: the
breaking-news monitor slept 12 s and sent the next query, so the IP was
being throttled almost all day (2026-11-08: 1,670 of its responses were 429
and 1,027 more timed out waiting for one). The scheduled GDELT puller then
got 429 on its *first* request in most runs and reported FAILED.

GDELT answers a throttled request with HTTP 429 and the text "Please limit
requests to one every 5 seconds"; it sends no ``Retry-After`` header
(probed 2026-11-09). GDELT also refuses most requests from a quiet IP: a
laptop that sent nothing else got 1 answer in 17 requests spaced 20-120 s
apart. So the documented 5 s is a floor, and no spacing makes the DOC API
reliable; what a client controls is not adding to the throttling, retrying
inside its budget, and reporting what happened.

What this module does
---------------------
Every DOC request goes through one file lock in the system temp directory
(all GRID units run as ``grid`` without PrivateTmp). The lock file holds
three numbers: when the last request ended, until when the host is cooling
down after a 429, and how many 429s came in a row.

- At most one DOC request is in flight per host, and a request starts at
  least ``MIN_INTERVAL_S`` after the previous one ended.
- A 429 starts a host-wide cooldown: ``Retry-After`` if GDELT ever sends
  one, otherwise ``BASE_COOLDOWN_S`` doubling per consecutive 429 up to
  ``MAX_COOLDOWN_S``. Every caller in every process waits it out; nobody
  sends into it. Any non-429 answer resets the streak.
- A caller that cannot get a slot within its ``max_wait_s`` gets
  ``GDELTRateLimited`` with ``sent=False`` -- nothing went on the wire.
  A 429 that is retried at the next free slot (when that still fits the
  budget) means a run resumes instead of abandoning its remaining queries.
- ``priority="low"`` callers (the nonstop monitor, batch crawlers) need an
  extra ``LOW_PRIORITY_EXTRA_S`` of idle time, so a waiting scheduled puller
  takes the next slot first.
"""

from __future__ import annotations

import email.utils
import fcntl
import json
import math
import os
import stat
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests
from loguru import logger as log

DOC_API_URL: str = "https://api.gdeltproject.org/api/v2/doc/doc"

# GDELT's 429 body: "Please limit requests to one every 5 seconds". The gap is
# measured from the end of the previous request (answers take 10-20 s).
MIN_INTERVAL_S: float = 6.0
LOW_PRIORITY_EXTRA_S: float = 6.0
BASE_COOLDOWN_S: float = 60.0
MAX_COOLDOWN_S: float = 900.0
DEFAULT_MAX_WAIT_S: float = 90.0
# (connect, read): GDELT routinely takes 10-20 s to answer, 429s included,
# and its TLS handshake alone sometimes outlasts a 10 s connect timeout
# (probe 2026-11-09: "Read timed out. (read timeout=10.0)" on the first request).
REQUEST_TIMEOUT: tuple[float, float] = (20.0, 40.0)
USER_AGENT: str = "GRID/1.0 (research; grid.stepdad.finance)"
LOCK_PATH_ENV: str = "GRID_GDELT_DOC_LOCK"

_POLL_S: float = 0.5

# Seams for tests: wall clock and sleep.
_now = time.time
_sleep = time.sleep


class GDELTRateLimited(RuntimeError):
    """No usable GDELT DOC slot within the caller's wait budget.

    Attributes:
        sent: True when this call sent a request that GDELT refused with 429;
            False when nothing was sent (another caller held the slot, or the
            host is cooling down after an earlier 429).
        retry_at: Epoch seconds when the host may send again, if known.
    """

    def __init__(self, message: str, *, sent: bool, retry_at: float | None) -> None:
        super().__init__(message)
        self.sent = sent
        self.retry_at = retry_at


@dataclass
class _GateState:
    last_end: float = 0.0
    blocked_until: float = 0.0
    strikes: int = 0


def _lock_path() -> str:
    return os.environ.get(LOCK_PATH_ENV) or os.path.join(
        tempfile.gettempdir(), "grid-gdelt-doc.lock")


def _open_lock() -> int | None:
    """Open the shared gate file, or None (uncoordinated) when it is unusable."""
    path = _lock_path()
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        log.warning("gdelt_doc: gate {p} unusable ({e}); DOC requests are not "
                    "coordinated across processes", p=path, e=str(exc))
        return None
    st = os.fstat(fd)
    # /tmp is shared: a file planted or left writable by another user could
    # stall or unthrottle every GDELT caller, so only our own private file counts.
    if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid()
            or stat.S_IMODE(st.st_mode) & 0o022):
        os.close(fd)
        log.warning("gdelt_doc: gate {p} is not a private regular file owned by "
                    "this user; DOC requests are not coordinated", p=path)
        return None
    return fd


def _read_state(fd: int | None, fallback: _GateState) -> _GateState:
    if fd is None:
        return fallback
    os.lseek(fd, 0, os.SEEK_SET)
    try:
        raw = json.loads(os.read(fd, 512).decode() or "{}")
        now = _now()
        return _GateState(
            # A timestamp in the future is a clock step or a bad write:
            # treat the last request as "just now", never as "far away".
            last_end=min(float(raw.get("last_end", 0.0)), now),
            blocked_until=min(float(raw.get("blocked_until", 0.0)), now + MAX_COOLDOWN_S),
            strikes=max(0, int(raw.get("strikes", 0))),
        )
    except (ValueError, TypeError, AttributeError):
        return _GateState()


def _write_state(fd: int | None, state: _GateState, fallback: _GateState) -> None:
    fallback.last_end, fallback.blocked_until, fallback.strikes = (
        state.last_end, state.blocked_until, state.strikes)
    if fd is None:
        return
    payload = json.dumps({"last_end": round(state.last_end, 3),
                          "blocked_until": round(state.blocked_until, 3),
                          "strikes": state.strikes}).encode()
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, payload)


# Used only when the shared file cannot be opened: still paces this process.
_PROCESS_STATE = _GateState()


def parse_retry_after(value: str | None, now: float) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (delta or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed is None:
            return None
        seconds = parsed.timestamp() - now
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def cooldown_after_429(strikes: int, retry_after: str | None, now: float) -> float:
    """Host-wide cooldown after the ``strikes``-th consecutive 429."""
    hinted = parse_retry_after(retry_after, now)
    if hinted is not None:
        return min(max(hinted, MIN_INTERVAL_S), MAX_COOLDOWN_S)
    return min(BASE_COOLDOWN_S * (2 ** max(0, strikes - 1)), MAX_COOLDOWN_S)


def next_slot_at(state: _GateState, priority: str = "normal") -> float:
    """Earliest epoch time a caller of this priority may send."""
    gap = MIN_INTERVAL_S + (LOW_PRIORITY_EXTRA_S if priority == "low" else 0.0)
    return max(state.last_end + gap, state.blocked_until)


def doc_get(
    params: Mapping[str, Any],
    *,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
    attempts: int = 2,
    priority: str = "normal",
    timeout: tuple[float, float] | float = REQUEST_TIMEOUT,
    session: Any = None,
    caller: str = "",
) -> requests.Response:
    """Send one GDELT DOC API GET through the shared gate.

    Parameters:
        params: Query parameters (``query``, ``mode``, ``timespan``, ...).
        max_wait_s: Longest this call may wait for a slot, cooldowns included.
        attempts: Requests this call may send; a 429 is retried at the next
            free slot while attempts and the wait budget remain.
        priority: ``"normal"`` or ``"low"`` (yields the next slot to normal).
        timeout: ``requests`` timeout.
        session: Optional ``requests.Session``-like object.
        caller: Short label for logs.

    Returns:
        The first non-429 response (the caller checks its status).

    Raises:
        GDELTRateLimited: No slot within ``max_wait_s``, or every attempt got 429.
        requests.RequestException: Network errors (timeouts, resets).
    """
    if priority not in {"normal", "low"}:
        raise ValueError("priority must be 'normal' or 'low'")
    http = session if session is not None else requests
    deadline = _now() + max(0.0, max_wait_s)
    sent = False
    retry_at: float | None = None
    label = caller or "gdelt"
    for attempt in range(1, max(1, attempts) + 1):
        fd = _open_lock()
        try:
            while True:
                if fd is not None:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        # Another process is mid-request; its answer sets the
                        # next slot, so poll rather than queue on the lock.
                        if _now() + _POLL_S > deadline:
                            raise GDELTRateLimited(
                                f"{label}: GDELT DOC slot held by another caller past the "
                                f"{max_wait_s:.0f}s wait budget", sent=sent, retry_at=None,
                            ) from None
                        _sleep(_POLL_S)
                        continue
                state = _read_state(fd, _PROCESS_STATE)
                slot = next_slot_at(state, priority)
                wait = slot - _now()
                if wait <= 0:
                    break  # keep the lock: the request below is ours alone
                if fd is not None:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                if _now() + wait > deadline:
                    cooling = state.blocked_until > _now()
                    raise GDELTRateLimited(
                        f"{label}: next GDELT DOC slot is {wait:.0f}s away"
                        + (" (host cooling down after HTTP 429)" if cooling else "")
                        + f", past the {max_wait_s:.0f}s wait budget",
                        sent=sent, retry_at=slot,
                    )
                _sleep(min(wait, 5.0))
            try:
                resp = http.get(DOC_API_URL, params=dict(params),
                                headers={"User-Agent": USER_AGENT}, timeout=timeout)
            except requests.RequestException:
                state.last_end = _now()
                _write_state(fd, state, _PROCESS_STATE)
                raise
            sent = True
            state.last_end = _now()
            if resp.status_code == 429:
                state.strikes += 1
                cool = cooldown_after_429(state.strikes, resp.headers.get("Retry-After"), state.last_end)
                state.blocked_until = state.last_end + cool
                retry_at = state.blocked_until
                _write_state(fd, state, _PROCESS_STATE)
                log.info(
                    "gdelt_doc: HTTP 429 for {c} (attempt {a}/{n}); host cools down {s:.0f}s "
                    "(429 streak {k})", c=label, a=attempt, n=attempts, s=cool, k=state.strikes,
                )
                continue
            state.strikes = 0
            _write_state(fd, state, _PROCESS_STATE)
            return resp
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
    raise GDELTRateLimited(
        f"{label}: GDELT DOC API answered HTTP 429 to {max(1, attempts)} attempt(s)",
        sent=True, retry_at=retry_at,
    )


__all__ = [
    "BASE_COOLDOWN_S",
    "DEFAULT_MAX_WAIT_S",
    "DOC_API_URL",
    "LOCK_PATH_ENV",
    "LOW_PRIORITY_EXTRA_S",
    "MAX_COOLDOWN_S",
    "MIN_INTERVAL_S",
    "REQUEST_TIMEOUT",
    "GDELTRateLimited",
    "cooldown_after_429",
    "doc_get",
    "next_slot_at",
    "parse_retry_after",
]
