"""Record prospective SPY red-band flags from the private minute recorder and completed bars."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
from time import sleep

from api import gamma_band_store as store
from api.gamma_board_intraday import NY, _bars, _history, _session
from api.gamma_time_store import DEFAULT_FILE, MAX_LEVELS, MAX_PAYLOAD
from config import settings

LOG = logging.getLogger("grid-spy-bands")


def utcnow():
    return datetime.now(timezone.utc)


def previous_levels(day, minute, path=None):
    """Use only the immutable same-expiry frame received before this bar began."""
    dbpath = Path(path or os.getenv("GRID_GAMMA_TIME_DB") or DEFAULT_FILE)
    if not dbpath.exists():
        return None
    with closing(
        sqlite3.connect(dbpath.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    ) as db:
        row = db.execute(
            """SELECT observed_at,levels FROM frames WHERE symbol='SPY'
            AND session_date=? AND expiry=session_date AND minute=? AND length(levels)<=?""",
            (day.isoformat(), minute - 1, MAX_PAYLOAD),
        ).fetchone()
    if row is None:
        return None
    observed = datetime.fromisoformat(row[0])
    if observed.utcoffset() is None or int(observed.timestamp() // 60) != minute - 1:
        return None
    pairs = json.loads(row[1])
    levels = dict(pairs)
    if not 0 < len(levels) == len(pairs) <= MAX_LEVELS or any(
        type(x) not in (int, float) or not math.isfinite(x)
        for pair in pairs
        for x in pair
    ):
        raise ValueError("Invalid saved SPY ladder")
    return levels


def cycle(
    last_done=None, *, clock=utcnow, fetch=_history, frames_path=None, journal=None
):
    if not settings.SCHWAB_PRIVATE_GEX_BOARD_ENABLED:
        return last_done
    now = clock()
    start, end, cutoff = _session(now)
    day = start.astimezone(NY).date()
    if day < store.FIRST_DAY or not start + timedelta(
        minutes=1
    ) <= now < end + timedelta(minutes=6):
        store.settle_expired(clock, journal)
        return last_done
    latest = int(cutoff.timestamp() // 60) - 1
    # Read again if the provider has not published the last completed candle.
    if latest == last_done and now < end:
        return last_done
    after = max(start, cutoff - timedelta(minutes=40))
    body = fetch("SPY", after, cutoff)
    bars = {
        int(datetime.fromisoformat(b["time"]).timestamp() // 60): b
        for b in _bars("SPY", body, after, cutoff)
    }
    levels = (
        previous_levels(day, latest, frames_path)
        if now < end and latest in bars
        else None
    )
    store.advance(
        day=day,
        bars=bars,
        latest=latest if now < end else None,
        levels=levels,
        session_end=int(end.timestamp() // 60),
        clock=clock,
        path=journal,
    )
    return latest if latest in bars else last_done


def recover_one(*, clock=utcnow, fetch=_history, journal=None):
    """One historical request for an issued, expired flag with incomplete coverage."""
    if not settings.SCHWAB_PRIVATE_GEX_BOARD_ENABLED:
        return
    job = store.claim_recovery(clock(), journal)
    if job is None:
        return
    _, _, first, deadline = job
    start, end = (
        datetime.fromtimestamp(m * 60, timezone.utc) for m in (first, deadline)
    )
    try:
        body = fetch("SPY", start, end)
        bars = {
            int(datetime.fromisoformat(b["time"]).timestamp() // 60): b
            for b in _bars("SPY", body, start, end)
        }
    except Exception as exc:
        LOG.warning("SPY grade recovery source unavailable (%s)", type(exc).__name__)
        bars = None
    store.finish_recovery(job, bars, clock(), journal)


def _recover_safely():
    try:
        recover_one()
    except Exception as exc:
        LOG.warning("SPY grade recovery unavailable (%s)", type(exc).__name__)


def main():
    logging.basicConfig(level=logging.INFO)
    store.initialize()
    LOG.info("SPY band journal ready; first prospective session %s", store.FIRST_DAY)
    last_done = None
    recovery = None
    with ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="spy-grade-recovery"
    ) as pool:
        while True:
            release = os.environ.get("GRID_GAMMA_RELEASE_PATH")
            if release and Path(release).resolve() != Path.cwd():
                return
            try:
                last_done = cycle(last_done)
            except Exception as exc:
                LOG.warning("SPY band observation unavailable (%s)", type(exc).__name__)
                # A provider outage must not leave old calls pending indefinitely.
                try:
                    store.settle_expired(utcnow)
                except sqlite3.Error:
                    LOG.warning("SPY band journal unavailable")
            if recovery is None or recovery.done():
                recovery = pool.submit(_recover_safely)
            # Poll at :07, :27 and :47. Successful minutes skip repeated feed reads.
            sleep(20 - ((utcnow().timestamp() - 7) % 20))


if __name__ == "__main__":
    main()
