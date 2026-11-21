"""One private strike-by-time frame per tracked ticker and regular-session minute."""

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, datetime, time, timedelta, timezone
import logging
import os
from pathlib import Path
from time import sleep

from api.gamma_board import session_open
from api.gamma_board_schwab import NY, read_board
from api.gamma_time_store import write_frame
from api.gamma_time_watchlist import PINNED, tracked_symbols
from config import settings

log = logging.getLogger("grid-gamma-time")


def _utc():
    return datetime.now(timezone.utc)


# Only these reader outcomes can be caused by an opening book that has not
# populated yet. Identity, authentication, quota, and storage failures stay final.
_OPENING_DATA_ERRORS = {
    "Underlying price unavailable",
    "No eligible expiry in this session",
    "No dated option quotes in scope",
    "No usable gamma exposure across the chain",
    "Insufficient matched contract open interest",
}


def capture(
    symbol,
    *,
    clock=_utc,
    read=read_board,
    write=write_frame,
    opening_minute=None,
    deadline=None,
):
    began = clock()
    if not session_open(began):
        return "closed"
    if deadline is not None and not opening_minute <= began < deadline:
        return "opening_window_elapsed"
    try:
        panel = read(symbol, now=began, time_map=True)
    except ValueError as exc:
        if opening_minute is not None and str(exc) in _OPENING_DATA_ERRORS:
            return "opening_data_pending"
        raise
    observed = clock()
    if deadline is not None and not began <= observed < deadline:
        return "opening_window_elapsed"
    if (
        not session_open(observed)
        or observed.astimezone(NY).date() != began.astimezone(NY).date()
    ):
        return "closed"
    day = observed.astimezone(NY).date()
    if (
        panel.get("symbol") != symbol
        or panel.get("status") != "available"
        or panel.get("session_date") != day.isoformat()
        or len(panel.get("expiries", [])) != 1
    ):
        return "unqualified"
    expiry = date.fromisoformat(panel["expiries"][0])
    if expiry < day or (symbol in PINNED and expiry != day):
        return "ineligible_expiry"
    if not panel.get("fresh"):
        return "opening_data_pending" if opening_minute is not None else "unqualified"
    clock_fields = ("as_of", "spot_as_of", "earliest_included_quote_at")
    if any(not panel.get(k) for k in clock_fields):
        return "stale"
    clocks = {k: datetime.fromisoformat(panel[k]) for k in clock_fields}
    session_start = datetime.combine(day, time(9, 30), NY)
    if any(
        v.utcoffset() is None
        or v < session_start
        or not timedelta(0) <= observed - v <= timedelta(seconds=120)
        for v in clocks.values()
    ):
        return "stale"
    cells = panel["cells"]
    if not cells or any(c["expiry"] != expiry.isoformat() for c in cells):
        return "ineligible_expiry"
    levels = {c["strike"]: c["net_gex"] for c in cells}
    if len(levels) != len(cells):
        return "ambiguous_strikes"
    if deadline is not None and not observed <= clock() < deadline:
        return "opening_window_elapsed"
    inserted = write(
        symbol,
        session_date=day,
        observed_at=observed,
        expiry=expiry,
        spot=panel["spot"],
        levels=levels,
        source=panel["source_label"],
        chain_as_of=clocks["as_of"],
        spot_as_of=clocks["spot_as_of"],
        oi_retrieved_at=datetime.fromisoformat(panel["oi_retrieved_at"])
        if panel.get("oi_retrieved_at")
        else None,
    )
    return "recorded" if inserted else "already_recorded"


def _safe_capture(symbol, opening_minute=None, deadline=None):
    try:
        return capture(symbol, opening_minute=opening_minute, deadline=deadline)
    except Exception as exc:
        # No provider error bodies, contracts or URLs in journal.
        return "unavailable:" + type(exc).__name__


def _retry_opening(results, minute, *, clock=_utc, pause=sleep, run=_safe_capture):
    pending = [s for s in PINNED if results.get(s) in {"opening_data_pending", "stale"}]
    if not pending:
        return
    now = clock()
    # Give the feed at least four seconds after the first pass and retain
    # headroom for the existing bounded chain/OI calls and the next minute.
    retry_at = max(minute + timedelta(seconds=12), now + timedelta(seconds=4))
    latest_start = minute + timedelta(seconds=20)
    cutoff = minute + timedelta(seconds=55)
    if not minute <= now or retry_at > latest_start:
        return
    pause((retry_at - now).total_seconds())
    if not retry_at <= clock() <= latest_start:
        return
    pool = ThreadPoolExecutor(max_workers=len(pending))
    try:
        futures = {pool.submit(run, s, minute, cutoff): s for s in pending}
        # A slow provider cannot hold the scheduler across 09:31. Workers
        # enforce the same cutoff again after reading and before persistence.
        done, outstanding = wait(
            futures, timeout=max(0, (cutoff - clock()).total_seconds())
        )
        for future in done:
            symbol = futures[future]
            results[symbol] = (
                f"{future.result()} (opening retry; first={results[symbol]})"
            )
        for future in outstanding:
            results[futures[future]] = "opening_retry_timed_out"
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def cycle():
    if not settings.SCHWAB_PRIVATE_GEX_BOARD_ENABLED:
        return "disabled"
    now = _utc()
    if not session_open(now):
        return "closed"
    local = now.astimezone(NY)
    opening = (
        now.replace(second=0, microsecond=0)
        if (local.hour, local.minute) == (9, 30)
        else None
    )
    symbols = tracked_symbols()

    def first(symbol):
        if opening is not None and symbol in PINNED:
            return _safe_capture(symbol, opening, opening + timedelta(minutes=1))
        return _safe_capture(symbol)

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = dict(zip(symbols, pool.map(first, symbols)))
    if opening is not None:
        _retry_opening(results, opening)
    return ", ".join(f"{s}={status}" for s, status in results.items())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="One capture if regular trading is open; otherwise no source reads",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    log.info("Gamma time recorder started")
    while True:
        release = os.environ.get("GRID_GAMMA_RELEASE_PATH")
        if release and Path(release).resolve() != Path.cwd():
            return  # systemd restarts in the new deployed release
        status = cycle()
        if args.once or status not in ("disabled", "closed"):
            log.info("Gamma time: %s", status)
        if args.once:
            return
        sleep(60 - (_utc().timestamp() % 60) + 0.15)


if __name__ == "__main__":
    main()
