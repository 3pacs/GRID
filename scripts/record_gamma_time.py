"""One private strike-by-time frame per tracked ticker and regular-session minute."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
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


def capture(symbol, *, clock=_utc, read=read_board, write=write_frame):
    began = clock()
    if not session_open(began):
        return "closed"
    panel = read(symbol, now=began, time_map=True)
    observed = clock()
    if (
        not session_open(observed)
        or observed.astimezone(NY).date() != began.astimezone(NY).date()
    ):
        return "closed"
    day = observed.astimezone(NY).date()
    if (
        panel.get("symbol") != symbol
        or panel.get("status") != "available"
        or not panel.get("fresh")
        or panel.get("session_date") != day.isoformat()
        or len(panel.get("expiries", [])) != 1
    ):
        return "unqualified"
    expiry = date.fromisoformat(panel["expiries"][0])
    if expiry < day or (symbol in PINNED and expiry != day):
        return "ineligible_expiry"
    clocks = {k: datetime.fromisoformat(panel[k]) for k in ("as_of", "spot_as_of")}
    if any(
        v.tzinfo is None or not timedelta(0) <= observed - v <= timedelta(seconds=120)
        for v in clocks.values()
    ):
        return "stale"
    cells = panel["cells"]
    if not cells or any(c["expiry"] != expiry.isoformat() for c in cells):
        return "ineligible_expiry"
    levels = {c["strike"]: c["net_gex"] for c in cells}
    if len(levels) != len(cells):
        return "ambiguous_strikes"
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


def _safe_capture(symbol):
    try:
        return capture(symbol)
    except Exception as exc:
        # No provider error bodies, contracts or URLs in journal.
        return "unavailable:" + type(exc).__name__


def cycle():
    if not settings.SCHWAB_PRIVATE_GEX_BOARD_ENABLED:
        return "disabled"
    if not session_open(_utc()):
        return "closed"
    symbols = tracked_symbols()
    with ThreadPoolExecutor(max_workers=3) as pool:
        return ", ".join(
            f"{s}={status}"
            for s, status in zip(symbols, pool.map(_safe_capture, symbols))
        )


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
