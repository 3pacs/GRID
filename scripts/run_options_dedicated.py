#!/usr/bin/env python3
"""One schwab-mode full-universe options run, from its own timer (grid-options-dedicated.timer).

    python3 scripts/run_options_dedicated.py                  # what the unit runs
    python3 scripts/run_options_dedicated.py --budget-seconds 600

Why (2026-11-22, pre-open list item 7): a full schwab-mode run (about 80 Schwab
calls at 2 s plus about 960 Yahoo requests at >= 1 s, about 19 minutes) does not
fit the smart scheduler's options job (timeout_s 900 less stop_margin_s 60, so
840 s), and the Hermes step that encloses that job is pinned at 1,260 s
(tests/test_hermes_timeout_budgets.py). This runner gives the run its own wall
budget instead. With GRID_OPTIONS_DEDICATED_TIMER=1 the smart scheduler and the
daily scheduler's slots from 10:20 New York hand their full-universe run to it
(``ingestion.options.dedicated_timer_handoff``); the daily scheduler's morning
pull stays where it is, because GEM's trigger reads its completion line.

What it does: ``OptionsPuller.pull_all`` over the full default universe with
``capture_source="options_dedicated"``, so the tail cursor file and the Schwab
per-writer 45-minute gap are this writer's own. It passes no ``deadline_at``:
the Yahoo request budget is the module cap (``YAHOO_RUN_REQUEST_BUDGET``, 1,000)
and is not sized down to a scheduler window. Everything else in schwab mode
still applies inside ``pull_all``: the Yahoo pacing, the 429 latch and the
30-minute hold marker, the 6-expiry cap for non-core names, the Schwab ledger
holds, the cash-close cut-off and the cursor. The wall budget
(``DEFAULT_BUDGET_SECONDS``, 25 min) is the cooperative ``should_continue``
check: names it does not reach are DEFERRED and the cursor resumes them on the
next slot.

Refusals, all exit 0 with one ``OPTIONS_DEDICATED`` JSON line and no provider,
database or lock touched: not in schwab mode (GRID_OPTIONS_PRIMARY_PROVIDER is
not schwab, GRID_OPTIONS_SCHWAB_READERS is not admin-only, or the selector did
not choose schwab), or the timer not declared (GRID_OPTIONS_DEDICATED_TIMER is
not 1). A second run while one holds the lock file also exits 0.

Exit status: 0 for every run outcome except FAILED (no ticker succeeded), which
is 1 so the unit shows red; an unexpected error propagates (non-zero).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Measured shape of a full schwab-mode run (PR1331, 2026-11-21): ~80 Schwab calls x 2 s (~160 s)
# plus ~160 non-core names x 6 expiries at >= 1 s (~960 s), about 19 minutes with the database
# writes. 25 minutes leaves about 6 minutes for a slower Yahoo, the catalog publication (15 s) and
# start-up. A slower day DEFERs its last names; the cursor reads them first on the next slot.
DEFAULT_BUDGET_SECONDS = 25 * 60
MIN_BUDGET_SECONDS = 60
# Stays under the unit's TimeoutStartSec=35min with room for start-up and the final publication.
MAX_BUDGET_SECONDS = 30 * 60
LOCK_FILE_DEFAULT = "~/.local/state/grid/options_dedicated.lock"


def lock_file() -> Path:
    """GRID_OPTIONS_DEDICATED_LOCK_FILE, else beside the other schwab-mode state files."""
    return Path(os.getenv("GRID_OPTIONS_DEDICATED_LOCK_FILE", "").strip() or LOCK_FILE_DEFAULT).expanduser()


def refusal() -> str | None:
    """Why this runner must not run now, or None. Reads the environment only."""
    from ingestion import options

    if os.getenv("GRID_OPTIONS_PRIMARY_PROVIDER", "").strip().lower() != "schwab":
        return "not in schwab mode: GRID_OPTIONS_PRIMARY_PROVIDER is not schwab"
    if os.getenv("GRID_OPTIONS_SCHWAB_READERS", "").strip().lower() != "admin-only":
        return "not in schwab mode: GRID_OPTIONS_SCHWAB_READERS is not admin-only"
    if options.options_primary_provider() != "schwab":
        return "not in schwab mode: the provider selector did not choose schwab"
    if not options.dedicated_timer_declared():
        return "dedicated timer not declared: GRID_OPTIONS_DEDICATED_TIMER is not 1"
    return None


def _acquire(path: Path) -> int | None:
    """An exclusive non-blocking flock on ``path`` (created 0600), or None when another run holds it."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def _release(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _report(payload: dict[str, Any]) -> None:
    print("OPTIONS_DEDICATED " + json.dumps(payload, sort_keys=True, default=str), flush=True)


def _budget(raw: str) -> int:
    value = int(raw)
    if not MIN_BUDGET_SECONDS <= value <= MAX_BUDGET_SECONDS:
        raise argparse.ArgumentTypeError(f"must be between {MIN_BUDGET_SECONDS} and {MAX_BUDGET_SECONDS}")
    return value


def main(argv: list[str] | None = None, *, engine: Any = None,
         clock: Callable[[], float] = time.monotonic) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--budget-seconds", type=_budget, default=DEFAULT_BUDGET_SECONDS,
                    help=f"cooperative wall budget (default {DEFAULT_BUDGET_SECONDS}, at most {MAX_BUDGET_SECONDS})")
    args = ap.parse_args(argv)

    from config import log
    from ingestion import options

    why = refusal()
    if why is not None:
        log.info("options-dedicated: {why}; nothing to do", why=why)
        _report({"status": "REFUSED", "reason": why})
        return 0

    fd = _acquire(lock_file())
    if fd is None:
        log.warning("options-dedicated: another run holds {p}; nothing to do", p=lock_file())
        _report({"status": "REFUSED", "reason": "another dedicated run holds the lock"})
        return 0
    try:
        if engine is None:
            from db import get_engine

            engine = get_engine()
        end = clock() + args.budget_seconds
        log.info("options-dedicated: schwab-mode full universe, wall budget {s} s, writer {w}",
                 s=args.budget_seconds, w=options.DEDICATED_CAPTURE_SOURCE)
        # No deadline_at: the Yahoo request budget stays the module cap (1,000), not a window size.
        results = options.OptionsPuller(db_engine=engine).pull_all(
            should_continue=lambda: clock() < end,
            capture_source=options.DEDICATED_CAPTURE_SOURCE,
        )
        summary = results.summary
        _report({"budget_seconds": args.budget_seconds, "writer": options.DEDICATED_CAPTURE_SOURCE, **summary})
        return 1 if summary["status"] == "FAILED" else 0
    finally:
        _release(fd)


if __name__ == "__main__":
    sys.exit(main())
