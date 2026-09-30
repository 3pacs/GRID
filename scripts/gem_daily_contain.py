#!/usr/bin/env python3
"""CONTAIN: record the terminal result of one GEM daily systemd invocation.

Runs as ``ExecStopPost=-`` after ``scripts/gem_daily_capture.py``, whatever
happened (success, gate skip, failure, timeout, kill). It appends one line to
the same-day claim receipt so every attempt ends with a terminal record, and
never fails the unit itself.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

_ATTEMPTS = Path("/data/grid_v4/gem_daily/attempts")
_ALLOWED = {"success", "exit-code", "timeout", "signal", "core-dump", "watchdog",
            "resources", "protocol", "start-limit-hit"}


def main() -> int:
    result = os.environ.get("SERVICE_RESULT", "unknown")
    if result not in _ALLOWED:
        result = "unknown"
    exit_status = os.environ.get("EXIT_STATUS", "")
    if not exit_status.isalnum():
        exit_status = "unknown"
    now = datetime.now(timezone.utc)
    receipt = _ATTEMPTS / now.date().isoformat()
    if not receipt.is_file():
        # The runner stopped before claiming the day (e.g. activation gate).
        print(f"GEM_CONTAIN result={result} status={exit_status} no-claim", flush=True)
        return 0
    with receipt.open("a", encoding="ascii") as handle:
        handle.write(f"{now.isoformat()} CONTAINED result={result} status={exit_status}\n")
    print(f"GEM_CONTAIN result={result} status={exit_status}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
