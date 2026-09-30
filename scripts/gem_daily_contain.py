#!/usr/bin/env python3
"""Record terminal containment of one systemd GEM daily invocation."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

_ATTEMPTS = Path("/data/grid_v4/gem_daily/attempts")
_ALLOWED = {"success", "exit-code", "timeout", "signal", "watchdog", "resources", "protocol"}


def main() -> int:
    result = os.environ.get("SERVICE_RESULT", "unknown")
    if result not in _ALLOWED:
        result = "unknown"
    day = datetime.now(timezone.utc).date().isoformat()
    receipt = _ATTEMPTS / day
    if not receipt.is_file():
        print("GEM_CONTAIN no same-day attempt receipt", flush=True)
        return 1
    with receipt.open("a", encoding="ascii") as handle:
        handle.write(f"{datetime.now(timezone.utc).isoformat()} CONTAINED {result}\n")
    print(f"GEM_CONTAIN result={result}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
