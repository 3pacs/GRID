#!/usr/bin/env python3
"""Build the pinned VS1 XNYS session manifest (``grid.xnys-session-manifest.v1``).

    usage: build_vs1_calendar_manifest.py [OUT_DIR]

Generates ``vs1-xnys-2007-11-01-2026-06-30.json`` (default directory:
``docs/reference/calendar-manifests``) using ``exchange_calendars`` 4.11.2,
``pandas`` 2.3.3, ``numpy`` 2.4.6, and ``tzdata`` 2026.6 so the build string
and serialization match the repository's existing pinned XNYS calendar parts.

This script imports no GRID code and reads no market data.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

PREFIX = "vs1-xnys"
START = "2007-11-01"
END = "2026-06-30"
REQUIRED_VERSIONS = {
    "exchange_calendars": "4.11.2",
    "pandas": "2.3.3",
    "numpy": "2.4.6",
    "tzdata": "2026.6",
}


def build(out_dir: Path) -> tuple[str, int, str]:
    """Write ``vs1-xnys-2007-11-01-2026-06-30.json``; return ``(filename, sessions, sha256)``."""
    import exchange_calendars as xc
    import numpy
    import pandas
    import tzdata

    actual = {
        "exchange_calendars": xc.__version__,
        "pandas": pandas.__version__,
        "numpy": numpy.__version__,
        "tzdata": tzdata.__version__,
    }
    if actual != REQUIRED_VERSIONS:
        raise RuntimeError(f"calendar build environment mismatch: got {actual}, required {REQUIRED_VERSIONS}")

    calendar = xc.get_calendar("XNYS", start=START, end=END)
    version = (
        f"exchange_calendars:{xc.__version__}:pandas:{pandas.__version__}:"
        f"numpy:{numpy.__version__}:tzdata:{tzdata.__version__}:XNYS:{START}:{END}"
    )
    sessions = [
        {
            "closes": row.close.tz_convert("UTC").isoformat(),
            "key": f"XNYS:{day.date().isoformat()}:RTH",
            "opens": row.open.tz_convert("UTC").isoformat(),
        }
        for day, row in calendar.schedule.loc[START:END].iterrows()
    ]
    doc = {
        "schema": "grid.xnys-session-manifest.v1",
        "exchange": "XNYS",
        "session_type": "RTH",
        "timezone": "America/New_York",
        "start": START,
        "end": END,
        "calendar_version": version,
        "sessions": sessions,
    }
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    name = f"{PREFIX}-{START}-{END}.json"
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / name
    target.write_bytes(raw)
    target.chmod(0o644)
    return name, len(sessions), hashlib.sha256(raw).hexdigest()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) > 1:
        sys.stderr.write(__doc__)
        return 2
    out_dir = Path(args[0]) if args else Path(__file__).resolve().parent.parent / "docs/reference/calendar-manifests"
    name, count, sha = build(out_dir)
    sys.stdout.write(f"{name} sessions {count} sha256 {sha}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
