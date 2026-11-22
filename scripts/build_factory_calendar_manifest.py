#!/usr/bin/env python3
"""Build pinned XNYS session manifests (``grid.xnys-session-manifest.v1``) from ``exchange_calendars``.

    usage: build_factory_calendar_manifest.py OUT_DIR PREFIX DEP_START DEP_END START:END [START:END ...]

The factory's calendar manifests under ``docs/reference/calendar-manifests/`` name their exporter in
``calendar_version``: exchange_calendars 4.11.2 with pandas 2.3.3. Neither is a GRID dependency; the manifests
are read with the standard library only (``ingestion/market_calendar.ManifestCalendar``). Run this in a
throwaway environment that has exactly those two versions; the reader refuses any other provenance.

It writes one canonical JSON file ``PREFIX-START-END.json`` per ``START:END`` span (a reader accepts at most 1,096 days and 1,000
sessions a file) and prints each file's sha256, which is then pinned in source. ``DEP_START`` / ``DEP_END``
are the calendar bounds written into ``calendar_version``; every span must lie inside them.

Check before trusting a new file: rebuild an existing span and compare its ``sessions`` with the reviewed
file. Done 2026-11-20 for 2022-11-01:2024-12-31 and 2025-01-01:2026-11-10: identical.

This script imports no GRID code and reads no market data.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def build(out_dir: Path, prefix: str, dep_start: str, dep_end: str,
          spans: list[str]) -> list[tuple[str, int, str]]:
    """Write one manifest per span; returns ``[(file name, sessions, sha256)]``."""
    import exchange_calendars as xc
    import numpy
    import pandas
    import tzdata

    calendar = xc.get_calendar("XNYS", start=dep_start, end=dep_end)
    version = (f"exchange_calendars:{xc.__version__}:pandas:{pandas.__version__}:numpy:{numpy.__version__}:"
               f"tzdata:{tzdata.__version__}:XNYS:{dep_start}:{dep_end}")
    done = []
    for span in spans:
        start, end = span.split(":")
        sessions = [
            {"closes": row.close.tz_convert("UTC").isoformat(), "key": f"XNYS:{day.date().isoformat()}:RTH",
             "opens": row.open.tz_convert("UTC").isoformat()}
            for day, row in calendar.schedule.loc[start:end].iterrows()
        ]
        doc = {"schema": "grid.xnys-session-manifest.v1", "exchange": "XNYS", "session_type": "RTH",
               "timezone": "America/New_York", "start": start, "end": end, "calendar_version": version,
               "sessions": sessions}
        raw = json.dumps(doc, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        name = f"{prefix}-{start}-{end}.json"
        (out_dir / name).write_bytes(raw)
        done.append((name, len(sessions), hashlib.sha256(raw).hexdigest()))
    return done


def main(argv: list[str]) -> int:
    if len(argv) < 6:
        sys.stderr.write(__doc__)
        return 2
    for name, count, sha in build(Path(argv[1]), argv[2], argv[3], argv[4], argv[5:]):
        sys.stdout.write(f"{name} sessions {count} sha256 {sha}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
