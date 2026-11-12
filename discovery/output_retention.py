"""Retention for the chart/CSV files the discovery audits write on every run.

``discovery/orthogonality.py`` and ``discovery/clustering.py`` write
timestamped files (``<name>_YYYYMMDD_HHMMSS.<ext>``) into ``outputs/`` on every
run and nothing ever removed them: on 2026-11-12 grid-svr held 848 files /
5.2 GB under ``outputs/orthogonality`` (37 MB correlation heatmaps going back
to 2026-03-21) and 2,992 files / 165 MB under ``outputs/clustering``. No reader
uses anything but the newest run.

``prune_outputs`` deletes only regular files whose name carries that writer
timestamp and whose timestamp is older than ``keep_days``. It never follows a
symlink out of the directory, never touches a file without the timestamp
pattern, and never raises: a failed prune is logged and the audit carries on.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger as log

DEFAULT_KEEP_DAYS = 14

_STAMPED = re.compile(r"_(\d{8}_\d{6})\.(png|csv|json|txt)$")


def prune_outputs(
    directory: str | Path,
    keep_days: int = DEFAULT_KEEP_DAYS,
    now: datetime | None = None,
) -> int:
    """Delete writer-stamped files older than ``keep_days``; return the count."""
    if keep_days < 1:
        raise ValueError("keep_days must be >= 1")
    root = Path(directory)
    cutoff = (now or datetime.now()) - timedelta(days=keep_days)
    removed = 0
    try:
        entries = list(root.iterdir())
    except OSError as exc:
        log.warning("output retention: cannot list {d}: {e}", d=root, e=str(exc))
        return 0
    for entry in entries:
        m = _STAMPED.search(entry.name)
        if m is None:
            continue
        try:
            stamp = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        if stamp >= cutoff:
            continue
        try:
            if entry.is_symlink() or not entry.is_file():
                continue
            entry.unlink()
            removed += 1
        except OSError as exc:
            log.warning("output retention: cannot remove {f}: {e}", f=entry, e=str(exc))
    if removed:
        log.info(
            "output retention: removed {n} file(s) older than {k}d from {d}",
            n=removed, k=keep_days, d=root,
        )
    return removed
