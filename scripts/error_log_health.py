"""Quick triage report over ``.server-logs/errors.jsonl``.

Run after a Hermes cycle (or before a release) to confirm the log
isn't being flooded again. Exits non-zero when any single
(module, function, message) signature crosses ``--threshold``
occurrences in the inspected window.

Examples
--------
$ python -m scripts.error_log_health                  # last 24h, threshold 50
$ python -m scripts.error_log_health --hours 6        # last 6h
$ python -m scripts.error_log_health --threshold 100  # tolerate more
$ python -m scripts.error_log_health --top 30         # show 30 rows
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

_DEFAULT_LOG_PATH = (
    Path(__file__).resolve().parent.parent / ".server-logs" / "errors.jsonl"
)


def _parse_ts(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None


def _load_entries(path: Path) -> list[dict]:
    entries: list[dict] = []
    if not path.exists():
        return entries
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _within_window(entries: list[dict], hours: float) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out: list[dict] = []
    for entry in entries:
        ts = _parse_ts(entry.get("ts", ""))
        if ts is None or ts >= cutoff:
            out.append(entry)
    return out


def _signature(entry: dict) -> tuple[str, str, str]:
    return (
        entry.get("module", ""),
        entry.get("function", ""),
        (entry.get("message", "") or "")[:120],
    )


def report(
    log_path: Path,
    hours: float,
    threshold: int,
    top: int,
) -> int:
    """Render the report and return the suggested exit status.

    Returns 0 when no signature breaches the threshold, 1 otherwise.
    """
    entries = _load_entries(log_path)
    if not entries:
        print(f"[ok] {log_path} is empty or missing — nothing to triage")
        return 0

    window = _within_window(entries, hours)
    print(f"Log: {log_path}")
    print(
        f"Total entries: {len(entries):,} | last {hours:g}h: {len(window):,}"
    )

    if not window:
        print("[ok] no errors inside the inspected window")
        return 0

    counts = Counter(_signature(e) for e in window)
    by_module = Counter(e.get("module", "") for e in window)

    print("\nTop modules in window:")
    for mod, c in by_module.most_common(min(10, top)):
        print(f"  {c:6}  {mod}")

    print("\nTop signatures in window:")
    over_threshold: list[tuple[tuple[str, str, str], int]] = []
    for sig, c in counts.most_common(top):
        marker = "!! " if c >= threshold else "   "
        if c >= threshold:
            over_threshold.append((sig, c))
        print(f"  {marker}{c:6}  {sig[0]}::{sig[1]} :: {sig[2]}")

    if over_threshold:
        print(
            f"\n[fail] {len(over_threshold)} signature(s) exceed threshold "
            f"({threshold}) in last {hours:g}h:"
        )
        for sig, c in over_threshold:
            print(f"  {c:6}  {sig[0]}::{sig[1]}")
        return 1

    print(
        f"\n[ok] no signature exceeds threshold ({threshold}) in last {hours:g}h"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log",
        type=Path,
        default=_DEFAULT_LOG_PATH,
        help="Path to errors.jsonl (default: .server-logs/errors.jsonl)",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Window in hours to inspect (default: 24)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=50,
        help="Signature count above which the report fails (default: 50)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Top N rows to print (default: 20)",
    )
    args = parser.parse_args(argv)
    return report(args.log, args.hours, args.threshold, args.top)


if __name__ == "__main__":
    sys.exit(main())
