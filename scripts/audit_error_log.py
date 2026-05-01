#!/usr/bin/env python3
"""audit_error_log.py — surface fresh, recurring, and noisy patterns in
``.server-logs/errors.jsonl``.

Goal: a single command that tells an operator (or another agent) what's
actually broken right now, without scrolling through 4,500 lines of JSONL.
The output is small enough to paste into a Slack channel or a daily digest.

Usage::

    python -m scripts.audit_error_log                     # last 24h, default
    python -m scripts.audit_error_log --hours 168         # last week
    python -m scripts.audit_error_log --since 2026-04-25  # since a date
    python -m scripts.audit_error_log --top 20            # top 20 patterns
    python -m scripts.audit_error_log --new-only          # only new patterns

Designed to be cheap to run from a systemd timer or `cron @hourly` so the
next-most-frequent error pattern surfaces before it's a fire.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_LOG = _REPO_ROOT / ".server-logs" / "errors.jsonl"

# Strip volatile noise from messages so similar-but-not-identical errors
# bucket together (timestamps, request IDs, hex addresses, FRED series IDs
# in known templates, ticker symbols, etc.).
_NORMALIZERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"), "<TS>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}\b"), "<DATE>"),
    (re.compile(r"\bapiKey=[A-Za-z0-9]+"), "apiKey=<REDACTED>"),
    (re.compile(r"\b(0x)?[0-9a-fA-F]{32,}\b"), "<HEX>"),
    (re.compile(r"\b\d+\.\d+\.\d+\.\d+\b"), "<IP>"),
    (re.compile(r"\b[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}\b", re.I), "<UUID>"),
    (re.compile(r"FRED pull failed for [A-Z0-9_]+:"), "FRED pull failed for <SID>:"),
    (re.compile(r"failed for [A-Z0-9_]{1,8}:"), "failed for <SYMBOL>:"),
    (re.compile(r"\b\d{4,}\b"), "<N>"),
)


def _normalize(msg: str) -> str:
    s = msg or ""
    for pat, repl in _NORMALIZERS:
        s = pat.sub(repl, s)
    return s[:240]


def _iter_entries(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _parse_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def audit(
    log_path: Path,
    cutoff: datetime,
    top: int,
    new_only: bool = False,
    baseline_hours: int = 168,
) -> int:
    if not log_path.exists():
        print(f"no error log at {log_path}")
        return 0

    recent: Counter[tuple[str, str, str]] = Counter()
    baseline: Counter[tuple[str, str, str]] = Counter()
    recent_first_seen: dict[tuple[str, str, str], datetime] = {}
    recent_last_seen: dict[tuple[str, str, str], datetime] = {}
    levels: Counter[str] = Counter()

    baseline_cutoff = cutoff - timedelta(hours=baseline_hours)

    total_recent = 0
    for entry in _iter_entries(log_path):
        ts = _parse_ts(entry.get("ts", ""))
        if ts is None:
            continue
        module = entry.get("module") or ""
        function = entry.get("function") or ""
        message = _normalize(entry.get("message") or "")
        key = (module, function, message)

        if ts >= cutoff:
            recent[key] += 1
            recent_first_seen.setdefault(key, ts)
            recent_last_seen[key] = ts
            levels[entry.get("level") or "UNKNOWN"] += 1
            total_recent += 1
        elif ts >= baseline_cutoff:
            baseline[key] += 1

    if total_recent == 0:
        print(f"no entries since {cutoff.isoformat()}  (clean)")
        return 0

    print(
        f"errors.jsonl audit | window: since {cutoff.isoformat()} "
        f"({total_recent} entries, levels={dict(levels)})"
    )
    print()

    items = recent.most_common()
    if new_only:
        items = [it for it in items if it[0] not in baseline]
        if not items:
            print("(no NEW patterns vs prior baseline window — nothing to flag)")
            return 0
        print(f"NEW patterns (absent in prior {baseline_hours}h):")
    else:
        print(f"top {top} patterns by frequency:")

    for (module, function, message), count in items[:top]:
        first = recent_first_seen[(module, function, message)].isoformat()
        last = recent_last_seen[(module, function, message)].isoformat()
        baseline_count = baseline.get((module, function, message), 0)
        marker = "NEW" if baseline_count == 0 else f"prior={baseline_count}"
        print(
            f"  [{count:>4}] {marker:<12} {module}::{function}\n"
            f"           first={first}  last={last}\n"
            f"           {message}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log", type=Path, default=_DEFAULT_LOG,
        help=f"Path to errors.jsonl (default: {_DEFAULT_LOG})",
    )
    parser.add_argument(
        "--hours", type=float, default=24.0,
        help="Look back this many hours (default: 24)",
    )
    parser.add_argument(
        "--since", type=str, default=None,
        help="Look back since this ISO timestamp (overrides --hours)",
    )
    parser.add_argument(
        "--top", type=int, default=15,
        help="Show top N patterns (default: 15)",
    )
    parser.add_argument(
        "--new-only", action="store_true",
        help="Show only patterns that didn't appear in the prior baseline window",
    )
    parser.add_argument(
        "--baseline-hours", type=float, default=168.0,
        help="Baseline window for --new-only comparison (default: 168 = 1 week)",
    )
    args = parser.parse_args(argv)

    if args.since:
        cutoff = _parse_ts(args.since)
        if cutoff is None:
            try:
                cutoff = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
            except ValueError:
                print(f"invalid --since: {args.since}", file=sys.stderr)
                return 2
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    return audit(
        log_path=args.log,
        cutoff=cutoff,
        top=args.top,
        new_only=args.new_only,
        baseline_hours=int(args.baseline_hours),
    )


if __name__ == "__main__":
    raise SystemExit(main())
