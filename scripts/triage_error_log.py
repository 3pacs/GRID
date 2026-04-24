"""Triage the structured error log at `.server-logs/errors.jsonl`.

Groups errors by module+function+normalised-message and prints the top
offenders so operators can quickly see what is actually broken vs. what is
a handled-degradation warning. Safe to run from any shell — no DB or
network dependency.

Usage:
    python3 -m scripts.triage_error_log              # all time, top 25
    python3 -m scripts.triage_error_log --since 2026-04
    python3 -m scripts.triage_error_log --top 50
    python3 -m scripts.triage_error_log --module ingestion.fred

The intent is that this script is part of the "make it so it's never fucked
again" discipline: if the top N list grows week-over-week, something
regressed. If it shrinks, we're winning. Run it as part of a daily
heartbeat to get a trend line.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Iterator

_DEFAULT_LOG_PATH: Path = (
    Path(__file__).resolve().parent.parent / ".server-logs" / "errors.jsonl"
)


def _iter_records(path: Path) -> Iterator[dict]:
    """Yield one parsed JSON record per line, skipping malformed lines."""
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _normalise_message(msg: str, width: int = 140) -> str:
    """Trim messages so run-to-run variance (timestamps, URLs) doesn't
    fragment the bucket count.
    """
    return msg[:width]


def triage(
    path: Path,
    since: str | None = None,
    module_filter: str | None = None,
    top: int = 25,
) -> int:
    """Print a triage report. Returns 0 on success, 2 if the log is missing."""
    if not path.exists():
        print(f"error log not found: {path}", file=sys.stderr)
        return 2

    module_counter: Counter[str] = Counter()
    bucket_counter: Counter[tuple[str, str, str]] = Counter()
    total = 0
    first_ts: str | None = None
    last_ts: str | None = None

    for rec in _iter_records(path):
        ts = rec.get("ts", "")
        if since and not ts.startswith(since):
            continue
        module = rec.get("module", "")
        if module_filter and module != module_filter:
            continue
        fn = rec.get("function", "")
        msg = _normalise_message(rec.get("message", ""))
        module_counter[module] += 1
        bucket_counter[(module, fn, msg)] += 1
        total += 1
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts

    if total == 0:
        scope = f"since={since}" if since else "all time"
        print(f"no errors found ({scope}{', module=' + module_filter if module_filter else ''})")
        return 0

    print(f"=== Error triage: {total} records ({first_ts}  ->  {last_ts}) ===")
    if since:
        print(f"since filter: {since}")
    if module_filter:
        print(f"module filter: {module_filter}")
    print()
    print(f"-- Top modules --")
    for module, count in module_counter.most_common(15):
        print(f"  {count:6d}  {module}")
    print()
    print(f"-- Top {top} unique (module.function -> message) buckets --")
    for (module, fn, msg), count in bucket_counter.most_common(top):
        print(f"  {count:5d}  {module}.{fn}  ->  {msg}")
    return 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log",
        type=Path,
        default=_DEFAULT_LOG_PATH,
        help=f"path to errors.jsonl (default: {_DEFAULT_LOG_PATH})",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO date/month prefix; e.g. 2026-04 or 2026-04-18",
    )
    parser.add_argument(
        "--module",
        type=str,
        default=None,
        help="filter to a single module name",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=25,
        help="number of unique buckets to show (default: 25)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return triage(args.log, since=args.since, module_filter=args.module, top=args.top)


if __name__ == "__main__":
    raise SystemExit(main())
