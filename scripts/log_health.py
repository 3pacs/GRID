"""Server-log health canary.

Reads ``.server-logs/errors.jsonl`` and prints a human-readable summary
of recent error patterns plus a top-offenders breakdown. Designed to be
run from cron / CI / a quick console session::

    python -m scripts.log_health                    # last 24h, console summary
    python -m scripts.log_health --window 7d        # last 7 days
    python -m scripts.log_health --json             # machine-readable
    python -m scripts.log_health --fail-over 50     # exit 1 if >50 errors in window

The canary intentionally categorises transient upstream failures (HTTP
500/502/503/504, RetryError, connection-aborted) separately from genuine
defects, so the headline number tracks bugs we can actually fix.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
ERRORS_PATH = REPO_ROOT / ".server-logs" / "errors.jsonl"

# Buckets we use to group errors.  The last matching family wins, so order
# more-specific patterns earlier.
_FAMILIES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("rate_limit_429", re.compile(r"\b429\b|Too Many Requests")),
    ("forbidden_403", re.compile(r"\b403\b|Forbidden")),
    ("not_found_404", re.compile(r"\b404\b|Not Found")),
    ("bad_request_400", re.compile(r"\b400\b|Bad Request")),
    ("server_5xx", re.compile(r"\b5\d\d\b|Bad Gateway|Service Unavailable|Gateway Timeout|Internal Server Error")),
    ("retry_exhausted", re.compile(r"RetryError|failed after \d+ attempts")),
    ("timeout", re.compile(r"timed out|TimeoutError|TIMED OUT", re.IGNORECASE)),
    ("connection", re.compile(r"Connection (aborted|reset|refused)|RemoteDisconnected|address associated|Errno -[25]")),
    ("missing_module", re.compile(r"No module named|cannot import name")),
    ("git_sink", re.compile(r"\[server_log\] git ")),
    ("key_error", re.compile(r"KeyError|: '[A-Za-z_]\w*'$|^'\w+'$")),
)

# Anything in this set is treated as a third-party hiccup, not a bug.
_TRANSIENT_FAMILIES = {
    "rate_limit_429", "forbidden_403", "not_found_404", "bad_request_400",
    "server_5xx", "retry_exhausted", "timeout", "connection",
}


def _classify(message: str) -> str:
    for name, pat in _FAMILIES:
        if pat.search(message):
            return name
    return "other"


def _parse_window(spec: str) -> timedelta:
    """Accept '24h', '7d', '30m', '90m', etc."""
    m = re.fullmatch(r"(\d+)([smhd])", spec)
    if not m:
        raise argparse.ArgumentTypeError(
            f"window must look like 24h/7d/30m, got {spec!r}",
        )
    n = int(m.group(1))
    unit = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}[m.group(2)]
    return timedelta(**{unit: n})


def _load_errors(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _filter_window(errs: Iterable[dict[str, Any]], window: timedelta) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - window
    kept: list[dict[str, Any]] = []
    for e in errs:
        ts = e.get("ts")
        if not ts:
            continue
        try:
            when = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if when >= cutoff:
            kept.append(e)
    return kept


def _summarise(errs: list[dict[str, Any]], top_n: int = 10) -> dict[str, Any]:
    by_family: Counter[str] = Counter()
    by_module: Counter[str] = Counter()
    by_message: Counter[str] = Counter()
    transient = 0
    real = 0

    for e in errs:
        msg = str(e.get("message", ""))
        family = _classify(msg)
        by_family[family] += 1
        if family in _TRANSIENT_FAMILIES:
            transient += 1
        else:
            real += 1
        mod_fn = f"{e.get('module', '')}.{e.get('function', '')}"
        by_module[mod_fn] += 1
        # Truncate message for grouping; keep a stable prefix
        by_message[msg[:140]] += 1

    return {
        "total": len(errs),
        "real_errors": real,
        "transient_errors": transient,
        "families": dict(by_family.most_common()),
        "top_modules": dict(by_module.most_common(top_n)),
        "top_messages": dict(by_message.most_common(top_n)),
    }


def _format_report(summary: dict[str, Any], window: str) -> str:
    lines = [
        f"GRID server-log health — last {window}",
        "=" * 60,
        f"Total errors:     {summary['total']:>6}",
        f"  Real defects:   {summary['real_errors']:>6}  (the ones to actually fix)",
        f"  Transient/3p:   {summary['transient_errors']:>6}  (upstream API hiccups)",
        "",
        "By family:",
    ]
    for fam, n in summary["families"].items():
        marker = " " if fam in _TRANSIENT_FAMILIES else "*"
        lines.append(f"  {marker} {fam:<20} {n:>6}")
    lines.append("")
    lines.append("Top modules:")
    for mod, n in summary["top_modules"].items():
        lines.append(f"    {n:>6}  {mod}")
    lines.append("")
    lines.append("Top messages:")
    for msg, n in summary["top_messages"].items():
        lines.append(f"    {n:>6}  {msg}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--window", type=_parse_window, default=_parse_window("24h"),
        help="Time window to inspect (e.g. 24h, 7d, 30m). Default: 24h.",
    )
    parser.add_argument(
        "--path", type=Path, default=ERRORS_PATH,
        help=f"Errors JSONL path (default: {ERRORS_PATH}).",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Emit machine-readable JSON instead of a human report.",
    )
    parser.add_argument(
        "--fail-over", type=int, default=None, metavar="N",
        help="Exit non-zero if real-defect count exceeds N within the window.",
    )
    parser.add_argument(
        "--top", type=int, default=10,
        help="Number of top modules/messages to show (default: 10).",
    )
    args = parser.parse_args(argv)

    window_str = re.sub(r",? 0:00:00$", "", str(args.window))
    errs_all = _load_errors(args.path)
    errs = _filter_window(errs_all, args.window)
    summary = _summarise(errs, top_n=args.top)
    summary["window"] = window_str
    summary["source"] = str(args.path)

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(_format_report(summary, window_str))

    if args.fail_over is not None and summary["real_errors"] > args.fail_over:
        print(
            f"\nFAIL: {summary['real_errors']} real errors exceeds threshold "
            f"{args.fail_over} within {window_str}.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
