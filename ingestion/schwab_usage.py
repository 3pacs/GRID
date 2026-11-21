"""Shared Schwab call ledger: one line per HTTP request, from every client that shares the one Schwab app.

Why (2026-11-20): Schwab answered gex-watch's SPY quote call with HTTP 429 three times (15:37:07Z, 16:30:13Z,
17:00:11Z) and the spot record lost four minutes. Several processes share the app's per-minute allowance (gex-watch's
quote leg and chain fallback, scripts.record_gamma_time, scripts.gex_native_volume_snapshot, the grid-api gamma pages)
and none of them recorded its calls, so who burst could not be read. Every client now appends a line here.

Format (schwab-usage/1), shared byte-for-byte with gex-watch's writer (3pacs/gex src/gex-watch/schwab_usage.py):

  <dir>/calls-YYYYMMDD.tsv   (UTC day; dir 0700, files 0600, owner only)
  epoch_ms <TAB> client <TAB> pid <TAB> METHOD path <TAB> status

`dir` is `usage/` beside the token file (~/.config/grid/schwab/usage). `client` is the caller's name ([A-Za-z0-9._:/-],
max 60): the `client=` given to the client, else $GRID_SCHWAB_CLIENT, else the program's stem
(`python -m scripts.record_gamma_time` -> record_gamma_time). `path` is the market-data path or `oauth/token`, never a
query string. `status` is the HTTP status, or `net` for a transport error. No token, key, query, symbol list or body is
ever written. Each line is one O_APPEND write, so concurrent writers do not interleave. Files older than KEEP_DAYS are
removed when a writer opens a new day. Writing never raises into the caller.

Reader:  python -m ingestion.schwab_usage --around 2026-11-20T15:37:07Z [--minutes 3] [--dir DIR]
         prints, per minute, the calls by client (and the 429s), the minutes before and after the given time.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMA = "schwab-usage/1"
KEEP_DAYS = 14
_NAME = re.compile(r"[^A-Za-z0-9._:/-]+")
_DAY = re.compile(r"^calls-(\d{8})\.tsv$")
_STATE: dict[str, str] = {}   # dir -> the UTC day this process last wrote (prune once per new day)


def client_name(explicit: str | None = None) -> str:
    raw = explicit or os.environ.get("GRID_SCHWAB_CLIENT") or Path(sys.argv[0] if sys.argv and sys.argv[0] else "python").stem or "python"
    return (_NAME.sub("_", raw).strip("_") or "python")[:60]


def usage_dir(token_file: str | os.PathLike[str]) -> Path:
    return Path(token_file).expanduser().parent / "usage"


def _prune(d: Path, today: str) -> None:
    cutoff = (datetime.strptime(today, "%Y%m%d") - timedelta(days=KEEP_DAYS)).strftime("%Y%m%d")
    for p in d.iterdir():
        m = _DAY.match(p.name)
        if m and m.group(1) < cutoff:
            try:
                p.unlink()
            except OSError:
                pass


def record(d: str | os.PathLike[str], client: str, method: str, path: str, status: int | str,
           now: float | None = None) -> bool:
    """Append one call line. True when written; never raises."""
    try:
        t = time.time() if now is None else float(now)
        d = Path(d)
        day = datetime.fromtimestamp(t, timezone.utc).strftime("%Y%m%d")
        if _STATE.get(str(d)) != day:
            d.mkdir(mode=0o700, parents=True, exist_ok=True)
            if d.stat().st_mode & 0o077:
                os.chmod(d, 0o700)
            _prune(d, day)
            _STATE[str(d)] = day
        line = "%d\t%s\t%d\t%s %s\t%s\n" % (int(t * 1000), client_name(client), os.getpid(), method.upper()[:8],
                                             _NAME.sub("_", path.split("?")[0])[:80], status)
        fd = os.open(d / f"calls-{day}.tsv", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, line.encode())
        finally:
            os.close(fd)
        return True
    except Exception:   # the ledger must never break a market-data call
        return False


def read(d: str | os.PathLike[str], start: float, end: float) -> list[tuple[float, str, int, str, str]]:
    """Lines with start <= t < end: (epoch_s, client, pid, 'METHOD path', status)."""
    d = Path(d); out = []
    day = datetime.fromtimestamp(start, timezone.utc).date()
    while day <= datetime.fromtimestamp(end, timezone.utc).date():
        p = d / f"calls-{day.strftime('%Y%m%d')}.tsv"
        try:
            text = p.read_text()
        except OSError:
            text = ""
        for ln in text.splitlines():
            parts = ln.split("\t")
            if len(parts) != 5:
                continue
            try:
                t = int(parts[0]) / 1000.0; pid = int(parts[2])
            except ValueError:
                continue
            if start <= t < end:
                out.append((t, parts[1], pid, parts[3], parts[4]))
        day += timedelta(days=1)
    return sorted(out)


def per_minute(rows) -> dict[str, dict]:
    """{'HH:MMZ': {'calls': Counter(client), 'refused': Counter(client)}} in time order."""
    out: dict[str, dict] = defaultdict(lambda: {"calls": Counter(), "refused": Counter()})
    for t, client, _pid, _what, status in rows:
        k = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%MZ")
        out[k]["calls"][client] += 1
        if status == "429":
            out[k]["refused"][client] += 1
    return dict(sorted(out.items()))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Schwab calls per minute and client around a moment (schwab-usage/1)")
    ap.add_argument("--around", required=True, help="ISO time, e.g. 2026-11-20T15:37:07Z")
    ap.add_argument("--minutes", type=int, default=3, help="minutes either side (default 3)")
    ap.add_argument("--dir", default=None, help="usage dir (default: usage/ beside the token file)")
    a = ap.parse_args(argv)
    at = datetime.fromisoformat(a.around.replace("Z", "+00:00"))
    if at.tzinfo is None:
        raise SystemExit("--around needs a timezone")
    if a.dir:
        d = Path(a.dir)
    else:
        from config import settings   # only the CLI needs GRID's settings
        d = usage_dir(settings.SCHWAB_TOKEN_FILE)
    t0 = at.timestamp() - 60 * a.minutes; t1 = at.timestamp() + 60 * (a.minutes + 1)
    table = per_minute(read(d, t0, t1))
    if not table:
        print("no calls recorded in %s between %s and %s" % (d, datetime.fromtimestamp(t0, timezone.utc).isoformat(), datetime.fromtimestamp(t1, timezone.utc).isoformat()))
        return 0
    for minute, row in table.items():
        total = sum(row["calls"].values())
        by = ", ".join("%s %d" % kv for kv in row["calls"].most_common())
        ref = (" | 429: " + ", ".join("%s %d" % kv for kv in row["refused"].most_common())) if row["refused"] else ""
        print("%s  %3d calls  %s%s" % (minute, total, by, ref))
    return 0


if __name__ == "__main__":
    sys.exit(main())
