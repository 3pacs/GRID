#!/usr/bin/env python3
"""Write the GEX native-volume snapshot (``ingestion.gex_native_volume``).

One Schwab ``/quotes`` and one ``/pricehistory`` call per run, read-only.
The snapshot is written atomically (mkstemp + fsync + os.replace) into a
directory the operator created for it: owned by this uid, mode 0700. The file
is mode 0600, which is what gex-watch's reader requires (both run as grid).

    python scripts/gex_native_volume_snapshot.py --out /data/agent-home/anikdang/gex-native-volume/snapshot.json
    python scripts/gex_native_volume_snapshot.py --out ... --loop 60     # every 60 s
    python scripts/gex_native_volume_snapshot.py --print                 # compute and print, write nothing
    python scripts/gex_native_volume_snapshot.py --dry-run --loop 60     # one line a minute: status and the
                                                                         # bars-vs-quote gap, no prices, no file

Exit 0 when a payload was produced, whatever its status ("closed",
"unreconciled" ... are honest outcomes); 1 on a Schwab or file error.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion import gex_native_volume as nv


def write_private(path: Path, data: dict) -> None:
    parent = path.parent
    st = parent.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.geteuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise SystemExit(f"{parent} must be a directory owned by this user with mode 0700")
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=".snapshot-", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, sort_keys=True, separators=(",", ":"), allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


MARKS_NAME = "quote-marks.json"     # beside the snapshot: this session's (completed_minutes, quote counter) pairs
MAX_MARKS = 450


def read_marks(path: Path, session: str) -> list[tuple[int, int]]:
    """This session's quote marks, or [] (missing, unreadable, another day, or not ours and private)."""
    try:
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid() or stat.S_IMODE(st.st_mode) & 0o077:
            return []
        body = json.loads(path.read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(body, dict) or body.get("session") != session or not isinstance(body.get("marks"), list):
        return []
    return [(m[0], m[1]) for m in body["marks"]
            if isinstance(m, list) and len(m) == 2 and type(m[0]) is int and type(m[1]) is int]


def keep_mark(path: Path, session: str, marks: list[tuple[int, int]], mark: tuple[int, int] | None) -> None:
    """Add this run's mark (the latest counter per completed-minute count wins) and write the file privately."""
    if mark is None:
        return
    by_n = dict(marks)
    by_n[mark[0]] = mark[1]
    kept = sorted(by_n.items())[-MAX_MARKS:]
    write_private(path, {"session": session, "marks": [[n, q] for n, q in kept]})


def run_once(client, out: Path | None) -> tuple[dict, dict]:
    now = time.time()
    try:
        _, current, _ = nv.calendar_window(now)
    except (OSError, ValueError):
        current = None
    if current is None:
        # Known closure or unavailable pinned calendar: no Schwab calls and no
        # write (the private reader lets the previous snapshot age out).
        payload, info = nv.compute({}, None, now=now, receipt_at=now)
        return nv.envelope(payload, nv.provenance([])), info
    session = current.opens.astimezone(nv.ET).date().isoformat()
    marks_path = out.parent / MARKS_NAME if out is not None else None
    marks = read_marks(marks_path, session) if marks_path is not None else []
    quote = client.quotes(nv.SYMBOL).get(nv.SYMBOL)
    history = client.price_history(nv.SYMBOL, **nv.history_params(now))
    receipt = time.time()
    payload, info = nv.compute(history, getattr(quote, "total_volume", None), now=time.time(), receipt_at=receipt,
                               quote_marks=marks)
    env = nv.envelope(payload, nv.provenance(info["bars"]))
    if out is not None:
        write_private(out, env)
    if marks_path is not None:
        keep_mark(marks_path, session, marks, info.get("quote_mark"))
    return env, info


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", type=Path, help="snapshot file (its directory must be 0700 and ours)")
    p.add_argument("--print", action="store_true", help="print the envelope; write nothing")
    p.add_argument("--dry-run", action="store_true", help="compute and log status + reconciliation only; write nothing")
    p.add_argument("--loop", type=float, default=0.0, help="repeat every N seconds")
    args = p.parse_args(argv)
    if not (args.print or args.dry_run) and args.out is None:
        p.error("pass --out, --print or --dry-run")
    from ingestion.schwab_market import (
        SchwabError,
        SchwabMarketClient,
    )

    client = SchwabMarketClient()
    while True:
        try:
            env, info = run_once(client, None if (args.print or args.dry_run) else args.out)
            pl = env["payload"]
            r = info.get("reconciliation") or {}
            line = (f"{env['generated_at']} {pl['status']} {pl.get('reason') or '-'} "
                    f"bars={r.get('bars_total_volume')} quote={r.get('quote_total_volume')} "
                    f"diff={r.get('relative_difference')}")
            # The first line keeps its exact form (the burn-in grader's journal pattern ends at diff=).
            if info.get("quote_mark") is not None and not args.print:
                line += f"\n{env['generated_at']} vwap-scaled {info.get('vwap_scaled_bars')} of {pl.get('completed_minutes')}"
            print(json.dumps(env, indent=1) if args.print else line, flush=True)
            rc = 0
        except SchwabError as exc:
            print(f"schwab error: {type(exc).__name__}", file=sys.stderr, flush=True)
            rc = 1
        if args.loop <= 0:
            return rc
        time.sleep(args.loop)


if __name__ == "__main__":
    sys.exit(main())
