#!/usr/bin/env python3
"""Write one desk brief (3pacs/GRID#1164): pre-open or post-close, for one XNYS session.

    python -m scripts.desk_brief run --kind post_close [--session YYYY-MM-DD] [--as-of ISO] \
        --out /data/agent-home/anikdang/desk-brief-data [--config config/desk_brief_sources.json] \
        [--narrate --llm-url http://gridz4:8080 --llm-model Qwen3.8-27B-Q4_K_M] [--dry-run]

Without --session: pre_open is for the next session whose open is still ahead (today's on a trading morning),
post_close for the latest session that has closed. Without --as-of the brief's clock is the registered one
(``analysis.desk_brief.default_as_of``); a run before that clock refuses (exit 2), so a timer that fires early
writes nothing.

Output: ``<out>/<session>-<kind>.json`` and ``.md``, owner-only (folder 0700, files 0600), written once. A second
run for the same session and kind writes nothing and exits 0. The brief is private: never served, never posted.
The database is read in a read-only transaction; ledgers are opened ``mode=ro``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from analysis import desk_brief as B
from analysis import desk_brief_sources as S


def _code_version() -> str | None:
    try:
        return subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True,
                              timeout=5).stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def pick_session(kind: str, now: datetime, cal) -> date:
    """pre_open: the first session that has not opened; post_close: the latest session that has closed."""
    for s in cal.sessions:
        if kind == "pre_open" and s.opens > now:
            return date.fromisoformat(s.key.split(":")[1])
    if kind == "pre_open":
        raise ValueError("the pinned calendar has no later session")
    closed = [s for s in cal.sessions if s.closes <= now]
    if not closed:
        raise ValueError("no closed session in the pinned calendar")
    return date.fromisoformat(closed[-1].key.split(":")[1])


def collect(kind: str, session: B.SessionTimes, prev_day: date, as_of: datetime, cfg: dict, engine) -> tuple[list, list]:
    sections, trials = [], []
    expected = prev_day if kind == "post_close" else S.previous_session(prev_day)
    ms = S.market_state(engine, kind, session, as_of, expected) if engine is not None else B.Section(
        "market_state", "Market state", "unknown", "griddb market_state_daily", problems=("no database configured",))
    sections.append(ms)
    sections.append(S.calendar(session, as_of, market_events=(ms.facts or {}).get("events")))
    if kind == "post_close":
        sections.append(S.paper_record(cfg.get("paper_record"), kind, as_of))
    sections.append(S.desk_evening(cfg["desk_evening_dir"], kind, session, prev_day, as_of))
    sec, tr = S.records(cfg["records_url"], as_of)
    sections.append(sec)
    trials += tr
    for spec in cfg.get("ledgers") or []:
        trials.append(S.ledger_trial(spec, kind, session, prev_day, as_of))
    if cfg.get("selector"):
        trials.append(S.selector(cfg["selector"], kind, session, as_of))
    if cfg.get("plays"):
        trials.append(S.plays(cfg["plays"], kind, session, prev_day, as_of))
    if cfg.get("trader"):
        trials.append(S.trader_day(cfg["trader"], kind, session, as_of))
    sections.append(S.receipts(cfg["receipts_dir"], prev_day if kind == "pre_open" else session.day, as_of))
    sections.append(S.gex_state(cfg["gex_state_url"], as_of))
    sections.append(S.desk_health(cfg["desk_health_dir"], session, as_of))
    return sections, trials


def _engine():
    url = os.environ.get("DESK_BRIEF_DB_URL", "postgresql+psycopg2:///griddb")
    if not url:
        return None
    from sqlalchemy import create_engine

    return create_engine(url, connect_args={"options": "-c default_transaction_read_only=on"}
                         if url.startswith("postgresql") else {}, pool_pre_ping=False)


def write_once(out: Path, stem: str, brief: dict, md: str) -> bool:
    out.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(out, 0o700)
    paths = (out / f"{stem}.json", out / f"{stem}.md")
    if any(p.exists() for p in paths):
        return False
    for p, data in zip(paths, (json.dumps(brief, indent=1, sort_keys=True) + "\n", md)):
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(data)
    return True


def run(args) -> int:
    from analysis.factory_session_calendar import PINS, FactoryCalendar

    cal = FactoryCalendar(pins=PINS)
    now = datetime.now(UTC)
    kind = args.kind
    day = date.fromisoformat(args.session) if args.session else pick_session(kind, now, cal)
    session = S.session_times(day, calendar=cal)
    prev_day = S.previous_session(day, calendar=cal)
    if args.as_of:
        as_of = B.parse_clock(args.as_of)
        if as_of is None:
            print(json.dumps({"refused": "--as-of must carry its offset"}))
            return 2
        registered = as_of == B.default_as_of(kind, session)
    else:
        as_of, registered = B.default_as_of(kind, session), True
    if as_of > now:
        print(json.dumps({"refused": f"the brief's clock {B.iso(as_of)} is still ahead", "session": day.isoformat()}))
        return 2
    cfg = S.load_config(Path(args.config))
    sections, trials = collect(kind, session, prev_day, as_of, cfg, None if args.no_db else _engine())
    brief = B.build_brief(kind, session, as_of, sections, trials, as_of_registered=registered,
                          generated_at=datetime.now(UTC), code_version=_code_version())
    if args.narrate:
        brief["narrative"] = B.narrate(brief, url=args.llm_url, model=args.llm_model)
    md = B.render_markdown(brief)
    stem = f"{day.isoformat()}-{kind}"
    if args.dry_run:
        sys.stdout.write(md)
        return 0
    wrote = write_once(Path(args.out), stem, brief, md)
    print(json.dumps({"session": day.isoformat(), "kind": kind, "as_of": brief["as_of"], "written": wrote,
                      "file": str(Path(args.out) / f"{stem}.md"), "problems": len(brief["data_problems"]),
                      "narrative": bool((brief.get("narrative") or {}).get("text"))}))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--kind", required=True, choices=B.KINDS)
    r.add_argument("--session")
    r.add_argument("--as-of")
    r.add_argument("--out", default="/data/agent-home/anikdang/desk-brief-data")
    r.add_argument("--config", default=str(REPO / "config" / "desk_brief_sources.json"))
    r.add_argument("--no-db", action="store_true", help="skip griddb (market state reads unknown)")
    r.add_argument("--narrate", action="store_true")
    r.add_argument("--llm-url", default="http://gridz4:8080")
    r.add_argument("--llm-model", default="Qwen3.8-27B-Q4_K_M")
    r.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
