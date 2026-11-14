#!/usr/bin/env python3
"""Writer for ``market_state_daily``: the standardized market-state record (``intelligence/regime/market_state.py``).

One row per session, insert-only: a date that already has a row is left as it is unless ``--replace`` is given.
Computes the prior completed trading session by default (never today's), the same rule as
``scripts/run_regime_state_vectors.py``. ``--from/--to`` backfills a range of sessions in date order; every record is
computed only from what was known by the end of its own date, so a backfilled row equals what the nightly run would
have written then (apart from the revised-vintage caveat in the module docstring).

Usage:
    python3 scripts/run_market_state.py                         # prior session
    python3 scripts/run_market_state.py --dry-run --json        # print the record, write nothing
    python3 scripts/run_market_state.py --from 2019-01-02 --to 2026-11-12   # backfill sessions

Before computing, a real run fetches the FRED vintages published since the last stored one for the news indexes
(``ingestion/fred_vintages.py``; ``--no-refresh`` skips it). First time: ``python -m ingestion.fred_vintages backfill``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _d(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--as-of", type=_d, default=None, help="one session (YYYY-MM-DD), strictly before today")
    p.add_argument("--from", dest="start", type=_d, default=None, help="backfill start (inclusive)")
    p.add_argument("--to", dest="end", type=_d, default=None, help="backfill end (inclusive), before today")
    p.add_argument("--dry-run", action="store_true", help="compute without writing")
    p.add_argument("--json", action="store_true", help="print each record (dry run) or summary as JSON")
    p.add_argument("--replace", action="store_true", help="overwrite an existing row for the same date")
    p.add_argument("--no-refresh", action="store_true",
                   help="skip fetching new FRED vintages of the news indexes first (a dry run never fetches)")
    p.add_argument("--require-target-dealer", action="store_true",
                   help="hold a daily write unless dealer gamma is from the exact target session; exit 3 on hold")
    return p


def sessions(start: date, end: date) -> list[date]:
    from ingestion.market_calendar import trading_days_between

    return [d for d in trading_days_between(start, end) if start <= d <= end]


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def existing_row(engine, target: date) -> dict | None:
    """Read existing metadata only; never create tables or requalify a stored row."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("market_state_daily"):
        return None
    with engine.connect() as conn:
        row = conn.execute(text("SELECT rule, computed_at FROM market_state_daily WHERE as_of_date = :d"),
                           {"d": target}).mappings().first()
    return dict(row) if row is not None else None


def readiness(record: dict, target: date) -> dict:
    gamma = record.get("gamma_state") or {}
    dealer_ready = gamma.get("status") == "ok" and gamma.get("obs_date") == target.isoformat()
    variables = record.get("variables") or {}
    return {
        "target_session": target.isoformat(),
        "dealer_target_ready": dealer_ready,
        "dealer_observation_date": gamma.get("obs_date"),
        "dealer_reason": None if dealer_ready else gamma.get("reason", "no exact target-session dealer row"),
        "variable_status": {k: {a: v.get(a) for a in ("status", "obs_date", "age_days", "reason")}
                            for k, v in variables.items()},
        "analysis_gaps": sorted(k for k, v in variables.items() if v.get("status") != "ok"),
        "event_status": (record.get("events") or {}).get("status", "unknown"),
        "pending": sorted(record.get("pending") or {}),
        # Process success cannot establish vintage lineage or a timer's identity.
        "forward_qualified": False,
        "qualification_reason": "requires scheduled-run and source-vintage evidence; not certified by writer exit",
    }


def code_receipt() -> dict:
    names = ("scripts/run_market_state.py", "intelligence/regime/market_state.py",
             "intelligence/regime/baseline.py", "intelligence/regime/state_vector.py",
             "physics/dealer_gex_daily.py", "ingestion/market_calendar.py",
             "store/observations.py", "store/fred_vintages.py",
             "config/econ_release_dates.json")
    return {name: hashlib.sha256((_ROOT / name).read_bytes()).hexdigest()
            if (_ROOT / name).is_file() else None for name in names}


def main(argv: list[str] | None = None, engine=None) -> int:
    a = build_parser().parse_args(argv)
    today = utc_today()  # Explicit UTC; laptop/server local timezone cannot change the target.
    if a.require_target_dealer and (a.replace or a.start or a.end):
        print("market-state: target-dealer gate is daily, insert-only; no --replace or range", file=sys.stderr)
        return 2
    if a.start or a.end:
        if not (a.start and a.end) or a.start > a.end:
            print("market-state: --from and --to require an ordered pair", file=sys.stderr)
            return 2
        targets = sessions(a.start, a.end)
    elif a.as_of:
        targets = [a.as_of]
    else:
        from ingestion.market_calendar import last_trading_day

        targets = [last_trading_day(today - timedelta(days=1))]
    if any(t >= today for t in targets) or any(
            t is not None and t >= today for t in (a.start, a.end, a.as_of)):
        print(f"market-state: refusing a date on or after today ({today})", file=sys.stderr)
        return 2
    if not targets or any(t not in sessions(t, t) for t in targets):
        print("market-state: refusing empty or nontrading target sessions", file=sys.stderr)
        return 2

    from intelligence.regime.market_state import compute_market_state, load_calendar, store_market_state

    if engine is None:
        from db import get_engine

        engine = get_engine()
    started = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())
    outcomes, pending = [], []
    for t in targets:
        old = None if a.dry_run or a.replace else existing_row(engine, t)
        if old is None:
            pending.append(t)
        else:
            outcomes.append({"target_session": t.isoformat(), "outcome": "already_present",
                             "written_this_run": False, "forward_qualified": False,
                             "stored_computed_at": str(old["computed_at"]), "stored_rule": old["rule"],
                             "reason": "existing row preserved; backfill/timer origin not inferred"})
    calendar = load_calendar()
    refresh = []
    if pending and not a.dry_run and not a.no_refresh:
        from ingestion.fred_vintages import SERIES, refresh as refresh_vintages

        refresh = [refresh_vintages(engine, s) for s in SERIES]
    written = held = 0
    code = code_receipt()
    for t in pending:
        state = compute_market_state(engine, t, calendar=calendar)
        rec = state.to_dict()
        ready = readiness(rec, t)
        state.writer_provenance = {
            "run_id": run_id, "started_at": started, "target_session": t.isoformat(),
            "cutoff_exclusive": (t + timedelta(days=1)).isoformat() + "T00:00:00+00:00",
            "mode": "replacement" if a.replace else "backfill" if a.start else
                    "explicit_session" if a.as_of else "daily",
            "dry_run": a.dry_run, "target_dealer_required": a.require_target_dealer,
            "execution_origin": "unverified", "code_sha256": code, "readiness": ready,
        }
        outcome = {**ready, "written_this_run": False}
        if a.dry_run:
            outcome["outcome"] = "dry_run"
            outcome["would_hold"] = a.require_target_dealer and not ready["dealer_target_ready"]
            if a.json:
                print(json.dumps(state.to_dict(), sort_keys=True, default=str))
            else:
                print(f"market-state {t}: {rec['coverage']['ok']}/{rec['coverage']['total']} variables ok (dry run)")
        elif a.require_target_dealer and not ready["dealer_target_ready"]:
            outcome["outcome"] = "held_missing_target_dealer"
            held += 1
        elif store_market_state(engine, state, replace=a.replace):
            written += 1
            outcome["outcome"] = "replaced" if a.replace else "written"
            outcome["written_this_run"] = True
        else:
            # A competing insert can win after our preflight. Never report our computed
            # metadata as the winner's stored provenance or overwrite that record.
            outcome = {"target_session": t.isoformat(), "outcome": "already_present_race",
                       "written_this_run": False, "forward_qualified": False}
        outcomes.append(outcome)
    if not a.dry_run:
        summary = {"job": "run_market_state", "run_id": run_id, "sessions": len(targets),
                   "written": written, "held": held,
                   "already_present": sum(o["outcome"].startswith("already_present") for o in outcomes),
                   "outcomes": sorted(outcomes, key=lambda o: o["target_session"]),
                   "vintage_refresh": [{k: r.get(k) for k in ("series", "status", "rows_sent")} for r in refresh],
                   "generated_at": datetime.now(timezone.utc).isoformat()}
        print(json.dumps(summary, default=str) if a.json else
              f"market-state: {written} written, {summary['already_present']} already present, {held} held")
    return 3 if held else 0


if __name__ == "__main__":
    sys.exit(main())
