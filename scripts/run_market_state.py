#!/usr/bin/env python3
"""Writer for ``market_state_daily``: the standardized market-state record (``intelligence/regime/market_state.py``).

One row per session, insert-only: a date that already has a row is left as it is unless ``--replace`` is given.
Computes the prior completed trading session by default (never today's), the same rule as
``scripts/run_regime_state_vectors.py``. ``--from/--to`` backfills a range of sessions in date order; every record is
computed using date bounds and declared source availability policies. Modeled lags on revised backfills do not
reconstruct the historical nightly result; that requires retained vintages and publication/capture evidence.

Usage:
    python3 scripts/run_market_state.py                         # prior session
    python3 scripts/run_market_state.py --dry-run --json        # print the record, write nothing
    python3 scripts/run_market_state.py --from 2019-01-02 --to 2026-11-12   # backfill sessions
    python3 scripts/run_market_state.py --provisional --json    # today's session at its close, display only

``--provisional`` (#1312) writes a display-only record for today's UTC session, and only at or after that session's
close in the pinned XNYS calendar (``analysis/factory_session_calendar.PINS``). Before the close, or on a day outside
the pinned parts, it refuses with exit 2. On a day that is not a session (a weekday market holiday such as
Thanksgiving, or a weekend) there is nothing to write: it says so (``outcome: not_a_session``) and exits 0, so the
weekday timer does not end failed on a holiday. It runs ``compute_market_state`` with ``known_by`` = that close, an
exact availability cutoff, so a rerun hours later reads only what GRID held at the close. It never refreshes the FRED
vintages (``--no-refresh`` is accepted and changes nothing; no flag turns a refresh on). It writes to its own table
(never ``market_state_daily``), insert-only unless ``--replace``, and never takes ``--from/--to``, ``--as-of`` or
``--require-target-dealer``. Nothing that decides reads it; the decision record for the session is still the one this
writer makes the next evening.

Each decision record also carries a ``spells`` block (``intelligence/regime/spells.py``, #1309): every variable's
current label spell and three named spells, derived from the stored labels of earlier sessions plus this record, and
the transitions read at this session (``source: writer``). A provisional record has none. A dry run follows the
same admission as the command without ``--dry-run``: it reads which targets already have rows and records, in
``writer_provenance.dry_run_admission``, what the live run would do (``would_write``, ``would_replace``,
``would_hold`` or ``existing_row_preserved``). Later targets' spells follow only the records the live run would
store, so a dry run's spells blocks are the live run's.

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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analysis.factory_session_calendar import FactoryCalendar
    from ingestion.market_calendar import MarketSession

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
    p.add_argument("--provisional", action="store_true",
                   help="display-only record for today's UTC session, from the inputs held at its close (#1312); "
                        "refuses before the close or outside the pinned calendar (exit 2); on a day that is not a "
                        "session it writes nothing and exits 0; never refreshes vintages")
    return p


def sessions(start: date, end: date) -> list[date]:
    from ingestion.market_calendar import trading_days_between

    return [d for d in trading_days_between(start, end) if start <= d <= end]


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
        "variable_status": {k: {a: v.get(a) for a in ("status", "obs_date", "age_days", "freshness", "reason")}
                            for k, v in variables.items()},
        "analysis_gaps": sorted(k for k, v in variables.items() if v.get("status") != "ok"),
        "event_status": (record.get("events") or {}).get("status", "unknown"),
        "pending": sorted(record.get("pending") or {}),
        # Process success cannot establish vintage lineage or a timer's identity.
        "forward_qualified": False,
        "qualification_reason": "requires scheduled-run and source-vintage evidence; not certified by writer exit",
    }


def dry_run_admission(*, existing: bool, replace: bool, hold: bool) -> str:
    """What the same command without ``--dry-run`` would do with one target, in the live path's order (#1309).

    The live run skips a target that already has a row (unless ``--replace``) before computing it, then holds one
    whose dealer gate fails, then writes. Only ``would_write`` and ``would_replace`` advance later targets' spells.
    """
    if existing and not replace:
        return "existing_row_preserved"
    if hold:
        return "would_hold"
    return "would_replace" if replace else "would_write"


ADVANCES_SPELLS = frozenset({"would_write", "would_replace"})


def code_receipt() -> dict:
    names = ("scripts/run_market_state.py", "intelligence/regime/market_state.py",
             "intelligence/regime/baseline.py", "intelligence/regime/spells.py", "intelligence/regime/state_vector.py",
             "physics/dealer_gex_daily.py", "ingestion/market_calendar.py",
             "store/observations.py", "store/fred_vintages.py", "analysis/factory_session_calendar.py",
             "config/econ_release_dates.json")
    return {name: hashlib.sha256((_ROOT / name).read_bytes()).hexdigest()
            if (_ROOT / name).is_file() else None for name in names}


def main(argv: list[str] | None = None, engine=None) -> int:
    a = build_parser().parse_args(argv)
    if a.provisional:
        return run_provisional(a, engine)
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

    from intelligence.regime.market_state import RECORD_RULE, compute_market_state, load_calendar, store_market_state
    from intelligence.regime.spells import SpellRow, load_rows, record_spells, spells_unavailable

    if engine is None:
        from db import get_engine

        engine = get_engine()
    started = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())
    outcomes, pending, preserved = [], [], set()
    for t in targets:
        old = None if a.replace else existing_row(engine, t)   # a read; a dry run needs it to mirror admission
        if old is None:
            pending.append(t)
        elif a.dry_run:          # still computed and printed, but the live run would leave the stored row
            pending.append(t)
            preserved.add(t)
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
    # Spells (#1309) read the stored labels once, then follow the records this run stores (a dry run: would store),
    # so a range sees its earlier targets and a dry run's blocks are the live run's. A failed read leaves every record
    # without spells, never unwritten.
    spell_rows: dict[date, SpellRow] | None = {}
    spell_error = ""
    if pending:
        try:
            spell_rows = {r.as_of: r for r in load_rows(engine, through=max(pending))}
        except Exception as exc:  # spells never sink the record
            spell_rows, spell_error = None, f"error reading stored labels: {type(exc).__name__}"
    for t in pending:
        state = compute_market_state(engine, t, calendar=calendar)
        rec = state.to_dict()
        ready = readiness(rec, t)
        hold = a.require_target_dealer and not ready["dealer_target_ready"]
        state.writer_provenance = {
            "run_id": run_id, "started_at": started, "target_session": t.isoformat(),
            "cutoff_exclusive": (t + timedelta(days=1)).isoformat() + "T00:00:00+00:00",
            "mode": "replacement" if a.replace else "backfill" if a.start else
                    "explicit_session" if a.as_of else "daily",
            "dry_run": a.dry_run, "target_dealer_required": a.require_target_dealer,
            "execution_origin": "unverified", "code_sha256": code, "readiness": ready,
        }
        admission = dry_run_admission(existing=t in preserved, replace=a.replace, hold=hold) if a.dry_run else None
        if admission is not None:
            state.writer_provenance["dry_run_admission"] = admission
        state.spells = (record_spells(spell_rows.values(), t, RECORD_RULE, state.to_dict())
                        if spell_rows is not None else spells_unavailable(t, spell_error))
        outcome = {**ready, "written_this_run": False, "spells_status": state.spells.get("status")}
        if a.dry_run:
            outcome["outcome"] = "dry_run"
            outcome["dry_run_admission"] = admission
            if spell_rows is not None and admission in ADVANCES_SPELLS:
                spell_rows[t] = SpellRow.from_record(t, RECORD_RULE, state.to_dict())
            outcome["would_hold"] = hold
            if a.json:
                print(json.dumps(state.to_dict(), sort_keys=True, default=str))
            else:
                print(f"market-state {t}: {rec['coverage']['ok']}/{rec['coverage']['total']} variables ok "
                      f"(dry run: {admission})")
        elif hold:
            outcome["outcome"] = "held_missing_target_dealer"
            held += 1
        elif store_market_state(engine, state, replace=a.replace):
            written += 1
            outcome["outcome"] = "replaced" if a.replace else "written"
            outcome["written_this_run"] = True
            if spell_rows is not None:
                spell_rows[t] = SpellRow.from_record(t, RECORD_RULE, state.to_dict())
        else:
            # A competing insert can win after our preflight. Never report our computed
            # metadata as the winner's stored provenance or overwrite that record.
            outcome = {"target_session": t.isoformat(), "outcome": "already_present_race",
                       "written_this_run": False, "forward_qualified": False}
            if spell_rows is not None:  # later targets follow the winner's stored labels, not ours
                spell_rows.pop(t, None)
                try:
                    spell_rows.update({r.as_of: r for r in load_rows(engine, through=t) if r.as_of == t})
                except Exception as exc:  # spells never sink the record
                    spell_rows, spell_error = None, f"error reading stored labels: {type(exc).__name__}"
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


# ── --provisional (#1312): display only, at the close ───────────────────────

class NotASession(ValueError):
    """Today's UTC day is not an XNYS session in the pinned calendar: nothing to write, which is not a fault."""

    def __init__(self, day: date, calendar_version: str) -> None:
        super().__init__(f"{day} is not an XNYS session in the pinned calendar")
        self.day, self.calendar_version = day, calendar_version


def provisional_target(now: datetime,
                       calendar: FactoryCalendar | None = None) -> tuple[date, MarketSession, FactoryCalendar]:
    """(session day, its pinned session, the calendar) for today's UTC session once its close has passed.

    ``now`` must be aware. ValueError when the day is outside the pinned XNYS parts or its close (``closes`` in the
    manifest: 21:00Z or 20:00Z, earlier on an early-close day) is still ahead; :class:`NotASession` (a ValueError)
    when the day is not a session. Every XNYS close is
    before midnight UTC, so the session is the one dated today in UTC; after midnight UTC the next day is targeted
    and refused until its own close.
    """
    if now.utcoffset() is None:
        raise ValueError("the provisional clock must be timezone-aware")
    from analysis.factory_session_calendar import PINS, FactoryCalendar

    now = now.astimezone(timezone.utc)
    cal = calendar or FactoryCalendar(pins=PINS)
    day = now.date()
    session = cal.for_day(day.isoformat())       # ValueError outside the pinned parts
    if session is None:
        raise NotASession(day, cal.version)
    if now < session.closes:
        raise ValueError(f"session {day} closes at {session.closes.isoformat()}; it is {now.isoformat()}")
    return day, session, cal


def existing_provisional_row(engine, target: date) -> dict | None:  # engine: sqlalchemy Engine, as existing_row
    """Metadata of a stored provisional row; never creates the table."""
    from sqlalchemy import inspect, text

    if not inspect(engine).has_table("market_state_provisional"):
        return None
    with engine.connect() as conn:
        row = conn.execute(text("SELECT rule, computed_at FROM market_state_provisional WHERE as_of_date = :d"),
                           {"d": target}).mappings().first()
    return dict(row) if row is not None else None


def not_a_session(a: argparse.Namespace, now: datetime, exc: NotASession) -> int:
    """No session today: report it and exit 0. Nothing is computed, read or written, and no table is touched."""
    from intelligence.regime.market_state import PROVISIONAL_DECISION_USE, PROVISIONAL_MODE, PROVISIONAL_TABLE

    if a.json:
        print(json.dumps({"job": "run_market_state", "mode": PROVISIONAL_MODE,
                          "decision_use": PROVISIONAL_DECISION_USE, "table": PROVISIONAL_TABLE,
                          "day": exc.day.isoformat(), "target_session": None,
                          "outcome": "not_a_session", "written": 0, "reason": str(exc),
                          "calendar": exc.calendar_version, "dry_run": a.dry_run, "generated_at": now.isoformat()},
                         default=str))
    else:
        print(f"market-state provisional {exc.day}: not an XNYS session in the pinned calendar; nothing to write")
    return 0


def run_provisional(a: argparse.Namespace, engine=None) -> int:
    """Today's UTC session at or after its close, into market_state_provisional. Display only (#1312).

    The record is computed with the session's close as its availability cutoff, and this path never refreshes the
    FRED vintages: a refresh writes rows fetched now, and those are after the close by construction.
    """
    if a.start or a.end or a.as_of or a.require_target_dealer:
        print("market-state: --provisional targets today's session only; no --from/--to, --as-of or "
              "--require-target-dealer", file=sys.stderr)
        return 2
    now = utc_now()
    try:
        target, session, cal = provisional_target(now)
    except NotASession as exc:
        return not_a_session(a, now, exc)
    except ValueError as exc:
        print(f"market-state: provisional refused: {exc}", file=sys.stderr)
        return 2

    from intelligence.regime.market_state import (
        PROVISIONAL_DECISION_USE,
        PROVISIONAL_MODE,
        PROVISIONAL_TABLE,
        compute_market_state,
        load_calendar,
        provisional_record,
        store_provisional_market_state,
    )

    if engine is None:
        from db import get_engine

        engine = get_engine()
    started = now.isoformat()
    cutoff = session.closes.astimezone(timezone.utc)
    run_id = str(uuid.uuid4())
    written = 0
    old = None if a.dry_run or a.replace else existing_provisional_row(engine, target)
    if old is not None:
        outcome = {"target_session": target.isoformat(), "outcome": "already_present", "written_this_run": False,
                   "forward_qualified": False, "stored_computed_at": str(old["computed_at"]),
                   "stored_rule": old["rule"], "reason": "existing provisional row preserved"}
    else:
        state = compute_market_state(engine, target, calendar=load_calendar(), known_by=cutoff)
        ready = readiness(state.to_dict(), target)
        state.writer_provenance = {
            "run_id": run_id, "started_at": started, "target_session": target.isoformat(),
            "availability_cutoff": cutoff.isoformat(),
            "minutes_after_close": round((now - cutoff).total_seconds() / 60, 1),
            "mode": PROVISIONAL_MODE, "decision_use": PROVISIONAL_DECISION_USE, "table": PROVISIONAL_TABLE,
            "session_closes": session.closes.isoformat(), "calendar": cal.version,
            "computed_after_close": True, "vintage_refresh": "never",
            "dry_run": a.dry_run, "replace": a.replace, "target_dealer_required": False,
            "execution_origin": "unverified", "code_sha256": code_receipt(), "readiness": ready,
        }
        outcome = {**ready, "written_this_run": False}
        if a.dry_run:
            rec = provisional_record(state)
            if a.json:
                print(json.dumps(rec, sort_keys=True, default=str))
            else:
                print(f"market-state provisional {target}: {rec['coverage']['ok']}/{rec['coverage']['total']} "
                      "variables ok (dry run)")
            return 0
        if store_provisional_market_state(engine, state, replace=a.replace):
            written = 1
            outcome["outcome"] = "replaced" if a.replace else "written"
            outcome["written_this_run"] = True
        else:
            outcome = {"target_session": target.isoformat(), "outcome": "already_present_race",
                       "written_this_run": False, "forward_qualified": False}
    summary = {"job": "run_market_state", "mode": PROVISIONAL_MODE, "decision_use": PROVISIONAL_DECISION_USE,
               "table": PROVISIONAL_TABLE, "run_id": run_id, "target_session": target.isoformat(),
               "session_closes": session.closes.isoformat(), "written": written,
               "already_present": int(outcome["outcome"].startswith("already_present")), "outcomes": [outcome],
               "availability_cutoff": cutoff.isoformat(),
               "vintage_refresh": "never: a provisional run reads only what GRID held at the close",
               "generated_at": datetime.now(timezone.utc).isoformat()}
    print(json.dumps(summary, default=str) if a.json else
          f"market-state provisional {target}: {written} written, {summary['already_present']} already present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
