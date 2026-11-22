"""Read-only adapters for the desk brief (3pacs/GRID#1164). Each returns ``Section`` or ``Trial`` values.

Every adapter reads a record another job already keeps and copies what it says, with that record's own clock:

* ``market_state``: the latest ``market_state_daily`` row (#1191; its own ``rule``, market-state/1 or /2) for an earlier
  session, computed before the brief's clock. Read-only transaction, parameterized SQL. ``computed_at`` is a naive TIMESTAMP written
  with CURRENT_TIMESTAMP; it is read as UTC only when the session's TimeZone is UTC, otherwise the section is unknown.
  Post-close only, and only while the session's own decision record is not there yet: the provisional record computed
  at that session's close (#1312), display only. Every line it adds starts with ``PROVISIONAL (display only)`` and its
  facts sit under their own ``provisional`` key; nothing that decides reads it.
* ``calendar``: the pinned XNYS parts and the pinned event parts (``analysis/factory_session_calendar``, forward
  parts from #1293), asked "what was known at the brief's clock" (``FactoryEvents.known``). A day the parts do not
  answer is unknown, never quiet. The market-state record's own event list (GDP, PCE, ...) is copied beside it.
* ``records``: gex-watch's gex-records/1 block: every gex-signals trial, the factory slots' feature history, the
  shadow trader and GEX levels v1, counts only, as that page publishes them.
* ``ledger_trials``: the shadow ledgers (sqlite, opened ``mode=ro``) named in ``config/desk_brief_sources.json``:
  the session's row (status and reason) and the number of rows that count toward the registered look.
* ``selector``, ``plays``, ``trader_day``: the session's choices from the selector, the overnight plays table and
  the shadow trader's day state. Choices only; no expected values or P&L.
* ``desk_health`` / ``desk_evening``: the desk-state jobs' own daily files (desk-health/1, desk-evening/1).
* ``gex_state``: gex-watch ``/api/state``: whether current GEX is served or withheld, and why.
* ``receipts``: the Desk source receipt for the latest closed session (``intelligence/desk_source_receipts``).
* ``paper_record``: the graded paper record (D4, 3pacs/GRID#14), when it exists. Until then: not supplied.

Identifiers in ledger SQL come from the reviewed config and are checked against ``IDENT`` and the table's own
columns; every value is a bound parameter. A row whose clock cannot be read as an aware instant is not counted, and
the section says how many were dropped.
"""

from __future__ import annotations

import json
import re
import sqlite3
import urllib.request
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from analysis.desk_brief import Section, SessionTimes, Trial, iso, parse_clock

IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,63}$")
SOURCES_SCHEMA = "desk-brief-sources/1"
NOTABLE_LABELS = ("high", "low", "above", "below")
Fetch = Callable[[str, float], bytes]


def _fetch(url: str, timeout: float) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def load_config(path: Path) -> dict:
    cfg = json.loads(Path(path).read_text())
    if cfg.get("schema") != SOURCES_SCHEMA:
        raise ValueError(f"{path}: schema must be {SOURCES_SCHEMA}")
    return cfg


def ro_sqlite(path: str | Path) -> sqlite3.Connection:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(str(p))
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5)


# ── session ──────────────────────────────────────────────────────────────────

def session_times(day: date, *, calendar=None) -> SessionTimes:
    """The session's open and close from the pinned XNYS parts (forward part included). Closed day: ValueError."""
    from analysis.factory_session_calendar import PINS, FactoryCalendar

    cal = calendar or FactoryCalendar(pins=PINS)
    s = cal.for_day(day.isoformat())
    if s is None:
        raise ValueError(f"{day} is not an XNYS session")
    return SessionTimes(day=day, opens=s.opens.astimezone(UTC), closes=s.closes.astimezone(UTC))


def previous_session(day: date, *, calendar=None) -> date:
    from analysis.factory_session_calendar import PINS, FactoryCalendar

    cal = calendar or FactoryCalendar(pins=PINS)
    return date.fromisoformat(cal.previous_day(day.isoformat()))


# ── market state (#1191) ─────────────────────────────────────────────────────

_MS_SQL = text("SELECT as_of_date, computed_at, rule, record FROM market_state_daily "
               "WHERE as_of_date <= :d ORDER BY as_of_date DESC LIMIT 10")
# Display only (#1312): the record the provisional writer computed at the session's close. Read for the post-close
# brief's own session and shown only while that session's decision record is not there yet.
_MSP_SQL = text("SELECT as_of_date, computed_at, rule, record FROM market_state_provisional WHERE as_of_date = :d")
PROVISIONAL_PREFIX = "PROVISIONAL (display only)"


def _stored_clock(raw: Any) -> datetime:
    computed = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    return computed.replace(tzinfo=UTC) if computed.utcoffset() is None else computed


def _record_lines(rec: dict) -> tuple[list[str], list[str]]:
    """One market-state record's coverage, away-from-normal, not-usable, dealer-gamma and news lines, and the
    away-from-normal entries alone."""
    lines: list[str] = []
    cov = rec.get("coverage") or {}
    if cov:
        lines.append("coverage: " + ", ".join(f"{k} {v}" for k, v in sorted(cov.items())))
    notable, unavailable = [], []
    for name, v in sorted((rec.get("variables") or {}).items()):
        if not isinstance(v, dict):
            continue
        if v.get("status") != "ok":
            unavailable.append(f"{name} ({v.get('status')})")
            continue
        if v.get("label") in NOTABLE_LABELS:
            z = v.get("robust_z")
            ztxt = f"z {z:+.2f}" if isinstance(z, (int, float)) else "no z"
            notable.append(f"{name} {v['label']} ({ztxt}, observed {v.get('obs_date')})")
    lines.append("away from normal: " + ("; ".join(notable) if notable else "none"))
    if unavailable:
        lines.append("not usable: " + "; ".join(unavailable))
    g = rec.get("gamma_state") or {}
    if g:
        lines.append(f"dealer gamma ({g.get('obs_date')}): {g.get('regime')}, spot {g.get('spot_vs_flip')} the flip"
                     if g.get("status") == "ok" else f"dealer gamma: {g.get('status')}")
    ns = rec.get("news_shock") or {}
    if ns:
        lines.append(f"news uncertainty ({ns.get('obs_date')}): {ns.get('kind')}; attribution {ns.get('news_attribution')}")
    pr = rec.get("policy_response") or {}
    if pr:
        lines.append(_policy_line(pr))
    return lines, notable


def _policy_line(pr: dict) -> str:
    """market-state/3's policy_response (the Fed put composite): research only, so the line says so."""
    head = f"policy response ({pr.get('rule')}, research only, decides nothing): "
    if pr.get("status") != "ok":
        return head + f"unknown ({pr.get('reason')})"
    c = pr.get("controls") or {}
    text_ = (f"{pr.get('label')}, VIX z {c.get('vix_only_z'):+.2f}, front-end easing z "
             f"{c.get('front_end_easing_z'):+.2f}")
    nm = pr.get("next_meeting") or {}
    if nm.get("status") == "ok":
        text_ += f"; next meeting {str(nm.get('meeting_at'))[:10]} expected upper {nm.get('expected_upper'):.3f}%"
        odds = nm.get("odds")
        if odds:
            text_ += f" (hike {odds['hike']:.0%}, hold {odds['hold']:.0%}, cut {odds['cut']:.0%})"
    return head + text_


def _provisional(row: Any, session: SessionTimes, as_of: datetime) -> tuple[list[str], dict | None, list[str]]:
    """Lines, facts and problems for the session's provisional record (#1312). Shown only when it was computed after
    the session's close and before the brief's clock, declares itself provisional and display only, and names the
    session's close as its availability cutoff (every input it read was held by then)."""
    if row is None:
        return [], None, []
    day = session.day.isoformat()
    try:
        computed = _stored_clock(row[1])
        rec = row[3] if isinstance(row[3], dict) else json.loads(row[3])
    except (TypeError, ValueError) as exc:
        return [], None, [f"provisional record for {day} unreadable ({type(exc).__name__}); not shown"]
    if computed > as_of:
        return [], None, []                      # not there yet at the brief's clock
    if computed < session.closes:
        return [], None, [(f"provisional record for {day} was computed at {iso(computed)}, before the session's "
                           f"close {iso(session.closes)}; not shown")]
    if (rec.get("provisional") is not True or rec.get("decision_use") != "display_only"
            or rec.get("as_of") != day):
        return [], None, [(f"provisional record for {day} does not declare itself provisional, display only and "
                           "for that session; not shown")]
    if parse_clock(rec.get("availability_cutoff")) != session.closes:
        return [], None, [(f"provisional record for {day} names availability cutoff {rec.get('availability_cutoff')!r},"
                           f" not the session's close {iso(session.closes)}; not shown")]
    body, notable = _record_lines(rec)
    after = round((computed - session.closes).total_seconds() / 60)
    lines = [(f"{PROVISIONAL_PREFIX}: record for session {day} (rule {row[2]}), from the inputs GRID held at the close "
              f"({iso(session.closes)}); computed {iso(computed)}, {after} min after the close. The session's "
              "decision record is written the next evening (#1191) and nothing that decides reads this one")]
    lines += [f"{PROVISIONAL_PREFIX}: {line}" for line in body]
    facts = {"table": "market_state_provisional", "decision_use": "display_only", "record_date": day,
             "availability_cutoff": iso(session.closes), "computed_at": iso(computed), "minutes_after_close": after,
             "rule": row[2], "label": rec.get("label"), "notable": notable, "coverage": rec.get("coverage") or {}}
    return lines, facts, []


def market_state(engine: Engine, kind: str, session: SessionTimes, as_of: datetime,
                 expected: date | None = None) -> Section:
    """``expected``: the newest session the writer normally has computed by the brief's clock (it writes a session's
    record the evening after it). An older record is a data problem; the expected lag is only stated.

    Post-close, while the session's own decision record is not there yet (it is written the next evening), the
    provisional record computed at the session's close is added for display (#1312): each of its lines starts with
    :data:`PROVISIONAL_PREFIX`, and its facts sit under ``provisional``, apart from the decision record's."""
    title, key = "Market state", "market_state"
    src = "griddb market_state_daily"  # each row names its own rule (market-state/1, /2 since #1303)
    last = session.day - timedelta(days=1) if kind == "pre_open" else session.day
    prov_row, prov_problems = None, []
    try:
        with engine.connect() as conn:
            if conn.dialect.name == "postgresql":
                conn.execute(text("SET TRANSACTION READ ONLY"))
                tz = conn.execute(text("SHOW TimeZone")).scalar()
                if str(tz) not in ("UTC", "Etc/UTC", "GMT"):
                    return Section(key, title, "unknown", src, problems=(f"session TimeZone {tz}; computed_at not readable as UTC",))
            rows = conn.execute(_MS_SQL, {"d": last}).fetchall()
            if kind == "post_close":
                # Last read in the transaction: a failure here leaves the decision record's section as it is.
                try:
                    if inspect(conn).has_table("market_state_provisional"):
                        prov_row = conn.execute(_MSP_SQL, {"d": session.day}).first()
                except Exception as exc:  # noqa: BLE001
                    prov_problems.append(f"provisional record read failed ({type(exc).__name__})")
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",))
    chosen = None
    for r in rows:
        computed = _stored_clock(r[1])
        if computed <= as_of:
            chosen = (r, computed)
            break
    prov_lines, prov_facts = [], None
    if kind == "post_close" and (chosen is None or str(chosen[0][0]) != session.day.isoformat()):
        prov_lines, prov_facts, more = _provisional(prov_row, session, as_of)
        prov_problems += more
    else:
        prov_problems = []                       # the session's decision record is there: nothing provisional is read
    if prov_lines or prov_problems:
        src += "; display-only provisional line from griddb market_state_provisional (#1312)"
    if chosen is None:
        return Section(key, title, "empty", src, None, tuple(prov_lines),
                       {"provisional": prov_facts} if prov_facts else {},
                       ("no record computed before the brief's clock",) + tuple(prov_problems))
    r, computed = chosen
    rec = r[3] if isinstance(r[3], dict) else json.loads(r[3])
    problems = []
    rec_day = str(r[0])
    body, notable = _record_lines(rec)
    lines = [f"record for session {rec_day} (rule {r[2]}), computed {iso(computed)}", *body, *prov_lines]
    if expected is not None and rec_day < expected.isoformat():
        problems.append(f"latest record is for {rec_day}; the writer normally has {expected.isoformat()} by now")
    facts = {"record_date": rec_day, "computed_at": iso(computed), "notable": notable,
             "events": rec.get("events"), "pending": sorted((rec.get("pending") or {}).keys())}
    if prov_facts:
        facts["provisional"] = prov_facts
    return Section(key, title, "ok", src, computed, tuple(lines), facts, tuple(problems + prov_problems))


# ── event calendar (#1293 forward parts) ─────────────────────────────────────

def calendar(session: SessionTimes, as_of: datetime, *, market_events: dict | None = None,
             events=None, xnys=None, horizon: int = 5) -> Section:
    from analysis.factory_session_calendar import (
        EVENT_PINS,
        EVENT_TIME,
        PINS,
        FactoryCalendar,
        FactoryEvents,
    )

    title, key = "Event calendar", "calendar"
    src = "pinned XNYS and factory-events parts (analysis/factory_session_calendar.py)"
    try:
        ev = events or FactoryEvents(pins=EVENT_PINS)
        cal = xnys or FactoryCalendar(pins=PINS)
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"pinned parts did not load ({type(exc).__name__}: {exc})",))
    lines, facts, problems = [], {"days": []}, []
    day = session.day
    for i in range(horizon):
        d = day.isoformat()
        known = ev.known(d, as_of)
        if known is None:
            lines.append(f"{d}: the pinned event parts do not answer for this day at this clock (unknown, not quiet)")
            problems.append(f"no event part answers for {d}")
        else:
            names = sorted(known)
            when = ", ".join(f"{n.upper()} {EVENT_TIME.get(n, '?')} ET" for n in names)
            lines.append(f"{d}{' (this session)' if i == 0 else ''}: {when or 'no FOMC, CPI or NFP'}")
        facts["days"].append({"day": d, "known": None if known is None else sorted(known)})
        try:
            nxt = cal.sessions[cal.index[d] + 1].key.split(":")[1]
        except (KeyError, IndexError):
            problems.append(f"the pinned XNYS parts end at {d}")
            break
        day = date.fromisoformat(nxt)
    lines.append(f"session {session.day}: opens {iso(session.opens)}, closes {iso(session.closes)}")
    if market_events:
        within = market_events.get("within_14d") or []
        listed = ", ".join(f"{e.get('event')} {e.get('date')}" for e in within) or "none"
        lines.append(f"market-state record's release list (FRED dates, no known-at): {listed}")
        facts["market_state_events"] = within
    return Section(key, title, "ok", src, as_of, tuple(lines), facts, tuple(problems))


# ── gex-records/1 (gex-watch Records page) ───────────────────────────────────

def records(url: str, as_of: datetime, *, fetch: Fetch = _fetch, timeout: float = 15.0) -> tuple[Section, list[Trial]]:
    title, key, src = "GEX lane records (gex-records/1)", "records", url
    try:
        d = json.loads(fetch(url, timeout))
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",)), []
    if d.get("schema") != "gex-records/1":
        return Section(key, title, "unknown", src, problems=(f"schema {d.get('schema')!r}, expected gex-records/1",)), []
    clock = parse_clock(d.get("generated_at"))
    trials = []
    for f in d.get("forward") or []:
        have, need = f.get("have"), f.get("need")
        trials.append(Trial(
            trial_id=str(f.get("id")), name=str(f.get("name") or f.get("id")), source=f"records:{f.get('source')}",
            registration=f.get("doc") or f.get("registered_at"), have=have if isinstance(have, int) else None,
            need=need if isinstance(need, int) else None, unit=str(f.get("unit") or ""),
            status=f.get("status"), clock=clock, live=True, daily=False))
    counts = d.get("counts") or {}
    lines = ["forward records by look status: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())),
             f"looks already spent: {len(d.get('spent') or [])} (results are on the Records page, not repeated here)"]
    if (d.get("ledger") or {}).get("chain_ok") is False:
        lines.append("trials ledger hash chain: BROKEN")
    problems = tuple(f"{k}: {v}" for k, v in sorted((d.get("errors") or {}).items()))
    return Section(key, title, "ok" if d.get("status") == "ok" else "unknown", src, clock, tuple(lines),
                   {"counts": counts, "live": True}, problems), trials


# ── shadow ledgers (sqlite, read-only) ───────────────────────────────────────

def _cols(con: sqlite3.Connection, table: str) -> set[str]:
    if not IDENT.match(table):
        raise ValueError(f"bad table name {table!r}")
    if not con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        raise LookupError(f"no table {table}")
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def _select(con: sqlite3.Connection, table: str, columns: list[str]) -> list[sqlite3.Row]:
    have = _cols(con, table)
    for c in columns:
        if not IDENT.match(c) or c not in have:
            raise LookupError(f"{table} has no column {c!r}")
    con.row_factory = sqlite3.Row
    return con.execute(f"SELECT {', '.join(columns)} FROM {table}").fetchall()


def ledger_trial(spec: dict, kind: str, session: SessionTimes, prev_day: date, as_of: datetime) -> Trial:
    """One registered shadow record: the session's row, and the rows that count toward its look.

    spec keys: id, name, db, table, day, clock, status, reason, counts (status values that count), need, unit,
    count_from, registration, decides, decision ("session" or "previous_session"), and optionally count_table /
    count_day / count_clock / count_status when the graded rows live in another table.
    """
    tid, name = spec["id"], spec["name"]
    src = f"{spec['db']}:{spec['table']}"
    base = dict(trial_id=tid, name=name, source=src, registration=spec.get("registration"),
                need=spec.get("need"), unit=spec.get("unit", ""), decides=spec.get("decides"))
    try:
        con = ro_sqlite(spec["db"])
    except Exception as exc:  # noqa: BLE001
        return Trial(have=None, withheld=f"ledger unreadable ({type(exc).__name__})", **base)
    try:
        cols = [spec["day"], spec["clock"], spec["status"]] + ([spec["reason"]] if spec.get("reason") else [])
        rows = _select(con, spec["table"], cols)
        ct = spec.get("count_table", spec["table"])
        cday, cclock, cstat = (spec.get("count_day", spec["day"]), spec.get("count_clock", spec["clock"]),
                               spec.get("count_status", spec["status"]))
        crows = rows if ct == spec["table"] and (cday, cclock, cstat) == (spec["day"], spec["clock"], spec["status"]) \
            else _select(con, ct, [cday, cclock, cstat])
    except Exception as exc:  # noqa: BLE001
        return Trial(have=None, withheld=f"ledger read failed ({type(exc).__name__}: {exc})", **base)
    finally:
        con.close()
    counting = set(spec.get("counts") or [])
    start = spec.get("count_from") or "0000-00-00"
    have, newest, unclocked, late = set(), None, 0, 0
    for r in crows:
        c = parse_clock(r[cclock])
        if c is None:
            unclocked += 1
            continue
        if c > as_of:
            late += 1
            continue
        newest = c if newest is None or c > newest else newest
        if r[cstat] in counting and str(r[cday]) >= start:
            have.add(str(r[cday]))
    want = (prev_day if spec.get("decision") == "previous_session" else session.day).isoformat()
    today = withheld = None
    row_clock = None
    for r in rows:
        if str(r[spec["day"]]) != want:
            continue
        c = parse_clock(r[spec["clock"]])
        if c is None:
            withheld = "the session's row has no clock"
            break
        if c > as_of:
            # Not yet written at the brief's clock. Before the session decides, that is the normal state.
            if not (kind == "pre_open" and spec.get("decision") != "previous_session"):
                withheld = f"not yet recorded at the brief's clock (written {iso(c)})"
            break
        row_clock = c
        st = r[spec["status"]]
        reason = r[spec["reason"]] if spec.get("reason") else None
        label = spec.get("status_words", {}).get(str(st), str(st))
        if st in counting:
            today = f"{label} ({want})" if want != session.day.isoformat() else label
        else:
            withheld = f"{label}: {reason}" if reason else label
        break
    if today is None and withheld is None and not (kind == "pre_open" and spec.get("decision") != "previous_session"):
        withheld = f"no row for {want}"
    note = []
    if unclocked:
        note.append(f"{unclocked} row(s) without a readable clock not counted")
    if note:
        withheld = "; ".join(([withheld] if withheld else []) + note)
    return Trial(have=len(have), today=today, withheld=withheld, clock=row_clock or newest, **base)


def selector(spec: dict, kind: str, session: SessionTimes, as_of: datetime) -> Trial:
    """selector/1: each chooser's choice for the session (no outcomes), and the graded days so far."""
    base = dict(trial_id=spec["id"], name=spec["name"], source=f"{spec['db']}:decisions",
                registration=spec.get("registration"), need=spec.get("need"), unit=spec.get("unit", "graded sessions"),
                decides=spec.get("decides"))
    try:
        con = ro_sqlite(spec["db"])
        dec = _select(con, "decisions", ["day", "chooser", "choice", "decided_at"])
        out = _select(con, "outcomes", ["day", "graded_at"])
        con.close()
    except Exception as exc:  # noqa: BLE001
        return Trial(have=None, withheld=f"ledger unreadable ({type(exc).__name__})", **base)
    graded = {r["day"] for r in out if (c := parse_clock(r["graded_at"])) and c <= as_of}
    day = session.day.isoformat()
    picks = sorted((r["chooser"], r["choice"]) for r in dec
                   if r["day"] == day and (c := parse_clock(r["decided_at"])) and c <= as_of)
    today = "; ".join(f"{ch}: {choice}" for ch, choice in picks) or None
    withheld = None if today or kind == "pre_open" else f"no decision recorded for {day}"
    clocks = [c for r in dec if (c := parse_clock(r["decided_at"])) and c <= as_of]
    return Trial(have=len(graded), today=today, withheld=withheld, clock=max(clocks) if clocks else None, **base)


def plays(spec: dict, kind: str, session: SessionTimes, prev_day: date, as_of: datetime) -> Trial:
    """The overnight plays table: which plays it switched on for the night (names only), and nights graded."""
    base = dict(trial_id=spec["id"], name=spec["name"], source=f"{spec['db']}:nights",
                registration=spec.get("registration"), need=spec.get("need"), unit=spec.get("unit", "nights graded"),
                decides=spec.get("decides"))
    try:
        con = ro_sqlite(spec["db"])
        nights = _select(con, "nights", ["decision_date", "recorded_at", "choices_json"])
        out = _select(con, "outcomes", ["decision_date", "graded_at"])
        con.close()
    except Exception as exc:  # noqa: BLE001
        return Trial(have=None, withheld=f"ledger unreadable ({type(exc).__name__})", **base)
    graded = {r["decision_date"] for r in out if (c := parse_clock(r["graded_at"])) and c <= as_of}
    want = (prev_day if kind == "pre_open" else session.day).isoformat()
    today = withheld = clock = None
    for r in nights:
        if r["decision_date"] != want:
            continue
        c = parse_clock(r["recorded_at"])
        if c is None or c > as_of:
            withheld = f"the {want} night was recorded after the brief's clock"
            break
        clock = c
        try:
            choices = json.loads(r["choices_json"])
        except ValueError:
            withheld = "choices unreadable"
            break
        on = sorted(str(x.get("play")) for x in choices if x.get("on"))
        held = sorted(f"{x.get('play')} ({x.get('held_back')})" for x in choices if not x.get("on") and x.get("held_back"))
        today = f"night of {want}: on " + (", ".join(on) or "none")
        if held:
            withheld = "held back: " + "; ".join(held)
    if today is None and withheld is None:
        withheld = f"no night recorded for {want}"
    return Trial(have=len(graded), today=today, withheld=withheld, clock=clock, **base)


def trader_day(spec: dict, kind: str, session: SessionTimes, as_of: datetime) -> Trial:
    """gex-trader (shadow): the session's day state and shadow intents. Counts toward Stage 0 come from records."""
    base = dict(trial_id=spec["id"], name=spec["name"], source=f"{spec['db']}:day_state",
                registration=spec.get("registration"), need=None, unit="",
                decides=spec.get("decides"), counts_in=spec.get("counts_in", "trader-stage0 (Records)"))
    try:
        con = ro_sqlite(spec["db"])
        ds = _select(con, "day_state", ["day", "mode", "breaker", "entries", "halt", "session_result", "updated_at"])
        it = _select(con, "intents", ["day", "created_at", "mode", "call", "trigger"])
        con.close()
    except Exception as exc:  # noqa: BLE001
        return Trial(have=None, withheld=f"ledger unreadable ({type(exc).__name__})", **base)
    day = session.day.isoformat()
    today = withheld = clock = None
    for r in ds:
        if r["day"] != day:
            continue
        c = parse_clock(r["updated_at"])
        if c is None or c > as_of:
            break
        clock = c
        intents = [f"{x['call']} on {x['trigger']} ({x['mode']})" for x in it
                   if x["day"] == day and (ci := parse_clock(x["created_at"])) and ci <= as_of]
        today = f"{len(intents)} shadow intent(s)" + (": " + "; ".join(intents) if intents else "")
        if r["halt"]:
            withheld = f"halted: {r['halt']}"
        elif r["breaker"] != "CLOSED":
            withheld = f"breaker {r['breaker']}"
    if today is None and kind == "post_close":
        withheld = f"no day state for {day}"
    return Trial(have=None, today=today, withheld=withheld, clock=clock, **base)


# ── factory live slots (panel-factory/1, gex-signals; slot owner: the factory-slot session) ──────────────────

# Slot states as the slot owner defines them (live: collecting .. underpowered; additive drafts gex#26/#29/#32/#38:
# retirement_check_due, lapsed). Only a slot whose record says ``citable`` may be cited; a passed slot that is not
# citable (re-check owed, lapsed, calendar ended) is never shown as a plain pass.
SLOT_STATE_WORDS = {
    "collecting": "collecting",
    "look_due": "look due: the owner runs the one look",
    "underpowered_due": "deadline passed short of its count: the owner's look is owed",
    "pass": "passed its look",
    "fail": "failed its look",
    "underpowered": "underpowered at its look",
    "retirement_check_due": "passed, re-check owed",
    "lapsed": "passed, then lapsed",
}
SLOT_COUNTING = ("collecting", "look_due", "underpowered_due")   # still "unproven, n of N"

_FIRES_SQL = ("SELECT trial_id, decision, z, fired, side, abstain, recorded_at FROM factory_fires WHERE day = ?")
_FEATURES_SQL = ("SELECT eligible, recorded_at FROM factory_feature_values WHERE day = ?")


def factory_slots(panel_path: str, signals_db: str | None, kind: str, session: SessionTimes,
                  as_of: datetime) -> tuple[Section, list[Trial]]:
    """The live slots as gex-signals publishes them: the panel's ``factory`` block (rewritten every 30 s in session,
    so read as live) for registration, state and counts, and ``signals.db`` (``mode=ro``) for each slot's say on the
    session (``factory_fires``: fired, or the abstain reason) and the session's feature values. Only a slot whose
    ``citable`` is true may be cited; every other slot reads "unproven, n of N"."""
    title, key = "Factory live slots (panel-factory/1)", "factory_slots"
    try:
        panel = json.loads(Path(panel_path).read_text())
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", panel_path, problems=(f"panel unreadable ({type(exc).__name__})",)), []
    f = panel.get("factory")
    clock = parse_clock(panel.get("generated_at"))
    if not isinstance(f, dict) or not str(f.get("version") or "").startswith("panel-factory/"):
        return Section(key, title, "unknown", panel_path, clock,
                       problems=("the panel has no panel-factory block",)), []
    fam, feats, slots = f.get("family") or {}, f.get("features") or {}, f.get("slots") or []
    lines, problems = [], []
    if slots:
        lines.append(f"slots registered: {len(slots)} of {fam.get('k')} (family looks used {fam.get('used')})")
    else:
        lines.append(f"no slot registered (0 of {fam.get('k')})")
    lines.append(f"feature history: {feats.get('z_history_have_min')} of {feats.get('z_history_needed')} sessions "
                 "(the fewest at any decision time)")
    status = f.get("status")
    exhausted = status == "calendar_exhausted"
    if exhausted:
        last = (f.get("calendar") or {}).get("last_day")
        lines.append(f"calendar ended {last}: nothing recorded since, nothing citable")
        problems.append(f"the slots' pinned calendar ended {last}; nothing recorded since")
    elif status != "ok":
        problems.append(f"factory block status {status}" + (f": {f['error']}" if f.get("error") else ""))
    fires: dict[str, Any] = {}
    if signals_db:
        day = session.day.isoformat()
        try:
            con = ro_sqlite(signals_db)
            con.row_factory = sqlite3.Row
            rows = con.execute(_FIRES_SQL, (day,)).fetchall()
            fv = con.execute(_FEATURES_SQL, (day,)).fetchall()
            con.close()
        except Exception as exc:  # noqa: BLE001
            rows, fv = [], []
            problems.append(f"slot ledger unreadable ({type(exc).__name__}); per-session says not shown")
        for r in rows:
            c = parse_clock(r["recorded_at"])
            if c is not None and c <= as_of:
                fires[r["trial_id"]] = r
        known = [r for r in fv if (c := parse_clock(r["recorded_at"])) and c <= as_of]
        if known:
            lines.append(f"session {day} feature values recorded: {sum(1 for r in known if r['eligible'])} of "
                         f"{len(known)} eligible")
        elif kind == "post_close":
            problems.append(f"no feature values recorded for {day} by the brief's clock")
    trials = []
    for sl in slots:
        tid = str(sl.get("trial_id"))
        r = fires.get(tid)
        today = withheld = None
        if r is not None:
            if r["fired"]:
                today = f"fired at {r['decision']} (side {r['side']:+d})"
            else:
                withheld = f"no fire at {r['decision']}" + (f": {r['abstain']}" if r["abstain"] else "")
        have, need = sl.get("resolved_fires"), sl.get("min_n")
        state = str(sl.get("state") or "")
        citable = bool(sl.get("citable")) and state == "pass" and not exhausted
        label = None
        if not citable and state not in SLOT_COUNTING:
            label = SLOT_STATE_WORDS.get(state, state or "state not supplied") + "; not citable"
            if sl.get("citable_withheld"):
                label += f" ({sl['citable_withheld']})"
        trials.append(Trial(
            trial_id=tid, name=str(sl.get("name") or tid), source=f"{panel_path}:factory", registration=sl.get("rule"),
            have=have if isinstance(have, int) else None, need=need if isinstance(need, int) else None,
            unit="resolved fires", today=today, withheld=withheld, status=SLOT_STATE_WORDS.get(state, state or None),
            clock=clock, live=True, decides="10:00-15:00 ET decision times", counts_in=None,
            citable=citable, label=label))
    return Section(key, title, "ok", panel_path, clock, tuple(lines),
                   {"slots": len(slots), "k": fam.get("k"), "live": True}, tuple(problems)), trials


# ── desk-state daily files ───────────────────────────────────────────────────

def _latest_file(folder: Path, last: date, clock_key: str, as_of: datetime) -> tuple[dict, datetime] | None:
    for p in sorted(folder.glob("????-??-??.json"), reverse=True):
        if p.stem > last.isoformat():
            continue
        d = json.loads(p.read_text())
        c = parse_clock(d.get(clock_key))
        if c is not None and c <= as_of:
            return d, c
    return None


def desk_health(folder: str, session: SessionTimes, as_of: datetime) -> Section:
    title, key, src = "Desk jobs' health line (desk-health/1)", "desk_health", folder
    try:
        got = _latest_file(Path(folder), session.day, "checked_at", as_of)
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",))
    if got is None:
        return Section(key, title, "empty", src, problems=("no health line checked before the brief's clock",))
    d, c = got
    lines = [f"session {d.get('day')}: {len(d.get('ok') or [])} of {d.get('checks')} checks ok"]
    lines += [f"note: {n}" for n in d.get("notes") or []]
    return Section(key, title, "ok", src, c, tuple(lines), {"day": d.get("day"), "ok": d.get("ok")},
                   tuple(f"{d.get('day')}: {p}" for p in d.get("problems") or []))


def desk_evening(folder: str, kind: str, session: SessionTimes, prev_day: date, as_of: datetime) -> Section:
    title, key, src = "The session in prices (desk-evening/1)", "desk_evening", folder
    last = prev_day if kind == "pre_open" else session.day
    try:
        got = _latest_file(Path(folder), last, "computed_at", as_of)
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",))
    if got is None:
        return Section(key, title, "empty", src, problems=(f"no evening text for {last} before the brief's clock",))
    d, c = got
    body = ((d.get("text") or {}).get("body") or "").splitlines()
    problems = [f"missing at send: {m}" for m in d.get("missing") or []]
    if d.get("day") != last.isoformat():
        problems.append(f"latest evening text is for {d.get('day')}, not {last}")
    return Section(key, title, "ok", src, c, tuple(x for x in body if x.strip()), {"day": d.get("day")},
                   tuple(problems))


# ── live GEX state (withheld or served) ──────────────────────────────────────

def gex_state(url: str, as_of: datetime, *, fetch: Fetch = _fetch, timeout: float = 15.0) -> Section:
    title, key, src = "GEX site state", "gex_state", url
    try:
        d = json.loads(fetch(url, timeout))
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",))
    clock = parse_clock(d.get("served_at"))
    cur, dly = d.get("current_gex") or {}, d.get("delayed_gex") or {}
    lines = [f"current GEX: {cur.get('status')}" + (f" ({cur.get('reason')})" if cur.get("reason") else ""),
             f"delayed GEX: {dly.get('status')}" + (f", {int(dly['age_seconds'] // 60)} min old"
                                                    if isinstance(dly.get("age_seconds"), (int, float)) else "")]
    problems = []
    if cur.get("status") != "ok" and cur.get("reason") not in (None, "scheduled_stop"):
        problems.append(f"current GEX withheld: {cur.get('reason')}")
    return Section(key, title, "ok", src, clock, tuple(lines),
                   {"current": cur.get("status"), "reason": cur.get("reason"), "live": True}, tuple(problems))


# ── Desk source receipts (#1272) ─────────────────────────────────────────────

def receipts(folder: str, prev_day: date, as_of: datetime) -> Section:
    from intelligence.desk_source_receipts import ReceiptLog

    title, key, src = "Desk source receipt (leads, factory ledger, thesis log)", "receipts", folder
    try:
        rec = ReceiptLog(Path(folder)).receipt_for(prev_day)
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", src, problems=(f"read failed ({type(exc).__name__})",))
    if rec is None:
        return Section(key, title, "empty", src, problems=(f"no receipt for session {prev_day}",))
    clock = parse_clock(rec.get("read_finished_at"))
    lines = [f"session {rec.get('session')}, sources read {rec.get('read_started_at')} to {rec.get('read_finished_at')}"]
    problems = []
    for name, entry in sorted((rec.get("sources") or {}).items()):
        st = entry.get("status")
        if st == "read":
            lines.append(f"{name}: read, {entry.get('rows')} row(s)" + (" (research leads; not findings)" if name == "sleuth_leads" else ""))
        else:
            problems.append(f"{name}: {st} ({entry.get('reason') or entry.get('failure')})")
    return Section(key, title, "ok", src, clock, tuple(lines), {"session": rec.get("session")}, tuple(problems))


# ── the paper record (D4, 3pacs/GRID#14) ─────────────────────────────────────

PAPER_SCHEMA = "desk-paper-summary/1"
PAPER_STALE = timedelta(hours=36)
LOOK_WORDS = {"LOOK DUE": "LOOK DUE: the owner runs the registered look once (not a pass)",
              "NO LOOK": "NO LOOK: description, not evidence", "COLLECTING": "COLLECTING", "SPENT": "SPENT"}


def _r(v: Any) -> str:
    return f"{v:+.3f}" if isinstance(v, (int, float)) else "n/a"


def paper_record(path: str | None, kind: str, as_of: datetime | None = None) -> Section:
    """The D4 paper record's summary (desk-paper-summary/1, written by the paper-record job, 3pacs/gex).

    Read format agreed with its owner (side-session note paper-record-read-format-20261121.md): the summary file
    only, never its ledger. The summary has already applied each registration's disclosure rule: ``stats`` is
    null unless the registration allows interim outcomes or the look is SPENT. The brief copies ``stats`` when it is
    non-null and never derives a mean, sign or hit rate from counts. Gross-only stats (before costs) are not shown.
    """
    title, key = "Graded paper calls (R after costs vs the null)", "paper_record"
    if not path:
        return Section(key, title, "not_supplied", "paper record (D4 #14)",
                       problems=("the graded paper record does not exist yet; no call is graded in R here",))
    p = Path(path)
    if not p.is_file():
        return Section(key, title, "not_supplied", path,
                       problems=("the paper record's summary is not written yet; no call is graded in R here",))
    try:
        d = json.loads(p.read_text())
    except Exception as exc:  # noqa: BLE001
        return Section(key, title, "unknown", path, problems=(f"summary unreadable ({type(exc).__name__})",))
    if d.get("schema") != PAPER_SCHEMA:
        return Section(key, title, "unknown", path, problems=(f"schema {d.get('schema')!r}, expected {PAPER_SCHEMA}",))
    if d.get("paper_only") is not True:
        return Section(key, title, "unknown", path, problems=("summary does not declare paper_only; not used",))
    clock = parse_clock(d.get("as_of"))
    problems = [f"{k}: {v} (its modules were not refreshed)" for k, v in sorted((d.get("errors") or {}).items())]
    if clock is not None and as_of is not None and as_of - clock > PAPER_STALE:
        problems.append(f"summary as of {iso(clock)} is older than 36 h")
    lines = [f"grade run {d.get('run_id')}, as of {iso(clock)}"]
    for m in d.get("modules") or []:
        look = m.get("look") or {}
        status = LOOK_WORDS.get(look.get("status"), str(look.get("status")))
        g = m.get("grades") or {}
        need = f" of {look['need']}" if look.get("need") is not None else ""
        line = (f"{m.get('name') or m.get('module_id')}: {status}; {look.get('have')}{need} "
                f"{look.get('unit') or ''}".rstrip() + f"; graded {g.get('graded', 0)}, no trade {g.get('no_trade', 0)}")
        st = m.get("stats")
        if isinstance(st, dict):
            line += (f"; R after costs (registration allows display): n {st.get('n')}, mean {_r(st.get('mean_r'))} "
                     f"vs null {_r(st.get('null_mean_r'))}, excess {_r(st.get('excess_mean_r'))}")
            if look.get("status") == "SPENT" and look.get("result"):
                line += f"; look result {look.get('result')}"
        elif m.get("stats_withheld"):
            line += f"; outcomes withheld ({m['stats_withheld']})"
        lines.append(line)
    desk = d.get("desk") or {}
    if desk:
        looks = ", ".join(f"{k} {v}" for k, v in sorted((desk.get("looks") or {}).items()))
        lines.append(f"desk: {desk.get('modules')} modules, {desk.get('graded')} graded calls; looks {looks or 'none'}")
    return Section(key, title, "ok", path, clock, tuple(lines), {"run_id": d.get("run_id")}, tuple(problems))
