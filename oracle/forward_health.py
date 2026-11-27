"""Did the Oracle forward pass produce a usable forecast this session, and if not, why.

From 2026-11-23 to 2026-11-25 every candidate of the hourly SPY/QQQ forward
pass abstained at creation: 1,830 ``no_data`` rows over three sessions. The
pass reported ``completed``, its attempt was recorded as a success, and the
only human-facing output (the Oracle report e-mail) is sent when there are new
predictions, so the failure showed up as an e-mail that stopped arriving. It
was found four days later by reading the table.

This module is the read side of making that loud:

* :func:`pass_outcome` classifies one pass from what the engine stored, so the
  operator can tell "every ticker abstained" from "the morning's forecasts
  were repeated" (both store zero new qualified rows);
* :func:`forward_health` answers the question for a whole session from the
  durable record (``oracle_iterations`` attempts and ``oracle_predictions``
  rows), for the Oracle page, the daily sweep and anyone who asks.

It reads; it never writes, scores, repairs or regenerates a row. An abstention
stays the honest record of a pass that had no qualified quote.

Who sees what. Predictions are read through
``store.options_provenance.oracle_predictions_from``: an admin request reads
every row, everyone else the public cohort. The sentence that names a withheld
private-use capture (``operator_notes``) is built for admin requests only.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from store.options_provenance import (
    include_schwab_rows, oracle_predictions_from, visible_sql, withheld_capture_note,
)

#: The cohort the Hermes forward pass measures (``scripts/hermes_operator.py``
#: ``ORACLE_FORWARD_TICKERS``; ``tests/test_oracle_forward_health.py`` pins the two together).
FORWARD_TICKERS: tuple[str, ...] = ("SPY", "QQQ")
#: The ``oracle_iterations.notes`` value the forward pass commits before each attempt.
FORWARD_ATTEMPT_NOTE = "oracle_forward_v2:SPY_QQQ:1d:attempt;evolution_held"
FORWARD_RECORDING_POLICY = "oracle_forward_v2"

#: A session younger than this with nothing recorded yet is ``pending``, not a failure:
#: the first pass follows the first Hermes cycle after the open.
PENDING_GRACE = timedelta(minutes=90)
#: How far back "the last real forecast" is looked for. ``oracle_predictions``
#: holds millions of rows; an unbounded MAX() read all of them (2.9 s in
#: production). Bounded, it walks the created_at index back from now.
LAST_FORECAST_LOOKBACK = timedelta(days=90)

STATUS_OK = "ok"                              # every cohort ticker has a forecast
STATUS_PARTIAL = "partial"                    # some do, some do not
STATUS_ALL_ABSTAINED = "all_abstained"        # rows exist and every one is an abstention
STATUS_NO_ROWS = "no_rows"                    # passes ran and stored nothing
STATUS_NO_ATTEMPTS = "no_attempts"            # no pass ran at all
STATUS_PENDING = "pending"                    # session just opened
STATUS_NO_PUBLIC_FORECAST = "no_public_forecast"  # nothing in this viewer's cohort

#: Statuses that mean the session has no usable forecast and somebody should look.
FAILING = frozenset({STATUS_ALL_ABSTAINED, STATUS_NO_ROWS, STATUS_NO_ATTEMPTS})

_NY = ZoneInfo("America/New_York")
_OPEN = time(9, 30)


def pass_outcome(captured: list[str] | tuple[str, ...], detail: Any) -> dict[str, Any]:
    """Classify one forward pass from ``OracleEngine._last_forward_detail``.

    A ticker *has a forecast* when this pass stored a qualified row for it or
    repeated one stored earlier today (a duplicate forecast is this morning's
    forecast, still standing). It *abstained* when all it has is abstentions,
    new or repeated. A captured ticker with neither had no candidate at all.

    ``all_abstained`` is true only when at least one ticker was captured, none
    has a forecast and at least one abstained. Missing detail (an engine that
    predates it) classifies nothing and is never ``all_abstained``.
    """
    tickers = (detail or {}).get("tickers") if isinstance(detail, dict) else None
    tickers = tickers if isinstance(tickers, dict) else {}
    reasons = (detail or {}).get("abstention_reasons") if isinstance(detail, dict) else None
    reasons = reasons if isinstance(reasons, dict) else {}

    def count(name: str, *keys: str) -> int:
        row = tickers.get(name) or {}
        return sum(int(row.get(key) or 0) for key in keys)

    with_forecast = [t for t in captured if count(t, "qualified", "duplicate_forecasts") > 0]
    abstained = [t for t in captured if t not in with_forecast
                 and count(t, "abstained", "duplicate_abstentions") > 0]
    without_candidates = [t for t in captured if t not in with_forecast and t not in abstained]
    ordered = dict(sorted(reasons.items(), key=lambda item: (-int(item[1]), item[0])))
    return {
        "tickers_with_forecast": with_forecast,
        "tickers_abstained": abstained,
        "tickers_without_candidates": without_candidates if tickers else [],
        "abstention_reasons": ordered,
        "all_abstained": bool(captured) and not with_forecast and bool(abstained),
    }


def _aware(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return (value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _iso(value: Any) -> str | None:
    clock = _aware(value)
    return clock.isoformat() if clock is not None else None


def forward_session(now: datetime, *, ended_only: bool = False) -> dict[str, Any]:
    """The New York cash session the forward pass should have measured by ``now``.

    Today's, once it has opened; otherwise the most recent one. With
    ``ended_only`` an open session is skipped for the last one that closed (the
    daily sweep judges whole sessions). Early closes come from the market
    calendar, as in the pass itself.
    """
    from ingestion.market_calendar import is_market_open, market_close_time

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("forward_session needs a timezone-aware clock")
    local = now.astimezone(_NY)
    day = local.date()
    for _ in range(15):
        if is_market_open(day):
            opens = datetime.combine(day, _OPEN, tzinfo=_NY)
            closes = datetime.combine(day, market_close_time(day), tzinfo=_NY)
            if local >= opens and not (ended_only and local < closes):
                return {
                    "session_date": day, "opens_at": opens.astimezone(timezone.utc),
                    "closes_at": closes.astimezone(timezone.utc),
                    "state": "open" if local < closes else "ended",
                }
        day -= timedelta(days=1)
    raise RuntimeError("no trading session found in the last 15 days")


def _message(health: dict[str, Any]) -> str:
    session, status = health["session_date"], health["status"]
    last = health.get("last_qualified_at")
    tail = (f" Last real forecast: {last}." if last
            else f" No real forecast in the {LAST_FORECAST_LOOKBACK.days} days before it.")
    attempts = health["attempts"]
    if status == STATUS_OK:
        return f"Oracle forward pass has a forecast for every cohort ticker in the {session} session."
    if status == STATUS_PARTIAL:
        missing = ", ".join(t for t, row in health["tickers"].items() if not row["qualified"])
        return f"Oracle forward pass has no forecast for {missing} in the {session} session."
    if status == STATUS_ALL_ABSTAINED:
        total = sum(row["abstained"] for row in health["tickers"].values())
        reason = health.get("top_reason") or "no reason recorded"
        return (f"No usable Oracle forecast in the {session} session: all {total} candidates abstained"
                f" over {attempts} attempt(s). Top reason: {reason}.{tail}")
    if status == STATUS_NO_ROWS:
        return (f"No usable Oracle forecast in the {session} session: {attempts} attempt(s) ran and"
                f" stored nothing (no fresh capture, or no model produced a candidate).{tail}")
    if status == STATUS_NO_ATTEMPTS:
        return f"The Oracle forward pass did not run in the {session} session.{tail}"
    if status == STATUS_NO_PUBLIC_FORECAST:
        return f"No Oracle forecast is published for the {session} session."
    return f"The {session} session has just opened; the first Oracle forward pass has not reported yet."


def forward_health(
    conn: Any, *, now: datetime, include_schwab: Any = False, ended_only: bool = False,
    tickers: tuple[str, ...] = FORWARD_TICKERS,
) -> dict[str, Any]:
    """Whether the forward pass produced a usable forecast in the current (or last) session.

    ``include_schwab`` is the viewer flag of ``store.options_provenance``:
    exactly ``True`` reads every prediction and adds ``operator_notes``;
    anything else reads the public cohort and gets no note. ``level`` is
    ``ok``, ``pending``, ``degraded`` (a cohort ticker has no forecast),
    ``failing`` (the session has none) or ``info``.
    """
    session = forward_session(now, ended_only=ended_only)
    admin = include_schwab_rows(include_schwab)
    day: date = session["session_date"]
    start = datetime.combine(day, time(0), tzinfo=_NY).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=_NY).astimezone(timezone.utc)
    window = {"start": start, "end": end}

    attempt_row = conn.execute(text(
        "SELECT COUNT(*), MAX(iteration_at) FROM oracle_iterations"
        " WHERE notes = :note AND iteration_at >= :start AND iteration_at < :end"
    ), {"note": FORWARD_ATTEMPT_NOTE, **window}).fetchone()
    attempts = int(attempt_row[0] or 0) if attempt_row else 0

    source = oracle_predictions_from(include_schwab)
    rows = conn.execute(visible_sql(
        "SELECT ticker, CASE WHEN direction = 'NONE' THEN 1 ELSE 0 END AS abstained, score_notes,"
        " COUNT(*), MIN(created_at) FROM " + source + " oracle_predictions"
        " WHERE recording_policy = :policy AND dedup_keep = TRUE"
        " AND created_at >= :start AND created_at < :end"
        " GROUP BY ticker, CASE WHEN direction = 'NONE' THEN 1 ELSE 0 END, score_notes",
        include_schwab,
    ), {"policy": FORWARD_RECORDING_POLICY, **window}).fetchall()
    last_row = conn.execute(visible_sql(
        "SELECT created_at FROM " + source + " oracle_predictions"
        " WHERE recording_policy = :policy AND dedup_keep = TRUE AND direction <> 'NONE'"
        " AND created_at >= :floor AND created_at < :end ORDER BY created_at DESC LIMIT 1",
        include_schwab,
    ), {"policy": FORWARD_RECORDING_POLICY, "floor": start - LAST_FORECAST_LOOKBACK,
        "end": end}).fetchone()

    per_ticker: dict[str, dict[str, Any]] = {
        name: {"qualified": 0, "abstained": 0, "top_reason": None,
               "first_forecast_at": None, "first_abstention_at": None}
        for name in tickers
    }
    reasons: dict[str, int] = {}
    ticker_reasons: dict[str, dict[str, int]] = {}
    for name, abstained, note, count, first in rows:
        row = per_ticker.setdefault(name, {
            "qualified": 0, "abstained": 0, "top_reason": None,
            "first_forecast_at": None, "first_abstention_at": None,
        })
        first_at = _aware(first)
        key = "first_abstention_at" if abstained else "first_forecast_at"
        if first_at is not None and (row[key] is None or first_at < row[key]):
            row[key] = first_at
        if abstained:
            row["abstained"] += int(count)
            reason = note or "no reason recorded"
            reasons[reason] = reasons.get(reason, 0) + int(count)
            per = ticker_reasons.setdefault(name, {})
            per[reason] = per.get(reason, 0) + int(count)
        else:
            row["qualified"] += int(count)

    def top(counts: dict[str, int]) -> str | None:
        return min(counts, key=lambda reason: (-counts[reason], reason)) if counts else None

    for name, per in ticker_reasons.items():
        per_ticker[name]["top_reason"] = top(per)
    cohort = [per_ticker[name] for name in tickers]
    with_forecast = sum(1 for row in cohort if row["qualified"])
    any_rows = any(row["qualified"] or row["abstained"] for row in per_ticker.values())
    if with_forecast == len(cohort) and cohort:
        status = STATUS_OK
    elif with_forecast:
        status = STATUS_PARTIAL
    elif any(row["abstained"] for row in cohort):
        status = STATUS_ALL_ABSTAINED
    elif not admin and attempts and not any_rows:
        # Passes ran and nothing is in this viewer's cohort: the session's rows,
        # if any, are private-use. Not a failure this viewer can act on.
        status = STATUS_NO_PUBLIC_FORECAST
    elif attempts:
        status = STATUS_NO_ROWS
    elif session["state"] == "open" and _aware(now) - session["opens_at"] < PENDING_GRACE:
        status = STATUS_PENDING
    else:
        status = STATUS_NO_ATTEMPTS

    level = ("ok" if status == STATUS_OK else "pending" if status == STATUS_PENDING
             else "degraded" if status == STATUS_PARTIAL
             else "failing" if status in FAILING else "info")
    health: dict[str, Any] = {
        "status": status, "level": level,
        "session_date": day.isoformat(), "session_state": session["state"],
        "session_opens_at": session["opens_at"].isoformat(),
        "session_closes_at": session["closes_at"].isoformat(),
        "attempts": attempts, "last_attempt_at": _iso(attempt_row[1]) if attempt_row else None,
        "tickers": {
            name: {**row, "first_forecast_at": _iso(row["first_forecast_at"]),
                   "first_abstention_at": _iso(row["first_abstention_at"])}
            for name, row in per_ticker.items()
        },
        "abstention_reasons": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
        "top_reason": top(reasons),
        "last_qualified_at": _iso(last_row[0]) if last_row else None,
        "cohort": "all" if admin else "public",
        "checked_at": _aware(now).isoformat(),
    }
    if admin:
        notes: dict[str, str] = {}
        for name, row in per_ticker.items():
            if row["qualified"] or row["first_abstention_at"] is None:
                continue
            try:
                note = withheld_capture_note(conn, name, as_of=row["first_abstention_at"])
            except Exception:  # noqa: BLE001 - a diagnosis must not fail the health read
                note = None
            if note:
                notes[name] = note
        health["operator_notes"] = notes
    health["message"] = _message(health)
    return health


def alert_text(health: dict[str, Any]) -> tuple[str, str]:
    """Subject and plain body of the operator alert for a failing session."""
    subject = f"Oracle forward pass: no usable forecast ({health['session_date']})"
    lines = [health["message"], "",
             f"status: {health['status']}   attempts: {health['attempts']}"
             f"   last attempt: {health.get('last_attempt_at') or '-'}"]
    for name, row in health["tickers"].items():
        lines.append(f"{name}: {row['qualified']} forecast(s), {row['abstained']} abstention(s)"
                     + (f"; {row['top_reason']}" if row.get("top_reason") else ""))
    for name, note in (health.get("operator_notes") or {}).items():
        lines.append(f"{name}: {note}")
    lines += ["", "Abstentions are the honest record of a pass with no qualified quote."
              " Nothing was repaired or regenerated."]
    return subject, "\n".join(lines)
