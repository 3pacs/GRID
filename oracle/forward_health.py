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
every row, everyone else the public cohort. Three things are for admin
requests only, because they come from tables that are not filtered by cohort:
the attempt count and last attempt time (``oracle_iterations`` records every
pass, private-use ones included), how the last pass ended, the retry plan,
whether the alert was sent, and the sentence about a private-use capture
(``operator_notes``). The public answer
is built from public rows and the session clock alone, so it is the same
whatever happened in the private cohort.

Every read is capped at the instant the caller supplies: asked about 12:00,
it does not see an attempt or a row from 13:00 of the same day.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

from oracle.entry_price_policy import ABSTENTION_KIND_KEY
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

#: After a pass that left the session waiting, the next pass may start this
#: soon instead of an hour later. Each pass is a new decision: its own fresh
#: capture, its own decision clock, its own frozen quote. Nothing of the
#: earlier pass is completed or backdated.
RETRY_INTERVAL = timedelta(minutes=10)
#: Attempts in one session that may use the short interval (the first pass
#: included). Every attempt is a provider request, so the short interval is
#: bounded: after this many the pass is hourly again.
MAX_QUICK_ATTEMPTS = 13
#: No short-interval retry this close to the close. The hourly pass is not
#: affected; it runs to the close as before.
RETRY_CUTOFF_BEFORE_CLOSE = timedelta(minutes=30)
HOURLY_INTERVAL = timedelta(hours=1)
#: ``oracle_iterations.notes`` prefix of the row a pass writes when it ends.
#: The note is ``<prefix><status>|attempt=<the attempt row's own
#: iteration_at>|<TICKER>=<outcome>|...``: how the pass ended, which attempt
#: it belongs to, and what each cohort ticker ended that pass with. Stored
#: predictions cannot say this. They accumulate over the session, so an
#: abstention from 14:31 is still there after a 14:41 pass in which the quote
#: qualified and no model voted; and a ticker with no row at all may have had
#: no capture or no candidate. Only the latest pass's own account tells them
#: apart, and only the account of the newest attempt is read (:func:`outcome_note`,
#: :func:`parse_outcome_note`).
FORWARD_OUTCOME_NOTE_PREFIX = "oracle_forward_v2:SPY_QQQ:1d:outcome:"
OUTCOME_NO_CAPTURE = "no_capture"

#: What one ticker ended one pass with.
TICKER_FORECAST = "forecast"                # holds a forecast (stored now or repeated)
TICKER_NO_QUOTE = "no_quote"                # abstained for want of a qualified quote
TICKER_ABSTAINED_OTHER = "abstained_other"  # abstained for another reason
TICKER_NO_VOTE = "no_vote"                  # captured, and no model produced a candidate
TICKER_NO_CAPTURE = "no_capture"            # the provider gave no capture for it
#: The two a fresh capture could change.
TICKER_RETRYABLE = frozenset({TICKER_NO_QUOTE, TICKER_NO_CAPTURE})

STATUS_OK = "ok"                              # every cohort ticker has a forecast
STATUS_PARTIAL = "partial"                    # some do, some do not
STATUS_ALL_ABSTAINED = "all_abstained"        # rows exist and every one is an abstention
STATUS_NO_ROWS = "no_rows"                    # passes ran and stored nothing (admin only)
STATUS_NO_ATTEMPTS = "no_attempts"            # no pass ran at all (admin only)
STATUS_PENDING = "pending"                    # session just opened
STATUS_NO_PUBLIC_FORECAST = "no_public_forecast"  # nothing in the public cohort

#: Statuses that mean the session has no usable forecast and somebody should look.
FAILING = frozenset({STATUS_ALL_ABSTAINED, STATUS_NO_ROWS, STATUS_NO_ATTEMPTS})

#: ``oracle_iterations.notes`` of the row that records "the operator was told
#: about this session" (``oracle.forward_alert``). One per session, whoever
#: sent it: the Hermes operator or the daily sweep.
FORWARD_ALERT_NOTE_PREFIX = "oracle_forward_v2:SPY_QQQ:1d:alert_sent:"

_NY = ZoneInfo("America/New_York")
_OPEN = time(9, 30)


def alert_note(session_date: date | str) -> str:
    """The marker note for one session's alert."""
    day = session_date.isoformat() if isinstance(session_date, date) else str(session_date)
    return FORWARD_ALERT_NOTE_PREFIX + day


def pass_outcome(captured: list[str] | tuple[str, ...], detail: Any) -> dict[str, Any]:
    """Classify one forward pass from ``OracleEngine._last_forward_detail``.

    A ticker *has a forecast* when this pass stored a qualified row for it or
    repeated one stored earlier today (a duplicate forecast is this morning's
    forecast, still standing). It *abstained* when all it has is abstentions,
    new or repeated. A captured ticker with neither had no candidate at all.

    With at least one ticker captured and none holding a forecast, the pass is
    ``all_abstained`` when some ticker abstained and ``no_result`` when none
    even had a candidate (no model voted, or no model is admitted). Both mean
    the pass produced nothing usable; a pass that only repeated this morning's
    forecasts is neither. Missing detail (an engine that predates it)
    classifies nothing: it is never ``all_abstained`` or ``no_result``.
    """
    tickers = (detail or {}).get("tickers") if isinstance(detail, dict) else None
    known = isinstance(tickers, dict)
    tickers = tickers if known else {}
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
    # What each captured ticker ended this pass with (retry_plan reads the
    # stored copy). An abstention counts as "no quote" only when every
    # abstention of the ticker in this pass lacked one.
    ticker_outcomes: dict[str, str] = {}
    if known:
        for name in captured:
            if name in with_forecast:
                ticker_outcomes[name] = TICKER_FORECAST
            elif name in abstained:
                lacking = count(name, "without_quote")
                total = count(name, "abstained", "duplicate_abstentions")
                ticker_outcomes[name] = TICKER_NO_QUOTE if lacking and lacking == total else TICKER_ABSTAINED_OTHER
            else:
                ticker_outcomes[name] = TICKER_NO_VOTE
    return {
        "ticker_outcomes": ticker_outcomes,
        "tickers_with_forecast": with_forecast,
        "tickers_abstained": abstained,
        "tickers_without_candidates": without_candidates if known else [],
        "abstention_reasons": ordered,
        "no_result": known and bool(captured) and not with_forecast and not abstained,
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


def outcome_note(status: str, attempt_at: Any, ticker_outcomes: dict[str, str]) -> str:
    """The note of a pass's outcome row. ``attempt_at`` is that pass's own attempt row's clock."""
    parts = [FORWARD_OUTCOME_NOTE_PREFIX + status, "attempt=" + (_iso(attempt_at) or "")]
    parts += [f"{name}={outcome}" for name, outcome in sorted(ticker_outcomes.items())]
    return "|".join(parts)


def parse_outcome_note(note: Any) -> dict[str, Any] | None:
    """``{"status", "attempt", "tickers"}`` from an outcome note, or None when it is not one."""
    if not isinstance(note, str) or not note.startswith(FORWARD_OUTCOME_NOTE_PREFIX):
        return None
    status, *fields = note[len(FORWARD_OUTCOME_NOTE_PREFIX):].split("|")
    parsed: dict[str, Any] = {"status": status, "attempt": None, "tickers": {}}
    for field in fields:
        key, _, value = field.partition("=")
        if key == "attempt":
            parsed["attempt"] = value or None
        elif key and value:
            parsed["tickers"][key] = value
    return parsed


def retry_plan(health: dict[str, Any], now: datetime, *, blocked: Any = ()) -> dict[str, Any]:
    """How soon the next forward pass may start, judged from the durable record. Admin answers only.

    Judged on **what each cohort ticker ended the newest finished pass with**
    (``health["ticker_outcomes"]``: that pass's own stored account, tied to
    its attempt), never on the rows the session has accumulated. A ticker
    *waits* when a fresh capture could give it what it lacks:

    * ``no_quote``: it abstained for want of a qualified quote; a later one
      may qualify;
    * ``no_capture``: the provider gave no capture; it may ten minutes on.

    It does not wait after ``forecast``, after ``no_vote`` (the quote
    qualified and no model produced a candidate) or after an abstention of
    another kind. So a ticker that abstained for its quote at 14:31 and at
    14:41 got a qualified quote and no vote is done waiting, although its
    14:31 abstention rows are still there and it still has no forecast.

    While any ticker waits, the next pass may start after ``RETRY_INTERVAL``
    instead of an hour, until the wait ends, the attempt cap, or the cut-off
    before the close. When the newest attempt has no stored account (it timed
    out, or is still running) nothing is known to be waiting: hourly.

    ``blocked`` names tickers whose newest capture is a private-use one
    (:func:`store.options_provenance.withheld_capture_note`). If the Oracle
    cannot read that one it cannot read the next either, so when every ticker
    that waits does so for a quote and is blocked, the pass stays hourly.

    Read from stored rows only, so a restarted operator reaches the same
    answer. ``quick`` says whether the short interval applies.
    """
    clock = _aware(now)
    closes = _aware(health.get("session_closes_at"))
    cutoff = closes - RETRY_CUTOFF_BEFORE_CLOSE if closes is not None else None
    outcomes = health.get("ticker_outcomes") or {}
    cohort = list(health.get("tickers") or {})
    waiting_for_quote = [name for name in cohort if outcomes.get(name) == TICKER_NO_QUOTE]
    waiting_for_capture = [name for name in cohort if outcomes.get(name) == TICKER_NO_CAPTURE]
    waiting_tickers = [name for name in cohort if outcomes.get(name) in TICKER_RETRYABLE]
    attempts = int(health.get("attempts") or 0)
    if not waiting_tickers:
        why = "not_waiting"
    elif not waiting_for_capture and all(name in blocked for name in waiting_for_quote):
        why = "newest_capture_is_private_use"
    elif health.get("session_state") != "open":
        why = "session_ended"
    elif attempts >= MAX_QUICK_ATTEMPTS:
        why = "attempt_cap_reached"
    elif cutoff is None or clock is None or clock >= cutoff:
        why = "past_retry_cutoff"
    else:
        why = "waiting_for_a_qualified_quote" if waiting_for_quote else "waiting_for_a_fresh_capture"
    quick = why.startswith("waiting_for")
    return {
        "quick": quick, "reason": why,
        "interval_seconds": int((RETRY_INTERVAL if quick else HOURLY_INTERVAL).total_seconds()),
        "waiting_tickers": waiting_tickers,
        "attempts": attempts, "max_quick_attempts": MAX_QUICK_ATTEMPTS,
        "retry_cutoff_at": cutoff.isoformat() if cutoff is not None else None,
    }


def _message(health: dict[str, Any]) -> str:
    session, status = health["session_date"], health["status"]
    last = health.get("last_qualified_at")
    tail = (f" Last real forecast: {last}." if last
            else f" No real forecast in the {LAST_FORECAST_LOOKBACK.days} days before it.")
    # Attempt counts are in an admin answer only.
    attempts = health.get("attempts")
    over = f" over {attempts} attempt(s)" if attempts is not None else ""
    retry = health.get("retry") or {}
    if retry.get("quick"):
        tail += (f" Trying again about every {retry['interval_seconds'] // 60} minutes with a fresh"
                 f" capture until a quote qualifies, {retry['max_quick_attempts']} attempts have been"
                 f" made, or {retry['retry_cutoff_at']}.")
    if status == STATUS_OK:
        return f"Oracle forward pass has a forecast for every cohort ticker in the {session} session."
    if status == STATUS_PARTIAL:
        missing = ", ".join(t for t, row in health["tickers"].items() if not row["qualified"])
        return f"Oracle forward pass has no forecast for {missing} in the {session} session."
    if status == STATUS_ALL_ABSTAINED:
        total = sum(row["abstained"] for row in health["tickers"].values())
        reason = health.get("top_reason") or "no reason recorded"
        return (f"No usable Oracle forecast in the {session} session: all {total} candidates abstained"
                f"{over}. Top reason: {reason}.{tail}")
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
    exactly ``True`` reads every prediction and adds the admin-only fields
    (``attempts``, ``first_attempt_at``, ``last_attempt_at``, ``last_outcome``,
    ``ticker_outcomes``, ``alert_sent``, ``operator_notes``, ``retry``); anything else reads the
    public cohort and gets none of them. ``level`` is ``ok``, ``pending``,
    ``degraded`` (a cohort ticker has no forecast), ``retrying`` (none yet, and
    the next pass is minutes away: :func:`retry_plan`; admin answers only),
    ``failing`` (the session has none) or ``info``.

    Nothing later than ``now`` is read, on any path.
    """
    session = forward_session(now, ended_only=ended_only)
    admin = include_schwab_rows(include_schwab)
    day: date = session["session_date"]
    start = datetime.combine(day, time(0), tzinfo=_NY).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=_NY).astimezone(timezone.utc)
    # The session's New York day, and never past the supplied instant.
    window = {"start": start, "end": end, "now": _aware(now)}

    attempts = 0
    attempt_row = None
    alert_sent = False
    last_outcome = None
    ticker_outcomes: dict[str, str] = {}
    if admin:
        # oracle_iterations is not cohort-filtered: every pass is recorded,
        # private-use ones included. Read for admin requests only.
        attempt_row = conn.execute(text(
            "SELECT COUNT(*), MAX(iteration_at), MIN(iteration_at) FROM oracle_iterations WHERE notes = :note"
            " AND iteration_at >= :start AND iteration_at < :end AND iteration_at <= :now"
        ), {"note": FORWARD_ATTEMPT_NOTE, **window}).fetchone()
        attempts = int(attempt_row[0] or 0) if attempt_row else 0
        alert_sent = conn.execute(text(
            "SELECT 1 FROM oracle_iterations WHERE notes = :note AND iteration_at <= :now LIMIT 1"
        ), {"note": alert_note(day), "now": window["now"]}).fetchone() is not None
        # The newest attempt's own account of how it ended, matched by the
        # attempt clock written into the note. No account for the newest
        # attempt (it timed out, or is still running) means none is used: an
        # earlier pass's account says nothing about this one.
        newest_attempt = _iso(attempt_row[1]) if attempt_row else None
        if newest_attempt is not None:
            for (note,) in conn.execute(text(
                "SELECT notes FROM oracle_iterations WHERE notes LIKE :prefix"
                " AND iteration_at >= :start AND iteration_at < :end AND iteration_at <= :now"
                " ORDER BY iteration_at DESC LIMIT 20"
            ), {"prefix": FORWARD_OUTCOME_NOTE_PREFIX + "%", **window}).fetchall():
                parsed = parse_outcome_note(note)
                if parsed is not None and parsed["attempt"] == newest_attempt:
                    last_outcome, ticker_outcomes = parsed["status"], parsed["tickers"]
                    break

    source = oracle_predictions_from(include_schwab)
    rows = conn.execute(visible_sql(
        "SELECT ticker, CASE WHEN direction = 'NONE' THEN 1 ELSE 0 END AS abstained, score_notes,"
        " COUNT(*), MIN(created_at), flow_context ->> :kind_key AS kind"
        " FROM " + source + " oracle_predictions"
        " WHERE recording_policy = :policy AND dedup_keep = TRUE"
        " AND created_at >= :start AND created_at < :end AND created_at <= :now"
        " GROUP BY ticker, CASE WHEN direction = 'NONE' THEN 1 ELSE 0 END, score_notes,"
        " flow_context ->> :kind_key",
        include_schwab,
    ), {"policy": FORWARD_RECORDING_POLICY, "kind_key": ABSTENTION_KIND_KEY, **window}).fetchall()
    last_row = conn.execute(visible_sql(
        "SELECT created_at FROM " + source + " oracle_predictions"
        " WHERE recording_policy = :policy AND dedup_keep = TRUE AND direction <> 'NONE'"
        " AND created_at >= :floor AND created_at < :end AND created_at <= :now"
        " ORDER BY created_at DESC LIMIT 1",
        include_schwab,
    ), {"policy": FORWARD_RECORDING_POLICY, "floor": start - LAST_FORECAST_LOOKBACK,
        "end": end, "now": window["now"]}).fetchone()

    per_ticker: dict[str, dict[str, Any]] = {
        name: {"qualified": 0, "abstained": 0, "top_reason": None,
               "first_forecast_at": None, "first_abstention_at": None}
        for name in tickers
    }
    reasons: dict[str, int] = {}
    ticker_reasons: dict[str, dict[str, int]] = {}
    kinds: dict[str, int] = {}             # every abstention by the kind its writer gave it
    for name, abstained, note, count, first, kind in rows:
        if abstained:
            kinds[kind or "unspecified"] = kinds.get(kind or "unspecified", 0) + int(count)
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
    just_opened = session["state"] == "open" and _aware(now) - session["opens_at"] < PENDING_GRACE
    if with_forecast == len(cohort) and cohort:
        status = STATUS_OK
    elif with_forecast:
        status = STATUS_PARTIAL
    elif any(row["abstained"] for row in cohort):
        status = STATUS_ALL_ABSTAINED
    elif not admin:
        # Nothing in the public cohort. Whether passes ran, and whether the
        # session's rows are private-use, is not this viewer's to know: the
        # answer depends on the clock alone.
        status = STATUS_PENDING if just_opened else STATUS_NO_PUBLIC_FORECAST
    elif attempts:
        status = STATUS_NO_ROWS
    elif just_opened:
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
        "tickers": {
            name: {**row, "first_forecast_at": _iso(row["first_forecast_at"]),
                   "first_abstention_at": _iso(row["first_abstention_at"])}
            for name, row in per_ticker.items()
        },
        "abstention_reasons": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
        "top_reason": top(reasons),
        # Why the session's abstentions abstained, by the kind on each row
        # (``no_qualified_quote`` from the engine; "unspecified" for rows
        # written before kinds existed). The stored reasons above say it in words.
        "abstention_kinds": dict(sorted(kinds.items(), key=lambda item: (-item[1], item[0]))),
        "last_qualified_at": _iso(last_row[0]) if last_row else None,
        "cohort": "all" if admin else "public",
        "checked_at": _aware(now).isoformat(),
    }
    if admin:
        health["attempts"] = attempts
        health["last_attempt_at"] = _iso(attempt_row[1]) if attempt_row else None
        health["first_attempt_at"] = _iso(attempt_row[2]) if attempt_row else None
        health["alert_sent"] = alert_sent
        health["last_outcome"] = last_outcome
        health["ticker_outcomes"] = ticker_outcomes
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
        # The cadence is the operator's: it depends on attempts, outcomes and
        # private-use captures, none of which a non-admin answer may show.
        health["retry"] = retry_plan(health, now, blocked=notes)
        if level == "failing" and health["retry"]["quick"]:
            # Still no forecast, and the session is not over: the next pass is minutes away.
            health["level"] = "retrying"
    health["message"] = _message(health)
    return health


def local_report(health: dict[str, Any], *, with_notes: bool = False) -> str:
    """The detailed account of a session, for the host only. Never e-mailed.

    Two uses: the sweep's alert file (a file in the service user's own
    directory) and an operator's terminal. It is built from an admin answer:
    counts, per-ticker stored reasons and the admin message, all of which
    include the private-use cohort. The e-mail is
    ``oracle.forward_alert.outbound_alert`` instead, which carries none of it.

    ``with_notes`` adds the ``operator_notes`` (when a private-use capture
    completed). That is for the terminal: the alert file is written without it.
    """
    lines = [health["message"], "",
             f"status: {health['status']}   attempts: {health.get('attempts', '-')}"
             f"   last attempt: {health.get('last_attempt_at') or '-'}"]
    for name, row in health["tickers"].items():
        lines.append(f"{name}: {row['qualified']} forecast(s), {row['abstained']} abstention(s)"
                     + (f"; {row['top_reason']}" if row.get("top_reason") else ""))
    if with_notes:
        for name, note in (health.get("operator_notes") or {}).items():
            lines.append(f"{name}: {note}")
    else:
        lines += ["", "Possible causes, where the record shows one: run"
                  " scripts/oracle_forward_health_check.py --notes on this host."]
    lines += ["", "Abstentions are the honest record of a pass with no qualified quote."
              " Nothing was repaired or regenerated."]
    return "\n".join(lines)
