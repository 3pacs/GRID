#!/usr/bin/env python3
"""
Score Oracle Trades — fix directions, score expired predictions.

Steps:
1. Load settled expiry closes from raw_series through store/observations.py
   (EXPIRY_CLOSE_RULE: SUCCESS rows, latest vintage, raw close, nothing dated
   after the expiry; weekend/holiday expiry = previous trading close)
2. Count (never backfill) missing/invalid entry_price -- HISTORICAL-WRITE
   HOLD: a non-null 0/negative entry_price is a legacy row and is left
   exactly as it is; a NULL entry_price is a new-policy row and is never
   fabricated with a price discovered after the fact.
3. Map BULLISH→CALL, NEUTRAL→no_data verdict
4. Score expired predictions (expiry <= today)  ← CHUNKED since 2026-05-13
5. Print scorecard

Step 4 (the only one that touches large row counts) runs in chunks of
``--chunk-size`` rows, each chunk in its own short transaction. This
prevents the single-transaction lock-and-OOM failure mode that would
otherwise hit when scoring large expiry waves (e.g. the 2026-05-15
window where ~2.27M predictions expire at once).

``--rescore-no-data`` is a separate mode (Steps 1-5 do not run): it
re-prices the rows the pre-2026-11 bug closed as "No price data at expiry"
under EXPIRY_CLOSE_RULE and reports a projection. Dry run unless ``--apply``;
with ``--apply`` it only appends to ``oracle_prediction_rescores`` (marked
exploratory hindsight, never official) and never updates oracle_predictions.
"""

import argparse
import importlib.util
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger as log
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# Every first-party (repo-root) import this module needs -- directly or,
# like intelligence.postmortem below, lazily inside a function -- must be
# resolvable BEFORE the sys.path insertion further down ever has a chance
# to run. See that insertion's own comment for why, and
# tests/test_score_oracle_trades_stale_repo_shadow.py for the regression
# proof (it also covers db and intelligence.postmortem, which this module
# does not import itself but which must be equally unaffected by anything
# this import does to sys.path).
from config import settings
from ingestion.market_calendar import is_market_open
from oracle.entry_price_policy import (
    NULL_WRITE_POLICY,
    SCORE_NOTE_ENTRY_NULL,
    entry_price_score_note,
)
from store.observations import (
    Observation,
    SplitEvent,
    price_in_basis,
    read_splits,
    read_window,
)

# Compute-node fallback for a genuinely standalone
# `python scripts/score_oracle_trades.py` invocation whose CWD/PYTHONPATH
# does not already make this repo importable. Two guards, both required:
#
# * `__name__ == "__main__"` -- this must NEVER run when the module is
#   merely imported (Hermes's oracle step does not import this module, but
#   anything that ever does -- directly or transitively -- must not have
#   sys.path mutated as a side effect). The imports above already
#   succeeded by this point however this module ended up loaded, so this
#   line can only ever add a path, never fix a failure that already
#   happened.
# * `importlib.util.find_spec("config") is None` -- even under direct
#   execution, only insert the fallback if this repo is not ALREADY
#   importable (e.g. via PYTHONPATH or CWD). Preferring whatever already
#   resolves correctly over a hardcoded, potentially stale path is strictly
#   safer, and `find_spec` never imports/executes `config` -- it only
#   locates it.
#
# /data/grid_v4/grid_repo is a STALE checkout confirmed on grid-svr
# (revision 5facbdf0, no oracle/entry_price_policy.py) and on the gridz4
# PostgreSQL-proof host. The previous, unconditional
# `sys.path.insert(0, "/data/grid_v4/grid_repo")` ran on every import of
# this module, on every host where that directory exists, for the rest of
# the process's lifetime -- shadowing this repo's real `oracle`, `config`
# and any other first-party package for every import that happened to run
# after it, not just the first one (reproduced: ModuleNotFoundError on
# oracle.entry_price_policy during pytest collection, and separately on
# scripts/score_oracle_trades.py's own real scorer-path PostgreSQL proof).
if __name__ == "__main__" and importlib.util.find_spec("config") is None:
    sys.path.insert(0, "/data/grid_v4/grid_repo")

# Ticker → yfinance-style symbol used in the ``YF:{symbol}:close`` series id.
YF_MAP = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "DXY": "DX-Y.NYB",
    "BRK-B": "BRK-B",
}

def get_yf_symbol(ticker: str) -> str:
    return YF_MAP.get(ticker, ticker)


# ── Expiry close lookup ─────────────────────────────────────────────────────
#
# Root cause this replaces (measured on griddb 2026-11-06, read only): every
# one of the 15,792 dedup_keep rows noted "No price data at expiry" was
# stamped at 00:00-01:59 UTC *on its own expiry date* (scored_at::date -
# expiry = 0 for all of them). The old get_price_for_date() returned None for
# "the expiry-day close does not exist yet" (its same-day guard, added after
# 2026-05-15) and score_one_chunk() treated that None as final and wrote
# verdict='no_data'. So every row whose expiry fell after 05-15 was closed as
# unscorable hours before the market even opened on its expiry day, and a
# weekend expiry (no close ever exists on that date) could never price at all.
# The closes were there: raw_series held TIINGO/yfinance closes for the same
# tickers and dates (e.g. SPY 2026-11-03 754.35, pulled 23:35Z that day).
#
# The rule below (EXPIRY_CLOSE_RULE) is what both the live scorer and the
# hindsight rescore use:
#
# * Prices come only from ``raw_series`` through ``store/observations.py``:
#   SUCCESS rows, the latest vintage per obs_date, ``YF:{symbol}:close`` (raw,
#   unadjusted -- the same basis as entry_price, which oracle/engine.py takes
#   from options_daily_signals.spot_price), one source per price
#   (PRICE_SOURCES order: TIINGO, then yfinance).
# * A close counts only once its session is settled: the vintage must have
#   been pulled at or after 17:00 America/New_York on its obs_date (crypto: the
#   end of the UTC day). An intraday capture is never a close.
# * Trading-day expiry: the close dated exactly on the expiry date. No
#   earlier close stands in for it.
# * Non-trading expiry (weekend / NYSE holiday): the close of the last trading
#   day BEFORE the expiry -- the last price that existed at the expiry
#   instant. Never the next trading day's close: that is data from after
#   expiry (lookahead).
# * Nothing dated after the expiry is ever read (``as_of=expiry`` bound).
# * A prediction created at or after that session's 16:00 ET close (e.g.
#   created Saturday, expiring Sunday) saw no session at all; it is no_data
#   with NOTE_NO_SESSION rather than scored against a price it already knew.
# * A missing close leaves the row 'pending' until EXPIRY_CLOSE_GRACE_DAYS
#   after the session settled; only then is it closed as no_data.

EXPIRY_CLOSE_RULE = "oracle_expiry_close_v2"
PRICE_SOURCES: tuple[str, ...] = ("TIINGO", "yfinance")
EXPIRY_CLOSE_GRACE_DAYS = 4
NOTE_NO_PRICE = "No price data at expiry"
NOTE_NO_SESSION = "No trading session closed between creation and expiry"
_NY = ZoneInfo("America/New_York")
_REGULAR_CLOSE_NY = time(16, 0)
_SETTLED_NY = time(17, 0)
_CRYPTO_SYMBOLS = frozenset({"BTC-USD", "ETH-USD", "SOL-USD"})
# Longest US equity closure is 4 calendar days (e.g. Good Friday weekend
# plus margin); a gap longer than this is missing data, not a holiday.
_MAX_SESSION_LOOKBACK_DAYS = 7

PriceBook = dict[str, dict[date, Observation]]
SplitBook = dict[str, list[SplitEvent]]

# ── Stock splits (W2-10) ────────────────────────────────────────────────────
#
# entry_price is the as-traded spot when the prediction was made; the expiry
# close is the as-traded print on the session date. A split between the two
# (NVDA 10-for-1 on 2024-06-10) made a flat stock score as -90% and a 1-for-10
# reverse split as +900%. The expiry close is therefore put on the entry's
# share basis with store.observations.price_in_basis before the verdict: only
# splits dated after the creation date and on/before the session date are
# applied (never a split that had not happened by the expiry), and a yfinance
# vintage pulled after a later split is first restored to as-traded.
# SPLIT_RULE is stamped into score_notes whenever a factor != 1 was applied.
SPLIT_RULE = "split_adjusted_v1"


def close_series_id(ticker: str) -> str:
    """The raw (unadjusted) close series for ``ticker``. Never ``adj_close``."""
    return f"YF:{get_yf_symbol(ticker)}:close"


def _is_crypto(ticker: str) -> bool:
    return get_yf_symbol(ticker) in _CRYPTO_SYMBOLS


def is_trading_day(ticker: str, d: date) -> bool:
    return True if _is_crypto(ticker) else is_market_open(d)


def expiry_session_date(ticker: str, expiry: date) -> date | None:
    """The session whose close is the price at ``expiry`` (never after it)."""
    for back in range(_MAX_SESSION_LOOKBACK_DAYS + 1):
        d = expiry - timedelta(days=back)
        if is_trading_day(ticker, d):
            return d
    return None


def session_close_utc(ticker: str, d: date) -> datetime:
    """The instant session ``d`` stops trading (16:00 ET; crypto: UTC day end)."""
    if _is_crypto(ticker):
        return datetime.combine(d + timedelta(days=1), time.min, timezone.utc)
    return datetime.combine(d, _REGULAR_CLOSE_NY, _NY).astimezone(timezone.utc)


def session_settled_utc(ticker: str, d: date) -> datetime:
    """Earliest pull time at which a vintage of ``d``'s close is accepted."""
    if _is_crypto(ticker):
        return session_close_utc(ticker, d)
    return datetime.combine(d, _SETTLED_NY, _NY).astimezone(timezone.utc)


def _as_utc(ts: Any) -> datetime | None:
    """psycopg2 hands back aware datetimes; SQLite fixtures hand back text."""
    if ts is None:
        return None
    if not isinstance(ts, datetime):
        try:
            ts = datetime.fromisoformat(str(ts))
        except ValueError:
            return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _as_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def load_expiry_closes(
    conn: Any,
    tickers: list[str],
    start: date,
    end: date,
    *,
    as_of_ts: datetime | None = None,
) -> PriceBook:
    """Settled raw closes per ticker and obs_date in ``[start, end]``.

    One bounded ``read_window`` per (ticker, source): SUCCESS only, latest
    vintage per obs_date, never an obs_date after ``end``. For each date the
    first source in PRICE_SOURCES whose latest vintage was pulled after the
    session settled wins; sources are never blended within one price.
    """
    book: PriceBook = {}
    for ticker in sorted(set(tickers)):
        sid = close_series_id(ticker)
        chosen: dict[date, Observation] = {}
        for source in PRICE_SOURCES:
            for obs in read_window(
                conn, sid, source=source, start=start, as_of=end, as_of_ts=as_of_ts,
            ):
                if obs.obs_date in chosen or not (obs.value > 0):
                    continue
                pulled = _as_utc(obs.pull_timestamp)
                if pulled is None or pulled < session_settled_utc(ticker, obs.obs_date):
                    continue  # intraday/unsettled capture: not a close
                chosen[obs.obs_date] = obs
        if chosen:
            book[ticker] = chosen
    return book


def load_split_book(
    conn: Any, tickers: list[str], *, as_of_ts: datetime | None = None,
) -> SplitBook:
    """Recorded splits per ticker (``YF:{symbol}:split_factor``), as known at ``as_of_ts``."""
    book: SplitBook = {}
    for ticker in sorted(set(tickers)):
        if _is_crypto(ticker):
            continue
        events = read_splits(conn, get_yf_symbol(ticker), as_of_ts=as_of_ts)
        if events:
            book[ticker] = events
    return book


def _created_date(created_at: Any) -> date | None:
    created = _as_utc(created_at)
    if created is None:
        return None
    return created.astimezone(_NY).date()


def split_adjusted_close(
    splits: SplitBook | None, ticker: str, price: float, session: date, *,
    created_at: Any, source: str | None = None, pull_timestamp: Any = None,
) -> tuple[float, float]:
    """``(price on the entry's share basis, factor applied)`` for one expiry close.

    ``factor`` is ``adjusted / price`` (1.0 when no recorded split falls in
    ``(creation date, session]`` and the vintage needs no basis repair).
    """
    events = (splits or {}).get(ticker)
    entry_day = _created_date(created_at)
    if not events or entry_day is None or price <= 0:
        return price, 1.0
    adjusted = price_in_basis(
        price, session, basis_date=entry_day, splits=events,
        source=source, pull_timestamp=_as_utc(pull_timestamp),
    )
    return adjusted, adjusted / price


@dataclass(frozen=True)
class ExpiryClose:
    """Outcome of the expiry-close lookup for one prediction.

    ``status`` is ``priced`` (price + provenance set), ``pending`` (the close
    may still arrive: write nothing) or ``no_data`` (final, ``note`` says why).
    """

    status: str
    price: float | None = None
    session_date: date | None = None
    source: str | None = None
    pull_timestamp: datetime | None = None
    note: str | None = None


def resolve_expiry_close(
    book: PriceBook,
    ticker: str,
    expiry: Any,
    *,
    created_at: Any,
    now: datetime,
) -> ExpiryClose:
    """Apply EXPIRY_CLOSE_RULE (see the block comment above) to one prediction."""
    expiry_d = _as_date(expiry)
    session = expiry_session_date(ticker, expiry_d)
    if session is None:
        return ExpiryClose("no_data", note=NOTE_NO_PRICE)
    created = _as_utc(created_at)
    if created is not None and created >= session_close_utc(ticker, session):
        return ExpiryClose("no_data", session_date=session, note=NOTE_NO_SESSION)
    obs = book.get(ticker, {}).get(session)
    if obs is not None:
        return ExpiryClose(
            "priced", price=float(obs.value), session_date=session,
            source=obs.source, pull_timestamp=obs.pull_timestamp,
        )
    deadline = session_settled_utc(ticker, session) + timedelta(days=EXPIRY_CLOSE_GRACE_DAYS)
    if now < deadline:
        return ExpiryClose("pending", session_date=session)
    return ExpiryClose("no_data", session_date=session, note=NOTE_NO_PRICE)


def classify_outcome(
    direction: str, entry: float, actual: float, expected: float | None,
) -> tuple[str, float, float] | None:
    """``(verdict, actual_move_pct, pnl_pct)`` or None for a non-directional row."""
    actual_move = (actual - entry) / entry * 100
    if direction == "CALL":
        hit, pnl = actual > entry, actual_move
    elif direction == "PUT":
        hit, pnl = actual < entry, -actual_move
    else:
        return None
    exp_move = expected if expected else 1.0
    if hit and abs(actual_move) >= abs(exp_move) * 0.5:
        return "hit", actual_move, pnl
    if hit:
        return "partial", actual_move, pnl
    return "miss", actual_move, pnl


def _score_note(entry: float, res: ExpiryClose, actual_move: float, factor: float = 1.0) -> str:
    if abs(factor - 1.0) > 1e-9:
        actual = res.price * factor
        return (
            f"Entry ${entry:.2f} → Actual ${actual:.2f} ({actual_move:+.1f}%) "
            f"[close {res.session_date} {res.source} ${res.price:.2f} x{factor:g} split, "
            f"{EXPIRY_CLOSE_RULE}, {SPLIT_RULE}]"
        )
    return (
        f"Entry ${entry:.2f} → Actual ${res.price:.2f} ({actual_move:+.1f}%) "
        f"[close {res.session_date} {res.source}, {EXPIRY_CLOSE_RULE}]"
    )


def _parse_signals_blob(blob: Any) -> dict[str, Any] | None:
    """Coerce the oracle_predictions.signals JSONB into a dict for the
    ReasoningBank fingerprint builder. Returns None on any failure so the
    caller can fall back to a thin fingerprint without crashing scoring.
    """
    if blob is None:
        return None
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, str):
        try:
            parsed = json.loads(blob)
            return parsed if isinstance(parsed, dict) else None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return None


def _record_success_lesson_safe(
    *,
    engine: Engine,
    prediction_id: str,
    ticker: str,
    direction: str,
    verdict: str,
    confidence: float | None,
    expected_move_pct: float | None,
    actual_move_pct: float,
    pnl_pct: float,
    signals_blob: Any,
    created_at: Any,
    expiry: Any,
    model: str | None,
) -> None:
    """Persist a success-side ReasoningLesson for a hit/partial prediction.

    Defensive at every step — failure to record a lesson must NEVER block
    scoring. Lazy-imports postmortem so this script is still runnable in
    environments where the intelligence subpackage isn't installed.
    """
    try:
        from intelligence.postmortem import record_success_lesson
    except Exception as exc:
        log.debug("score: skipped success lesson (import failed): {e}", e=str(exc))
        return

    try:
        data_at_decision = _parse_signals_blob(signals_blob)

        horizon_days: int | None = None
        try:
            if created_at is not None and expiry is not None:
                start = created_at.date() if hasattr(created_at, "date") else created_at
                horizon_days = max(0, (expiry - start).days)
        except Exception:
            horizon_days = None

        thesis = (
            f"Oracle {model or '?'} predicted {direction} on {ticker} "
            f"(confidence={confidence:.2f}, expected_move={expected_move_pct:.2f}%)"
            if confidence is not None and expected_move_pct is not None
            else f"Oracle {model or '?'} predicted {direction} on {ticker}"
        )
        what_worked = (
            f"realized {actual_move_pct:+.2f}% pnl={pnl_pct:+.2f}% over "
            f"{horizon_days}d horizon" if horizon_days is not None else
            f"realized {actual_move_pct:+.2f}% pnl={pnl_pct:+.2f}%"
        )
        takeaway = (
            f"{ticker} {direction} setup at this fingerprint paid out as "
            f"{verdict}; reuse weighting if same regime/horizon recurs."
        )

        record_success_lesson(
            engine,
            trade_id_or_prediction_id=prediction_id,
            ticker=ticker,
            direction=direction,
            outcome=verdict,
            data_at_decision=data_at_decision,
            thesis_at_decision=thesis,
            what_worked=what_worked,
            generalizable_takeaway=takeaway,
            horizon_days=horizon_days,
        )
    except Exception as exc:
        log.debug("score: success lesson capture failed for {p}: {e}",
                  p=prediction_id, e=str(exc))


_ALLOWED_VERDICT_COLS: frozenset[str] = frozenset({"hits", "partials", "misses"})


def score_one_chunk(
    conn,
    *,
    engine: Engine,
    prices: PriceBook,
    today: date,
    chunk_size: int,
    now: datetime | None = None,
    after: tuple[Any, str] | None = None,
    splits: SplitBook | None = None,
) -> dict[str, Any]:
    """Score one chunk of expired-and-scoreable pending predictions.

    Returns counters {scored, hits, misses, partials, skipped, no_data,
    pending_price, fetched, cursor} for the chunk. The caller accumulates
    across chunks and passes ``cursor`` back as ``after``.

    ``prices`` is a :data:`PriceBook` from :func:`load_expiry_closes`;
    ``splits`` (from :func:`load_split_book`) puts the expiry close on the
    entry's share basis when a split falls between creation and expiry;
    ``now`` (default: midnight UTC starting ``today``) is the instant the
    grace window in :func:`resolve_expiry_close` is measured against.

    Rows are walked in ``(expiry, id)`` order from ``after`` (keyset). A row
    whose close has not arrived yet stays 'pending' and is not refetched by
    a later chunk of the same run -- without the cursor such rows would sit at
    the head of every chunk and the run would never finish.

    Each call must run inside its own ``engine.begin()`` block — that
    way the row-level locks released between chunks instead of being
    held for the entire 2.27M-row scoring run.
    """
    if now is None:
        now = datetime.combine(today, time.min, timezone.utc)
    after_expiry, after_id = after if after is not None else (None, None)
    chunk = conn.execute(text("""
        SELECT id, ticker, direction, target_price, entry_price, expiry,
               confidence, expected_move_pct, model_name,
               signals, created_at
        FROM oracle_predictions
        WHERE verdict = 'pending'
          AND expiry <= :today
          AND entry_price IS NOT NULL
          AND entry_price > 0
          AND (:after_expiry IS NULL OR expiry > :after_expiry
               OR (expiry = :after_expiry AND id > :after_id))
        ORDER BY expiry, id
        LIMIT :chunk_size
    """), {
        "today": today, "chunk_size": int(chunk_size),
        "after_expiry": after_expiry, "after_id": after_id,
    }).fetchall()

    counters: dict[str, Any] = {
        "scored": 0, "hits": 0, "misses": 0, "partials": 0,
        "skipped": 0, "no_data": 0, "fetched": len(chunk),
        # Rows the chunk WHERE should already have excluded. Counted
        # separately from `skipped` so a non-zero value is visible as
        # the defect it would be, rather than hiding in the noise.
        "unscorable_entry_price": 0,
        # Expired rows whose expiry close has not been captured yet and
        # are still inside the grace window: left 'pending', nothing
        # written (the bug this replaces wrote verdict='no_data' here).
        "pending_price": 0,
        "cursor": (chunk[-1][5], chunk[-1][0]) if chunk else None,
    }

    for r in chunk:
        (
            pred_id, ticker, direction, target, entry, expiry,
            conf, expected, model, signals_blob, created_at,
        ) = r

        if direction not in ("CALL", "PUT"):
            counters["skipped"] += 1
            continue

        # Belt-and-braces against a future edit to the chunk WHERE above.
        # Settled BEFORE the division: a NULL entry is not a zero entry, a
        # zero entry is not a 0% move, and neither is an infinite one. The
        # row is closed with the reason rather than skipped silently, so it
        # cannot sit 'pending' forever, and the two reasons stay distinct.
        entry_note = entry_price_score_note(entry)
        if entry_note is not None:
            conn.execute(text("""
                UPDATE oracle_predictions
                SET verdict = 'no_data', scored_at = NOW(),
                    score_notes = :notes
                WHERE id = :id
            """), {"id": pred_id, "notes": entry_note})
            counters["unscorable_entry_price"] += 1
            counters["no_data"] += 1
            continue

        res = resolve_expiry_close(prices, ticker, expiry, created_at=created_at, now=now)
        if res.status == "pending":
            counters["pending_price"] += 1
            continue
        if res.status != "priced":
            conn.execute(text("""
                UPDATE oracle_predictions
                SET verdict = 'no_data', scored_at = NOW(),
                    score_notes = :notes
                WHERE id = :id
            """), {"id": pred_id, "notes": res.note})
            counters["no_data"] += 1
            continue

        actual, factor = split_adjusted_close(
            splits, ticker, res.price, res.session_date, created_at=created_at,
            source=res.source, pull_timestamp=res.pull_timestamp,
        )
        outcome = classify_outcome(direction, entry, actual, expected)
        if outcome is None:
            counters["skipped"] += 1
            continue
        verdict, actual_move, pnl = outcome
        counters[{"hit": "hits", "partial": "partials", "miss": "misses"}[verdict]] += 1
        if abs(factor - 1.0) > 1e-9:
            counters["split_adjusted"] = counters.get("split_adjusted", 0) + 1

        conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = :v, actual_price = :ap, actual_move_pct = :am,
                pnl_pct = :pnl, scored_at = NOW(),
                score_notes = :notes
            WHERE id = :id
        """), {
            "v": verdict, "ap": actual, "am": round(actual_move, 2),
            "pnl": round(pnl, 2), "id": pred_id,
            "notes": _score_note(entry, res, actual_move, factor),
        })
        counters["scored"] += 1

        if verdict in ("hit", "partial"):
            _record_success_lesson_safe(
                engine=engine,
                prediction_id=pred_id,
                ticker=ticker,
                direction=direction,
                verdict=verdict,
                confidence=conf,
                expected_move_pct=expected,
                actual_move_pct=actual_move,
                pnl_pct=pnl,
                signals_blob=signals_blob,
                created_at=created_at,
                expiry=expiry,
                model=model,
            )

        col_map = {"hit": "hits", "partial": "partials", "miss": "misses"}
        verdict_col = col_map.get(verdict)
        if verdict_col:
            assert verdict_col in _ALLOWED_VERDICT_COLS, \
                f"Blocked DDL: verdict_col '{verdict_col}' not in allowed set"
            conn.execute(text(f"""
                UPDATE oracle_models
                SET {verdict_col} = {verdict_col} + 1,
                    predictions_made = predictions_made + 1,
                    cumulative_pnl = cumulative_pnl + :pnl,
                    last_updated = NOW()
                WHERE name = :model
            """), {"pnl": pnl, "model": model})

    return counters


# ── Hindsight rescore of the bug-closed no_data rows ───────────────────────
#
# The rows the old lookup closed as NOTE_NO_PRICE are final in
# oracle_predictions and stay exactly as they are: prediction/score rows are
# immutable. A rescore is an additive record in its own table, one row per
# (prediction_id, scoring_version), written with ON CONFLICT DO NOTHING so a
# second run inserts nothing new.
#
# It is exploratory hindsight, not a forward score: the rule was settled after
# these outcomes were observable. Every row carries score_class =
# 'exploratory_hindsight' and official = FALSE, both pinned by CHECK
# constraints, so it can never be read as an official (E2) score. Readers of
# oracle_predictions (scoreboard, calibration, model stats, success lessons)
# never see these rows, and oracle_models is never touched by a rescore.

RESCORE_TABLE = "oracle_prediction_rescores"
RESCORE_SCORING_VERSION = "oracle_rescore_hindsight_v1"
RESCORE_SCORE_CLASS = "exploratory_hindsight"

_RESCORE_DDL = """
CREATE TABLE IF NOT EXISTS oracle_prediction_rescores (
    prediction_id TEXT NOT NULL,
    scoring_version TEXT NOT NULL,
    score_class TEXT NOT NULL CHECK (score_class = 'exploratory_hindsight'),
    official BOOLEAN NOT NULL DEFAULT FALSE CHECK (official = FALSE),
    prior_verdict TEXT NOT NULL,
    prior_score_notes TEXT,
    verdict TEXT NOT NULL CHECK (verdict IN ('hit', 'partial', 'miss')),
    entry_price DOUBLE PRECISION NOT NULL,
    actual_price DOUBLE PRECISION NOT NULL,
    actual_move_pct DOUBLE PRECISION NOT NULL,
    pnl_pct DOUBLE PRECISION NOT NULL,
    price_session_date DATE NOT NULL,
    price_source TEXT,
    price_pull_timestamp TIMESTAMPTZ,
    expiry_rule TEXT NOT NULL,
    notes TEXT,
    rescored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prediction_id, scoring_version)
)
"""

_RESCORE_INSERT = """
INSERT INTO oracle_prediction_rescores (
    prediction_id, scoring_version, score_class, official,
    prior_verdict, prior_score_notes, verdict,
    entry_price, actual_price, actual_move_pct, pnl_pct,
    price_session_date, price_source, price_pull_timestamp,
    expiry_rule, notes, rescored_at
) VALUES (
    :prediction_id, :scoring_version, :score_class, FALSE,
    :prior_verdict, :prior_score_notes, :verdict,
    :entry_price, :actual_price, :actual_move_pct, :pnl_pct,
    :price_session_date, :price_source, :price_pull_timestamp,
    :expiry_rule, :notes, :rescored_at
)
ON CONFLICT (prediction_id, scoring_version) DO NOTHING
"""

# Only rows the price-lookup bug closed: a NEUTRAL row or a NULL/zero entry is
# unscorable for its own reason and is not a candidate.
_RESCORE_CANDIDATES = """
SELECT p.id, p.ticker, p.direction, p.entry_price, p.expiry,
       p.expected_move_pct, p.created_at, p.dedup_keep, p.verdict, p.score_notes
FROM oracle_predictions p
WHERE p.verdict = 'no_data'
  AND p.score_notes = :note
  AND p.entry_price > 0
  AND p.direction IN ('CALL', 'PUT')
ORDER BY p.expiry, p.id
"""


def _rescore_table_exists(conn: Any) -> bool:
    return inspect(conn).has_table(RESCORE_TABLE)


def rescore_no_data(engine: Engine, *, apply: bool, now: datetime | None = None) -> dict[str, Any]:
    """Re-price bug-closed no_data rows under EXPIRY_CLOSE_RULE; project or append.

    Dry run (``apply=False``) runs in a READ ONLY transaction on PostgreSQL and
    writes nothing anywhere. ``apply=True`` creates the rescore table if needed
    and appends one row per newly priced prediction; oracle_predictions and
    oracle_models are never written. Returns the projection either way.
    """
    now = now or datetime.now(timezone.utc)
    pg = engine.dialect.name == "postgresql"
    with engine.connect() as conn:
        if pg:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '600s'"))
        candidates = conn.execute(text(_RESCORE_CANDIDATES), {"note": NOTE_NO_PRICE}).fetchall()
        already: set[str] = set()
        if _rescore_table_exists(conn):
            already = {
                r[0] for r in conn.execute(text(
                    "SELECT prediction_id FROM oracle_prediction_rescores "
                    "WHERE scoring_version = :v"
                ), {"v": RESCORE_SCORING_VERSION}).fetchall()
            }
        book: PriceBook = {}
        if candidates:
            start = min(_as_date(r[4]) for r in candidates) - timedelta(days=_MAX_SESSION_LOOKBACK_DAYS)
            end = max(_as_date(r[4]) for r in candidates)
            book = load_expiry_closes(
                conn, sorted({r[1] for r in candidates}), start, end, as_of_ts=now,
            )
        dedup_verdicts = {
            r[0]: int(r[1]) for r in conn.execute(text(
                "SELECT verdict, COUNT(*) FROM oracle_predictions "
                "WHERE dedup_keep GROUP BY verdict"
            )).fetchall()
        }
        official_dates = {
            _as_date(r[0]) for r in conn.execute(text(
                "SELECT DISTINCT expiry FROM oracle_predictions "
                "WHERE dedup_keep AND verdict IN ('hit', 'miss', 'partial')"
            )).fetchall()
        }
        conn.rollback()

    tally: dict[str, dict[str, int]] = {"dedup_keep": {}, "other": {}}
    new_dates: set[date] = set()
    inserts: list[dict[str, Any]] = []
    for pred_id, ticker, direction, entry, expiry, expected, created_at, keep, prior, notes in candidates:
        bucket = tally["dedup_keep" if keep else "other"]
        bucket["candidates"] = bucket.get("candidates", 0) + 1
        if pred_id in already:
            bucket["already_rescored"] = bucket.get("already_rescored", 0) + 1
            continue
        res = resolve_expiry_close(book, ticker, expiry, created_at=created_at, now=now)
        if res.status != "priced":
            key = "still_pending_close" if res.status == "pending" else (
                "no_session" if res.note == NOTE_NO_SESSION else "still_no_price")
            bucket[key] = bucket.get(key, 0) + 1
            continue
        outcome = classify_outcome(direction, float(entry), res.price, expected)
        if outcome is None:
            continue
        verdict, move, pnl = outcome
        bucket[verdict] = bucket.get(verdict, 0) + 1
        if keep:
            new_dates.add(_as_date(expiry))
        inserts.append({
            "prediction_id": pred_id, "scoring_version": RESCORE_SCORING_VERSION,
            "score_class": RESCORE_SCORE_CLASS, "prior_verdict": prior,
            "prior_score_notes": notes, "verdict": verdict,
            "entry_price": float(entry), "actual_price": res.price,
            "actual_move_pct": round(move, 4), "pnl_pct": round(pnl, 4),
            "price_session_date": res.session_date, "price_source": res.source,
            "price_pull_timestamp": res.pull_timestamp,
            "expiry_rule": EXPIRY_CLOSE_RULE,
            "notes": _score_note(float(entry), res, move), "rescored_at": now,
        })

    keep_tally = tally["dedup_keep"]
    newly_priced_keep = sum(keep_tally.get(v, 0) for v in ("hit", "miss", "partial"))
    total_keep = sum(dedup_verdicts.values())
    no_data_after = dedup_verdicts.get("no_data", 0) - newly_priced_keep
    projection: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "scoring_version": RESCORE_SCORING_VERSION,
        "score_class": RESCORE_SCORE_CLASS,
        "expiry_rule": EXPIRY_CLOSE_RULE,
        "as_of": now.isoformat(),
        "tickers_with_closes": len(book),
        "tally": tally,
        "dedup_keep_verdicts_before": dedup_verdicts,
        "dedup_keep_no_data_share_before": round(dedup_verdicts.get("no_data", 0) / total_keep, 4) if total_keep else None,
        "dedup_keep_no_data_share_after": round(no_data_after / total_keep, 4) if total_keep else None,
        "official_scored_expiry_dates": len(official_dates),
        "rescored_expiry_dates": len(new_dates),
        "expiry_dates_scored_combined": len(official_dates | new_dates),
        "rows_to_append": len(inserts),
        "rows_appended": 0,
    }

    if apply and inserts:
        with engine.begin() as conn:
            conn.execute(text(_RESCORE_DDL))
            appended = sum(
                conn.execute(text(_RESCORE_INSERT), row).rowcount or 0 for row in inserts
            )
        projection["rows_appended"] = appended
    return projection


# ── Split rescore (W2-10) ───────────────────────────────────────────────────
#
# Verdicts written before SPLIT_RULE compared a pre-split entry with a
# post-split close. They stay exactly as they are; the corrected score is an
# additive oracle_prediction_rescores row under SPLIT_RESCORE_SCORING_VERSION
# (exploratory hindsight, official = FALSE, same CHECK constraints). Covered:
# official verdicts (hit/partial/miss) and oracle_rescore_hindsight_v1 rows
# whose (creation date, expiry] window contains a recorded split. The close is
# re-read under EXPIRY_CLOSE_RULE and put on the entry's share basis. When the
# rule finds no settled close (older verdicts were priced from
# options_daily_signals spot, e.g. IWF/VGT in April 2026), the price the
# verdict itself recorded (actual_price, as-traded at expiry) is re-based
# instead and labelled price_source = STORED_PRICE_SOURCE.

SPLIT_RESCORE_SCORING_VERSION = "oracle_rescore_split_adjusted_v1"
STORED_PRICE_SOURCE = "stored_actual_price"

_SPLIT_OFFICIAL_CANDIDATES = """
SELECT p.id, p.direction, p.entry_price, p.expiry, p.expected_move_pct,
       p.created_at, p.dedup_keep, p.verdict, p.score_notes, p.actual_price
FROM oracle_predictions p
WHERE p.ticker = :ticker
  AND p.verdict IN ('hit', 'partial', 'miss')
  AND p.direction IN ('CALL', 'PUT')
  AND p.entry_price > 0
  AND p.created_at < :split_start
  AND p.expiry >= :split_date
"""

_SPLIT_RESCORED_CANDIDATES = """
SELECT p.id, p.direction, r.entry_price, p.expiry, p.expected_move_pct,
       p.created_at, p.dedup_keep, r.verdict, r.notes, r.actual_price
FROM oracle_prediction_rescores r
JOIN oracle_predictions p ON p.id = r.prediction_id
WHERE r.scoring_version = :prior_version
  AND p.ticker = :ticker
  AND p.direction IN ('CALL', 'PUT')
  AND r.entry_price > 0
  AND p.created_at < :split_start
  AND p.expiry >= :split_date
"""


def rescore_splits(
    engine: Engine, *, apply: bool, now: datetime | None = None,
) -> dict[str, Any]:
    """Re-score verdicts whose window spans a recorded split; project or append.

    Dry run (``apply=False``) runs READ ONLY on PostgreSQL and writes nothing.
    ``apply=True`` appends one ``SPLIT_RESCORE_SCORING_VERSION`` row per
    affected prediction (ON CONFLICT DO NOTHING); oracle_predictions and
    oracle_models are never written.
    """
    now = now or datetime.now(timezone.utc)
    pg = engine.dialect.name == "postgresql"
    candidates: dict[str, dict[str, Any]] = {}
    books: dict[str, PriceBook] = {}
    with engine.connect() as conn:
        if pg:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '600s'"))
        tickers = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT ticker FROM oracle_predictions WHERE ticker IS NOT NULL"
        )).fetchall()]
        splits = load_split_book(conn, tickers, as_of_ts=now)
        has_rescores = _rescore_table_exists(conn)
        already: set[str] = set()
        if has_rescores:
            already = {r[0] for r in conn.execute(text(
                "SELECT prediction_id FROM oracle_prediction_rescores WHERE scoring_version = :v"
            ), {"v": SPLIT_RESCORE_SCORING_VERSION}).fetchall()}
        for ticker, events in sorted(splits.items()):
            for ev in events:
                params = {
                    "ticker": ticker, "split_date": ev.effective_date,
                    "split_start": datetime.combine(ev.effective_date, time.min, _NY).astimezone(timezone.utc),
                }
                queries = [("official", _SPLIT_OFFICIAL_CANDIDATES, params)]
                if has_rescores:
                    queries.append(("rescore_v1", _SPLIT_RESCORED_CANDIDATES,
                                    {**params, "prior_version": RESCORE_SCORING_VERSION}))
                for kind, sql, bound in queries:
                    for row in conn.execute(text(sql), bound).fetchall():
                        candidates.setdefault(row[0], {"ticker": ticker, "kind": kind, "row": row})
        by_ticker: dict[str, list[date]] = {}
        for c in candidates.values():
            by_ticker.setdefault(c["ticker"], []).append(_as_date(c["row"][3]))
        for ticker, expiries in by_ticker.items():
            books[ticker] = load_expiry_closes(
                conn, [ticker], min(expiries) - timedelta(days=_MAX_SESSION_LOOKBACK_DAYS),
                max(expiries), as_of_ts=now,
            ).get(ticker, {})
        conn.rollback()

    tally: dict[str, dict[str, int]] = {}
    inserts: list[dict[str, Any]] = []
    for pred_id, c in sorted(candidates.items()):
        ticker, kind = c["ticker"], c["kind"]
        (_id, direction, entry, expiry, expected, created_at, keep, prior, prior_notes,
         stored_actual) = c["row"]
        bucket = tally.setdefault(f"{kind}:{'dedup_keep' if keep else 'other'}", {})
        bucket["spanning_split"] = bucket.get("spanning_split", 0) + 1
        if pred_id in already:
            bucket["already_rescored"] = bucket.get("already_rescored", 0) + 1
            continue
        res = resolve_expiry_close({ticker: books.get(ticker, {})}, ticker, expiry,
                                   created_at=created_at, now=now)
        if res.status != "priced" and stored_actual is not None and float(stored_actual) > 0:
            res = ExpiryClose(
                "priced", price=float(stored_actual),
                session_date=expiry_session_date(ticker, _as_date(expiry)) or _as_date(expiry),
                source=STORED_PRICE_SOURCE,
            )
            bucket["stored_price"] = bucket.get("stored_price", 0) + 1
        if res.status != "priced":
            bucket["no_close"] = bucket.get("no_close", 0) + 1
            continue
        actual, factor = split_adjusted_close(
            splits, ticker, res.price, res.session_date, created_at=created_at,
            source=res.source, pull_timestamp=res.pull_timestamp,
        )
        if abs(factor - 1.0) <= 1e-9:
            bucket["factor_one"] = bucket.get("factor_one", 0) + 1
            continue
        outcome = classify_outcome(direction, float(entry), actual, expected)
        if outcome is None:
            continue
        verdict, move, pnl = outcome
        change = f"{prior}->{verdict}"
        bucket[change] = bucket.get(change, 0) + 1
        inserts.append({
            "prediction_id": pred_id, "scoring_version": SPLIT_RESCORE_SCORING_VERSION,
            "score_class": RESCORE_SCORE_CLASS, "prior_verdict": prior,
            "prior_score_notes": prior_notes, "verdict": verdict,
            "entry_price": float(entry), "actual_price": actual,
            "actual_move_pct": round(move, 4), "pnl_pct": round(pnl, 4),
            "price_session_date": res.session_date, "price_source": res.source,
            "price_pull_timestamp": res.pull_timestamp,
            "expiry_rule": f"{EXPIRY_CLOSE_RULE}+{SPLIT_RULE}",
            "notes": _score_note(float(entry), res, move, factor), "rescored_at": now,
        })

    projection: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "scoring_version": SPLIT_RESCORE_SCORING_VERSION,
        "score_class": RESCORE_SCORE_CLASS,
        "expiry_rule": f"{EXPIRY_CLOSE_RULE}+{SPLIT_RULE}",
        "as_of": now.isoformat(),
        "tickers_with_recorded_splits": len(splits),
        "split_events": sum(len(v) for v in splits.values()),
        "candidates_spanning_split": len(candidates),
        "tally": tally,
        "rows_to_append": len(inserts),
        "rows_appended": 0,
    }
    if apply and inserts:
        with engine.begin() as conn:
            conn.execute(text(_RESCORE_DDL))
            projection["rows_appended"] = sum(
                conn.execute(text(_RESCORE_INSERT), row).rowcount or 0 for row in inserts
            )
    return projection


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--chunk-size", type=int, default=5000,
        help="Rows processed per transaction (default 5000). Lower for less "
             "lock pressure on a busy DB; higher for fewer round-trips.",
    )
    ap.add_argument(
        "--max-rows", type=int, default=None,
        help="Stop after scoring this many rows (default: score every "
             "expired-and-scoreable pending). Useful for a probe run "
             "before the full sweep.",
    )
    ap.add_argument(
        "--progress-every", type=int, default=10,
        help="Log progress every N chunks (default 10).",
    )
    ap.add_argument(
        "--rescore-no-data", action="store_true",
        help="Re-price rows closed as 'No price data at expiry' under "
             f"{EXPIRY_CLOSE_RULE} and print the projection. Dry run (read "
             "only) unless --apply. Skips the normal scoring steps.",
    )
    ap.add_argument(
        "--rescore-splits", action="store_true",
        help="Re-score official verdicts and hindsight-v1 rescores whose "
             "window spans a recorded split (YF:{T}:split_factor) under "
             f"{SPLIT_RULE}; print the projection. Dry run unless --apply.",
    )
    ap.add_argument(
        "--apply", action="store_true",
        help=f"With --rescore-no-data / --rescore-splits: append the results to "
             f"{RESCORE_TABLE} (exploratory hindsight, never official). Never "
             "updates oracle_predictions.",
    )
    args = ap.parse_args(argv)
    if args.rescore_no_data and args.rescore_splits:
        ap.error("choose one of --rescore-no-data / --rescore-splits")
    if args.apply and not (args.rescore_no_data or args.rescore_splits):
        ap.error("--apply only applies to --rescore-no-data or --rescore-splits")
    return args


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    engine = create_engine(settings.DB_URL)

    if args.rescore_no_data:
        projection = rescore_no_data(engine, apply=args.apply)
        print(json.dumps(projection, indent=2, default=str))
        return
    if args.rescore_splits:
        projection = rescore_splits(engine, apply=args.apply)
        print(json.dumps(projection, indent=2, default=str))
        return

    with engine.begin() as conn:
        # ── Step 0: Get all pending predictions ──
        rows = conn.execute(text("""
            SELECT id, ticker, direction, entry_price, created_at::date, expiry,
                   null_write_policy
            FROM oracle_predictions
            WHERE verdict = 'pending'
            ORDER BY created_at
        """)).fetchall()

        log.info("\n{}", '='*60)
        log.info("ORACLE TRADE SCORER")
        log.info("{}", '='*60)
        log.info("Total pending predictions: {}", len(rows))
        if not rows:
            log.info("Nothing pending -- done.")
            return

        # Get unique tickers and date range
        tickers = sorted(set(r[1] for r in rows))
        min_date = min(r[4] for r in rows)
        max_expiry = max(r[5] for r in rows)
        # griddb and the cron host run Etc/UTC; the grace window in
        # resolve_expiry_close is measured against this instant.
        now = datetime.now(timezone.utc)
        today = now.date()

        log.info("Tickers: {}", len(tickers))
        log.info("Date range: {} to {}", min_date, max_expiry)
        log.info("Today: {}", today)

        # ── Step 1: Load expiry closes ──
        # raw_series through store.observations (SUCCESS, latest vintage,
        # settled sessions only, nothing dated after today). Bounded to the
        # tickers and dates the expired pending rows can actually need.
        log.info("\n--- STEP 1: Load Expiry Closes ({}) ---", EXPIRY_CLOSE_RULE)
        scoreable = [r for r in rows if r[5] <= today]
        prices: PriceBook = {}
        if scoreable:
            price_start = min(r[5] for r in scoreable) - timedelta(days=_MAX_SESSION_LOOKBACK_DAYS)
            prices = load_expiry_closes(
                conn, sorted({r[1] for r in scoreable}), price_start, today, as_of_ts=now,
            )
        log.info("  closes loaded for {} of {} tickers", len(prices),
                 len({r[1] for r in scoreable}))
        splits: SplitBook = load_split_book(
            conn, sorted({r[1] for r in scoreable}), as_of_ts=now,
        ) if scoreable else {}
        log.info("  recorded splits for {} tickers ({})", len(splits), SPLIT_RULE)

        # ── Step 2: Backfill entry prices ──
        # HISTORICAL-WRITE HOLD: this step used to backfill a missing/zero
        # entry_price with a historical close looked up just now. That is
        # repair of an already-published row, which is on hold for both
        # cases below:
        #   * entry_price is a non-null 0/negative value -- only possible on
        #     a legacy row written before oracle_pred_nullable_0918 made the
        #     column nullable. Never updated, closed, rescored or
        #     re-labelled; left exactly as it is.
        #   * entry_price IS NULL -- a new-policy row: nothing was measured
        #     at publish time. Writing a price discovered after the fact
        #     would be exactly the fabrication the new policy exists to
        #     avoid, so it is never backfilled either.
        # This step therefore only counts what it is holding; it writes
        # nothing to oracle_predictions.
        #
        # A NULL entry_price is split further by null_write_policy (item
        # (a)'s provenance boundary): NULL alone does not prove the row was
        # written under the honest-NULL policy, only the stamp does. An
        # entry_price IS NULL row whose null_write_policy does NOT match
        # NULL_WRITE_POLICY is unmarked -- held like a legacy row, not
        # closed in Step 3.5 below, and should be structurally impossible
        # today (both writers stamp every INSERT).
        log.info("\n--- STEP 2: Backfill Entry Prices (historical-write hold) ---")
        held_legacy = 0
        held_new_policy = 0
        held_unmarked_null = 0

        for r in rows:
            pred_id, ticker, direction, entry_price, created_date, expiry, null_write_policy = r

            if entry_price is not None and entry_price > 0:
                continue  # Already has a price

            if entry_price is None and null_write_policy == NULL_WRITE_POLICY:
                held_new_policy += 1
            elif entry_price is None:
                held_unmarked_null += 1
            else:
                held_legacy += 1

        log.info("  Held (legacy, non-null invalid entry_price): {}", held_legacy)
        log.info("  Held (new-policy, entry_price IS NULL, stamped): {}", held_new_policy)
        if held_unmarked_null:
            log.warning(
                "  Held (UNMARKED entry_price IS NULL, null_write_policy "
                "does not prove the honest-NULL policy): {} -- should be "
                "structurally impossible; investigate the writer",
                held_unmarked_null,
            )

        # ── Step 3: Fix direction mapping ──
        log.info("\n--- STEP 3: Fix Direction Mapping ---")

        # BULLISH → CALL
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET direction = 'CALL'
            WHERE verdict = 'pending' AND direction = 'BULLISH'
        """))
        log.info("  BULLISH → CALL: {}", res.rowcount)

        # BEARISH → PUT (just in case)
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET direction = 'PUT'
            WHERE verdict = 'pending' AND direction = 'BEARISH'
        """))
        log.info("  BEARISH → PUT: {}", res.rowcount)

        # NEUTRAL → no_data (unscorable)
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = 'no_data',
                score_notes = 'NEUTRAL direction is unscorable',
                scored_at = NOW()
            WHERE verdict = 'pending' AND direction = 'NEUTRAL'
        """))
        log.info("  NEUTRAL → no_data: {}", res.rowcount)

        # No entry price measured → no_data. Only entry_price IS NULL AND
        # null_write_policy proving the honest-NULL policy is closed here: a
        # new-policy row where nothing was measured at publish time, so it
        # would otherwise sit 'pending' forever, neither scored nor
        # accounted for. Not repaired — the row is closed and the note says
        # why. A bare `entry_price IS NULL` is NOT sufficient on its own
        # (item (a)'s provenance boundary exists precisely because the
        # nullable schema alone cannot prove this) -- the stamp is what
        # proves it.
        #
        # HISTORICAL-WRITE HOLD: a non-null 0/negative entry_price can only
        # be a legacy row written before oracle_pred_nullable_0918 made the
        # column nullable (the new publish path writes NULL, never 0/neg,
        # when nothing was measured). That row is deliberately excluded from
        # this WHERE — historical rescoring/repair is on hold, so it is
        # never updated, closed, rescored or re-labelled here. Likewise, an
        # entry_price IS NULL row whose null_write_policy does not match
        # NULL_WRITE_POLICY (unmarked provenance) is excluded and stays
        # pending, exactly as it was before this branch.
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = 'no_data',
                score_notes = :note_null,
                scored_at = NOW()
            WHERE verdict = 'pending'
              AND entry_price IS NULL
              AND null_write_policy = :policy
        """), {
            "note_null": SCORE_NOTE_ENTRY_NULL,
            "policy": NULL_WRITE_POLICY,
        })
        log.info("  no usable entry_price → no_data: {}", res.rowcount)

    # ── Step 4: Score expired predictions (CHUNKED) ──
    log.info("\n--- STEP 4: Score Expired Predictions ---")

    # Pre-count so progress logs have a denominator.
    with engine.connect() as conn:
        total_scoreable = int(conn.execute(text("""
            SELECT COUNT(*) FROM oracle_predictions
            WHERE verdict = 'pending'
              AND expiry <= :today
              AND entry_price IS NOT NULL
              AND entry_price > 0
        """), {"today": today}).scalar() or 0)

    log.info("  Expired & scoreable: {:,}", total_scoreable)
    log.info(
        "  Chunked: chunk-size={:,}, max-rows={}",
        args.chunk_size,
        args.max_rows if args.max_rows is not None else "all",
    )

    # unscorable_entry_price is score_one_chunk's own belt-and-braces
    # counter (rows its WHERE should already have excluded -- see that
    # function). Listed explicitly so a reader sees every key the chunk
    # loop can report, but the accumulation below never assumes this is
    # the complete set: a chunk counters dict growing a new key must never
    # crash main() with a KeyError, it should just be accumulated.
    totals = {"scored": 0, "hits": 0, "misses": 0, "partials": 0,
              "skipped": 0, "no_data": 0, "unscorable_entry_price": 0,
              "pending_price": 0}
    processed = 0
    chunk_idx = 0
    cursor = None
    while True:
        chunk_size = args.chunk_size
        if args.max_rows is not None:
            remaining = args.max_rows - processed
            if remaining <= 0:
                break
            chunk_size = min(chunk_size, remaining)

        with engine.begin() as chunk_conn:
            chunk_counters = score_one_chunk(
                chunk_conn,
                engine=engine,
                prices=prices,
                today=today,
                chunk_size=chunk_size,
                now=now,
                after=cursor,
                splits=splits,
            )

        chunk_idx += 1
        fetched = chunk_counters.pop("fetched", 0)
        cursor = chunk_counters.pop("cursor", None)
        # .get(k, 0) rather than totals[k]: score_one_chunk's counters dict
        # is not required to carry exactly the keys totals was seeded
        # with -- a chunk reporting a key totals does not already have
        # must be accumulated, not raise KeyError (this crashed main() on
        # the very first chunk that reported unscorable_entry_price before
        # this fix).
        for k, v in chunk_counters.items():
            totals[k] = totals.get(k, 0) + v
        processed += fetched

        # No more pending rows past the cursor — done.
        if fetched == 0 or cursor is None:
            break

        if chunk_idx % args.progress_every == 0 or fetched < chunk_size:
            pct = (processed / total_scoreable * 100) if total_scoreable else 0.0
            log.info(
                "  chunk {} done: processed {:,}/{:,} ({:.1f}%) "
                "[hits={:,}, partials={:,}, misses={:,}, no_data={:,}, skipped={:,}]",
                chunk_idx, processed, total_scoreable, pct,
                totals["hits"], totals["partials"], totals["misses"],
                totals["no_data"], totals["skipped"],
            )

    total_scored = totals["hits"] + totals["misses"] + totals["partials"]
    log.info(
        "  Scored: {:,} (Hits: {:,}, Miss: {:,}, Partial: {:,}, "
        "No-data: {:,}, Skipped: {:,}, Awaiting close: {:,}) across {} chunks",
        total_scored, totals["hits"], totals["misses"], totals["partials"],
        totals["no_data"], totals["skipped"], totals["pending_price"], chunk_idx,
    )

    # Re-open a connection for the scorecard reads.
    with engine.connect() as conn:

        # ── Step 5: Scorecard ──
        log.info("\n{}", '='*60)
        log.info("SCORECARD")
        log.info("{}", '='*60)

        # Overall stats
        stats = conn.execute(text("""
            SELECT verdict, COUNT(*) FROM oracle_predictions
            GROUP BY verdict ORDER BY count DESC
        """)).fetchall()

        log.info("\nOverall Verdict Distribution:")
        for v, c in stats:
            log.info("  {:12s}: {:>6,}", v, c)

        # By model
        log.info("\nBy Model:")
        model_stats = conn.execute(text("""
            SELECT model_name,
                   COUNT(*) as total,
                   SUM(CASE WHEN verdict='hit' THEN 1 ELSE 0 END) as hits,
                   SUM(CASE WHEN verdict='miss' THEN 1 ELSE 0 END) as misses,
                   SUM(CASE WHEN verdict='partial' THEN 1 ELSE 0 END) as partials,
                   SUM(CASE WHEN verdict='pending' THEN 1 ELSE 0 END) as pending,
                   AVG(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct END) as avg_pnl
            FROM oracle_predictions
            GROUP BY model_name
            ORDER BY model_name
        """)).fetchall()

        log.info("  {:<20s} {:>6s} {:>5s} {:>5s} {:>5s} {:>5s} {:>6s} {:>8s}", 'Model', 'Total', 'Hits', 'Miss', 'Part', 'Pend', 'Hit%', 'AvgPnL')
        log.info("  {} {} {} {} {} {} {} {}", '-'*20, '-'*6, '-'*5, '-'*5, '-'*5, '-'*5, '-'*6, '-'*8)
        for m in model_stats:
            name, total, h, mi, p, pend, avg_pnl = m
            scored = (h or 0) + (mi or 0) + (p or 0)
            hit_pct = f"{(h or 0)/scored*100:.1f}%" if scored > 0 else "N/A"
            pnl_str = f"{avg_pnl:+.2f}%" if avg_pnl is not None else "N/A"
            log.info("  {:<20s} {:>6,} {:>5} {:>5} {:>5} {:>5} {:>6s} {:>8s}", name, total, h or 0, mi or 0, p or 0, pend or 0, hit_pct, pnl_str)

        # By ticker (scored only)
        log.info("\nBy Ticker (scored only):")
        ticker_stats = conn.execute(text("""
            SELECT ticker,
                   COUNT(*) as total,
                   SUM(CASE WHEN verdict='hit' THEN 1 ELSE 0 END) as hits,
                   SUM(CASE WHEN verdict='miss' THEN 1 ELSE 0 END) as misses,
                   AVG(pnl_pct) as avg_pnl
            FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
            GROUP BY ticker
            ORDER BY avg_pnl DESC NULLS LAST
        """)).fetchall()

        if ticker_stats:
            log.info("  {:<8s} {:>6s} {:>5s} {:>5s} {:>6s} {:>8s}", 'Ticker', 'Scored', 'Hits', 'Miss', 'Hit%', 'AvgPnL')
            log.info("  {} {} {} {} {} {}", '-'*8, '-'*6, '-'*5, '-'*5, '-'*6, '-'*8)
            for t in ticker_stats:
                name, total, h, mi, avg_pnl = t
                hit_pct = f"{(h or 0)/total*100:.1f}%" if total > 0 else "N/A"
                pnl_str = f"{avg_pnl:+.2f}%" if avg_pnl is not None else "N/A"
                log.info("  {:<8s} {:>6,} {:>5} {:>5} {:>6s} {:>8s}", name, total, h or 0, mi or 0, hit_pct, pnl_str)
        else:
            log.info("  No scored predictions yet (none expired)")

        # Pending by expiry
        log.info("\nPending by Expiry:")
        pending_exp = conn.execute(text("""
            SELECT expiry, COUNT(*),
                   COUNT(CASE WHEN entry_price > 0 THEN 1 END) as with_price
            FROM oracle_predictions
            WHERE verdict = 'pending'
            GROUP BY expiry ORDER BY expiry
        """)).fetchall()

        for exp, cnt, wp in pending_exp:
            status = "← SCOREABLE" if exp <= today else ""
            log.info("  {}: {:>5,} predictions ({} with entry price) {}", exp, cnt, wp, status)

    log.info("\n{}", '='*60)
    log.info("DONE")
    log.info("{}", '='*60)


if __name__ == "__main__":
    main()
