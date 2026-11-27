"""
GRID Intelligence — Automated Post-Mortem Analysis for Failed Trades & Predictions.

Every failed trade or wrong prediction triggers an automated audit that explains
WHY it failed, categorises the failure mode, identifies which signals were right
vs wrong, and produces actionable lessons learned.

Pipeline:
  1. generate_postmortem       — single trade post-mortem with full context reconstruction
  2. generate_prediction_postmortem — same for oracle predictions
  3. batch_postmortem          — bulk analysis over N days + pattern aggregation
  4. generate_lessons_learned  — LLM synthesis of systemic issues + recommendations

Failure categories:
  - wrong_signal:              scanner signals pointed the wrong way
  - right_signal_wrong_timing: direction correct, expiry too early/late
  - external_shock:            unforeseeable event (FOMC, earnings surprise, geopolitical)
  - bad_data:                  stale or incorrect input data
  - model_error:               recommendation logic bug or miscalibration

Designed to be called by hermes_operator or via the API.
"""

from __future__ import annotations

import json
import math
import time
from collections import Counter
from itertools import zip_longest
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.entry_price_policy import entry_price_score_note
from store.options_provenance import VISIBLE_DAILY_SIGNALS, VISIBLE_ORACLE_PREDICTIONS, visible_sql


# Lessons come from official verdicts (oracle.engine.score_expired_predictions),
# not from postmortems; see intelligence/reasoning_bank.py "The rules".
POSTMORTEM_WRITES_LESSONS = False

# Every miss gets a record; narration is an upgrade of that record (owner decision 2026-11-27).
# A postmortem is first written by the rule-based classifier alone (failure category, root cause,
# signals right and wrong, a rule-based "what we missed"); LLM narration later rewrites the
# narrative fields of that same row. ``full_analysis.analysis_method`` says which a row is.
ANALYSIS_RULES = "rules"
ANALYSIS_LLM = "llm"
# What rows written before 2026-11-27 hold in what_we_missed when the LLM was unreachable
# (345,485 rows of the spring bulk run). They are rule-based records and are narrated like one.
LEGACY_LLM_FALLBACK_TEXT = "Unable to generate LLM analysis."
# Rows whose narrative is still the rule-based one (``tp`` is the trade_postmortems alias).
# NULL-safe: a pre-2026-11-27 LLM row has no marker, and ``NOT (NULL OR FALSE)`` would be NULL,
# dropping it from both "narrated" and "not narrated".
UNNARRATED_SQL = (
    "COALESCE(tp.full_analysis->>'analysis_method' = '" + ANALYSIS_RULES + "'"
    " OR tp.what_we_missed = '" + LEGACY_LLM_FALLBACK_TEXT + "', FALSE)"
)
# The newest row of its prediction or trade (older copies are the spring run's duplicates:
# 76,710 predictions have 2 to 34 rows). Narration takes only this one.
NEWEST_COPY_SQL = (
    "NOT EXISTS (SELECT 1 FROM trade_postmortems tp_newer WHERE tp_newer.id > tp.id AND ("
    "(tp.prediction_id IS NOT NULL AND tp_newer.prediction_id = tp.prediction_id)"
    " OR (tp.trade_id IS NOT NULL AND tp_newer.trade_id = tp.trade_id)))"
)
# The API trigger (POST /intelligence/postmortems/generate) narrates at most this many per call,
# and stops starting rows after this many seconds.
API_POSTMORTEM_NARRATE_LIMIT = 5
API_POSTMORTEM_DEADLINE_S = 120
# PostmortemCompleted feeds source trust and model-weight decay (contracts/handlers). Only a miss
# scored this recently emits it, so recording old misses cannot replay months of decay at once.
POSTMORTEM_EVENT_WINDOW_DAYS = 30
# Rows the nightly record phase selects from each source per run. Rule-based rows take ~20 ms
# (measured 2026-11-27); the busiest scoring day so far produced 717 misses.
RECORD_PHASE_MAX_ROWS = 5000


def miss_on_or_after_sql(bound: str, alias: str = "tp") -> str:
    """SQL predicate: the miss a postmortem explains was scored (or the trade closed) on or after ``bound``.

    Readers window postmortems by the miss's own date, not by when the row was written, so a record
    written today for a miss scored in May never reads as one of this month's lessons. ``bound`` is a
    SQL expression (``":cutoff"``, ``"NOW() - INTERVAL '30 days'"``). The ``generated_at`` term is
    implied (a postmortem is written after its miss is scored): the miss dates are checked by primary
    key only for rows written inside the window. That is as cheap as before while those rows are few
    (0.11 s on 2026-11-27); for 30 days after a backfill they include every backfilled row (measured
    ~4 s for 280 K rows). An index on ``oracle_predictions(scored_at) WHERE verdict = 'miss'`` (a
    migration) would let readers start from the miss date instead.
    """
    return (
        f"({alias}.generated_at >= {bound} AND ("
        f"EXISTS (SELECT 1 FROM {VISIBLE_ORACLE_PREDICTIONS} op_w WHERE op_w.id = {alias}.prediction_id"
        f" AND op_w.scored_at >= {bound})"
        f" OR EXISTS (SELECT 1 FROM options_recommendations orr_w WHERE orr_w.id = {alias}.trade_id"
        f" AND orr_w.closed_at >= {bound})))"
    )


def _as_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return None


def _utcnow() -> datetime:
    """The clock the event window reads (a seam, so tests never read the real clock)."""
    return datetime.now(timezone.utc)


def _miss_is_recent(miss_at: Any) -> bool:
    """True when a miss is inside the event window, or its date is unknown (the old behaviour)."""
    when = _as_utc(miss_at)
    if when is None:
        return True
    return when >= _utcnow() - timedelta(days=POSTMORTEM_EVENT_WINDOW_DAYS)


# ── Data Classes ──────────────────────────────────────────────────────────

FAILURE_CATEGORIES = [
    "wrong_signal",
    "right_signal_wrong_timing",
    "external_shock",
    "bad_data",
    "model_error",
    # Not a failure of the model: the prediction had no usable entry price,
    # so no return and no timing could be reconstructed from it at all. Kept
    # distinct from "bad_data" so these rows can be counted and fixed rather
    # than averaged into the model's failure profile.
    "not_measured",
    # Underlying spot at decision/entry time could not be resolved from any
    # PIT-safe price source, so the underlying price path cannot be compared
    # like-for-like against an entry basis.
    "insufficient_data",
]


@dataclass
class PostMortem:
    """Complete post-mortem analysis for a failed trade or prediction."""

    trade_id: int
    ticker: str
    direction: str
    outcome: str                          # LOSS, EXPIRED, miss

    actual_return: float

    # What we had at decision time
    data_at_decision: dict                # all signals/data at recommendation time
    thesis_at_decision: str
    sanity_results_at_decision: dict

    # What happened
    what_actually_happened: str
    price_path: list[dict]                # day-by-day price from entry to exit

    # Analysis
    failure_category: str                 # one of FAILURE_CATEGORIES
    root_cause: str                       # specific explanation
    which_signals_were_wrong: list[str]
    which_signals_were_right: list[str]
    what_we_missed: str

    # Learning
    recommended_fix: str
    confidence_in_analysis: float         # 0-1
    generated_at: str

    # Unit of ``actual_return``. New rows are always a fraction (-0.5 = -50%).
    # Rows stored before 2026-11-26 carry no marker, and their unit depends on
    # the writer: see ``stored_return_fraction``.
    actual_return_unit: str = "fraction"

    # False on a hydrated pre-fix prediction row: its free-form prose may state
    # the old x100 return in any wording, so the prose fields are left empty
    # and no free text of it may reach a prompt (storage is untouched).
    prose_trusted: bool = True

    # ANALYSIS_RULES or ANALYSIS_LLM for rows written from 2026-11-27; None on older rows.
    analysis_method: str | None = None
    # When the miss was scored (prediction) or the trade closed, ISO-8601; None when unknown.
    miss_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


RETURN_UNIT_FRACTION = "fraction"
RETURN_UNIT_PERCENT_POINTS = "percent_points"


def stored_return_fraction(record: dict[str, Any]) -> float | None:
    """``actual_return`` of a stored ``trade_postmortems`` record, as a fraction.

    Rows written from 2026-11-26 carry ``full_analysis.actual_return_unit``.
    Older rows carry none, and the unit follows the writer, which the table
    records exactly (every row has one of ``trade_id`` / ``prediction_id``,
    never both: 860 / 1,108,188 rows when checked):

    * ``prediction_id`` set: copied from ``oracle_predictions.pnl_pct``, in
      percent points (-50.24 = -50.24%), so divided by 100;
    * ``trade_id`` set: copied from ``options_recommendations.actual_return``,
      already a fraction.

    No decision uses the magnitude of the value. Returns None (the row is
    left out of any average) for a missing or non-finite return, an unknown
    marker, or a row whose writer cannot be told.
    """
    full = record.get("full_analysis") or {}
    if not isinstance(full, dict):
        return None
    try:
        value = float(full.get("actual_return"))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    unit = full.get("actual_return_unit")
    if unit == RETURN_UNIT_FRACTION:
        return value
    if unit == RETURN_UNIT_PERCENT_POINTS:
        return value / 100.0
    if unit is not None:
        return None
    has_prediction = record.get("prediction_id") is not None
    has_trade = record.get("trade_id") is not None
    if has_prediction and not has_trade:
        return value / 100.0
    if has_trade and not has_prediction:
        return value
    return None


PROSE_OMITTED = "[free text omitted: recorded before the return-unit fix]"


@dataclass
class HydratedPostmortems:
    """``postmortems_from_records`` result."""

    postmortems: list["PostMortem"]
    included: list[dict[str, Any]]    # the records behind ``postmortems``, same order
    excluded: int                     # records left out: no known unit for the return


def postmortems_from_records(
    records: list[dict[str, Any]], limit: int | None = None,
) -> HydratedPostmortems:
    """Hydrate ``PostMortem`` objects from ``load_postmortems`` records.

    ``actual_return`` is always a fraction (``stored_return_fraction``). A
    record with no analysis blob is skipped; one with no return that can be
    put in a known unit is left out and counted in ``excluded``, so a mixed
    history cannot reach an average. ``limit`` stops after that many included
    records, taking records in order, so an excluded row does not shrink the
    window. For a pre-fix prediction row (no unit marker, ``prediction_id``
    set) the free-form prose fields are left empty and ``prose_trusted`` is
    False: the stored text may state the old ×100 return in any wording, which
    no filter can enforce. Storage is not touched; marked and trade rows keep
    their prose.
    """
    out = HydratedPostmortems(postmortems=[], included=[], excluded=0)
    for r in records:
        if limit is not None and len(out.postmortems) >= limit:
            break
        full = r.get("full_analysis") or {}
        if not full:
            continue
        ret = stored_return_fraction(r)
        if ret is None:
            out.excluded += 1
            continue
        legacy_prediction = (
            full.get("actual_return_unit") is None and r.get("prediction_id") is not None
        )
        prose = (lambda v: "") if legacy_prediction else (lambda v: v)
        try:
            out.postmortems.append(PostMortem(
                trade_id=full.get("trade_id", 0),
                ticker=full.get("ticker", r.get("ticker", "")),
                direction=full.get("direction", ""),
                outcome=full.get("outcome", r.get("outcome", "")),
                actual_return=ret,
                data_at_decision=full.get("data_at_decision", {}),
                thesis_at_decision=full.get("thesis_at_decision", ""),
                sanity_results_at_decision=full.get("sanity_results_at_decision", {}),
                what_actually_happened=prose(full.get("what_actually_happened", "")),
                price_path=full.get("price_path", []),
                failure_category=full.get("failure_category", r.get("failure_category", "")),
                root_cause=prose(full.get("root_cause", r.get("root_cause", ""))),
                which_signals_were_wrong=full.get("which_signals_were_wrong", []),
                which_signals_were_right=full.get("which_signals_were_right", []),
                what_we_missed=prose(full.get("what_we_missed", r.get("what_we_missed", ""))),
                recommended_fix=prose(full.get("recommended_fix", r.get("recommended_fix", ""))),
                confidence_in_analysis=full.get("confidence_in_analysis", 0.5),
                generated_at=full.get("generated_at", ""),
                prose_trusted=not legacy_prediction,
            ))
            out.included.append(r)
        except Exception:
            continue
    return out


# ── Table Setup ───────────────────────────────────────────────────────────


# Output cap for the two postmortem LLM calls. They used to inherit the 3090
# client's 6000-token default; on the RESEARCH tier (x99-hex, 512 default)
# they would silently be cut, so the cap is explicit.
POSTMORTEM_NUM_PREDICT = 2000

def _ensure_tables(engine: Engine) -> None:
    """Create the trade_postmortems table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trade_postmortems (
                id              SERIAL PRIMARY KEY,
                trade_id        INT,
                prediction_id   TEXT,
                ticker          TEXT,
                outcome         TEXT,
                failure_category TEXT,
                root_cause      TEXT,
                signals_wrong   JSONB,
                signals_right   JSONB,
                what_we_missed  TEXT,
                recommended_fix TEXT,
                full_analysis   JSONB,
                confidence      NUMERIC,
                generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_postmortem_ticker
                ON trade_postmortems (ticker, generated_at DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_postmortem_category
                ON trade_postmortems (failure_category, generated_at DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_postmortem_trade
                ON trade_postmortems (trade_id) WHERE trade_id IS NOT NULL
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_postmortem_prediction
                ON trade_postmortems (prediction_id) WHERE prediction_id IS NOT NULL
        """))


# ── 1. Generate Post-Mortem for a Trade ───────────────────────────────────

def generate_postmortem(
    engine: Engine,
    trade_id: int,
    *,
    narrate: bool = True,
    update_row_id: int | None = None,
) -> PostMortem | None:
    """Generate a full post-mortem for a failed options trade.

    Loads the recommendation, reconstructs decision-time state, loads
    the actual price path, classifies the failure, and (``narrate``) uses
    the LLM to narrate it. Without narration, or when the LLM is not
    reachable, the row is a rule-based record (``analysis_method='rules'``).

    Args:
        engine: SQLAlchemy engine.
        trade_id: options_recommendations.id
        narrate: Ask the LLM for the narrative fields.
        update_row_id: Narrate this existing trade_postmortems row in place
            instead of inserting (see ``narrate_postmortem``).

    Returns:
        PostMortem, or None if the trade is not found, not a failure, or
        nothing was written (a record already exists; a narration produced
        no LLM narrative).
    """
    _ensure_tables(engine)

    # Load the recommendation
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, ticker, direction, strike, expiry, entry_price,
                   target_price, stop_loss, confidence, thesis,
                   sanity_status, generated_at, outcome, actual_return,
                   closed_at, dealer_context, signals, opposing_signals
            FROM options_recommendations
            WHERE id = :id
        """), {"id": trade_id}).fetchone()

    if not row:
        log.warning("Post-mortem: trade {id} not found", id=trade_id)
        return None

    (rec_id, ticker, direction, strike, expiry, entry_price,
     target_price, stop_loss, confidence, thesis,
     sanity_status, generated_at, outcome, actual_return,
     closed_at, dealer_context, signals_json, opposing_signals_json) = row

    decision_signals = _parse_json(signals_json) or []
    decision_opposing = _parse_json(opposing_signals_json) or []

    # Only generate for failures
    if outcome not in ("LOSS", "EXPIRED"):
        log.debug("Post-mortem: trade {id} outcome={o} — not a failure", id=trade_id, o=outcome)
        return None

    # Same contract as the prediction path below: an entry price nobody
    # recorded stays None rather than becoming a stated $0.00 entry.
    entry_f = float(entry_price) if entry_price is not None else None
    strike_f = float(strike) if strike else 0.0
    target_f = float(target_price) if target_price else 0.0
    # A LOSS/EXPIRED row with no recorded return has no measured return. `0.0` here was
    # stored as a measured 0%, narrated as "Return: +0.00%", averaged into the lessons
    # report and emitted as realized_pnl (#959). Write nothing instead, as the prediction
    # path does; batch_postmortem does not select such rows either.
    ret_f = _finite_or_none(actual_return)
    if ret_f is None:
        log.warning(
            "Post-mortem: trade {id} is a {o} with no recorded return (actual_return={r!r}); "
            "not generating, a missing return is not a 0% return",
            id=trade_id, o=outcome, r=actual_return,
        )
        return None
    actual_ret = ret_f
    float(confidence) if confidence else 0.0

    # Parse sanity_status
    sanity = _parse_json(sanity_status)
    thesis_str = thesis or ""
    dealer_ctx = dealer_context or ""

    # Reconstruct data state at decision time
    data_at_decision = _reconstruct_decision_data(
        engine, ticker, generated_at, sanity, dealer_ctx,
    )

    # Surface the actual scanner signals that drove the trade — these are
    # persisted on options_recommendations as of migration 0051. Older
    # rows may have NULLs, which the parse helper turns into empty lists.
    if decision_signals:
        data_at_decision["decision_signals"] = decision_signals
    if decision_opposing:
        data_at_decision["decision_signals_opposing"] = decision_opposing

    # Load price path from entry to outcome, tracking which source supplied it
    price_path, price_source = _load_price_path_with_source(
        engine, ticker, generated_at, closed_at or expiry,
    )

    # Underlying spot at decision/entry time (PIT-safe, same price source as
    # the path). options_recommendations.entry_price is the option premium, NOT
    # underlying spot — never mix units when evaluating the underlying path.
    # Only data known at generated_at may anchor it (Codex HOLD 6011903681),
    # and reconstructed scanner context never does (Codex HOLD 6030815214);
    # when nothing qualifies, the reason is kept with the record, not a guess.
    underlying_spot, spot_note = _load_underlying_entry_spot(
        engine, ticker, generated_at, data_at_decision, price_source=price_source,
    )
    if underlying_spot is not None:
        data_at_decision["underlying_entry_spot"] = underlying_spot
        data_at_decision["underlying_entry_spot_basis"] = spot_note
    else:
        data_at_decision["underlying_entry_spot_unavailable"] = spot_note

    # Classify the failure
    category, root_cause, signals_wrong, signals_right, what_missed = _classify_failure(
        ticker=ticker,
        direction=direction,
        entry_price=entry_f,
        strike=strike_f,
        target=target_f,
        expiry=expiry,
        outcome=outcome,
        actual_return=actual_ret,
        price_path=price_path,
        data_at_decision=data_at_decision,
        sanity=sanity,
        underlying_entry_price=underlying_spot,
        generated_at=generated_at,
    )

    # What actually happened (factual summary)
    what_happened = _summarise_what_happened(
        ticker, direction, entry_f, strike_f, outcome, actual_ret, price_path,
    )

    # LLM narrative, or the rule-based record. The narrative sees only active
    # sanity layers; the stored data_at_decision keeps the raw record.
    what_missed_final, recommended_fix, analysis_confidence, analysis_method = _narrative_fields(
        narrate,
        rule_what_missed=what_missed,
        ticker=ticker,
        direction=direction,
        thesis=thesis_str,
        outcome=outcome,
        actual_return=actual_ret,
        category=category,
        root_cause=root_cause,
        what_happened=what_happened,
        data_at_decision={**data_at_decision, "sanity_status": _active_sanity(sanity)},
        price_path=price_path,
        signals_wrong=signals_wrong,
        signals_right=signals_right,
        decision_signals=decision_signals,
        decision_opposing=decision_opposing,
    )

    now = datetime.now(timezone.utc)
    miss_at = _as_utc(closed_at)

    pm = PostMortem(
        analysis_method=analysis_method,
        miss_at=miss_at.isoformat() if miss_at else None,
        trade_id=trade_id,
        ticker=ticker,
        direction=direction,
        outcome=outcome,
        actual_return=actual_ret,
        data_at_decision=data_at_decision,
        thesis_at_decision=thesis_str,
        sanity_results_at_decision=sanity,
        what_actually_happened=what_happened,
        price_path=price_path,
        failure_category=category,
        root_cause=root_cause,
        which_signals_were_wrong=signals_wrong,
        which_signals_were_right=signals_right,
        what_we_missed=what_missed_final,
        recommended_fix=recommended_fix,
        confidence_in_analysis=analysis_confidence,
        generated_at=now.isoformat(),
    )

    # Persist
    if not _persist_postmortem(engine, pm, trade_id=trade_id, prediction_id=None, update_row_id=update_row_id):
        return None

    # SYNTH-C / SYNTH-41 — non-fatal PostmortemCompleted fanout for the
    # TRADE path. The contagion-feedback postmortem already emits its
    # own contract (see ``apply_contagion_feedback`` below) — this hook
    # is strictly for the trade post-mortem pipeline. Only a new record of
    # a recent miss emits (POSTMORTEM_EVENT_WINDOW_DAYS); a narration does not.
    if update_row_id is None and _miss_is_recent(closed_at):
        _emit_trade_postmortem(pm=pm, trade_id=trade_id)

    log.info(
        "Post-mortem generated for trade {id} ({t} {d}): {cat}",
        id=trade_id, t=ticker, d=direction, cat=category,
    )
    return pm


# ── 2. Generate Post-Mortem for an Oracle Prediction ──────────────────────

# SQL twins of `_finite_or_none`, for the batch selectors. `IS NOT NULL` alone still admits NaN and
# +/-Infinity (oracle_predictions.pnl_pct is DOUBLE PRECISION; options_recommendations.actual_return
# is NUMERIC, which also holds NaN), and a row the writer then refuses stays selectable, so enough
# newer ones fill every batch's LIMIT and older valid rows never advance (Codex hold on #1486).
# In PostgreSQL NaN compares equal to itself, so NOT IN excludes it. A measured 0 stays selected.
_NON_FINITE_FLOAT8_SQL = "'NaN'::float8, 'Infinity'::float8, '-Infinity'::float8"
FINITE_PNL_PCT_SQL = "pnl_pct IS NOT NULL AND pnl_pct::float8 NOT IN (" + _NON_FINITE_FLOAT8_SQL + ")"
# options_recommendations.actual_return is unconstrained NUMERIC, so it is compared in NUMERIC and never
# cast to float8: a value outside float8's range (e.g. 1e400) makes `::float8` raise and abort the whole
# selection, starving every older valid row (Codex hold on #1488). PostgreSQL orders NaN and +/-Infinity
# above every finite NUMERIC, so `ABS(x) < 1e300` rejects them and anything the writer's float() cannot
# hold, and keeps every real return (a return is a small fraction) including a measured 0.
FINITE_ACTUAL_RETURN_SQL = "actual_return IS NOT NULL AND ABS(actual_return) < 1e300"


def _finite_or_none(value: Any) -> float | None:
    """``float(value)`` when it is a finite number, else None (missing, blank or NaN/inf)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def generate_prediction_postmortem(
    engine: Engine,
    prediction_id: str,
    *,
    narrate: bool = True,
    update_row_id: int | None = None,
) -> PostMortem | None:
    """Generate a full post-mortem for a failed oracle prediction.

    Same analysis pipeline as trade post-mortems but queries the
    oracle_predictions table instead. ``narrate`` and ``update_row_id``
    as in ``generate_postmortem``.

    Args:
        engine: SQLAlchemy engine.
        prediction_id: oracle_predictions.id (text hash).

    Returns:
        PostMortem, or None if the prediction is not found, not a failure,
        or nothing was written.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, ticker, direction, target_price, entry_price, expiry,
                   confidence, expected_move_pct, model_name, signals,
                   anti_signals, flow_context, verdict, actual_price,
                   actual_move_pct, pnl_pct, scored_at, score_notes,
                   created_at
            FROM """ + VISIBLE_ORACLE_PREDICTIONS + """ oracle_predictions
            WHERE id = :id
        """), {"id": prediction_id}).fetchone()

    if not row:
        log.warning("Post-mortem: prediction {id} not found", id=prediction_id)
        return None

    # NOTE: row unpacking matches the SELECT order. The 3rd column is
    # `direction` ("CALL" / "PUT" / etc.). Previously this was named
    # `signal_type` here, leaving the `direction` symbol undefined further
    # down (line 336/346) and producing "name 'direction' is not defined"
    # on every astrogrid prediction.
    (pred_id, ticker, direction, target_price, entry_price, expiry,
     confidence, expected_move, model_name, signals_json,
     anti_signals_json, flow_context_json, verdict, actual_price,
     actual_move_pct, pnl_pct, scored_at, score_notes,
     created_at) = row

    # Only generate for failures
    if verdict not in ("miss",):
        log.debug(
            "Post-mortem: prediction {id} verdict={v} — not a failure",
            id=prediction_id, v=verdict,
        )
        return None

    # An unmeasured entry price and an unstated confidence are not zeros.
    # `entry_f = 0.0` narrated "entered at $0.00" and `conf = 0.0` narrated
    # "with 0% confidence" into the LLM prompt below, both of which read as
    # findings about the prediction rather than as missing data.
    entry_f = float(entry_price) if entry_price is not None else None
    target_f = float(target_price) if target_price else 0.0
    # oracle_predictions.pnl_pct is stored in PERCENT POINTS (-50.24 means
    # -50.24%), the same unit as actual_move_pct (2,317,610 of 2,317,610
    # scored rows agree). PostMortem.actual_return is a FRACTION everywhere
    # else (the trade path reads options_recommendations.actual_return) and the
    # {:+.2%} formatters and PostmortemCompleted.realized_pnl expect one, so
    # convert here. Without it a -3.81% miss read "-381.00%" and the lessons
    # report averaged two units together.
    # A prediction scored a miss with no recorded return has no measured return. Storing
    # `0.0` there stored a measured 0% (narrated as "Return: +0.00%", averaged into the
    # lessons report and emitted as realized_pnl), and #1471's reader cannot tell that zero
    # from a real one afterwards. Without a return there is nothing true to analyse here, so
    # no post-mortem is written; batch_postmortem does not select such rows either.
    pnl_f = _finite_or_none(pnl_pct)
    if pnl_f is None:
        log.warning(
            "Post-mortem: prediction {id} is a miss with no recorded return (pnl_pct={p!r}); "
            "not generating, a missing return is not a 0% return",
            id=prediction_id, p=pnl_pct,
        )
        return None
    actual_ret = pnl_f / 100.0
    conf = float(confidence) if confidence is not None else None

    signals = _parse_json(signals_json)
    anti_signals = _parse_json(anti_signals_json)
    flow_context = _parse_json(flow_context_json)

    # Build decision-time data from prediction signals
    data_at_decision = {
        "model_name": model_name,
        "confidence": conf,
        "expected_move_pct": float(expected_move) if expected_move else 0.0,
        "signals": signals,
        "anti_signals": anti_signals,
        "flow_context": flow_context,
    }

    # Build a thesis string from the prediction context
    signal_names = []
    if isinstance(signals, list):
        signal_names = [s.get("name", "") for s in signals if isinstance(s, dict)]
    conf_str = "unstated" if conf is None else f"{conf:.0%}"
    thesis_str = (
        f"{model_name} predicted {direction} on {ticker} with "
        f"{conf_str} confidence based on signals: {', '.join(signal_names[:5])}"
    )

    # Load price path
    price_path = _load_price_path(engine, ticker, created_at, scored_at or expiry)

    # Classify
    category, root_cause, signals_wrong, signals_right, what_missed = _classify_prediction_failure(
        ticker=ticker,
        direction=direction,
        entry_price=entry_f,
        target=target_f,
        expiry=expiry,
        actual_price=float(actual_price) if actual_price else None,
        actual_move_pct=float(actual_move_pct) if actual_move_pct else None,
        signals=signals,
        anti_signals=anti_signals,
        price_path=price_path,
    )

    what_happened = _summarise_what_happened(
        ticker, direction, entry_f, target_f,
        "miss", actual_ret, price_path,
    )

    what_missed_final, recommended_fix, analysis_confidence, analysis_method = _narrative_fields(
        narrate,
        rule_what_missed=what_missed,
        ticker=ticker,
        direction=direction,
        thesis=thesis_str,
        outcome="miss",
        actual_return=actual_ret,
        category=category,
        root_cause=root_cause,
        what_happened=what_happened,
        data_at_decision=data_at_decision,
        price_path=price_path,
        signals_wrong=signals_wrong,
        signals_right=signals_right,
    )

    now = datetime.now(timezone.utc)
    miss_at = _as_utc(scored_at)

    pm = PostMortem(
        analysis_method=analysis_method,
        miss_at=miss_at.isoformat() if miss_at else None,
        trade_id=0,  # no trade_id for predictions
        ticker=ticker,
        direction=direction,
        outcome="miss",
        actual_return=actual_ret,
        data_at_decision=data_at_decision,
        thesis_at_decision=thesis_str,
        sanity_results_at_decision={},
        what_actually_happened=what_happened,
        price_path=price_path,
        failure_category=category,
        root_cause=root_cause,
        which_signals_were_wrong=signals_wrong,
        which_signals_were_right=signals_right,
        what_we_missed=what_missed_final,
        recommended_fix=recommended_fix,
        confidence_in_analysis=analysis_confidence,
        generated_at=now.isoformat(),
    )

    if not _persist_postmortem(
        engine, pm, trade_id=None, prediction_id=prediction_id, update_row_id=update_row_id,
    ):
        return None

    # Only a new record of a recent miss emits (POSTMORTEM_EVENT_WINDOW_DAYS); a narration does not.
    if update_row_id is None and _miss_is_recent(scored_at):
        _emit_prediction_postmortem(
            pm=pm,
            prediction_id=prediction_id,
            model_name=model_name,
        )

    log.info(
        "Post-mortem {how} for prediction {id} ({t} {d}): {cat}",
        how="narrated" if update_row_id is not None else f"recorded ({analysis_method})",
        id=prediction_id, t=ticker, d=direction, cat=category,
    )
    return pm


def narrate_postmortem(engine: Engine, row_id: int) -> PostMortem | None:
    """LLM-narrate one stored rule-based postmortem in place.

    The miss is re-read from its source and re-classified (the same deterministic
    classifier), then the LLM writes the narrative; the row's classification, narrative
    fields and ``full_analysis`` are rewritten together, so they describe one analysis
    (the category can differ from the first record only if the source data changed
    since; ``full_analysis.narrated_at`` says when). ``generated_at`` (first written)
    is kept and no PostmortemCompleted is emitted (the record already emitted, or was
    too old to).
    Returns None and changes nothing when the row is not a rule-based record or the
    LLM produced no narrative.
    """
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT tp.trade_id, tp.prediction_id FROM trade_postmortems tp "
            "WHERE tp.id = :id AND " + UNNARRATED_SQL
        ), {"id": int(row_id)}).fetchone()
    if not row:
        return None
    trade_id, prediction_id = row
    if prediction_id is not None:
        return generate_prediction_postmortem(engine, prediction_id, narrate=True, update_row_id=int(row_id))
    if trade_id is not None:
        return generate_postmortem(engine, int(trade_id), narrate=True, update_row_id=int(row_id))
    return None


# ── 3. Batch Post-Mortem ──────────────────────────────────────────────────

_WAITING_COUNT_BOUND = 1000


_WAITING_COUNT_TIMEOUT_S = 3


def _count_waiting(
    engine: Engine,
    selections: dict[str, str],
    params: dict[str, Any],
    finish_by: float | None = None,
) -> str:
    """"trades=N, predictions=M" still eligible, each counted up to ``_WAITING_COUNT_BOUND`` ("1000+").

    Diagnostics must never push the batch past its own time budget: a source is not counted when fewer than
    ``_WAITING_COUNT_TIMEOUT_S + 1`` seconds remain before ``finish_by`` ("skipped"), and every count runs under
    a ``_WAITING_COUNT_TIMEOUT_S`` statement timeout. Never raises: a failed count reads "unknown".
    """
    cutoff_params = {"cutoff": params["cutoff"]}
    parts: list[str] = []
    for name, base_sql in selections.items():
        if finish_by is not None and time.monotonic() + _WAITING_COUNT_TIMEOUT_S + 1 > finish_by:
            parts.append(f"{name}=skipped (no time left)")
            continue
        try:
            with engine.connect() as conn:
                conn.execute(text("SET LOCAL statement_timeout = '" + str(_WAITING_COUNT_TIMEOUT_S * 1000) + "ms'"))
                n = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM (" + base_sql + "\n        LIMIT "
                        + str(_WAITING_COUNT_BOUND + 1) + ") AS waiting"
                    ),
                    cutoff_params,
                ).scalar()
            n = int(n)
            parts.append(f"{name}={_WAITING_COUNT_BOUND}+" if n > _WAITING_COUNT_BOUND else f"{name}={n}")
        except Exception as exc:
            log.debug("Post-mortem: waiting count for {k} failed: {e}", k=name, e=str(exc))
            parts.append(f"{name}=unknown")
    return ", ".join(parts)


def batch_postmortem(
    engine: Engine,
    days: int = 30,
    limit: int | None = None,
    deadline: float | None = None,
    finish_by: float | None = None,
) -> list[PostMortem]:
    """Record every failed trade and prediction of the last N days, then narrate the newest.

    Two phases (owner decision 2026-11-27, "every miss gets a record"):

    1. Record: every eligible failure in the window with no postmortem gets a
       rule-based record (no LLM, ~20 ms a row), newest first, up to
       ``RECORD_PHASE_MAX_ROWS`` per source. Nothing in the window ages out
       unrecorded while the batch runs.
    2. Narrate: rule-based records in the window (including pre-2026-11-27
       rows holding ``LEGACY_LLM_FALLBACK_TEXT``) are LLM-narrated in place,
       newest miss first, up to ``limit`` rows, until ``deadline``. Records
       left un-narrated stay labelled ``analysis_method='rules'``.

    Misses older than the window are not touched here; recording them is a
    separate, explicit run (``scripts/postmortem_backfill.py``).

    Args:
        engine: SQLAlchemy engine.
        days: Lookback window in days, on the miss's scored date (trade: closed date).
        limit: Optional cap on rows NARRATED per run (phase 2). Recording is
            bounded by ``RECORD_PHASE_MAX_ROWS`` and the deadline only.

    deadline: ``time.monotonic()`` value after which no NEW row is started (a row already running
        finishes). The caller picks it so the batch ends inside its own time budget: rows left over are
        simply picked up by the next run (the ``NOT IN trade_postmortems`` selection), and the batch
        completes and reports on time instead of being abandoned by its wrapper and finishing late.
    finish_by: ``time.monotonic()`` value by which the whole call must return (the wrapper's own limit minus a
        guard). The "still waiting" diagnostics run only if they fit before it, so they can never push the
        batch past its budget. Trades and predictions are served alternately, so a long trade queue cannot
        consume the window before predictions start.

    Returns:
        list of PostMortem objects: the records written, then the rows narrated.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)
    postmortems: list[PostMortem] = []

    # Phase 1 selects every unrecorded failure in the window (bounded per source).
    trade_sql = """
        SELECT id FROM options_recommendations
        WHERE outcome IN ('LOSS', 'EXPIRED')
          AND closed_at >= :cutoff
          AND """ + FINITE_ACTUAL_RETURN_SQL + """
          AND id NOT IN (
              SELECT trade_id FROM trade_postmortems
              WHERE trade_id IS NOT NULL
          )
        ORDER BY closed_at DESC
    """
    pred_sql = """
        SELECT id FROM """ + VISIBLE_ORACLE_PREDICTIONS + """ oracle_predictions
        WHERE verdict = 'miss'
          AND scored_at >= :cutoff
          AND """ + FINITE_PNL_PCT_SQL + """
          AND id NOT LIKE 'astrogrid:%'
          AND id NOT IN (
              SELECT prediction_id FROM trade_postmortems
              WHERE prediction_id IS NOT NULL
          )
        ORDER BY scored_at DESC
    """
    params: dict[str, Any] = {"cutoff": cutoff, "rec_lim": int(RECORD_PHASE_MAX_ROWS)}
    # The same selections without the batch LIMIT, for the bounded "how many are still waiting" count below.
    remaining_sql = {"trades": trade_sql, "predictions": pred_sql}
    trade_sql += "\n        LIMIT :rec_lim"
    pred_sql += "\n        LIMIT :rec_lim"

    with engine.connect() as conn:
        trade_rows = conn.execute(text(trade_sql), params).fetchall()
        pred_rows = conn.execute(text(pred_sql), params).fetchall()

    stopped_at_deadline = False

    def _past_deadline() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    # Phase 1 — record. Trades and predictions alternate: a long queue of one source cannot consume the whole
    # window before the other starts (a fixed trades-then-predictions order starved predictions under a deadline).
    recorded = 0
    for trade_row, pred_row in zip_longest(trade_rows, pred_rows):
        if trade_row is not None:
            if _past_deadline():
                stopped_at_deadline = True
                break
            (trade_id,) = trade_row
            try:
                pm = generate_postmortem(engine, trade_id, narrate=False)
                if pm:
                    postmortems.append(pm)
                    recorded += 1
            except Exception as exc:
                log.warning("Post-mortem failed for trade {id}: {e}", id=trade_id, e=str(exc))
        if pred_row is not None:
            if _past_deadline():
                stopped_at_deadline = True
                break
            (pred_id,) = pred_row
            try:
                pm = generate_prediction_postmortem(engine, pred_id, narrate=False)
                if pm:
                    postmortems.append(pm)
                    recorded += 1
            except Exception as exc:
                log.warning("Post-mortem failed for prediction {id}: {e}", id=pred_id, e=str(exc))

    # Aggregate patterns over the records (one per failure)
    if postmortems:
        _log_aggregate_patterns(postmortems, days)

    # Phase 2 — narrate the newest rule-based records in the window, in place.
    narrated = 0
    if not stopped_at_deadline:
        narrate_sql = (
            "SELECT tp.id, op.scored_at AS miss_at FROM trade_postmortems tp"
            " JOIN " + VISIBLE_ORACLE_PREDICTIONS + " op ON op.id = tp.prediction_id"
            " WHERE tp.generated_at >= :cutoff AND op.scored_at >= :cutoff AND " + UNNARRATED_SQL +
            " AND " + NEWEST_COPY_SQL +
            " UNION ALL"
            " SELECT tp.id, orr.closed_at AS miss_at FROM trade_postmortems tp"
            " JOIN options_recommendations orr ON orr.id = tp.trade_id"
            " WHERE tp.generated_at >= :cutoff AND orr.closed_at >= :cutoff AND " + UNNARRATED_SQL +
            " AND " + NEWEST_COPY_SQL +
            " ORDER BY miss_at DESC, id DESC"
        )
        narrate_params: dict[str, Any] = {"cutoff": cutoff}
        if limit is not None and int(limit) > 0:   # 0 or less: no cap (the old off-switch)
            narrate_sql += " LIMIT :lim"
            narrate_params["lim"] = int(limit)
        try:
            with engine.connect() as conn:
                narrate_rows = conn.execute(text(narrate_sql), narrate_params).fetchall()
        except Exception as exc:
            log.warning("Post-mortem: narration selection failed: {e}", e=str(exc))
            narrate_rows = []
        for row in narrate_rows:
            if _past_deadline():
                stopped_at_deadline = True
                break
            row_id = row[0]
            try:
                pm = narrate_postmortem(engine, row_id)
                if pm:
                    postmortems.append(pm)
                    narrated += 1
            except Exception as exc:
                log.warning("Post-mortem narration failed for row {id}: {e}", id=row_id, e=str(exc))

    # How many eligible rows are STILL waiting after this run, counted up to a bound (an unbounded NOT IN
    # count over the whole corpus is not something a nightly step should run). A batch that processes fewer
    # rows than arrive falls behind; this makes that visible in the log instead of silent.
    waiting = _count_waiting(engine, remaining_sql, params, finish_by)

    log.info(
        "Batch post-mortem complete: {n} failures analysed over {d} days ({r} recorded, {nr} narrated)"
        "{lim_note}{dl_note}; still waiting after this run: {w}",
        n=len(postmortems), d=days, r=recorded, nr=narrated,
        w=waiting,
        lim_note=f" (narration limit={limit})" if limit else "",
        dl_note=(
            " (stopped at its deadline; the remaining rows are picked up by the next run)"
            if stopped_at_deadline else ""
        ),
    )
    return postmortems


# ── 4. Lessons Learned Synthesis ──────────────────────────────────────────

def generate_lessons_learned(engine: Engine, postmortems: list[PostMortem]) -> str:
    """Synthesise actionable lessons from a batch of post-mortems.

    Uses LLM to identify systemic patterns and produce specific
    recommendations. Falls back to rule-based analysis if LLM
    is unavailable.

    Args:
        engine: SQLAlchemy engine.
        postmortems: List of PostMortem objects to analyse.

    Returns:
        str: Formatted lessons-learned report.
    """
    if not postmortems:
        return "No post-mortems to analyse — nothing to learn from."

    # Aggregate stats
    total = len(postmortems)
    categories = Counter(pm.failure_category for pm in postmortems)
    tickers = Counter(pm.ticker for pm in postmortems)
    directions = Counter(pm.direction for pm in postmortems)

    all_wrong = []
    all_right = []
    for pm in postmortems:
        all_wrong.extend(pm.which_signals_were_wrong)
        all_right.extend(pm.which_signals_were_right)
    wrong_counts = Counter(all_wrong)
    right_counts = Counter(all_right)

    avg_return = sum(pm.actual_return for pm in postmortems) / total

    # Try LLM synthesis
    llm_report = _get_llm_lessons_learned(postmortems, categories, tickers, wrong_counts, right_counts)
    if llm_report:
        return llm_report

    # Rule-based fallback
    lines = [
        "=" * 60,
        "  GRID Post-Mortem — Lessons Learned",
        "=" * 60,
        "",
        f"Failures analysed: {total}",
        f"Average return:    {avg_return:+.2%}",
        "",
        "-- Failure Category Breakdown --",
    ]
    for cat, count in categories.most_common():
        pct = count / total * 100
        lines.append(f"  {cat:35s}  {count:3d}  ({pct:.0f}%)")

    lines.extend(["", "-- Most Affected Tickers --"])
    for ticker, count in tickers.most_common(5):
        lines.append(f"  {ticker:8s}  {count} failures")

    lines.extend(["", "-- Signals Most Often Wrong --"])
    for sig, count in wrong_counts.most_common(5):
        lines.append(f"  {sig:25s}  wrong {count}x")

    lines.extend(["", "-- Signals Most Often Right (but overridden) --"])
    for sig, count in right_counts.most_common(5):
        lines.append(f"  {sig:25s}  right {count}x")

    # Actionable recommendations
    lines.extend(["", "-- Recommendations --"])

    top_cat = categories.most_common(1)[0] if categories else ("unknown", 0)
    if top_cat[0] == "wrong_signal" and top_cat[1] > total * 0.4:
        lines.append("  [!] >40% of failures are wrong signals — review scanner signal quality.")
        if wrong_counts:
            worst_sig = wrong_counts.most_common(1)[0][0]
            lines.append(f"      Worst offender: '{worst_sig}' — consider reducing its weight.")

    if top_cat[0] == "right_signal_wrong_timing" and top_cat[1] > total * 0.3:
        lines.append("  [!] >30% are timing failures — consider longer expiry windows.")
        lines.append("      Direction was right but options expired too early.")

    if top_cat[0] == "external_shock" and top_cat[1] > total * 0.25:
        lines.append("  [!] External shocks causing >25% of failures.")
        lines.append("      Add event calendar awareness (FOMC, earnings, OpEx).")

    if top_cat[0] == "bad_data" and top_cat[1] > total * 0.1:
        lines.append("  [!] Bad data contributing to failures — check data freshness pipeline.")

    # Ticker-specific
    if tickers:
        worst_ticker = tickers.most_common(1)[0]
        if worst_ticker[1] >= 3:
            lines.append(
                f"  [~] {worst_ticker[0]} has {worst_ticker[1]} failures — "
                f"consider excluding or reducing position sizes."
            )

    # Direction bias
    if directions.get("CALL", 0) > directions.get("PUT", 0) * 2:
        lines.append("  [~] Strong CALL bias in failures — check for bullish overconfidence.")
    elif directions.get("PUT", 0) > directions.get("CALL", 0) * 2:
        lines.append("  [~] Strong PUT bias in failures — check for bearish overconfidence.")

    lines.extend(["", "=" * 60])
    return "\n".join(lines)


# ── Persistence ───────────────────────────────────────────────────────────

# Canonical producer tags for PostmortemCompleted events.
# Options-trade outcomes (options_recommendations / options scanner) are NOT
# produced by oracle models and must carry OPTIONS_RECOMMENDER_PRODUCER so
# oracle_weights skips oracle-model decay. Only oracle-prediction outcomes
# carrying an exact model name use ORACLE_MODEL_PRODUCER_PREFIX.
OPTIONS_RECOMMENDER_PRODUCER = "trading.options_recommender"
ORACLE_MODEL_PRODUCER_PREFIX = "oracle.model:"
_SELF_MODULE_NAME = "intelligence.postmortem"

_OPTIONS_PRODUCER_ALIASES: frozenset[str] = frozenset({
    "trading.options_recommender",
    "options_recommender",
    "options_recommendations",
    "discovery.options_scanner",
    "options_scanner",
})


def _oracle_model_producer(model_name: Any) -> str | None:
    """Format an oracle prediction's ``model_name`` as ``oracle.model:<name>``."""
    if not isinstance(model_name, str):
        return None
    name = model_name.strip()
    if not name:
        return None
    low = name.lower()
    if low in ("unknown", _SELF_MODULE_NAME) or any(ch.isspace() for ch in name):
        return None
    if name.startswith(ORACLE_MODEL_PRODUCER_PREFIX):
        return _normalize_producer_key(name)
    return f"{ORACLE_MODEL_PRODUCER_PREFIX}{name}"


def _normalize_producer_key(raw: Any) -> str | None:
    """Normalize a mapped producer identifier onto a canonical producer key.

    Recognizes only:
      - Options recommender / scanner producers -> ``"trading.options_recommender"``
      - Explicit oracle-model tags ``"oracle.model:<model_name>"``

    Returns ``None`` (→ skip) for unmapped, blank, or self-attributed values
    rather than passing arbitrary strings through.
    """
    if not isinstance(raw, str):
        return None
    val = raw.strip()
    if not val:
        return None
    low = val.lower()
    if low in (_SELF_MODULE_NAME, "unknown"):
        return None
    if low in _OPTIONS_PRODUCER_ALIASES:
        return OPTIONS_RECOMMENDER_PRODUCER
    if val.startswith(ORACLE_MODEL_PRODUCER_PREFIX):
        model_name = val[len(ORACLE_MODEL_PRODUCER_PREFIX):].strip()
        if (
            not model_name
            or model_name.lower() in ("unknown", _SELF_MODULE_NAME)
            or any(ch.isspace() for ch in model_name)
        ):
            return None
        return f"{ORACLE_MODEL_PRODUCER_PREFIX}{model_name}"
    return None


def _resolve_originating_producer(
    pm: "PostMortem",
    producer_module: str | None = None,
    *,
    is_options_trade: bool = False,
) -> str:
    """Resolve the canonical ``producer_module`` for a postmortem event.

    - Options-trade outcomes (derived from ``options_recommendations`` / the
      scanner) resolve to ``"trading.options_recommender"``.
    - Oracle-prediction outcomes with a valid ``model_name`` resolve to
      ``"oracle.model:<model_name>"``.
    - Unmapped explicit values or unattributed outcomes never pass through and
      never return ``"intelligence.postmortem"``; they log a warning and return
      ``"unknown"``.
    """
    if producer_module is not None:
        explicit = _normalize_producer_key(producer_module)
        if explicit:
            return explicit
        log.warning(
            "postmortem: unmapped producer_module={p!r} for trade {tid} ({t}); "
            "emitting producer_module='unknown'",
            p=producer_module, tid=pm.trade_id, t=pm.ticker,
        )
        return "unknown"

    if is_options_trade or (isinstance(pm.trade_id, int) and pm.trade_id > 0):
        return OPTIONS_RECOMMENDER_PRODUCER

    data = pm.data_at_decision if isinstance(pm.data_at_decision, dict) else {}

    oracle_model = _oracle_model_producer(data.get("model_name"))
    if oracle_model:
        return oracle_model

    for key in ("producer_module", "source_module"):
        resolved = _normalize_producer_key(data.get(key))
        if resolved:
            return resolved

    if "scanner_signals" in data or "decision_signals" in data:
        return OPTIONS_RECOMMENDER_PRODUCER

    log.warning(
        "postmortem: originating producer unknown for trade {tid} ({t}); "
        "emitting producer_module='unknown'",
        tid=pm.trade_id, t=pm.ticker,
    )
    return "unknown"


def _verdict_from_outcome(outcome: str) -> str:
    """Map free-text outcome strings onto the PostmortemCompleted verdict
    enum ({HIT, MISS, PARTIAL}). Trade postmortems only run on failures
    so the default is ``MISS``. ``EXPIRED`` options expired worthless
    (intrinsic <= 0, -100% return) and map to ``MISS`` (total loss).
    """
    if not outcome:
        return "MISS"
    s = str(outcome).strip().upper()
    if s in {"HIT", "WIN"}:
        return "HIT"
    if s in {"PARTIAL", "NEUTRAL"}:
        return "PARTIAL"
    return "MISS"


def _build_signals_used_from_pm(
    pm: "PostMortem",
) -> list:
    """Pull the SignalRef list out of a trade postmortem's decision snapshot.

    The journal stores signal names without trust/weight metadata for
    trade tickets, so we default both to 0.5 / 1.0 (neutral priors).
    Returns a list of ``contracts.schemas.SignalRef`` instances.
    """
    from uuid import uuid5, NAMESPACE_URL

    from contracts.schemas import SignalRef

    right = list(pm.which_signals_were_right or [])
    wrong = list(pm.which_signals_were_wrong or [])
    all_names = [f"right:{n}" for n in right] + [f"wrong:{n}" for n in wrong]

    refs: list[SignalRef] = []
    for name in all_names:
        try:
            refs.append(
                SignalRef(
                    signal_id=uuid5(NAMESPACE_URL, f"grid:pm:signal:{name}"),
                    source=name,
                    trust_at_prediction=0.5,
                    weight_at_prediction=1.0,
                )
            )
        except Exception:  # pragma: no cover — defensive only
            continue
    return refs


def _emit_trade_postmortem(
    *,
    pm: "PostMortem",
    trade_id: int,
    producer_module: str | None = None,
) -> None:
    """Non-fatal PostmortemCompleted emit for the trade postmortem path."""
    try:
        from decimal import Decimal
        from uuid import uuid5, NAMESPACE_URL

        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import PostmortemCompleted
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug("postmortem: contracts import failed: {e}", e=str(exc))
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    try:
        prediction_uuid = uuid5(
            NAMESPACE_URL, f"grid:pm:trade:{trade_id}:{pm.ticker}"
        )
    except Exception:
        return

    # A post-mortem with no finite return carries no realized P&L; a 0 here would be invented.
    realized = _finite_or_none(pm.actual_return)
    if realized is None:
        log.warning("postmortem: trade {id} has no finite return; not emitting realized_pnl", id=trade_id)
        return

    signals_used = _build_signals_used_from_pm(pm)
    # ``contributing_signal_ids`` must be a list of UUIDs only — reuse
    # the signal_id from each SignalRef so the two fields agree.
    contributing_ids = [r.signal_id for r in signals_used]
    origin_producer = _resolve_originating_producer(
        pm,
        producer_module=producer_module,
        is_options_trade=True,
    )

    try:
        _emit(
            PostmortemCompleted(
                producer_module=origin_producer,
                correlation_id=corr_id,
                prediction_id=prediction_uuid,
                ticker=str(pm.ticker).upper(),
                verdict=_verdict_from_outcome(pm.outcome),
                realized_pnl=Decimal(str(realized)),
                signals_used=signals_used,
                root_cause=str(pm.root_cause or "")[:2000],
                contributing_signal_ids=contributing_ids,
            )
        )
    except Exception as exc:  # non-fatal per SYNTH-C contract
        log.debug(
            "postmortem emit failed for trade {i}: {e}",
            i=trade_id, e=str(exc),
        )


def _emit_prediction_postmortem(
    *,
    pm: "PostMortem",
    prediction_id: str,
    model_name: str | None = None,
) -> None:
    """Non-fatal PostmortemCompleted emit for the oracle-prediction postmortem path.

    Attributes the event to ``oracle.model:<model_name>`` only when the
    prediction carries a valid oracle model name; otherwise emits ``"unknown"``
    so ``oracle_weights`` skips decay cleanly.
    """
    try:
        from decimal import Decimal
        from uuid import UUID, uuid5, NAMESPACE_URL

        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import PostmortemCompleted
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug("postmortem: contracts import failed: {e}", e=str(exc))
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    try:
        try:
            prediction_uuid = UUID(str(prediction_id))
        except (ValueError, TypeError):
            prediction_uuid = uuid5(
                NAMESPACE_URL, f"grid:pm:prediction:{prediction_id}:{pm.ticker}"
            )
    except Exception:
        return

    # A post-mortem with no finite return carries no realized P&L; a 0 here would be invented.
    realized = _finite_or_none(pm.actual_return)
    if realized is None:
        log.warning("postmortem: prediction {id} has no finite return; not emitting realized_pnl", id=prediction_id)
        return

    signals_used = _build_signals_used_from_pm(pm)
    contributing_ids = [r.signal_id for r in signals_used]

    resolved_model = model_name
    if not resolved_model and isinstance(pm.data_at_decision, dict):
        resolved_model = pm.data_at_decision.get("model_name")
    origin_producer = _oracle_model_producer(resolved_model)
    if not origin_producer:
        log.warning(
            "postmortem: oracle prediction {pid} ({t}) has no valid model_name; "
            "emitting producer_module='unknown'",
            pid=prediction_id, t=pm.ticker,
        )
        origin_producer = "unknown"

    try:
        _emit(
            PostmortemCompleted(
                producer_module=origin_producer,
                correlation_id=corr_id,
                prediction_id=prediction_uuid,
                ticker=str(pm.ticker).upper(),
                verdict=_verdict_from_outcome(pm.outcome),
                realized_pnl=Decimal(str(realized)),
                signals_used=signals_used,
                root_cause=str(pm.root_cause or "")[:2000],
                contributing_signal_ids=contributing_ids,
            )
        )
    except Exception as exc:  # non-fatal per SYNTH-C contract
        log.debug(
            "postmortem emit failed for prediction {i}: {e}",
            i=prediction_id, e=str(exc),
        )


def _store_postmortem(
    engine: Engine,
    pm: PostMortem,
    trade_id: int | None,
    prediction_id: str | None,
) -> bool:
    """Persist a post-mortem, at most one per prediction or trade. False when one already exists.

    The spring bulk run left 76,710 predictions with 2 to 34 rows: overlapping writers each passed the
    batch's ``NOT IN`` check before either inserted. A transaction-scoped advisory lock on the key makes
    the existence check and the insert atomic between writers on PostgreSQL.
    """
    if prediction_id is not None:
        key, where, ident = f"trade_postmortems:prediction:{prediction_id}", "prediction_id = :ident", str(prediction_id)
    elif trade_id is not None:
        key, where, ident = f"trade_postmortems:trade:{trade_id}", "trade_id = :ident", int(trade_id)
    else:
        key = where = ident = None
    with engine.begin() as conn:
        if key is not None:
            if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": key})
            existing = conn.execute(
                text("SELECT 1 FROM trade_postmortems WHERE " + where + " LIMIT 1"), {"ident": ident},
            ).fetchone()
            if existing is not None:
                log.info("Post-mortem: a record already exists for {k}; not writing another", k=key)
                return False
        conn.execute(text("""
            INSERT INTO trade_postmortems
                (trade_id, prediction_id, ticker, outcome, failure_category,
                 root_cause, signals_wrong, signals_right, what_we_missed,
                 recommended_fix, full_analysis, confidence, generated_at)
            VALUES
                (:trade_id, :prediction_id, :ticker, :outcome, :failure_category,
                 :root_cause, :signals_wrong, :signals_right, :what_we_missed,
                 :recommended_fix, :full_analysis, :confidence, :generated_at)
        """), {
            "trade_id": trade_id,
            "prediction_id": prediction_id,
            "ticker": pm.ticker,
            "outcome": pm.outcome,
            "failure_category": pm.failure_category,
            "root_cause": pm.root_cause,
            "signals_wrong": json.dumps(pm.which_signals_were_wrong),
            "signals_right": json.dumps(pm.which_signals_were_right),
            "what_we_missed": pm.what_we_missed,
            "recommended_fix": pm.recommended_fix,
            "full_analysis": json.dumps(pm.to_dict(), default=str),
            "confidence": pm.confidence_in_analysis,
            "generated_at": pm.generated_at,
        })

    # Postmortems no longer write ReasoningBank lessons (2026-11-07): lessons
    # come from official verdicts, one per verdict and both sides
    # (intelligence/reasoning_bank.py, "The rules"). A postmortem explains a
    # miss; it does not count it a second time.
    if not POSTMORTEM_WRITES_LESSONS:
        return True
    try:
        _emit_lesson_from_postmortem(
            engine, pm, trade_id=trade_id, prediction_id=prediction_id,
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(
            "postmortem: reasoning-lesson emit failed for {tid}/{pid}: {e}",
            tid=trade_id, pid=prediction_id, e=str(exc),
        )
    return True


def _update_postmortem_narration(engine: Engine, row_id: int, pm: PostMortem) -> bool:
    """Rewrite one rule-based row with its LLM narration. False when the row is no longer rule-based."""
    with engine.begin() as conn:
        result = conn.execute(text(
            "UPDATE trade_postmortems tp SET failure_category = :failure_category, root_cause = :root_cause,"
            " signals_wrong = :signals_wrong, signals_right = :signals_right,"
            " what_we_missed = :what_we_missed, recommended_fix = :recommended_fix,"
            " full_analysis = :full_analysis, confidence = :confidence"
            " WHERE tp.id = :id AND " + UNNARRATED_SQL
        ), {
            "id": int(row_id),
            "failure_category": pm.failure_category,
            "root_cause": pm.root_cause,
            "signals_wrong": json.dumps(pm.which_signals_were_wrong),
            "signals_right": json.dumps(pm.which_signals_were_right),
            "what_we_missed": pm.what_we_missed,
            "recommended_fix": pm.recommended_fix,
            "full_analysis": json.dumps({**pm.to_dict(), "narrated_at": pm.generated_at}, default=str),
            "confidence": pm.confidence_in_analysis,
        })
    rowcount = getattr(result, "rowcount", None)
    return not isinstance(rowcount, int) or rowcount > 0


def _persist_postmortem(
    engine: Engine,
    pm: PostMortem,
    *,
    trade_id: int | None,
    prediction_id: str | None,
    update_row_id: int | None,
) -> bool:
    """Insert a new record, or narrate an existing row in place. True when something was written."""
    if update_row_id is not None:
        if pm.analysis_method != ANALYSIS_LLM:
            return False  # the LLM gave no narrative: the rule-based row stands as it is
        return _update_postmortem_narration(engine, update_row_id, pm)
    # ``is not False``: a None from a stubbed store still reads as written.
    return _store_postmortem(engine, pm, trade_id=trade_id, prediction_id=prediction_id) is not False


def _narrative_fields(
    narrate: bool,
    *,
    rule_what_missed: str,
    root_cause: str,
    **llm_kwargs: Any,
) -> tuple[str, str, float | None, str]:
    """``(what_we_missed, recommended_fix, confidence, analysis_method)`` for a postmortem.

    With ``narrate`` the LLM writes them; when it is not reachable, or without ``narrate``, the row
    is a rule-based record: the classifier's own "what we missed", the root cause as the fix, and no
    analysis confidence (no analysis was made to be confident in). Before 2026-11-27 an unreachable
    LLM wrote ``LEGACY_LLM_FALLBACK_TEXT`` and a 0.5 confidence instead.
    """
    if narrate:
        narrative = _get_llm_postmortem(root_cause=root_cause, **llm_kwargs)
        if narrative.get("narrated", True):
            return (
                narrative.get("what_we_missed", rule_what_missed),
                narrative.get("recommended_fix", root_cause),
                narrative.get("confidence", 0.5),
                ANALYSIS_LLM,
            )
    return rule_what_missed, root_cause, None, ANALYSIS_RULES


def _emit_lesson_from_postmortem(
    engine: Engine,
    pm: PostMortem,
    *,
    trade_id: int | None,
    prediction_id: str | None,
) -> None:
    """Distill a failure ReasoningLesson from a PostMortem and write it.

    Imported lazily to avoid any chance of an import cycle and to keep
    the postmortem path runnable even if reasoning_bank is removed.
    """
    try:
        from intelligence.reasoning_bank import (
            ReasoningLesson,
            build_fingerprint_from_decision_data,
            write_reasoning_lesson,
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(
            "postmortem: reasoning_bank import failed: {e}", e=str(exc),
        )
        return

    horizon_days: int | None = None
    data = pm.data_at_decision if isinstance(pm.data_at_decision, dict) else {}
    raw_h = data.get("horizon_days")
    if raw_h is not None:
        try:
            horizon_days = int(raw_h)
        except (TypeError, ValueError):
            horizon_days = None

    try:
        fingerprint = build_fingerprint_from_decision_data(
            ticker=pm.ticker,
            direction=pm.direction,
            data_at_decision=data,
            horizon_days=horizon_days,
        )
    except Exception:
        fingerprint = {
            "ticker": str(pm.ticker or "").upper(),
            "direction": str(pm.direction or "").lower(),
        }

    title = (
        f"{pm.ticker} {pm.direction} {pm.failure_category}".strip()
    ) or "postmortem-failure"
    description = (pm.root_cause or "")[:1000]
    content = (
        f"{(pm.what_we_missed or '').strip()} "
        f"| fix: {(pm.recommended_fix or '').strip()}"
    ).strip()

    source_id: str | None
    if trade_id is not None:
        source_id = str(trade_id)
    elif prediction_id is not None:
        source_id = str(prediction_id)
    else:
        source_id = None

    lesson = ReasoningLesson(
        title=title,
        description=description,
        content=content,
        outcome_class="failure",
        condition_fingerprint=fingerprint,
        source_type="postmortem_failure",
        source_id=source_id,
    )

    write_reasoning_lesson(engine, lesson)


def record_success_lesson(
    engine: Engine,
    *,
    trade_id_or_prediction_id: int | str | None,
    ticker: str,
    direction: str,
    outcome: str,
    data_at_decision: dict[str, Any] | None,
    thesis_at_decision: str | None,
    what_worked: str,
    generalizable_takeaway: str,
    horizon_days: int | None = None,
) -> int | None:
    """Persist a success-side ReasoningLesson.

    Counterpart to the LLM postmortem flow but kept simple — the caller
    (typically the scoring path) hands in already-known facts; no LLM
    call. Returns the lesson id or ``None`` on failure (defensive —
    never raises).
    """
    try:
        from intelligence.reasoning_bank import (
            ReasoningLesson,
            build_fingerprint_from_decision_data,
            write_reasoning_lesson,
        )
    except Exception as exc:
        log.debug(
            "postmortem: reasoning_bank import failed: {e}", e=str(exc),
        )
        return None

    try:
        fingerprint = build_fingerprint_from_decision_data(
            ticker=ticker,
            direction=direction,
            data_at_decision=data_at_decision,
            horizon_days=horizon_days,
        )
    except Exception:
        fingerprint = {
            "ticker": str(ticker or "").upper(),
            "direction": str(direction or "").lower(),
        }

    title = (
        f"{ticker} {direction} {outcome} success".strip()
    ) or "postmortem-success"
    description = (
        thesis_at_decision or generalizable_takeaway or ""
    )[:1000]
    content = (
        f"what worked: {(what_worked or '').strip()} "
        f"| takeaway: {(generalizable_takeaway or '').strip()}"
    ).strip()

    source_id = (
        str(trade_id_or_prediction_id)
        if trade_id_or_prediction_id is not None
        else None
    )

    lesson = ReasoningLesson(
        title=title,
        description=description,
        content=content,
        outcome_class="success",
        condition_fingerprint=fingerprint,
        source_type="postmortem_success",
        source_id=source_id,
    )

    return write_reasoning_lesson(engine, lesson)


def load_postmortems_top_n(
    engine: Engine,
    n: int = 5,
    days: int = 30,
) -> list[dict[str, Any]]:
    """Fast path: top-N most recent post-mortems for the dashboard.

    Perf 2026-05-16: the dashboard only displays the top-5, but
    ``load_postmortems`` hydrates every matching row (93K+ in last 30 days
    on grid-svr → 24-31s on cold cache). This variant SELECTs LIMIT n
    rows server-side and skips the heavy ``full_analysis`` JSONB column,
    cutting the call from tens of seconds to <100ms.

    Use ``load_postmortems`` (full hydration) only when the consumer
    genuinely needs every field — e.g. the standalone Postmortems
    widget or analytic exports. Always prefer this fast path on the
    cold dashboard render.

    Args:
        engine: SQLAlchemy engine.
        n: Maximum rows to return (default 5 — what the dashboard displays).
        days: Lookback window in days.

    Returns:
        List of post-mortem dicts (without ``full_analysis``) ordered by
        generated_at DESC.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)

    query = """
        SELECT id, trade_id, prediction_id, ticker, outcome,
               failure_category, root_cause, signals_wrong, signals_right,
               what_we_missed, recommended_fix,
               confidence, generated_at
        FROM trade_postmortems tp
        WHERE """ + miss_on_or_after_sql(":cutoff") + """
        ORDER BY generated_at DESC
        LIMIT :n
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), {"cutoff": cutoff, "n": int(n)}).fetchall()

    results: list[dict[str, Any]] = []
    for r in rows:
        results.append({
            "id": r[0],
            "trade_id": r[1],
            "prediction_id": r[2],
            "ticker": r[3],
            "outcome": r[4],
            "failure_category": r[5],
            "root_cause": r[6],
            "signals_wrong": _parse_json(r[7]),
            "signals_right": _parse_json(r[8]),
            "what_we_missed": r[9],
            "recommended_fix": r[10],
            # full_analysis intentionally omitted — heavy JSONB blob, not
            # used by the dashboard's top-5 widget. Callers that need it
            # should use load_postmortems() instead.
            "full_analysis": {},
            "confidence": float(r[11]) if r[11] is not None else None,
            "generated_at": r[12].isoformat() if r[12] else None,
        })

    return results


def count_postmortems(engine: Engine, days: int = 30) -> int:
    """Cheap COUNT(*) for the dashboard's "total in last N days" widget.

    Avoids hydrating rows just to call len(). Pairs with
    ``load_postmortems_top_n`` on the dashboard cold path.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        n = conn.execute(text("""
            SELECT COUNT(*) FROM trade_postmortems tp
            WHERE """ + miss_on_or_after_sql(":cutoff") + """
        """), {"cutoff": cutoff}).scalar()
    return int(n or 0)


def postmortem_coverage(engine: Engine, days: int = 30) -> dict[str, Any]:
    """How many of the window's failures have a postmortem, and how many of those are narrated.

    The window is on the miss's own date, the population is exactly what the batch selects, and a
    failure with no record is reported as ``not_analysed`` (not "waiting": nothing guarantees a later
    run reaches it once it leaves the window). Scans oracle_predictions by scored date (no index on it
    today), so it belongs on cached or batch paths, not on a hot request.
    """
    cutoff = date.today() - timedelta(days=days)
    has_row = "EXISTS (SELECT 1 FROM trade_postmortems tp WHERE tp.{col} = src.id)"
    narrated_row = "EXISTS (SELECT 1 FROM trade_postmortems tp WHERE tp.{col} = src.id AND NOT " + UNNARRATED_SQL + ")"
    sources = {
        "predictions": (
            "FROM " + VISIBLE_ORACLE_PREDICTIONS + " src WHERE src.verdict = 'miss' AND src.scored_at >= :cutoff"
            " AND " + FINITE_PNL_PCT_SQL + " AND src.id NOT LIKE 'astrogrid:%'",
            "prediction_id",
        ),
        "trades": (
            "FROM options_recommendations src WHERE src.outcome IN ('LOSS', 'EXPIRED')"
            " AND src.closed_at >= :cutoff AND " + FINITE_ACTUAL_RETURN_SQL,
            "trade_id",
        ),
    }
    out: dict[str, Any] = {"window_days": int(days), "window_on": "miss date"}
    totals = {"eligible": 0, "recorded": 0, "narrated": 0}
    with engine.connect() as conn:
        for name, (from_where, col) in sources.items():
            row = conn.execute(text(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE " + has_row.format(col=col) + "),"
                " COUNT(*) FILTER (WHERE " + narrated_row.format(col=col) + ") " + from_where
            ), {"cutoff": cutoff}).fetchone()
            eligible, recorded, narrated = (int(v or 0) for v in row)
            out[name] = {
                "eligible": eligible, "recorded": recorded, "narrated": narrated,
                "not_analysed": eligible - recorded,
            }
            totals["eligible"] += eligible
            totals["recorded"] += recorded
            totals["narrated"] += narrated
    out.update(totals)
    out["not_analysed"] = totals["eligible"] - totals["recorded"]
    return out


def load_postmortems(
    engine: Engine,
    days: int = 30,
    ticker: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """Load stored post-mortems for the API.

    Args:
        engine: SQLAlchemy engine.
        days: Lookback window.
        ticker: Optional ticker filter.
        category: Optional failure_category filter.

    Returns:
        list of post-mortem dicts.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)

    query = """
        SELECT id, trade_id, prediction_id, ticker, outcome,
               failure_category, root_cause, signals_wrong, signals_right,
               what_we_missed, recommended_fix, full_analysis,
               confidence, generated_at
        FROM trade_postmortems tp
        WHERE """ + miss_on_or_after_sql(":cutoff") + """
    """
    params: dict[str, Any] = {"cutoff": cutoff}

    if ticker:
        query += " AND ticker = :ticker"
        params["ticker"] = ticker
    if category:
        query += " AND failure_category = :category"
        params["category"] = category

    query += " ORDER BY generated_at DESC"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    results = []
    for r in rows:
        record = {
            "id": r[0],
            "trade_id": r[1],
            "prediction_id": r[2],
            "ticker": r[3],
            "outcome": r[4],
            "failure_category": r[5],
            "root_cause": r[6],
            "signals_wrong": _parse_json(r[7]),
            "signals_right": _parse_json(r[8]),
            "what_we_missed": r[9],
            "recommended_fix": r[10],
            "full_analysis": _parse_json(r[11]),
            "confidence": float(r[12]) if r[12] is not None else None,
            "generated_at": r[13].isoformat() if r[13] else None,
        }
        # The return in a known unit, so no reader has to guess from
        # ``full_analysis.actual_return`` (percent points for older prediction
        # rows). None when it cannot be told.
        record["actual_return_fraction"] = stored_return_fraction(record)
        results.append(record)

    return results


# ── Internal Helpers ──────────────────────────────────────────────────────

def _parse_json(val: Any) -> Any:
    """Safely parse a JSON value that may be str, dict, list, or None."""
    if val is None:
        return {}
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _to_date(val: Any) -> date | None:
    """Coerce a date, datetime, or ISO string to a ``date``, else None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        s = str(val).strip()
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date() if ("T" in s or " " in s) else date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _finite_positive(val: Any) -> float | None:
    """Return float(val) if it is a finite positive number (not bool), else None."""
    if val is None or isinstance(val, bool):
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f <= 0.0:
        return None
    return f


# ── Decision clock (Codex HOLD 6011903681) ───────────────────────────────
#
# A postmortem attributes a failure by comparing the underlying path with an
# entry anchor. The anchor, and the scanner context beside it, must be what
# GRID held at ``generated_at``: never that day's later close (an 11:00 ET
# decision has no 16:00 close), never a vintage pulled after the decision, and
# never a guess when nothing was known -- then the postmortem abstains
# (``insufficient_data``) and records why.
#
# * raw closes: ``evaluation.prices.CompletedCloseAccessor`` (through
#   ``store.observations.read_completed_closes``): a label counts only from a
#   pull at or after 17:00 ET on its session day and at or before the clock,
#   so a same-day close counts only when the decision came after it, a later
#   revision is invisible, and disagreeing completed vintages abstain;
# * ``options_daily_signals``: one mutable row per (ticker, signal_date) whose
#   ``created_at`` is its last-write time (``ingestion/options.py`` upsert).
#   Only a row last written by the clock is read for its values; a row
#   written or revised later is read for its date and write time alone, to
#   say why an older row is shown.
#
# What the reconstruction proves (Codex HOLD 6030815214). ``created_at`` is
# ``NOW()``, the writing transaction's start, so it can precede the commit;
# an old visible row can hide a gap in the same session; and
# options_recommendations records no lineage (scanner row id or capture
# batch) of the values the recommender consumed (analysis/thesis_scorer.py:
# not reproducible). So the scanner row, and the regime and capital-flow
# rows read by date only, go under ``descriptive_context`` with
# DESCRIPTIVE_CONTEXT_LABEL: shown, dated and explained, but never the
# consumed row, an entry anchor or an attribution input. Scanner values
# drive attribution only as ``scanner_signals`` carrying recorded
# ``lineage``, which nothing in GRID writes today.

_ET = ZoneInfo("America/New_York")
# Calendar days searched back for the last completed close: any weekend plus
# a holiday run; older than that is not an entry anchor.
ENTRY_CLOSE_LOOKBACK_DAYS = 7

DESCRIPTIVE_CONTEXT_LABEL = (
    "descriptive only: not proven known at decision, not the inputs the "
    "recommendation consumed, never an entry anchor or an attribution input"
)
SCANNER_ATTRIBUTION_ABSTAINED = (
    "scanner-based attribution abstained: no lineage from the recommendation to an "
    "options_daily_signals row is recorded, and created_at is a write-start proxy"
)
_DATE_ONLY_BASIS = "read by date only, not proven known at decision"
_SCANNER_NOT_AN_ANCHOR = (
    "the price path is options_daily_signals, whose spot values are descriptive "
    "context only (no recorded lineage; created_at is a write-start proxy), so none "
    "is a decision-quality entry anchor"
)

_SCANNER_ROW_SQL = (
    "SELECT put_call_ratio, iv_atm, iv_skew, max_pain, spot_price, "
    "total_oi, term_structure_slope, oi_concentration, signal_date, created_at "
    "FROM " + VISIBLE_DAILY_SIGNALS + " ods "
    "WHERE ticker = :t AND signal_date <= :d AND created_at <= :decision_ts "
    "ORDER BY signal_date DESC LIMIT 1"
)

# Date and write time only: never the values of a row written after the clock.
_SCANNER_NEWEST_SQL = (
    "SELECT signal_date, created_at FROM " + VISIBLE_DAILY_SIGNALS + " ods "
    "WHERE ticker = :t AND signal_date <= :d "
    "ORDER BY signal_date DESC LIMIT 1"
)


def _decision_clock(val: Any) -> datetime | None:
    """``val`` as an aware UTC instant, or None when it is not a timestamp.

    A naive datetime is taken as UTC (griddb runs Etc/UTC). A bare date (or
    date-only string) has no time of day, so it cannot say whether that day's
    close or scanner row existed yet: None, and the caller abstains.
    """
    if isinstance(val, datetime):
        if val.tzinfo is None or val.utcoffset() is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    if isinstance(val, str):
        s = val.strip()
        if "T" not in s and " " not in s:
            return None
        try:
            parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        return _decision_clock(parsed)
    return None


def _decision_session_date(clock: datetime) -> date:
    """The New York calendar date of the decision clock (session dates are ET)."""
    return clock.astimezone(_ET).date()


def _no_clock_note(decision_date: Any) -> str:
    return (
        f"decision clock {decision_date!r} is not a timestamp, so no value can be "
        f"shown to have been known at decision time"
    )


def _scanner_row_known_at(conn: Any, ticker: str, clock: datetime) -> Any:
    """Newest visible ``options_daily_signals`` row last written by ``clock``.

    Bounded twice: ``signal_date`` on or before the clock's ET date, and
    ``created_at`` at or before the clock. ``created_at`` is re-checked here
    so a NULL or unparseable write time never passes.
    """
    row = conn.execute(
        visible_sql(_SCANNER_ROW_SQL),
        {"t": ticker, "d": _decision_session_date(clock), "decision_ts": clock},
    ).fetchone()
    if row is None:
        return None
    written = _decision_clock(row[9])
    if written is None or written > clock:
        return None
    return row


def _scanner_with_lineage(data_at_decision: dict[str, Any]) -> dict[str, Any]:
    """``scanner_signals`` when it carries recorded lineage to the recommendation, else ``{}``.

    Only such values may anchor or drive attribution (Codex HOLD 6030815214).
    A reconstructed options_daily_signals row has none: it is descriptive.
    """
    scanner = data_at_decision.get("scanner_signals")
    if not isinstance(scanner, dict):
        return {}
    lineage = scanner.get("lineage")
    return scanner if isinstance(lineage, str) and lineage.strip() else {}


def _scanner_context_at(conn: Any, ticker: str, clock: datetime) -> tuple[dict[str, Any] | None, str]:
    """The newest scanner row written by ``clock`` as labelled context, or why there is none.

    The row carries its ``signal_date``, ``created_at``, ``age_days`` (calendar
    days to the decision's ET date) and a ``reason``. ``fallback`` is True when
    a newer row existed but was written after the clock (or at no recorded
    time); only that row's date and write time are read.
    """
    session = _decision_session_date(clock)
    row = _scanner_row_known_at(conn, ticker, clock)
    if row is None:
        return None, (
            f"no options_daily_signals row for {ticker} dated on or before "
            f"{session.isoformat()} was last written by the decision clock {clock.isoformat()}"
        )
    signal_date = _to_date(row[8])
    newest = conn.execute(visible_sql(_SCANNER_NEWEST_SQL), {"t": ticker, "d": session}).fetchone()
    newest_date = _to_date(newest[0]) if newest is not None else None
    fallback = newest_date is not None and signal_date is not None and newest_date > signal_date
    if fallback:
        newest_written = _decision_clock(newest[1])
        reason = (
            f"newest row written after decision: options_daily_signals {newest_date.isoformat()} "
            f"was written {newest_written.isoformat()}, after decision clock {clock.isoformat()}"
            if newest_written is not None else
            f"newest row has no recorded write time: options_daily_signals "
            f"{newest_date.isoformat()} cannot be shown written by decision clock {clock.isoformat()}"
        ) + "; this older row is context only"
    else:
        reason = (
            f"newest visible row at decision clock {clock.isoformat()}; created_at is a "
            f"write-start proxy, so its visibility at the decision is not proven"
        )
    return {
        "put_call_ratio": float(row[0]) if row[0] else None,
        "iv_atm": float(row[1]) if row[1] else None,
        "iv_skew": float(row[2]) if row[2] else None,
        "max_pain": float(row[3]) if row[3] else None,
        "spot_price": float(row[4]) if row[4] else None,
        "total_oi": float(row[5]) if row[5] else None,
        "term_structure_slope": float(row[6]) if row[6] else None,
        "oi_concentration": float(row[7]) if row[7] else None,
        "signal_date": str(row[8]) if row[8] else None,
        "created_at": _decision_clock(row[9]).isoformat(),
        "age_days": (session - signal_date).days if signal_date is not None else None,
        "fallback": fallback,
        "reason": reason,
    }, ""


def _reconstruct_decision_data(
    engine: Engine,
    ticker: str,
    decision_date: Any,
    sanity: dict,
    dealer_context: str,
) -> dict[str, Any]:
    """Reconstruct the data state at the time a recommendation was made.

    ``sanity_status`` and ``dealer_context`` are the recommendation's own
    record. The scanner row (:func:`_scanner_context_at`) and the regime and
    capital-flow rows (read by date only) go under ``descriptive_context``
    with DESCRIPTIVE_CONTEXT_LABEL: they are not proven known at decision and
    are never an entry anchor or an attribution input (Codex HOLD 6030815214).
    ``scanner_attribution_unavailable`` records why scanner-based attribution
    abstains.
    """
    context: dict[str, Any] = {"label": DESCRIPTIVE_CONTEXT_LABEL}
    data: dict[str, Any] = {
        "sanity_status": sanity,
        "dealer_context": dealer_context,
        "descriptive_context": context,
        "scanner_attribution_unavailable": SCANNER_ATTRIBUTION_ABSTAINED,
    }

    as_of = _to_date(decision_date)
    if as_of is None:
        return data
    data["decision_date"] = as_of.isoformat()
    clock = _decision_clock(decision_date)

    with engine.connect() as conn:
        # Options scanner row written by the decision clock: context only
        if clock is None:
            context["scanner_unavailable"] = _no_clock_note(decision_date)
        else:
            scanner, why_not = _scanner_context_at(conn, ticker, clock)
            if scanner is None:
                context["scanner_unavailable"] = why_not
            else:
                context["scanner"] = scanner

        # Regime on or before the decision date: context only
        regime_row = conn.execute(text("""
            SELECT inferred_state, state_confidence, transition_probability
            FROM decision_journal
            WHERE decision_timestamp <= :d
            ORDER BY decision_timestamp DESC LIMIT 1
        """), {"d": as_of}).fetchone()

        if regime_row:
            context["regime"] = {
                "state": regime_row[0],
                "confidence": float(regime_row[1]) if regime_row[1] else None,
                "transition_prob": float(regime_row[2]) if regime_row[2] else None,
                "basis": "decision_journal " + _DATE_ONLY_BASIS,
            }

        # Capital flow snapshot on or before the decision date: context only
        flow_row = conn.execute(text("""
            SELECT narrative, snapshot_date
            FROM capital_flow_snapshots
            WHERE snapshot_date <= :d
            ORDER BY snapshot_date DESC LIMIT 1
        """), {"d": as_of}).fetchone()

        if flow_row:
            context["capital_flow"] = {
                "narrative": str(flow_row[0])[:500] if flow_row[0] else None,
                "snapshot_date": str(flow_row[1]) if flow_row[1] else None,
                "basis": "capital_flow_snapshots " + _DATE_ONLY_BASIS,
            }

    return data


def _load_price_path_with_source(
    engine: Engine,
    ticker: str,
    start_date: Any,
    end_date: Any,
) -> tuple[list[dict], str | None]:
    """Load day-by-day underlying price data from entry to exit plus source tag.

    Returns ``(path, source)`` where ``source`` is ``"options_daily_signals"``,
    ``"raw_series"``, or ``None`` when no rows were found.
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    if not start or not end:
        return [], None

    path: list[dict] = []

    with engine.connect() as conn:
        # Try options_daily_signals first
        rows = conn.execute(visible_sql("""
            SELECT signal_date, spot_price FROM """ + VISIBLE_DAILY_SIGNALS + """ ods
            WHERE ticker = :t AND signal_date >= :s AND signal_date <= :e
              AND spot_price > 0
            ORDER BY signal_date
        """), {"t": ticker, "s": start, "e": end}).fetchall()

        if rows:
            for r in rows:
                price = _finite_positive(r[1])
                if price is not None:
                    path.append({"date": str(r[0]), "price": price})
            if path:
                return path, "options_daily_signals"

        # Fallback to raw_series (latest vintage per obs_date)
        rows = conn.execute(text("""
            SELECT obs_date, value FROM raw_series
            WHERE series_id = :sid
              AND obs_date >= :s AND obs_date <= :e
              AND pull_status = 'SUCCESS'
              AND value > 0
            ORDER BY obs_date ASC, pull_timestamp DESC
        """), {"sid": f"YF:{ticker}:close", "s": start, "e": end}).fetchall()

        seen_dates: set[str] = set()
        for r in rows:
            d_str = str(r[0])
            if d_str in seen_dates:
                continue
            price = _finite_positive(r[1])
            if price is not None:
                seen_dates.add(d_str)
                path.append({"date": d_str, "price": price})

    return (path, "raw_series") if path else ([], None)


def _load_price_path(
    engine: Engine,
    ticker: str,
    start_date: Any,
    end_date: Any,
) -> list[dict]:
    """Load day-by-day price data from entry to exit.

    Returns a list of {date, price} dicts.
    """
    path, _ = _load_price_path_with_source(engine, ticker, start_date, end_date)
    return path


def _load_underlying_entry_spot(
    engine: Engine,
    ticker: str,
    decision_date: Any,
    data_at_decision: dict[str, Any],
    *,
    price_source: str | None = None,
) -> tuple[float | None, str]:
    """Return ``(spot, note)``: the underlying entry anchor known at ``decision_date``.

    ``decision_date`` is the decision clock (``generated_at``). The anchor
    shares the price source that supplied ``price_path``:
      - ``"raw_series"`` or ``None`` (no path loaded): the newest completed
        ``YF:{ticker}:close`` known at the clock
        (:class:`evaluation.prices.CompletedCloseAccessor`);
      - ``"options_daily_signals"``: none. Its spot values are descriptive
        context, not proven known at decision (Codex HOLD 6030815214), and a
        raw close is not mixed into that path.
    Scanner values with recorded lineage (:func:`_scanner_with_lineage`) are
    the one scanner anchor; nothing in GRID records that lineage today.

    ``note`` names the basis when a spot is returned, and otherwise why none
    was provably known. Never falls back to option premium, to a close of a
    session that had not ended by the clock, or to a vintage pulled after it.
    """
    clock = _decision_clock(decision_date)
    if clock is None:
        return None, _no_clock_note(decision_date)
    unknown = f"no underlying entry spot for {ticker} was known at decision clock {clock.isoformat()}: "

    linked = _scanner_with_lineage(data_at_decision)
    linked_spot = _finite_positive(linked.get("spot_price"))
    if price_source != "raw_series" and linked_spot is not None:
        return linked_spot, (
            f"options_daily_signals {linked.get('signal_date')} with recorded lineage "
            f"({linked.get('lineage')})"
        )
    if price_source == "options_daily_signals":
        return None, unknown + _SCANNER_NOT_AN_ANCHOR

    with engine.connect() as conn:
        spot, note = _completed_close_known_at(conn, ticker, clock)
    if spot is not None:
        return spot, note
    return None, unknown + note


def _completed_close_known_at(conn: Any, ticker: str, clock: datetime) -> tuple[float | None, str]:
    """Newest completed raw close of ``ticker`` known at ``clock``, or why there is none."""
    from evaluation.prices import (
        AmbiguousPriceError,
        CompletedCloseAccessor,
        UnsupportedInstrumentError,
    )

    session = _decision_session_date(clock)
    start = session - timedelta(days=ENTRY_CLOSE_LOOKBACK_DAYS)
    try:
        point = CompletedCloseAccessor(conn, start=start, evaluated_at=clock)(ticker, session)
    except AmbiguousPriceError as exc:
        return None, f"completed close is ambiguous ({exc})"
    except UnsupportedInstrumentError as exc:
        return None, f"no completed-close policy for {ticker!r} ({exc})"
    if point is None:
        return None, (
            f"no completed YF:{ticker}:close from {start.isoformat()} to "
            f"{session.isoformat()} was pulled by the clock"
        )
    spot = _finite_positive(point.value)
    if spot is None:
        return None, f"completed close {point.obs_date.isoformat()} is not a positive price"
    return spot, (
        f"{point.source_ref} {point.obs_date.isoformat()}, pulled after its close "
        f"and at or before decision clock {clock.isoformat()}"
    )


def _active_sanity(sanity: dict | None) -> dict:
    """The sanity layers that count as evidence in a post-mortem.

    A shadow verdict is kept in the raw record (``sanity_results_at_decision``
    and ``data_at_decision``) but never attributed as a right or wrong signal,
    never counted toward ``model_error`` and never shown to the narrative LLM
    as evidence (Codex review 6030902445 on #1400). A layer is shadow when its
    entry says ``"shadow": true`` (rows written since #1400) or when it is a
    shadow check by name (``trading.options_recommender.SHADOW_CHECKS``:
    CROSS_ASSET).

    The name rule also covers the rows persisted before #1400, which carry no
    flag. On griddb (2026-11-23) all 778 had CROSS_ASSET = PASS (only passing
    recommendations were stored). Those verdicts came from the momentum read
    #1400 replaced: WTI crude for CL, a fake 0% for every other ticker. They
    are no evidence either, so excluding them removes only a
    ``sanity_CROSS_ASSET_passed`` from ``signals_right`` and can never change
    a failure category.
    """
    from trading.options_recommender import SHADOW_CHECKS

    return {
        layer: result for layer, result in (sanity or {}).items()
        if layer not in SHADOW_CHECKS
        and not (isinstance(result, dict) and result.get("shadow") is True)
    }


def _classify_failure(
    *,
    ticker: str,
    direction: str,
    entry_price: float | None,
    strike: float,
    target: float,
    expiry: Any,
    outcome: str,
    actual_return: float,
    price_path: list[dict],
    data_at_decision: dict,
    sanity: dict,
    underlying_entry_price: float | None = None,
    generated_at: Any = None,
) -> tuple[str, str, list[str], list[str], str]:
    """Classify a trade failure into a category with root cause analysis.

    ``entry_price`` on ``options_recommendations`` is the OPTION PREMIUM;
    ``price_path`` is UNDERLYING spot. Directional timing is always computed
    against ``underlying_entry_price`` (or a lineage-backed scanner
    ``spot_price``), never against the option premium. If no underlying
    entry spot is available, fails closed with ``"insufficient_data"``.

    Scanner values (signal names, staleness, spot) count only as
    ``scanner_signals`` with recorded lineage (:func:`_scanner_with_lineage`);
    ``descriptive_context`` is never read here (Codex HOLD 6030815214).

    Only active sanity layers (:func:`_active_sanity`) are attributed or
    counted; shadow verdicts are not evidence.

    Returns:
        (failure_category, root_cause, signals_wrong, signals_right, what_we_missed)
    """
    signals_wrong: list[str] = []
    signals_right: list[str] = []
    what_missed = ""
    active_sanity = _active_sanity(sanity)

    # Check scanner signals: only those with recorded lineage to the recommendation
    scanner = _scanner_with_lineage(data_at_decision)
    if scanner:
        pcr = scanner.get("put_call_ratio")
        if pcr is not None:
            if direction == "CALL" and pcr < 0.7:
                signals_right.append("pcr_bullish")
            elif direction == "CALL" and pcr > 1.2:
                signals_wrong.append("pcr_was_bearish")
            elif direction == "PUT" and pcr > 1.2:
                signals_right.append("pcr_bearish")
            elif direction == "PUT" and pcr < 0.7:
                signals_wrong.append("pcr_was_bullish")

        iv_skew = scanner.get("iv_skew")
        if iv_skew is not None:
            if abs(iv_skew) > 0.1:
                signals_right.append("iv_skew_signal")
            else:
                signals_wrong.append("iv_skew_flat")

        max_pain = scanner.get("max_pain")
        spot = scanner.get("spot_price")
        if max_pain and spot and spot > 0:
            mp_gap = (spot - max_pain) / spot * 100
            if direction == "CALL" and mp_gap < -3:
                signals_right.append("max_pain_bullish")
            elif direction == "PUT" and mp_gap > 3:
                signals_right.append("max_pain_bearish")
            else:
                signals_wrong.append("max_pain_neutral")

    # Check sanity layers (active only: shadow verdicts are not signals)
    for layer, result in active_sanity.items():
        if isinstance(result, dict):
            status = result.get("status", "")
            if status == "FAIL":
                signals_wrong.append(f"sanity_{layer}_failed")
            elif status == "PASS":
                signals_right.append(f"sanity_{layer}_passed")

    # Resolve underlying entry spot — NEVER substitute option premium entry_price.
    # When the loader recorded why no anchor was known at the decision clock,
    # that abstention stands: no other price source is tried here.
    abstained = data_at_decision.get("underlying_entry_spot_unavailable")
    spot_basis = (
        _finite_positive(underlying_entry_price)
        or _finite_positive(data_at_decision.get("underlying_entry_spot"))
        or (None if abstained else _finite_positive((scanner or {}).get("spot_price")))
    )
    if spot_basis is None:
        log.warning(
            "Post-mortem: missing underlying entry spot for {t}; "
            "labeling insufficient_data instead of guessing",
            t=ticker,
        )
        why = f" {abstained}." if abstained else ""
        return (
            "insufficient_data",
            f"Underlying spot price at entry time is unavailable for {ticker}; "
            f"cannot compare underlying price path against option premium.{why}",
            signals_wrong,
            signals_right,
            "Underlying entry spot price was not available at decision time.",
        )

    # Check if direction was ever right during the trade (underlying vs underlying)
    direction_was_right_at_some_point = False
    max_favorable_move = 0.0
    dir_upper = (direction or "").strip().upper()
    if price_path:
        for p in price_path:
            price = _finite_positive(p.get("price"))
            if price is None:
                continue
            move_pct = (price - spot_basis) / spot_basis * 100
            if dir_upper in ("CALL", "LONG") and move_pct > 0:
                direction_was_right_at_some_point = True
                max_favorable_move = max(max_favorable_move, move_pct)
            elif dir_upper in ("PUT", "SHORT") and move_pct < 0:
                direction_was_right_at_some_point = True
                max_favorable_move = max(max_favorable_move, abs(move_pct))

    # Classify
    # 1. Right direction, wrong timing
    if direction_was_right_at_some_point and max_favorable_move > 1.0:
        category = "right_signal_wrong_timing"
        root_cause = (
            f"Direction was correct — {ticker} moved {max_favorable_move:.1f}% "
            f"in the predicted direction during the trade window, but the option "
            f"expired before the move was realised or the move reversed."
        )
        what_missed = "Expiry was too short or position should have been rolled."
        return category, root_cause, signals_wrong, signals_right, what_missed

    # 2. Bad data — check for stale signals at decision/entry time (NOT future expiry)
    scanner_date = (scanner or {}).get("signal_date")
    decision_ref = generated_at if generated_at is not None else data_at_decision.get("decision_date")
    if scanner_date and decision_ref is not None:
        sig_date = _to_date(scanner_date)
        decision_d = _to_date(decision_ref)
        if sig_date is not None and decision_d is not None:
            signal_age_days = (decision_d - sig_date).days
            if signal_age_days > 7:
                category = "bad_data"
                root_cause = (
                    f"Scanner signals were {signal_age_days} days old at decision time. "
                    f"Data may have been stale."
                )
                what_missed = "Data freshness check should have flagged stale signals."
                return category, root_cause, signals_wrong, signals_right, what_missed

    # 3. Check for external shock — large sudden move against position
    if price_path and len(price_path) >= 2:
        for i in range(1, len(price_path)):
            prev_price = price_path[i - 1].get("price", 0)
            curr_price = price_path[i].get("price", 0)
            if prev_price and curr_price:
                daily_move = abs(curr_price - prev_price) / prev_price * 100
                if daily_move > 5.0:  # >5% single-day move suggests external event
                    category = "external_shock"
                    root_cause = (
                        f"Large single-day move of {daily_move:.1f}% detected on "
                        f"{price_path[i].get('date', 'unknown')}. "
                        f"Likely an external event (earnings, FOMC, geopolitical)."
                    )
                    what_missed = (
                        "Event calendar awareness needed. Check for earnings dates, "
                        "FOMC meetings, or other catalysts near the expiry."
                    )
                    return category, root_cause, signals_wrong, signals_right, what_missed

    # 4. Check sanity failures — potential model error (shadow FAILs never count)
    failed_sanity_count = sum(
        1 for v in active_sanity.values()
        if isinstance(v, dict) and v.get("status") == "FAIL"
    )
    if failed_sanity_count >= 2:
        category = "model_error"
        root_cause = (
            f"{failed_sanity_count} sanity layers failed at decision time. "
            f"The recommendation should not have been generated."
        )
        what_missed = "Sanity gating should have blocked this recommendation."
        return category, root_cause, signals_wrong, signals_right, what_missed

    # 5. Default: wrong signal
    category = "wrong_signal"
    root_cause = (
        f"The {direction} recommendation for {ticker} was wrong: the underlying "
        f"moved against the position. Actual return: {actual_return:+.2%}."
    )
    what_missed = "Signal ensemble did not capture the true market direction."

    return category, root_cause, signals_wrong, signals_right, what_missed


def _classify_prediction_failure(
    *,
    ticker: str,
    direction: str,
    entry_price: float | None,
    target: float,
    expiry: Any,
    actual_price: float | None,
    actual_move_pct: float | None,
    signals: Any,
    anti_signals: Any,
    price_path: list[dict],
) -> tuple[str, str, list[str], list[str], str]:
    """Classify a prediction failure into a category.

    Similar logic to trade classification but works with oracle signals.
    """
    signals_wrong: list[str] = []
    signals_right: list[str] = []

    # Analyse which signals were right/wrong
    if isinstance(signals, list):
        for sig in signals:
            if not isinstance(sig, dict):
                continue
            name = sig.get("name", "unknown")
            sig_dir = sig.get("direction", "neutral")
            target_dir = "bullish" if direction in ("CALL", "LONG") else "bearish"
            if sig_dir == target_dir:
                signals_wrong.append(name)  # aligned with prediction but prediction failed
            elif sig_dir != "neutral":
                signals_right.append(name)  # was contra but got ignored

    # Check for anti-signals that were ignored
    if isinstance(anti_signals, list) and len(anti_signals) >= 2:
        high_severity = [
            a for a in anti_signals
            if isinstance(a, dict) and float(a.get("severity", 0)) > 0.5
        ]
        if high_severity:
            what_missed = (
                f"{len(high_severity)} high-severity anti-signals were present "
                f"but ignored: {', '.join(a.get('name', '?') for a in high_severity[:3])}"
            )
            return (
                "wrong_signal",
                "Anti-signals warned against this prediction but were overridden. "
                "Net signal strength was insufficient.",
                signals_wrong,
                signals_right,
                what_missed,
            )

    # Check timing. The move is `(price - entry) / entry`, so the entry has
    # to be a positive number before the loop runs: `None > 0` raises and a
    # zero basis makes every move undefined, not infinite. Both are settled
    # here, before the division, and the reason is carried into what_missed
    # rather than being a silent skip that reads as "timing was simply wrong".
    entry_note = entry_price_score_note(entry_price)
    direction_was_right = False
    if entry_note is not None:
        return (
            "not_measured",
            f"Timing could not be reconstructed: {entry_note}.",
            signals_wrong,
            signals_right,
            f"No entry basis to time the move against — {entry_note}.",
        )
    if price_path:
        for p in price_path:
            price = p.get("price", 0)
            if not price:
                continue
            move = (price - entry_price) / entry_price * 100
            if direction in ("CALL", "LONG") and move > 1.0:
                direction_was_right = True
                break
            elif direction in ("PUT", "SHORT") and move < -1.0:
                direction_was_right = True
                break

    if direction_was_right:
        return (
            "right_signal_wrong_timing",
            "Direction was correct at some point during the prediction window "
            "but the move did not persist through expiry.",
            signals_wrong,
            signals_right,
            "Consider longer prediction horizons or trailing exit logic.",
        )

    # Check for external shock
    if price_path and len(price_path) >= 2:
        for i in range(1, len(price_path)):
            prev = price_path[i - 1].get("price", 0)
            curr = price_path[i].get("price", 0)
            if prev and curr:
                daily_move = abs(curr - prev) / prev * 100
                if daily_move > 5.0:
                    return (
                        "external_shock",
                        f"Large {daily_move:.1f}% single-day move on "
                        f"{price_path[i].get('date', 'unknown')} suggests external event.",
                        signals_wrong,
                        signals_right,
                        "Event calendar awareness needed.",
                    )

    # Default: wrong signal
    return (
        "wrong_signal",
        f"Model predicted {direction} for {ticker} but actual move was "
        f"{actual_move_pct:+.1f}%." if actual_move_pct else
        f"Model predicted {direction} for {ticker} but the prediction missed.",
        signals_wrong,
        signals_right,
        "Signal ensemble did not capture the true market direction.",
    )


def _summarise_what_happened(
    ticker: str,
    direction: str,
    entry_price: float | None,
    strike: float,
    outcome: str,
    actual_return: float,
    price_path: list[dict],
) -> str:
    """Build a factual summary of what happened."""
    # "entered at $0.00" was, for a NULL, a measurement nobody took — it read
    # as a finding about the prediction. A NULL says so instead of printing a
    # price. A *measured* zero still prints as $0.00, because that is what was
    # measured, followed by the reason no return can be taken off it.
    entry_note = entry_price_score_note(entry_price)
    if entry_note is None:
        entry_str = f"entered at ${entry_price:.2f}"
    elif entry_price is None:
        entry_str = "no measured entry price"
    else:
        entry_str = f"entered at ${float(entry_price):.2f} — {entry_note}"
    parts = [f"{ticker} {direction}: {entry_str}, strike ${strike:.2f}."]

    if price_path:
        first_price = price_path[0].get("price")
        last_price = price_path[-1].get("price")
        if first_price and last_price:
            total_move = (last_price - first_price) / first_price * 100
            parts.append(
                f"Price moved from ${first_price:.2f} to ${last_price:.2f} "
                f"({total_move:+.1f}%) over {len(price_path)} trading days."
            )

        # High and low during the period
        prices = [p.get("price", 0) for p in price_path if p.get("price")]
        if prices:
            high = max(prices)
            low = min(prices)
            parts.append(f"Range: ${low:.2f} — ${high:.2f}.")

    parts.append(f"Outcome: {outcome}. Return: {actual_return:+.2%}.")
    return " ".join(parts)


def _log_aggregate_patterns(postmortems: list[PostMortem], days: int) -> None:
    """Log aggregate pattern analysis for a batch of post-mortems."""
    total = len(postmortems)
    categories = Counter(pm.failure_category for pm in postmortems)
    tickers = Counter(pm.ticker for pm in postmortems)

    log.info("── Post-Mortem Aggregate ({d} days, {n} failures) ──", d=days, n=total)
    for cat, count in categories.most_common():
        pct = count / total * 100
        log.info("  {cat}: {n} ({p:.0f}%)", cat=cat, n=count, p=pct)

    if tickers:
        worst = tickers.most_common(3)
        log.info("  Worst tickers: {w}", w=", ".join(f"{t}({n})" for t, n in worst))


# ── LLM Integration ──────────────────────────────────────────────────────

def _get_llm_postmortem(
    *,
    ticker: str,
    direction: str,
    thesis: str,
    outcome: str,
    actual_return: float,
    category: str,
    root_cause: str,
    what_happened: str,
    data_at_decision: dict,
    price_path: list[dict],
    signals_wrong: list[str],
    signals_right: list[str],
    decision_signals: list[dict] | None = None,
    decision_opposing: list[dict] | None = None,
) -> dict[str, Any]:
    """Ask the LLM for a narrative post-mortem explanation.

    Returns a dict with keys: recommended_fix, what_we_missed, confidence, narrated.
    ``narrated`` is False when the LLM is unavailable or answered without a MISSED/FIX line.
    """
    # "narrated": False tells the caller no LLM narrative was produced, so the row is
    # written as a rule-based record (see _narrative_fields).
    defaults = {
        "recommended_fix": root_cause,
        "what_we_missed": LEGACY_LLM_FALLBACK_TEXT,
        "confidence": 0.5,
        "narrated": False,
    }

    try:
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.RESEARCH)
        if not llm.is_available:
            return defaults
    except Exception:
        return defaults

    # Build price path summary (truncate for context window)
    price_summary = ""
    if price_path:
        sample = price_path[:5] + (price_path[-3:] if len(price_path) > 8 else [])
        price_lines = [f"  {p['date']}: ${p['price']:.2f}" for p in sample if p.get("price")]
        price_summary = "\n".join(price_lines)

    # Compact data_at_decision — no pretty-print, hard cap for local model context.
    # Descriptive context gets its own labelled block (Codex HOLD 6030815214).
    decision_data = {k: v for k, v in data_at_decision.items() if k != "descriptive_context"}
    data_summary = json.dumps(decision_data, separators=(",", ":"), default=str)[:800]
    descriptive = data_at_decision.get("descriptive_context")
    descriptive_block = (
        "DESCRIPTIVE CONTEXT (not proven known at decision; not the inputs the "
        "recommendation consumed; do not attribute the failure to it):\n"
        + json.dumps(descriptive, separators=(",", ":"), default=str)[:600] + "\n\n"
    ) if descriptive else ""

    # RAG: retrieve historical context — past failures, lessons learned
    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = f"{ticker} {direction} postmortem {category} {root_cause}"
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=1500)
    except Exception as exc:
        log.warning("RAG context retrieval failed in postmortem: {e}", e=exc)

    # Top contributors block — pulled from the persisted scanner signals
    # on options_recommendations (migration 0051). Sorted by score so the
    # LLM sees the highest-weight features first when explaining failure.
    top_contrib_lines: list[str] = []
    if decision_signals:
        for s in decision_signals[:8]:
            name = s.get("name", "?")
            score = s.get("score", 0)
            sig_dir = s.get("direction") or "—"
            value = s.get("value")
            top_contrib_lines.append(
                f"  {name} (dir={sig_dir}, score={score}, value={value})"
            )
    top_contrib_block = (
        "TOP CONTRIBUTING SIGNALS (highest-score first):\n"
        + "\n".join(top_contrib_lines) + "\n"
    ) if top_contrib_lines else ""

    opposing_lines: list[str] = []
    if decision_opposing:
        for s in decision_opposing[:5]:
            name = s.get("name", "?")
            score = s.get("score", 0)
            sig_dir = s.get("direction") or "—"
            value = s.get("value")
            opposing_lines.append(
                f"  {name} (dir={sig_dir}, score={score}, value={value})"
            )
    opposing_block = (
        "OPPOSING SIGNALS WE OVERRODE:\n"
        + "\n".join(opposing_lines) + "\n"
    ) if opposing_lines else ""

    prompt = (
        f"TRADE: {ticker} {direction}\n"
        f"THESIS: {thesis}\n"
        f"OUTCOME: {outcome} (return: {actual_return:+.2%})\n"
        f"CATEGORY: {category}\n"
        f"ROOT CAUSE: {root_cause}\n\n"
    )
    if rag_context:
        prompt += f"{rag_context}\n"
    prompt += (
        f"WHAT HAPPENED:\n{what_happened}\n\n"
        f"PRICE PATH:\n{price_summary}\n\n"
        f"{top_contrib_block}"
        f"{opposing_block}"
        f"DATA AT DECISION:\n{data_summary}\n\n"
        f"{descriptive_block}"
        f"SIGNALS WRONG: {', '.join(signals_wrong) or 'none identified'}\n"
        f"SIGNALS RIGHT: {', '.join(signals_right) or 'none identified'}\n\n"
        f"Provide:\n"
        f"1. LEVER we missed — identify WHO did WHAT affecting WHICH liquidity valve that moved price. Reference similar past failures if available. (1-2 sentences)\n"
        f"2. CONDITIONS that amplified/dampened the lever (e.g. low volume, expiry week) — these are NOT causes. (1 sentence)\n"
        f"3. INVALIDATION signal we should have caught — the specific condition that proved the thesis wrong. (1 sentence)\n"
        f"4. Recommended fix (1-2 specific, actionable sentences)\n"
        f"5. Confidence in this analysis (0.0 to 1.0)\n\n"
        f"Format as:\n"
        f"MISSED: ...\n"
        f"FIX: ...\n"
        f"CONFIDENCE: 0.X"
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="Identify the ROOT CAUSE using LEVER→CONDITION separation. LEVER: [Who] did [what] affecting [which liquidity valve]. CONDITION: [Environmental factor] that amplified/dampened the lever — conditions alone are NOT causes. State the INVALIDATION signal we missed. Label each finding confirmed/derived/estimated. Be specific and concise.",
            temperature=0.3,
            num_predict=POSTMORTEM_NUM_PREDICT,
        )

        if not response:
            return defaults

        result = dict(defaults)
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.upper().startswith("MISSED:"):
                result["what_we_missed"] = line[7:].strip()
                result["narrated"] = True
            elif line.upper().startswith("FIX:"):
                result["recommended_fix"] = line[4:].strip()
                result["narrated"] = True
            elif line.upper().startswith("CONFIDENCE:"):
                try:
                    result["confidence"] = min(1.0, max(0.0, float(line[11:].strip())))
                except ValueError:
                    pass

        return result

    except Exception as exc:
        log.debug("LLM post-mortem failed: {e}", e=str(exc))
        return defaults


def _get_llm_lessons_learned(
    postmortems: list[PostMortem],
    categories: Counter,
    tickers: Counter,
    wrong_counts: Counter,
    right_counts: Counter,
) -> str | None:
    """Use LLM to synthesise lessons learned from a batch of post-mortems.

    Returns formatted report string, or None if LLM unavailable.
    """
    try:
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.RESEARCH)
        if not llm.is_available:
            return None
    except Exception:
        return None

    total = len(postmortems)
    avg_return = sum(pm.actual_return for pm in postmortems) / total

    # Build summary for LLM
    cat_lines = "\n".join(
        f"  {cat}: {count} ({count / total * 100:.0f}%)"
        for cat, count in categories.most_common()
    )
    ticker_lines = "\n".join(
        f"  {t}: {c} failures"
        for t, c in tickers.most_common(5)
    )
    wrong_lines = "\n".join(
        f"  {s}: wrong {c}x" for s, c in wrong_counts.most_common(5)
    )
    right_lines = "\n".join(
        f"  {s}: right {c}x" for s, c in right_counts.most_common(5)
    )

    # Sample of individual root causes
    # Only trusted prose is sampled; an untrusted (pre-fix prediction) row
    # contributes its structured category instead of any free text.
    root_causes = "\n".join(
        f"  - {pm.ticker} {pm.direction}: "
        + (pm.root_cause[:100] if pm.prose_trusted
           else f"{pm.failure_category} {PROSE_OMITTED}")
        for pm in postmortems[:8]
    )

    prompt = (
        f"You are a quantitative trading analyst synthesising lessons from "
        f"{total} failed trades/predictions.\n\n"
        f"Average return: {avg_return:+.2%}\n\n"
        f"FAILURE CATEGORIES:\n{cat_lines}\n\n"
        f"WORST TICKERS:\n{ticker_lines}\n\n"
        f"SIGNALS MOST OFTEN WRONG:\n{wrong_lines}\n\n"
        f"SIGNALS MOST OFTEN RIGHT (but overridden):\n{right_lines}\n\n"
        f"SAMPLE ROOT CAUSES:\n{root_causes}\n\n"
        f"Provide 5-7 specific, actionable recommendations. Format as bullet points.\n"
        f"For each recommendation, use LEVER→CONDITION separation:\n"
        f"- Name the LEVER (actor + action + valve) that we failed to detect or weighted wrong.\n"
        f"- Separate CONDITIONS (amplifiers/dampeners) from actual causes.\n"
        f"- Never present a condition (low volume, expiry week, sentiment) as a root cause.\n"
        f"Focus on what to STOP doing, what to WEIGHT more heavily, and what to ADD.\n"
        f"Be concise — one line per recommendation."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="Synthesize root causes into actionable fixes using LEVER→CONDITION causation. Each recommendation must identify: LEVER ([Who] did [what] affecting [which valve]), CONDITION (amplifier/dampener — NOT a cause), and what INVALIDATION signal we missed. Never present conditions alone as causes. Label confidence: confirmed/derived/estimated.",
            temperature=0.3,
            num_predict=POSTMORTEM_NUM_PREDICT,
        )
        if not response:
            return None

        lines = [
            "=" * 60,
            "  GRID Post-Mortem — Lessons Learned (LLM Analysis)",
            "=" * 60,
            "",
            f"Failures analysed: {total}",
            f"Average return:    {avg_return:+.2%}",
            "",
            "-- Failure Categories --",
            cat_lines,
            "",
            "-- LLM Recommendations --",
            "",
            response.strip(),
            "",
            "=" * 60,
        ]
        return "\n".join(lines)

    except Exception as exc:
        log.debug("LLM lessons learned failed: {e}", e=str(exc))
        return None


# ── Contagion Feedback Loop ───────────────────────────────────────────────
#
# Close the loop between contagion predictions and reality. When a
# contagion_backtest_results row lands with low accuracy, walk the
# originating prediction's ranked_impact, find the supply_chain_edges that
# drove each victim's predicted margin hit, and decay the stored
# pct_downstream_cogs toward the value that would have produced the actual
# price move. When accuracy is high, flag the edge as backtest_validated
# without touching the stored value.
#
# Algorithm:
#   For each recent backtest row with accuracy_score < LOW_ACCURACY_THRESHOLD:
#     1. Load the originating contagion_predictions row and its shock magnitude.
#     2. Compute the implied pct_downstream_cogs that WOULD have produced the
#        actual move:
#           implied = |actual_price_move_pct| / (shock_magnitude * pass_through)
#     3. Blend with the currently stored value using a confidence-weighted
#        average (0.7 * old + 0.3 * implied).
#     4. Cap the delta at +/- MAX_DELTA_PER_UPDATE so one bad backtest cannot
#        flip a well-calibrated edge.
#     5. Write the updated value back to supply_chain_edges with
#        confidence='derived_from_backtest' and log an audit row.
#
#   For each recent backtest row with accuracy_score >= HIGH_ACCURACY_THRESHOLD:
#     Mark the touched edges as backtest_validated=TRUE and write a
#     'confirm' audit row. Do not mutate pct_downstream_cogs.

LOW_ACCURACY_THRESHOLD: float = 0.5
HIGH_ACCURACY_THRESHOLD: float = 0.8
OLD_WEIGHT: float = 0.7
NEW_WEIGHT: float = 0.3
MAX_DELTA_PER_UPDATE: float = 0.02
DEFAULT_PASS_THROUGH: float = 0.70  # mirrors intelligence.chain_contagion


def _load_recent_backtests(conn: Any, since_hours: int) -> list[dict[str, Any]]:
    """Return contagion_backtest_results rows scored in the last
    ``since_hours`` hours. Each row carries enough context to find the
    originating prediction and the implicated ticker.
    """
    try:
        rows = conn.execute(
            text(
                """
                SELECT r.id,
                       r.prediction_id,
                       r.ticker,
                       r.predicted_margin_impact_pct,
                       r.actual_price_move_pct,
                       r.accuracy_score,
                       p.shock_node,
                       p.magnitude
                FROM contagion_backtest_results r
                JOIN contagion_predictions p ON p.id = r.prediction_id
                WHERE r.scored_at >= NOW() - (:h || ' hours')::INTERVAL
                  AND r.accuracy_score IS NOT NULL
                  AND r.actual_price_move_pct IS NOT NULL
                  AND r.predicted_margin_impact_pct IS NOT NULL
                ORDER BY r.scored_at DESC
                """
            ),
            {"h": int(since_hours)},
        ).fetchall()
    except Exception as exc:
        log.warning(
            "contagion_feedback: backtest fetch failed: {e}", e=str(exc)
        )
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "backtest_result_id": int(r[0]),
                "prediction_id": int(r[1]) if r[1] is not None else None,
                "ticker": r[2],
                "predicted_margin_impact_pct": float(r[3]) if r[3] is not None else None,
                "actual_price_move_pct": float(r[4]) if r[4] is not None else None,
                "accuracy_score": float(r[5]) if r[5] is not None else None,
                "shock_node": r[6],
                "shock_magnitude": float(r[7]) if r[7] is not None else None,
            }
        )
    return out


def _load_edges_for_pair(
    conn: Any,
    shock_node: str,
    ticker: str,
) -> list[dict[str, Any]]:
    """Find the supply_chain_edges that connect ``shock_node`` (upstream)
    to ``ticker`` (downstream). Case-insensitive match on the downstream
    id because tickers in ranked_impact are uppercased while edges may
    use slugs.
    """
    if not shock_node or not ticker:
        return []
    try:
        rows = conn.execute(
            text(
                """
                SELECT id, upstream_id, downstream_id,
                       pct_downstream_cogs, confidence
                FROM supply_chain_edges
                WHERE upstream_id = :u
                  AND UPPER(downstream_id) = UPPER(:d)
                """
            ),
            {"u": shock_node, "d": ticker},
        ).fetchall()
    except Exception as exc:
        log.debug(
            "contagion_feedback: edge fetch failed for {u}->{d}: {e}",
            u=shock_node, d=ticker, e=str(exc),
        )
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r[0]),
                "upstream_id": r[1],
                "downstream_id": r[2],
                "pct_downstream_cogs": float(r[3]) if r[3] is not None else None,
                "confidence": r[4],
            }
        )
    return out


def _compute_implied_pct(
    actual_price_move_pct: float,
    shock_magnitude: float,
    pass_through: float = DEFAULT_PASS_THROUGH,
    predicted_margin_impact_pct: float | None = None,
) -> float | None:
    """Return the pct_downstream_cogs that WOULD have produced the actual
    move, given the shock magnitude, pass-through assumption, and predicted
    direction.

    Uses the signed actual move relative to the predicted direction:
      ``signed_move = actual_price_move_pct * pred_sign``
    where ``pred_sign`` is the sign of ``predicted_margin_impact_pct`` (or,
    when omitted, ``-1`` for a positive upstream cost shock and ``+1`` for a
    negative shock, matching ``chain_contagion._edge_impact``).

    A wrong-direction (or zero) move yields ``signed_move <= 0`` and returns
    ``0.0`` so the edge decays toward zero instead of being reinforced.
    Returns ``None`` when inputs are invalid or when ``implied > 1.0``.
    """
    if actual_price_move_pct is None or shock_magnitude is None:
        return None
    shock_f = float(shock_magnitude)
    mag = abs(shock_f)
    if mag < 1e-9:
        return None
    if predicted_margin_impact_pct is not None:
        pred_f = float(predicted_margin_impact_pct)
        if abs(pred_f) < 1e-9:
            return None
        pred_sign = 1.0 if pred_f > 0 else -1.0
    else:
        pred_sign = -1.0 if shock_f > 0 else 1.0
    signed_move = float(actual_price_move_pct) * pred_sign
    if signed_move <= 0.0:
        return 0.0
    pt = max(1e-6, float(pass_through))
    implied = signed_move / (mag * pt)
    if implied > 1.0:
        return None
    return implied


def _blend_and_cap(
    old_value: float | None,
    implied_value: float,
    max_delta: float = MAX_DELTA_PER_UPDATE,
) -> tuple[float, float, bool]:
    """Return ``(new_value, delta, was_capped)``.

    Blend: ``new = 0.7 * old + 0.3 * implied`` when ``old`` is known,
    otherwise ``new = implied``. Cap the absolute delta at ``max_delta``.
    """
    if old_value is None:
        blended = implied_value
    else:
        blended = OLD_WEIGHT * float(old_value) + NEW_WEIGHT * float(implied_value)
    if old_value is None:
        return float(blended), float(blended), False
    delta = blended - float(old_value)
    capped = False
    if delta > max_delta:
        delta = max_delta
        capped = True
    elif delta < -max_delta:
        delta = -max_delta
        capped = True
    return float(old_value) + delta, float(delta), capped


def _write_edge_update(
    conn: Any,
    edge_id: int,
    new_value: float,
) -> None:
    conn.execute(
        text(
            """
            UPDATE supply_chain_edges
            SET pct_downstream_cogs = :v,
                confidence = 'derived_from_backtest',
                last_backtest_at = NOW()
            WHERE id = :id
            """
        ),
        {"v": float(new_value), "id": int(edge_id)},
    )


def _write_edge_confirmation(conn: Any, edge_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE supply_chain_edges
            SET backtest_validated = TRUE,
                last_backtest_at = NOW()
            WHERE id = :id
            """
        ),
        {"id": int(edge_id)},
    )


def _write_adjustment_audit(
    conn: Any,
    *,
    edge_id: int,
    upstream_id: str,
    downstream_id: str,
    prediction_id: int | None,
    backtest_result_id: int | None,
    event_type: str,
    old_value: float | None,
    new_value: float | None,
    implied_value: float | None,
    accuracy: float | None,
    delta: float | None,
    capped: bool,
    reason: str,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO supply_chain_edge_adjustments (
                edge_id, upstream_id, downstream_id,
                prediction_id, backtest_result_id,
                event_type,
                old_pct_downstream_cogs, new_pct_downstream_cogs,
                implied_pct_cogs, accuracy_score,
                delta, capped, reason
            ) VALUES (
                :edge_id, :u, :d,
                :pid, :brid,
                :ev,
                :old, :new,
                :implied, :acc,
                :delta, :capped, :reason
            )
            """
        ),
        {
            "edge_id": int(edge_id),
            "u": upstream_id,
            "d": downstream_id,
            "pid": int(prediction_id) if prediction_id is not None else None,
            "brid": int(backtest_result_id) if backtest_result_id is not None else None,
            "ev": event_type,
            "old": float(old_value) if old_value is not None else None,
            "new": float(new_value) if new_value is not None else None,
            "implied": float(implied_value) if implied_value is not None else None,
            "acc": float(accuracy) if accuracy is not None else None,
            "delta": float(delta) if delta is not None else None,
            "capped": bool(capped),
            "reason": reason,
        },
    )


def apply_contagion_feedback(
    engine: Engine,
    since_hours: int = 24,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Close the contagion prediction -> reality feedback loop.

    Walks ``contagion_backtest_results`` rows scored in the last
    ``since_hours`` hours and, depending on accuracy:

        accuracy_score < LOW_ACCURACY_THRESHOLD -> decay the driving
            supply_chain_edges.pct_downstream_cogs toward the implied
            empirical value, capped at ±MAX_DELTA_PER_UPDATE.

        accuracy_score >= HIGH_ACCURACY_THRESHOLD -> mark the touched
            edges as backtest_validated=TRUE. Value is not mutated.

    Every change (decay or confirmation) is logged as an audit row in
    ``supply_chain_edge_adjustments``.

    When ``dry_run=True`` the function performs every computation and
    reports counts, but does not write to the database.

    Returns a summary dict: ``{"considered", "decayed", "confirmed",
    "skipped_no_edge", "skipped_no_implied", "errors"}``.
    """
    summary: dict[str, Any] = {
        "considered": 0,
        "decayed": 0,
        "confirmed": 0,
        "skipped_no_edge": 0,
        "skipped_no_implied": 0,
        "errors": 0,
        "dry_run": bool(dry_run),
    }

    try:
        with engine.connect() as conn:
            rows = _load_recent_backtests(conn, since_hours=since_hours)
    except Exception as exc:
        log.warning("contagion_feedback: cannot open connection: {e}", e=str(exc))
        summary["errors"] += 1
        return summary

    if not rows:
        log.info(
            "contagion_feedback: no eligible backtest rows in last {h}h",
            h=since_hours,
        )
        return summary

    for row in rows:
        summary["considered"] += 1
        accuracy = row["accuracy_score"]
        if accuracy is None:
            continue

        is_miss = accuracy < LOW_ACCURACY_THRESHOLD
        is_hit = accuracy >= HIGH_ACCURACY_THRESHOLD
        if not (is_miss or is_hit):
            continue

        try:
            with engine.begin() as conn:
                edges = _load_edges_for_pair(
                    conn,
                    shock_node=row["shock_node"],
                    ticker=row["ticker"],
                )
                if not edges:
                    summary["skipped_no_edge"] += 1
                    continue

                for edge in edges:
                    if is_miss:
                        implied = _compute_implied_pct(
                            actual_price_move_pct=row["actual_price_move_pct"],
                            shock_magnitude=row["shock_magnitude"],
                            predicted_margin_impact_pct=row["predicted_margin_impact_pct"],
                        )
                        if implied is None:
                            summary["skipped_no_implied"] += 1
                            continue

                        new_value, delta, capped = _blend_and_cap(
                            old_value=edge["pct_downstream_cogs"],
                            implied_value=implied,
                        )

                        reason = (
                            f"accuracy={accuracy:.2f} < {LOW_ACCURACY_THRESHOLD:.2f}: "
                            f"decay toward implied {implied:.4f}"
                            + (" (capped)" if capped else "")
                        )

                        if not dry_run:
                            _write_edge_update(
                                conn,
                                edge_id=edge["id"],
                                new_value=new_value,
                            )
                            _write_adjustment_audit(
                                conn,
                                edge_id=edge["id"],
                                upstream_id=edge["upstream_id"],
                                downstream_id=edge["downstream_id"],
                                prediction_id=row["prediction_id"],
                                backtest_result_id=row["backtest_result_id"],
                                event_type="decay",
                                old_value=edge["pct_downstream_cogs"],
                                new_value=new_value,
                                implied_value=implied,
                                accuracy=accuracy,
                                delta=delta,
                                capped=capped,
                                reason=reason,
                            )
                        summary["decayed"] += 1
                        log.info(
                            "contagion_feedback: decay edge={eid} "
                            "{u}->{down} {old}->{new} "
                            "(implied={imp:.4f}, delta={dlt:.4f}{cap})",
                            eid=edge["id"],
                            u=edge["upstream_id"],
                            down=edge["downstream_id"],
                            old=edge["pct_downstream_cogs"],
                            new=round(new_value, 6),
                            imp=implied,
                            dlt=delta,
                            cap=" capped" if capped else "",
                        )
                    else:  # is_hit
                        reason = (
                            f"accuracy={accuracy:.2f} >= {HIGH_ACCURACY_THRESHOLD:.2f}: "
                            "mark edge validated"
                        )
                        if not dry_run:
                            _write_edge_confirmation(conn, edge_id=edge["id"])
                            _write_adjustment_audit(
                                conn,
                                edge_id=edge["id"],
                                upstream_id=edge["upstream_id"],
                                downstream_id=edge["downstream_id"],
                                prediction_id=row["prediction_id"],
                                backtest_result_id=row["backtest_result_id"],
                                event_type="confirm",
                                old_value=edge["pct_downstream_cogs"],
                                new_value=edge["pct_downstream_cogs"],
                                implied_value=None,
                                accuracy=accuracy,
                                delta=0.0,
                                capped=False,
                                reason=reason,
                            )
                        summary["confirmed"] += 1
                        log.info(
                            "contagion_feedback: confirm edge={eid} {u}->{d} "
                            "(accuracy={a:.2f})",
                            eid=edge["id"],
                            u=edge["upstream_id"],
                            d=edge["downstream_id"],
                            a=accuracy,
                        )
        except Exception as exc:
            summary["errors"] += 1
            log.warning(
                "contagion_feedback: failed on backtest {bid}: {e}",
                bid=row.get("backtest_result_id"),
                e=str(exc),
            )

    log.info(
        "contagion_feedback: considered={c} decayed={d} confirmed={h} "
        "no_edge={ne} no_implied={ni} errors={e} dry_run={dr}",
        c=summary["considered"],
        d=summary["decayed"],
        h=summary["confirmed"],
        ne=summary["skipped_no_edge"],
        ni=summary["skipped_no_implied"],
        e=summary["errors"],
        dr=summary["dry_run"],
    )
    return summary
