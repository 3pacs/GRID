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
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ──────────────────────────────────────────────────────────

FAILURE_CATEGORIES = [
    "wrong_signal",
    "right_signal_wrong_timing",
    "external_shock",
    "bad_data",
    "model_error",
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Table Setup ───────────────────────────────────────────────────────────

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

def generate_postmortem(engine: Engine, trade_id: int) -> PostMortem | None:
    """Generate a full post-mortem for a failed options trade.

    Loads the recommendation, reconstructs decision-time state, loads
    the actual price path, classifies the failure, and uses the LLM
    to produce a narrative explanation (rule-based fallback if offline).

    Args:
        engine: SQLAlchemy engine.
        trade_id: options_recommendations.id

    Returns:
        PostMortem or None if trade not found or not a failure.
    """
    _ensure_tables(engine)

    # Load the recommendation
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, ticker, direction, strike, expiry, entry_price,
                   target_price, stop_loss, confidence, thesis,
                   sanity_status, generated_at, outcome, actual_return,
                   closed_at, dealer_context
            FROM options_recommendations
            WHERE id = :id
        """), {"id": trade_id}).fetchone()

    if not row:
        log.warning("Post-mortem: trade {id} not found", id=trade_id)
        return None

    (rec_id, ticker, signal_type, strike, expiry, entry_price,
     target_price, stop_loss, confidence, thesis,
     sanity_status, generated_at, outcome, actual_return,
     closed_at, dealer_context) = row

    # Only generate for failures
    if outcome not in ("LOSS", "EXPIRED"):
        log.debug("Post-mortem: trade {id} outcome={o} — not a failure", id=trade_id, o=outcome)
        return None

    entry_f = float(entry_price) if entry_price else 0.0
    strike_f = float(strike) if strike else 0.0
    target_f = float(target_price) if target_price else 0.0
    actual_ret = float(actual_return) if actual_return else 0.0
    conf = float(confidence) if confidence else 0.0

    # Parse sanity_status
    sanity = _parse_json(sanity_status)
    thesis_str = thesis or ""
    dealer_ctx = dealer_context or ""

    # Reconstruct data state at decision time
    data_at_decision = _reconstruct_decision_data(
        engine, ticker, generated_at, sanity, dealer_ctx,
    )

    # Load price path from entry to outcome
    price_path = _load_price_path(engine, ticker, generated_at, closed_at or expiry)

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
    )

    # What actually happened (factual summary)
    what_happened = _summarise_what_happened(
        ticker, signal_type, entry_f, strike_f, outcome, actual_ret, price_path,
    )

    # LLM narrative (or rule-based fallback)
    llm_narrative = _get_llm_postmortem(
        ticker=ticker,
        direction=direction,
        thesis=thesis_str,
        outcome=outcome,
        actual_return=actual_ret,
        category=category,
        root_cause=root_cause,
        what_happened=what_happened,
        data_at_decision=data_at_decision,
        price_path=price_path,
        signals_wrong=signals_wrong,
        signals_right=signals_right,
    )

    recommended_fix = llm_narrative.get("recommended_fix", root_cause)
    what_missed_final = llm_narrative.get("what_we_missed", what_missed)
    analysis_confidence = llm_narrative.get("confidence", 0.5)

    now = datetime.now(timezone.utc)

    pm = PostMortem(
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
    _store_postmortem(engine, pm, trade_id=trade_id, prediction_id=None)

    log.info(
        "Post-mortem generated for trade {id} ({t} {d}): {cat}",
        id=trade_id, t=ticker, d=direction, cat=category,
    )
    return pm


# ── 2. Generate Post-Mortem for an Oracle Prediction ──────────────────────

def generate_prediction_postmortem(engine: Engine, prediction_id: str) -> PostMortem | None:
    """Generate a full post-mortem for a failed oracle prediction.

    Same analysis pipeline as trade post-mortems but queries the
    oracle_predictions table instead.

    Args:
        engine: SQLAlchemy engine.
        prediction_id: oracle_predictions.id (text hash).

    Returns:
        PostMortem or None if prediction not found or not a failure.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, ticker, direction, target_price, entry_price, expiry,
                   confidence, expected_move_pct, model_name, signals,
                   anti_signals, flow_context, verdict, actual_price,
                   actual_move_pct, pnl_pct, scored_at, score_notes,
                   created_at
            FROM oracle_predictions
            WHERE id = :id
        """), {"id": prediction_id}).fetchone()

    if not row:
        log.warning("Post-mortem: prediction {id} not found", id=prediction_id)
        return None

    (pred_id, ticker, signal_type, target_price, entry_price, expiry,
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

    entry_f = float(entry_price) if entry_price else 0.0
    target_f = float(target_price) if target_price else 0.0
    actual_ret = float(pnl_pct) if pnl_pct else 0.0
    conf = float(confidence) if confidence else 0.0

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
    thesis_str = (
        f"{model_name} predicted {direction} on {ticker} with "
        f"{conf:.0%} confidence based on signals: {', '.join(signal_names[:5])}"
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
        ticker, signal_type, entry_f, target_f,
        "miss", actual_ret, price_path,
    )

    llm_narrative = _get_llm_postmortem(
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

    recommended_fix = llm_narrative.get("recommended_fix", root_cause)
    what_missed_final = llm_narrative.get("what_we_missed", what_missed)
    analysis_confidence = llm_narrative.get("confidence", 0.5)

    now = datetime.now(timezone.utc)

    pm = PostMortem(
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

    _store_postmortem(engine, pm, trade_id=None, prediction_id=prediction_id)

    log.info(
        "Post-mortem generated for prediction {id} ({t} {d}): {cat}",
        id=prediction_id, t=ticker, d=direction, cat=category,
    )
    return pm


# ── 3. Batch Post-Mortem ──────────────────────────────────────────────────

def batch_postmortem(engine: Engine, days: int = 30) -> list[PostMortem]:
    """Generate post-mortems for all failed trades and predictions in the last N days.

    Aggregates patterns across failures and logs systemic issues.

    Args:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        list of PostMortem objects.
    """
    _ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)
    postmortems: list[PostMortem] = []

    # Failed trades
    with engine.connect() as conn:
        trade_rows = conn.execute(text("""
            SELECT id FROM options_recommendations
            WHERE outcome IN ('LOSS', 'EXPIRED')
              AND closed_at >= :cutoff
              AND id NOT IN (
                  SELECT trade_id FROM trade_postmortems
                  WHERE trade_id IS NOT NULL
              )
            ORDER BY closed_at DESC
        """), {"cutoff": cutoff}).fetchall()

        pred_rows = conn.execute(text("""
            SELECT id FROM oracle_predictions
            WHERE verdict = 'miss'
              AND scored_at >= :cutoff
              AND id NOT IN (
                  SELECT prediction_id FROM trade_postmortems
                  WHERE prediction_id IS NOT NULL
              )
            ORDER BY scored_at DESC
        """), {"cutoff": cutoff}).fetchall()

    # Generate trade post-mortems
    for (trade_id,) in trade_rows:
        try:
            pm = generate_postmortem(engine, trade_id)
            if pm:
                postmortems.append(pm)
        except Exception as exc:
            log.warning("Post-mortem failed for trade {id}: {e}", id=trade_id, e=str(exc))

    # Generate prediction post-mortems
    for (pred_id,) in pred_rows:
        try:
            pm = generate_prediction_postmortem(engine, pred_id)
            if pm:
                postmortems.append(pm)
        except Exception as exc:
            log.warning("Post-mortem failed for prediction {id}: {e}", id=pred_id, e=str(exc))

    # Aggregate patterns
    if postmortems:
        _log_aggregate_patterns(postmortems, days)

    log.info(
        "Batch post-mortem complete: {n} failures analysed over {d} days",
        n=len(postmortems), d=days,
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

def _store_postmortem(
    engine: Engine,
    pm: PostMortem,
    trade_id: int | None,
    prediction_id: str | None,
) -> None:
    """Persist a post-mortem to the database."""
    with engine.begin() as conn:
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
        FROM trade_postmortems
        WHERE generated_at >= :cutoff
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
            "full_analysis": _parse_json(r[11]),
            "confidence": float(r[12]) if r[12] is not None else None,
            "generated_at": r[13].isoformat() if r[13] else None,
        })

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


def _reconstruct_decision_data(
    engine: Engine,
    ticker: str,
    decision_date: Any,
    sanity: dict,
    dealer_context: str,
) -> dict[str, Any]:
    """Reconstruct the data state at the time a recommendation was made.

    Gathers scanner signals, GEX data, and regime context that were
    available on the decision date.
    """
    data: dict[str, Any] = {
        "sanity_status": sanity,
        "dealer_context": dealer_context,
    }

    if not decision_date:
        return data

    # Coerce to date
    if isinstance(decision_date, datetime):
        as_of = decision_date.date()
    elif isinstance(decision_date, date):
        as_of = decision_date
    else:
        try:
            as_of = datetime.fromisoformat(str(decision_date)).date()
        except (ValueError, TypeError):
            return data

    with engine.connect() as conn:
        # Options scanner signals near decision date
        opt_row = conn.execute(text("""
            SELECT put_call_ratio, iv_atm, iv_skew, max_pain, spot_price,
                   total_oi, term_structure_slope, oi_concentration, signal_date
            FROM options_daily_signals
            WHERE ticker = :t AND signal_date <= :d
            ORDER BY signal_date DESC LIMIT 1
        """), {"t": ticker, "d": as_of}).fetchone()

        if opt_row:
            data["scanner_signals"] = {
                "put_call_ratio": float(opt_row[0]) if opt_row[0] else None,
                "iv_atm": float(opt_row[1]) if opt_row[1] else None,
                "iv_skew": float(opt_row[2]) if opt_row[2] else None,
                "max_pain": float(opt_row[3]) if opt_row[3] else None,
                "spot_price": float(opt_row[4]) if opt_row[4] else None,
                "total_oi": float(opt_row[5]) if opt_row[5] else None,
                "term_structure_slope": float(opt_row[6]) if opt_row[6] else None,
                "oi_concentration": float(opt_row[7]) if opt_row[7] else None,
                "signal_date": str(opt_row[8]) if opt_row[8] else None,
            }

        # Regime at decision time
        regime_row = conn.execute(text("""
            SELECT inferred_state, state_confidence, transition_probability
            FROM decision_journal
            WHERE decision_timestamp <= :d
            ORDER BY decision_timestamp DESC LIMIT 1
        """), {"d": as_of}).fetchone()

        if regime_row:
            data["regime"] = {
                "state": regime_row[0],
                "confidence": float(regime_row[1]) if regime_row[1] else None,
                "transition_prob": float(regime_row[2]) if regime_row[2] else None,
            }

        # Capital flow context at decision time
        flow_row = conn.execute(text("""
            SELECT narrative, snapshot_date
            FROM capital_flow_snapshots
            WHERE snapshot_date <= :d
            ORDER BY snapshot_date DESC LIMIT 1
        """), {"d": as_of}).fetchone()

        if flow_row:
            data["flow_narrative"] = str(flow_row[0])[:500] if flow_row[0] else None
            data["flow_date"] = str(flow_row[1]) if flow_row[1] else None

    return data


def _load_price_path(
    engine: Engine,
    ticker: str,
    start_date: Any,
    end_date: Any,
) -> list[dict]:
    """Load day-by-day price data from entry to exit.

    Returns a list of {date, price} dicts.
    """
    if not start_date or not end_date:
        return []

    # Coerce dates
    def _to_date(val: Any) -> date | None:
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        try:
            return datetime.fromisoformat(str(val)).date()
        except (ValueError, TypeError):
            return None

    start = _to_date(start_date)
    end = _to_date(end_date)
    if not start or not end:
        return []

    path: list[dict] = []

    with engine.connect() as conn:
        # Try options_daily_signals first
        rows = conn.execute(text("""
            SELECT signal_date, spot_price FROM options_daily_signals
            WHERE ticker = :t AND signal_date >= :s AND signal_date <= :e
              AND spot_price > 0
            ORDER BY signal_date
        """), {"t": ticker, "s": start, "e": end}).fetchall()

        if rows:
            for r in rows:
                path.append({"date": str(r[0]), "price": float(r[1])})
            return path

        # Fallback to raw_series
        rows = conn.execute(text("""
            SELECT obs_date, value FROM raw_series
            WHERE series_id = :sid
              AND obs_date >= :s AND obs_date <= :e
              AND pull_status = 'SUCCESS'
            ORDER BY obs_date
        """), {"sid": f"YF:{ticker}:close", "s": start, "e": end}).fetchall()

        for r in rows:
            path.append({"date": str(r[0]), "price": float(r[1])})

    return path


def _classify_failure(
    *,
    ticker: str,
    direction: str,
    entry_price: float,
    strike: float,
    target: float,
    expiry: Any,
    outcome: str,
    actual_return: float,
    price_path: list[dict],
    data_at_decision: dict,
    sanity: dict,
) -> tuple[str, str, list[str], list[str], str]:
    """Classify a trade failure into a category with root cause analysis.

    Returns:
        (failure_category, root_cause, signals_wrong, signals_right, what_we_missed)
    """
    signals_wrong: list[str] = []
    signals_right: list[str] = []
    what_missed = ""

    # Check if direction was ever right during the trade
    direction_was_right_at_some_point = False
    max_favorable_move = 0.0
    if price_path and entry_price > 0:
        for p in price_path:
            price = p.get("price", 0)
            if not price:
                continue
            move_pct = (price - entry_price) / entry_price * 100
            if direction == "CALL" and move_pct > 0:
                direction_was_right_at_some_point = True
                max_favorable_move = max(max_favorable_move, move_pct)
            elif direction == "PUT" and move_pct < 0:
                direction_was_right_at_some_point = True
                max_favorable_move = max(max_favorable_move, abs(move_pct))

    # Check scanner signals
    scanner = data_at_decision.get("scanner_signals", {})
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

    # Check sanity layers
    for layer, result in sanity.items():
        if isinstance(result, dict):
            status = result.get("status", "")
            if status == "FAIL":
                signals_wrong.append(f"sanity_{layer}_failed")
            elif status == "PASS":
                signals_right.append(f"sanity_{layer}_passed")

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

    # 2. Bad data — check for stale signals
    scanner_date = data_at_decision.get("scanner_signals", {}).get("signal_date")
    if scanner_date:
        try:
            sig_date = datetime.fromisoformat(str(scanner_date)).date() if "T" in str(scanner_date) else date.fromisoformat(str(scanner_date))
            expiry_d = datetime.fromisoformat(str(expiry)).date() if isinstance(expiry, str) else expiry
            if isinstance(expiry_d, date) and (expiry_d - sig_date).days > 7:
                category = "bad_data"
                root_cause = (
                    f"Scanner signals were {(expiry_d - sig_date).days} days old at decision time. "
                    f"Data may have been stale."
                )
                what_missed = "Data freshness check should have flagged stale signals."
                return category, root_cause, signals_wrong, signals_right, what_missed
        except (ValueError, TypeError):
            pass

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

    # 4. Check sanity failures — potential model error
    failed_sanity_count = sum(
        1 for v in sanity.values()
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
        f"Scanner signals pointed {direction} for {ticker} but the underlying "
        f"moved against the position. Actual return: {actual_return:+.2%}."
    )
    what_missed = "Signal ensemble did not capture the true market direction."

    return category, root_cause, signals_wrong, signals_right, what_missed


def _classify_prediction_failure(
    *,
    ticker: str,
    direction: str,
    entry_price: float,
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
                f"Anti-signals warned against this prediction but were overridden. "
                f"Net signal strength was insufficient.",
                signals_wrong,
                signals_right,
                what_missed,
            )

    # Check timing
    direction_was_right = False
    if price_path and entry_price > 0:
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
            f"Direction was correct at some point during the prediction window "
            f"but the move did not persist through expiry.",
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
    entry_price: float,
    strike: float,
    outcome: str,
    actual_return: float,
    price_path: list[dict],
) -> str:
    """Build a factual summary of what happened."""
    parts = [f"{ticker} {direction}: entered at ${entry_price:.2f}, strike ${strike:.2f}."]

    if price_path:
        first_price = price_path[0].get("price", entry_price)
        last_price = price_path[-1].get("price", entry_price)
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
) -> dict[str, Any]:
    """Ask the LLM for a narrative post-mortem explanation.

    Returns a dict with keys: recommended_fix, what_we_missed, confidence.
    Falls back to rule-based defaults if LLM is unavailable.
    """
    defaults = {
        "recommended_fix": root_cause,
        "what_we_missed": "Unable to generate LLM analysis.",
        "confidence": 0.5,
    }

    try:
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.REASON)
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

    # Truncate data_at_decision for the prompt
    data_summary = json.dumps(data_at_decision, indent=2, default=str)[:1500]

    # RAG: retrieve historical context — past failures, lessons learned
    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = f"{ticker} {direction} postmortem {category} {root_cause}"
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=2000)
    except Exception:
        pass

    prompt = (
        f"You are a quantitative trading analyst conducting a post-mortem.\n\n"
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
        f"DATA AT DECISION:\n{data_summary}\n\n"
        f"SIGNALS WRONG: {', '.join(signals_wrong) or 'none identified'}\n"
        f"SIGNALS RIGHT: {', '.join(signals_right) or 'none identified'}\n\n"
        f"Provide:\n"
        f"1. What we missed — reference similar past failures if available (1-2 sentences)\n"
        f"2. Recommended fix (1-2 specific, actionable sentences)\n"
        f"3. Confidence in this analysis (0.0 to 1.0)\n\n"
        f"Format as:\n"
        f"MISSED: ...\n"
        f"FIX: ...\n"
        f"CONFIDENCE: 0.X"
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="You are a quantitative trading analyst. Be specific, data-driven, concise.",
            temperature=0.3,
        )

        if not response:
            return defaults

        result = dict(defaults)
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.upper().startswith("MISSED:"):
                result["what_we_missed"] = line[7:].strip()
            elif line.upper().startswith("FIX:"):
                result["recommended_fix"] = line[4:].strip()
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
        llm = get_llm(Tier.REASON)
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
    root_causes = "\n".join(
        f"  - {pm.ticker} {pm.direction}: {pm.root_cause[:100]}"
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
        f"Focus on what to STOP doing, what to WEIGHT more heavily, and what to ADD.\n"
        f"Be concise — one line per recommendation."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system=(
                "You are a quantitative trading analyst. Be specific and data-driven. "
                "Every recommendation must reference the data above."
            ),
            temperature=0.3,
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
