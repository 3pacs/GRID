"""
GRID — Contagion → Dealer Gamma → Options Trade Ticket bridge (adapter).

For every recent contagion prediction, for each ranked victim downstream,
this module:

  1. Pulls dealer gamma context (gamma wall, put wall, max pain, flip)
  2. Pulls options_daily_signals metadata (ATM IV, max_pain, spot)
  3. Builds a LEVER / CONDITION / THESIS / INVALIDATION trade ticket
  4. Kelly-sizes from historical contagion backtest accuracy
  5. Persists to the immutable decision journal

It is the "connect the dots" glue between:
  - ``intelligence.chain_contagion.simulate_contagion`` (who gets hit)
  - ``physics.dealer_gamma.DealerGammaEngine``         (where price magnets)
  - ``options_daily_signals``                          (IV / max_pain)
  - ``journal.log.DecisionJournal``                    (immutable audit)

SYNTH-13 merge (Wave 3): all pricing and sizing math (Kelly, strike
picker, expiry picker, premium estimator) now lives in
``trading.options_recommender`` as the single canonical source.  This
module is a thin adapter that:

  - re-exports the canonical helpers for backward compatibility
  - owns only contagion-specific data loading and ticket assembly
  - preserves the public entry points ``generate_tickets_for_prediction``
    and ``generate_tickets_for_recent_predictions`` so
    ``api.routers.trade_tickets`` and the frontend response shape are
    untouched.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Canonical pricing / sizing helpers — single source of truth lives in
# options_recommender.py.  Re-exported below so existing callers and
# tests keep working without change.
from trading.options_recommender import (
    DTE_SCALE_BY_IMPACT,
    MAX_KELLY_PER_TICKET,
    MIN_DTE,
    compute_kelly_fraction,
    estimate_premium,
    pick_expiry,
    pick_strike,
)

# ── Contagion-specific tunables ────────────────────────────────────────────

MIN_ABS_MARGIN_IMPACT: float = 0.01
DEFAULT_CONFIDENCE_NO_HISTORY: float = 0.55
INVALIDATION_SHORT_UP_PCT: float = 0.02
INVALIDATION_LONG_DOWN_PCT: float = 0.02
FLOW_THESIS_TAG: str = "contagion_derived"


# ── Data loaders ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ContagionRow:
    """Typed view of a ``contagion_predictions`` row."""

    id: int
    shock_node: str
    shock_type: str
    magnitude: float
    max_depth: int
    simulated_at: datetime
    summary: dict[str, Any]
    ranked_impact: list[dict[str, Any]]


def _as_json(value: Any) -> Any:
    """Coerce a JSONB column to python (psycopg2 returns dict; SA sometimes str)."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, json.JSONDecodeError):
            return None
    return value


def _row_to_contagion(row: Any) -> ContagionRow:
    summary = _as_json(row[6]) or {}
    ranked = _as_json(row[7]) or []
    return ContagionRow(
        id=int(row[0]),
        shock_node=str(row[1]),
        shock_type=str(row[2]),
        magnitude=float(row[3]),
        max_depth=int(row[4]),
        simulated_at=row[5],
        summary=summary if isinstance(summary, dict) else {},
        ranked_impact=ranked if isinstance(ranked, list) else [],
    )


def _load_prediction(engine: Engine, prediction_id: int) -> ContagionRow | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, shock_node, shock_type, magnitude, max_depth,
                       simulated_at, summary, ranked_impact
                FROM contagion_predictions
                WHERE id = :id
                """
            ),
            {"id": int(prediction_id)},
        ).fetchone()
    return _row_to_contagion(row) if row is not None else None


def _load_recent_predictions(
    engine: Engine, since_hours: int
) -> list[ContagionRow]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, shock_node, shock_type, magnitude, max_depth,
                       simulated_at, summary, ranked_impact
                FROM contagion_predictions
                WHERE simulated_at >= NOW() - (:h || ' hours')::INTERVAL
                ORDER BY simulated_at DESC
                """
            ),
            {"h": int(since_hours)},
        ).fetchall()
    return [_row_to_contagion(r) for r in rows]


def _load_options_signal(engine: Engine, ticker: str) -> dict[str, Any] | None:
    """Latest row from ``options_daily_signals`` for a ticker."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT ticker, signal_date, put_call_ratio, max_pain,
                       iv_skew, spot_price, iv_atm, near_expiry
                FROM options_daily_signals
                WHERE LOWER(ticker) = LOWER(:t)
                ORDER BY signal_date DESC
                LIMIT 1
                """
            ),
            {"t": ticker},
        ).fetchone()
    if row is None:
        return None
    spot = float(row[5]) if row[5] is not None else None
    if spot is None or spot <= 0:
        return None
    return {
        "ticker": str(row[0]),
        "signal_date": row[1],
        "put_call_ratio": float(row[2]) if row[2] is not None else None,
        "max_pain": float(row[3]) if row[3] is not None else None,
        "iv_skew": float(row[4]) if row[4] is not None else None,
        "spot_price": spot,
        "iv_atm": float(row[6]) if row[6] is not None else 0.30,
        "near_expiry": row[7],
    }


def _load_dealer_gamma_context(
    engine: Engine, ticker: str
) -> dict[str, Any] | None:
    """Wrap ``DealerGammaEngine.compute_gex_profile`` and return magnets."""
    try:
        from physics.dealer_gamma import DealerGammaEngine

        eng = DealerGammaEngine(engine)
        profile = eng.compute_gex_profile(ticker)
    except Exception as exc:  # pragma: no cover — defensive
        log.debug("dealer_gamma failed for {t}: {e}", t=ticker, e=str(exc))
        return None

    if not profile or "error" in profile:
        return None
    return {
        "gamma_wall": profile.get("gamma_wall"),
        "put_wall": profile.get("put_wall"),
        "call_wall": profile.get("call_wall"),
        "flip_level": profile.get("gamma_flip"),
        "regime": profile.get("regime"),
        "spot": profile.get("spot"),
    }


def _load_contagion_accuracy(
    engine: Engine, shock_type: str | None = None
) -> tuple[float, int]:
    """Return (accuracy, sample_size) from ``contagion_backtest_results``."""
    query = """
        SELECT AVG(b.accuracy_score), COUNT(*)
        FROM contagion_backtest_results b
    """
    params: dict[str, Any] = {}
    if shock_type:
        query += """
            JOIN contagion_predictions p ON p.id = b.prediction_id
            WHERE p.shock_type = :st AND b.accuracy_score IS NOT NULL
        """
        params["st"] = shock_type
    else:
        query += " WHERE b.accuracy_score IS NOT NULL"

    try:
        with engine.connect() as conn:
            row = conn.execute(text(query), params).fetchone()
    except Exception as exc:
        log.debug("contagion accuracy query failed: {e}", e=str(exc))
        return -1.0, 0

    if row is None or row[0] is None:
        return -1.0, 0
    return float(row[0]), int(row[1] or 0)


# ── Ticket assembly ───────────────────────────────────────────────────────


def _build_thesis(
    prediction: ContagionRow,
    victim: dict[str, Any],
    ticker: str,
    direction: str,
    invalidation_price: float,
    dte: int,
) -> str:
    """Construct the LEVER / CONDITION / THESIS / INVALIDATION string."""
    shock_type_h = prediction.shock_type.replace("_", " ")
    margin_pct = float(victim.get("margin_impact_pct", 0.0)) * 100.0
    path = victim.get("path") or []
    chain_descr = " → ".join(path) if path else f"{prediction.shock_node} → {ticker}"
    price_move_pct = abs(margin_pct) * 2.0
    magnitude_pct = prediction.magnitude * 100.0

    return (
        f"LEVER: {prediction.shock_node} {shock_type_h} "
        f"(magnitude {magnitude_pct:.0f}%, shock_node={prediction.shock_node}). "
        f"CONDITION: {ticker} exposed via supply chain "
        f"({chain_descr}). "
        f"THESIS: {margin_pct:+.1f}% margin hit translates to "
        f"~{-price_move_pct:+.1f}% price move over next {dte}d. "
        f"INVALIDATION: {direction} thesis dies if {ticker} "
        f"{'reclaims' if direction == 'short' else 'breaks'} "
        f"${invalidation_price:.2f}."
    )


def _build_single_ticket(
    engine: Engine,
    prediction: ContagionRow,
    victim: dict[str, Any],
    accuracy: float,
    accuracy_n: int,
) -> tuple[dict[str, Any] | None, str | None]:
    """Turn one ranked-impact entry into a trade ticket."""
    raw_id = str(victim.get("id") or "").strip()
    if not raw_id:
        return None, "victim missing id"
    ticker = raw_id.upper()

    margin = float(victim.get("margin_impact_pct", 0.0))
    if abs(margin) < MIN_ABS_MARGIN_IMPACT:
        return None, f"margin_impact too small ({margin:.4f})"

    signal = _load_options_signal(engine, ticker)
    if signal is None:
        return None, "no options_daily_signals data"

    gamma_ctx = _load_dealer_gamma_context(engine, ticker)

    spot = float(signal["spot_price"])
    iv_atm = float(signal.get("iv_atm") or 0.30)
    max_pain = signal.get("max_pain")

    direction = "short" if margin < 0 else "long"
    instrument = "put" if direction == "short" else "call"

    # Canonical pricing/sizing math — imported from options_recommender.
    strike = pick_strike(spot, direction, gamma_ctx, max_pain)
    expiry_iso, dte = pick_expiry(prediction.simulated_at, margin)
    entry_premium, target_premium, stop_premium = estimate_premium(
        spot, iv_atm, dte
    )

    if direction == "short":
        invalidation_price = round(spot * (1 + INVALIDATION_SHORT_UP_PCT), 2)
    else:
        invalidation_price = round(spot * (1 - INVALIDATION_LONG_DOWN_PCT), 2)

    # Kelly sizing: fall back to a conservative confidence when history is empty.
    if accuracy_n > 0 and accuracy >= 0:
        confidence = float(accuracy)
    else:
        confidence = DEFAULT_CONFIDENCE_NO_HISTORY
    kelly = compute_kelly_fraction(confidence)

    thesis = _build_thesis(
        prediction, victim, ticker, direction, invalidation_price, dte
    )

    ticket: dict[str, Any] = {
        "prediction_id": prediction.id,
        "ticker": ticker.lower(),
        "thesis": thesis,
        "direction": direction,
        "instrument": instrument,
        "strike": strike,
        "expiry": expiry_iso,
        "dte": dte,
        "entry_premium": entry_premium,
        "target_premium": target_premium,
        "stop_premium": stop_premium,
        "kelly_size": kelly,
        "invalidation_price": invalidation_price,
        "underlying_price": round(spot, 2),
        "dealer_gamma_context": {
            "gamma_wall": gamma_ctx.get("gamma_wall") if gamma_ctx else None,
            "max_pain": max_pain,
            "flip_level": gamma_ctx.get("flip_level") if gamma_ctx else None,
            "put_wall": gamma_ctx.get("put_wall") if gamma_ctx else None,
            "call_wall": gamma_ctx.get("call_wall") if gamma_ctx else None,
            "regime": gamma_ctx.get("regime") if gamma_ctx else None,
        },
        "confidence": round(confidence, 4),
        "margin_impact_pct": round(margin, 6),
        "shock_node": prediction.shock_node,
        "shock_type": prediction.shock_type,
        "flow_thesis": FLOW_THESIS_TAG,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return ticket, None


# ── Journal writer ────────────────────────────────────────────────────────


def _get_default_model_version_id(engine: Engine) -> int | None:
    """Find the model_version_id to attach ticket journal entries to."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id FROM model_registry
                    WHERE state = 'PRODUCTION'
                    ORDER BY id ASC
                    LIMIT 1
                    """
                )
            ).fetchone()
            if row:
                return int(row[0])
            row = conn.execute(
                text("SELECT id FROM model_registry ORDER BY id ASC LIMIT 1")
            ).fetchone()
            return int(row[0]) if row else None
    except Exception as exc:
        log.debug("model_registry lookup failed: {e}", e=str(exc))
        return None


def write_ticket_to_journal(
    engine: Engine,
    ticket: dict[str, Any],
    model_version_id: int | None = None,
) -> int | None:
    """Append a ticket to ``decision_journal``."""
    try:
        from journal.log import DecisionJournal
    except Exception as exc:  # pragma: no cover — import guard
        log.warning("journal import failed: {e}", e=str(exc))
        return None

    mv_id = model_version_id or _get_default_model_version_id(engine)
    if mv_id is None:
        log.debug("journal skip: no model_version_id available")
        return None

    confidence = float(ticket.get("confidence") or 0.5)
    confidence = max(0.0, min(1.0, confidence))
    state_label = f"CONTAGION_{str(ticket.get('shock_type','')).upper()}"
    action = (
        f"{ticket['direction'].upper()} {ticket['instrument'].upper()} "
        f"{ticket['ticker'].upper()} {ticket['strike']} "
        f"exp {ticket['expiry']} kelly={ticket['kelly_size']}"
    )
    contradiction = {
        "flow_thesis": FLOW_THESIS_TAG,
        "prediction_id": ticket.get("prediction_id"),
        "shock_node": ticket.get("shock_node"),
        "shock_type": ticket.get("shock_type"),
        "margin_impact_pct": ticket.get("margin_impact_pct"),
        "dealer_gamma_context": ticket.get("dealer_gamma_context"),
    }

    try:
        journal = DecisionJournal(db_engine=engine)
        decision_id = journal.log_decision(
            model_version_id=mv_id,
            inferred_state=state_label,
            state_confidence=confidence,
            transition_probability=min(1.0, max(0.0, ticket.get("kelly_size", 0.0))),
            contradiction_flags=contradiction,
            grid_recommendation=action,
            baseline_recommendation="HOLD",
            action_taken=action,
            counterfactual=ticket.get("thesis", ""),
            operator_confidence=(
                "HIGH" if confidence >= 0.65
                else ("MEDIUM" if confidence >= 0.5 else "LOW")
            ),
        )
        return int(decision_id)
    except Exception as exc:
        log.warning(
            "journal write failed for ticket {p}/{t}: {e}",
            p=ticket.get("prediction_id"),
            t=ticket.get("ticker"),
            e=str(exc),
        )
        return None


# ── Public API ────────────────────────────────────────────────────────────


def generate_tickets_for_prediction(
    engine: Engine,
    prediction_id: int,
    journal: bool = True,
) -> list[dict[str, Any]]:
    """For each victim in a contagion prediction, emit a trade ticket."""
    prediction = _load_prediction(engine, prediction_id)
    if prediction is None:
        log.warning("contagion prediction {id} not found", id=prediction_id)
        return []

    accuracy, n = _load_contagion_accuracy(engine, prediction.shock_type)
    tickets: list[dict[str, Any]] = []
    skip_counts: dict[str, int] = {}

    for victim in prediction.ranked_impact:
        ticket, reason = _build_single_ticket(
            engine, prediction, victim, accuracy, n
        )
        if ticket is None:
            if reason:
                skip_counts[reason] = skip_counts.get(reason, 0) + 1
                log.debug("skip {t}: {r}", t=victim.get("id"), r=reason)
            continue
        if journal:
            jid = write_ticket_to_journal(engine, ticket)
            if jid is not None:
                ticket["journal_id"] = jid
        tickets.append(ticket)

    if skip_counts:
        log.info(
            "prediction {id}: {n} tickets, skips={s}",
            id=prediction_id,
            n=len(tickets),
            s=skip_counts,
        )
    return tickets


def generate_tickets_for_recent_predictions(
    engine: Engine,
    since_hours: int = 24,
    journal: bool = True,
) -> list[dict[str, Any]]:
    """Scan ``contagion_predictions`` within the window and produce tickets."""
    rows = _load_recent_predictions(engine, since_hours=since_hours)
    all_tickets: list[dict[str, Any]] = []
    for row in rows:
        all_tickets.extend(
            generate_tickets_for_prediction(engine, row.id, journal=journal)
        )
    log.info(
        "contagion→ticket scan: {n} tickets from {p} predictions in last {h}h",
        n=len(all_tickets),
        p=len(rows),
        h=since_hours,
    )
    return all_tickets


_PRODUCER_MODULE = "trading.contagion_to_ticket"


def finalize_ticket(
    engine: Engine,
    ticket_id: int | str,
    pnl: float,
    outcome: str,
    ticker: str,
    strategy: str = "contagion",
    signals_used: list[str] | None = None,
    duration_s: int = 0,
) -> None:
    """Mark a contagion ticket as closed and emit ``OptionsTradeOutcome``.

    The immutable decision journal forbids UPDATEs, so the "close" event
    is surfaced via the contracts layer rather than by mutating the
    original journal row. The handler on the other end
    (``contracts.handlers.trade_outcomes``) consumes this to nudge the
    contagion oracle model head's weight.

    Non-fatal: any emit failure is logged and swallowed.
    """
    try:
        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import OptionsTradeOutcome
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug("contagion_to_ticket: contracts import failed: {e}", e=str(exc))
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    try:
        trade_id_int = int(ticket_id)
    except (TypeError, ValueError):
        # The OptionsTradeOutcome schema pins ``trade_id: int`` — use a
        # stable hash when the caller passes a string key.
        trade_id_int = abs(hash(str(ticket_id))) % (10**9)

    signal_mix: dict[str, float] = {}
    for tag in signals_used or []:
        if not tag:
            continue
        signal_mix[str(tag)] = signal_mix.get(str(tag), 0.0) + 1.0
    # Normalise so the mix sums to ~1 without being mathematically brittle
    total = sum(signal_mix.values())
    if total > 0:
        signal_mix = {k: v / total for k, v in signal_mix.items()}

    hit_levels = {
        "pnl_positive": bool(pnl > 0),
        "closed": True,
        outcome.upper(): True,
    }

    from decimal import Decimal

    try:
        pnl_decimal = Decimal(str(float(pnl)))
    except Exception:
        pnl_decimal = Decimal("0")

    try:
        _emit(
            OptionsTradeOutcome(
                producer_module=_PRODUCER_MODULE,
                correlation_id=corr_id,
                trade_id=trade_id_int,
                ticker=str(ticker).upper(),
                strategy=str(strategy),
                pnl=pnl_decimal,
                signal_mix=signal_mix,
                hit_levels=hit_levels,
                duration_s=int(duration_s),
            )
        )
    except Exception as exc:  # non-fatal per SYNTH-C contract
        log.debug(
            "contagion_to_ticket finalize emit failed for {i}: {e}",
            i=ticket_id, e=str(exc),
        )


__all__ = [
    "generate_tickets_for_prediction",
    "generate_tickets_for_recent_predictions",
    "write_ticket_to_journal",
    "finalize_ticket",
    # Re-exported canonical helpers for backward compatibility.
    "compute_kelly_fraction",
    "pick_strike",
    "pick_expiry",
    "estimate_premium",
    # Tunables used by callers / tests.
    "FLOW_THESIS_TAG",
    "MAX_KELLY_PER_TICKET",
    "MIN_ABS_MARGIN_IMPACT",
    "MIN_DTE",
    "DTE_SCALE_BY_IMPACT",
]
