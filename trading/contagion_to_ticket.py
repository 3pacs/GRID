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
    PREMIUM_BASIS_MODELLED,
    compute_kelly_fraction,
    estimate_premium,
    pick_expiry,
    pick_strike,
)

# ── Contagion-specific tunables ────────────────────────────────────────────

MIN_ABS_MARGIN_IMPACT: float = 0.01
INVALIDATION_SHORT_UP_PCT: float = 0.02
INVALIDATION_LONG_DOWN_PCT: float = 0.02
FLOW_THESIS_TAG: str = "contagion_derived"

# Reported as ``confidence_basis`` when ``contagion_backtest_results`` holds
# nothing for this shock type.  Audit C-H4: this branch used to substitute a
# module-level 0.55 and feed it straight into ``compute_kelly_fraction``, so
# a ticket with ZERO backtest history was position-sized as though it had a
# measured 55% edge.  That constant is deleted: no history means no
# confidence and no size.
CONFIDENCE_BASIS_NO_HISTORY: str = "no_backtest_history"
CONFIDENCE_BASIS_BACKTEST: str = "contagion_backtest_accuracy"


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
    engine: Engine,
    since_hours: int,
    limit: int | None = None,
) -> list[ContagionRow]:
    params: dict[str, int] = {"h": int(since_hours)}
    limit_clause = ""
    if limit is not None:
        limit = int(limit)
        if limit > 0:
            params["limit"] = limit
            limit_clause = "LIMIT :limit"

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, shock_node, shock_type, magnitude, max_depth,
                       simulated_at, summary, ranked_impact
                FROM contagion_predictions
                WHERE simulated_at >= NOW() - (:h || ' hours')::INTERVAL
                ORDER BY simulated_at DESC
                """ + limit_clause + """
                """
            ),
            params,
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
        # options_daily_signals.iv_atm is nullable: ingestion writes NULL when
        # no contract sits inside the ATM band. Audit C-H5: this used to
        # become 0.30, which then priced every premium on the ticket through
        # estimate_premium() — an entry, target and stop derived from a
        # constant nobody measured. A missing IV stays missing; the ticket is
        # skipped rather than modelled off a placeholder.
        "iv_atm": float(row[6]) if row[6] is not None else None,
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


# GEX-7: dealer_flow (crypto-first via Deribit/Paradex) is wired here even
# though the pipeline is still a scaffold. Once GEX-8 implements the real
# Deribit adapter this path lights up automatically — no contagion_to_ticket
# changes needed. Until then it returns None and we fall back to the equity
# dealer_gamma path. The split is by underlying: BTC/ETH/SOL → dealer_flow,
# everything else → dealer_gamma.

_CRYPTO_UNDERLYINGS: frozenset[str] = frozenset({"BTC", "ETH", "SOL"})


def _load_dealer_flow_context(ticker: str) -> dict[str, Any] | None:
    """Return crypto dealer-flow exposures for BTC/ETH/SOL via the GEX V2 pipeline.

    Calls ``physics.dealer_flow.pipeline.run`` with the appropriate venue and
    underlying. Returns ``None`` for non-crypto tickers so the caller can fall
    back to the equity ``_load_dealer_gamma_context`` path.

    The pipeline is a scaffold today — it returns ``{"status": "stub"}``. Once
    GEX-8 lands we get OptionExposure payloads with gamma_wall, vanna_wall,
    flip_level, etc. with no caller change.
    """
    upper = ticker.upper().split("-")[0].split("USDT")[0].split("USD")[0]
    if upper not in _CRYPTO_UNDERLYINGS:
        return None
    try:
        from physics.dealer_flow.pipeline import run as _run_dealer_flow

        result = _run_dealer_flow(venue="deribit", underlying=upper, max_dte_days=7)
    except Exception as exc:  # pragma: no cover — defensive
        log.debug("dealer_flow pipeline failed for {t}: {e}", t=ticker, e=str(exc))
        return None

    if not result or result.get("status") == "stub":
        # Scaffold response — surface nothing yet, but log so the upgrade
        # is visible the first cycle GEX-8 produces real data.
        log.debug(
            "dealer_flow returned scaffold for {t}; awaiting GEX-8",
            t=ticker,
        )
        return None
    # Once GEX-8 lands the result is an OptionExposure payload. Surface
    # the same magnet keys the equity path uses so the ticket schema is
    # identical regardless of venue.
    return {
        "gamma_wall": result.get("gamma_wall"),
        "vanna_wall": result.get("vanna_wall"),
        "charm_wall": result.get("charm_wall"),
        "flip_level": result.get("gamma_flip") or result.get("flip_level"),
        "put_wall": result.get("put_wall"),
        "call_wall": result.get("call_wall"),
        "regime": result.get("regime"),
        "venue": result.get("venue", "deribit"),
        "spot": result.get("spot"),
        "max_pain": result.get("max_pain"),
        "row_confidence": result.get("row_confidence"),
        "snapshot_confidence": result.get("snapshot_confidence"),
    }


def _load_dealer_context(engine: Engine, ticker: str) -> dict[str, Any] | None:
    """Unified dealer-context loader.

    Crypto underlyings (BTC/ETH/SOL) route to the dealer_flow pipeline (GEX V2);
    everything else uses the equity dealer_gamma engine. Result schema is the
    same set of magnet keys so downstream callers are venue-agnostic.
    """
    crypto = _load_dealer_flow_context(ticker)
    if crypto is not None:
        return crypto
    return _load_dealer_gamma_context(engine, ticker)


def _verify_contract(
    engine: Engine,
    ticker: str,
    strike: float,
    expiry_iso: str,
    instrument: str,
) -> bool | None:
    """Is ``(strike, expiry)`` an actually-listed contract?

    Returns ``True`` when a matching ``options_snapshots`` row exists,
    ``False`` when the chain has been seen but this contract is not in it,
    and ``None`` when we could not check (query failed / no chain stored),
    which is a third state and must not be collapsed into ``False``.

    Audit C-M24: ``pick_strike`` falls back to "2% OTM from spot" and
    ``pick_expiry`` snaps to "the next Friday", described in its own
    docstring as "close enough to a listed monthly cycle for a ticket
    card".  Neither is guaranteed to exist.  A ticket naming a strike and
    expiry that were never listed is not a trade anyone can place, and
    nothing in the payload said so.
    """
    if not ticker or not strike or strike <= 0 or not expiry_iso:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM options_snapshots
                    WHERE UPPER(ticker) = UPPER(:t)
                      AND strike = :k
                      AND expiry = CAST(:e AS DATE)
                      AND opt_type = :ot
                    LIMIT 1
                    """
                ),
                {
                    "t": ticker,
                    "k": float(strike),
                    "e": expiry_iso,
                    "ot": instrument,
                },
            ).fetchone()
    except Exception as exc:  # noqa: BLE001 — any DB failure means "unknown"
        # An unanswerable question is not a "no". Returning False here would
        # let a failed query masquerade as a verified-absent contract.
        log.debug(
            "contract verification failed for {t} {k} {e}: {err}",
            t=ticker, k=strike, e=expiry_iso, err=str(exc),
        )
        return None
    return row is not None


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

    # GEX-7: unified dealer-context loader — crypto goes through dealer_flow,
    # equity through dealer_gamma. Schema is identical so no downstream change.
    gamma_ctx = _load_dealer_context(engine, ticker)

    spot = float(signal["spot_price"])
    max_pain = signal.get("max_pain")

    # No measured ATM IV → no modelled premium → no ticket (audit C-H5).
    iv_raw = signal.get("iv_atm")
    if iv_raw is None:
        return None, "no iv_atm in options_daily_signals"
    iv_atm = float(iv_raw)
    if iv_atm <= 0:
        return None, f"non-positive iv_atm ({iv_atm})"

    direction = "short" if margin < 0 else "long"
    instrument = "put" if direction == "short" else "call"

    # Canonical pricing/sizing math — imported from options_recommender.
    strike = pick_strike(spot, direction, gamma_ctx, max_pain)
    expiry_iso, dte = pick_expiry(prediction.simulated_at, margin)
    entry_premium, target_premium, stop_premium = estimate_premium(
        spot, iv_atm, dte
    )

    # These premiums are model output from a 1σ move, not quotes. Say so on
    # the ticket rather than letting them read as prices someone showed.
    contract_verified = _verify_contract(
        engine, ticker, strike, expiry_iso, instrument,
    )

    if direction == "short":
        invalidation_price = round(spot * (1 + INVALIDATION_SHORT_UP_PCT), 2)
    else:
        invalidation_price = round(spot * (1 - INVALIDATION_LONG_DOWN_PCT), 2)

    # Kelly sizing comes from measured backtest accuracy or it does not
    # happen (audit C-H4). Zero history is not a 55% edge.
    if accuracy_n > 0 and accuracy >= 0:
        confidence: float | None = float(accuracy)
        confidence_basis = CONFIDENCE_BASIS_BACKTEST
        kelly = compute_kelly_fraction(confidence)
    else:
        confidence = None
        confidence_basis = CONFIDENCE_BASIS_NO_HISTORY
        kelly = 0.0

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
        # Provenance for the three numbers above and for the contract itself.
        "premium_basis": PREMIUM_BASIS_MODELLED,
        "iv_atm": round(iv_atm, 6),
        "contract_verified": contract_verified,
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
        "confidence": round(confidence, 4) if confidence is not None else None,
        "confidence_basis": confidence_basis,
        "confidence_n": int(accuracy_n),
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


def _unscored_reason(ticket: dict[str, Any]) -> str:
    """Spell out, for the permanent record, why a ticket has no confidence.

    Stored in ``decision_journal.confidence_reason``. It must be readable on
    its own years later, so it names the basis tag and the sample size rather
    than assuming the reader can re-derive them.
    """
    basis = ticket.get("confidence_basis") or CONFIDENCE_BASIS_NO_HISTORY
    n = ticket.get("confidence_n")
    n_txt = f"n={n}" if n is not None else "n unknown"
    if basis == CONFIDENCE_BASIS_NO_HISTORY:
        return (
            "unscored: no contagion backtest history for shock_type="
            f"{ticket.get('shock_type')} ({basis}, {n_txt}) — no confidence "
            "was measured and none was invented"
        )
    return (
        f"unscored: confidence unavailable ({basis}, {n_txt}) — no confidence "
        "was measured and none was invented"
    )


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

    # Three states, not two. The pre-#539 code wrote a fabricated 0.5 into the
    # permanent audit record whenever a ticket had no measured confidence; #539
    # replaced that with skipping the row, which traded a false number for a
    # missing record. Neither is the audit the operator needs. Since
    # journal_unscored_conf_0918 the journal can hold the honest third state:
    # state_confidence NULL plus a mandatory reason. Nothing is invented and
    # nothing is omitted.
    raw_confidence = ticket.get("confidence")
    confidence: float | None
    confidence_reason: str | None
    if raw_confidence is None:
        confidence = None
        confidence_reason = _unscored_reason(ticket)
        log.info(
            "journal unscored for {t}: {r}",
            t=ticket.get("ticker"),
            r=confidence_reason,
        )
    else:
        confidence = max(0.0, min(1.0, float(raw_confidence)))
        confidence_reason = (
            f"scored: {ticket.get('confidence_basis') or CONFIDENCE_BASIS_BACKTEST} "
            f"(n={ticket.get('confidence_n')})"
        )
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
            confidence_reason=confidence_reason,
            transition_probability=min(1.0, max(0.0, ticket.get("kelly_size", 0.0))),
            contradiction_flags=contradiction,
            grid_recommendation=action,
            baseline_recommendation="HOLD",
            action_taken=action,
            counterfactual=ticket.get("thesis", ""),
            # operator_confidence is a categorical NOT NULL column and is a
            # different thing from the measured state_confidence. With no
            # measurement at all there is no evidence of an edge, so the
            # truthful category is the floor, LOW — and the row carries
            # confidence_reason so "LOW" is never mistaken for a measurement.
            operator_confidence=(
                "LOW" if confidence is None
                else (
                    "HIGH" if confidence >= 0.65
                    else ("MEDIUM" if confidence >= 0.5 else "LOW")
                )
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
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Scan ``contagion_predictions`` within the window and produce tickets."""
    rows = _load_recent_predictions(
        engine,
        since_hours=since_hours,
        limit=limit,
    )
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
    "PREMIUM_BASIS_MODELLED",
    "CONFIDENCE_BASIS_NO_HISTORY",
    "CONFIDENCE_BASIS_BACKTEST",
]
