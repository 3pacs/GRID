"""CAT-190 — Automatic thesis invalidation monitor.

Every prediction/trade recommendation in the system MUST carry a
pre-registered invalidation condition. Per the CLAUDE.md causation SOP:

    INVALIDATION: [Specific condition] that proves the lever thesis wrong

When the invalidation condition triggers, we want the system to:
  1. ALERT the operator immediately
  2. AUTO-SIZE-DOWN any open positions attached to the prediction
  3. Mark the prediction as 'invalidated' in the decision journal so
     the postmortem attributes the loss correctly

This module is the monitoring loop — it reads active predictions from
the decision journal, evaluates each prediction's invalidation
condition against current market data, and emits InvalidationEvent
records for any that have triggered.

Invalidation condition grammar (parsed from the journal's ``metadata``
JSON field):

    {
      "type": "price_level",
      "ticker": "AAPL",
      "operator": "below" | "above",
      "threshold": 180.0,
      "triggers_on_close": bool           # intraday pokes don't count
    }

    {
      "type": "event",
      "event_name": "fomc_hawkish",
      "window_days": 7
    }

    {
      "type": "signal_flip",
      "signal_family": "liquidity",
      "from_state": "EASY",
      "to_state": "TIGHT"
    }

Unknown types are logged and skipped — the monitor never crashes on
a malformed invalidation spec.

The detector is pure of DB semantics for the condition evaluation
math — a thin wrapper pulls active predictions + price quotes + regime
state from existing stores. All functions return dataclasses so the
monitor is trivially testable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Invalidation condition types ─────────────────────────────────────────

INVAL_PRICE_LEVEL = "price_level"
INVAL_EVENT = "event"
INVAL_SIGNAL_FLIP = "signal_flip"

KNOWN_TYPES: frozenset[str] = frozenset({
    INVAL_PRICE_LEVEL,
    INVAL_EVENT,
    INVAL_SIGNAL_FLIP,
})


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class InvalidationEvent:
    """One triggered invalidation."""

    journal_id: int
    ticker: str | None
    inval_type: str
    triggered_at: datetime
    reason: str
    current_value: float | str | None
    threshold_value: float | str | None
    auto_size_down_to: float  # Kelly fraction to shrink to (0.0 = close)

    def to_dict(self) -> dict[str, Any]:
        return {
            "journal_id": self.journal_id,
            "ticker": self.ticker,
            "inval_type": self.inval_type,
            "triggered_at": self.triggered_at.isoformat(),
            "reason": self.reason,
            "current_value": self.current_value,
            "threshold_value": self.threshold_value,
            "auto_size_down_to": self.auto_size_down_to,
        }


@dataclass(frozen=True)
class MonitorRun:
    """One full monitor sweep over active predictions."""

    as_of: datetime
    predictions_scanned: int
    events: list[InvalidationEvent] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def triggered_count(self) -> int:
        return len(self.events)

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of.isoformat(),
            "predictions_scanned": self.predictions_scanned,
            "triggered_count": self.triggered_count,
            "events": [e.to_dict() for e in self.events],
            "errors": list(self.errors),
        }


# ── Pure condition evaluators ────────────────────────────────────────────


def evaluate_price_level(
    condition: dict[str, Any],
    current_price: float,
    *,
    last_close_price: float | None = None,
) -> tuple[bool, str]:
    """Return (triggered, reason) for a price_level invalidation.

    When ``triggers_on_close=True``, only evaluates against the last
    close. Otherwise intraday prices count.
    """
    operator = (condition.get("operator") or "").lower()
    threshold = condition.get("threshold")
    triggers_on_close = bool(condition.get("triggers_on_close", False))

    if threshold is None or operator not in ("below", "above"):
        return False, f"malformed price_level: {condition}"

    reference = last_close_price if triggers_on_close else current_price
    if reference is None:
        return False, "no reference price available"

    try:
        ref = float(reference)
        th = float(threshold)
    except (TypeError, ValueError):
        return False, "non-numeric price/threshold"

    if operator == "below" and ref < th:
        return True, f"{'close' if triggers_on_close else 'price'} {ref:.2f} < {th:.2f}"
    if operator == "above" and ref > th:
        return True, f"{'close' if triggers_on_close else 'price'} {ref:.2f} > {th:.2f}"
    return False, f"not triggered ({ref:.2f} vs {th:.2f})"


def evaluate_event(
    condition: dict[str, Any],
    *,
    recent_events: list[str],
    as_of: datetime,
) -> tuple[bool, str]:
    """Return (triggered, reason) for an event invalidation.

    ``recent_events`` is a list of event-name strings that fired in the
    last ``condition.window_days`` days. The condition triggers when the
    target event_name is in the list.
    """
    event_name = condition.get("event_name")
    if not event_name:
        return False, "missing event_name"
    if event_name in recent_events:
        return True, f"event '{event_name}' fired within window"
    return False, f"event '{event_name}' not observed"


def evaluate_signal_flip(
    condition: dict[str, Any],
    *,
    current_state: str | None,
    prior_state: str | None,
) -> tuple[bool, str]:
    """Return (triggered, reason) for a signal_flip invalidation.

    The state must have moved FROM ``from_state`` TO ``to_state``
    between the last evaluation and now. We need both the prior and
    current state to be sure a flip actually happened — a static
    'TIGHT' regime shouldn't trigger repeatedly.
    """
    from_state = condition.get("from_state")
    to_state = condition.get("to_state")
    if not from_state or not to_state:
        return False, "missing from_state or to_state"
    if prior_state is None or current_state is None:
        return False, "need both prior and current state"
    if str(prior_state).upper() == str(from_state).upper() and \
       str(current_state).upper() == str(to_state).upper():
        return True, f"regime flipped {prior_state} → {current_state}"
    return False, f"no matching flip (current={current_state}, prior={prior_state})"


# ── Core dispatcher ──────────────────────────────────────────────────────


def evaluate_condition(
    condition: dict[str, Any],
    *,
    current_price: float | None = None,
    last_close_price: float | None = None,
    recent_events: list[str] | None = None,
    current_state: str | None = None,
    prior_state: str | None = None,
    as_of: datetime | None = None,
) -> tuple[bool, str]:
    """Dispatch to the right evaluator based on condition type.

    Returns ``(triggered, reason)``. Unknown types return ``(False,
    "unknown condition type: <type>")``.
    """
    if as_of is None:
        as_of = datetime.now(timezone.utc)
    if recent_events is None:
        recent_events = []

    inval_type = condition.get("type")
    if inval_type not in KNOWN_TYPES:
        return False, f"unknown condition type: {inval_type}"

    if inval_type == INVAL_PRICE_LEVEL:
        if current_price is None and last_close_price is None:
            return False, "no price data"
        return evaluate_price_level(
            condition,
            current_price=current_price if current_price is not None else (last_close_price or 0.0),
            last_close_price=last_close_price,
        )

    if inval_type == INVAL_EVENT:
        return evaluate_event(
            condition,
            recent_events=recent_events,
            as_of=as_of,
        )

    if inval_type == INVAL_SIGNAL_FLIP:
        return evaluate_signal_flip(
            condition,
            current_state=current_state,
            prior_state=prior_state,
        )

    return False, "unhandled dispatch"


def determine_size_down(triggered_type: str) -> float:
    """How much to shrink the Kelly fraction when an invalidation fires.

    price_level and signal_flip are hard invalidations → close the position
    (size 0). event is softer → shrink to 20% of original Kelly.
    """
    if triggered_type == INVAL_EVENT:
        return 0.20
    return 0.0


# ── DB I/O ───────────────────────────────────────────────────────────────


def _read_active_predictions(engine: Engine) -> list[dict[str, Any]]:
    """Read active predictions from decision_journal with invalidation metadata."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, ticker, metadata, logged_at
                    FROM decision_journal
                    WHERE outcome IS NULL
                      AND metadata IS NOT NULL
                      AND metadata ? 'invalidation'
                      AND logged_at >= NOW() - INTERVAL '90 days'
                    ORDER BY logged_at DESC
                    """
                ),
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("active predictions read failed: {e}", e=str(exc))
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r[0],
            "ticker": r[1],
            "metadata": r[2] if isinstance(r[2], dict) else {},
            "logged_at": r[3],
        })
    return out


def _read_last_price(engine: Engine, ticker: str) -> tuple[float | None, float | None]:
    """Return (current_price, last_close_price) for a ticker."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT close_price
                    FROM ticker_metrics_daily
                    WHERE ticker = :t AND close_price IS NOT NULL
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """
                ),
                {"t": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("price read failed for {t}: {e}", t=ticker, e=str(exc))
        return None, None
    if row is None:
        return None, None
    close = float(row[0])
    return close, close


def run_monitor(
    engine: Engine,
    *,
    as_of: datetime | None = None,
    recent_events: list[str] | None = None,
    current_regime_state: str | None = None,
    prior_regime_state: str | None = None,
) -> MonitorRun:
    """Sweep every active prediction, evaluate its invalidation condition,
    and return a :class:`MonitorRun` with the list of triggered events.

    Pass ``recent_events`` / ``current_regime_state`` / ``prior_regime_state``
    to enable the event + signal_flip paths. Price_level paths read from
    ticker_metrics_daily directly.
    """
    if as_of is None:
        as_of = datetime.now(timezone.utc)

    predictions = _read_active_predictions(engine)
    events: list[InvalidationEvent] = []
    errors: list[str] = []

    for pred in predictions:
        metadata = pred.get("metadata") or {}
        invalidation = metadata.get("invalidation") or {}
        if not isinstance(invalidation, dict) or "type" not in invalidation:
            errors.append(f"pred {pred['id']}: malformed invalidation")
            continue

        ticker = pred.get("ticker")
        inval_type = invalidation.get("type")

        # Price data for price_level path
        current_price: float | None = None
        last_close_price: float | None = None
        if inval_type == INVAL_PRICE_LEVEL and ticker:
            current_price, last_close_price = _read_last_price(engine, ticker)

        triggered, reason = evaluate_condition(
            invalidation,
            current_price=current_price,
            last_close_price=last_close_price,
            recent_events=recent_events or [],
            current_state=current_regime_state,
            prior_state=prior_regime_state,
            as_of=as_of,
        )

        if triggered:
            current_val: float | str | None = None
            threshold_val: float | str | None = None
            if inval_type == INVAL_PRICE_LEVEL:
                current_val = current_price
                threshold_val = invalidation.get("threshold")
            elif inval_type == INVAL_EVENT:
                current_val = invalidation.get("event_name")
                threshold_val = invalidation.get("event_name")
            elif inval_type == INVAL_SIGNAL_FLIP:
                current_val = current_regime_state
                threshold_val = invalidation.get("to_state")

            events.append(InvalidationEvent(
                journal_id=int(pred["id"]),
                ticker=ticker,
                inval_type=inval_type,
                triggered_at=as_of,
                reason=reason,
                current_value=current_val,
                threshold_value=threshold_val,
                auto_size_down_to=determine_size_down(inval_type),
            ))

    if events:
        log.warning(
            "thesis_invalidation_monitor: {n} invalidation(s) triggered",
            n=len(events),
        )
    else:
        log.debug(
            "thesis_invalidation_monitor: scanned {n} predictions, none triggered",
            n=len(predictions),
        )

    return MonitorRun(
        as_of=as_of,
        predictions_scanned=len(predictions),
        events=events,
        errors=errors,
    )
