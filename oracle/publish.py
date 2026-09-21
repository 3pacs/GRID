"""Explicit publish contract for comparable AstroGrid oracle records."""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.dedup_index import ensure_dedup_index
from oracle.entry_price_policy import NULL_WRITE_POLICY
from oracle.prediction_context import (
    build_prediction_context,
    enrich_signals_payload,
)


def _compact_text(value: Any, fallback: str = "") -> str:
    return " ".join(str(value or fallback).split())[:240]


def _measured_or_none(value: Any) -> float | None:
    """A caller-supplied metric, or None. Never a midpoint stand-in.

    ``None`` stays ``None`` and a genuine ``0.0`` stays ``0.0`` (the retired
    ``or 0.5`` form silently rewrote both).
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


_ENTRY_PRICE_SOURCE = "options_daily_signals.spot_price"


def _as_of_date(payload: dict[str, Any]) -> date:
    """The prediction's as-of date. Never raises; falls back to today (UTC)."""
    as_of = payload.get("as_of_ts")
    try:
        if isinstance(as_of, str):
            return datetime.fromisoformat(as_of.replace("Z", "+00:00")).date()
        if isinstance(as_of, datetime):
            return as_of.date()
    except (TypeError, ValueError):
        pass
    return datetime.now(timezone.utc).date()


def _measured_entry_price(
    engine: Engine, ticker: str, as_of: date
) -> tuple[float | None, dict[str, Any]]:
    """The last raw spot observed on or before ``as_of``, with its date.

    An entry price is a measurement, so it ships with the day it was measured
    on or it does not ship at all. The retired literal here was ``0.0``: it
    rendered as "$0.00" on the prediction card and turned every downstream
    ``(exit - entry) / entry`` into nonsense, while being indistinguishable
    from a price somebody had actually looked up.

    Source and basis: ``options_daily_signals.spot_price`` is the same raw
    (unadjusted) series ``oracle/engine.py:_get_spot_price`` writes as
    ``entry_price`` and that ``scripts/score_oracle_trades.py`` scores against
    (``fetch_prices(..., auto_adjust=False)``, PR #516). Mixing an adjusted
    entry against a raw exit manufactures a dividend/split-sized return out of
    nothing — see ``tests/test_oracle_engine_spot_price_basis.py``.

    Point-in-time: ``signal_date <= as_of`` never reads a close from after the
    prediction was made. No observation on or before that date means ``None``,
    not a stand-in.

    Returns ``(price, basis)``. ``basis`` always carries a ``status`` of
    ``"measured"`` or ``"unavailable"`` and is stored on the row beside the
    price so a reader can tell a looked-up entry from a missing one.
    """
    as_of_iso = as_of.isoformat()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT spot_price, signal_date
                    FROM options_daily_signals
                    WHERE ticker = :t
                      AND signal_date <= :d
                      AND spot_price > 0
                    ORDER BY signal_date DESC
                    LIMIT 1
                    """
                ),
                {"t": ticker, "d": as_of},
            ).fetchone()
    except Exception as exc:  # pragma: no cover - defensive; never block a publish
        return None, {
            "status": "unavailable",
            "source": None,
            "as_of": as_of_iso,
            "reason": f"entry price lookup failed: {exc}",
        }

    price = _measured_or_none(row[0]) if row else None
    if price is None or price <= 0:
        return None, {
            "status": "unavailable",
            "source": None,
            "as_of": as_of_iso,
            "reason": (
                f"no {_ENTRY_PRICE_SOURCE} observation for {ticker} "
                f"on or before {as_of_iso}"
            ),
        }

    observed_on = row[1]
    return price, {
        "status": "measured",
        "source": _ENTRY_PRICE_SOURCE,
        "observed_on": (
            observed_on.isoformat()
            if hasattr(observed_on, "isoformat")
            else str(observed_on)
        ),
        "as_of": as_of_iso,
        "ticker": ticker,
    }


def _prediction_direction(payload: dict[str, Any]) -> str:
    raw = " ".join(
        [
            str(payload.get("call") or ""),
            str(payload.get("setup") or ""),
            str(payload.get("note") or ""),
        ]
    ).lower()
    if any(token in raw for token in ("sell", "short", "hedge", "fade", "risk off", "bear")):
        return "BEARISH"
    if any(token in raw for token in ("buy", "long", "press", "accumulate", "risk on", "bull")):
        return "BULLISH"
    return "NEUTRAL"


def _prediction_expiry(payload: dict[str, Any]) -> date:
    horizon = str(payload.get("horizon_label") or "swing")
    as_of = payload.get("as_of_ts")
    if isinstance(as_of, str):
        as_of_dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    elif isinstance(as_of, datetime):
        as_of_dt = as_of
    else:
        as_of_dt = datetime.now(timezone.utc)
    if horizon == "macro":
        return (as_of_dt + timedelta(days=30)).date()
    return (as_of_dt + timedelta(days=7)).date()


def publish_astrogrid_prediction(engine: Engine, payload: dict[str, Any]) -> dict[str, Any]:
    """Publish a reduced comparable record into the shared Oracle path."""
    oracle_prediction_id = str(payload.get("oracle_prediction_id") or f"astrogrid:{payload['prediction_id']}")
    flow_context = {
        "source": "astrogrid",
        "target_universe": payload.get("target_universe") or "hybrid",
        "target_symbols": list(payload.get("target_symbols") or []),
        "horizon": payload.get("horizon_label") or "swing",
        "question": payload.get("question"),
        "call": payload.get("call"),
        "timing": payload.get("timing"),
        "invalidation": payload.get("invalidation"),
    }
    signals_list = [
        {"name": "astrogrid_grid", "detail": _compact_text(payload.get("grid_summary"))},
        {"name": "astrogrid_mystical", "detail": _compact_text(payload.get("mystical_summary"))},
    ]
    # Enrich with 11-layer conviction context: regime / fci_regime / vix_level /
    # signal_contributions. Never raises — defaults on any upstream failure.
    as_of_date = _as_of_date(payload)
    try:
        # No supplied confidence means no astrogrid weight to record, not a
        # 0.5 one. `_normalize_contributions` drops a None value, so the
        # context simply carries no astrogrid weight.
        astrogrid_weight = _measured_or_none(payload.get("confidence"))
        context = build_prediction_context(
            engine,
            as_of=as_of_date,
            model_weights=(
                {"astrogrid": astrogrid_weight}
                if astrogrid_weight is not None
                else None
            ),
        )
    except Exception:
        context = {
            "regime": "NEUTRAL",
            "fci_regime": "NEUTRAL",
            "vix_level": None,
            "signal_contributions": {},
        }
    signals = enrich_signals_payload(signals_list, context)
    ticker = (payload.get("target_symbols") or ["HYBRID"])[0]
    # D-M32: the entry price was the literal 0.0 on every published row. It is
    # now the measured spot at `as_of_date`, or NULL — and the basis travels
    # with it so "not looked up" is legible on the row itself.
    entry_price, entry_price_basis = _measured_entry_price(
        engine, ticker, as_of_date
    )
    signals["entry_price_basis"] = entry_price_basis
    # RECOVERY (packet 2a item b): stop publishing a comparable prediction
    # record when nothing measured an entry price, rather than fabricating
    # one. This branch exists only on the recovery target -- the candidate
    # (#593) publishes the row with entry_price NULL, closed later by the
    # scorers' no_data path; this recovery instead never creates that row,
    # so the scorers' no_data close-out never has to run for astrogrid
    # publishes at all. Never restores the retired `entry_price = 0.0` /
    # `confidence = payload.get(...) or 0.5` literals (docs/reference/
    # CONFIDENCE_POLICY.md, D-M32) -- an unscorable prediction is skipped,
    # not stamped with a value nobody measured. No row, no fabrication.
    if entry_price is None:
        return {
            "status": "skipped",
            "reason": "no_measured_entry_price",
            "oracle_prediction_id": oracle_prediction_id,
            "contract": "oracle.publish.v1",
            "entry_price_basis": entry_price_basis,
        }
    # Pre-migration safety: the ON CONFLICT below targets the partial unique
    # index oracle_predictions_dedup_unique. Ensure it exists (once/process)
    # so this insert can't raise 42P10 on a not-yet-migrated DB.
    ensure_dedup_index(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO oracle_predictions (
                    id,
                    created_at,
                    ticker,
                    prediction_type,
                    direction,
                    target_price,
                    entry_price,
                    expiry,
                    confidence,
                    expected_move_pct,
                    signal_strength,
                    coherence,
                    model_name,
                    model_version,
                    signals,
                    anti_signals,
                    flow_context,
                    model_weights,
                    null_write_policy
                )
                VALUES (
                    :id,
                    NOW(),
                    :ticker,
                    :prediction_type,
                    :direction,
                    NULL,
                    :entry_price,
                    :expiry,
                    :confidence,
                    NULL,
                    :signal_strength,
                    :coherence,
                    :model_name,
                    :model_version,
                    CAST(:signals AS jsonb),
                    CAST(:anti_signals AS jsonb),
                    CAST(:flow_context AS jsonb),
                    CAST(:model_weights AS jsonb),
                    :null_write_policy
                )
                ON CONFLICT (
                    ticker, direction, expiry, prediction_type,
                    (COALESCE(model_version, '')),
                    ((created_at AT TIME ZONE 'UTC')::date)
                ) WHERE dedup_keep = TRUE
                DO UPDATE SET
                    confidence = CASE
                        WHEN EXCLUDED.confidence IS NULL
                          OR oracle_predictions.confidence IS NULL
                        THEN NULL
                        ELSE GREATEST(
                            EXCLUDED.confidence, oracle_predictions.confidence
                        )
                    END,
                    signals    = EXCLUDED.signals,
                    signal_strength = EXCLUDED.signal_strength,
                    coherence  = EXCLUDED.coherence,
                    model_weights = EXCLUDED.model_weights
                """
            ),
            {
                "id": oracle_prediction_id,
                "ticker": ticker,
                "prediction_type": "astrogrid",
                "direction": _prediction_direction(payload),
                # NULL, not 0.0, when no spot was observed on or before
                # as_of_date. `signals.entry_price_basis` says which it was.
                "entry_price": entry_price,
                "expiry": _prediction_expiry(payload),
                # Three nominally independent metrics used to be the same
                # number, and that number was 0.5 whenever the caller omitted
                # a confidence (D-H11). `or 0.5` also rewrote a legitimate
                # 0.0 to 0.5. Each is now its own input, or NULL: a caller
                # that did not supply it does not get one invented.
                "confidence": _measured_or_none(payload.get("confidence")),
                "signal_strength": _measured_or_none(payload.get("signal_strength")),
                "coherence": _measured_or_none(payload.get("coherence")),
                "model_name": "astrogrid",
                "model_version": str(payload.get("model_version") or "astrogrid-oracle-v1"),
                "signals": json.dumps(signals),
                "anti_signals": json.dumps([
                    {"name": "astrogrid_invalidation", "detail": _compact_text(payload.get("invalidation"))},
                ]),
                "flow_context": json.dumps(flow_context),
                "model_weights": json.dumps({
                    "weight_version": payload.get("weight_version") or "astrogrid-v1",
                    "publish_contract": "oracle.publish.v1",
                }),
                # Historical-NULL provenance boundary: stamped on every
                # INSERT, never on the ON CONFLICT UPDATE below, so a reader
                # can prove this row's entry_price/confidence NULL (if
                # either is NULL) is this policy's honest NULL. See
                # oracle/entry_price_policy.py and the migration docstring.
                "null_write_policy": NULL_WRITE_POLICY,
            },
        )
    return {
        "status": "published",
        "oracle_prediction_id": oracle_prediction_id,
        "contract": "oracle.publish.v1",
        "entry_price": entry_price,
        "entry_price_basis": entry_price_basis,
    }
