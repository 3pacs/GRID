"""
PSI Oracle — Planetary Stress Index market timing oracle.

Uses the proven PSI+VIX→GLD signal (Sharpe 2.59) to generate
directional predictions through AstroGrid's prediction infrastructure.

Proven configurations:
  GLD: PSI<5.25 + VIX<22 → Sharpe 2.587, 101% ann return, 1024 trading days
  QQQ: PSI>2.00 → Sharpe 2.012, 52% ann return, 413 days
  GLD: PSI>1.00 + VIX<17 → Sharpe 2.319, tightest risk

The oracle reads current PSI and VIX, determines if conditions favor
going long on GLD/QQQ, and emits a prediction payload compatible with
AstroGrid's save_prediction() interface.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class PSISignal:
    """Immutable signal output from the PSI oracle."""

    symbol: str
    direction: str  # "bullish", "bearish", "neutral"
    confidence: float  # 0.0 to 1.0
    psi_value: float
    vix_value: float | None
    config_name: str
    config_sharpe: float
    horizon_label: str  # "swing" (5-day)
    reasoning: str


# Proven configurations ranked by Sharpe
_PSI_CONFIGS: list[dict[str, Any]] = [
    {
        "name": "gld_psi_lt525_vix_lt22",
        "symbol": "GLD",
        "psi_op": "lt",
        "psi_threshold": 5.25,
        "vix_threshold": 22.0,
        "direction": "bullish",
        "sharpe": 2.587,
        "ann_return": 1.011,
        "max_dd": -0.419,
        "trading_days": 1024,
    },
    {
        "name": "gld_psi_lt525_vix_lt20",
        "symbol": "GLD",
        "psi_op": "lt",
        "psi_threshold": 5.25,
        "vix_threshold": 20.0,
        "direction": "bullish",
        "sharpe": 2.519,
        "ann_return": 0.879,
        "max_dd": -0.531,
        "trading_days": 861,
    },
    {
        "name": "gld_psi_gt100_vix_lt17",
        "symbol": "GLD",
        "psi_op": "gt",
        "psi_threshold": 1.00,
        "vix_threshold": 17.0,
        "direction": "bullish",
        "sharpe": 2.319,
        "ann_return": 0.497,
        "max_dd": -0.266,
        "trading_days": 306,
    },
    {
        "name": "gld_psi_lt525_no_vix",
        "symbol": "GLD",
        "psi_op": "lt",
        "psi_threshold": 5.25,
        "vix_threshold": None,
        "direction": "bullish",
        "sharpe": 2.235,
        "ann_return": 1.150,
        "max_dd": -0.686,
        "trading_days": 1421,
    },
    {
        "name": "qqq_psi_gt200_no_vix",
        "symbol": "QQQ",
        "psi_op": "gt",
        "psi_threshold": 2.00,
        "vix_threshold": None,
        "direction": "bullish",
        "sharpe": 2.012,
        "ann_return": 0.522,
        "max_dd": -0.439,
        "trading_days": 413,
    },
]


def _load_latest_value(engine: Engine, feature_name: str) -> float | None:
    """Load the most recent value for a feature from resolved_series."""
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT rs.value, rs.obs_date
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :name
                ORDER BY rs.obs_date DESC
                LIMIT 1
            """),
            {"name": feature_name},
        ).fetchone()
    if row:
        return float(row[0])
    return None


def _check_psi_condition(psi_value: float, op: str, threshold: float) -> bool:
    """Check if PSI meets the configured condition."""
    if op == "lt":
        return psi_value < threshold
    if op == "gt":
        return psi_value > threshold
    return False


def evaluate_psi_signals(engine: Engine) -> list[PSISignal]:
    """
    Evaluate all PSI configurations against current data.

    Returns a list of PSISignal objects for each triggered configuration.
    """
    psi_value = _load_latest_value(engine, "planetary_stress_index")
    vix_value = _load_latest_value(engine, "vix_spot")

    if psi_value is None:
        log.warning("PSI oracle: no planetary_stress_index data available")
        return []

    log.info(
        "PSI oracle: PSI={psi:.2f}, VIX={vix}",
        psi=psi_value,
        vix=f"{vix_value:.1f}" if vix_value is not None else "N/A",
    )

    signals = []
    for config in _PSI_CONFIGS:
        # Check PSI condition
        if not _check_psi_condition(psi_value, config["psi_op"], config["psi_threshold"]):
            continue

        # Check VIX condition (if configured)
        vix_thresh = config["vix_threshold"]
        if vix_thresh is not None:
            if vix_value is None:
                continue
            if vix_value >= vix_thresh:
                continue

        # Signal triggered — compute confidence from historical Sharpe
        # Scale: Sharpe 1.5 → 0.5 confidence, Sharpe 3.0 → 0.9
        base_confidence = min(0.95, max(0.3, (config["sharpe"] - 1.0) / 3.0 + 0.3))

        op_str = "<" if config["psi_op"] == "lt" else ">"
        vix_str = f", VIX={vix_value:.1f}<{vix_thresh}" if vix_thresh else ""
        reasoning = (
            f"PSI={psi_value:.2f}{op_str}{config['psi_threshold']}{vix_str}. "
            f"Historical: Sharpe {config['sharpe']:.2f}, "
            f"ann return {config['ann_return']:.0%}, "
            f"max DD {config['max_dd']:.0%} over {config['trading_days']} days. "
            f"Config: {config['name']}"
        )

        signals.append(
            PSISignal(
                symbol=config["symbol"],
                direction=config["direction"],
                confidence=round(base_confidence, 3),
                psi_value=psi_value,
                vix_value=vix_value,
                config_name=config["name"],
                config_sharpe=config["sharpe"],
                horizon_label="swing",
                reasoning=reasoning,
            )
        )

    log.info("PSI oracle: {n} signals triggered", n=len(signals))
    return signals


def build_astrogrid_prediction_payload(signal: PSISignal) -> dict[str, Any]:
    """
    Convert a PSISignal into an AstroGrid prediction payload
    compatible with AstroGridStore.save_prediction().
    """
    now = datetime.now(timezone.utc)

    return {
        "prediction_id": str(uuid4()),
        "as_of_ts": now.isoformat(),
        "horizon_label": signal.horizon_label,
        "target_universe": "hybrid",
        "scoring_class": "liquid_market",
        "target_symbols": [signal.symbol],
        "question": f"Should I go long {signal.symbol} based on planetary stress conditions?",
        "call": f"{signal.direction.upper()} {signal.symbol} — PSI oracle signal",
        "timing": "5-day holding period, review on next PSI reading",
        "setup": signal.reasoning,
        "invalidation": f"VIX spike above 30 or PSI regime change invalidates this call",
        "note": f"Automated PSI oracle signal. Config: {signal.config_name}",
        "seer_summary": (
            f"The Planetary Stress Index reads {signal.psi_value:.2f}. "
            f"This configuration ({signal.config_name}) has historically produced "
            f"Sharpe {signal.config_sharpe:.2f}. "
            f"The oracle sees {signal.direction} conditions for {signal.symbol}."
        ),
        "mystical_feature_payload": {
            "planetary_stress_index": signal.psi_value,
            "oracle_config": signal.config_name,
            "oracle_sharpe": signal.config_sharpe,
            "source": "psi_oracle",
        },
        "grid_feature_payload": {
            "vix": signal.vix_value,
        },
        "weight_version": "psi-oracle-v1",
        "model_version": "psi-oracle-v1",
        "live_or_local": "local",
        "status": "pending",
        "mode": "psi_oracle",
        "lens_ids": ["planetary_stress", "vix_regime"],
    }


def run_psi_oracle(engine: Engine) -> list[dict[str, Any]]:
    """
    Full oracle run: evaluate signals and return prediction payloads.

    These can be fed directly into AstroGridStore.save_prediction().
    """
    signals = evaluate_psi_signals(engine)
    payloads = [build_astrogrid_prediction_payload(s) for s in signals]

    for payload in payloads:
        log.info(
            "PSI oracle prediction: {sym} {dir} (confidence={conf}, config={cfg})",
            sym=payload["target_symbols"][0],
            dir=payload["call"],
            conf=payload["mystical_feature_payload"]["oracle_sharpe"],
            cfg=payload["mystical_feature_payload"]["oracle_config"],
        )

    return payloads
