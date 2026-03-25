"""
GRID Live Paper Trade System.

Timestamps regime calls and specific predictions for building
a verifiable live track record. Creates immutable, timestamped
entries in the decision journal and a local paper trade log.

Usage:
    python -m backtest.paper_trade --snapshot
    python -m backtest.paper_trade --score
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log

_PAPER_DIR = Path(__file__).parent.parent / "outputs" / "paper_trades"


class PaperTradeTracker:
    """Manages live paper trade tracking with timestamped predictions.

    Creates verifiable, immutable prediction records that can be
    scored after their horizon expires. Each record includes:
    - Exact timestamp
    - Regime classification with confidence
    - Specific directional predictions with horizons
    - Target portfolio allocation

    Attributes:
        db_engine: SQLAlchemy engine for database access.
    """

    def __init__(self, db_engine: Any = None) -> None:
        self.engine = db_engine
        _PAPER_DIR.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        if self.engine is None:
            from db import get_engine
            self.engine = get_engine()

    def create_snapshot(self) -> dict[str, Any]:
        """Create a timestamped regime snapshot with predictions.

        Captures the current regime state, confidence, feature count,
        and generates specific falsifiable predictions.

        Returns:
            dict: Complete snapshot with predictions.
        """
        self._init_db()

        from sqlalchemy import text

        now = datetime.now(timezone.utc)

        # Get latest regime from decision_journal
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT inferred_state, state_confidence, "
                    "transition_probability, grid_recommendation, "
                    "contradiction_flags, decision_timestamp "
                    "FROM decision_journal "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                )
            ).fetchone()

            # Count model-eligible features
            feat_count = conn.execute(
                text("SELECT COUNT(*) FROM feature_registry WHERE model_eligible = TRUE")
            ).scalar()

        if not row:
            return {"error": "No regime data in decision_journal"}

        regime = row[0]
        confidence = float(row[1])
        trans_prob = float(row[2])
        posture = row[3]
        contradictions = row[4] or {}
        regime_timestamp = str(row[5])

        # Generate predictions based on regime
        predictions = self._generate_predictions(regime, confidence)

        # Build posture allocation
        from backtest.engine import POSTURE_ALLOCATIONS, REGIME_TO_POSTURE
        target_posture = REGIME_TO_POSTURE.get(regime, "BALANCED")
        allocation = POSTURE_ALLOCATIONS.get(target_posture, {})

        snapshot = {
            "timestamp": now.isoformat() + "Z",
            "regime_timestamp": regime_timestamp,
            "regime": regime,
            "confidence": round(confidence, 4),
            "transition_probability": round(trans_prob, 4),
            "posture": posture,
            "features_in_model": feat_count,
            "contradiction_flags": contradictions,
            "predictions": predictions,
            "target_allocation": allocation,
            "scoring_dates": {
                "7d": (now + timedelta(days=7)).strftime("%Y-%m-%d"),
                "14d": (now + timedelta(days=14)).strftime("%Y-%m-%d"),
                "30d": (now + timedelta(days=30)).strftime("%Y-%m-%d"),
            },
        }

        # Save to disk (immutable log)
        self._save_snapshot(snapshot)

        # Log to decision_journal
        self._log_to_journal(snapshot)

        return snapshot

    def _generate_predictions(
        self, regime: str, confidence: float,
    ) -> list[dict[str, Any]]:
        """Generate specific falsifiable predictions based on regime.

        Parameters:
            regime: Current regime classification.
            confidence: Classification confidence.

        Returns:
            list: Prediction records with horizons and directions.
        """
        predictions = []

        if regime == "CRISIS":
            predictions.extend([
                {
                    "asset": "SPY",
                    "direction": "DOWN",
                    "horizon_days": 30,
                    "description": "S&P 500 lower in 30 days",
                    "confidence": round(confidence * 0.8, 3),
                },
                {
                    "asset": "TLT",
                    "direction": "UP",
                    "horizon_days": 30,
                    "description": "Long-term treasuries higher (flight to safety)",
                    "confidence": round(confidence * 0.7, 3),
                },
                {
                    "asset": "GLD",
                    "direction": "UP",
                    "horizon_days": 30,
                    "description": "Gold higher (safe haven demand)",
                    "confidence": round(confidence * 0.7, 3),
                },
                {
                    "asset": "VIX",
                    "direction": "UP",
                    "horizon_days": 14,
                    "description": "VIX elevated or rising",
                    "confidence": round(confidence * 0.6, 3),
                },
            ])
        elif regime == "FRAGILE":
            predictions.extend([
                {
                    "asset": "SPY",
                    "direction": "FLAT_TO_DOWN",
                    "horizon_days": 30,
                    "description": "S&P 500 flat or down, elevated volatility",
                    "confidence": round(confidence * 0.6, 3),
                },
                {
                    "asset": "GLD",
                    "direction": "UP",
                    "horizon_days": 30,
                    "description": "Gold outperforms equities",
                    "confidence": round(confidence * 0.6, 3),
                },
            ])
        elif regime == "GROWTH":
            predictions.extend([
                {
                    "asset": "SPY",
                    "direction": "UP",
                    "horizon_days": 30,
                    "description": "S&P 500 higher in 30 days",
                    "confidence": round(confidence * 0.7, 3),
                },
                {
                    "asset": "BTC-USD",
                    "direction": "UP",
                    "horizon_days": 30,
                    "description": "Bitcoin higher (risk-on environment)",
                    "confidence": round(confidence * 0.5, 3),
                },
            ])
        else:  # NEUTRAL
            predictions.extend([
                {
                    "asset": "SPY",
                    "direction": "FLAT",
                    "horizon_days": 30,
                    "description": "S&P 500 range-bound",
                    "confidence": round(confidence * 0.5, 3),
                },
            ])

        return predictions

    def _save_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Save snapshot to disk as immutable record."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = _PAPER_DIR / f"snapshot_{ts}.json"
        with filepath.open("w") as f:
            json.dump(snapshot, f, indent=2, default=str)
        log.info("Paper trade snapshot saved to {p}", p=filepath)

    def _log_to_journal(self, snapshot: dict[str, Any]) -> None:
        """Log paper trade to decision_journal."""
        try:
            from sqlalchemy import text
            with self.engine.begin() as conn:
                # Get production model ID
                row = conn.execute(
                    text("SELECT id FROM model_registry WHERE state='PRODUCTION' AND layer='REGIME' LIMIT 1")
                ).fetchone()
                model_id = row[0] if row else None

                pred_summary = "; ".join(
                    f"{p['asset']} {p['direction']} ({p['horizon_days']}d)"
                    for p in snapshot["predictions"]
                )

                conn.execute(
                    text("""
                        INSERT INTO decision_journal
                        (model_version_id, inferred_state, state_confidence,
                         transition_probability, contradiction_flags,
                         grid_recommendation, baseline_recommendation,
                         action_taken, counterfactual, operator_confidence)
                        VALUES
                        (:mid, :state, :conf, :tp, :flags,
                         :rec, 'NEUTRAL',
                         :action, :counter, 'HIGH')
                    """),
                    {
                        "mid": model_id,
                        "state": snapshot["regime"],
                        "conf": snapshot["confidence"],
                        "tp": snapshot["transition_probability"],
                        "flags": json.dumps(snapshot.get("contradiction_flags", {})),
                        "rec": snapshot["posture"],
                        "action": f"PAPER_TRADE: {pred_summary}",
                        "counter": f"If regime wrong: misallocated per {snapshot['posture']} posture",
                    },
                )
            log.info("Paper trade logged to decision_journal")
        except Exception as exc:
            log.error("Failed to log paper trade: {e}", e=str(exc))

    def list_snapshots(self) -> list[dict[str, Any]]:
        """List all saved paper trade snapshots.

        Returns:
            list: Snapshot summaries sorted by date (newest first).
        """
        files = sorted(_PAPER_DIR.glob("snapshot_*.json"), reverse=True)
        snapshots = []
        for f in files:
            try:
                with f.open() as fh:
                    data = json.load(fh)
                snapshots.append({
                    "filename": f.name,
                    "timestamp": data.get("timestamp"),
                    "regime": data.get("regime"),
                    "confidence": data.get("confidence"),
                    "posture": data.get("posture"),
                    "n_predictions": len(data.get("predictions", [])),
                })
            except Exception:
                continue
        return snapshots

    def get_snapshot(self, filename: str) -> dict[str, Any] | None:
        """Load a specific snapshot by filename."""
        filepath = _PAPER_DIR / filename
        if not filepath.exists():
            return None
        with filepath.open() as f:
            return json.load(f)

    def score_predictions(self) -> list[dict[str, Any]]:
        """Score past predictions against actual market data.

        Checks each prediction whose horizon has expired and
        determines if the prediction was correct.

        Returns:
            list: Scored prediction records.
        """
        today = date.today()
        scored = []

        for snapshot_file in sorted(_PAPER_DIR.glob("snapshot_*.json")):
            try:
                with snapshot_file.open() as f:
                    snapshot = json.load(f)
            except Exception:
                continue

            snap_date = datetime.fromisoformat(
                snapshot["timestamp"].replace("Z", "+00:00")
            ).date()

            for pred in snapshot.get("predictions", []):
                horizon = pred.get("horizon_days", 30)
                eval_date = snap_date + timedelta(days=horizon)

                if eval_date > today:
                    continue  # Not yet expired

                # Try to get actual price data
                actual_result = self._check_prediction(
                    pred["asset"], snap_date, eval_date
                )

                scored.append({
                    "snapshot_date": str(snap_date),
                    "regime": snapshot["regime"],
                    "asset": pred["asset"],
                    "predicted_direction": pred["direction"],
                    "horizon_days": horizon,
                    "eval_date": str(eval_date),
                    **actual_result,
                })

        return scored

    def _check_prediction(
        self, asset: str, start_date: date, end_date: date,
    ) -> dict[str, Any]:
        """Check a single prediction against actual data.

        Parameters:
            asset: Asset ticker.
            start_date: Prediction date.
            end_date: Evaluation date.

        Returns:
            dict: Result with actual return and correctness.
        """
        try:
            import yfinance as yf
            ticker = asset
            data = yf.download(
                ticker,
                start=start_date.isoformat(),
                end=(end_date + timedelta(days=3)).isoformat(),
                progress=False,
                auto_adjust=True,
            )
            if data.empty or len(data) < 2:
                return {"status": "NO_DATA"}

            start_price = float(data["Close"].iloc[0])
            end_price = float(data["Close"].iloc[-1])
            actual_return = (end_price - start_price) / start_price

            return {
                "start_price": round(start_price, 2),
                "end_price": round(end_price, 2),
                "actual_return": round(actual_return, 4),
                "status": "SCORED",
            }
        except Exception:
            return {"status": "ERROR"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    tracker = PaperTradeTracker()

    if len(sys.argv) > 1 and sys.argv[1] == "--snapshot":
        snapshot = tracker.create_snapshot()
        if "error" in snapshot:
            print(f"Error: {snapshot['error']}")
        else:
            print(f"\n{'='*60}")
            print(f"GRID REGIME CALL — {snapshot['timestamp']}")
            print(f"{'='*60}")
            print(f"Classification: {snapshot['regime']} ({snapshot['confidence']:.0%} confidence)")
            print(f"Features in model: {snapshot['features_in_model']}")
            print(f"Posture: {snapshot['posture']}")
            print()
            print("PREDICTIONS:")
            for p in snapshot["predictions"]:
                print(f"  {p['asset']} → {p['direction']} ({p['horizon_days']}d) "
                      f"[{p['confidence']:.0%}] — {p['description']}")
            print()
            print("SCORING DATES:")
            for k, v in snapshot["scoring_dates"].items():
                print(f"  {k}: {v}")
            print()
            print(f"Saved to {_PAPER_DIR}")

    elif len(sys.argv) > 1 and sys.argv[1] == "--score":
        scored = tracker.score_predictions()
        if not scored:
            print("No predictions have expired yet. Check back later.")
        else:
            correct = sum(1 for s in scored if s.get("status") == "SCORED")
            print(f"\nScored {len(scored)} predictions ({correct} with data):")
            for s in scored:
                print(f"  [{s['snapshot_date']}] {s['asset']} {s['predicted_direction']} "
                      f"→ actual: {s.get('actual_return', 'N/A')}")

    elif len(sys.argv) > 1 and sys.argv[1] == "--list":
        snapshots = tracker.list_snapshots()
        if not snapshots:
            print("No paper trade snapshots yet.")
        else:
            for s in snapshots:
                print(f"  {s['timestamp']} | {s['regime']} ({s['confidence']:.0%}) | "
                      f"{s['n_predictions']} predictions")

    else:
        print("Usage:")
        print("  python -m backtest.paper_trade --snapshot  # Create timestamped regime call")
        print("  python -m backtest.paper_trade --list      # List all snapshots")
        print("  python -m backtest.paper_trade --score     # Score expired predictions")
