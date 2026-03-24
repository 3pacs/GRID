"""
GRID analytical snapshot persistence.

Saves every analytical output (clustering, orthogonality, feature importance,
regime detection, options scans) to a database table with full provenance.
This allows comparing results across time — e.g. "how did the correlation
structure differ 6 months ago?" or "when did the regime model start
disagreeing with itself?"

Schema: ``analytical_snapshots`` table (see migration in scripts/migrate_snapshots.py).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class _NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types and pandas objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            val = float(obj)
            if np.isnan(val) or np.isinf(val):
                return None
            return val
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict("records")
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        return super().default(obj)


def _safe_json(data: Any) -> str:
    """Serialize data to JSON, handling numpy/pandas types."""
    return json.dumps(data, cls=_NumpyEncoder, default=str)


class AnalyticalSnapshotStore:
    """Persist and query analytical outputs for historical comparison.

    Every run of clustering, orthogonality, regime detection, feature
    engineering, or options scanning gets a row in ``analytical_snapshots``
    with the full result payload as JSONB.

    Attributes:
        engine: SQLAlchemy engine for database access.
    """

    # Recognised snapshot categories
    CATEGORIES = (
        "clustering",
        "orthogonality",
        "regime_detection",
        "feature_engineering",
        "feature_importance",
        "options_scan",
        "conflict_resolution",
        "pipeline_summary",
    )

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the analytical_snapshots table if it doesn't exist."""
        ddl = text("""
            CREATE TABLE IF NOT EXISTS analytical_snapshots (
                id            BIGSERIAL PRIMARY KEY,
                snapshot_date DATE NOT NULL,
                category      TEXT NOT NULL,
                subcategory   TEXT,
                as_of_date    DATE NOT NULL,
                payload       JSONB NOT NULL,
                metrics       JSONB,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        idx_date = text("""
            CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_date
                ON analytical_snapshots (snapshot_date DESC)
        """)
        idx_cat = text("""
            CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_category
                ON analytical_snapshots (category, snapshot_date DESC)
        """)
        try:
            with self.engine.begin() as conn:
                conn.execute(ddl)
                conn.execute(idx_date)
                conn.execute(idx_cat)
        except Exception as exc:
            log.warning("Could not ensure analytical_snapshots table: {e}", e=str(exc))

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_snapshot(
        self,
        category: str,
        payload: dict[str, Any],
        as_of_date: date | None = None,
        subcategory: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        """Persist a single analytical snapshot.

        Parameters:
            category: One of CATEGORIES (e.g. 'clustering', 'orthogonality').
            payload: Full result dict to store as JSONB.
            as_of_date: The decision date the analysis was run for.
            subcategory: Optional refinement (e.g. 'k=4', 'pre_2008').
            metrics: Optional summary metrics for fast querying.

        Returns:
            int: Snapshot row ID, or None on failure.
        """
        if as_of_date is None:
            as_of_date = date.today()

        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        "INSERT INTO analytical_snapshots "
                        "(snapshot_date, category, subcategory, as_of_date, payload, metrics) "
                        "VALUES (:sd, :cat, :sub, :aod, :payload::jsonb, :metrics::jsonb) "
                        "RETURNING id"
                    ),
                    {
                        "sd": date.today(),
                        "cat": category,
                        "sub": subcategory,
                        "aod": as_of_date,
                        "payload": _safe_json(payload),
                        "metrics": _safe_json(metrics) if metrics else None,
                    },
                ).fetchone()
            snap_id = row[0] if row else None
            log.info(
                "Snapshot saved — id={id}, category={cat}, as_of={d}",
                id=snap_id, cat=category, d=as_of_date,
            )
            return snap_id
        except Exception as exc:
            log.error("Failed to save snapshot ({cat}): {e}", cat=category, e=str(exc))
            return None

    def save_pipeline_snapshots(self, step_results: dict[str, Any]) -> int:
        """Save snapshots for all pipeline step results.

        Maps each pipeline step result to the appropriate category and
        persists it.  Handles None results (failed steps) gracefully.

        Parameters:
            step_results: Dict of step_name -> result from run_full_pipeline.

        Returns:
            int: Number of snapshots successfully saved.
        """
        today = date.today()
        saved = 0

        # Map pipeline step names to snapshot categories
        step_to_category = {
            "resolution": "conflict_resolution",
            "features": "feature_engineering",
            "orthogonality": "orthogonality",
            "regime": "regime_detection",
            "options_scan": "options_scan",
            "importance": "feature_importance",
        }

        for step_name, result in step_results.items():
            if result is None:
                continue
            category = step_to_category.get(step_name)
            if category is None:
                continue

            # Extract metrics for fast querying
            metrics = self._extract_metrics(category, result)

            snap_id = self.save_snapshot(
                category=category,
                payload=result if isinstance(result, dict) else {"result": result},
                as_of_date=today,
                metrics=metrics,
            )
            if snap_id is not None:
                saved += 1

        # Also save the full pipeline summary
        pipeline_metrics = {
            "steps_succeeded": sum(1 for v in step_results.values() if v is not None),
            "steps_failed": sum(1 for v in step_results.values() if v is None),
            "total_steps": len(step_results),
        }
        self.save_snapshot(
            category="pipeline_summary",
            payload=step_results,
            as_of_date=today,
            metrics=pipeline_metrics,
        )
        saved += 1

        return saved

    def _extract_metrics(self, category: str, result: Any) -> dict[str, Any] | None:
        """Extract summary metrics from a result for fast querying."""
        if not isinstance(result, dict):
            return None

        if category == "clustering":
            return {
                "best_k": result.get("best_k"),
                "n_observations": result.get("n_observations"),
                "variance_explained": result.get("variance_explained"),
            }
        elif category == "orthogonality":
            return {
                "n_features": result.get("n_features_analyzed"),
                "true_dimensionality": result.get("true_dimensionality"),
                "n_correlated_pairs": len(result.get("highly_correlated_pairs", [])),
                "n_unstable_pairs": len(result.get("unstable_pairs", [])),
            }
        elif category == "conflict_resolution":
            return {
                "resolved": result.get("resolved"),
                "conflicts_found": result.get("conflicts_found"),
                "errors": result.get("errors"),
            }
        elif category == "feature_engineering":
            if isinstance(result, dict):
                return {
                    "n_features": len(result),
                    "n_non_null": sum(1 for v in result.values() if v is not None),
                }
        elif category == "options_scan":
            return {
                "opportunities": result.get("opportunities"),
                "n_100x": result.get("100x"),
            }
        elif category == "feature_importance":
            summary = result.get("summary", [])
            return {
                "n_features": result.get("n_features"),
                "top_feature": summary[0]["feature_name"] if summary else None,
                "top_composite": summary[0]["composite_score"] if summary else None,
            }
        return None

    # ------------------------------------------------------------------
    # Read / compare
    # ------------------------------------------------------------------

    def get_latest(
        self,
        category: str,
        n: int = 1,
    ) -> list[dict[str, Any]]:
        """Retrieve the most recent snapshots for a category.

        Parameters:
            category: Snapshot category to query.
            n: Number of recent snapshots to return.

        Returns:
            list[dict]: Snapshot rows with id, snapshot_date, payload, metrics.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, snapshot_date, as_of_date, subcategory, "
                    "       payload, metrics, created_at "
                    "FROM analytical_snapshots "
                    "WHERE category = :cat "
                    "ORDER BY snapshot_date DESC, created_at DESC "
                    "LIMIT :n"
                ),
                {"cat": category, "n": n},
            ).fetchall()

        return [
            {
                "id": r[0],
                "snapshot_date": r[1],
                "as_of_date": r[2],
                "subcategory": r[3],
                "payload": r[4],
                "metrics": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]

    def get_history(
        self,
        category: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Retrieve snapshot metrics over a date range for trending.

        Returns a DataFrame of (snapshot_date, metrics) rows — useful for
        plotting how e.g. true_dimensionality or best_k changes over time.

        Parameters:
            category: Snapshot category.
            start_date: Earliest snapshot date (default: 90 days ago).
            end_date: Latest snapshot date (default: today).

        Returns:
            pd.DataFrame: Columns include snapshot_date + all metrics keys.
        """
        from datetime import timedelta

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=90)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT snapshot_date, metrics "
                    "FROM analytical_snapshots "
                    "WHERE category = :cat "
                    "  AND snapshot_date >= :sd "
                    "  AND snapshot_date <= :ed "
                    "ORDER BY snapshot_date"
                ),
                {"cat": category, "sd": start_date, "ed": end_date},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            rec = {"snapshot_date": r[0]}
            if r[1]:
                rec.update(r[1])
            records.append(rec)

        return pd.DataFrame(records)

    def compare_snapshots(
        self,
        category: str,
        date_a: date,
        date_b: date,
    ) -> dict[str, Any]:
        """Compare two snapshots from different dates.

        Parameters:
            category: Snapshot category.
            date_a: First date.
            date_b: Second date.

        Returns:
            dict with keys: date_a, date_b, metrics_a, metrics_b, deltas.
        """
        with self.engine.connect() as conn:
            snap_a = conn.execute(
                text(
                    "SELECT metrics, payload FROM analytical_snapshots "
                    "WHERE category = :cat AND snapshot_date = :d "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"cat": category, "d": date_a},
            ).fetchone()

            snap_b = conn.execute(
                text(
                    "SELECT metrics, payload FROM analytical_snapshots "
                    "WHERE category = :cat AND snapshot_date = :d "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"cat": category, "d": date_b},
            ).fetchone()

        if snap_a is None or snap_b is None:
            missing = []
            if snap_a is None:
                missing.append(str(date_a))
            if snap_b is None:
                missing.append(str(date_b))
            return {"error": f"Missing snapshots for: {', '.join(missing)}"}

        metrics_a = snap_a[0] or {}
        metrics_b = snap_b[0] or {}

        # Compute deltas for numeric metrics
        deltas: dict[str, Any] = {}
        all_keys = set(metrics_a.keys()) | set(metrics_b.keys())
        for key in all_keys:
            val_a = metrics_a.get(key)
            val_b = metrics_b.get(key)
            if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
                deltas[key] = {"a": val_a, "b": val_b, "delta": val_b - val_a}
            else:
                deltas[key] = {"a": val_a, "b": val_b}

        return {
            "category": category,
            "date_a": date_a.isoformat(),
            "date_b": date_b.isoformat(),
            "metrics_a": metrics_a,
            "metrics_b": metrics_b,
            "deltas": deltas,
        }
