"""
GRID feature importance tracking.

Records, queries, and monitors feature importance scores across model
versions and time to detect drift and inform feature selection.

-- DDL (also appended to schema.sql) ------------------------------------
-- CREATE TABLE IF NOT EXISTS feature_importance_log (
--     id BIGSERIAL PRIMARY KEY,
--     model_version_id INTEGER REFERENCES model_registry(id),
--     feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
--     importance_score DOUBLE PRECISION NOT NULL,
--     importance_method TEXT NOT NULL DEFAULT 'permutation',
--     as_of_date DATE NOT NULL,
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
-- );
-- CREATE INDEX IF NOT EXISTS idx_feature_importance_feature
--     ON feature_importance_log (feature_id, as_of_date DESC);
-- CREATE INDEX IF NOT EXISTS idx_feature_importance_model
--     ON feature_importance_log (model_version_id);
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class FeatureImportanceTracker:
    """Track and analyse feature importance scores over time.

    Attributes:
        engine: SQLAlchemy engine for database access.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        log.info("FeatureImportanceTracker initialised")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _feature_id_for_name(self, name: str) -> int | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :name"),
                {"name": name},
            ).fetchone()
        return row[0] if row else None

    def _feature_name_for_id(self, feature_id: int) -> str | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT name FROM feature_registry WHERE id = :id"),
                {"id": feature_id},
            ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # 1. record_importance
    # ------------------------------------------------------------------

    def record_importance(
        self,
        model_version_id: int,
        feature_importances: dict[str, float],
        as_of_date: date,
        method: str = "permutation",
    ) -> int:
        """Persist importance scores for a set of features.

        Parameters:
            model_version_id: FK into model_registry.
            feature_importances: Mapping of feature name -> importance score.
            as_of_date: Date the scores were computed for.
            method: Importance method (e.g. 'permutation', 'shap', 'gain').

        Returns:
            int: Number of rows inserted.
        """
        rows_inserted = 0
        with self.engine.begin() as conn:
            for fname, score in feature_importances.items():
                fid = self._resolve_feature_id(conn, fname)
                if fid is None:
                    log.warning("Feature '{f}' not in registry — skipped", f=fname)
                    continue
                conn.execute(
                    text(
                        "INSERT INTO feature_importance_log "
                        "(model_version_id, feature_id, importance_score, "
                        " importance_method, as_of_date) "
                        "VALUES (:mid, :fid, :score, :method, :aod)"
                    ),
                    {
                        "mid": model_version_id,
                        "fid": fid,
                        "score": score,
                        "method": method,
                        "aod": as_of_date,
                    },
                )
                rows_inserted += 1

        log.info(
            "Recorded {n} importance scores for model_version={m}, as_of={d}",
            n=rows_inserted, m=model_version_id, d=as_of_date,
        )
        return rows_inserted

    @staticmethod
    def _resolve_feature_id(conn: Any, name: str) -> int | None:
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": name},
        ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # 2. get_importance_history
    # ------------------------------------------------------------------

    def get_importance_history(
        self,
        feature_name: str,
        days_back: int = 90,
    ) -> pd.DataFrame:
        """Return a time series of importance scores for one feature.

        Parameters:
            feature_name: Name in the feature registry.
            days_back: How many calendar days of history to fetch.

        Returns:
            DataFrame with columns [as_of_date, importance_score,
            importance_method, model_version_id].
        """
        fid = self._feature_id_for_name(feature_name)
        if fid is None:
            log.warning("Feature '{f}' not found", f=feature_name)
            return pd.DataFrame()

        cutoff = date.today() - timedelta(days=days_back)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT as_of_date, importance_score, importance_method, "
                    "       model_version_id "
                    "FROM feature_importance_log "
                    "WHERE feature_id = :fid AND as_of_date >= :cutoff "
                    "ORDER BY as_of_date"
                ),
                {"fid": fid, "cutoff": cutoff},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(
            rows,
            columns=["as_of_date", "importance_score",
                      "importance_method", "model_version_id"],
        )

    # ------------------------------------------------------------------
    # 3. get_current_rankings
    # ------------------------------------------------------------------

    def get_current_rankings(self, days_back: int = 30) -> pd.DataFrame:
        """Rank features by average recent importance.

        Parameters:
            days_back: Window (calendar days) for computing the average.

        Returns:
            DataFrame with columns [feature_name, avg_importance, n_obs]
            sorted descending by avg_importance.
        """
        cutoff = date.today() - timedelta(days=days_back)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT fr.name, "
                    "       AVG(fil.importance_score) AS avg_importance, "
                    "       COUNT(*) AS n_obs "
                    "FROM feature_importance_log fil "
                    "JOIN feature_registry fr ON fr.id = fil.feature_id "
                    "WHERE fil.as_of_date >= :cutoff "
                    "GROUP BY fr.name "
                    "ORDER BY avg_importance DESC"
                ),
                {"cutoff": cutoff},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(
            rows,
            columns=["feature_name", "avg_importance", "n_obs"],
        )

    # ------------------------------------------------------------------
    # 4. detect_importance_drift
    # ------------------------------------------------------------------

    def detect_importance_drift(
        self,
        feature_name: str,
        window: int = 30,
        threshold: float = 2.0,
    ) -> dict[str, Any]:
        """Flag a feature whose importance shifted significantly.

        Compares the mean importance in the most recent *window* days
        against the preceding *window* days.  A z-score above *threshold*
        (in absolute value) is flagged as drift.

        Parameters:
            feature_name: Feature to check.
            window: Number of calendar days per half-window.
            threshold: Z-score threshold for flagging drift.

        Returns:
            dict with keys: drifted (bool), z_score, recent_mean,
            prior_mean, prior_std.
        """
        fid = self._feature_id_for_name(feature_name)
        if fid is None:
            log.warning("Feature '{f}' not found", f=feature_name)
            return {"drifted": False, "error": "feature_not_found"}

        today = date.today()
        recent_start = today - timedelta(days=window)
        prior_start = recent_start - timedelta(days=window)

        with self.engine.connect() as conn:
            recent = conn.execute(
                text(
                    "SELECT importance_score FROM feature_importance_log "
                    "WHERE feature_id = :fid "
                    "  AND as_of_date >= :rs AND as_of_date < :today"
                ),
                {"fid": fid, "rs": recent_start, "today": today},
            ).fetchall()

            prior = conn.execute(
                text(
                    "SELECT importance_score FROM feature_importance_log "
                    "WHERE feature_id = :fid "
                    "  AND as_of_date >= :ps AND as_of_date < :rs"
                ),
                {"fid": fid, "ps": prior_start, "rs": recent_start},
            ).fetchall()

        if len(recent) < 2 or len(prior) < 2:
            return {
                "drifted": False,
                "error": "insufficient_data",
                "recent_n": len(recent),
                "prior_n": len(prior),
            }

        recent_vals = pd.Series([r[0] for r in recent])
        prior_vals = pd.Series([r[0] for r in prior])

        recent_mean = float(recent_vals.mean())
        prior_mean = float(prior_vals.mean())
        prior_std = float(prior_vals.std())

        if prior_std < 1e-12:
            z_score = 0.0
        else:
            z_score = (recent_mean - prior_mean) / prior_std

        drifted = abs(z_score) > threshold

        if drifted:
            log.warning(
                "Importance drift detected for '{f}': z={z:.2f} "
                "(recent_mean={rm:.4f}, prior_mean={pm:.4f})",
                f=feature_name, z=z_score, rm=recent_mean, pm=prior_mean,
            )

        return {
            "drifted": drifted,
            "z_score": round(z_score, 4),
            "recent_mean": round(recent_mean, 6),
            "prior_mean": round(prior_mean, 6),
            "prior_std": round(prior_std, 6),
        }
