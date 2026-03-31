"""
GRID feature importance tracking module.

Records, queries, and monitors feature importance scores across model
versions and time.  Computes three complementary importance measures:

- Permutation importance: how much accuracy drops when a feature is shuffled.
- Regime correlation: how each feature correlates with regime transitions.
- Rolling stability: does the feature matter consistently across time?

All analytical data access is PIT-correct via the PITStore.

-- DDL (also in schema.sql) -----------------------------------------------
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

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


class FeatureImportanceTracker:
    """Track, compute, and analyse feature importance scores over time.

    Computes and stores feature importance metrics:
    - Permutation importance (how much accuracy drops when feature is shuffled)
    - Correlation with regime transitions
    - Rolling contribution stability (does feature matter consistently?)

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore instance for point-in-time queries.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the feature importance tracker.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for point-in-time data access.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("FeatureImportanceTracker initialised")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _feature_id_for_name(self, name: str) -> int | None:
        """Look up a feature_registry ID by name."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :name"),
                {"name": name},
            ).fetchone()
        return row[0] if row else None

    def _feature_name_for_id(self, feature_id: int) -> str | None:
        """Look up a feature name by ID."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT name FROM feature_registry WHERE id = :id"),
                {"id": feature_id},
            ).fetchone()
        return row[0] if row else None

    def _get_feature_names(self, feature_ids: list[int]) -> dict[int, str]:
        """Map feature IDs to their names.

        Parameters:
            feature_ids: List of feature_registry IDs.

        Returns:
            dict mapping feature_id -> feature name.
        """
        if not feature_ids:
            return {}

        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE id = ANY(:fids)"),
                {"fids": feature_ids},
            ).fetchall()

        return {row[0]: row[1] for row in rows}

    def _get_model_info(self, model_id: int) -> dict[str, Any] | None:
        """Look up model details from the registry.

        Parameters:
            model_id: Model registry ID.

        Returns:
            dict with model fields, or None if not found.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, name, layer, version, feature_set, "
                    "parameter_snapshot, hypothesis_id "
                    "FROM model_registry WHERE id = :mid"
                ),
                {"mid": model_id},
            ).fetchone()

        if row is None:
            return None

        return {
            "id": row[0],
            "name": row[1],
            "layer": row[2],
            "version": row[3],
            "feature_set": row[4],
            "parameter_snapshot": row[5],
            "hypothesis_id": row[6],
        }

    def _build_feature_matrix(
        self,
        feature_ids: list[int],
        as_of_date: date,
        lookback_days: int = 756,
    ) -> pd.DataFrame:
        """Build a PIT-correct feature matrix for the lookback window.

        Parameters:
            feature_ids: List of feature_registry IDs.
            as_of_date: Decision date for PIT filtering.
            lookback_days: Calendar days of history to include.

        Returns:
            pd.DataFrame: Wide-format matrix indexed by obs_date, columns
            are feature IDs.
        """
        start_date = as_of_date - timedelta(days=lookback_days)
        matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="LATEST_AS_OF",
        )
        return matrix

    def _score_model(
        self,
        feature_matrix: pd.DataFrame,
        parameter_snapshot: dict[str, Any],
        feature_names: dict[int, str],
    ) -> pd.Series:
        """Score a feature matrix using the model's parameter snapshot.

        Applies the weighted threshold scoring from the model parameters
        to produce a score series.  Uses the same logic as
        ``LiveInference._generate_recommendation``.

        Parameters:
            feature_matrix: Wide DataFrame with feature_id columns.
            parameter_snapshot: Model parameters (thresholds, weights).
            feature_names: Mapping of feature_id -> feature name.

        Returns:
            pd.Series: Score per observation date (higher = stronger signal).
        """
        thresholds = parameter_snapshot.get("state_thresholds", {})
        if not thresholds:
            # Fallback: simple mean across features
            return feature_matrix.mean(axis=1)

        # Aggregate absolute weighted score across all states
        scores = pd.Series(0.0, index=feature_matrix.index)
        name_to_id = {v: k for k, v in feature_names.items()}

        for _state, state_config in thresholds.items():
            weights = state_config.get("weights", {})
            for fname, weight in weights.items():
                fid = name_to_id.get(fname)
                if fid is not None and fid in feature_matrix.columns:
                    scores += abs(weight) * feature_matrix[fid].fillna(0)

        return scores

    @staticmethod
    def _resolve_feature_id(conn: Any, name: str) -> int | None:
        """Resolve feature name to ID within an existing connection."""
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": name},
        ).fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Record / query (existing functionality)
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
                    log.warning("Feature '{f}' not in registry -- skipped", f=fname)
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

    # ------------------------------------------------------------------
    # Analytical importance computation
    # ------------------------------------------------------------------

    def compute_permutation_importance(
        self,
        model_id: int,
        as_of_date: date,
        n_repeats: int = 10,
    ) -> dict[str, float]:
        """Compute permutation importance for all features in a model.

        For each feature, shuffle its values and measure how much the
        model's recommendation changes.  Features that cause large changes
        when shuffled are more important.

        Parameters:
            model_id: Model registry ID.
            as_of_date: Decision date for PIT-correct data access.
            n_repeats: Number of shuffle repetitions (default 10).

        Returns:
            dict of feature_name -> importance_score (0-1 normalised).
        """
        log.info(
            "Computing permutation importance -- model={m}, as_of={d}, repeats={r}",
            m=model_id, d=as_of_date, r=n_repeats,
        )

        model = self._get_model_info(model_id)
        if model is None:
            log.warning("Model {m} not found", m=model_id)
            return {}

        feature_ids: list[int] = model["feature_set"] or []
        if not feature_ids:
            log.warning("Model {m} has empty feature set", m=model_id)
            return {}

        feature_names = self._get_feature_names(feature_ids)
        matrix = self._build_feature_matrix(feature_ids, as_of_date)

        if matrix.empty or matrix.shape[0] < 10:
            log.warning(
                "Insufficient data for permutation importance ({n} rows)",
                n=matrix.shape[0],
            )
            return {}

        # Forward-fill up to 5 days (weekends/holidays), then drop remaining NaN
        matrix = matrix.ffill(limit=5).dropna()
        if matrix.empty:
            return {}

        params = model["parameter_snapshot"] or {}
        baseline_scores = self._score_model(matrix, params, feature_names)
        baseline_var = baseline_scores.var()

        if baseline_var == 0 or np.isnan(baseline_var):
            log.warning("Baseline score variance is zero; cannot measure importance")
            return {
                feature_names.get(fid, f"feature_{fid}"): 0.0
                for fid in feature_ids
            }

        importance_raw: dict[str, float] = {}
        rng = np.random.default_rng(seed=42)

        for fid in feature_ids:
            if fid not in matrix.columns:
                continue
            fname = feature_names.get(fid, f"feature_{fid}")
            drops: list[float] = []

            for _ in range(n_repeats):
                shuffled = matrix.copy()
                shuffled[fid] = rng.permutation(shuffled[fid].values)
                shuffled_scores = self._score_model(shuffled, params, feature_names)

                # Measure the change in score (drop = importance)
                score_diff = (baseline_scores - shuffled_scores).abs().mean()
                drops.append(float(score_diff))

            importance_raw[fname] = float(np.mean(drops))

        # Normalise to 0-1 range
        max_imp = max(importance_raw.values()) if importance_raw else 1.0
        if max_imp == 0:
            max_imp = 1.0

        importance: dict[str, float] = {
            fname: round(score / max_imp, 6)
            for fname, score in importance_raw.items()
        }

        # Persist to feature_importance_log
        self._persist_importance(model_id, feature_names, importance, as_of_date, "permutation")

        log.info(
            "Permutation importance computed -- {n} features, top={t}",
            n=len(importance),
            t=max(importance, key=importance.get) if importance else "none",
        )
        return importance

    def compute_regime_correlation(
        self,
        feature_ids: list[int],
        as_of_date: date,
        lookback_days: int = 756,
    ) -> dict[str, dict[str, float]]:
        """Compute how each feature correlates with regime transitions.

        Looks for lead/lag relationships between feature changes and
        regime label changes from the decision journal.

        Parameters:
            feature_ids: List of feature_registry IDs.
            as_of_date: Decision date for PIT-correct data access.
            lookback_days: Calendar days of history to analyse.

        Returns:
            dict of feature_name -> {correlation, p_value, lead_days}.
        """
        log.info(
            "Computing regime correlation -- {n} features, as_of={d}",
            n=len(feature_ids), d=as_of_date,
        )

        feature_names = self._get_feature_names(feature_ids)
        matrix = self._build_feature_matrix(feature_ids, as_of_date, lookback_days)

        if matrix.empty:
            log.warning("Empty feature matrix for regime correlation")
            return {}

        # Get regime transitions from the decision journal
        start_date = as_of_date - timedelta(days=lookback_days)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT DATE(decision_timestamp) AS dt, inferred_state "
                    "FROM decision_journal "
                    "WHERE decision_timestamp >= :start "
                    "  AND decision_timestamp <= :end "
                    "ORDER BY decision_timestamp"
                ),
                {"start": start_date, "end": as_of_date},
            ).fetchall()

        if len(rows) < 5:
            log.warning(
                "Too few journal entries ({n}) for regime correlation",
                n=len(rows),
            )
            return {}

        # Build a regime change indicator series
        regime_df = pd.DataFrame(rows, columns=["dt", "state"])
        regime_df["dt"] = pd.to_datetime(regime_df["dt"])
        regime_df = regime_df.drop_duplicates(subset="dt", keep="last")
        regime_df = regime_df.set_index("dt").sort_index()

        # Encode regime as integer for correlation
        states = regime_df["state"].unique()
        state_map = {s: i for i, s in enumerate(states)}
        regime_df["regime_code"] = regime_df["state"].map(state_map)
        regime_df["transition"] = (
            regime_df["regime_code"].diff().abs() > 0
        ).astype(float)

        results: dict[str, dict[str, float]] = {}
        matrix.index = pd.to_datetime(matrix.index)

        for fid in feature_ids:
            fname = feature_names.get(fid, f"feature_{fid}")
            if fid not in matrix.columns:
                continue

            feat_series = matrix[fid].dropna()
            feat_changes = feat_series.diff().dropna()

            # Find the lead with the strongest correlation
            best_corr = 0.0
            best_pval = 1.0
            best_lead = 0

            for lead in range(0, 22):  # 0 to 21 trading days
                shifted_feat = feat_changes.shift(lead).dropna()
                common = shifted_feat.index.intersection(regime_df.index)
                if len(common) < 10:
                    continue

                corr, pval = stats.pearsonr(
                    shifted_feat.loc[common].values,
                    regime_df.loc[common, "transition"].values,
                )
                if abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_pval = pval
                    best_lead = lead

            results[fname] = {
                "correlation": round(float(best_corr), 6),
                "p_value": round(float(best_pval), 6),
                "lead_days": best_lead,
            }

        log.info("Regime correlation computed for {n} features", n=len(results))
        return results

    def compute_rolling_stability(
        self,
        feature_ids: list[int],
        as_of_date: date,
        window: int = 63,
    ) -> dict[str, dict[str, float]]:
        """Compute rolling importance stability for each feature.

        Measures how consistently important a feature is over time by
        looking at the variance of its rolling absolute z-score.

        Parameters:
            feature_ids: List of feature_registry IDs.
            as_of_date: Decision date for PIT-correct data access.
            window: Rolling window size in trading days (default 63 = 3 months).

        Returns:
            dict of feature_name -> {mean_importance, std_importance, stability_score}.
            A feature with high mean importance and low std is reliably important.
        """
        log.info(
            "Computing rolling stability -- {n} features, window={w}, as_of={d}",
            n=len(feature_ids), w=window, d=as_of_date,
        )

        feature_names = self._get_feature_names(feature_ids)
        matrix = self._build_feature_matrix(feature_ids, as_of_date)

        if matrix.empty:
            log.warning("Empty feature matrix for stability computation")
            return {}

        results: dict[str, dict[str, float]] = {}

        for fid in feature_ids:
            fname = feature_names.get(fid, f"feature_{fid}")
            if fid not in matrix.columns:
                continue

            series = matrix[fid].dropna()
            if len(series) < window:
                results[fname] = {
                    "mean_importance": 0.0,
                    "std_importance": 0.0,
                    "stability_score": 0.0,
                }
                continue

            # Rolling absolute z-score as a proxy for feature "activity"
            rolling_mean = series.rolling(
                window=window, min_periods=window // 2
            ).mean()
            rolling_std = series.rolling(
                window=window, min_periods=window // 2
            ).std()
            rolling_std = rolling_std.replace(0, np.nan)
            rolling_zscore = ((series - rolling_mean) / rolling_std).abs().dropna()

            if rolling_zscore.empty:
                results[fname] = {
                    "mean_importance": 0.0,
                    "std_importance": 0.0,
                    "stability_score": 0.0,
                }
                continue

            mean_imp = float(rolling_zscore.mean())
            std_imp = float(rolling_zscore.std())

            # Stability = mean / (mean + std) -- higher is more stable
            stability = (
                mean_imp / (mean_imp + std_imp)
                if (mean_imp + std_imp) > 0
                else 0.0
            )

            results[fname] = {
                "mean_importance": round(mean_imp, 6),
                "std_importance": round(std_imp, 6),
                "stability_score": round(stability, 6),
            }

        log.info("Rolling stability computed for {n} features", n=len(results))
        return results

    def get_importance_report(
        self,
        model_id: int,
        as_of_date: date | None = None,
    ) -> dict[str, Any]:
        """Generate a complete feature importance report.

        Combines permutation importance, regime correlation, and rolling
        stability into a single report with a composite ranking.

        Parameters:
            model_id: Model registry ID.
            as_of_date: Decision date (default: today).

        Returns:
            dict with keys: model_id, as_of_date, permutation_importance,
            regime_correlation, rolling_stability, summary.
        """
        if as_of_date is None:
            as_of_date = date.today()

        log.info(
            "Generating feature importance report -- model={m}, as_of={d}",
            m=model_id, d=as_of_date,
        )

        model = self._get_model_info(model_id)
        if model is None:
            return {"error": f"Model {model_id} not found"}

        feature_ids: list[int] = model["feature_set"] or []
        if not feature_ids:
            return {"error": f"Model {model_id} has no features"}

        perm_importance = self.compute_permutation_importance(
            model_id, as_of_date
        )
        regime_corr = self.compute_regime_correlation(
            feature_ids, as_of_date
        )
        stability = self.compute_rolling_stability(
            feature_ids, as_of_date
        )

        # Build a summary ranking
        feature_names = self._get_feature_names(feature_ids)
        summary: list[dict[str, Any]] = []

        for fid in feature_ids:
            fname = feature_names.get(fid, f"feature_{fid}")
            perm_score = perm_importance.get(fname, 0.0)
            corr_info = regime_corr.get(fname, {})
            stab_info = stability.get(fname, {})

            # Composite score: weighted average of normalised metrics
            corr_abs = abs(corr_info.get("correlation", 0.0))
            stab_score = stab_info.get("stability_score", 0.0)
            composite = round(
                0.5 * perm_score + 0.3 * corr_abs + 0.2 * stab_score, 6
            )

            summary.append({
                "feature_name": fname,
                "feature_id": fid,
                "permutation_importance": perm_score,
                "regime_correlation": corr_info.get("correlation", 0.0),
                "regime_p_value": corr_info.get("p_value", 1.0),
                "regime_lead_days": corr_info.get("lead_days", 0),
                "stability_score": stab_score,
                "composite_score": composite,
            })

        # Sort by composite score descending
        summary.sort(key=lambda x: x["composite_score"], reverse=True)

        report: dict[str, Any] = {
            "model_id": model_id,
            "model_name": model["name"],
            "as_of_date": as_of_date.isoformat(),
            "n_features": len(feature_ids),
            "permutation_importance": perm_importance,
            "regime_correlation": regime_corr,
            "rolling_stability": stability,
            "summary": summary,
        }

        log.info(
            "Feature importance report complete -- {n} features ranked",
            n=len(summary),
        )
        return report

    # ------------------------------------------------------------------
    # Persistence helper
    # ------------------------------------------------------------------

    def _persist_importance(
        self,
        model_id: int,
        feature_names: dict[int, str],
        importance: dict[str, float],
        as_of_date: date,
        method: str,
    ) -> None:
        """Write importance scores to the feature_importance_log table.

        Parameters:
            model_id: Model registry ID.
            feature_names: Mapping of feature_id -> name.
            importance: Mapping of feature_name -> score.
            as_of_date: Date the importance was computed for.
            method: Importance method label (e.g. 'permutation').
        """
        name_to_id = {v: k for k, v in feature_names.items()}

        rows_to_insert = []
        for fname, score in importance.items():
            fid = name_to_id.get(fname)
            if fid is None:
                continue
            rows_to_insert.append({
                "mid": model_id,
                "fid": fid,
                "score": score,
                "method": method,
                "aod": as_of_date,
            })

        if not rows_to_insert:
            return

        with self.engine.begin() as conn:
            for row in rows_to_insert:
                conn.execute(
                    text(
                        "INSERT INTO feature_importance_log "
                        "(model_version_id, feature_id, importance_score, "
                        "importance_method, as_of_date) "
                        "VALUES (:mid, :fid, :score, :method, :aod)"
                    ),
                    row,
                )

        log.debug(
            "Persisted {n} importance scores (method={m})",
            n=len(rows_to_insert), m=method,
        )

    # ------------------------------------------------------------------
    # Enhanced drift detection
    # ------------------------------------------------------------------

    def detect_data_distribution_drift(
        self,
        feature_ids: list[int],
        window: int = 63,
        threshold: float = 0.05,
    ) -> dict[str, dict[str, Any]]:
        """Two-sample KS test comparing recent vs prior feature distributions.

        For each feature, compare the distribution of values in the most recent
        `window` trading days vs the preceding `window` days.
        """
        from scipy.stats import ks_2samp

        df = self.pit_store.get_feature_matrix(feature_ids=feature_ids, as_of_date=None)
        if df is None or len(df) < window * 2:
            return {}

        results = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) < window * 2:
                continue
            recent = series.iloc[-window:].values
            prior = series.iloc[-window * 2:-window].values
            stat, p_value = ks_2samp(recent, prior)
            results[str(col)] = {
                "ks_statistic": round(float(stat), 4),
                "p_value": round(float(p_value), 4),
                "drifted": p_value < threshold,
                "recent_mean": round(float(recent.mean()), 4),
                "prior_mean": round(float(prior.mean()), 4),
            }
        return results

    def detect_prediction_confidence_drift(
        self,
        model_id: int,
        window: int = 30,
    ) -> dict[str, Any]:
        """Detect declining model confidence over time via Welch's t-test."""
        from scipy.stats import ttest_ind
        from sqlalchemy import text

        with self.db_engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT state_confidence FROM decision_journal "
                "WHERE model_version_id = :mid "
                "ORDER BY decision_timestamp DESC LIMIT :lim"
            ), {"mid": model_id, "lim": window * 2}).fetchall()

        if len(rows) < window:
            return {"sufficient_data": False}

        confidences = [float(r[0]) for r in rows]
        recent = confidences[:window]
        prior = confidences[window:window * 2] if len(confidences) >= window * 2 else confidences[window:]

        if len(prior) < 5:
            return {"sufficient_data": False}

        import numpy as np
        t_stat, p_value = ttest_ind(recent, prior, equal_var=False)
        return {
            "sufficient_data": True,
            "recent_mean_confidence": round(float(np.mean(recent)), 4),
            "prior_mean_confidence": round(float(np.mean(prior)), 4),
            "t_statistic": round(float(t_stat), 4),
            "p_value": round(float(p_value), 4),
            "declining": float(np.mean(recent)) < float(np.mean(prior)) and p_value < 0.05,
        }

    def get_comprehensive_drift_report(
        self,
        model_id: int,
    ) -> dict[str, Any]:
        """Combine all drift types into a single report with recommended action."""
        import numpy as np
        from sqlalchemy import text

        # Get model's feature set
        with self.db_engine.connect() as conn:
            row = conn.execute(text(
                "SELECT feature_set FROM model_registry WHERE id = :mid"
            ), {"mid": model_id}).fetchone()

        feature_ids = list(row[0]) if row and row[0] else []

        # Run all drift checks
        importance_drift = self.detect_importance_drift(model_id) if hasattr(self, 'detect_importance_drift') else {}
        data_drift = self.detect_data_distribution_drift(feature_ids) if feature_ids else {}
        confidence_drift = self.detect_prediction_confidence_drift(model_id)

        # Compute composite drift score (0-1)
        scores = []

        # Importance drift contribution
        if importance_drift and isinstance(importance_drift, dict):
            n_drifted = sum(1 for v in importance_drift.values() if isinstance(v, dict) and v.get("drifted"))
            n_total = sum(1 for v in importance_drift.values() if isinstance(v, dict))
            if n_total > 0:
                scores.append(n_drifted / n_total)

        # Data distribution drift contribution
        if data_drift:
            n_drifted = sum(1 for v in data_drift.values() if v.get("drifted"))
            n_total = len(data_drift)
            if n_total > 0:
                scores.append(n_drifted / n_total)

        # Confidence drift contribution
        if confidence_drift.get("declining"):
            scores.append(0.8)
        elif confidence_drift.get("sufficient_data"):
            scores.append(0.1)

        overall_score = float(np.mean(scores)) if scores else 0.0

        # Determine action
        if overall_score > 0.8:
            action = "FLAG"
        elif overall_score > 0.5:
            action = "RETRAIN"
        elif overall_score > 0.2:
            action = "MONITOR"
        else:
            action = "OK"

        return {
            "model_id": model_id,
            "importance_drift": importance_drift,
            "data_distribution_drift": data_drift,
            "prediction_confidence_drift": confidence_drift,
            "overall_drift_score": round(overall_score, 4),
            "recommended_action": action,
        }
