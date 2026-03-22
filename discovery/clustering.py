"""
GRID cluster discovery engine.

Performs unsupervised regime discovery using PCA-reduced features and
multiple clustering algorithms (GMM, KMeans, Agglomerative).  Identifies
regime transition leaders.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import calinski_harabasz_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


class ClusterDiscovery:
    """Unsupervised regime discovery via PCA + clustering.

    Tests multiple values of k using GMM, KMeans, and Agglomerative
    clustering to find the empirically best number of market regimes.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore for point-in-time data retrieval.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the cluster discovery engine.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for point-in-time data access.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("ClusterDiscovery initialised")

    def _get_eligible_feature_ids(self) -> list[int]:
        """Retrieve all model-eligible feature IDs.

        Returns:
            list[int]: Feature IDs where model_eligible = TRUE.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM feature_registry WHERE model_eligible = TRUE ORDER BY id")
            ).fetchall()
        return [row[0] for row in rows]

    def run_cluster_discovery(
        self,
        n_components: int,
        as_of_date: date | None = None,
        start_date: date | None = None,
        output_dir: str = "outputs/clustering",
    ) -> dict[str, Any]:
        """Run full cluster discovery using PCA-reduced features.

        Parameters:
            n_components: Number of PCA components to use.
            as_of_date: Decision date (default: today).
            start_date: Earliest date for feature data (default: 1947-01-01).
            output_dir: Directory for saving output files.

        Returns:
            dict: Summary with best_k, all metrics, and cluster assignments.
        """
        if as_of_date is None:
            as_of_date = date.today()
        if start_date is None:
            start_date = date(1947, 1, 1)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        log.info(
            "Starting cluster discovery — n_components={nc}, as_of={d}",
            nc=n_components,
            d=as_of_date,
        )

        # Step a: Load feature matrix, standardise, PCA
        feature_ids = self._get_eligible_feature_ids()
        matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=start_date,
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="FIRST_RELEASE",
        )

        if matrix.empty:
            log.warning("Empty feature matrix — cannot cluster")
            return {"error": "Empty feature matrix"}

        # Drop high-missing features, forward-fill, drop remaining NaNs
        missing_pct = matrix.isnull().mean()
        matrix = matrix.drop(columns=missing_pct[missing_pct > 0.3].index)
        matrix = matrix.ffill(limit=5).dropna()

        if matrix.shape[0] < 30:
            log.warning("Insufficient data rows ({n}) for clustering", n=matrix.shape[0])
            return {"error": f"Insufficient data rows: {matrix.shape[0]}"}

        scaler = StandardScaler()
        scaled = scaler.fit_transform(matrix)

        actual_components = min(n_components, matrix.shape[1], matrix.shape[0])
        pca = PCA(n_components=actual_components)
        pca_features = pca.fit_transform(scaled)
        dates = matrix.index

        log.info(
            "PCA: {nc} components, {v:.1%} variance explained",
            nc=actual_components,
            v=sum(pca.explained_variance_ratio_),
        )

        # Step b: Test k = 2,3,4,5,6
        k_values = [2, 3, 4, 5, 6]
        all_results: list[dict[str, Any]] = []

        for k in k_values:
            log.info("Testing k={k}", k=k)
            result = self._evaluate_k(pca_features, k, dates)
            all_results.append(result)

        # Step d: Save results
        results_df = pd.DataFrame(all_results)
        results_df.to_csv(out_path / f"cluster_metrics_{timestamp}.csv", index=False)

        # Find best k by silhouette score (from KMeans)
        best_idx = results_df["kmeans_silhouette"].idxmax()
        best_k = results_df.loc[best_idx, "k"]

        log.info("Best k={k} (silhouette={s:.4f})", k=best_k, s=results_df.loc[best_idx, "kmeans_silhouette"])

        # Step e: Generate final assignments with best k
        gmm = GaussianMixture(n_components=int(best_k), random_state=42, n_init=5)
        labels = gmm.fit_predict(pca_features)
        probs = gmm.predict_proba(pca_features)
        confidence = probs.max(axis=1)

        assignments = pd.DataFrame({
            "date": dates,
            "cluster_id": labels,
            "confidence": confidence,
        })
        assignments.to_csv(out_path / f"cluster_assignments_{timestamp}.csv", index=False)

        # Transition matrix
        transition = self._compute_transition_matrix(labels, int(best_k))
        pd.DataFrame(
            transition,
            index=[f"from_{i}" for i in range(int(best_k))],
            columns=[f"to_{i}" for i in range(int(best_k))],
        ).to_csv(out_path / f"transition_matrix_{timestamp}.csv")

        # Summary plot
        self._save_summary_plot(results_df, out_path / f"cluster_summary_{timestamp}.png")

        summary: dict[str, Any] = {
            "best_k": int(best_k),
            "all_metrics": results_df.to_dict("records"),
            "transition_matrix": transition.tolist(),
            "n_observations": len(dates),
            "pca_components_used": actual_components,
            "variance_explained": float(sum(pca.explained_variance_ratio_)),
        }

        # Optional: LLM-assisted interpretation of changing correlations
        try:
            from hyperspace.client import get_client
            from hyperspace.reasoner import GRIDReasoner

            hs_client = get_client()
            if hs_client.is_available:
                reasoner = GRIDReasoner(hs_client)
                # Use unstable_pairs from metrics if available
                interp_pairs = [
                    r for r in all_results
                    if r.get("transition_entropy", 0) > 0
                ][:5]
                if interp_pairs:
                    log.info(
                        "Requesting LLM interpretation for cluster dynamics"
                    )
                    explanation = reasoner.explain_relationship(
                        "cluster_structure",
                        "market_regimes",
                        f"Best k={best_k} with persistence={all_results[int(best_k)-2].get('gmm_persistence', 0):.1f} days. "
                        f"Transition entropy varies across k values.",
                    )
                    if explanation:
                        log.info(
                            "Cluster interpretation: {e}",
                            e=explanation[:200],
                        )
                        summary["llm_interpretation"] = explanation
        except Exception as exc:
            log.debug("Hyperspace interpretation skipped: {e}", e=str(exc))

        log.info("Cluster discovery complete — best_k={k}", k=best_k)
        return summary

    def _evaluate_k(
        self,
        features: np.ndarray,
        k: int,
        dates: pd.DatetimeIndex,
    ) -> dict[str, Any]:
        """Evaluate clustering quality for a given k.

        Parameters:
            features: PCA-transformed feature matrix (n_samples, n_components).
            k: Number of clusters to test.
            dates: Date index for cluster persistence computation.

        Returns:
            dict: Metrics for this k value.
        """
        result: dict[str, Any] = {"k": k}

        # GMM
        gmm = GaussianMixture(n_components=k, random_state=42, n_init=5)
        gmm_labels = gmm.fit_predict(features)
        result["gmm_bic"] = float(gmm.bic(features))
        result["gmm_aic"] = float(gmm.aic(features))

        # KMeans
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km_labels = km.fit_predict(features)
        result["kmeans_inertia"] = float(km.inertia_)
        result["kmeans_silhouette"] = float(silhouette_score(features, km_labels))

        # Agglomerative
        agg = AgglomerativeClustering(n_clusters=k)
        agg_labels = agg.fit_predict(features)
        result["agg_calinski_harabasz"] = float(calinski_harabasz_score(features, agg_labels))

        # Cluster persistence (average run length of same label using GMM)
        result["gmm_persistence"] = float(self._compute_persistence(gmm_labels))

        # Transition matrix (using GMM labels)
        transition = self._compute_transition_matrix(gmm_labels, k)
        result["transition_entropy"] = float(
            -np.nansum(transition * np.log2(np.clip(transition, 1e-10, 1)))
        )

        return result

    def _compute_persistence(self, labels: np.ndarray) -> float:
        """Compute average run length (persistence) of cluster assignments.

        Parameters:
            labels: Array of cluster labels.

        Returns:
            float: Average number of consecutive days in the same cluster.
        """
        if len(labels) == 0:
            return 0.0

        run_lengths: list[int] = []
        current_run = 1

        for i in range(1, len(labels)):
            if labels[i] == labels[i - 1]:
                current_run += 1
            else:
                run_lengths.append(current_run)
                current_run = 1
        run_lengths.append(current_run)

        return float(np.mean(run_lengths))

    def _compute_transition_matrix(
        self,
        labels: np.ndarray,
        k: int,
    ) -> np.ndarray:
        """Compute the cluster transition probability matrix.

        Parameters:
            labels: Array of cluster labels.
            k: Number of clusters.

        Returns:
            np.ndarray: k x k transition probability matrix.
        """
        trans = np.zeros((k, k))
        np.add.at(trans, (labels[:-1], labels[1:]), 1)

        # Normalise rows to probabilities
        row_sums = trans.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1  # Avoid division by zero
        return trans / row_sums

    def _save_summary_plot(self, results_df: pd.DataFrame, filepath: Path) -> None:
        """Save a summary plot of clustering metrics across k values.

        Parameters:
            results_df: DataFrame with one row per k value.
            filepath: Output PNG path.
        """
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))

        ks = results_df["k"]

        axes[0, 0].plot(ks, results_df["gmm_bic"], "b-o", label="BIC")
        axes[0, 0].plot(ks, results_df["gmm_aic"], "r-o", label="AIC")
        axes[0, 0].set_title("GMM Model Selection")
        axes[0, 0].set_xlabel("k")
        axes[0, 0].legend()

        axes[0, 1].plot(ks, results_df["kmeans_silhouette"], "g-o")
        axes[0, 1].set_title("KMeans Silhouette Score")
        axes[0, 1].set_xlabel("k")

        axes[1, 0].plot(ks, results_df["agg_calinski_harabasz"], "m-o")
        axes[1, 0].set_title("Agglomerative Calinski-Harabasz")
        axes[1, 0].set_xlabel("k")

        axes[1, 1].plot(ks, results_df["gmm_persistence"], "c-o")
        axes[1, 1].set_title("Cluster Persistence (avg run length)")
        axes[1, 1].set_xlabel("k")

        fig.suptitle("Cluster Discovery — Metric Comparison", fontsize=14)
        fig.tight_layout()
        fig.savefig(filepath, dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("Summary plot saved to {p}", p=filepath)

    def identify_transition_leaders(
        self,
        cluster_assignments: pd.DataFrame,
        feature_matrix: pd.DataFrame,
    ) -> pd.DataFrame:
        """Identify features that predict cluster transitions.

        For each cluster transition, tests which features most reliably
        changed 1, 2, 4, and 8 weeks prior.

        Parameters:
            cluster_assignments: DataFrame with columns [date, cluster_id, confidence].
            feature_matrix: Wide DataFrame with obs_date index and feature columns.

        Returns:
            pd.DataFrame: Feature rankings by transition predictiveness.
        """
        log.info("Identifying transition leaders")

        # Find transition points
        transitions: list[int] = []
        labels = cluster_assignments["cluster_id"].values
        for i in range(1, len(labels)):
            if labels[i] != labels[i - 1]:
                transitions.append(i)

        if not transitions:
            log.warning("No transitions found")
            return pd.DataFrame()

        log.info("Found {n} transitions", n=len(transitions))

        lookback_weeks = [1, 2, 4, 8]
        lookback_days = [w * 5 for w in lookback_weeks]  # Trading days

        results: list[dict[str, Any]] = []

        for feature_col in feature_matrix.columns:
            feature_scores: dict[str, float] = {"feature": str(feature_col)}

            for weeks, days in zip(lookback_weeks, lookback_days):
                changes_at_transition: list[float] = []
                changes_at_non_transition: list[float] = []

                for i in range(days, len(feature_matrix)):
                    pct_chg = (
                        (feature_matrix.iloc[i][feature_col] - feature_matrix.iloc[i - days][feature_col])
                        / abs(feature_matrix.iloc[i - days][feature_col])
                        if feature_matrix.iloc[i - days][feature_col] != 0
                        else 0.0
                    )

                    if np.isnan(pct_chg):
                        continue

                    if i in transitions:
                        changes_at_transition.append(pct_chg)
                    else:
                        changes_at_non_transition.append(pct_chg)

                # Test if transition changes are significantly different
                if len(changes_at_transition) >= 3 and len(changes_at_non_transition) >= 3:
                    t_stat, p_val = stats.ttest_ind(
                        changes_at_transition,
                        changes_at_non_transition,
                        equal_var=False,
                    )
                    feature_scores[f"t_stat_{weeks}w"] = round(float(t_stat), 4)
                    feature_scores[f"p_val_{weeks}w"] = round(float(p_val), 6)
                else:
                    feature_scores[f"t_stat_{weeks}w"] = np.nan
                    feature_scores[f"p_val_{weeks}w"] = np.nan

            results.append(feature_scores)

        df = pd.DataFrame(results)

        # Rank by most significant across all lookbacks
        p_cols = [c for c in df.columns if c.startswith("p_val_")]
        df["min_p_val"] = df[p_cols].min(axis=1)
        df = df.sort_values("min_p_val")

        log.info("Transition leader analysis complete — top feature: {f}", f=df.iloc[0]["feature"] if len(df) > 0 else "N/A")
        return df


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    pit = PITStore(engine)
    cd = ClusterDiscovery(engine, pit)
    summary = cd.run_cluster_discovery(n_components=5)
    print(f"Best k: {summary.get('best_k')}")
