"""
GRID orthogonality audit module.

Performs comprehensive analysis of feature redundancy and dimensionality:
correlation heatmaps, PCA decomposition, scree plots, and rolling
correlation stability analysis.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server use

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from loguru import logger as log
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


class OrthogonalityAudit:
    """Comprehensive orthogonality and dimensionality audit for GRID features.

    Analyses the full feature set for redundancy, correlation stability,
    and true dimensionality via PCA.

    Attributes:
        engine: SQLAlchemy engine for database access.
        pit_store: PITStore for point-in-time data retrieval.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the orthogonality audit.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: PITStore instance for point-in-time data access.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("OrthogonalityAudit initialised")

    def _get_eligible_feature_ids(self) -> list[int]:
        """Retrieve all model-eligible feature IDs from the registry.

        Returns:
            list[int]: Feature IDs where model_eligible = TRUE.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id FROM feature_registry WHERE model_eligible = TRUE ORDER BY id")
            ).fetchall()
        return [row[0] for row in rows]

    def _get_feature_names(self, feature_ids: list[int]) -> dict[int, str]:
        """Map feature IDs to their names.

        Parameters:
            feature_ids: List of feature registry IDs.

        Returns:
            dict: Mapping of feature_id -> feature_name.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE id = ANY(:ids)"),
                {"ids": feature_ids},
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def run_full_audit(
        self,
        as_of_date: date | None = None,
        output_dir: str = "outputs/orthogonality",
    ) -> dict[str, Any]:
        """Run the complete orthogonality audit.

        Produces correlation heatmaps, PCA analysis, scree plots, and
        rolling correlation stability analysis.  All outputs are saved
        to ``output_dir`` with timestamps in filenames.

        Parameters:
            as_of_date: Decision date for PIT queries (default: today).
            output_dir: Directory for saving output files.

        Returns:
            dict: Summary with keys including n_features_analyzed,
                  true_dimensionality, highly_correlated_pairs, etc.
        """
        if as_of_date is None:
            as_of_date = date.today()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        log.info("Starting orthogonality audit — as_of={d}, output={o}", d=as_of_date, o=output_dir)

        # Step a: Load feature matrix
        feature_ids = self._get_eligible_feature_ids()
        if not feature_ids:
            log.warning("No model-eligible features found")
            return {"n_features_analyzed": 0, "error": "No eligible features"}

        feature_names = self._get_feature_names(feature_ids)
        matrix = self.pit_store.get_feature_matrix(
            feature_ids=feature_ids,
            start_date=date(2024, 4, 1),
            end_date=as_of_date,
            as_of_date=as_of_date,
            vintage_policy="FIRST_RELEASE",
        )

        if matrix.empty:
            log.warning("Feature matrix is empty — no data to audit")
            return {"n_features_analyzed": 0, "error": "Empty feature matrix"}

        # Rename columns to feature names
        matrix.columns = [feature_names.get(c, str(c)) for c in matrix.columns]

        # Forward-fill first (weekends/holidays/monthly series carry forward)
        matrix = matrix.ffill().bfill()
        # Step b: Drop features with > 30% missing values
        missing_pct = matrix.isnull().mean()
        dropped = missing_pct[missing_pct > 0.5].index.tolist()
        if dropped:
            log.warning("Dropping {n} features with >30% missing: {f}", n=len(dropped), f=dropped)
            matrix = matrix.drop(columns=dropped)

        if matrix.empty or matrix.shape[1] < 2:
            log.warning("Insufficient features after dropping — need at least 2")
            return {"n_features_analyzed": matrix.shape[1], "n_features_dropped": len(dropped)}

        # Step c: Forward-fill NaNs up to 5 trading days, log larger gaps
        for col in matrix.columns:
            gaps = matrix[col].isnull()
            if gaps.any():
                # Find gap lengths
                gap_groups = (~gaps).cumsum()
                gap_lengths = gaps.groupby(gap_groups).transform("sum")
                long_gaps = (gap_lengths > 5).any()
                if long_gaps:
                    log.warning("Feature '{c}' has gaps longer than 5 days", c=col)

        matrix = matrix.ffill(limit=5)
        # Drop remaining NaN rows
        matrix = matrix.dropna()

        n_features = matrix.shape[1]
        log.info("Audit matrix: {r} rows x {c} features", r=matrix.shape[0], c=n_features)

        # Step d: Correlation analysis
        corr_matrix = matrix.corr()

        # Full-period heatmap
        self._save_heatmap(
            corr_matrix,
            f"Full Period Correlation ({matrix.index.min().date()} to {as_of_date})",
            out_path / f"corr_full_{timestamp}.png",
        )

        # Pre-2008 subperiod
        pre_2008 = matrix[matrix.index < "2008-01-01"]
        if len(pre_2008) > 30:
            corr_pre = pre_2008.corr()
            self._save_heatmap(
                corr_pre,
                "Pre-2008 Correlation (1990–2007)",
                out_path / f"corr_pre2008_{timestamp}.png",
            )

        # Post-2015 subperiod
        post_2015 = matrix[matrix.index >= "2015-01-01"]
        if len(post_2015) > 30:
            corr_post = post_2015.corr()
            self._save_heatmap(
                corr_post,
                "Post-2015 Correlation",
                out_path / f"corr_post2015_{timestamp}.png",
            )

        # Save correlation CSV
        corr_matrix.to_csv(out_path / f"correlation_matrix_{timestamp}.csv")

        # Optional: Hyperspace semantic similarity analysis
        feature_name_list = list(matrix.columns)
        try:
            from hyperspace.client import get_client
            from hyperspace.embeddings import GRIDEmbeddings

            hs_client = get_client()
            if hs_client.is_available:
                embedder = GRIDEmbeddings(hs_client)
                sem_sim = embedder.semantic_similarity_matrix(
                    feature_name_list, self.engine
                )
                if sem_sim is not None:
                    # Flag cases where statistical correlation is low but
                    # semantic similarity is high — "hidden redundancies"
                    for i, fa in enumerate(feature_name_list):
                        for j, fb in enumerate(feature_name_list[i + 1 :], i + 1):
                            stat_corr = abs(corr_matrix.loc[fa, fb])
                            sem_score = sem_sim.loc[fa, fb]
                            if stat_corr < 0.3 and sem_score > 0.75:
                                log.warning(
                                    "Hidden redundancy: {fa} x {fb} | "
                                    "stat_corr={sc:.2f} sem_sim={ss:.2f}",
                                    fa=fa,
                                    fb=fb,
                                    sc=stat_corr,
                                    ss=sem_score,
                                )
                    sem_sim.to_csv(
                        out_path / f"semantic_similarity_{timestamp}.csv"
                    )
                    log.info("Semantic similarity matrix saved")
        except Exception as exc:
            log.debug(
                "Hyperspace semantic similarity skipped: {e}", e=str(exc)
            )

        # Find highly correlated pairs (|corr| > 0.8)
        highly_correlated: list[tuple[str, str, float]] = []
        cols = corr_matrix.columns
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                c = corr_matrix.iloc[i, j]
                if abs(c) > 0.8:
                    highly_correlated.append((cols[i], cols[j], round(c, 4)))
        highly_correlated.sort(key=lambda x: abs(x[2]), reverse=True)

        # Step e: PCA analysis
        scaler = StandardScaler()
        scaled = scaler.fit_transform(matrix)
        pca = PCA()
        pca.fit(scaled)

        explained = pca.explained_variance_ratio_
        cumulative = np.cumsum(explained)
        true_dim = int(np.searchsorted(cumulative, 0.85) + 1)

        # Scree plot
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        ax1.bar(range(1, len(explained) + 1), explained, alpha=0.7, label="Individual")
        ax1.plot(range(1, len(cumulative) + 1), cumulative, "r-o", markersize=4, label="Cumulative")
        ax1.axhline(y=0.85, color="g", linestyle="--", label="85% threshold")
        ax1.axvline(x=true_dim, color="orange", linestyle="--", label=f"True dim = {true_dim}")
        ax1.set_xlabel("Component")
        ax1.set_ylabel("Variance Explained")
        ax1.set_title("PCA Scree Plot")
        ax1.legend()

        # Factor loadings for top components
        loadings = pd.DataFrame(
            pca.components_[:min(true_dim, 5)].T,
            index=matrix.columns,
            columns=[f"PC{i+1}" for i in range(min(true_dim, 5))],
        )
        sns.heatmap(loadings, ax=ax2, cmap="RdBu_r", center=0, annot=True, fmt=".2f")
        ax2.set_title(f"Factor Loadings (Top {min(true_dim, 5)} Components)")

        fig.tight_layout()
        fig.savefig(out_path / f"pca_scree_{timestamp}.png", dpi=150, bbox_inches="tight")
        plt.close(fig)

        # Save loadings CSV
        loadings_full = pd.DataFrame(
            pca.components_.T,
            index=matrix.columns,
            columns=[f"PC{i+1}" for i in range(len(explained))],
        )
        loadings_full.to_csv(out_path / f"factor_loadings_{timestamp}.csv")

        # Dominant factor loadings
        dominant_loadings: dict[str, list[str]] = {}
        for pc_col in loadings.columns:
            top_features = loadings[pc_col].abs().nlargest(3).index.tolist()
            dominant_loadings[pc_col] = top_features

        # Step f: Rolling correlation stability
        unstable_pairs: list[tuple[str, str, float, float]] = []
        rolling_window = 252  # 1 year

        if matrix.shape[0] > rolling_window:
            rolling_summary: list[dict[str, Any]] = []

            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    rolling_corr = matrix[cols[i]].rolling(rolling_window).corr(matrix[cols[j]])
                    rc_clean = rolling_corr.dropna()
                    if len(rc_clean) > 0:
                        max_corr = float(rc_clean.max())
                        min_corr = float(rc_clean.min())
                        if max_corr - min_corr > 0.6:
                            unstable_pairs.append((cols[i], cols[j], max_corr, min_corr))
                        rolling_summary.append({
                            "feature_a": cols[i],
                            "feature_b": cols[j],
                            "mean_corr": round(float(rc_clean.mean()), 4),
                            "max_corr": round(max_corr, 4),
                            "min_corr": round(min_corr, 4),
                            "range": round(max_corr - min_corr, 4),
                        })

            pd.DataFrame(rolling_summary).to_csv(
                out_path / f"rolling_correlation_summary_{timestamp}.csv",
                index=False,
            )

        # Step g: Summary
        summary: dict[str, Any] = {
            "n_features_analyzed": n_features,
            "n_features_dropped": len(dropped),
            "true_dimensionality": true_dim,
            "variance_explained_by_true_dim": round(float(cumulative[true_dim - 1]), 4),
            "highly_correlated_pairs": highly_correlated,
            "unstable_pairs": unstable_pairs,
            "dominant_factor_loadings": dominant_loadings,
        }

        log.info(
            "Orthogonality audit complete — {n} features, true_dim={d}, "
            "{hc} highly correlated pairs, {up} unstable pairs",
            n=n_features,
            d=true_dim,
            hc=len(highly_correlated),
            up=len(unstable_pairs),
        )

        # Print summary
        print("\n=== ORTHOGONALITY AUDIT SUMMARY ===")
        print(f"Features analysed:    {n_features}")
        print(f"Features dropped:     {len(dropped)}")
        print(f"True dimensionality:  {true_dim}")
        print(f"Variance at true dim: {summary['variance_explained_by_true_dim']:.1%}")
        if highly_correlated:
            print(f"\nHighly correlated pairs ({len(highly_correlated)}):")
            for a, b, c in highly_correlated[:10]:
                print(f"  {a} <-> {b}: {c:.4f}")
        if unstable_pairs:
            print(f"\nUnstable pairs ({len(unstable_pairs)}):")
            for a, b, mx, mn in unstable_pairs[:10]:
                print(f"  {a} <-> {b}: [{mn:.4f}, {mx:.4f}]")

        return summary

    def _save_heatmap(
        self,
        corr: pd.DataFrame,
        title: str,
        filepath: Path,
    ) -> None:
        """Save a correlation heatmap as a PNG file.

        Parameters:
            corr: Correlation matrix DataFrame.
            title: Plot title.
            filepath: Output file path.
        """
        n = len(corr)
        figsize = (max(10, n * 0.6), max(8, n * 0.5))
        fig, ax = plt.subplots(figsize=figsize)

        mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
        sns.heatmap(
            corr,
            mask=mask,
            ax=ax,
            cmap="RdBu_r",
            center=0,
            vmin=-1,
            vmax=1,
            annot=n <= 15,
            fmt=".2f" if n <= 15 else "",
            square=True,
            linewidths=0.5,
        )
        ax.set_title(title, fontsize=14)
        fig.tight_layout()
        fig.savefig(filepath, dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("Heatmap saved to {p}", p=filepath)


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    pit = PITStore(engine)
    audit = OrthogonalityAudit(engine, pit)
    summary = audit.run_full_audit()
