# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID semantic embedding layer.

Uses the Hyperspace node's local embedding model to compute semantic
similarity between feature descriptions, hypothesis statements, and
regime narratives.  All inputs are public concept descriptions — never
raw market data or discovered signal logic.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text

from hyperspace.client import HyperspaceClient


class GRIDEmbeddings:
    """Semantic embedding interface for GRID feature and hypothesis analysis.

    Wraps the Hyperspace embedding endpoint to provide:
    - Feature-description embeddings for semantic similarity
    - Hypothesis deduplication via cosine similarity
    - Natural-language feature search

    All methods return ``None`` when Hyperspace is unavailable.

    Attributes:
        client: HyperspaceClient instance.
    """

    def __init__(self, hyperspace_client: HyperspaceClient) -> None:
        """Initialise the embedding layer.

        Parameters:
            hyperspace_client: A connected HyperspaceClient.
        """
        self.client = hyperspace_client
        log.info("GRIDEmbeddings initialised — available={a}", a=self.client.is_available)

    def embed_features(
        self,
        feature_names: list[str],
        db_engine: Any = None,
    ) -> pd.DataFrame | None:
        """Embed feature registry entries by their public descriptions.

        For each feature name, constructs a descriptive text from the
        feature_registry (name + family + description + transformation)
        and embeds the batch in one call.

        Parameters:
            feature_names: List of feature_registry name strings.
            db_engine: Optional SQLAlchemy engine.  If provided, enriches
                       the text with registry metadata.

        Returns:
            pd.DataFrame: Feature names as index, embedding dimensions as
                columns.  Returns ``None`` if Hyperspace is unavailable.
        """
        if not self.client.is_available:
            return None

        texts = self._build_feature_texts(feature_names, db_engine)
        if not texts:
            return None

        vectors = self.client.embed(texts)
        if vectors is None:
            return None

        df = pd.DataFrame(vectors, index=feature_names)
        log.info("Embedded {n} features — dim={d}", n=len(df), d=df.shape[1])
        return df

    def _build_feature_texts(
        self,
        feature_names: list[str],
        db_engine: Any = None,
    ) -> list[str]:
        """Construct descriptive texts for a list of features.

        Parameters:
            feature_names: Feature names to describe.
            db_engine: Optional engine for registry lookup.

        Returns:
            list[str]: One description string per feature.
        """
        if db_engine is not None:
            try:
                with db_engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT name, family, description, transformation "
                            "FROM feature_registry WHERE name = ANY(:names)"
                        ),
                        {"names": feature_names},
                    ).fetchall()
                registry = {
                    r[0]: {
                        "family": r[1],
                        "description": r[2],
                        "transformation": r[3],
                    }
                    for r in rows
                }
            except Exception as exc:
                log.warning("Could not load feature registry: {e}", e=str(exc))
                registry = {}
        else:
            registry = {}

        texts: list[str] = []
        for name in feature_names:
            info = registry.get(name)
            if info:
                text_str = (
                    f"{name} ({info['family']}): {info['description']}. "
                    f"Transformation: {info['transformation']}"
                )
            else:
                # Fallback: use the name itself with underscores → spaces
                text_str = name.replace("_", " ")
            texts.append(text_str)

        return texts

    def semantic_similarity_matrix(
        self,
        feature_names: list[str],
        db_engine: Any = None,
    ) -> pd.DataFrame | None:
        """Compute pairwise cosine similarity between feature embeddings.

        This is a semantic complement to the statistical correlation matrix
        in the orthogonality audit.  Two features can be statistically
        uncorrelated but semantically similar (same concept, different
        measurement), which is useful to flag.

        Parameters:
            feature_names: List of feature names to compare.
            db_engine: Optional engine for registry enrichment.

        Returns:
            pd.DataFrame: Square similarity matrix (feature × feature),
                or ``None`` if unavailable.
        """
        embed_df = self.embed_features(feature_names, db_engine)
        if embed_df is None:
            return None

        # Cosine similarity
        norms = np.linalg.norm(embed_df.values, axis=1, keepdims=True)
        norms[norms == 0] = 1.0  # avoid division by zero
        normalised = embed_df.values / norms
        sim_matrix = normalised @ normalised.T

        result = pd.DataFrame(
            sim_matrix,
            index=feature_names,
            columns=feature_names,
        )
        log.info("Semantic similarity matrix computed — {n}×{n}", n=len(feature_names))
        return result

    def find_similar_features(
        self,
        query: str,
        feature_names: list[str],
        top_k: int = 5,
        db_engine: Any = None,
    ) -> list[tuple[str, float]]:
        """Find features most semantically similar to a natural-language query.

        Parameters:
            query: Natural-language search string (e.g. "credit stress indicators").
            feature_names: Candidate feature names to search among.
            top_k: Number of top results to return.
            db_engine: Optional engine for registry enrichment.

        Returns:
            list[tuple[str, float]]: Up to ``top_k`` (feature_name, similarity)
                pairs sorted descending.  Empty list if unavailable.
        """
        if not self.client.is_available:
            return []

        # Embed the query
        query_vec = self.client.embed([query])
        if query_vec is None:
            return []

        # Embed all features
        feature_df = self.embed_features(feature_names, db_engine)
        if feature_df is None:
            return []

        # Cosine similarity between query and each feature
        q = np.array(query_vec[0])
        q_norm = np.linalg.norm(q)
        if q_norm == 0:
            return []
        q = q / q_norm

        scores: list[tuple[str, float]] = []
        for name in feature_names:
            if name not in feature_df.index:
                continue
            f_vec = feature_df.loc[name].values
            f_norm = np.linalg.norm(f_vec)
            if f_norm == 0:
                continue
            sim = float(np.dot(q, f_vec / f_norm))
            scores.append((name, round(sim, 4)))

        scores.sort(key=lambda x: x[1], reverse=True)
        result = scores[:top_k]
        log.info(
            "Feature search for '{q}' — top match: {t}",
            q=query[:50],
            t=result[0] if result else "none",
        )
        return result

    def embed_hypothesis(self, statement: str) -> list[float] | None:
        """Embed a single hypothesis statement.

        Parameters:
            statement: The hypothesis text.

        Returns:
            list[float]: Embedding vector, or ``None`` if unavailable.
        """
        if not self.client.is_available:
            return None

        vectors = self.client.embed([statement])
        if vectors is None or len(vectors) == 0:
            return None
        return vectors[0]

    def hypothesis_dedup_check(
        self,
        new_statement: str,
        existing_statements: list[str],
        threshold: float = 0.92,
    ) -> tuple[bool, str | None]:
        """Check whether a new hypothesis is a near-duplicate of an existing one.

        Parameters:
            new_statement: The candidate hypothesis text.
            existing_statements: List of existing hypothesis texts.
            threshold: Cosine similarity threshold above which a pair is
                       considered duplicate (default 0.92).

        Returns:
            tuple: (is_duplicate, most_similar_existing_statement).
                ``is_duplicate`` is True if any existing statement exceeds
                the threshold.  ``most_similar_existing_statement`` is the
                closest match (or None if no embeddings available).
        """
        if not self.client.is_available or not existing_statements:
            return (False, None)

        all_texts = [new_statement] + existing_statements
        vectors = self.client.embed(all_texts)
        if vectors is None or len(vectors) < 2:
            return (False, None)

        new_vec = np.array(vectors[0])
        new_norm = np.linalg.norm(new_vec)
        if new_norm == 0:
            return (False, None)
        new_vec = new_vec / new_norm

        best_sim = -1.0
        best_match: str | None = None

        for i, existing in enumerate(existing_statements):
            ex_vec = np.array(vectors[i + 1])
            ex_norm = np.linalg.norm(ex_vec)
            if ex_norm == 0:
                continue
            sim = float(np.dot(new_vec, ex_vec / ex_norm))
            if sim > best_sim:
                best_sim = sim
                best_match = existing

        is_dup = best_sim >= threshold
        log.debug(
            "Hypothesis dedup — best_sim={s:.4f}, threshold={t}, dup={d}",
            s=best_sim,
            t=threshold,
            d=is_dup,
        )
        return (is_dup, best_match)


if __name__ == "__main__":
    from hyperspace.client import get_client

    client = get_client()
    embedder = GRIDEmbeddings(client)

    sample_features = [
        "yld_curve_2s10s",
        "hy_spread_proxy",
        "vix_spot",
        "copper_gold_ratio",
    ]

    sim = embedder.semantic_similarity_matrix(sample_features)
    if sim is not None:
        print("Semantic similarity matrix:")
        print(sim.to_string(float_format="{:.3f}".format))
    else:
        print("Hyperspace unavailable — no embeddings computed")
