"""
GRID feature registry query interface.

Provides read access to the ``feature_registry`` table with convenience
methods for querying features by family, eligibility, and name.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class FeatureRegistry:
    """Query interface for the feature_registry table.

    Provides methods to look up features by name, family, or eligibility
    status.  Does not handle writes — those are managed via schema.sql
    migrations and seed data.

    Attributes:
        engine: SQLAlchemy engine for database queries.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the feature registry interface.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        log.info("FeatureRegistry initialised")

    def get_all(self) -> pd.DataFrame:
        """Return all features in the registry.

        Returns:
            pd.DataFrame: Full feature registry contents.
        """
        with self.engine.connect() as conn:
            df = pd.read_sql(
                text("SELECT * FROM feature_registry ORDER BY family, name"),
                conn,
            )
        log.info("Loaded {n} features from registry", n=len(df))
        return df

    def get_eligible(self, as_of_date: date | None = None) -> pd.DataFrame:
        """Return all model-eligible features.

        Parameters:
            as_of_date: If provided, only return features with
                        eligible_from_date <= as_of_date and no
                        deprecated_at date.

        Returns:
            pd.DataFrame: Eligible features.
        """
        query = "SELECT * FROM feature_registry WHERE model_eligible = TRUE"
        params: dict[str, Any] = {}

        if as_of_date is not None:
            query += " AND eligible_from_date <= :aod"
            query += " AND (deprecated_at IS NULL OR deprecated_at > :aod)"
            params["aod"] = as_of_date

        query += " ORDER BY family, name"

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        log.info("Found {n} eligible features", n=len(df))
        return df

    def get_by_family(self, family: str) -> pd.DataFrame:
        """Return all features in a given family.

        Parameters:
            family: Feature family name (e.g. 'rates', 'credit', 'vol').

        Returns:
            pd.DataFrame: Features matching the family.
        """
        with self.engine.connect() as conn:
            df = pd.read_sql(
                text("SELECT * FROM feature_registry WHERE family = :f ORDER BY name"),
                conn,
                params={"f": family},
            )
        log.info("Found {n} features in family '{f}'", n=len(df), f=family)
        return df

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        """Look up a single feature by name.

        Parameters:
            name: Feature name.

        Returns:
            dict: Feature record as a dictionary, or None if not found.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM feature_registry WHERE name = :name"),
                {"name": name},
            ).fetchone()

        if row is None:
            return None
        return dict(row._mapping)

    def get_feature_ids(self, names: list[str]) -> list[int]:
        """Look up feature_registry IDs for a list of feature names.

        Parameters:
            names: List of feature names.

        Returns:
            list[int]: Corresponding feature IDs (in the same order as names).
                       Missing names are excluded.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
                {"names": names},
            ).fetchall()

        name_to_id = {row[1]: row[0] for row in rows}
        result = [name_to_id[n] for n in names if n in name_to_id]

        if len(result) < len(names):
            missing = [n for n in names if n not in name_to_id]
            log.warning("Features not found in registry: {m}", m=missing)

        return result

    def list_families(self) -> list[str]:
        """Return a sorted list of all feature families.

        Returns:
            list[str]: Unique family names.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT DISTINCT family FROM feature_registry ORDER BY family")
            ).fetchall()
        return [row[0] for row in rows]


if __name__ == "__main__":
    from db import get_engine

    reg = FeatureRegistry(db_engine=get_engine())
    print("Feature families:", reg.list_families())
    print(f"\nTotal features: {len(reg.get_all())}")
    print(f"Model-eligible: {len(reg.get_eligible())}")

    for family in reg.list_families():
        features = reg.get_by_family(family)
        print(f"\n{family} ({len(features)} features):")
        for _, f in features.iterrows():
            print(f"  {f['name']:30s} eligible={f['model_eligible']}")
