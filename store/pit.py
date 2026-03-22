"""
GRID Point-in-Time (PIT) query engine.

The most critical correctness component in the entire system. Enforces strict
no-lookahead constraints ensuring that no data with ``release_date > as_of_date``
is ever returned.  Supports both FIRST_RELEASE and LATEST_AS_OF vintage policies
for backtesting and live inference.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class PITStore:
    """Point-in-time query engine for resolved_series.

    Guarantees that every value returned was available at the specified
    ``as_of_date``, preventing any form of look-ahead bias in backtests
    and live inference.

    Attributes:
        engine: SQLAlchemy engine for database queries.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the PIT store.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        log.info("PITStore initialised")

    def get_pit(
        self,
        feature_ids: list[int],
        as_of_date: date,
        vintage_policy: str = "LATEST_AS_OF",
    ) -> pd.DataFrame:
        """Return point-in-time correct data for the given features and date.

        HARD CONSTRAINT 1: Every returned row has release_date <= as_of_date.
        HARD CONSTRAINT 2: Every returned row has obs_date <= as_of_date.
        HARD CONSTRAINT 3:
            FIRST_RELEASE  — earliest vintage_date per (feature_id, obs_date).
            LATEST_AS_OF   — latest vintage_date per (feature_id, obs_date)
                             where release_date <= as_of_date.

        Parameters:
            feature_ids: List of feature_registry IDs to query.
            as_of_date: Decision date. No data released after this date is
                        included.
            vintage_policy: Either 'FIRST_RELEASE' or 'LATEST_AS_OF'.

        Returns:
            pd.DataFrame: Columns [feature_id, obs_date, value, release_date,
                          vintage_date].

        Raises:
            ValueError: If vintage_policy is not valid.
            ValueError: If any returned row violates the no-lookahead constraint
                        (safety net via assert_no_lookahead).
        """
        if vintage_policy not in ("FIRST_RELEASE", "LATEST_AS_OF"):
            raise ValueError(
                f"Invalid vintage_policy '{vintage_policy}'. "
                "Must be 'FIRST_RELEASE' or 'LATEST_AS_OF'."
            )

        if not feature_ids:
            log.warning("get_pit called with empty feature_ids list")
            return pd.DataFrame(
                columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
            )

        log.debug(
            "PIT query — {n} features, as_of={d}, policy={p}",
            n=len(feature_ids),
            d=as_of_date,
            p=vintage_policy,
        )

        if vintage_policy == "FIRST_RELEASE":
            # For each (feature_id, obs_date), return the row with the
            # MINIMUM vintage_date, provided release_date <= as_of_date.
            query = text("""
                SELECT DISTINCT ON (feature_id, obs_date)
                    feature_id, obs_date, value, release_date, vintage_date
                FROM resolved_series
                WHERE feature_id = ANY(:fids)
                  AND obs_date <= :aod
                  AND release_date <= :aod
                ORDER BY feature_id, obs_date, vintage_date ASC
            """)
        else:
            # LATEST_AS_OF: for each (feature_id, obs_date), return the row
            # with the MAXIMUM vintage_date where release_date <= as_of_date.
            query = text("""
                SELECT DISTINCT ON (feature_id, obs_date)
                    feature_id, obs_date, value, release_date, vintage_date
                FROM resolved_series
                WHERE feature_id = ANY(:fids)
                  AND obs_date <= :aod
                  AND release_date <= :aod
                ORDER BY feature_id, obs_date, vintage_date DESC
            """)

        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {"fids": feature_ids, "aod": as_of_date},
            ).fetchall()

        df = pd.DataFrame(
            rows,
            columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"],
        )

        # Safety net: verify no lookahead
        self.assert_no_lookahead(df, as_of_date)

        log.debug("PIT query returned {n} rows", n=len(df))
        return df

    def get_feature_matrix(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        as_of_date: date,
        vintage_policy: str = "FIRST_RELEASE",
    ) -> pd.DataFrame:
        """Return a wide feature matrix for backtesting.

        Produces a DataFrame with obs_date as the index and feature_id as
        column headers.  Default vintage policy is FIRST_RELEASE for
        backtest correctness.

        Parameters:
            feature_ids: List of feature_registry IDs.
            start_date: First observation date to include.
            end_date: Last observation date to include.
            as_of_date: Decision date for PIT filtering.
            vintage_policy: 'FIRST_RELEASE' (default) or 'LATEST_AS_OF'.

        Returns:
            pd.DataFrame: Wide-format DataFrame indexed by obs_date.
        """
        log.info(
            "Building feature matrix — {n} features, {sd} to {ed}, as_of={aod}",
            n=len(feature_ids),
            sd=start_date,
            ed=end_date,
            aod=as_of_date,
        )

        pit_df = self.get_pit(feature_ids, as_of_date, vintage_policy)

        # Filter to date range
        pit_df = pit_df[
            (pit_df["obs_date"] >= start_date) & (pit_df["obs_date"] <= end_date)
        ]

        if pit_df.empty:
            log.warning("Feature matrix is empty after date filtering")
            return pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))

        # Pivot to wide format
        matrix = pit_df.pivot_table(
            index="obs_date",
            columns="feature_id",
            values="value",
            aggfunc="first",
        )
        matrix.index = pd.DatetimeIndex(matrix.index, name="obs_date")
        matrix = matrix.sort_index()

        log.info(
            "Feature matrix built — shape {r}x{c}",
            r=matrix.shape[0],
            c=matrix.shape[1],
        )
        return matrix

    def assert_no_lookahead(self, df: pd.DataFrame, as_of_date: date) -> None:
        """Verify that no row in the DataFrame has release_date > as_of_date.

        This is a safety net called automatically by ``get_pit`` before
        returning results. Raises immediately if any violation is found.

        Parameters:
            df: DataFrame with a 'release_date' column.
            as_of_date: The decision date.

        Raises:
            ValueError: If any row has release_date > as_of_date, with a
                        message identifying the violating rows.
        """
        if df.empty or "release_date" not in df.columns:
            return

        violations = df[df["release_date"] > as_of_date]
        if not violations.empty:
            detail = violations[["feature_id", "obs_date", "release_date"]].head(5).to_string()
            log.critical(
                "LOOKAHEAD VIOLATION detected — {n} rows with release_date > {d}",
                n=len(violations),
                d=as_of_date,
            )
            # Return empty DataFrame instead of partial results to prevent
            # any downstream use of tainted data
            df.drop(df.index, inplace=True)
            raise ValueError(
                f"LOOKAHEAD VIOLATION: {len(violations)} row(s) have "
                f"release_date > as_of_date ({as_of_date}).\n"
                f"First violations:\n{detail}"
            )

    def get_latest_values(self, feature_ids: list[int]) -> pd.DataFrame:
        """Return the single most recent value for each feature.

        Uses LATEST_AS_OF with ``as_of_date = today``. Intended for
        live inference.

        Parameters:
            feature_ids: List of feature_registry IDs.

        Returns:
            pd.DataFrame: One row per feature with the most recent value.
        """
        today = date.today()
        log.info(
            "Fetching latest values for {n} features (as_of={d})",
            n=len(feature_ids),
            d=today,
        )

        pit_df = self.get_pit(feature_ids, today, vintage_policy="LATEST_AS_OF")

        if pit_df.empty:
            return pit_df

        # Keep only the most recent obs_date per feature_id
        idx = pit_df.groupby("feature_id")["obs_date"].idxmax()
        latest = pit_df.loc[idx].reset_index(drop=True)

        log.info("Returning latest values for {n} features", n=len(latest))
        return latest


if __name__ == "__main__":
    from db import get_engine

    store = PITStore(db_engine=get_engine())

    # Quick test: fetch all features as of today
    with get_engine().connect() as conn:
        fids = [
            row[0]
            for row in conn.execute(
                text("SELECT id FROM feature_registry WHERE model_eligible = TRUE")
            ).fetchall()
        ]

    if fids:
        latest = store.get_latest_values(fids)
        print(f"Latest values for {len(fids)} features:")
        print(latest)
    else:
        print("No model-eligible features found in registry")
