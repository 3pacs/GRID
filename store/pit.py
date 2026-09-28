"""
GRID Point-in-Time (PIT) query engine.

The most critical correctness component in the entire system. Enforces strict
no-lookahead constraints ensuring that no data with ``release_date > as_of_date``
is ever returned.  Supports both FIRST_RELEASE and LATEST_AS_OF vintage policies
for backtesting and live inference.

Retractions (``resolved_series_retractions``,
migrations/versions/resolved_retractions_20260927.py): a resolved row that was
found to hold a wrong value with no clean replacement is retracted by its key
``(feature_id, obs_date, vintage_date)`` at ``retracted_at``. Every query here
excludes it when ``as_of`` is on or after the retraction (for a date ``as_of``:
the retraction happened by the end of that UTC day, the same day-level
convention as ``release_date <= as_of``). Because the comparison is at day
level, a retraction at 15:00 UTC on day D already hides the row for
``as_of = D``, including for a read made earlier that day. Replays are
reproduced exactly only for ``as_of`` dates *before* the retraction's UTC
date; ``as_of = D`` can differ depending on when it ran (the resolver's own
same-day vintages behave the same way).
The vintage policy then picks among the remaining vintages; a cell with none
left returns no row -- never a zero and never another feature's value.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from typing import Generator

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


def retraction_cutoff(as_of: date | datetime) -> datetime:
    """Inclusive ``retracted_at`` cutoff for a point-in-time read.

    A row retracted at ``retracted_at`` is hidden from a read when
    ``retracted_at <= retraction_cutoff(as_of)``.

    * ``datetime`` (``as_of_ts``): the instant itself. A naive value is taken
      as UTC (the database time zone is ``Etc/UTC``).
    * ``date``: the last microsecond of that UTC day -- "known by the end of
      the day", matching ``release_date <= as_of``.

    Pass the result as a bind parameter; never interpolate it into SQL.
    """
    if isinstance(as_of, datetime):
        return as_of if as_of.tzinfo is not None else as_of.replace(tzinfo=timezone.utc)
    return datetime.combine(as_of, time.max, tzinfo=timezone.utc)


def retractions_table_exists(engine: Engine) -> bool:
    """True if ``resolved_series_retractions`` exists (PR #683's migration).

    A caller that writes derived rows into ``resolved_series`` from inputs
    read across *all* vintages (no PIT, no retraction filter) can silently
    compute from a contaminated or since-retracted vintage that is still
    kept as superseded history -- see
    ``GRID-RERESOLVE-PLAN-20260927``, "DERIVED FEATURES". Any writer that
    switches its reads to ``get_pit(..., "LATEST_AS_OF")`` should call this
    first and refuse to run entirely when it returns ``False``, rather than
    silently falling back to an unfiltered read.

    Runs on its own connection so a failure here (missing table, or any
    other error) can never poison a caller's outer transaction; any
    failure is treated as "table absent". Unqualified (relies on
    ``search_path``), matching how every other query against
    ``resolved_series_retractions`` in this codebase addresses it -- a
    hardcoded ``public.`` prefix would silently miss the table on a
    non-default search_path (e.g. a test's scratch schema).
    """
    try:
        with engine.connect() as conn:
            with conn.begin():
                return bool(
                    conn.execute(
                        text("SELECT to_regclass(:table_name) IS NOT NULL"),
                        {"table_name": "resolved_series_retractions"},
                    ).scalar()
                )
    except Exception:
        return False


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
        HARD CONSTRAINT 4: rows retracted in resolved_series_retractions by
            the end of as_of_date are excluded *before* the vintage policy
            picks a row. as_of dates before the retraction's UTC date
            still see them (a retraction at any time on day D hides the row
            for as_of = D). A cell whose every vintage is retracted returns
            no row.

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
                SELECT DISTINCT ON (rs.feature_id, rs.obs_date)
                    rs.feature_id, rs.obs_date, rs.value, rs.release_date, rs.vintage_date
                FROM resolved_series rs
                WHERE rs.feature_id = ANY(:fids)
                  AND rs.obs_date <= :aod
                  AND rs.release_date <= :aod
                  AND NOT EXISTS (
                      SELECT 1 FROM resolved_series_retractions rr
                      WHERE rr.feature_id = rs.feature_id
                        AND rr.obs_date = rs.obs_date
                        AND rr.vintage_date = rs.vintage_date
                        AND rr.retracted_at <= :retraction_cutoff
                  )
                ORDER BY rs.feature_id, rs.obs_date, rs.vintage_date ASC
            """)
        else:
            # LATEST_AS_OF: for each (feature_id, obs_date), return the row
            # with the MAXIMUM vintage_date where release_date <= as_of_date.
            query = text("""
                SELECT DISTINCT ON (rs.feature_id, rs.obs_date)
                    rs.feature_id, rs.obs_date, rs.value, rs.release_date, rs.vintage_date
                FROM resolved_series rs
                WHERE rs.feature_id = ANY(:fids)
                  AND rs.obs_date <= :aod
                  AND rs.release_date <= :aod
                  AND NOT EXISTS (
                      SELECT 1 FROM resolved_series_retractions rr
                      WHERE rr.feature_id = rs.feature_id
                        AND rr.obs_date = rs.obs_date
                        AND rr.vintage_date = rs.vintage_date
                        AND rr.retracted_at <= :retraction_cutoff
                  )
                ORDER BY rs.feature_id, rs.obs_date, rs.vintage_date DESC
            """)

        with self.engine.connect() as conn:
            rows = conn.execute(
                query,
                {
                    "fids": feature_ids,
                    "aod": as_of_date,
                    "retraction_cutoff": retraction_cutoff(as_of_date),
                },
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

        # Safety cap: prevent unbounded loads for very wide date ranges.
        max_years = int(os.getenv("GRID_PIT_MAX_YEARS", "10"))
        date_range_years = (end_date - start_date).days / 365.25
        if date_range_years > max_years:
            capped_start = date(end_date.year - max_years, end_date.month, end_date.day)
            log.warning(
                "Feature matrix date range {sd} to {ed} ({y:.1f} years) exceeds "
                "GRID_PIT_MAX_YEARS={cap}. Truncating start_date to {capped}.",
                sd=start_date,
                ed=end_date,
                y=date_range_years,
                cap=max_years,
                capped=capped_start,
            )
            start_date = capped_start

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

    @contextmanager
    def safe_inference_context(
        self,
        feature_ids: list[int],
        as_of_date: date,
        vintage_policy: str = "LATEST_AS_OF",
    ) -> Generator[tuple[pd.DataFrame, Connection], None, None]:
        """Context manager that provides PIT data inside a transaction.

        If ``assert_no_lookahead`` fails, the transaction is rolled back
        before the ValueError propagates.  This prevents partial inference
        results from persisting in the database.

        Usage::

            with pit_store.safe_inference_context(fids, as_of) as (df, conn):
                # Use df for inference, write results via conn
                conn.execute(text("INSERT INTO ..."), {...})
            # Auto-committed if no exception; rolled back on lookahead violation

        Parameters:
            feature_ids: List of feature_registry IDs.
            as_of_date: Decision date for PIT filtering.
            vintage_policy: 'FIRST_RELEASE' or 'LATEST_AS_OF'.

        Yields:
            (DataFrame, Connection): The PIT data and an active transactional
            connection. Write inference results using this connection.

        Raises:
            ValueError: If lookahead violation detected (transaction is
                        rolled back before the exception propagates).
        """
        pit_df = self.get_pit(feature_ids, as_of_date, vintage_policy)

        with self.engine.begin() as conn:
            try:
                yield pit_df, conn
            except ValueError:
                # Lookahead or validation error — rollback is automatic
                # because engine.begin() rolls back on exception
                raise

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
