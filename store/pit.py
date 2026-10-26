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
import math
import time as monotonic_time
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Generator

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from normalization.entity_map import SELECTED_QUERY_TIMEOUT_MS, validate_selected_request


def validate_source_snapshot_request(
    selections: list[dict] | None, *, as_of: datetime, start_date: date,
    vintage_policy: str = "LATEST_AS_OF",
) -> dict:
    """Pure pre-connection validation shared with the explicit stored-only API."""
    return validate_selected_request(selections, as_of=as_of, start_date=start_date,
                                     vintage_policy=vintage_policy)


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

    def get_source_snapshot(
        self, selections: list[dict] | None, *, as_of: datetime, start_date: date,
        vintage_policy: str = "LATEST_AS_OF",
    ) -> dict:
        """At most seven provenance-bearing, day-storage snapshots; no fallback.

        No engine is touched for missing bindings or unsupported event storage.
        Bound queries are read-only, have a shared five-second deadline, select
        registry entries exactly, and return at most one resolved row per source.
        This does not certify provider publication time, raw-row lineage, freshness
        or durability. In particular release/vintage dates have only day precision.
        """
        request = validate_source_snapshot_request(selections, as_of=as_of, start_date=start_date,
                                                  vintage_policy=vintage_policy)
        cutoff = request["as_of"]
        deadline = monotonic_time.monotonic() + SELECTED_QUERY_TIMEOUT_MS / 1000

        def remaining_timeout_ms() -> int:
            # Zero disables PostgreSQL statement_timeout. Refuse even a positive
            # sub-millisecond remainder rather than accidentally granting zero.
            remaining = int((deadline - monotonic_time.monotonic()) * 1000)
            if remaining <= 0:
                raise ValueError("snapshot deadline exceeded")
            return min(SELECTED_QUERY_TIMEOUT_MS, remaining)

        def prepare_query(conn: Connection) -> None:
            conn.execute(text("SELECT set_config('statement_timeout', :timeout_ms, true)"),
                         {"timeout_ms": str(remaining_timeout_ms())})
            # Configuration itself can consume the last budget. Do not start a
            # data query after detecting expiry, even if configuration succeeded.
            remaining_timeout_ms()

        output = {"schema": "stored-source-snapshot-v1", "as_of": cutoff.isoformat(),
                  "start_date": start_date.isoformat(), "vintage_policy": vintage_policy,
                  "selections": []}
        direction = "ASC" if vintage_policy == "FIRST_RELEASE" else "DESC"
        for selection in request["selections"]:
            row = {k: selection.get(k) for k in
                   ("source_key", "scope", "ticker_or_cusip", "series_id", "feature_id",
                    "feature_name", "expected_source_name")}
            row.update({"status": "unavailable", "reason": "selection_missing", "value": None,
                        "obs_date": None, "release_date": None, "vintage_date": None,
                        "actual_source": None, "conflict_flag": None, "conflict_detail": None,
                        "conflict_detail_truncated": False,
                        "freshness": {"status": "unknown", "reason": "max_no_observation"},
                        "storage_clock": "pull_date_not_provider_publication",
                        "lineage": "resolved_source_not_raw_row"})
            output["selections"].append(row)
            if selection["source_key"] == "quiverquant":
                row["reason"] = "resolver_unsupported"
                continue
            if selection.get("series_id") is None or selection.get("expected_source_name") is None:
                continue
            if selection.get("feature_id") is None:
                row["reason"] = "mapping_missing"
                continue
            try:
                remaining_timeout_ms()
                with self.engine.begin() as conn:
                    # Pool/connection acquisition is not cancellable here. If it
                    # returns too late, issue no setup or data SQL on its behalf.
                    remaining_timeout_ms()
                    conn.execute(text("SET TRANSACTION READ ONLY"))
                    prepare_query(conn)
                    registry = conn.execute(text("""
                        SELECT id, name FROM feature_registry
                        WHERE id = :feature_id AND name = :feature_name LIMIT 2
                    """), {"feature_id": selection["feature_id"],
                            "feature_name": selection["feature_name"]}).fetchall()
                    remaining_timeout_ms()
                    if len(registry) != 1 or tuple(registry[0]) != (
                            selection["feature_id"], selection["feature_name"]):
                        row["reason"] = "mapping_missing"
                        continue
                    prepare_query(conn)
                    # direction is one of two validated literals, never caller SQL.
                    result = conn.execute(text("""
                        WITH eligible AS (
                            SELECT rs.feature_id, rs.obs_date, rs.release_date, rs.vintage_date,
                                   rs.value, rs.source_priority_used, sc.name AS source_name,
                                   rs.conflict_flag,
                                   LEFT(CAST(rs.conflict_detail AS text), 2048) AS conflict_detail,
                                   LENGTH(CAST(rs.conflict_detail AS text)) > 2048 AS detail_truncated,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY rs.feature_id, rs.obs_date
                                       ORDER BY rs.vintage_date """ + direction + """, rs.id DESC
                                   ) AS vintage_rank
                            FROM resolved_series rs
                            JOIN feature_registry fr ON fr.id = rs.feature_id AND fr.name = :feature_name
                            LEFT JOIN source_catalog sc ON sc.id = rs.source_priority_used
                            WHERE rs.feature_id = :feature_id
                              AND rs.obs_date >= :start_date AND rs.obs_date <= :as_of_date
                              AND rs.release_date <= :as_of_date AND rs.vintage_date <= :as_of_date
                              AND NOT EXISTS (
                                  SELECT 1 FROM resolved_series_retractions rr
                                  WHERE rr.feature_id = rs.feature_id AND rr.obs_date = rs.obs_date
                                    AND rr.vintage_date = rs.vintage_date
                                    AND rr.retracted_at <= :as_of
                              )
                        )
                        SELECT feature_id, obs_date, release_date, vintage_date, value,
                               source_priority_used, source_name, conflict_flag,
                               conflict_detail, detail_truncated
                        FROM eligible WHERE vintage_rank = 1
                        ORDER BY obs_date DESC LIMIT 1
                    """), {"feature_id": selection["feature_id"],
                            "feature_name": selection["feature_name"], "start_date": start_date,
                            "as_of_date": cutoff.date(), "as_of": cutoff}).mappings().all()
                    remaining_timeout_ms()
                remaining_timeout_ms()
                if not result:
                    row["reason"] = "no_resolved_row"
                    continue
                if len(result) != 1:
                    raise ValueError("snapshot exceeded one-row cap")
                candidate = dict(result[0])
                if candidate.get("feature_id") != selection["feature_id"]:
                    raise ValueError("snapshot feature mismatch")
                observed = candidate.get("obs_date")
                released, vintage = candidate.get("release_date"), candidate.get("vintage_date")
                if (any(type(d) is not date for d in (observed, released, vintage)) or
                        not start_date <= observed <= cutoff.date() or
                        released > cutoff.date() or vintage > cutoff.date()):
                    raise ValueError("snapshot storage clock mismatch")
                row.update({"obs_date": observed.isoformat(), "release_date": released.isoformat(),
                            "vintage_date": vintage.isoformat()})
                actual_id = candidate.get("source_priority_used")
                actual_name = candidate.get("source_name")
                if type(actual_id) is int and actual_id > 0 and type(actual_name) is str:
                    row["actual_source"] = {"id": actual_id, "name": actual_name}
                if row["actual_source"] is None or actual_name != selection["expected_source_name"]:
                    row["reason"] = "source_mismatch"
                    continue
                value = candidate.get("value")
                try:
                    valid = type(value) in (int, float, Decimal) and math.isfinite(float(value))
                except (ValueError, OverflowError):
                    valid = False
                if not valid:
                    row["reason"] = "invalid_value"
                    continue
                row.update({"status": "available", "reason": "resolved_observation",
                            "value": float(value), "conflict_flag": candidate.get("conflict_flag"),
                            "conflict_detail": candidate.get("conflict_detail"),
                            "conflict_detail_truncated": bool(candidate.get("detail_truncated"))})
                policy = selection.get("freshness_policy")
                if policy is None:
                    row["freshness"] = {"status": "unknown", "reason": "freshness_policy_absent"}
                else:
                    stale = (cutoff.date() - observed).days > policy["max_age_days"]
                    row["freshness"] = {"status": "stale" if stale else "fresh",
                                        "reason": "exceeds_declared_age" if stale else "within_declared_age"}
            except Exception:
                row.update({"status": "unavailable", "reason": "read_failed", "value": None})
        return output

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
