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

import gzip
import hashlib
import json
import math
import os
import time as monotonic_time
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Generator

import numpy as np
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


#: Open bounds for ``get_pit``'s obs_date window (PostgreSQL accepts both).
OBS_DATE_MIN = date(1, 1, 1)
OBS_DATE_MAX = date(9999, 12, 31)


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

    def get_closed_crypto_candles(self, symbol: str, *, as_of: datetime) -> list[dict]:
        """Timestamp-bounded, observed Binance 5m buckets for live research.

        The daily resolved store cannot express these intraday observations.
        A bar is eligible only after its end AND its local availability time.
        This is not a provider-certified-final completeness assertion.
        """
        from datetime import timedelta

        if symbol not in {"BTC", "ETH", "SOL"} or as_of.tzinfo is None:
            raise ValueError("Unsupported crypto symbol or naive cutoff")
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ts, close, volume, created_at
                FROM realtime_candles
                WHERE source = :source AND symbol = :instrument AND interval = :interval
                  AND ts >= :since AND ts + INTERVAL '5 minutes' <= :as_of
                  AND created_at <= :as_of
                ORDER BY ts
            """), {"source": "binance", "instrument": symbol + "USDT", "interval": "5m",
                   "since": as_of - timedelta(hours=25), "as_of": as_of}).fetchall()
        return [{"ts": r[0], "close": r[1], "volume": r[2], "available_at": r[3]} for r in rows]

    def get_crypto_actor_actions(self, *, as_of: datetime) -> list[dict]:
        """Actor-action candidates known at decision time, with their source ids.

        Eligibility is not validation: consumers still require a named actor,
        measured flow, valve and direction. Sentiment is not a liquidity action.
        """
        from datetime import timedelta

        if as_of.tzinfo is None:
            raise ValueError("Actor-action cutoff must be timezone-aware")
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, ticker, source_type, source_id, signal_date, signal_type,
                       signal_value, created_at
                FROM signal_sources
                WHERE ticker = ANY(:symbols) AND created_at <= :as_of
                  AND created_at >= :since
                  AND signal_date BETWEEN :first_day AND :last_day
                  AND source_type = ANY(:types)
                  AND signal_type <> 'spot_taker_flow'
                ORDER BY created_at DESC, id DESC LIMIT 100
            """), {"symbols": ["BTC", "ETH", "SOL"], "as_of": as_of,
                   "since": as_of - timedelta(hours=24),
                   "first_day": (as_of - timedelta(hours=24)).date(), "last_day": as_of.date(),
                   "types": ["institutional", "whale", "exchange_flow"]}).fetchall()
        return [{"id": r[0], "symbol": r[1], "source": r[2], "source_id": r[3],
                 "event_date": r[4].isoformat(), "signal_type": r[5], "details": r[6],
                 "available_at": r[7].isoformat()} for r in rows]

    def get_spot_order_flow_receipts(self, *, as_of: datetime) -> list[dict]:
        """Actual Binance spot 5m taker measurements known by this clock."""
        if as_of.tzinfo is None:
            raise ValueError("Spot flow cutoff must be timezone-aware")
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, ticker, signal_value, created_at, source_id
                FROM signal_sources
                WHERE source_type = 'exchange_flow' AND signal_type = 'spot_taker_flow'
                  AND source_id LIKE 'binance_spot:5m:%'
                  AND ticker = ANY(:symbols) AND created_at <= :as_of
                  AND signal_date BETWEEN :since AND :day
                ORDER BY id DESC LIMIT 300
            """), {"symbols": ["BTC", "ETH", "SOL"], "as_of": as_of,
                   "since": (as_of - timedelta(days=1)).date(), "day": as_of.date()}).fetchall()
        return [{"id": r[0], "symbol": r[1], "payload": r[2],
                 "available_at": r[3], "source_id": r[4]} for r in rows]

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
        *,
        start_date: date | None = None,
        end_date: date | None = None,
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
            start_date: Optional first obs_date to return (inclusive).
            end_date: Optional last obs_date to return (inclusive).
                The bounds are applied in SQL before ``DISTINCT ON``. The
                vintage policy picks per ``(feature_id, obs_date)``, so a
                bounded read returns exactly the rows of the unbounded read
                that fall inside the window; it only reads fewer of them.
                Without bounds every obs_date since inception is read
                (rotation backtests read ~1 GB per job that way, W2-7).

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
                  AND rs.obs_date BETWEEN :obs_start AND :obs_end
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
                  AND rs.obs_date BETWEEN :obs_start AND :obs_end
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
                    "obs_start": start_date or OBS_DATE_MIN,
                    "obs_end": end_date or OBS_DATE_MAX,
                    "retraction_cutoff": retraction_cutoff(as_of_date),
                },
            ).fetchall()

        # Plain tuples, not SQLAlchemy ``Row`` objects: handed Rows, pandas
        # probes each one with ``hasattr`` and every probe raises and
        # swallows a KeyError inside SQLAlchemy. Measured read-only on
        # grid-svr 2026-11-06: 37 s of the 53 s ``/signals/snapshot`` build
        # (3.2M probes for ~800K rows) was this construction. Same values,
        # same column order, same dtypes.
        df = pd.DataFrame(
            [tuple(r) for r in rows],
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

        pit_df = self.get_pit(
            feature_ids,
            as_of_date,
            vintage_policy,
            start_date=start_date,
            end_date=end_date,
        )

        # Filter to date range (the read is already bounded; this keeps the
        # contract for readers that ignore the bounds)
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

    def feature_ids_by_name(self, names: list[str]) -> dict[str, int]:
        """Exact ``feature_registry`` name -> id for the registered *names*."""
        if not names:
            return {}
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
                {"names": list(names)},
            ).fetchall()
        return {r[1]: int(r[0]) for r in rows}

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


# ── Bounded in-memory PIT window (W2-7, 2026-11-06) ─────────────────────
#
# A rotation backtest asks the same PIT question ~117 times, once per weekly
# rebalance, each time over a window that overlaps the last one by 51 weeks.
# ``PITWindowCache`` reads every candidate vintage of a bounded window ONCE
# (one SQL statement) and then answers ``get_pit`` for any ``as_of`` inside
# it in memory, applying the same three filters and the same vintage pick as
# the SQL above. The same object serialises to a hash-receipted snapshot file
# so a remote worker can run the job without reading the database at all.

PIT_WINDOW_FORMAT = "grid.pit_window.v1"

PIT_WINDOW_COLUMNS = (
    "feature_id", "obs_date", "value", "release_date", "vintage_date", "retracted_at",
)

#: Every vintage that could answer a PIT read inside the window, with the
#: retraction time of its key (keys are unique in both tables). Retractions
#: after the window's last ``as_of`` can never hide a row from a read the
#: window answers, so they are left out and the content (and its hash) is
#: fixed by the window.
PIT_WINDOW_SELECT_SQL = """
    SELECT rs.feature_id, rs.obs_date, rs.value, rs.release_date, rs.vintage_date,
           rr.retracted_at
    FROM resolved_series rs
    LEFT JOIN resolved_series_retractions rr
      ON rr.feature_id = rs.feature_id
     AND rr.obs_date = rs.obs_date
     AND rr.vintage_date = rs.vintage_date
     AND rr.retracted_at <= :retraction_cutoff
    WHERE rs.feature_id = ANY(:fids)
      AND rs.obs_date BETWEEN :obs_start AND :obs_end
      AND rs.release_date <= :max_as_of
    ORDER BY rs.feature_id, rs.obs_date, rs.vintage_date
"""

# == np.iinfo(np.int64).max, written as a literal: tests exec this module with
# its imports stripped, so module-level code must not need numpy.
_NO_RETRACTION_US = 2**63 - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class PITWindowMiss(ValueError):
    """A read asked for features, dates or an ``as_of`` the window does not hold."""


def _to_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, str):
        return date.fromisoformat(value)
    if isinstance(value, datetime):
        return value.date()
    return value


def _to_utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_us(value: datetime) -> int:
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


class PITWindowCache(PITStore):
    """``PITStore`` over an in-memory window of ``resolved_series`` vintages.

    Holds every vintage of ``feature_ids`` with ``obs_date`` in
    ``[start_date, end_date]`` and ``release_date <= max_as_of``, plus each
    row's retraction time. ``get_pit`` then reproduces the SQL read for any
    ``as_of <= max_as_of`` whose obs_date window lies inside the held one:
    same three filters (obs_date and release_date <= as_of, retraction by the
    end of as_of), same vintage pick, same columns and row order, and the
    same ``assert_no_lookahead`` safety net. A read outside the window
    raises ``PITWindowMiss`` or, when ``allow_db_fallback`` is set and an
    engine is attached, is answered by the database with a warning.

    ``feature_names`` records the ``feature_registry`` name lookups made at
    build time (``None`` = name not registered), so callers that resolve
    names first need no database either.
    """

    def __init__(
        self,
        rows: pd.DataFrame,
        *,
        feature_ids: list[int],
        start_date: date | None,
        end_date: date,
        max_as_of: date,
        feature_names: dict[str, int | None] | None = None,
        engine: Engine | None = None,
        allow_db_fallback: bool = False,
        receipt: dict | None = None,
    ) -> None:
        super().__init__(engine)  # type: ignore[arg-type]
        if start_date is not None and start_date > end_date:
            raise ValueError(f"window start {start_date} is after its end {end_date}")
        self.feature_ids = frozenset(int(f) for f in feature_ids)
        self.start_date = start_date
        self.end_date = end_date
        self.max_as_of = max_as_of
        self.feature_names = dict(feature_names or {})
        self.allow_db_fallback = allow_db_fallback
        self.receipt = receipt
        self.db_fallbacks = 0
        self.reads = 0

        frame = pd.DataFrame(rows, columns=list(PIT_WINDOW_COLUMNS))
        frame = frame.sort_values(["feature_id", "obs_date", "vintage_date"], kind="mergesort")
        frame = frame.reset_index(drop=True)
        self._rows = frame[["feature_id", "obs_date", "value", "release_date", "vintage_date"]]
        self._fid = frame["feature_id"].to_numpy(dtype=np.int64)
        self._obs = np.fromiter((d.toordinal() for d in frame["obs_date"]), np.int64, len(frame))
        self._release = np.fromiter((d.toordinal() for d in frame["release_date"]), np.int64, len(frame))
        self._vintage = np.fromiter((d.toordinal() for d in frame["vintage_date"]), np.int64, len(frame))
        self._retracted = np.fromiter(
            (
                _NO_RETRACTION_US if (v is None or v is pd.NaT or (isinstance(v, float) and math.isnan(v)))
                else _utc_us(_to_utc(v))
                for v in frame["retracted_at"]
            ),
            np.int64,
            len(frame),
        )
        self._retracted_at = [
            None if r == _NO_RETRACTION_US else _EPOCH + timedelta(microseconds=int(r))
            for r in self._retracted
        ]

    # -- construction ------------------------------------------------------

    @classmethod
    def load(
        cls,
        engine: Engine,
        feature_ids: list[int],
        start_date: date | None,
        end_date: date,
        max_as_of: date,
        *,
        feature_names: dict[str, int | None] | None = None,
        allow_db_fallback: bool = True,
    ) -> PITWindowCache:
        """Read the window with ONE bounded statement (parameterised)."""
        fids = sorted({int(f) for f in feature_ids})
        rows: list = []
        if fids:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(PIT_WINDOW_SELECT_SQL),
                    {
                        "fids": fids,
                        "obs_start": start_date or OBS_DATE_MIN,
                        "obs_end": end_date,
                        "max_as_of": max_as_of,
                        "retraction_cutoff": retraction_cutoff(max_as_of),
                    },
                ).fetchall()
        log.info(
            "PIT window loaded — {n} vintages, {f} features, obs {s}..{e}, as_of <= {a}",
            n=len(rows), f=len(fids), s=start_date, e=end_date, a=max_as_of,
        )
        return cls(
            pd.DataFrame([tuple(r) for r in rows], columns=list(PIT_WINDOW_COLUMNS)),
            feature_ids=fids,
            start_date=start_date,
            end_date=end_date,
            max_as_of=max_as_of,
            feature_names=feature_names,
            engine=engine,
            allow_db_fallback=allow_db_fallback,
        )

    # -- reads -------------------------------------------------------------

    def covers(
        self,
        feature_ids: list[int],
        as_of_date: date,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> bool:
        """True when ``get_pit`` with these arguments can be answered in memory."""
        if isinstance(as_of_date, datetime) or as_of_date > self.max_as_of:
            return False
        if not {int(f) for f in feature_ids} <= self.feature_ids:
            return False
        hi = as_of_date if end_date is None else min(end_date, as_of_date)
        if start_date is not None and hi < start_date:
            return True  # empty window: nothing to miss
        if self.start_date is not None and (start_date is None or start_date < self.start_date):
            return False
        return hi <= self.end_date

    def get_pit(
        self,
        feature_ids: list[int],
        as_of_date: date,
        vintage_policy: str = "LATEST_AS_OF",
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        if vintage_policy not in ("FIRST_RELEASE", "LATEST_AS_OF"):
            raise ValueError(
                f"Invalid vintage_policy '{vintage_policy}'. "
                "Must be 'FIRST_RELEASE' or 'LATEST_AS_OF'."
            )
        if not feature_ids:
            return pd.DataFrame(
                columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
            )
        if not self.covers(feature_ids, as_of_date, start_date, end_date):
            if self.allow_db_fallback and self.engine is not None:
                self.db_fallbacks += 1
                log.warning(
                    "PIT window miss (as_of={a}, obs {s}..{e}) — reading the database",
                    a=as_of_date, s=start_date, e=end_date,
                )
                return super().get_pit(
                    feature_ids, as_of_date, vintage_policy,
                    start_date=start_date, end_date=end_date,
                )
            raise PITWindowMiss(
                f"PIT window (features={len(self.feature_ids)}, obs {self.start_date}.."
                f"{self.end_date}, as_of <= {self.max_as_of}) cannot answer as_of="
                f"{as_of_date}, obs {start_date}..{end_date}"
            )

        self.reads += 1
        aod = as_of_date.toordinal()
        mask = np.isin(self._fid, np.fromiter((int(f) for f in feature_ids), np.int64))
        mask &= self._obs <= aod
        mask &= self._release <= aod
        if start_date is not None:
            mask &= self._obs >= start_date.toordinal()
        if end_date is not None:
            mask &= self._obs <= end_date.toordinal()
        mask &= self._retracted > _utc_us(retraction_cutoff(as_of_date))
        idx = np.flatnonzero(mask)
        if vintage_policy == "LATEST_AS_OF":
            # Rows are sorted by (feature, obs, vintage): the last of each
            # (feature, obs) run is the latest vintage.
            keep = np.ones(len(idx), dtype=bool)
            if len(idx) > 1:
                same_next = (self._fid[idx[1:]] == self._fid[idx[:-1]]) & (
                    self._obs[idx[1:]] == self._obs[idx[:-1]]
                )
                keep[:-1] = ~same_next
        else:
            keep = np.ones(len(idx), dtype=bool)
            if len(idx) > 1:
                same_prev = (self._fid[idx[1:]] == self._fid[idx[:-1]]) & (
                    self._obs[idx[1:]] == self._obs[idx[:-1]]
                )
                keep[1:] = ~same_prev
        df = self._rows.iloc[idx[keep]].reset_index(drop=True)
        self.assert_no_lookahead(df, as_of_date)
        return df

    def feature_ids_by_name(self, names: list[str]) -> dict[str, int]:
        missing = [n for n in names if n not in self.feature_names]
        if missing:
            if self.allow_db_fallback and self.engine is not None:
                self.db_fallbacks += 1
                self.feature_names.update(
                    {n: None for n in missing} | PITStore.feature_ids_by_name(self, missing)
                )
            else:
                raise PITWindowMiss(f"feature names not in the PIT window: {sorted(missing)[:5]}")
        return {n: self.feature_names[n] for n in names if self.feature_names.get(n) is not None}

    # -- snapshot files ----------------------------------------------------

    def _content(self) -> dict:
        rows = [
            list(r)
            for r in zip(
                self._fid.tolist(),
                [d.isoformat() for d in self._rows["obs_date"]],
                [float(v) for v in self._rows["value"]],
                [d.isoformat() for d in self._rows["release_date"]],
                [d.isoformat() for d in self._rows["vintage_date"]],
                [None if t is None else t.isoformat() for t in self._retracted_at],
            )
        ]
        return {
            "format": PIT_WINDOW_FORMAT,
            "window": {
                "feature_ids": sorted(self.feature_ids),
                "start_date": None if self.start_date is None else self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
                "max_as_of": self.max_as_of.isoformat(),
                "retraction_cutoff": retraction_cutoff(self.max_as_of).isoformat(),
            },
            "feature_names": dict(sorted(self.feature_names.items())),
            "columns": list(PIT_WINDOW_COLUMNS),
            "rows": rows,
        }

    def content_sha256(self) -> str:
        """sha256 over the canonical JSON of window, name map and every row."""
        return _canonical_sha256(self._content())

    def write_snapshot(self, directory: str | Path, *, built_by: str = "") -> Path:
        """Write ``pit-window-<sha256>.json.gz`` into *directory* (never overwrites).

        The digest covers only the content, so the same window built twice
        (or on two hosts) has the same name; ``built_at``/``built_by`` sit
        outside it.
        """
        content = self._content()
        sha = _canonical_sha256(content)
        path = Path(directory) / f"pit-window-{sha}.json.gz"
        if path.exists():
            if PITWindowCache.read_snapshot(path).content_sha256() != sha:
                raise ValueError(f"existing snapshot {path} does not match its name")
            return path
        envelope = {
            "content": content,
            "sha256": sha,
            "built_at": datetime.now(timezone.utc).isoformat(),
            "built_by": built_by,
            "n_rows": len(content["rows"]),
        }
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
            gz.write(json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        os.replace(tmp, path)
        return path

    @classmethod
    def read_snapshot(
        cls,
        path: str | Path,
        *,
        expected_sha256: str | None = None,
        engine: Engine | None = None,
        allow_db_fallback: bool = False,
    ) -> PITWindowCache:
        """Load a snapshot file, recomputing its digest; refuse on any mismatch."""
        with gzip.open(path, "rb") as gz:
            envelope = json.loads(gz.read())
        content = envelope["content"]
        if content.get("format") != PIT_WINDOW_FORMAT:
            raise ValueError(f"{path}: unknown snapshot format {content.get('format')!r}")
        sha = _canonical_sha256(content)
        if sha != envelope.get("sha256"):
            raise ValueError(f"{path}: content digest {sha} does not match its receipt")
        if expected_sha256 is not None and sha != expected_sha256:
            raise ValueError(f"{path}: digest {sha} is not the expected {expected_sha256}")
        window = content["window"]
        frame = pd.DataFrame(
            [
                (
                    int(r[0]), date.fromisoformat(r[1]), float(r[2]),
                    date.fromisoformat(r[3]), date.fromisoformat(r[4]),
                    None if r[5] is None else datetime.fromisoformat(r[5]),
                )
                for r in content["rows"]
            ],
            columns=list(PIT_WINDOW_COLUMNS),
        )
        cache = cls(
            frame,
            feature_ids=window["feature_ids"],
            start_date=_to_date(window["start_date"]),
            end_date=_to_date(window["end_date"]),
            max_as_of=_to_date(window["max_as_of"]),
            feature_names=content.get("feature_names") or {},
            engine=engine,
            allow_db_fallback=allow_db_fallback,
            receipt={
                "sha256": sha,
                "path": str(path),
                "built_at": envelope.get("built_at"),
                "built_by": envelope.get("built_by"),
                "n_rows": len(content["rows"]),
            },
        )
        if cache.content_sha256() != sha:  # round-trip guard (types, ordering)
            raise ValueError(f"{path}: snapshot does not round-trip to its digest")
        return cache


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
