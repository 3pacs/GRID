"""
GRID Fed liquidity equation data ingestion module.

The single most important macro signal for risk assets:

    Net Liquidity = Fed Balance Sheet (WALCL) - TGA (WTREGEN) - Reverse Repo (RRPONTSYD)

When net liquidity rises, risk assets go up. Period.

This puller fetches the raw FRED components, computes derived liquidity
features, and stores everything in ``raw_series`` with ``COMPUTED:`` prefixed
series IDs for the derived metrics.

Raw FRED series pulled (only those NOT already in FRED_SERIES_LIST):
- RRPONTSYD — Overnight reverse repo (daily)
- WSHOSHO — Fed holdings of Treasury securities (weekly)
- SWPT — Central bank liquidity swaps (weekly)
- H8B1023NCBCMG — Bank reserves at Fed (weekly)
- TOTRESNS — Total reserves (monthly)

Already pulled by fred.py (reused here from raw_series):
- WALCL — Fed total assets (weekly)
- WTREGEN — TGA balance (weekly)

Derived features stored as COMPUTED:* series:
- fed_net_liquidity = WALCL - WTREGEN - RRPONTSYD
- fed_net_liquidity_change_1w = week-over-week change
- fed_net_liquidity_change_1m = month-over-month change
- reverse_repo_pct_of_peak = current RRP / max(RRP history)
- tga_drawdown = 30-day change in TGA (negative = spending = liquidity injection)
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# FRED series this puller is responsible for fetching directly.
# WALCL and WTREGEN are already in fred.py's FRED_SERIES_LIST,
# so we only read their stored values — we do NOT re-pull them.
FED_LIQUIDITY_SERIES: dict[str, dict[str, str]] = {
    "RRPONTSYD": {
        "description": "Overnight reverse repo facility usage (daily, billions)",
        "frequency": "daily",
    },
    "WSHOSHO": {
        "description": "Fed holdings of Treasury securities (weekly, millions)",
        "frequency": "weekly",
    },
    "SWPT": {
        "description": "Central bank liquidity swaps outstanding (weekly, millions)",
        "frequency": "weekly",
    },
    "H8B1023NCBCMG": {
        "description": "Bank reserves at Federal Reserve Banks (weekly, billions)",
        "frequency": "weekly",
    },
    "TOTRESNS": {
        "description": "Total reserves of depository institutions (monthly, billions)",
        "frequency": "monthly",
    },
}

# Series already pulled by fred.py that we read from raw_series
REUSED_FRED_SERIES: list[str] = ["WALCL", "WTREGEN"]

# Derived feature IDs stored with COMPUTED: prefix
DERIVED_FEATURES: list[str] = [
    "COMPUTED:fed_net_liquidity",
    "COMPUTED:fed_net_liquidity_change_1w",
    "COMPUTED:fed_net_liquidity_change_1m",
    "COMPUTED:reverse_repo_pct_of_peak",
    "COMPUTED:tga_drawdown",
]

# Rate limit between FRED API calls
_RATE_LIMIT_DELAY: float = 0.3
_REQUEST_TIMEOUT: int = 30


class FedLiquidityPuller(BasePuller):
    """Pulls Fed liquidity components from FRED and computes derived metrics.

    The net liquidity equation is the dominant driver of risk asset prices.
    This puller:
    1. Fetches raw FRED series not already in the main FRED pull list.
    2. Reads WALCL and WTREGEN from raw_series (already pulled by FREDPuller).
    3. Computes derived liquidity features and stores them as COMPUTED:* series.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = "FRED"  # Reuses existing FRED source entry

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the Fed liquidity puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        if not api_key:
            log.warning(
                "FedLiquidityPuller: FRED_API_KEY not set — pulls will fail"
            )
        self._api_key = api_key
        super().__init__(db_engine)
        log.info(
            "FedLiquidityPuller initialised — source_id={sid}", sid=self.source_id
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, Exception),
    )
    def _fetch_fred_series(
        self, series_id: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """Fetch a single FRED series via the fedfred API.

        Parameters:
            series_id: FRED series identifier.
            start_date: Start date for observations.
            end_date: End date for observations.

        Returns:
            DataFrame with 'date' and 'value' columns.
        """
        try:
            from fedfred import FredAPI
        except ImportError:
            log.error("fedfred not installed — run: pip install fedfred")
            raise ImportError("fedfred library required for FedLiquidityPuller")

        fred = FredAPI(self._api_key)
        df = fred.get_series_observations(
            series_id,
            observation_start=str(start_date),
            observation_end=str(end_date),
        )

        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.DataFrame(columns=["date", "value"])

        if isinstance(df, pd.DataFrame):
            result = pd.DataFrame()

            # Find date column — fedfred may return date as a column
            # named 'date', 'observation_date', or as the DataFrame index
            _date_col_names = ("date", "Date", "observation_date", "realtime_start")
            for col in _date_col_names:
                if col in df.columns:
                    result["date"] = pd.to_datetime(df[col], errors="coerce")
                    break
            if "date" not in result.columns:
                # Check if the index is a DatetimeIndex or has a date-like name
                idx = df.index
                if isinstance(idx, pd.DatetimeIndex):
                    result["date"] = idx
                elif idx.name in _date_col_names or idx.name is not None:
                    result["date"] = pd.to_datetime(idx, errors="coerce")
                elif len(df.columns) > 0:
                    result["date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

            # Find value column
            _val_col_names = ("value", "Value", series_id)
            for col in _val_col_names:
                if col in df.columns:
                    result["value"] = pd.to_numeric(df[col], errors="coerce")
                    break
            if "value" not in result.columns:
                numeric_cols = df.select_dtypes(include=["number"]).columns
                if len(numeric_cols) > 0:
                    result["value"] = df[numeric_cols[0]]
                elif len(df.columns) > 0:
                    result["value"] = pd.to_numeric(df.iloc[:, -1], errors="coerce")

            # Log coerced values (ATTENTION.md #13)
            nan_count = result["value"].isna().sum()
            if nan_count > 0:
                log.warning(
                    "FRED {sid}: {n} values coerced to NaN",
                    sid=series_id,
                    n=nan_count,
                )

            result = result.dropna(subset=["date", "value"])
            return result

        return pd.DataFrame(columns=["date", "value"])

    def _pull_raw_series(
        self,
        series_id: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        """Pull a single FRED series and store in raw_series.

        Parameters:
            series_id: FRED series identifier (used as both FRED ID and series_id).
            start_date: Earliest observation date.
            end_date: Latest observation date.

        Returns:
            Result dict with status and rows_inserted.
        """
        rows_inserted = 0

        try:
            df = self._fetch_fred_series(series_id, start_date, end_date)
            if df.empty:
                return {
                    "series": series_id,
                    "status": "NO_DATA",
                    "rows_inserted": 0,
                }

            with self.engine.begin() as conn:
                existing_dates = self._get_existing_dates(series_id, conn)
                for _, row in df.iterrows():
                    obs_date = row["date"].date()
                    value = float(row["value"])

                    if obs_date in existing_dates:
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={"fred_series": series_id},
                    )
                    rows_inserted += 1

            log.info(
                "FedLiquidity {sid} — {n} rows inserted",
                sid=series_id,
                n=rows_inserted,
            )

        except ImportError:
            return {
                "series": series_id,
                "status": "FAILED",
                "rows_inserted": 0,
                "error": "fedfred not installed",
            }
        except Exception as exc:
            log.error(
                "FedLiquidity {sid} pull failed: {e}", sid=series_id, e=str(exc)
            )
            return {
                "series": series_id,
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        return {
            "series": series_id,
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
        }

    def _load_series_from_db(
        self,
        series_id: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, float]:
        """Load a stored series from raw_series as {obs_date: value} dict.

        Parameters:
            series_id: Series identifier in raw_series.
            start_date: Earliest date to load.
            end_date: Latest date to load.

        Returns:
            Dict mapping obs_date to float value.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, value FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND obs_date >= :start AND obs_date <= :end "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date"
                ),
                {
                    "sid": series_id,
                    "src": self.source_id,
                    "start": start_date,
                    "end": end_date,
                },
            ).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _compute_derived(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Compute derived liquidity features from raw series data.

        Reads WALCL, WTREGEN, and RRPONTSYD from raw_series and computes:
        - fed_net_liquidity = WALCL - WTREGEN - RRPONTSYD
        - fed_net_liquidity_change_1w = 7-day change
        - fed_net_liquidity_change_1m = 30-day change
        - reverse_repo_pct_of_peak = current / max(historical)
        - tga_drawdown = 30-day change in TGA

        Parameters:
            start_date: Earliest date for computation window.
            end_date: Latest date for computation.

        Returns:
            List of result dicts per derived feature.
        """
        results: list[dict[str, Any]] = []

        # Load all three components — go back further for change calculations
        lookback_start = start_date - timedelta(days=60)

        walcl = self._load_series_from_db("WALCL", lookback_start, end_date)
        wtregen = self._load_series_from_db("WTREGEN", lookback_start, end_date)
        rrp = self._load_series_from_db("RRPONTSYD", lookback_start, end_date)

        if not walcl or not wtregen or not rrp:
            log.warning(
                "FedLiquidity: missing raw data for derived features "
                "(WALCL={w}, WTREGEN={t}, RRPONTSYD={r} rows)",
                w=len(walcl),
                t=len(wtregen),
                r=len(rrp),
            )

        # Build aligned net liquidity series using forward-fill for
        # weekly/daily frequency mismatch
        all_dates = sorted(
            set(walcl.keys()) | set(wtregen.keys()) | set(rrp.keys())
        )

        # Forward-fill: carry last known value for each component
        net_liq: dict[date, float] = {}
        last_w, last_t, last_r = None, None, None
        for d in all_dates:
            if d in walcl:
                last_w = walcl[d]
            if d in wtregen:
                last_t = wtregen[d]
            if d in rrp:
                last_r = rrp[d]

            if last_w is not None and last_t is not None and last_r is not None:
                net_liq[d] = last_w - last_t - last_r

        # Convert to sorted list for temporal lookups
        net_liq_dates = sorted(net_liq.keys())

        with self.engine.begin() as conn:
            # ── COMPUTED:fed_net_liquidity ─────────────────────────────────
            nl_rows = 0
            for d in net_liq_dates:
                if d < start_date:
                    continue
                sid = "COMPUTED:fed_net_liquidity"
                if not self._row_exists(sid, d, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=d,
                        value=net_liq[d],
                        raw_payload={
                            "walcl": walcl.get(d),
                            "wtregen": wtregen.get(d),
                            "rrpontsyd": rrp.get(d),
                        },
                    )
                    nl_rows += 1
            results.append({
                "feature": "COMPUTED:fed_net_liquidity",
                "status": "SUCCESS",
                "rows_inserted": nl_rows,
            })

            # ── COMPUTED:fed_net_liquidity_change_1w ───────────────────────
            chg_1w_rows = 0
            for d in net_liq_dates:
                if d < start_date:
                    continue
                prev_d = d - timedelta(days=7)
                # Find closest date on or before prev_d
                prev_val = None
                for pd_ in reversed(net_liq_dates):
                    if pd_ <= prev_d:
                        prev_val = net_liq[pd_]
                        break
                if prev_val is not None:
                    chg = net_liq[d] - prev_val
                    sid = "COMPUTED:fed_net_liquidity_change_1w"
                    if not self._row_exists(sid, d, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=d,
                            value=chg,
                            raw_payload={"current": net_liq[d], "prev_1w": prev_val},
                        )
                        chg_1w_rows += 1
            results.append({
                "feature": "COMPUTED:fed_net_liquidity_change_1w",
                "status": "SUCCESS",
                "rows_inserted": chg_1w_rows,
            })

            # ── COMPUTED:fed_net_liquidity_change_1m ───────────────────────
            chg_1m_rows = 0
            for d in net_liq_dates:
                if d < start_date:
                    continue
                prev_d = d - timedelta(days=30)
                prev_val = None
                for pd_ in reversed(net_liq_dates):
                    if pd_ <= prev_d:
                        prev_val = net_liq[pd_]
                        break
                if prev_val is not None:
                    chg = net_liq[d] - prev_val
                    sid = "COMPUTED:fed_net_liquidity_change_1m"
                    if not self._row_exists(sid, d, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=d,
                            value=chg,
                            raw_payload={"current": net_liq[d], "prev_1m": prev_val},
                        )
                        chg_1m_rows += 1
            results.append({
                "feature": "COMPUTED:fed_net_liquidity_change_1m",
                "status": "SUCCESS",
                "rows_inserted": chg_1m_rows,
            })

            # ── COMPUTED:reverse_repo_pct_of_peak ──────────────────────────
            # Use ALL historical RRP data for peak calculation
            all_rrp = self._load_series_from_db(
                "RRPONTSYD", date(2000, 1, 1), end_date
            )
            rrp_peak = max(all_rrp.values()) if all_rrp else 1.0
            if rrp_peak == 0:
                rrp_peak = 1.0

            rrp_pct_rows = 0
            for d, val in sorted(rrp.items()):
                if d < start_date:
                    continue
                pct = val / rrp_peak
                sid = "COMPUTED:reverse_repo_pct_of_peak"
                if not self._row_exists(sid, d, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=d,
                        value=pct,
                        raw_payload={"rrp": val, "peak": rrp_peak},
                    )
                    rrp_pct_rows += 1
            results.append({
                "feature": "COMPUTED:reverse_repo_pct_of_peak",
                "status": "SUCCESS",
                "rows_inserted": rrp_pct_rows,
            })

            # ── COMPUTED:tga_drawdown ──────────────────────────────────────
            # 30-day change in TGA: negative means Treasury spending = liquidity injection
            tga_dd_rows = 0
            tga_dates = sorted(wtregen.keys())
            for d in tga_dates:
                if d < start_date:
                    continue
                prev_d = d - timedelta(days=30)
                prev_val = None
                for pd_ in reversed(tga_dates):
                    if pd_ <= prev_d:
                        prev_val = wtregen[pd_]
                        break
                if prev_val is not None:
                    drawdown = wtregen[d] - prev_val
                    sid = "COMPUTED:tga_drawdown"
                    if not self._row_exists(sid, d, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=d,
                            value=drawdown,
                            raw_payload={
                                "tga_current": wtregen[d],
                                "tga_30d_ago": prev_val,
                            },
                        )
                        tga_dd_rows += 1
            results.append({
                "feature": "COMPUTED:tga_drawdown",
                "status": "SUCCESS",
                "rows_inserted": tga_dd_rows,
            })

        return results

    def pull_all(
        self,
        start_date: str | date = "2013-01-01",
        days_back: int = 365,
    ) -> list[dict[str, Any]]:
        """Pull all Fed liquidity series (raw + derived).

        Parameters:
            start_date: Earliest observation date. Defaults to 2013 when RRP
                        facility became relevant.
            days_back: Number of days back for incremental pulls.

        Returns:
            List of result dicts per series/feature.
        """
        start = (
            date.fromisoformat(start_date)
            if isinstance(start_date, str)
            else start_date
        )
        end = date.today()

        # Use incremental start if we already have data
        for sid in FED_LIQUIDITY_SERIES:
            latest = self._get_latest_date(sid)
            if latest is not None:
                incremental = latest - timedelta(days=7)
                if incremental > start:
                    start = incremental
                    log.info(
                        "FedLiquidity: incremental from {d} (last={l})",
                        d=start,
                        l=latest,
                    )
                break  # All series share roughly the same date range

        results: list[dict[str, Any]] = []

        # Step 1: Pull raw FRED series that we own
        for series_id in FED_LIQUIDITY_SERIES:
            res = self._pull_raw_series(series_id, start, end)
            results.append(res)
            time.sleep(_RATE_LIMIT_DELAY)

        # Step 2: Compute derived liquidity features
        try:
            derived = self._compute_derived(start, end)
            results.extend(derived)
        except Exception as exc:
            log.error("FedLiquidity derived computation failed: {e}", e=str(exc))
            results.append({
                "feature": "derived_features",
                "status": "FAILED",
                "error": str(exc),
            })

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        ok = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "FedLiquidity pull complete — {ok}/{total} succeeded, "
            "{ins} total rows inserted",
            ok=ok,
            total=len(results),
            ins=total_inserted,
        )
        return results


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    puller = FedLiquidityPuller(
        api_key=settings.FRED_API_KEY, db_engine=get_engine()
    )
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(
            f"  {r.get('series', r.get('feature', '?'))}: "
            f"{r['status']} ({r.get('rows_inserted', 0)} rows)"
        )
