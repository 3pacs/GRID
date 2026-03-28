"""
GRID NY Fed data ingestion module.

Pulls freely available data from the New York Federal Reserve:
- Staff Nowcast GDP estimates (current and next quarter)
- SOMA (System Open Market Account) holdings breakdown
- Treasury securities open market operations

No API key required -- all endpoints are public.

Series produced:
    nyfed.nowcast_gdp_q1    Current-quarter GDP nowcast (% annualised)
    nyfed.nowcast_gdp_q2    Next-quarter GDP nowcast (% annualised)
    nyfed.soma_total_par_bn  Total SOMA par value (billions USD)
    nyfed.soma_treasury_par_bn  Treasury holdings par value (billions)
    nyfed.soma_mbs_par_bn   MBS holdings par value (billions)
    nyfed.tsy_ops_total_bn  Treasury operations volume (billions, last 2 weeks)
"""

from __future__ import annotations

import io
import math
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REQUEST_TIMEOUT: int = 45
_USER_AGENT: str = "GRID-Ingestion/1.0 (research; +https://grid.local)"

# NY Fed Nowcast spreadsheet (updated weekly, history back to 2002)
_NOWCAST_URL: str = (
    "https://www.newyorkfed.org/medialibrary/research/policy/nowcast/"
    "new-york-fed-staff-nowcast_data_2002-present.xlsx"
)

# SOMA endpoints (Markets API, JSON, no key)
_SOMA_ASOF_URL: str = "https://markets.newyorkfed.org/api/soma/asofdates/latest.json"
_SOMA_SUMMARY_URL: str = "https://markets.newyorkfed.org/api/soma/summary.json"

# Treasury operations (last two weeks of results)
_TSY_OPS_URL: str = (
    "https://markets.newyorkfed.org/api/tsy/all/results/lastTwoWeeks.json"
)

# Series ID prefix
_PFX: str = "nyfed"

# All series managed by this puller
ALL_SERIES: list[str] = [
    f"{_PFX}.nowcast_gdp_q1",
    f"{_PFX}.nowcast_gdp_q2",
    f"{_PFX}.soma_total_par_bn",
    f"{_PFX}.soma_treasury_par_bn",
    f"{_PFX}.soma_mbs_par_bn",
    f"{_PFX}.tsy_ops_total_bn",
]


class NYFedPuller(BasePuller):
    """Pulls macro-financial data from the New York Federal Reserve.

    All endpoints are freely accessible with no API key. The puller
    fetches GDP nowcasts, SOMA balance-sheet data, and Treasury open
    market operation results.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = "NY_Fed"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://markets.newyorkfed.org/api",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the NY Fed puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})
        log.info(
            "NYFedPuller initialised -- source_id={sid}", sid=self.source_id
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_json(self, url: str) -> dict[str, Any]:
        """GET a JSON endpoint from the NY Fed Markets API.

        Parameters:
            url: Full URL to fetch.

        Returns:
            Parsed JSON as a dict.

        Raises:
            requests.exceptions.HTTPError: On non-2xx response.
        """
        resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_bytes(self, url: str) -> bytes:
        """GET binary content (e.g. an Excel file).

        Parameters:
            url: Full URL to fetch.

        Returns:
            Raw response bytes.

        Raises:
            requests.exceptions.HTTPError: On non-2xx response.
        """
        resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # Nowcast GDP
    # ------------------------------------------------------------------

    def _pull_nowcast(self) -> list[dict[str, Any]]:
        """Pull NY Fed Staff Nowcast GDP estimates.

        The spreadsheet contains weekly nowcast vintages for the current
        and next quarter. We store the latest estimate per forecast date.

        Returns:
            List of result dicts (one per series).
        """
        import pandas as pd

        results: list[dict[str, Any]] = []
        q1_sid = f"{_PFX}.nowcast_gdp_q1"
        q2_sid = f"{_PFX}.nowcast_gdp_q2"

        try:
            raw_bytes = self._fetch_bytes(_NOWCAST_URL)
            xls = pd.ExcelFile(io.BytesIO(raw_bytes))

            # The spreadsheet has a "Forecast" sheet with dated nowcast rows
            # Try common sheet names
            sheet_name: str | None = None
            for candidate in ("Forecast", "Forecasts", "Nowcast", xls.sheet_names[0]):
                if candidate in xls.sheet_names:
                    sheet_name = candidate
                    break
            if sheet_name is None:
                sheet_name = xls.sheet_names[0]

            df = pd.read_excel(xls, sheet_name=sheet_name)
            log.debug(
                "Nowcast sheet '{s}' — {r} rows, columns: {c}",
                s=sheet_name,
                r=len(df),
                c=list(df.columns),
            )

            # Normalise column names to lowercase for easier matching
            df.columns = [str(c).strip().lower() for c in df.columns]

            # Identify date column
            date_col: str | None = None
            for col in ("date", "forecast date", "forecastdate", "release date"):
                if col in df.columns:
                    date_col = col
                    break
            if date_col is None:
                # Fall back to first column
                date_col = df.columns[0]

            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=[date_col])

            # Identify GDP forecast columns (Q1 = current, Q2 = next quarter)
            # Common patterns: "gdp", "nowcast", quarter labels
            numeric_cols = [
                c for c in df.columns
                if c != date_col and pd.api.types.is_numeric_dtype(df[c])
            ]

            if len(numeric_cols) == 0:
                log.warning("Nowcast: no numeric columns found")
                for sid in (q1_sid, q2_sid):
                    results.append({
                        "feature": sid, "status": "NO_DATA", "rows_inserted": 0,
                    })
                return results

            # Use first numeric column for Q1, second for Q2 (if available)
            q1_col = numeric_cols[0]
            q2_col = numeric_cols[1] if len(numeric_cols) > 1 else None

            with self.engine.begin() as conn:
                existing_q1 = self._get_existing_dates(q1_sid, conn)
                existing_q2 = self._get_existing_dates(q2_sid, conn)

                q1_inserted = 0
                q2_inserted = 0

                for _, row in df.iterrows():
                    obs_date = row[date_col].date()

                    # Q1 nowcast
                    val = row[q1_col]
                    if not (pd.isna(val) or math.isinf(val)):
                        if obs_date not in existing_q1:
                            self._insert_raw(
                                conn=conn,
                                series_id=q1_sid,
                                obs_date=obs_date,
                                value=float(val),
                                raw_payload={"source_col": q1_col},
                            )
                            q1_inserted += 1

                    # Q2 nowcast
                    if q2_col is not None:
                        val2 = row[q2_col]
                        if not (pd.isna(val2) or math.isinf(val2)):
                            if obs_date not in existing_q2:
                                self._insert_raw(
                                    conn=conn,
                                    series_id=q2_sid,
                                    obs_date=obs_date,
                                    value=float(val2),
                                    raw_payload={"source_col": q2_col},
                                )
                                q2_inserted += 1

            log.info(
                "Nowcast GDP -- Q1: {q1} rows, Q2: {q2} rows",
                q1=q1_inserted,
                q2=q2_inserted,
            )
            results.append({
                "feature": q1_sid, "status": "SUCCESS", "rows_inserted": q1_inserted,
            })
            results.append({
                "feature": q2_sid,
                "status": "SUCCESS" if q2_col else "NO_DATA",
                "rows_inserted": q2_inserted,
            })

        except Exception as exc:
            log.error("Nowcast pull failed: {e}", e=str(exc))
            for sid in (q1_sid, q2_sid):
                results.append({
                    "feature": sid,
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "error": str(exc),
                })

        return results

    # ------------------------------------------------------------------
    # SOMA Holdings
    # ------------------------------------------------------------------

    def _pull_soma(self) -> list[dict[str, Any]]:
        """Pull SOMA holdings summary from the NY Fed Markets API.

        The summary endpoint returns current total par values broken down
        by security type (Treasuries, MBS, etc.).

        Returns:
            List of result dicts per SOMA series.
        """
        total_sid = f"{_PFX}.soma_total_par_bn"
        tsy_sid = f"{_PFX}.soma_treasury_par_bn"
        mbs_sid = f"{_PFX}.soma_mbs_par_bn"
        results: list[dict[str, Any]] = []

        try:
            # Get the as-of date for the latest SOMA snapshot
            asof_data = self._fetch_json(_SOMA_ASOF_URL)

            # Extract as-of date from response
            asof_date_str: str | None = None
            if "soma" in asof_data and "asOfDates" in asof_data["soma"]:
                dates_list = asof_data["soma"]["asOfDates"]
                if dates_list:
                    asof_date_str = dates_list[-1]  # latest
            elif "soma" in asof_data and "asOfDate" in asof_data["soma"]:
                asof_date_str = asof_data["soma"]["asOfDate"]

            if asof_date_str is None:
                # Try flat structure
                for key in ("asOfDate", "as_of_date", "date"):
                    if key in asof_data:
                        asof_date_str = str(asof_data[key])
                        break

            if asof_date_str is None:
                log.warning("SOMA: could not determine as-of date from response")
                asof_date_obj = date.today()
            else:
                asof_date_obj = datetime.strptime(
                    asof_date_str[:10], "%Y-%m-%d"
                ).date()

            # Fetch summary holdings
            summary_data = self._fetch_json(_SOMA_SUMMARY_URL)

            # Parse summary -- the Markets API returns nested JSON
            summary_list: list[dict[str, Any]] = []
            if "soma" in summary_data and "summary" in summary_data["soma"]:
                summary_list = summary_data["soma"]["summary"]
            elif "soma" in summary_data:
                # Might be directly under soma
                val = summary_data["soma"]
                if isinstance(val, list):
                    summary_list = val
                elif isinstance(val, dict):
                    summary_list = [val]
            elif isinstance(summary_data, list):
                summary_list = summary_data

            total_par: float = 0.0
            treasury_par: float = 0.0
            mbs_par: float = 0.0

            for entry in summary_list:
                par_raw = entry.get("parValue", entry.get("par_value", 0))
                par_val = float(par_raw) / 1_000_000_000  # Convert to billions

                sec_type = str(
                    entry.get("securityType", entry.get("security_type", ""))
                ).upper()

                total_par += par_val

                if "TREASURY" in sec_type or "NOTE" in sec_type or "BOND" in sec_type:
                    treasury_par += par_val
                elif "MBS" in sec_type or "MORTGAGE" in sec_type:
                    mbs_par += par_val

            # If no breakdown found but total is available, try top-level keys
            if total_par == 0.0 and summary_list:
                for entry in summary_list:
                    for key in ("totalParValue", "total_par_value", "total"):
                        if key in entry:
                            total_par = float(entry[key]) / 1_000_000_000
                            break

            with self.engine.begin() as conn:
                existing_total = self._get_existing_dates(total_sid, conn)
                existing_tsy = self._get_existing_dates(tsy_sid, conn)
                existing_mbs = self._get_existing_dates(mbs_sid, conn)

                for sid, val, existing in (
                    (total_sid, total_par, existing_total),
                    (tsy_sid, treasury_par, existing_tsy),
                    (mbs_sid, mbs_par, existing_mbs),
                ):
                    inserted = 0
                    if val > 0 and asof_date_obj not in existing:
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=asof_date_obj,
                            value=val,
                            raw_payload={
                                "asof_date": str(asof_date_obj),
                                "raw_value_usd": val * 1_000_000_000,
                            },
                        )
                        inserted = 1
                    results.append({
                        "feature": sid,
                        "status": "SUCCESS" if val > 0 else "NO_DATA",
                        "rows_inserted": inserted,
                    })

            log.info(
                "SOMA -- total={t:.1f}B, treasury={tr:.1f}B, mbs={m:.1f}B (as of {d})",
                t=total_par,
                tr=treasury_par,
                m=mbs_par,
                d=asof_date_obj,
            )

        except Exception as exc:
            log.error("SOMA pull failed: {e}", e=str(exc))
            for sid in (total_sid, tsy_sid, mbs_sid):
                results.append({
                    "feature": sid,
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "error": str(exc),
                })

        return results

    # ------------------------------------------------------------------
    # Treasury Operations
    # ------------------------------------------------------------------

    def _pull_treasury_ops(self) -> list[dict[str, Any]]:
        """Pull Treasury securities open market operations (last 2 weeks).

        Aggregates total accepted amounts per operation date.

        Returns:
            List with a single result dict.
        """
        sid = f"{_PFX}.tsy_ops_total_bn"
        rows_inserted = 0

        try:
            data = self._fetch_json(_TSY_OPS_URL)

            # Parse operations list
            ops_list: list[dict[str, Any]] = []
            if "treasury" in data and "auctions" in data["treasury"]:
                ops_list = data["treasury"]["auctions"]
            elif "treasury" in data:
                val = data["treasury"]
                if isinstance(val, list):
                    ops_list = val
                elif isinstance(val, dict) and "results" in val:
                    ops_list = val["results"]
            elif isinstance(data, list):
                ops_list = data

            # Aggregate total accepted amount per operation date
            daily_totals: dict[date, float] = {}
            for op in ops_list:
                # Extract date
                op_date_str = str(
                    op.get("operationDate", op.get("operation_date", op.get("date", "")))
                )
                if not op_date_str or op_date_str == "":
                    continue
                try:
                    op_date = datetime.strptime(op_date_str[:10], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    continue

                # Extract accepted amount
                amt_raw = op.get(
                    "totalAmtAccepted",
                    op.get("total_amt_accepted", op.get("accepted", 0)),
                )
                try:
                    amt = float(amt_raw) / 1_000_000_000  # Convert to billions
                except (ValueError, TypeError):
                    continue

                if math.isnan(amt) or math.isinf(amt):
                    log.warning(
                        "TSY ops: invalid amount on {d}: {a}",
                        d=op_date,
                        a=amt_raw,
                    )
                    continue

                daily_totals[op_date] = daily_totals.get(op_date, 0.0) + amt

            with self.engine.begin() as conn:
                existing = self._get_existing_dates(sid, conn)

                for op_date, total_bn in sorted(daily_totals.items()):
                    if op_date in existing:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=op_date,
                        value=total_bn,
                        raw_payload={"raw_total_usd": total_bn * 1_000_000_000},
                    )
                    rows_inserted += 1

            log.info(
                "TSY ops -- {n} days, {r} new rows inserted",
                n=len(daily_totals),
                r=rows_inserted,
            )

        except Exception as exc:
            log.error("Treasury ops pull failed: {e}", e=str(exc))
            return [{
                "feature": sid,
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }]

        return [{
            "feature": sid,
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
        }]

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull all NY Fed series: nowcast, SOMA, and Treasury operations.

        Returns:
            List of result dicts per series with status and rows_inserted.
        """
        results: list[dict[str, Any]] = []

        # 1. GDP Nowcast
        log.info("NYFed: pulling GDP nowcast...")
        results.extend(self._pull_nowcast())

        # 2. SOMA holdings
        log.info("NYFed: pulling SOMA holdings...")
        results.extend(self._pull_soma())

        # 3. Treasury operations
        log.info("NYFed: pulling Treasury operations...")
        results.extend(self._pull_treasury_ops())

        # Summary
        total_rows = sum(r.get("rows_inserted", 0) for r in results)
        succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "NYFed pull_all -- {ok}/{total} series, {rows} total rows inserted",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
