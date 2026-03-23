"""
GRID AKShare China macro ingestion module.

Pulls Chinese macroeconomic data (M2, TSF, industrial production, PMI, trade)
via the AKShare library. Includes credit impulse derivation.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# AKShare function -> feature name mapping
AKSHARE_SERIES: dict[str, str] = {
    "macro_china_money_supply": "china_m2_yoy",
    "macro_china_new_financial_credit": "china_tss_yoy",
    "macro_china_industrial_production_yoy": "china_indpro_yoy",
    "macro_china_pmi_yearly": "china_pmi_mfg",
    "macro_china_gdp_yearly": "china_gdp_yoy",
    "macro_china_cpi_yearly": "china_cpi_yoy",
    "macro_china_ppi_yearly": "china_ppi_yoy",
    "macro_china_imports_yoy": "china_imports_yoy",
    "macro_china_exports_yoy": "china_exports_yoy",
    "macro_china_fx_reserves": "china_fx_reserves",
    "macro_china_real_estate": "china_real_estate_index",
}

_RATE_LIMIT_DELAY: float = 3.0


class AKShareMacroPuller:
    """Pulls China macro data via the AKShare library."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("AKShareMacroPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "AKShare"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', FALSE, 'NEVER', 'MED', 21, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "AKShare", "url": "https://akshare.akfamily.xyz"},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    def _find_date_column(self, df: pd.DataFrame) -> str | None:
        """Identify the date column in AKShare DataFrames.

        AKShare uses Chinese column names: 月份 (month), 日期 (date),
        统计时间 (statistical period), etc.
        """
        date_candidates = ["月份", "日期", "统计时间", "年份", "date", "Date", "时间"]
        for col in date_candidates:
            if col in df.columns:
                return col
        # Fall back to first column — log warning since API may have changed
        if len(df.columns) > 0:
            log.warning(
                "AKShare: no known date column found in {cols}, falling back to '{c}'",
                cols=list(df.columns),
                c=df.columns[0],
            )
            return df.columns[0]
        return None

    def _find_value_column(self, df: pd.DataFrame, date_col: str) -> str | None:
        """Identify the primary value column.

        Looks for 今值 (current value), 最新值 (latest value), or first numeric column.
        """
        value_candidates = ["今值", "最新值", "当月", "累计", "M2同比增长"]
        for col in value_candidates:
            if col in df.columns and col != date_col:
                return col
        # Fall back to first numeric column that isn't the date
        log.warning(
            "AKShare: no known value column found in {cols}, trying first numeric column",
            cols=list(df.columns),
        )
        for col in df.columns:
            if col == date_col:
                continue
            if df[col].dtype in ("float64", "int64"):
                return col
            try:
                pd.to_numeric(df[col], errors="raise")
                return col
            except (ValueError, TypeError):
                continue
        return None

    def pull_series(self, ak_function_name: str, feature_name: str) -> dict[str, Any]:
        """Pull a single AKShare series.

        Parameters:
            ak_function_name: Name of the AKShare function to call.
            feature_name: Canonical feature name for storage.
        """
        log.info("Pulling AKShare {fn} via {ak}", fn=feature_name, ak=ak_function_name)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            import akshare as ak

            # Call the AKShare function dynamically
            func = getattr(ak, ak_function_name, None)
            if func is None:
                result["status"] = "FAILED"
                result["errors"].append(f"AKShare function {ak_function_name} not found")
                return result

            df = func()

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            date_col = self._find_date_column(df)
            if date_col is None:
                result["status"] = "FAILED"
                result["errors"].append("No date column found")
                return result

            value_col = self._find_value_column(df, date_col)
            if value_col is None:
                result["status"] = "FAILED"
                result["errors"].append("No value column found")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        # Parse date
                        date_val = row[date_col]
                        if pd.isna(date_val):
                            continue
                        obs_dt = pd.Timestamp(str(date_val)).date()

                        # Parse value
                        val_raw = row[value_col]
                        if pd.isna(val_raw) or str(val_raw).strip() in ("", "-", "--"):
                            continue
                        value = float(str(val_raw).replace(",", "").replace("%", ""))

                        if self._row_exists(feature_name, obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                        )
                        inserted += 1
                    except (ValueError, TypeError) as row_exc:
                        log.debug("Skipping AKShare row: {err}", err=str(row_exc))
                        continue

            result["rows_inserted"] = inserted
            log.info("AKShare {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("AKShare pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def _compute_credit_impulse(self) -> dict[str, Any]:
        """Derive China credit impulse: 12-month change in TSF/GDP ratio.

        credit_impulse = (TSF_t / GDP_t) - (TSF_{t-12m} / GDP_{t-12m}) * 100
        """
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                # Get TSF and GDP data
                tss_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'china_tss_yoy' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                gdp_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'china_gdp_yoy' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                if not tss_rows or not gdp_rows:
                    result["status"] = "PARTIAL"
                    result["errors"].append("Insufficient data for credit impulse")
                    return result

                # Build TSF/GDP ratio series using nearest GDP value
                gdp_dict = {r[0]: r[1] for r in gdp_rows}
                tss_dict = {r[0]: r[1] for r in tss_rows}

                ratios: dict[date, float] = {}
                for obs_dt, tss_val in tss_dict.items():
                    # Find nearest GDP value
                    gdp_val = gdp_dict.get(obs_dt)
                    if gdp_val and gdp_val != 0:
                        ratios[obs_dt] = tss_val / gdp_val * 100

                # Compute 12-month change
                sorted_dates = sorted(ratios.keys())
                inserted = 0
                for i, dt in enumerate(sorted_dates):
                    # Find date ~12 months ago
                    target = dt.replace(year=dt.year - 1) if dt.month > 1 else dt.replace(year=dt.year - 1)
                    closest = min(sorted_dates[:i], key=lambda d: abs((d - target).days), default=None)
                    if closest is None or abs((closest - target).days) > 60:
                        continue

                    impulse = ratios[dt] - ratios[closest]

                    if self._row_exists("china_credit_impulse", dt, conn):
                        continue

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": "china_credit_impulse", "src": self.source_id, "od": dt, "val": impulse},
                    )
                    inserted += 1

                result["rows_inserted"] = inserted
                log.info("China credit impulse: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("Credit impulse computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all AKShare China macro series and derive credit impulse."""
        log.info("Starting AKShare bulk pull")
        results: list[dict[str, Any]] = []

        for ak_func, feature in AKSHARE_SERIES.items():
            res = self.pull_series(ak_func, feature)
            results.append(res)
            time.sleep(_RATE_LIMIT_DELAY)

        # Derive credit impulse after pulling TSS and GDP
        impulse_result = self._compute_credit_impulse()
        results.append(impulse_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "AKShare bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "AKShare",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
