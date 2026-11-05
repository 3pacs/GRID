"""
GRID AKShare China macro ingestion module.

Pulls Chinese macroeconomic data (M2, TSF, industrial production, PMI, trade)
via the AKShare library. Includes credit impulse derivation.
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller, log_pull_failure

# AKShare is an optional dependency. When missing, every series in
# AKSHARE_SERIES used to crash with `ModuleNotFoundError` and emit a
# fresh ERROR row per (series × scheduler cycle) = 132 noise rows.
# Detect once at import time and skip cleanly.
try:
    import akshare  # noqa: F401  (probe only)

    _AKSHARE_AVAILABLE = True
except Exception:  # ImportError + any transitive package issue
    _AKSHARE_AVAILABLE = False

# (AKShare function, feature name, value column) for every pulled series.
#
# 2026-11-04 rebuild, from a probe of akshare 1.18.52 on grid-svr:
# * The jin10-backed "*_yearly" / "*_yoy" functions (industrial production,
#   PMI, GDP, CPI, PPI, imports, exports) stopped updating at 2025-09 and
#   took 3-22 s each; together with the 3 s pause per series the run blew
#   the scheduler's 120 s budget (TIMEOUT, "overdue 765h"). They are
#   replaced by the eastmoney-backed functions, which are current
#   (2026-09/10) and answer in ~1 s.
# * macro_china_fx_reserves no longer exists in akshare; FX reserves come
#   from macro_china_fx_gold.
# * Eastmoney tables carry several value columns, so each series names its
#   column explicitly. The old heuristic matched none of the M2 table's
#   columns, warned, and fell back to the first numeric column -- the M2
#   *level*, not its growth rate -- and the "2026年09月份" month labels never
#   parsed, so 0 rows were written.
# obs_date is the first day of the reference period (month, or the last
# quarter of a cumulative "第1-3季度" GDP figure), not the release date the
# jin10 tables used.
AKSHARE_PULLS: tuple[tuple[str, str, str], ...] = (
    ("macro_china_money_supply", "china_m2_yoy", "货币和准货币(M2)-同比增长"),
    ("macro_china_new_financial_credit", "china_tss_yoy", "当月"),
    ("macro_china_gyzjz", "china_indpro_yoy", "同比增长"),
    ("macro_china_pmi", "china_pmi_mfg", "制造业-指数"),
    ("macro_china_gdp", "china_gdp_yoy", "国内生产总值-同比增长"),
    ("macro_china_cpi", "china_cpi_yoy", "全国-同比增长"),
    ("macro_china_ppi", "china_ppi_yoy", "当月同比增长"),
    ("macro_china_hgjck", "china_imports_yoy", "当月进口额-同比增长"),
    ("macro_china_hgjck", "china_exports_yoy", "当月出口额-同比增长"),
    ("macro_china_fx_gold", "china_fx_reserves", "国家外汇储备-数值"),
    ("macro_china_real_estate", "china_real_estate_index", "最新值"),
)

# AKShare function -> feature name (informational; one entry per function).
AKSHARE_SERIES: dict[str, str] = {fn: feat for fn, feat, _ in AKSHARE_PULLS}

_RATE_LIMIT_DELAY: float = 1.5

_CN_MONTH_RE = re.compile(r"^(\d{4})年(\d{1,2})月")
_CN_QUARTER_RE = re.compile(r"^(\d{4})年第(?:\d-)?(\d)季度")
_COMPACT_MONTH_RE = re.compile(r"^(\d{4})(\d{2})$")


def parse_akshare_period(raw: Any) -> date | None:
    """Parse AKShare period labels to the first day of the period.

    Handles ``2026年09月份``, ``2026年第1-3季度`` / ``2026年第2季度``,
    ``202609`` and anything pandas can parse (``2025-12-01``, date objects).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    m = _CN_MONTH_RE.match(s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)
    m = _CN_QUARTER_RE.match(s)
    if m:
        quarter = int(m.group(2))
        if 1 <= quarter <= 4:
            return date(int(m.group(1)), (quarter - 1) * 3 + 1, 1)
        return None
    m = _COMPACT_MONTH_RE.match(s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)
    try:
        return pd.Timestamp(s).date()
    except (ValueError, TypeError):
        return None


class AKShareMacroPuller(BasePuller):
    """Pulls China macro data via the AKShare library."""

    SOURCE_NAME = "AKShare"
    SOURCE_CONFIG = {"base_url": "https://akshare.akfamily.xyz", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": False, "revision_behavior": "RARE", "trust_score": "MED", "priority_rank": 23}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("AKShareMacroPuller initialised — source_id={sid}", sid=self.source_id)

    def _find_date_column(self, df: pd.DataFrame) -> str | None:
        """Identify the date column in AKShare DataFrames.

        AKShare uses Chinese column names: 月份 (month), 日期 (date),
        统计时间 (statistical period), etc.
        """
        date_candidates = ["月份", "季度", "日期", "统计时间", "年份", "date", "Date", "时间"]
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

    def pull_series(
        self,
        ak_function_name: str,
        feature_name: str,
        value_column: str | None = None,
        frame: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Pull a single AKShare series.

        Parameters:
            ak_function_name: Name of the AKShare function to call.
            feature_name: Canonical feature name for storage.
            value_column: Exact column holding this series' value. When the
                table no longer has it the pull FAILS with the column list
                instead of guessing another column.
            frame: An already-fetched table for ``ak_function_name`` (two
                features share ``macro_china_hgjck``).
        """
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        if not _AKSHARE_AVAILABLE:
            # Stay silent at the per-series level — pull_all logs once.
            result["status"] = "SKIPPED"
            result["errors"].append("akshare not installed")
            return result

        log.info("Pulling AKShare {fn} via {ak}", fn=feature_name, ak=ak_function_name)
        try:
            import akshare as ak

            # Call the AKShare function dynamically
            func = getattr(ak, ak_function_name, None)
            if func is None:
                result["status"] = "FAILED"
                result["errors"].append(f"AKShare function {ak_function_name} not found")
                return result

            df = frame if frame is not None else func()

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            date_col = self._find_date_column(df)
            if date_col is None:
                result["status"] = "FAILED"
                result["errors"].append("No date column found")
                return result

            if value_column is not None:
                if value_column not in df.columns:
                    result["status"] = "FAILED"
                    result["errors"].append(
                        f"value column {value_column!r} missing from {ak_function_name}; "
                        f"columns: {list(df.columns)}"
                    )
                    log.warning(
                        "AKShare {fn}: value column {vc!r} missing from {ak} (columns {cols})",
                        fn=feature_name, vc=value_column, ak=ak_function_name, cols=list(df.columns),
                    )
                    return result
                value_col = value_column
            else:
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
                        obs_dt = parse_akshare_period(row[date_col])
                        if obs_dt is None:
                            continue

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
            log_pull_failure("AKShare", feature_name, exc)
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    @staticmethod
    def _fetch_frame(ak_function_name: str) -> pd.DataFrame | None:
        """Fetch one AKShare table once for features that share it."""
        try:
            import akshare as ak

            func = getattr(ak, ak_function_name, None)
            return func() if func is not None else None
        except Exception as exc:  # noqa: BLE001 — pull_series re-fetches and reports
            log.debug("AKShare shared fetch {f} failed: {e}", f=ak_function_name, e=exc)
            return None

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

        # Short-circuit when the optional `akshare` dependency is missing —
        # otherwise every feature in AKSHARE_SERIES raises ImportError and
        # logs an ERROR, flooding errors.jsonl (132 rows/week observed).
        try:
            import akshare  # noqa: F401
        except ImportError as exc:
            log.warning(
                "AKShare dependency not installed ({e}); skipping all {n} "
                "China macro series for this cycle",
                e=str(exc), n=len(AKSHARE_SERIES),
            )
            return {
                "source": "AKShare",
                "total_rows": 0,
                "succeeded": 0,
                "total": 0,
                "skipped_reason": "akshare package not installed",
            }

        results: list[dict[str, Any]] = []

        frames: dict[str, pd.DataFrame | None] = {}
        for ak_func, feature, value_col in AKSHARE_PULLS:
            shared = sum(1 for f, _, _ in AKSHARE_PULLS if f == ak_func) > 1
            frame = frames.get(ak_func) if shared else None
            if shared and ak_func not in frames:
                frame = self._fetch_frame(ak_func)
                frames[ak_func] = frame
            res = self.pull_series(ak_func, feature, value_column=value_col, frame=frame)
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
