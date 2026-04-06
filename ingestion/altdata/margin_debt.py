"""
GRID — Margin Debt Monthly puller and materializer.

Populates the ``margin_debt_monthly`` table from two sources:

1. **FINRA margin statistics** — monthly, scraped from finra.org (primary)
2. **FRED series BOGZ1FL663067003Q** — quarterly, materialized from raw_series
   (fallback / historical backfill)

The FRED series gives *quarterly* data from the Federal Reserve Z.1 flow-of-funds
report. Values are in millions of USD. FINRA publishes monthly data, so FRED is
the reliable fallback for historical coverage.

Downstream consumers:
  - analysis/money_flow_engine/layer_retail.py  _build_margin_debt_node()
  - analysis/flow_thesis_data.py                _get_margin_debt_leverage_state()
  - analysis/sector_map.py                      margin_debt entry
"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


class MarginDebtPuller(BasePuller):
    """Pulls margin debt data and stores into margin_debt_monthly.

    Primary source: FRED quarterly series (already ingested into raw_series).
    Future: FINRA monthly scrape can be added as an enhancement.
    """

    SOURCE_NAME: str = "FINRA_MARGIN"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.finra.org/investors/learn-to-invest/advanced-investing/margin-statistics",
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 38,
    }

    _FRED_SERIES_ID: str = "BOGZ1FL663067003Q"

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    def pull(self) -> dict[str, Any]:
        """Run the full pull pipeline.

        Currently materializes FRED data into margin_debt_monthly.
        Returns a summary dict with status and row count.
        """
        result: dict[str, Any] = {
            "source": self.SOURCE_NAME,
            "status": "SUCCESS",
            "rows_inserted": 0,
            "errors": [],
        }

        try:
            inserted = materialize_margin_debt_from_fred(self.engine)
            result["rows_inserted"] = inserted
            log.info(
                "MarginDebtPuller: materialized {n} rows from FRED",
                n=inserted,
            )
        except Exception as exc:
            log.error("MarginDebtPuller failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result


def _store_row(
    conn: Any,
    obs_date: date,
    margin_debt: float,
    free_credit_cash: float | None = None,
    free_credit_margin: float | None = None,
    source: str = "FINRA",
) -> bool:
    """Insert a single row into margin_debt_monthly with conflict handling.

    Returns True if a new row was inserted, False if it already existed.
    """
    net_margin: float | None = None
    if margin_debt is not None:
        fcc = free_credit_cash or 0.0
        fcm = free_credit_margin or 0.0
        net_margin = margin_debt - fcc - fcm

    res = conn.execute(
        text("""
            INSERT INTO margin_debt_monthly
                (obs_date, margin_debt, free_credit_cash, free_credit_margin,
                 net_margin, source)
            VALUES (:d, :md, :fcc, :fcm, :nm, :src)
            ON CONFLICT (obs_date) DO NOTHING
        """),
        {
            "d": obs_date,
            "md": margin_debt,
            "fcc": free_credit_cash,
            "fcm": free_credit_margin,
            "nm": net_margin,
            "src": source,
        },
    )
    return res.rowcount > 0


def materialize_margin_debt_from_fred(engine: Engine) -> int:
    """Transform FRED quarterly margin debt into margin_debt_monthly rows.

    Reads from ``raw_series`` where series_id = 'BOGZ1FL663067003Q'
    and inserts into ``margin_debt_monthly``. Uses ON CONFLICT to skip
    duplicates.

    The FRED Z.1 series reports values in millions of USD.

    Returns:
        int: Number of rows inserted.
    """
    fred_series_id = "BOGZ1FL663067003Q"

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT obs_date, value
                FROM raw_series
                WHERE series_id = :sid
                  AND value IS NOT NULL
                  AND pull_status = 'SUCCESS'
                ORDER BY obs_date
            """),
            {"sid": fred_series_id},
        ).fetchall()

        if not rows:
            log.warning(
                "No FRED data found for {sid} in raw_series — "
                "run FRED puller first",
                sid=fred_series_id,
            )
            return 0

        inserted = 0
        for obs_date, value in rows:
            # FRED Z.1 values are in millions of USD; store as-is
            # (consumers interpret the unit from the source)
            was_inserted = _store_row(
                conn,
                obs_date=obs_date,
                margin_debt=float(value),
                source="FRED",
            )
            if was_inserted:
                inserted += 1

        log.info(
            "Materialized {ins}/{total} margin debt rows from FRED",
            ins=inserted,
            total=len(rows),
        )

    # Compute change_mom and change_yoy after insert
    _compute_changes(engine)

    return inserted


def _compute_changes(engine: Engine) -> None:
    """Back-fill change_mom and change_yoy columns.

    Uses window functions to compute period-over-period deltas.
    For quarterly FRED data, change_mom is the quarter-over-quarter change
    and change_yoy uses a ~4-quarter lookback.
    """
    try:
        with engine.begin() as conn:
            # Quarter-over-quarter (or month-over-month for monthly data)
            conn.execute(text("""
                UPDATE margin_debt_monthly m
                SET change_mom = m.margin_debt - prev.prev_debt
                FROM (
                    SELECT id,
                           LAG(margin_debt) OVER (ORDER BY obs_date) AS prev_debt
                    FROM margin_debt_monthly
                ) prev
                WHERE m.id = prev.id
                  AND prev.prev_debt IS NOT NULL
            """))
            # Year-over-year: find closest row ~12 months ago (3-4 quarters)
            conn.execute(text("""
                UPDATE margin_debt_monthly m
                SET change_yoy = m.margin_debt - yoy.margin_debt
                FROM margin_debt_monthly yoy
                WHERE yoy.obs_date = (
                    SELECT MAX(y2.obs_date)
                    FROM margin_debt_monthly y2
                    WHERE y2.obs_date <= m.obs_date - INTERVAL '11 months'
                      AND y2.obs_date >= m.obs_date - INTERVAL '13 months'
                )
            """))
        log.debug("margin_debt_monthly: change_mom/change_yoy computed")
    except Exception as exc:
        log.warning("Failed to compute margin debt changes: {e}", e=str(exc))
