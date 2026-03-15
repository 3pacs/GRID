"""
GRID ingestion scheduler.

Orchestrates daily, weekly, and monthly data pulls using the ``schedule``
library.  Runs FRED and yfinance pulls on weekday evenings, EDGAR Form 4
daily, SEC velocity weekly, BLS and 13F quarterly.
"""

from __future__ import annotations

import time
from datetime import date

import schedule
from loguru import logger as log


def run_daily_pulls(start_date: str | date = "1990-01-01") -> None:
    """Execute daily FRED and yfinance data pulls.

    Pulls all configured series from FRED and all tickers from yfinance.
    Handles and logs any exception without crashing the scheduler.

    Parameters:
        start_date: Earliest observation date to fetch on first run.
                    Subsequent runs only fetch recent data.
    """
    log.info("Starting daily pulls — start_date={sd}", sd=start_date)

    # FRED pull
    try:
        from config import settings
        from db import get_engine
        from ingestion.fred import FREDPuller

        engine = get_engine()
        fred = FREDPuller(api_key=settings.FRED_API_KEY, db_engine=engine)
        results = fred.pull_all(start_date=start_date)
        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "FRED daily pull complete — {ok}/{total} series, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
    except Exception as exc:
        log.error("FRED daily pull failed: {err}", err=str(exc))

    # yfinance pull
    try:
        from db import get_engine
        from ingestion.yfinance_pull import YFinancePuller

        engine = get_engine()
        yf_puller = YFinancePuller(db_engine=engine)
        results = yf_puller.pull_all(start_date=start_date)
        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "yfinance daily pull complete — {ok}/{total} tickers, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
    except Exception as exc:
        log.error("yfinance daily pull failed: {err}", err=str(exc))

    # EDGAR Form 4 insider transactions (daily)
    try:
        from db import get_engine
        from ingestion.edgar import EDGARPuller

        engine = get_engine()
        edgar = EDGARPuller(db_engine=engine)
        result = edgar.pull_form4_transactions(days_back=1)
        log.info(
            "Form 4 daily pull complete — {rows} rows, status={st}",
            rows=result["rows_inserted"],
            st=result["status"],
        )
    except Exception as exc:
        log.error("Form 4 daily pull failed: {err}", err=str(exc))

    log.info("Daily pulls finished")


def run_monthly_pulls(start_date: str | date = "1990-01-01") -> None:
    """Execute monthly BLS data pulls.

    Pulls all configured BLS series. Handles exceptions without crashing.

    Parameters:
        start_date: Earliest year to fetch is derived from this date.
    """
    log.info("Starting monthly BLS pull — start_date={sd}", sd=start_date)

    try:
        from db import get_engine
        from ingestion.bls import BLSPuller

        start_year = int(str(start_date)[:4])
        engine = get_engine()
        bls = BLSPuller(db_engine=engine)
        result = bls.pull_series(start_year=start_year)
        log.info(
            "BLS monthly pull complete — {rows} rows, status={st}",
            rows=result["rows_inserted"],
            st=result["status"],
        )
    except Exception as exc:
        log.error("BLS monthly pull failed: {err}", err=str(exc))

    # 13F quarterly holdings (run monthly, only new filings)
    try:
        from db import get_engine
        from ingestion.edgar import EDGARPuller

        engine = get_engine()
        edgar = EDGARPuller(db_engine=engine)
        result = edgar.pull_13f_holdings(max_filings_per_fund=1)
        log.info(
            "13F pull complete — {rows} rows, status={st}",
            rows=result["rows_inserted"],
            st=result["status"],
        )
    except Exception as exc:
        log.error("13F pull failed: {err}", err=str(exc))

    log.info("Monthly pulls finished")


def start_scheduler() -> None:
    """Start the GRID ingestion scheduler.

    Schedules:
    - Daily pulls at 6:00 PM ET on weekdays (Mon–Fri)
    - Monthly pulls on the 5th of each month

    Runs ``schedule.run_pending()`` in a loop with 60-second sleep.
    Handles KeyboardInterrupt gracefully.
    """
    log.info("Starting GRID ingestion scheduler")

    # For ongoing pulls, use recent date
    ongoing_start = date.today().isoformat()

    # Schedule daily pulls at 6:00 PM (18:00) on weekdays
    schedule.every().monday.at("18:00").do(run_daily_pulls, start_date=ongoing_start)
    schedule.every().tuesday.at("18:00").do(run_daily_pulls, start_date=ongoing_start)
    schedule.every().wednesday.at("18:00").do(run_daily_pulls, start_date=ongoing_start)
    schedule.every().thursday.at("18:00").do(run_daily_pulls, start_date=ongoing_start)
    schedule.every().friday.at("18:00").do(run_daily_pulls, start_date=ongoing_start)

    # Schedule monthly BLS pull on the 5th (check daily, run if day == 5)
    def _monthly_check() -> None:
        """Run monthly pull only on the 5th of the month."""
        if date.today().day == 5:
            run_monthly_pulls(start_date=ongoing_start)

    schedule.every().day.at("09:00").do(_monthly_check)

    # Schedule weekly SEC velocity pull on Sundays
    def _weekly_velocity() -> None:
        """Run SEC 8-K velocity pull weekly."""
        try:
            from db import get_engine
            from ingestion.sec_velocity import SECVelocityPuller

            engine = get_engine()
            puller = SECVelocityPuller(db_engine=engine)
            result = puller.pull_weekly_velocity(weeks_back=1)
            log.info(
                "SEC velocity weekly pull — {rows} rows, {s} sectors",
                rows=result["rows_inserted"],
                s=result.get("sectors_found", 0),
            )
        except Exception as exc:
            log.error("SEC velocity pull failed: {err}", err=str(exc))

    schedule.every().sunday.at("10:00").do(_weekly_velocity)

    log.info("Scheduler configured — entering run loop (Ctrl+C to stop)")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Scheduler stopped by operator (KeyboardInterrupt)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--historical":
        log.info("Running historical pull (1990-01-01 to today)")
        run_daily_pulls(start_date="1990-01-01")
        run_monthly_pulls(start_date="1990-01-01")
    else:
        start_scheduler()
