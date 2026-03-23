"""
GRID ingestion scheduler (v1 — DEPRECATED).

Use ``scheduler_v2.py`` instead, which includes all international, trade,
physical, and alternative data sources in addition to the core feeds below.

This module is retained for backwards compatibility but will be removed
in a future release.

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

    # Options chain pull
    try:
        from db import get_engine
        from ingestion.options import OptionsPuller

        engine = get_engine()
        puller = OptionsPuller(db_engine=engine)
        results = puller.pull_all()
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_snaps = sum(r.get("snapshots", 0) for r in results)
        log.info(
            "Options daily pull complete — {ok}/{total} tickers, {snaps} snapshots",
            ok=succeeded, total=len(results), snaps=total_snaps,
        )
    except Exception as exc:
        log.error("Options daily pull failed: {err}", err=str(exc))

    # Options mispricing scan (runs after pull)
    try:
        from db import get_engine
        from discovery.options_scanner import OptionsScanner

        engine = get_engine()
        scanner = OptionsScanner(engine)
        opps = scanner.scan_all(min_score=5.0)
        n_100x = sum(1 for o in opps if o.is_100x)
        if opps:
            scanner.persist_scan(opps)
        log.info(
            "Options mispricing scan — {n} opportunities, {x} potential 100x+",
            n=len(opps), x=n_100x,
        )
    except Exception as exc:
        log.error("Options mispricing scan failed: {err}", err=str(exc))

    # Regime detection (runs after all data is fresh)
    try:
        from scripts.auto_regime import run_auto_regime
        result = run_auto_regime()
        log.info(
            "Auto regime detection — state={s}, confidence={c}",
            s=result.get("regime", "?"),
            c=result.get("confidence", "?"),
        )
    except Exception as exc:
        log.error("Auto regime detection failed: {err}", err=str(exc))

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
    """Start the unified GRID ingestion scheduler.

    Registers all domestic (v1) and international/trade/physical (v2)
    schedules on a single ``schedule`` instance with one run loop.

    Domestic schedules:
    - Daily pulls at 6:00 PM ET on weekdays (FRED, yfinance, EDGAR Form 4)
    - Monthly pulls on the 5th (BLS, EDGAR 13F)
    - Weekly SEC velocity on Sundays at 10:00 AM

    International/extended schedules:
    - Daily at 8:00 PM ET on weekdays (ECB, BCB, MAS, AKShare, etc.)
    - Weekly on Sundays at 3:00 AM (OECD, BIS, IMF, etc.)
    - Monthly on the 2nd at 4:00 AM (Comtrade, Eurostat, NOAA, VIIRS)
    - Annual on Jan 15 at 4:30 AM (Atlas ECI, WIOD, EU KLEMS, Patents)

    Runs ``schedule.run_pending()`` in a single loop with 60-second sleep.
    """
    log.info("Starting GRID unified ingestion scheduler")

    # For ongoing pulls, use recent date
    ongoing_start = date.today().isoformat()

    # --- Domestic schedules (v1) ---

    # Daily pulls at 6:00 PM (18:00) on weekdays
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at("18:00").do(
            run_daily_pulls, start_date=ongoing_start
        )

    # Monthly BLS pull on the 5th (check daily, run if day == 5)
    def _monthly_check() -> None:
        if date.today().day == 5:
            run_monthly_pulls(start_date=ongoing_start)

    schedule.every().day.at("09:00").do(_monthly_check)

    # Weekly SEC velocity pull on Sundays
    def _weekly_velocity() -> None:
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

    # --- International/extended schedules (v2) ---
    try:
        from db import get_engine
        from ingestion.scheduler_v2 import run_pull_group

        ext_engine = get_engine()

        # Daily at 8:00 PM on weekdays
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at("20:00").do(
                run_pull_group, "daily", ext_engine
            )

        # Weekly on Sundays at 3:00 AM
        schedule.every().sunday.at("03:00").do(run_pull_group, "weekly", ext_engine)

        # Monthly on the 2nd
        def _monthly_extended() -> None:
            if date.today().day == 2:
                run_pull_group("monthly", ext_engine)

        schedule.every().day.at("04:00").do(_monthly_extended)

        # Annual on January 15
        def _annual_extended() -> None:
            if date.today().month == 1 and date.today().day == 15:
                run_pull_group("annual", ext_engine)

        schedule.every().day.at("04:30").do(_annual_extended)

        log.info("International/trade/physical schedules registered")
    except Exception as exc:
        log.warning(
            "Extended schedules (international/trade/physical) not registered: {e}",
            e=str(exc),
        )

    log.info(
        "Unified scheduler configured — entering run loop (Ctrl+C to stop)"
    )

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
