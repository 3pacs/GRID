"""
GRID unified ingestion scheduler.

Orchestrates daily, weekly, monthly, and annual data pulls using the
``schedule`` library. Includes both domestic (FRED, yfinance, EDGAR,
options) and international/trade/physical sources.

Idempotency: uses ``_last_run`` timestamps to prevent duplicate pulls
if the server restarts mid-period.  DB failures during pulls are retried
with exponential backoff.
"""

from __future__ import annotations

import time
from datetime import date

import schedule
from loguru import logger as log

# Track last successful run per schedule type for idempotency
_last_run: dict[str, str] = {}


def _should_run(key: str, period: str) -> bool:
    """Return True if the task hasn't run in the current period."""
    today = date.today()
    last = _last_run.get(key)
    if last is None:
        return True
    last_date = date.fromisoformat(last)
    if period == "day":
        return last_date < today
    elif period == "month":
        return (last_date.year, last_date.month) < (today.year, today.month)
    elif period == "year":
        return last_date.year < today.year
    return True


def _mark_run(key: str) -> None:
    """Record that a scheduled task ran successfully today."""
    _last_run[key] = date.today().isoformat()


def _with_db_retry(fn, *args, max_retries: int = 3, **kwargs):
    """Execute fn with DB connection retry on failure (exponential backoff)."""
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            err_str = str(exc).lower()
            is_db_error = any(s in err_str for s in (
                "connection refused", "timeout", "could not connect",
                "server closed", "broken pipe", "connection reset",
            ))
            if is_db_error and attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                log.warning(
                    "DB error in {fn} (attempt {a}/{m}), retrying in {w}s: {e}",
                    fn=fn.__name__, a=attempt + 1, m=max_retries, w=wait, e=str(exc),
                )
                time.sleep(wait)
            else:
                raise


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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("FRED", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("yfinance", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("EDGAR Form 4", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("Options chain", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("Options mispricing scan", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("Auto regime detection", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("BLS", str(exc))
        except Exception:
            pass

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
        try:
            from alerts.email import alert_on_failure
            alert_on_failure("EDGAR 13F", str(exc))
        except Exception:
            pass

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

    # Monthly BLS pull on the 5th (idempotent — won't re-run if already done this month)
    def _monthly_check() -> None:
        if date.today().day >= 5 and _should_run("monthly_bls", "month"):
            _with_db_retry(run_monthly_pulls, start_date=ongoing_start)
            _mark_run("monthly_bls")

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
            try:
                from alerts.email import alert_on_failure
                alert_on_failure("SEC velocity", str(exc))
            except Exception:
                pass

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

        # Monthly on the 2nd (idempotent)
        def _monthly_extended() -> None:
            if date.today().day >= 2 and _should_run("monthly_intl", "month"):
                _with_db_retry(run_pull_group, "monthly", ext_engine)
                _mark_run("monthly_intl")

        schedule.every().day.at("04:00").do(_monthly_extended)

        # Annual on January 15 (idempotent)
        def _annual_extended() -> None:
            if date.today().month == 1 and date.today().day >= 15 and _should_run("annual_intl", "year"):
                _with_db_retry(run_pull_group, "annual", ext_engine)
                _mark_run("annual_intl")

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
