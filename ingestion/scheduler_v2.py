"""
GRID extended ingestion scheduler (v2).

Orchestrates all international, trade, physical, and alternative data pulls
across daily, weekly, monthly, and annual schedules. Includes incremental
start date computation and full historical backfill mode.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import schedule
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _get_incremental_start(db_engine: Engine, source_name: str, overlap_days: int = 30) -> str:
    """Compute incremental start date for a source.

    Returns the most recent obs_date in raw_series for the source
    minus the overlap window, or a default historical start.
    """
    try:
        with db_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT MAX(rs.obs_date) FROM raw_series rs "
                    "JOIN source_catalog sc ON rs.source_id = sc.id "
                    "WHERE sc.name = :name AND rs.pull_status = 'SUCCESS'"
                ),
                {"name": source_name},
            ).fetchone()
            if row and row[0]:
                start = row[0] - timedelta(days=overlap_days)
                return start.isoformat()
    except Exception:
        pass
    return "1990-01-01"


def run_pull_group(group_name: str, db_engine: Engine, config: dict | None = None) -> dict[str, Any]:
    """Run all pullers in a named schedule group.

    Parameters:
        group_name: One of 'daily', 'weekly', 'monthly', 'annual'.
        db_engine: SQLAlchemy engine for database access.
        config: Optional config dict with API keys, etc.

    Returns:
        Summary dict with success/failure counts per puller.
    """
    if config is None:
        config = {}

    log.info("Starting pull group: {g}", g=group_name)
    summary: dict[str, Any] = {
        "group": group_name,
        "results": [],
        "success_count": 0,
        "failure_count": 0,
    }

    pullers = _get_pullers_for_group(group_name, db_engine, config)

    for puller_name, puller_instance, method_name, kwargs in pullers:
        try:
            log.info("Running {p}.{m}()", p=puller_name, m=method_name)

            # Resolve 'incremental' start dates
            resolved_kwargs = dict(kwargs)
            for k, v in resolved_kwargs.items():
                if v == "incremental":
                    resolved_kwargs[k] = _get_incremental_start(db_engine, puller_name)

            method = getattr(puller_instance, method_name)
            result = method(**resolved_kwargs)
            summary["results"].append({"puller": puller_name, "status": "SUCCESS", "result": result})
            summary["success_count"] += 1
            log.info("{p} complete", p=puller_name)

        except Exception as exc:
            log.error("{p} failed: {err}", p=puller_name, err=str(exc))
            summary["results"].append({"puller": puller_name, "status": "FAILED", "error": str(exc)})
            summary["failure_count"] += 1

    log.info(
        "Pull group {g} complete — {ok} succeeded, {fail} failed",
        g=group_name,
        ok=summary["success_count"],
        fail=summary["failure_count"],
    )
    return summary


def _get_pullers_for_group(
    group_name: str,
    db_engine: Engine,
    config: dict,
) -> list[tuple[str, Any, str, dict]]:
    """Instantiate pullers for a given schedule group.

    Returns list of (name, instance, method, kwargs) tuples.
    Each puller is instantiated on demand to avoid import-time failures.
    """
    pullers: list[tuple[str, Any, str, dict]] = []

    if group_name == "daily":
        try:
            from ingestion.international.ecb import ECBPuller
            pullers.append(("ECB_SDW", ECBPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("ECB puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.bcb import BCBPuller
            pullers.append(("BCB_BR", BCBPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("BCB puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.mas import MASPuller
            pullers.append(("MAS_SG", MASPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("MAS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.akshare_macro import AKShareMacroPuller
            pullers.append(("AKShare", AKShareMacroPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("AKShare puller init failed: {err}", err=str(exc))
        try:
            from ingestion.altdata.opportunity import OppInsightsPuller
            pullers.append(("OppInsights", OppInsightsPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("OppInsights puller init failed: {err}", err=str(exc))
        try:
            from ingestion.altdata.gdelt import GDELTPuller
            pullers.append(("GDELT", GDELTPuller(db_engine), "pull_recent", {"days_back": 2}))
        except Exception as exc:
            log.warning("GDELT puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.ofr import OFRPuller
            pullers.append(("OFR", OFRPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("OFR puller init failed: {err}", err=str(exc))

    elif group_name == "weekly":
        try:
            from ingestion.international.oecd import OECDPuller
            pullers.append(("OECD_SDMX", OECDPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("OECD puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.bis import BISPuller
            pullers.append(("BIS", BISPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("BIS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.imf import IMFPuller
            pullers.append(("IMF_IFS", IMFPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("IMF puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.rbi import RBIPuller
            pullers.append(("RBI", RBIPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("RBI puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.abs_au import ABSPuller
            pullers.append(("ABS_AU", ABSPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("ABS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.kosis import KOSISPuller
            api_key = config.get("KOSIS_API_KEY", "")
            pullers.append(("KOSIS", KOSISPuller(db_engine, api_key), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("KOSIS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.usda_nass import USDAPuller
            api_key = config.get("USDA_NASS_API_KEY", "")
            pullers.append(("USDA_NASS", USDAPuller(db_engine, api_key), "pull_all", {}))
        except Exception as exc:
            log.warning("USDA puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.dbnomics import DBnomicsPuller
            pullers.append(("DBnomics", DBnomicsPuller(db_engine), "pull_all", {"start_date": "incremental"}))
        except Exception as exc:
            log.warning("DBnomics puller init failed: {err}", err=str(exc))

    elif group_name == "monthly":
        try:
            from ingestion.trade.comtrade import ComtradePuller
            api_key = config.get("COMTRADE_API_KEY")
            pullers.append(("Comtrade", ComtradePuller(db_engine, api_key), "pull_all", {}))
        except Exception as exc:
            log.warning("Comtrade puller init failed: {err}", err=str(exc))
        try:
            from ingestion.international.eurostat import EurostatPuller
            pullers.append(("Eurostat", EurostatPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Eurostat puller init failed: {err}", err=str(exc))
        try:
            from ingestion.altdata.noaa_ais import NOAAAISPuller
            today = date.today()
            m = today.month - 1 if today.month > 1 else 12
            y = today.year if today.month > 1 else today.year - 1
            pullers.append(("NOAA_AIS", NOAAAISPuller(db_engine), "pull_monthly_summary", {"year": y, "month": m}))
        except Exception as exc:
            log.warning("NOAA AIS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.viirs import VIIRSPuller
            pullers.append(("VIIRS", VIIRSPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("VIIRS puller init failed: {err}", err=str(exc))

    elif group_name == "annual":
        try:
            from ingestion.trade.atlas_eci import AtlasECIPuller
            pullers.append(("Atlas_ECI", AtlasECIPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Atlas ECI puller init failed: {err}", err=str(exc))
        try:
            from ingestion.trade.wiod import WIODPuller
            pullers.append(("WIOD", WIODPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("WIOD puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.euklems import EUKLEMSPuller
            pullers.append(("EU_KLEMS", EUKLEMSPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("EU KLEMS puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.patents import PatentsPuller
            pullers.append(("USPTO_PV", PatentsPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Patents puller init failed: {err}", err=str(exc))

    return pullers


def start_scheduler_v2(use_workflows: bool = False) -> None:
    """Start the extended GRID ingestion scheduler.

    .. deprecated::
        Use ``ingestion.scheduler.start_scheduler()`` instead, which runs
        both domestic and international schedules in a single thread.
        This function remains for standalone/CLI usage only.

    Parameters:
        use_workflows: If True, read schedules from workflow files in
                       workflows/enabled/ instead of hardcoded cron.
                       Falls back to hardcoded schedule if no workflow
                       files are found.

    Default (hardcoded) schedule:
    - Daily at 8:00 PM ET: international + altdata pulls
    - Sundays at 3:00 AM: weekly statistical agencies
    - 2nd of each month at 4:00 AM: trade + physical data
    - January 15 annually: annual datasets
    """
    import warnings
    warnings.warn(
        "start_scheduler_v2() is deprecated. Use ingestion.scheduler.start_scheduler() "
        "which now includes all international/trade/physical schedules.",
        DeprecationWarning,
        stacklevel=2,
    )
    from db import get_engine

    engine = get_engine()
    log.info("Starting GRID v2 scheduler (workflow_mode={wf})", wf=use_workflows)

    if use_workflows:
        _schedule_from_workflows(engine)
    else:
        _schedule_hardcoded(engine)

    log.info("v2 scheduler configured — entering run loop (Ctrl+C to stop)")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("v2 scheduler stopped by operator")


def _schedule_from_workflows(engine: Engine) -> None:
    """Configure scheduler from enabled workflow files.

    Reads the 'schedule' field from each enabled workflow with
    group='ingestion' and registers it with the schedule library.
    """
    try:
        from workflows.loader import load_enabled, parse_schedule
    except ImportError:
        log.warning("workflows.loader not available — falling back to hardcoded")
        _schedule_hardcoded(engine)
        return

    workflows = load_enabled()
    ingestion_wfs = [w for w in workflows if w["group"] == "ingestion"]

    if not ingestion_wfs:
        log.warning("No enabled ingestion workflows — falling back to hardcoded")
        _schedule_hardcoded(engine)
        return

    # Map workflow names to pull group names
    _wf_to_group = {
        "pull-fred": "daily",
        "pull-ecb": "daily",
        "pull-yfinance": "daily",
        "pull-bls": "daily",
        "pull-weekly-intl": "weekly",
        "pull-monthly-trade": "monthly",
        "pull-annual-datasets": "annual",
    }

    registered = 0
    for wf in ingestion_wfs:
        sched = parse_schedule(wf["schedule"])
        group = _wf_to_group.get(wf["name"])
        if not group:
            log.warning("No group mapping for workflow '{n}'", n=wf["name"])
            continue

        freq = sched.get("frequency", "manual")
        time_str = sched.get("time", "20:00")

        if freq == "daily":
            days = sched.get("days", ["monday", "tuesday", "wednesday", "thursday", "friday"])
            for day in days:
                getattr(schedule.every(), day).at(time_str).do(
                    run_pull_group, group, engine
                )
            registered += 1
            log.info(
                "Scheduled '{n}' ({g}): {f} at {t} on {d}",
                n=wf["name"], g=group, f=freq, t=time_str,
                d=", ".join(days),
            )

        elif freq == "weekly":
            days = sched.get("days", ["sunday"])
            for day in days:
                getattr(schedule.every(), day).at(time_str).do(
                    run_pull_group, group, engine
                )
            registered += 1
            log.info(
                "Scheduled '{n}' ({g}): weekly on {d} at {t}",
                n=wf["name"], g=group, d=days, t=time_str,
            )

        elif freq == "monthly":
            dom = sched.get("day_of_month", 2)

            def _monthly_runner(_group=group, _dom=dom):
                if date.today().day == _dom:
                    run_pull_group(_group, engine)

            schedule.every().day.at(time_str).do(_monthly_runner)
            registered += 1
            log.info(
                "Scheduled '{n}' ({g}): monthly on day {d} at {t}",
                n=wf["name"], g=group, d=dom, t=time_str,
            )

        elif freq == "annual":
            month_str = sched.get("month", "january")
            dom = sched.get("day_of_month", 15)
            month_num = {
                "january": 1, "february": 2, "march": 3, "april": 4,
                "may": 5, "june": 6, "july": 7, "august": 8,
                "september": 9, "october": 10, "november": 11, "december": 12,
            }.get(month_str, 1)

            def _annual_runner(_group=group, _month=month_num, _dom=dom):
                if date.today().month == _month and date.today().day == _dom:
                    run_pull_group(_group, engine)

            schedule.every().day.at(time_str).do(_annual_runner)
            registered += 1
            log.info(
                "Scheduled '{n}' ({g}): annually on {m} {d} at {t}",
                n=wf["name"], g=group, m=month_str, d=dom, t=time_str,
            )

        elif freq == "manual":
            log.info("Workflow '{n}' is manual — skipping scheduler", n=wf["name"])

    log.info("Registered {n} workflow-driven schedules", n=registered)


def _schedule_hardcoded(engine: Engine) -> None:
    """Original hardcoded schedule configuration."""
    # Daily at 8:00 PM (20:00)
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at("20:00").do(
            run_pull_group, "daily", engine
        )

    # Weekly on Sundays at 3:00 AM
    schedule.every().sunday.at("03:00").do(run_pull_group, "weekly", engine)

    # Monthly on the 2nd
    def _monthly_check() -> None:
        if date.today().day == 2:
            run_pull_group("monthly", engine)

    schedule.every().day.at("04:00").do(_monthly_check)

    # Annual on January 15
    def _annual_check() -> None:
        if date.today().month == 1 and date.today().day == 15:
            run_pull_group("annual", engine)

    schedule.every().day.at("04:30").do(_annual_check)


def backfill_all(start_date: str = "1970-01-01") -> None:
    """Run all pull groups with historical start dates.

    Sequential execution to avoid rate limiting. Estimated completion:
    48-72 hours depending on API rate limits.

    Parameters:
        start_date: Earliest date for historical backfill.
    """
    from db import get_engine

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    engine = get_engine()
    groups = ["daily", "weekly", "monthly", "annual"]

    log.info("Starting full historical backfill from {sd}", sd=start_date)

    if tqdm:
        for group in tqdm(groups, desc="Backfill groups"):
            log.info("Backfilling group: {g}", g=group)
            run_pull_group(group, engine)
    else:
        for group in groups:
            log.info("Backfilling group: {g}", g=group)
            run_pull_group(group, engine)

    log.info("Historical backfill complete")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        start = sys.argv[2] if len(sys.argv) > 2 else "1970-01-01"
        backfill_all(start_date=start)
    elif len(sys.argv) > 1 and sys.argv[1] == "--group":
        group = sys.argv[2] if len(sys.argv) > 2 else "daily"
        from db import get_engine
        run_pull_group(group, get_engine())
    elif len(sys.argv) > 1 and sys.argv[1] == "--workflows":
        start_scheduler_v2(use_workflows=True)
    else:
        start_scheduler_v2()
