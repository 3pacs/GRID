"""
GRID unified ingestion scheduler.

Orchestrates daily, weekly, monthly, and annual data pulls using the
``schedule`` library. Includes domestic (FRED, yfinance, EDGAR, options),
international (ECB, BCB, MAS, etc.), trade (Comtrade, CEPII, etc.),
and physical/alt data sources in a single scheduler.

Idempotency: uses ``_last_run`` timestamps to prevent duplicate pulls
if the server restarts mid-period.  DB failures during pulls are retried
with exponential backoff.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import schedule
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

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


# ── International/extended pull group infrastructure ──────────────────


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
            from ingestion.altdata.world_news import WorldNewsPuller
            pullers.append(("WorldNews", WorldNewsPuller(db_engine), "pull_all", {"days_back": 2}))
        except Exception as exc:
            log.warning("WorldNews puller init failed: {err}", err=str(exc))
        try:
            from ingestion.physical.ofr import OFRPuller
            pullers.append(("OFR", OFRPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("OFR puller init failed: {err}", err=str(exc))
        # JQuants — Japanese market indices (daily)
        try:
            from ingestion.international.jquants import JQuantsPuller
            pullers.append(("JQuants", JQuantsPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("JQuants puller init failed: {err}", err=str(exc))
        # EDINET — Japanese corporate filings (daily)
        try:
            from ingestion.international.edinet import EDINETPuller
            pullers.append(("EDINET", EDINETPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("EDINET puller init failed: {err}", err=str(exc))
        # Alpha Vantage News Sentiment — per-ticker sentiment (daily, rate-limited)
        try:
            from ingestion.altdata.alphavantage_sentiment import AlphaVantageSentimentPuller
            pullers.append(("alphavantage_news_sentiment", AlphaVantageSentimentPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("AlphaVantage sentiment puller init failed: {err}", err=str(exc))
        try:
            from ingestion.altdata.nyfed import NYFedPuller
            pullers.append(("NYFed", NYFedPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("NYFed puller init failed: {err}", err=str(exc))
        # Congressional trading disclosures (daily check, ~45-day lag)
        try:
            from ingestion.altdata.congressional import CongressionalTradingPuller
            pullers.append(("Congress_Trading", CongressionalTradingPuller(db_engine), "pull_all", {"days_back": 7}))
        except Exception as exc:
            log.warning("Congressional trading puller init failed: {err}", err=str(exc))
        # SEC Form 4 insider filings (daily)
        try:
            from ingestion.altdata.insider_filings import InsiderFilingsPuller
            pullers.append(("SEC_Insider", InsiderFilingsPuller(db_engine), "pull_all", {"days_back": 1}))
        except Exception as exc:
            log.warning("Insider filings puller init failed: {err}", err=str(exc))
        # Unusual options flow — whale-level activity from yfinance chains (daily)
        try:
            from ingestion.altdata.unusual_whales import UnusualWhalesPuller
            pullers.append(("Unusual_Whales", UnusualWhalesPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Unusual Whales puller init failed: {err}", err=str(exc))
        # Polymarket prediction market rapid-change detector (daily)
        try:
            from ingestion.altdata.prediction_odds import PredictionOddsPuller
            pullers.append(("Prediction_Odds", PredictionOddsPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Prediction Odds puller init failed: {err}", err=str(exc))
        # Social smart money — Reddit + Finviz insider tracking (daily)
        try:
            from ingestion.altdata.smart_money import SmartMoneyPuller
            pullers.append(("Smart_Money", SmartMoneyPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("Smart Money puller init failed: {err}", err=str(exc))

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
        # HuggingFace Financial News Multi-Source (weekly)
        try:
            from ingestion.altdata.hf_financial_news import HFFinancialNewsPuller
            pullers.append(("hf_financial_news", HFFinancialNewsPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("HF Financial News puller init failed: {err}", err=str(exc))
        # FINRA dark pool per-ticker volume (weekly, ~2-week lag)
        try:
            from ingestion.altdata.dark_pool import DarkPoolPuller
            pullers.append(("DarkPool", DarkPoolPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("DarkPool puller init failed: {err}", err=str(exc))
        # Supply chain leading indicators — Freightos + Drewry + ISM (weekly)
        try:
            from ingestion.altdata.supply_chain import SupplyChainPuller
            fred_key = config.get("FRED_API_KEY", "")
            pullers.append(("Supply_Chain", SupplyChainPuller(db_engine, fred_api_key=fred_key), "pull_all", {}))
        except Exception as exc:
            log.warning("Supply Chain puller init failed: {err}", err=str(exc))

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
        # CEPII BACI — bilateral trade data (monthly)
        try:
            from ingestion.trade.cepii import CEPIIPuller
            pullers.append(("CEPII_BACI", CEPIIPuller(db_engine), "pull_all", {}))
        except Exception as exc:
            log.warning("CEPII puller init failed: {err}", err=str(exc))

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


def backfill_all(start_date: str = "1970-01-01") -> None:
    """Run all pull groups with historical start dates.

    Sequential execution to avoid rate limiting.

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


def run_pushshift_backfill(data_dir: str = "/data/pushshift") -> dict[str, Any]:
    """Run Pushshift Reddit historical backfill (manual task).

    Processes all .zst dump files in data_dir.  Not scheduled — run
    manually after downloading dumps via scripts/download_pushshift.py.

    Parameters:
        data_dir: Directory containing Pushshift .zst dump files.

    Returns:
        Summary dict from PushshiftRedditPuller.ingest_directory().
    """
    from db import get_engine

    log.info("Starting Pushshift Reddit backfill from {d}", d=data_dir)

    engine = get_engine()

    from ingestion.altdata.pushshift_reddit import PushshiftRedditPuller

    puller = PushshiftRedditPuller(db_engine=engine)
    result = puller.ingest_directory(data_dir)

    log.info(
        "Pushshift backfill complete — {fp} files, {ri} rows, {err} errors",
        fp=result["files_processed"],
        ri=result["rows_inserted"],
        err=len(result["errors"]),
    )
    return result


# ── Domestic pull functions ───────────────────────────────────────────


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
            from alerts.email import alert_on_failure_with_fix
            alert_on_failure_with_fix("FRED", str(exc), {
                "diagnose": 'curl -s "https://api.stlouisfed.org/fred/series?api_key=$FRED_API_KEY&series_id=DFF&file_type=json" | head -5',
                "fix": "Check FRED_API_KEY in .env",
                "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.fred import FREDPuller; from config import settings; from db import get_engine; FREDPuller(settings.FRED_API_KEY, get_engine()).pull_all()"',
                "file": "grid/ingestion/fred.py",
            })
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
            from alerts.email import alert_on_failure_with_fix
            alert_on_failure_with_fix("yfinance", str(exc), {
                "diagnose": "python -c \"import yfinance; print(yfinance.Ticker('SPY').info.get('regularMarketPrice'))\"",
                "fix": "pip install --upgrade yfinance",
                "retry": 'cd /data/grid_v4/grid_repo/grid && python -c "from ingestion.yfinance_pull import YFinancePuller; from db import get_engine; YFinancePuller(get_engine()).pull_all()"',
                "file": "grid/ingestion/yfinance_pull.py",
            })
        except Exception:
            pass

    # Auto-fallback for stale price features
    try:
        from ingestion.price_fallback import PriceFallbackPuller
        stale_query = text(
            "SELECT fr.name FROM feature_registry fr "
            "LEFT JOIN LATERAL ("
            "  SELECT obs_date FROM resolved_series WHERE feature_id = fr.id "
            "  ORDER BY obs_date DESC LIMIT 1"
            ") rs ON TRUE "
            "WHERE fr.model_eligible = TRUE AND fr.family IN ('equity','crypto','commodity') "
            "AND (rs.obs_date IS NULL OR rs.obs_date < CURRENT_DATE - 1) "
            "AND fr.name LIKE '%_full'"
        )
        with engine.connect() as conn:
            stale = conn.execute(stale_query).fetchall()
        tickers = [r[0].replace('_full', '').upper().replace('_', '-') for r in stale]
        if tickers:
            pfp = PriceFallbackPuller(db_engine=engine)
            results = pfp.pull_many(tickers[:30])
            pfp.save_to_db(results)
            log.info("Price fallback: {n}/{t} stale tickers refreshed", n=len(results), t=len(tickers))
    except Exception as exc:
        log.warning("Price fallback failed: {e}", e=str(exc))

    # CoinGecko crypto prices (daily)
    try:
        from ingestion.coingecko import CoinGeckoPuller
        cg = CoinGeckoPuller(engine)
        cg.pull_all()
        log.info("CoinGecko crypto pull complete")
    except Exception as exc:
        log.warning("CoinGecko pull failed: {e}", e=str(exc))

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

    # Celestial / esoteric feature computation
    try:
        from ingestion.celestial.lunar import LunarCyclePuller
        from ingestion.celestial.planetary import PlanetaryAspectPuller
        from ingestion.celestial.vedic import VedicAstroPuller
        from ingestion.celestial.chinese import ChineseCalendarPuller
        from ingestion.celestial.solar import SolarActivityPuller

        engine = get_engine()
        for PullerClass in [LunarCyclePuller, PlanetaryAspectPuller, VedicAstroPuller, ChineseCalendarPuller, SolarActivityPuller]:
            try:
                puller = PullerClass(db_engine=engine)
                result = puller.pull_all()
                log.info("{cls} — {rows} rows", cls=PullerClass.__name__, rows=result.get("rows_inserted", 0))
            except Exception as exc:
                log.warning("{cls} failed: {e}", cls=PullerClass.__name__, e=str(exc))
    except Exception as exc:
        log.debug("Celestial data pull skipped: {e}", e=str(exc))

    # Google Trends sentiment pull
    try:
        from db import get_engine
        from ingestion.altdata.google_trends import GoogleTrendsPuller

        engine = get_engine()
        gt_puller = GoogleTrendsPuller(db_engine=engine)
        gt_results = gt_puller.pull_all(days_back=30)
        gt_rows = sum(r["rows_inserted"] for r in gt_results)
        log.info("Google Trends daily pull — {n} rows", n=gt_rows)
    except Exception as exc:
        log.warning("Google Trends pull failed: {err}", err=str(exc))

    # CBOE volatility indices pull
    try:
        from db import get_engine
        from ingestion.altdata.cboe_indices import CBOEIndicesPuller

        engine = get_engine()
        cboe_puller = CBOEIndicesPuller(db_engine=engine)
        cboe_results = cboe_puller.pull_all(days_back=30)
        cboe_rows = sum(r["rows_inserted"] for r in cboe_results)
        log.info("CBOE indices daily pull — {n} rows", n=cboe_rows)
    except Exception as exc:
        log.warning("CBOE indices pull failed: {err}", err=str(exc))

    # Federal Reserve speeches and FOMC calendar
    try:
        from db import get_engine
        from ingestion.altdata.fed_speeches import FedSpeechPuller

        engine = get_engine()
        fed_puller = FedSpeechPuller(db_engine=engine)
        fed_results = fed_puller.pull_all(days_back=30)
        fed_rows = sum(r["rows_inserted"] for r in fed_results)
        log.info("Fed speeches daily pull — {n} rows", n=fed_rows)
    except Exception as exc:
        log.warning("Fed speeches pull failed: {err}", err=str(exc))

    # Repo and money market stress indicators
    try:
        from config import settings
        from db import get_engine
        from ingestion.altdata.repo_market import RepoMarketPuller

        engine = get_engine()
        repo_puller = RepoMarketPuller(
            api_key=settings.FRED_API_KEY, db_engine=engine
        )
        repo_results = repo_puller.pull_all(days_back=30)
        repo_rows = sum(r["rows_inserted"] for r in repo_results)
        log.info("Repo market daily pull — {n} rows", n=repo_rows)
    except Exception as exc:
        log.warning("Repo market pull failed: {err}", err=str(exc))

    # Full yield curve pull
    try:
        from config import settings
        from db import get_engine
        from ingestion.altdata.yield_curve_full import FullYieldCurvePuller

        engine = get_engine()
        yc_puller = FullYieldCurvePuller(
            api_key=settings.FRED_API_KEY, db_engine=engine
        )
        yc_results = yc_puller.pull_all(days_back=30)
        yc_rows = sum(r["rows_inserted"] for r in yc_results)
        log.info("Full yield curve daily pull — {n} rows", n=yc_rows)
    except Exception as exc:
        log.warning("Full yield curve pull failed: {err}", err=str(exc))

    # Crucix bridge — 25+ intelligence sources from Crucix app
    try:
        from ingestion.crucix_bridge import CrucixBridgePuller
        from db import get_engine

        engine = get_engine()
        crucix = CrucixBridgePuller(db_engine=engine)
        result = crucix.pull_all()
        log.info(
            "Crucix bridge — {rows} rows from {src} sources",
            rows=result["rows_inserted"],
            src=result["sources_processed"],
        )
    except Exception as exc:
        log.warning("Crucix bridge failed: {err}", err=str(exc))

    # FinBERT sentiment scoring (runs after text sources are fresh)
    try:
        from db import get_engine
        from ingestion.ml.finbert_scorer import FinBERTScorer

        engine = get_engine()
        fb_scorer = FinBERTScorer(db_engine=engine, batch_size=64)
        fb_results = fb_scorer.score_all_sources()
        fb_total = sum(r.get("rows_scored", 0) for r in fb_results)
        fb_ok = sum(1 for r in fb_results if r.get("status") == "SUCCESS")
        log.info(
            "FinBERT scoring — {n} rows across {ok}/{total} sources",
            n=fb_total, ok=fb_ok, total=len(fb_results),
        )
    except Exception as exc:
        log.warning("FinBERT scoring failed: {err}", err=str(exc))

    # Regime detection (runs after all data is fresh)
    try:
        from scripts.auto_regime import run
        result = run()
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

        from config import settings
        start_year = int(str(start_date)[:4])
        engine = get_engine()
        bls = BLSPuller(db_engine=engine, api_key=settings.BLS_API_KEY or None)
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


# ── Scheduler entry point ─────────────────────────────────────────────


def start_scheduler() -> None:
    """Start the unified GRID ingestion scheduler.

    All source schedules are registered on a single ``schedule`` instance:

    Domestic:
    - Daily pulls at 6:00 PM ET on weekdays (FRED, yfinance, EDGAR, options, etc.)
    - Weekend Crucix bridge at 6:00 PM (OSINT is 24/7)
    - Monthly pulls on the 5th at 9:00 AM (BLS, EDGAR 13F)
    - Weekly SEC velocity on Sundays at 10:00 AM

    International/trade/physical:
    - Daily at 8:00 PM ET on weekdays (ECB, BCB, MAS, AKShare, JQuants, EDINET, etc.)
    - Weekly on Sundays at 3:00 AM (OECD, BIS, IMF, etc.)
    - Monthly on the 2nd at 4:00 AM (Comtrade, Eurostat, NOAA, VIIRS, CEPII)
    - Annual on Jan 15 at 4:30 AM (Atlas ECI, WIOD, EU KLEMS, Patents)

    Runs ``schedule.run_pending()`` in a single loop with 60-second sleep.
    """
    log.info("Starting GRID unified ingestion scheduler")

    # For ongoing pulls, use recent date
    ongoing_start = date.today().isoformat()

    # --- Domestic schedules ---

    # Daily pulls at 6:00 PM (18:00) on weekdays
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at("18:00").do(
            run_daily_pulls, start_date=ongoing_start
        )

    # Weekend Crucix bridge pull (OSINT data is 24/7)
    def _weekend_crucix() -> None:
        try:
            from ingestion.crucix_bridge import CrucixBridgePuller
            from db import get_engine as _get_engine

            _engine = _get_engine()
            crucix = CrucixBridgePuller(db_engine=_engine)
            result = crucix.pull_all()
            log.info(
                "Weekend Crucix bridge — {rows} rows from {src} sources",
                rows=result["rows_inserted"],
                src=result["sources_processed"],
            )
        except Exception as exc:
            log.warning("Weekend Crucix bridge failed: {err}", err=str(exc))

    for day in ["saturday", "sunday"]:
        getattr(schedule.every(), day).at("18:00").do(_weekend_crucix)

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

    # --- International/extended schedules (now unified) ---
    try:
        from db import get_engine

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
    elif len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        start = sys.argv[2] if len(sys.argv) > 2 else "1970-01-01"
        backfill_all(start_date=start)
    elif len(sys.argv) > 1 and sys.argv[1] == "--group":
        group = sys.argv[2] if len(sys.argv) > 2 else "daily"
        from db import get_engine
        run_pull_group(group, get_engine())
    elif len(sys.argv) > 1 and sys.argv[1] == "--pushshift":
        data_dir = sys.argv[2] if len(sys.argv) > 2 else "/data/pushshift"
        run_pushshift_backfill(data_dir=data_dir)
    else:
        start_scheduler()
