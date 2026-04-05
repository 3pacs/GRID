"""
GRID Smart Scheduler — runs only due/stale pullers per cycle.

Replaces the old pattern of running ALL 50+ pullers every pipeline cycle.
Each puller has an expected frequency. On each tick, we check which pullers
are overdue and run only those, capped at MAX_PULLERS_PER_TICK to keep
cycles short (< 5 minutes).

Pullers that fail or timeout get exponential backoff cooldowns. The old
full pipeline (run_full_pipeline.py) is still available for manual runs.

Usage from Hermes:
    from ingestion.smart_scheduler import SmartScheduler
    sched = SmartScheduler(engine)
    result = sched.tick()  # runs 3-5 due pullers, returns in < 5 min
"""

from __future__ import annotations

import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Puller Registry ──────────────────────────────────────────────────────
# Every puller with its import path, method, and expected update frequency.
# Frequency is in hours. "8" means run every 8 hours at most.

PULLER_REGISTRY: list[dict[str, Any]] = [
    # ── Fast domestic (run frequently) ──
    {"name": "yfinance",          "mod": "ingestion.yfinance_pull",       "cls": "YFinancePuller",           "method": "pull_all",  "freq_h": 4,  "timeout_s": 120},
    {"name": "options",           "mod": "ingestion.options",             "cls": "OptionsPuller",            "method": "pull_all",  "freq_h": 6,  "timeout_s": 180},
    {"name": "coingecko",         "mod": "ingestion.coingecko",           "cls": "CoinGeckoPuller",          "method": "pull_all",  "freq_h": 4,  "timeout_s": 60},
    {"name": "fred",              "mod": "ingestion.fred",                "cls": "FREDPuller",               "method": "pull_all",  "freq_h": 12, "timeout_s": 120, "api_key": "FRED_API_KEY"},

    # ── Alt data (daily) ──
    {"name": "insider_filings",   "mod": "ingestion.altdata.insider_filings",   "cls": "InsiderFilingsPuller",     "method": "pull_all",      "freq_h": 12, "timeout_s": 120},
    {"name": "congressional",     "mod": "ingestion.altdata.congressional",     "cls": "CongressionalTradingPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {"name": "unusual_whales",    "mod": "ingestion.altdata.unusual_whales",    "cls": "UnusualWhalesPuller",      "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_odds",   "mod": "ingestion.altdata.prediction_odds",   "cls": "PredictionOddsPuller",     "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "kalshi",            "mod": "ingestion.altdata.kalshi",            "cls": "KalshiPuller",             "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "prediction_pmxt",   "mod": "ingestion.altdata.prediction_pmxt",   "cls": "PmxtPredictionPuller",     "method": "pull",          "freq_h": 12, "timeout_s": 120},
    {"name": "smart_money",       "mod": "ingestion.altdata.smart_money",       "cls": "SmartMoneyPuller",         "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "fed_liquidity",     "mod": "ingestion.altdata.fed_liquidity",     "cls": "FedLiquidityPuller",       "method": "pull_all",      "freq_h": 12, "timeout_s": 60, "api_key": "FRED_API_KEY"},
    {"name": "etf_flows",         "mod": "ingestion.altdata.institutional_flows","cls": "InstitutionalFlowsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120},
    {"name": "analyst_ratings",   "mod": "ingestion.altdata.analyst_ratings",   "cls": "AnalystRatingsPuller",     "method": "pull_all",      "freq_h": 24, "timeout_s": 60},
    {"name": "gdelt",             "mod": "ingestion.altdata.gdelt",             "cls": "GDELTPuller",              "method": "pull_recent",   "freq_h": 12, "timeout_s": 60},
    {"name": "news_scraper",      "mod": "ingestion.altdata.news_scraper",      "cls": "NewsScraperPuller",        "method": "pull_all",      "freq_h": 6,  "timeout_s": 60},
    {"name": "opportunity",       "mod": "ingestion.altdata.opportunity",       "cls": "OppInsightsPuller","method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (daily, but slower) ──
    {"name": "ecb",               "mod": "ingestion.international.ecb",         "cls": "ECBPuller",                "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "bcb",               "mod": "ingestion.international.bcb",         "cls": "BCBPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "mas",               "mod": "ingestion.international.mas",         "cls": "MASPuller",                "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "akshare",           "mod": "ingestion.international.akshare_macro","cls": "AKShareMacroPuller",      "method": "pull_all",      "freq_h": 48, "timeout_s": 120},
    {"name": "rbi",               "mod": "ingestion.international.rbi",         "cls": "RBIPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Weekly ──
    {"name": "oecd",              "mod": "ingestion.international.oecd",        "cls": "OECDPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "bis",               "mod": "ingestion.international.bis",         "cls": "BISPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "imf",               "mod": "ingestion.international.imf",         "cls": "IMFPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "dark_pool",         "mod": "ingestion.altdata.dark_pool",         "cls": "DarkPoolPuller",           "method": "pull_weekly",   "freq_h": 168, "timeout_s": 60},
    {"name": "gov_contracts",     "mod": "ingestion.altdata.gov_contracts",     "cls": "GovContractsPuller",       "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "supply_chain",      "mod": "ingestion.altdata.supply_chain",      "cls": "SupplyChainPuller",        "method": "pull_all",      "freq_h": 168, "timeout_s": 60},

    # ── Monthly / slow (run rarely) ──
    {"name": "campaign_finance",  "mod": "ingestion.altdata.campaign_finance",  "cls": "CampaignFinancePuller",    "method": "pull_all",      "freq_h": 720, "timeout_s": 300},
    {"name": "lobbying",          "mod": "ingestion.altdata.lobbying",          "cls": "LobbyingPuller",           "method": "pull_all",      "freq_h": 720, "timeout_s": 120},
    {"name": "export_controls",   "mod": "ingestion.altdata.export_controls",   "cls": "ExportControlsPuller",     "method": "pull_all",      "freq_h": 720, "timeout_s": 60},

    # ── New intelligence sources (from PR merge) ──
    {"name": "fara",              "mod": "ingestion.altdata.fara",              "cls": "FARAPuller",               "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "foia_cables",       "mod": "ingestion.altdata.foia_cables",       "cls": "FOIACablesPuller",         "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Corporate registry / asset cross-reference ──
    {"name": "uk_companies",     "mod": "ingestion.altdata.uk_companies_house", "cls": "UKCompaniesHousePuller", "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "UK_COMPANIES_HOUSE_KEY"},
    {"name": "opencorporates",   "mod": "ingestion.altdata.opencorporates",     "cls": "OpenCorporatesPuller",   "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "asset_registries", "mod": "ingestion.altdata.asset_registries",  "cls": "AssetRegistryPuller",    "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Solana / memecoin scanners (from PR merge) ──
    {"name": "telegram_scanner",  "mod": "ingestion.altdata.telegram_scanner",  "cls": "TelegramScanner",          "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},
    {"name": "discord_scanner",   "mod": "ingestion.altdata.discord_scanner",   "cls": "DiscordScanner",           "method": "pull_all",      "freq_h": 4,  "timeout_s": 60},

    # ── Celestial ──
    {"name": "planetary",         "mod": "ingestion.celestial.planetary",       "cls": "PlanetaryAspectPuller",          "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "lunar",             "mod": "ingestion.celestial.lunar",           "cls": "LunarCyclePuller",         "method": "pull_all",      "freq_h": 24, "timeout_s": 30},
    {"name": "solar",             "mod": "ingestion.celestial.solar",           "cls": "SolarActivityPuller",      "method": "pull_all",      "freq_h": 24, "timeout_s": 30},

    # ── Paid APIs (MUST RUN — user is paying for these) ──
    {"name": "tiingo",            "mod": "ingestion.tiingo_pull",              "cls": "TiingoPuller",             "method": "pull_all",      "freq_h": 4,  "timeout_s": 120, "api_key": "TIINGO_API_KEY"},
    {"name": "tiingo_news",       "mod": "ingestion.tiingo_news_pull",         "cls": "TiingoNewsPuller",         "method": "pull_all",      "freq_h": 6,  "timeout_s": 120, "api_key": "TIINGO_API_KEY"},
    {"name": "tiingo_fundamentals","mod": "ingestion.tiingo_fundamentals_pull","cls": "TiingoFundamentalsPuller", "method": "pull_all",      "freq_h": 24, "timeout_s": 120, "api_key": "TIINGO_API_KEY"},
    {"name": "quiverquant",       "mod": "ingestion.quiverquant",             "cls": "QuiverQuantPuller",        "method": "pull_all",      "freq_h": 12, "timeout_s": 120, "api_key": "QUIVERQUANT_API_KEY"},

    # ── Crypto (DexScreener, PumpFun) ──
    {"name": "dexscreener",       "mod": "ingestion.dexscreener",             "cls": "DexScreenerPuller",        "method": "pull_aggregate_signals", "freq_h": 4,  "timeout_s": 60},
    {"name": "pumpfun",           "mod": "ingestion.pumpfun",                 "cls": "PumpFunPuller",            "method": "pull_all",      "freq_h": 6,  "timeout_s": 60},

    # ── Government / regulatory ──
    {"name": "bls",               "mod": "ingestion.bls",                     "cls": "BLSPuller",                "method": "pull_all",      "freq_h": 168, "timeout_s": 120},
    {"name": "edgar",             "mod": "ingestion.edgar",                   "cls": "EDGARPuller",              "method": "pull_all",      "freq_h": 24, "timeout_s": 180},
    {"name": "cftc_cot",          "mod": "ingestion.altdata.cftc_cot",        "cls": "CFTCCOTPuller",            "method": "pull_all",      "freq_h": 168, "timeout_s": 120},

    # ── Sentiment / alt ──
    {"name": "world_news",        "mod": "ingestion.altdata.world_news",      "cls": "WorldNewsPuller",          "method": "pull_all",      "freq_h": 6,  "timeout_s": 60, "api_key": "WORLDNEWS_API_KEY"},
    {"name": "fear_greed",        "mod": "ingestion.altdata.fear_greed",      "cls": "FearGreedPuller",          "method": "pull_all",      "freq_h": 12, "timeout_s": 30},
    {"name": "social_sentiment",  "mod": "ingestion.social_sentiment",        "cls": "SocialSentimentPuller",    "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "polymarket",        "mod": "ingestion.altdata.polymarket",      "cls": "PolymarketPuller",         "method": "pull_all",      "freq_h": 12, "timeout_s": 60},
    {"name": "wiki_history",      "mod": "ingestion.wiki_history",            "cls": "WikiHistoryPuller",        "method": "pull_all",      "freq_h": 24, "timeout_s": 60},

    # ── International (missing) ──
    {"name": "eurostat",          "mod": "ingestion.international.eurostat",   "cls": "EurostatPuller",           "method": "pull_all",      "freq_h": 168, "timeout_s": 180},
    {"name": "kosis",             "mod": "ingestion.international.kosis",     "cls": "KOSISPuller",              "method": "pull_all",      "freq_h": 168, "timeout_s": 120, "api_key": "KOSIS_API_KEY"},
]

# How many pullers to run per tick (keeps cycles short)
MAX_PULLERS_PER_TICK = 8

# Per-tick time budget (seconds) — stop scheduling more if we're over this
TICK_TIME_BUDGET_S = 300  # 5 minutes


class SmartScheduler:
    """Runs only due/stale pullers each tick, with per-source cooldowns."""

    # Maximum number of concurrent puller threads
    MAX_CONCURRENT_THREADS = 10

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        # In-memory tracking: {name: {last_success, last_attempt, consecutive_fails, cooldown_until}}
        self._state: dict[str, dict[str, Any]] = {}
        # Thread concurrency control
        self._thread_semaphore = threading.Semaphore(self.MAX_CONCURRENT_THREADS)
        self._active_threads: set[str] = set()
        self._threads_lock = threading.Lock()
        self._load_state_from_db()

    def _load_state_from_db(self) -> None:
        """Bootstrap state from source_catalog.last_pull_at."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT name, last_pull_at FROM source_catalog "
                    "WHERE last_pull_at IS NOT NULL"
                )).fetchall()
                for r in rows:
                    self._state[r[0].lower()] = {
                        "last_success": r[1],
                        "last_attempt": r[1],
                        "consecutive_fails": 0,
                        "cooldown_until": None,
                    }
            log.debug("SmartScheduler loaded {n} source states from DB", n=len(self._state))
        except Exception as exc:
            log.warning("SmartScheduler DB state load failed: {e}", e=str(exc))

    def _is_due(self, puller: dict) -> bool:
        """Check if a puller needs to run based on its frequency."""
        name = puller["name"]
        state = self._state.get(name)

        # Never run before → definitely due
        if state is None:
            return True

        # In cooldown after failure → not due
        cooldown = state.get("cooldown_until")
        if cooldown and datetime.now(timezone.utc) < cooldown:
            return False

        # Check if enough time has passed since last success
        last = state.get("last_success")
        if last is None:
            return True

        if hasattr(last, "tzinfo") and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        return age_hours >= puller["freq_h"]

    def _get_due_pullers(self) -> list[dict]:
        """Return pullers that are due, sorted by priority (most overdue first)."""
        due = []
        for p in PULLER_REGISTRY:
            if self._is_due(p):
                # Calculate how overdue (for priority sorting)
                state = self._state.get(p["name"])
                if state and state.get("last_success"):
                    last = state["last_success"]
                    if hasattr(last, "tzinfo") and last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    overdue_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600 - p["freq_h"]
                else:
                    overdue_h = 9999  # never run = most overdue
                due.append({**p, "_overdue_h": overdue_h})

        # Most overdue first, but fast pullers (low timeout) get priority
        due.sort(key=lambda x: (-x["_overdue_h"], x["timeout_s"]))
        return due

    def _run_puller(self, puller: dict) -> dict[str, Any]:
        """Import, instantiate, and run a single puller with timeout.

        Uses a semaphore to cap concurrent threads at MAX_CONCURRENT_THREADS
        and tracks active threads for observability.
        """
        import importlib
        import os

        name = puller["name"]
        timeout_s = puller.get("timeout_s", 120)
        result: dict[str, Any] = {"name": name, "status": "UNKNOWN"}

        # Acquire semaphore (non-blocking) to enforce thread limit
        if not self._thread_semaphore.acquire(blocking=False):
            with self._threads_lock:
                active = list(self._active_threads)
            log.warning(
                "SmartScheduler: thread limit ({lim}) reached, skipping {n} — active: {a}",
                lim=self.MAX_CONCURRENT_THREADS, n=name, a=active,
            )
            result["status"] = "SKIPPED"
            result["reason"] = f"Thread limit ({self.MAX_CONCURRENT_THREADS}) reached"
            return result

        # Register this thread as active
        with self._threads_lock:
            self._active_threads.add(name)

        try:
            mod = importlib.import_module(puller["mod"])
            cls = getattr(mod, puller["cls"])

            if "api_key" in puller:
                key_val = os.getenv(puller["api_key"], "")
                if not key_val:
                    result["status"] = "SKIPPED"
                    result["reason"] = f"Missing API key: {puller['api_key']}"
                    return result
                instance = cls(key_val, self.engine)
            else:
                instance = cls(db_engine=self.engine)

            method = getattr(instance, puller["method"])

            # Run with timeout — don't let any puller block for minutes
            out_box: list[Any] = [None]
            err_box: list[Exception | None] = [None]

            def _target() -> None:
                try:
                    out_box[0] = method()
                except Exception as e:
                    err_box[0] = e

            t = threading.Thread(target=_target, daemon=True)
            t.start()
            t.join(timeout=timeout_s)

            if t.is_alive():
                result["status"] = "TIMEOUT"
                result["error"] = f"Exceeded {timeout_s}s timeout"
                log.warning(
                    "SmartScheduler: {n} TIMEOUT after {s}s — skipping",
                    n=name, s=timeout_s,
                )
                return result

            if err_box[0]:
                raise err_box[0]

            result["status"] = "SUCCESS"
            result["detail"] = str(out_box[0])[:200] if out_box[0] else ""
            self._update_last_pull(name)

        except Exception as exc:
            result["status"] = "FAILED"
            result["error"] = str(exc)[:200]
            log.warning("SmartScheduler: {n} failed: {e}", n=name, e=str(exc))

        finally:
            # Always clean up: release semaphore and unregister thread
            with self._threads_lock:
                self._active_threads.discard(name)
            self._thread_semaphore.release()

        return result

    def _update_last_pull(self, name: str) -> None:
        """Update source_catalog.last_pull_at for a source."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE source_catalog SET last_pull_at = NOW() "
                    "WHERE LOWER(name) = :n"
                ), {"n": name.lower()})
        except Exception:
            pass  # best effort

    def _record_result(self, name: str, success: bool, error: str | None = None) -> None:
        """Record puller result and manage cooldowns."""
        state = self._state.get(name, {"consecutive_fails": 0})
        state["last_attempt"] = datetime.now(timezone.utc)

        if success:
            state["last_success"] = datetime.now(timezone.utc)
            state["consecutive_fails"] = 0
            state["cooldown_until"] = None
        else:
            fails = state.get("consecutive_fails", 0) + 1
            state["consecutive_fails"] = fails
            # Exponential backoff: 30min, 1h, 2h, 4h, 8h, max 24h
            cooldown_min = min(30 * (2 ** (fails - 1)), 1440)
            state["cooldown_until"] = (
                datetime.now(timezone.utc) + timedelta(minutes=cooldown_min)
            )
            log.info(
                "SmartScheduler: {n} failed {f}x, cooldown {c}min",
                n=name, f=fails, c=cooldown_min,
            )

        self._state[name] = state

    def tick(self) -> dict[str, Any]:
        """Run one scheduler tick — execute up to MAX_PULLERS_PER_TICK due pullers.

        Returns summary of what ran, what succeeded, what failed.
        """
        tick_start = time.monotonic()
        due = self._get_due_pullers()

        summary: dict[str, Any] = {
            "total_due": len(due),
            "ran": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "results": [],
            "still_due": [],
        }

        if not due:
            log.info("SmartScheduler tick: nothing due")
            return summary

        log.info(
            "SmartScheduler tick: {n} pullers due, running up to {m}",
            n=len(due), m=MAX_PULLERS_PER_TICK,
        )

        for puller in due[:MAX_PULLERS_PER_TICK]:
            # Check time budget
            elapsed = time.monotonic() - tick_start
            if elapsed > TICK_TIME_BUDGET_S:
                log.info("SmartScheduler: time budget exhausted ({e:.0f}s), deferring rest", e=elapsed)
                break

            name = puller["name"]
            log.info("SmartScheduler: running {n} (overdue {h:.0f}h)", n=name, h=puller.get("_overdue_h", 0))

            result = self._run_puller(puller)
            success = result["status"] == "SUCCESS"
            self._record_result(name, success, result.get("error"))

            summary["results"].append(result)
            summary["ran"] += 1
            if success:
                summary["succeeded"] += 1
            elif result["status"] == "SKIPPED":
                summary["skipped"] += 1
            else:
                summary["failed"] += 1

        # Report what's still due for next tick
        summary["still_due"] = [p["name"] for p in due[MAX_PULLERS_PER_TICK:]]

        elapsed = time.monotonic() - tick_start
        log.info(
            "SmartScheduler tick complete in {e:.1f}s — "
            "{ok}/{ran} succeeded, {f} failed, {d} still due",
            e=elapsed, ok=summary["succeeded"], ran=summary["ran"],
            f=summary["failed"], d=len(summary["still_due"]),
        )
        return summary

    def get_status(self) -> dict[str, Any]:
        """Return current scheduler state for the API."""
        due = self._get_due_pullers()
        in_cooldown = [
            {
                "name": name,
                "fails": s.get("consecutive_fails", 0),
                "cooldown_until": s["cooldown_until"].isoformat() if s.get("cooldown_until") else None,
            }
            for name, s in self._state.items()
            if s.get("cooldown_until") and datetime.now(timezone.utc) < s["cooldown_until"]
        ]
        return {
            "total_registered": len(PULLER_REGISTRY),
            "total_due": len(due),
            "due_names": [p["name"] for p in due[:20]],
            "in_cooldown": in_cooldown,
            "max_per_tick": MAX_PULLERS_PER_TICK,
            "tick_budget_s": TICK_TIME_BUDGET_S,
        }
