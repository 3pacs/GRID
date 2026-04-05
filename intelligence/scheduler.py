"""
GRID Intelligence Scheduler — background loop for periodic intelligence tasks.

Runs hourly briefings, capital flow refreshes, daily context pulls,
nightly research, taxonomy audits, celestial briefings, dealer flow
briefings, options recommendations/tracking, and paper trading signals.

Extracted from api/main.py lifespan to keep the API entry point lean
and make the scheduler independently testable.
"""

from __future__ import annotations

import time

import schedule as _sched
from loguru import logger as log


def run_intelligence_loop() -> None:
    """Background loop: hourly briefings, 4h capital flows, daily wiki + crypto.

    This function blocks forever (designed to run in a daemon thread).
    All task failures are caught and logged — the loop never crashes.
    """
    from config import Settings

    _s = Settings()  # noqa: F841 — kept for future use by scheduled tasks

    # ── Task definitions ────────────────────────────────────────────────

    def _hourly_briefing() -> None:
        try:
            from ollama.market_briefing import MarketBriefingEngine
            from db import get_engine as _ge
            mbe = MarketBriefingEngine(db_engine=_ge())
            mbe.generate_briefing("hourly", save=True)
            log.info("Hourly briefing generated (intelligence loop)")
        except Exception as exc:
            log.debug("Hourly briefing failed: {e}", e=str(exc))

    def _capital_flow_refresh() -> None:
        try:
            from analysis.capital_flows import CapitalFlowResearchEngine
            from db import get_engine as _ge
            cfe = CapitalFlowResearchEngine(db_engine=_ge())
            cfe.run_research(force=True)
            log.info("Capital flow refresh complete (intelligence loop)")
        except Exception as exc:
            log.debug("Capital flow refresh failed: {e}", e=str(exc))

    def _daily_context() -> None:
        try:
            from ingestion.wiki_history import WikiHistoryPuller
            from db import get_engine as _ge
            wp = WikiHistoryPuller(db_engine=_ge())
            data = wp.pull_today()
            wp.save_to_db(data)
            log.info("Wiki history ingested: {n} events", n=len(data.get("wiki_events", [])))
        except Exception as exc:
            log.debug("Wiki history failed: {e}", e=str(exc))

        try:
            from ingestion.coingecko import CoinGeckoPuller
            from db import get_engine as _ge
            cg = CoinGeckoPuller(_ge())
            cg.pull_all()
            log.info("CoinGecko crypto prices refreshed (intelligence loop)")
        except Exception as exc:
            log.debug("CoinGecko pull failed: {e}", e=str(exc))

        try:
            from ingestion.social_sentiment import SocialSentimentPuller
            from db import get_engine as _ge
            sp = SocialSentimentPuller(db_engine=_ge())
            result = sp.pull_all()
            sp.save_to_db(result)
            log.info("Social sentiment: {s}", s=result.get("summary", ""))
        except Exception as exc:
            log.debug("Social sentiment failed: {e}", e=str(exc))

    def _nightly_research() -> None:
        try:
            from analysis.research_agent import run_full_research
            from db import get_engine as _ge
            result = run_full_research(_ge())
            log.info("Nightly research complete: {r}", r=str(result)[:200])
        except Exception as exc:
            log.debug("Nightly research failed: {e}", e=str(exc))

    def _taxonomy_audit() -> None:
        try:
            from analysis.taxonomy_audit import run_taxonomy_audit
            from db import get_engine as _ge
            report = run_taxonomy_audit(_ge())
            fixes = len(report.get("auto_fixes", []))
            recs = len(report.get("recommendations", []))
            log.info(
                "Taxonomy audit: {f} auto-fixes, {r} recommendations, {c}% coverage",
                f=fixes, r=recs, c=report.get("stats", {}).get("coverage_pct", 0),
            )
        except Exception as exc:
            log.debug("Taxonomy audit failed: {e}", e=str(exc))

    def _price_fallback() -> None:
        """Pull stale equity/crypto prices via fallback sources."""
        try:
            from ingestion.price_fallback import PriceFallbackPuller
            from db import get_engine as _ge
            from sqlalchemy import text as _t

            eng = _ge()
            pfp = PriceFallbackPuller(db_engine=eng)
            with eng.connect() as conn:
                stale = conn.execute(_t(
                    "SELECT fr.name FROM feature_registry fr "
                    "LEFT JOIN LATERAL ("
                    "  SELECT obs_date FROM resolved_series WHERE feature_id = fr.id "
                    "  ORDER BY obs_date DESC LIMIT 1"
                    ") rs ON TRUE "
                    "WHERE fr.model_eligible = TRUE AND fr.family IN ('equity','crypto','commodity') "
                    "AND (rs.obs_date IS NULL OR rs.obs_date < CURRENT_DATE - 1) "
                    "AND fr.name LIKE '%\\_full' ESCAPE '\\'"
                )).fetchall()
            tickers = [r[0].replace('_full', '').upper().replace('_', '-') for r in stale]
            if tickers:
                results = pfp.pull_many(tickers[:20])
                pfp.save_to_db(results)
                log.info("Price fallback: {n}/{t} stale tickers refreshed", n=len(results), t=len(tickers))
        except Exception as exc:
            log.debug("Price fallback failed: {e}", e=str(exc))

    def _paper_trading_signals() -> None:
        try:
            from trading.signal_executor import execute_signals
            from db import get_engine as _ge
            result = execute_signals(_ge())
            log.info(
                "Paper trading: {o} opened, {c} closed",
                o=result.get("trades_opened", 0), c=result.get("trades_closed", 0),
            )
        except Exception as exc:
            log.debug("Paper trading signals failed: {e}", e=str(exc))

    def _celestial_briefing() -> None:
        try:
            from ollama.celestial_briefing import generate_celestial_briefing
            from db import get_engine as _ge
            result = generate_celestial_briefing(_ge())
            log.info("Celestial briefing generated: {n} chars", n=len(result.get("content", "")))
        except Exception as exc:
            log.debug("Celestial briefing failed: {e}", e=str(exc))

    def _weekly_astro_correlations() -> None:
        try:
            from analysis.astro_correlations import AstroCorrelationEngine
            from db import get_engine as _ge
            ace = AstroCorrelationEngine(_ge())
            results = ace.get_cached_or_compute(force_refresh=True)
            log.info("Weekly astro correlations: {n} significant pairs", n=len(results))
        except Exception as exc:
            log.debug("Astro correlations failed: {e}", e=str(exc))

    def _dealer_flow_briefing() -> None:
        try:
            from ollama.dealer_flow_briefing import generate_dealer_flow_briefing
            from db import get_engine as _ge
            result = generate_dealer_flow_briefing(_ge())
            log.info("Dealer flow briefing generated: {n} chars", n=len(result.get("content", "")))
        except Exception as exc:
            log.debug("Dealer flow briefing failed: {e}", e=str(exc))

    def _options_recommendations() -> None:
        try:
            from trading.options_recommender import OptionsRecommender
            from db import get_engine as _ge
            rec = OptionsRecommender(db_engine=_ge())
            recs = rec.generate_recommendations()
            log.info("Options recommendations generated: {n} recommendations", n=len(recs))
        except Exception as exc:
            log.debug("Options recommendations failed: {e}", e=str(exc))

    def _options_tracker() -> None:
        try:
            from trading.options_tracker import run_improvement_cycle
            from db import get_engine as _ge
            result = run_improvement_cycle(_ge())
            log.info(
                "Options tracker cycle complete — scored={s}",
                s=result.get("scoring_summary", {}).get("scored", 0),
            )
        except Exception as exc:
            log.debug("Options tracker failed: {e}", e=str(exc))

    # ── Schedule registration ───────────────────────────────────────────

    _sched.every(1).hours.do(_paper_trading_signals)
    _sched.every(1).hours.do(_hourly_briefing)
    _sched.every(4).hours.do(_capital_flow_refresh)
    _sched.every(6).hours.do(_price_fallback)
    _sched.every().day.at("02:00").do(_nightly_research)
    _sched.every().day.at("02:30").do(_taxonomy_audit)
    _sched.every().day.at("06:00").do(_daily_context)
    _sched.every().day.at("07:00").do(_options_recommendations)
    _sched.every().day.at("10:00").do(_celestial_briefing)
    _sched.every().day.at("15:00").do(_dealer_flow_briefing)
    _sched.every().day.at("18:00").do(_daily_context)
    _sched.every().sunday.at("03:00").do(_weekly_astro_correlations)
    _sched.every(7).days.do(_options_tracker)

    log.info(
        "Intelligence loop started — hourly briefings, 4h capital flows, "
        "6h price fallback, nightly research, daily context, weekly astro "
        "correlations, dealer flow briefing, daily options recommendations, "
        "weekly options tracker"
    )

    # ── Run forever ─────────────────────────────────────────────────────

    while True:
        _sched.run_pending()
        time.sleep(30)
