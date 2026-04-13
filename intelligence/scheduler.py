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

    def _crucix_ingest() -> None:
        """Bridge Crucix OSINT data into GRID every 15 minutes."""
        try:
            from ingestion.crucix_bridge import CrucixBridgePuller
            from db import get_engine as _ge
            puller = CrucixBridgePuller(_ge())
            result = puller.pull_all()
            log.info(
                "Crucix bridge: {s} series, {n} news, {sig} signals",
                s=result.get("rows_inserted", 0),
                n=result.get("news_inserted", 0),
                sig=result.get("signals_inserted", 0),
            )
        except Exception as exc:
            log.debug("Crucix bridge failed: {e}", e=str(exc))

    def _actor_news_top200() -> None:
        """INTEL-1: pull free-source news for top-200 weighted actors daily."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.actor_news_puller import (
                ActorNewsPuller,
                enumerate_sector_map_actors,
            )
            puller = ActorNewsPuller(_ge())
            actors = enumerate_sector_map_actors(priority_only=True)[:200]
            counts = {"actors": 0, "news": 0, "bios": 0}
            for actor in actors:
                try:
                    res = puller.pull_one_actor(
                        actor,
                        sources=["google_news", "gdelt", "wikipedia"],
                    )
                    counts["actors"] += 1
                    counts["news"] += res.get("news", 0)
                    counts["bios"] += res.get("bios", 0)
                except Exception as exc:  # noqa: BLE001
                    log.debug("actor_news daily skipped {a}: {e}",
                              a=actor.get("actor_id"), e=str(exc))
            log.info(
                "Actor news daily: {a} actors, {n} news, {b} bios",
                a=counts["actors"], n=counts["news"], b=counts["bios"],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Actor news daily failed: {e}", e=str(exc))

    def _actor_news_weekly_tail() -> None:
        """INTEL-1: pull free-source news for the long-tail (~3.3K) actors weekly."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.actor_news_puller import (
                ActorNewsPuller,
                enumerate_sector_map_actors,
            )
            puller = ActorNewsPuller(_ge())
            # Skip top-200 (handled daily); sweep the rest.
            actors = enumerate_sector_map_actors(priority_only=False)[200:]
            counts = {"actors": 0, "news": 0, "bios": 0}
            for actor in actors:
                try:
                    res = puller.pull_one_actor(
                        actor,
                        sources=["google_news", "wikipedia", "sec_edgar", "crossref"],
                    )
                    counts["actors"] += 1
                    counts["news"] += res.get("news", 0)
                    counts["bios"] += res.get("bios", 0)
                except Exception as exc:  # noqa: BLE001
                    log.debug("actor_news weekly skipped {a}: {e}",
                              a=actor.get("actor_id"), e=str(exc))
            log.info(
                "Actor news weekly tail: {a} actors, {n} news, {b} bios",
                a=counts["actors"], n=counts["news"], b=counts["bios"],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Actor news weekly tail failed: {e}", e=str(exc))

    # ── Schedule registration ───────────────────────────────────────────

    _sched.every(15).minutes.do(_crucix_ingest)
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
    _sched.every().day.at("03:30").do(_actor_news_top200)
    _sched.every().sunday.at("04:00").do(_actor_news_weekly_tail)

    def _actor_trust_cog_recompute() -> None:
        """INTEL-2: recompute trust-vs-cog classification for every lever puller."""
        try:
            from db import get_engine as _ge
            from intelligence.actor_trust_cog import score_all_actors
            counts = score_all_actors(_ge())
            log.info(
                "actor_trust_cog weekly: {t} trust, {c} cog, {m} mixed, {u} unknown ({n} total)",
                t=counts.get("trust", 0), c=counts.get("cog", 0),
                m=counts.get("mixed", 0), u=counts.get("unknown", 0),
                n=counts.get("total", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("actor_trust_cog weekly failed: {e}", e=str(exc))

    _sched.every().sunday.at("05:00").do(_actor_trust_cog_recompute)

    log.info(
        "Intelligence loop started — hourly briefings, 4h capital flows, "
        "6h price fallback, nightly research, daily context, weekly astro "
        "correlations, dealer flow briefing, daily options recommendations, "
        "weekly options tracker"
    )

    # ── Run forever ─────────────────────────────────────────────────────
    # Delay first run_pending by 120s so the API can serve requests before
    # heavy LLM/HTTP jobs fire (hourly tasks trigger immediately otherwise).
    time.sleep(120)
    log.info("Intelligence loop active — first run_pending cycle starting")

    while True:
        _sched.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    run_intelligence_loop()
