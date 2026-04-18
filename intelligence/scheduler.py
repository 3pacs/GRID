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

    def _calibration_snapshot_and_drift() -> None:
        """ALPHA-7: daily per-horizon calibration snapshot + drift detection.

        Runs once per day at 02:00 UTC (between the news cycle wrap and the
        morning briefing). Writes a row to oracle_calibration_history for
        every (model, horizon) pair that has enough scored predictions,
        then runs drift detection against the trailing 30-day baseline
        and logs any alerts. The alerts are also forwarded to alerts.email
        when running on the server so the operator sees a summary on drift
        events.
        """
        try:
            from db import get_engine as _ge
            from oracle.calibration import (
                detect_calibration_drift,
                snapshot_calibration_history,
            )
            engine_cal = _ge()
            counts = snapshot_calibration_history(engine_cal)
            alerts = detect_calibration_drift(
                engine_cal, window_days=30, sigma_threshold=2.0,
            )
            log.info(
                "calibration daily: {m} models, {b} buckets snapshot, "
                "{a} drift alert(s)",
                m=counts.get("models", 0), b=counts.get("buckets", 0),
                a=len(alerts),
            )
            if alerts:
                for a in alerts:
                    log.warning(
                        "DRIFT [{sev}] {m}/{h}d {metric}: cur={cur:.4f} "
                        "base={mean:.4f}±{std:.4f} z={z:.2f}σ",
                        sev=a.severity.upper(), m=a.model_name,
                        h=a.horizon_days, metric=a.metric,
                        cur=a.current, mean=a.baseline_mean,
                        std=a.baseline_std, z=a.z_score,
                    )
        except Exception as exc:  # noqa: BLE001
            log.warning("calibration daily failed: {e}", e=str(exc))

    _sched.every().day.at("02:15").do(_calibration_snapshot_and_drift)

    # ── SWEEP: new puller hooks (CAT-25/27/30/49/71/81) ────────────────

    def _fed_h8_weekly() -> None:
        """CAT-27: H.8 bank balance sheet weekly puller."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.h8_bank_balance import run_h8_puller
            result = run_h8_puller(_ge())
            log.info(
                "h8 bank balance: {f} fetched, {i} new ({s} series)",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
                s=len(result.get("series", {})),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("h8 weekly failed: {e}", e=str(exc))

    def _mmf_composition_weekly() -> None:
        """CAT-30: Money market fund composition weekly puller."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.mmf_composition import run_mmf_puller
            result = run_mmf_puller(_ge())
            log.info(
                "mmf composition: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("mmf composition weekly failed: {e}", e=str(exc))

    def _treasury_auction_daily() -> None:
        """CAT-25: Treasury auction results EOD puller."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.treasury_auction import run_treasury_auction_puller
            result = run_treasury_auction_puller(_ge())
            log.info(
                "treasury auction: {a} auctions, {r} rows, {i} new",
                a=result.get("auctions", 0), r=result.get("rows", 0),
                i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("treasury auction daily failed: {e}", e=str(exc))

    def _freight_cass_ata_monthly() -> None:
        """CAT-81: Cass Freight + ATA Truck Tonnage monthly puller."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.freight_cass_ata import run_freight_puller
            result = run_freight_puller(_ge())
            log.info(
                "freight cass/ata: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("freight cass/ata monthly failed: {e}", e=str(exc))

    def _wage_tracker_monthly() -> None:
        """CAT-49: Atlanta Fed Wage Growth Tracker monthly puller."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.wage_tracker import run_wage_tracker_puller
            result = run_wage_tracker_puller(_ge())
            log.info(
                "wage tracker: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("wage tracker monthly failed: {e}", e=str(exc))

    def _cot_extremes_weekly() -> None:
        """CAT-35: CFTC COT extremes scan (consumes existing cftc_cot data)."""
        try:
            from db import get_engine as _ge
            from intelligence.cot_extremes import rank_contrarian_signals, scan_all_extremes
            extremes = scan_all_extremes(_ge())
            ranked = rank_contrarian_signals(extremes)
            top = ranked[:10]
            log.info(
                "COT extremes weekly: {n} scanned, {e} extreme, {m} elevated",
                n=len(extremes),
                e=sum(1 for x in extremes if x.severity == "extreme"),
                m=sum(1 for x in extremes if x.severity == "elevated"),
            )
            for ext in top:
                log.info(
                    "  {c}/{m}: {s} {d} z={z:.2f}",
                    c=ext.contract, m=ext.metric, s=ext.severity,
                    d=ext.direction, z=ext.z_score,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("COT extremes weekly failed: {e}", e=str(exc))

    def _eight_k_clusters_daily() -> None:
        """CAT-61: 8-K cluster detection over the last 90 days of filings."""
        try:
            from db import get_engine as _ge
            from intelligence.eight_k_clustering import scan_for_clusters
            alerts = scan_for_clusters(_ge())
            log.info(
                "8-K clusters: {n} tickers flagged ({c} critical, {e} elevated, {w} warn)",
                n=len(alerts),
                c=sum(1 for a in alerts if a.severity_label == "critical"),
                e=sum(1 for a in alerts if a.severity_label == "elevated"),
                w=sum(1 for a in alerts if a.severity_label == "warn"),
            )
            for a in alerts[:10]:
                log.info(
                    "  {t}: {n} filings, severity={sev} ({lbl}), top={top}",
                    t=a.ticker, n=a.filing_count,
                    sev=round(a.composite_severity, 2),
                    lbl=a.severity_label, top=a.top_item,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("8-K clusters daily failed: {e}", e=str(exc))

    # Scheduling:
    #   H.8 weekly → Fridays 17:00 UTC (after Fed release)
    #   MMF weekly → Wednesdays 22:00 UTC
    #   Treasury auction → daily 23:00 UTC
    #   Freight monthly → 21st 12:00 UTC
    #   Wage tracker monthly → 15th 12:00 UTC
    #   COT extremes → Saturdays 01:00 UTC (after CFTC Friday release)
    #   8-K clusters → daily 02:30 UTC
    _sched.every().friday.at("17:00").do(_fed_h8_weekly)
    _sched.every().wednesday.at("22:00").do(_mmf_composition_weekly)
    _sched.every().day.at("23:00").do(_treasury_auction_daily)
    _sched.every().day.at("12:00").do(_freight_cass_ata_monthly)
    _sched.every().day.at("12:05").do(_wage_tracker_monthly)
    _sched.every().saturday.at("01:00").do(_cot_extremes_weekly)
    _sched.every().day.at("02:30").do(_eight_k_clusters_daily)

    def _refinery_cracks_weekly() -> None:
        """CAT-54: refinery utilization + 3-2-1 crack spreads weekly."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.refinery_cracks import run_refinery_cracks_puller
            result = run_refinery_cracks_puller(_ge())
            log.info(
                "refinery/cracks: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("refinery/cracks weekly failed: {e}", e=str(exc))

    def _credit_card_spending_weekly() -> None:
        """CAT-75: credit card outstanding + delinquency weekly."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.credit_card_spending import run_credit_card_puller
            result = run_credit_card_puller(_ge())
            log.info(
                "credit card spending: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("credit card weekly failed: {e}", e=str(exc))

    def _buyback_execution_quarterly() -> None:
        """CAT-67: corporate buyback execution rate vs authorization."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.buyback_execution import run_buyback_puller
            result = run_buyback_puller(_ge())
            log.info(
                "buyback execution: {f} fetched, {i} new",
                f=result.get("fetched", 0), i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("buyback quarterly failed: {e}", e=str(exc))

    def _semi_book_to_bill_monthly() -> None:
        """CAT-89: SEMI North American equipment book-to-bill ratio."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.semi_book_to_bill import run_semi_book_to_bill_puller
            result = run_semi_book_to_bill_puller(_ge())
            log.info(
                "SEMI book-to-bill: {f} fetched, {i} new (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("SEMI book-to-bill monthly failed: {e}", e=str(exc))

    def _ecb_tltro_weekly() -> None:
        """CAT-12: ECB TLTRO-III outstanding balance + repayment calendar."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.ecb_tltro import run_ecb_tltro_puller
            result = run_ecb_tltro_puller(_ge())
            log.info(
                "ECB TLTRO: outstanding={o} EUR bn, next={n}, {f} fetched, {i} new",
                o=result.get("outstanding_eur_bn"),
                n=result.get("next_repayment"),
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("ECB TLTRO weekly failed: {e}", e=str(exc))

    # Cadence:
    #   Refinery cracks → Thu 16:00 UTC (EIA Wed 10:30 ET release)
    #   Credit card → Fri 18:00 UTC (FRED weekly update)
    #   Buyback execution → 21st 13:00 UTC (Z.1 Flow of Funds is quarterly)
    #   SEMI book-to-bill → 21st 11:00 UTC (monthly ~3-week lag)
    #   ECB TLTRO → Mon 09:00 UTC (ECB publishes weekly balance-sheet updates)
    def _pboc_omo_daily() -> None:
        """CAT-3: PBoC 7-day reverse repo + MLF daily pull."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.pboc_omo import run_pboc_omo_puller
            result = run_pboc_omo_puller(_ge())
            log.info(
                "PBoC OMO: {o} OMO rows, {m} MLF rows, {i} inserted",
                o=result.get("omo_rows", 0),
                m=result.get("mlf_rows", 0),
                i=result.get("inserted", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("PBoC OMO daily failed: {e}", e=str(exc))

    def _taiwan_exports_monthly() -> None:
        """CAT-9: Taiwan export orders + foundry utilization monthly."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.taiwan_exports import run_taiwan_exports_puller
            result = run_taiwan_exports_puller(_ge())
            log.info(
                "Taiwan exports: {f} fetched, {i} new (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Taiwan exports monthly failed: {e}", e=str(exc))

    def _container_freight_weekly() -> None:
        """CAT-82: Drewry WCI + SCFI weekly container freight rates."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.container_freight import run_container_freight_puller
            result = run_container_freight_puller(_ge())
            log.info(
                "Container freight: {f} fetched, {i} new (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Container freight weekly failed: {e}", e=str(exc))

    _sched.every().thursday.at("16:00").do(_refinery_cracks_weekly)
    _sched.every().friday.at("18:00").do(_credit_card_spending_weekly)
    _sched.every().day.at("13:00").do(_buyback_execution_quarterly)
    _sched.every().day.at("11:00").do(_semi_book_to_bill_monthly)
    _sched.every().monday.at("09:00").do(_ecb_tltro_weekly)
    # Cadence:
    #   PBoC OMO → daily 01:30 UTC (PBoC publishes after Beijing market close)
    #   Taiwan exports → daily 10:30 UTC (MOEA monthly ~3-week lag; poll daily
    #     for idempotent dedup)
    #   Container freight → Fridays 17:00 UTC (Drewry Thu release, SCFI Fri
    #     release)
    _sched.every().day.at("01:30").do(_pboc_omo_daily)
    _sched.every().day.at("10:30").do(_taiwan_exports_monthly)
    _sched.every().friday.at("17:00").do(_container_freight_weekly)

    def _lme_warehouse_daily() -> None:
        """CAT-51: LME daily warehouse stocks + cancelled-warrant ratio."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.lme_warehouse import run_lme_warehouse_puller
            result = run_lme_warehouse_puller(_ge())
            log.info(
                "LME warehouse: {f} fetched, {i} new (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("LME warehouse daily failed: {e}", e=str(exc))

    def _iron_ore_ports_daily() -> None:
        """CAT-52: Chinese iron ore port stocks + throughput."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.iron_ore_ports import run_iron_ore_ports_puller
            result = run_iron_ore_ports_puller(_ge())
            log.info(
                "iron ore ports: {f} fetched, {i} new (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("iron ore ports daily failed: {e}", e=str(exc))

    def _taiwan_strait_osint_daily() -> None:
        """CAT-91: Taiwan MND daily ADIZ incursion count + PLA events."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.taiwan_strait_osint import run_taiwan_strait_puller
            result = run_taiwan_strait_puller(_ge())
            log.info(
                "Taiwan Strait: {f} fetched, {i} new, latest_aircraft={a} (source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                a=result.get("latest_aircraft_count"),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Taiwan Strait daily failed: {e}", e=str(exc))

    # Cadence:
    #   LME warehouse → daily 09:00 UTC (LME publishes ~08:00 London)
    #   Iron ore ports → Fri 10:00 UTC (Mysteel weekly Thursday release)
    #   Taiwan Strait → daily 23:30 UTC (MND publishes Taiwan morning)
    _sched.every().day.at("09:00").do(_lme_warehouse_daily)
    _sched.every().friday.at("10:00").do(_iron_ore_ports_daily)
    _sched.every().day.at("23:30").do(_taiwan_strait_osint_daily)

    def _credit_index_proxies_daily() -> None:
        """CAT-7 / CAT-13 / CAT-42: FRED ICE BofA cash-bond OAS proxies
        for paywalled Markit / S&P credit indices."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.credit_index_proxies import run_credit_index_proxies_puller
            result = run_credit_index_proxies_puller(_ge())
            log.info(
                "credit proxies: {f} fetched, {i} new (groups={g})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                g=result.get("groups", {}),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("credit index proxies daily failed: {e}", e=str(exc))

    def _ais_ground_truth_4h() -> None:
        """Novel: AIS ship-at-berth count across 15 global ports.
        Cross-check layer for CAT-51 / CAT-52 / CAT-82 reported stats."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.ais_ground_truth import run_ais_ground_truth_puller
            result = run_ais_ground_truth_puller(_ge())
            log.info(
                "AIS ground truth: {f} fetched, {i} new, scraped={s}, failed={fl}",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=len(result.get("ports_scraped", [])),
                fl=len(result.get("ports_failed", [])),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("AIS ground truth 4h failed: {e}", e=str(exc))

    def _social_port_activity_daily() -> None:
        """Novel: Reddit + YouTube + nitter + Bilibili port activity
        velocity across 15 global ports. Cross-check for CAT-51/52/82."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.social_port_activity import run_social_port_activity_puller
            result = run_social_port_activity_puller(_ge())
            log.info(
                "social port activity: {f} fetched, {i} new, "
                "source_mix={sm}",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                sm=result.get("source_mix", {}),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("social port activity daily failed: {e}", e=str(exc))

    # Cadence:
    #   Credit proxies → daily 14:00 UTC (FRED updates end-of-day NY)
    #   AIS ground truth → every 4 hours (matches VesselFinder cadence)
    #   Social port activity → daily 06:00 UTC (off-peak for all social APIs)
    _sched.every().day.at("14:00").do(_credit_index_proxies_daily)
    _sched.every(4).hours.do(_ais_ground_truth_4h)
    _sched.every().day.at("06:00").do(_social_port_activity_daily)

    def _shipping_fudge_detector_4h() -> None:
        """Capstone: cross-reference reported shipping stats (CAT-51/52/82)
        against AIS + social ground-truth, fire fudge alerts on divergence."""
        try:
            from db import get_engine as _ge
            from intelligence.shipping_fudge_detector import (
                run_shipping_fudge_detector,
            )
            report = run_shipping_fudge_detector(_ge())
            log.info(
                "shipping fudge detector: {n} checks, {r} red flags",
                n=len(report.checks), r=len(report.red_flags),
            )
            for flag in report.red_flags[:5]:
                log.warning("FUDGE [{a}] {i}", a=flag.assessment, i=flag.implication)
        except Exception as exc:  # noqa: BLE001
            log.warning("shipping fudge detector 4h failed: {e}", e=str(exc))

    # Runs on the same 4h cadence as AIS ground truth so the detector
    # always sees the freshest observed deltas.
    _sched.every(4).hours.do(_shipping_fudge_detector_4h)

    def _jodi_oil_monthly() -> None:
        """Novel: JODI global oil inventory monthly puller — covers
        Saudi/UAE/Russia/Iran/etc. producers that EIA/IEA don't detail."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.jodi_oil import run_jodi_oil_puller
            result = run_jodi_oil_puller(_ge())
            log.info(
                "JODI oil: {f} fetched, {i} new (source={s}, countries={c})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                s=result.get("source", "none"),
                c=len(result.get("countries_seen", [])),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("JODI oil monthly failed: {e}", e=str(exc))

    def _sge_premium_daily() -> None:
        """Novel: Shanghai Gold Exchange premium vs London daily —
        cleanest public real-time China physical gold demand signal."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.sge_premium import run_sge_premium_puller
            result = run_sge_premium_puller(_ge())
            log.info(
                "SGE premium: {f} fetched, {i} new, latest=${p:.2f}/oz "
                "({sev}, source={s})",
                f=result.get("fetched", 0),
                i=result.get("inserted", 0),
                p=result.get("latest_premium_usd") or 0.0,
                sev=result.get("latest_severity", "unknown"),
                s=result.get("source", "none"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("SGE premium daily failed: {e}", e=str(exc))

    def _reddit_options_pulse_daily() -> None:
        """Novel: Reddit /r/options daily discussion thread pulse —
        retail positioning leads meme/AI-momentum moves by 1-3 days."""
        try:
            from db import get_engine as _ge
            from ingestion.altdata.reddit_options_pulse import (
                run_reddit_options_pulse_puller,
            )
            result = run_reddit_options_pulse_puller(_ge())
            log.info(
                "Reddit options pulse: thread={t}, comments={c}, "
                "bull_bear={bb:.2f}, 0dte={z}",
                t=result.get("thread_date", "none"),
                c=result.get("comment_count", 0),
                bb=result.get("bull_bear_ratio") or 0.0,
                z=result.get("zero_dte_count", 0),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Reddit options pulse daily failed: {e}", e=str(exc))

    # Cadence:
    #   JODI oil → 25th 12:00 UTC (JODI publishes ~3 weeks after month-end)
    #   SGE premium → daily 08:00 UTC (Shanghai close + London open overlap)
    #   Reddit options pulse → daily 22:00 UTC (after US close, capturing
    #     the full post-close retail narrative)
    _sched.every().day.at("12:00").do(_jodi_oil_monthly)
    _sched.every().day.at("08:00").do(_sge_premium_daily)
    _sched.every().day.at("22:00").do(_reddit_options_pulse_daily)

    # ── SWEEP: unscheduled intelligence modules ────────────────────────

    def _fci_compute_6h() -> None:
        """CAT-124: Financial Conditions Index composite refresh."""
        try:
            from db import get_engine as _ge
            from intelligence.financial_conditions_index import compute_fci
            r = compute_fci(_ge())
            log.info(
                "FCI: score={s:.3f} regime={reg} components={c}",
                s=r.score, reg=r.regime, c=len(r.components or []),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("FCI compute failed: {e}", e=str(exc))

    def _hmm_transition_matrix_daily() -> None:
        """CAT-121: refit the regime transition matrix from the last 2y
        of regime_labels history."""
        try:
            from db import get_engine as _ge
            from intelligence.hmm_regime_transitions import fit_from_db
            matrix = fit_from_db(_ge(), lookback_days=730)
            if matrix is None:
                log.info("HMM transition matrix: insufficient history")
            else:
                log.info(
                    "HMM transition matrix refit: {n} states, {s} samples",
                    n=len(matrix.states), s=matrix.n_samples,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("HMM transition matrix daily failed: {e}", e=str(exc))

    def _thesis_invalidation_hourly() -> None:
        """CAT-190: sweep every active thesis against its invalidation
        conditions. Fires the auto-size-down policy on triggers."""
        try:
            from db import get_engine as _ge
            from intelligence.thesis_invalidation_monitor import run_monitor
            result = run_monitor(_ge())
            log.info(
                "thesis invalidation: {t} theses, {i} invalidated, "
                "{s} size-down",
                t=result.theses_checked,
                i=len(result.invalidations),
                s=sum(1 for e in result.invalidations if e.size_down_applied),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("thesis invalidation hourly failed: {e}", e=str(exc))

    def _credit_novelty_daily() -> None:
        """CAT-152 + CAT-162: per-ticker 10-K risk factor novelty and
        credit event PD for a rotating slice of the top-weighted tickers.

        Keeps each nightly run bounded (<= 30 tickers) so it can't
        starve the intelligence loop.
        """
        try:
            from db import get_engine as _ge
            from sqlalchemy import text as _t
            from intelligence.risk_factor_novelty import compute_novelty
            from intelligence.credit_event_probability import (
                compute_credit_event_probability,
            )

            eng = _ge()
            try:
                with eng.connect() as conn:
                    rows = conn.execute(
                        _t(
                            "SELECT ticker FROM actor_registry "
                            "WHERE ticker IS NOT NULL AND weight > 0 "
                            "ORDER BY weight DESC NULLS LAST LIMIT 30"
                        )
                    ).fetchall()
                tickers = [r[0] for r in rows if r and r[0]]
            except Exception as exc:  # noqa: BLE001
                log.debug("credit_novelty: ticker pull failed: {e}", e=str(exc))
                tickers = []

            novelty_hits = 0
            pd_computed = 0
            for t in tickers:
                try:
                    n = compute_novelty(eng, t)
                    if n is not None and n.is_novel:
                        novelty_hits += 1
                except Exception as exc:  # noqa: BLE001
                    log.debug("novelty {t} skipped: {e}", t=t, e=str(exc))
                try:
                    pd_res = compute_credit_event_probability(eng, t)
                    if pd_res is not None:
                        pd_computed += 1
                except Exception as exc:  # noqa: BLE001
                    log.debug("credit PD {t} skipped: {e}", t=t, e=str(exc))

            log.info(
                "credit/novelty daily: {n} tickers scanned, {h} novel filings, "
                "{p} PDs computed",
                n=len(tickers), h=novelty_hits, p=pd_computed,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("credit/novelty daily failed: {e}", e=str(exc))

    # Cadence:
    #   FCI     → every 6 h (macro conditions don't change faster)
    #   HMM     → daily 04:00 UTC (after overnight regime labels update)
    #   Thesis  → hourly (active risk monitoring)
    #   Credit/novelty → daily 04:30 UTC
    _sched.every(6).hours.do(_fci_compute_6h)
    _sched.every().day.at("04:00").do(_hmm_transition_matrix_daily)
    _sched.every(1).hours.do(_thesis_invalidation_hourly)
    _sched.every().day.at("04:30").do(_credit_novelty_daily)

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
