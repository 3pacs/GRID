"""
GRID LLM Task Queue — background task generators and API router.

Contains all background work generators (_gen_*), the expectation result
handler, and the FastAPI router builder.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Forward reference — LLMTaskQueue is imported at runtime to avoid circular
# ---------------------------------------------------------------------------


def _generate_background_tasks(
    engine: Any,
    tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate a batch of background work when the queue runs dry.

    Checks what needs doing and returns a list of
    ``(task_type, prompt, context)`` tuples.
    """
    tasks: list[tuple[str, str, dict]] = []

    # 0. BACKLOG DRAIN FIRST
    try:
        tasks.extend(_gen_from_backlog(engine, tq))
    except Exception as exc:
        log.debug("Backlog drain failed: {e}", e=str(exc))

    # 1. Feature interpretations
    try:
        tasks.extend(_gen_feature_interpretations(engine, tq))
    except Exception as exc:
        log.debug("Feature interpretation gen failed: {e}", e=str(exc))

    # 2. Actor research
    try:
        tasks.extend(_gen_actor_research(engine, tq))
    except Exception as exc:
        log.debug("Actor research gen failed: {e}", e=str(exc))

    # 3. Market briefing
    try:
        tasks.extend(_gen_market_briefing(engine, tq))
    except Exception as exc:
        log.debug("Market briefing gen failed: {e}", e=str(exc))

    # 4. Anomaly detection
    try:
        tasks.extend(_gen_anomaly_detection(engine, tq))
    except Exception as exc:
        log.debug("Anomaly detection gen failed: {e}", e=str(exc))

    # 5. Hypothesis generation
    try:
        tasks.extend(_gen_hypothesis_tasks(engine, tq))
    except Exception as exc:
        log.debug("Hypothesis gen failed: {e}", e=str(exc))

    # 6. Narrative history
    try:
        tasks.extend(_gen_narrative_history(engine, tq))
    except Exception as exc:
        log.debug("Narrative history gen failed: {e}", e=str(exc))

    # 7. Prediction refinement
    try:
        tasks.extend(_gen_prediction_refinement(engine, tq))
    except Exception as exc:
        log.debug("Prediction refinement gen failed: {e}", e=str(exc))

    # 8. Company analysis
    try:
        tasks.extend(_gen_company_analysis(engine, tq))
    except Exception as exc:
        log.debug("Company analysis gen failed: {e}", e=str(exc))

    # 9. Correlation discovery
    try:
        tasks.extend(_gen_correlation_discovery(engine, tq))
    except Exception as exc:
        log.debug("Correlation discovery gen failed: {e}", e=str(exc))

    # 10. Panama Papers / ICIJ offshore leak research
    try:
        tasks.extend(_gen_panama_papers_research(engine, tq))
    except Exception as exc:
        log.debug("Panama Papers research gen failed: {e}", e=str(exc))

    # 11. Expectation tracker
    try:
        tasks.extend(_gen_expectation_tracking(engine, tq))
    except Exception as exc:
        log.debug("Expectation tracking gen failed: {e}", e=str(exc))

    # 12. Deep forensic analysis
    try:
        tasks.extend(_gen_deep_forensics(engine, tq))
    except Exception as exc:
        log.debug("Deep forensics gen failed: {e}", e=str(exc))

    # 13. Offshore network analysis
    try:
        tasks.extend(_gen_offshore_analysis(engine, tq))
    except Exception as exc:
        log.debug("Offshore analysis gen failed: {e}", e=str(exc))

    # 14. Sector rotation analysis
    try:
        tasks.extend(_gen_sector_rotation(engine, tq))
    except Exception as exc:
        log.debug("Sector rotation gen failed: {e}", e=str(exc))

    # 15. Signal cross-validation
    try:
        tasks.extend(_gen_signal_cross_validation(engine, tq))
    except Exception as exc:
        log.debug("Signal cross-validation gen failed: {e}", e=str(exc))

    # 16. Earnings preview
    try:
        tasks.extend(_gen_earnings_preview(engine, tq))
    except Exception as exc:
        log.debug("Earnings preview gen failed: {e}", e=str(exc))

    # 17. Alpha101 computation
    try:
        tasks.extend(_gen_alpha101_compute(engine, tq))
    except Exception as exc:
        log.debug("Alpha101 gen failed: {e}", e=str(exc))

    # 18. Strategy151 signals
    try:
        tasks.extend(_gen_strategy151_signals(engine, tq))
    except Exception as exc:
        log.debug("Strategy151 gen failed: {e}", e=str(exc))

    # 19. S&P 500 insider/board mapping
    try:
        tasks.extend(_gen_sp500_insider_mapping(engine, tq))
    except Exception as exc:
        log.debug("S&P 500 insider mapping gen failed: {e}", e=str(exc))

    return tasks


def _gen_from_backlog(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Pull pending tasks from the llm_task_backlog table."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        import json

        with engine.begin() as conn:
            rows = conn.execute(sa_text(
                "UPDATE llm_task_backlog SET status = 'processing' "
                "WHERE id IN ("
                "  SELECT id FROM llm_task_backlog "
                "  WHERE status = 'pending' "
                "  ORDER BY priority ASC, created_at ASC "
                "  LIMIT 150 "
                "  FOR UPDATE SKIP LOCKED"
                ") RETURNING id, task_type, prompt, context"
            )).fetchall()

            for row in rows:
                ctx = row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {}
                ctx["backlog_id"] = row[0]
                tasks.append((row[1], row[2], ctx))

        if tasks:
            log.info("LLM-TQ backlog: drained {n} tasks", n=len(tasks))
    except Exception as exc:
        log.debug("LLM-TQ: backlog drain query failed: {e}", e=str(exc))
    return tasks


# --- Individual generators ---

def _gen_feature_interpretations(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate interpretation tasks for un-interpreted features."""
    from sqlalchemy import text

    tasks: list[tuple[str, str, dict]] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT name, family, description FROM feature_registry "
                "WHERE model_eligible = TRUE "
                "ORDER BY name LIMIT 100"
            )).fetchall()

        for row in rows:
            name, family, desc = row[0], row[1], row[2] or ""
            if name in tq._interpreted_features:
                continue
            tq._interpreted_features.add(name)

            prompt = (
                f"Feature: {name}\n"
                f"Family: {family}\n"
                f"Description: {desc}\n\n"
                f"Write a one-paragraph explanation of what this feature measures, "
                f"why it matters for market prediction, and what economic mechanism "
                f"connects it to asset prices."
            )
            tasks.append(("feature_interpretation", prompt, {"feature": name, "family": family}))
            if len(tasks) >= 3:
                break
    except Exception as exc:
        log.debug("LLM-TQ: feature interpretation gen failed: {e}", e=str(exc))
    return tasks


def _gen_actor_research(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate research tasks for actors without recent profiles."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from analysis.sector_map import SECTOR_MAP, get_actor_influence

        for sector_name in SECTOR_MAP:
            actors = get_actor_influence(sector_name)
            for actor in actors[:5]:
                key = f"{sector_name}:{actor['name']}"
                if key in tq._researched_actors:
                    continue
                tq._researched_actors.add(key)

                prompt = (
                    f"Actor: {actor['name']} ({actor['type']})\n"
                    f"Sector: {sector_name}\n"
                    f"Influence weight: {actor['influence']:.0%}\n"
                    f"Description: {actor.get('description', 'N/A')}\n\n"
                    f"Research this actor's current market positioning, recent "
                    f"actions (last 30 days), and likely next moves. What is the "
                    f"single most important catalyst to watch? Distinguish confirmed "
                    f"facts from inference."
                )
                tasks.append(("actor_research", prompt, {
                    "actor": actor["name"],
                    "sector": sector_name,
                    "influence": actor["influence"],
                }))
                if len(tasks) >= 2:
                    return tasks
    except Exception as exc:
        log.debug("LLM-TQ: actor research gen failed: {e}", e=str(exc))
    return tasks


def _gen_market_briefing(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate a market briefing if the last one is >2 hours old."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    if tq._last_briefing and (now - tq._last_briefing) < timedelta(hours=2):
        return []

    tq._last_briefing = now

    context_parts: list[str] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT payload FROM analytical_snapshots "
                "WHERE category = 'regime_labels' "
                "ORDER BY created_at DESC LIMIT 1"
            )).fetchone()
            if row:
                context_parts.append(f"Current regime snapshot: {str(row[0])[:500]}")

            rows = conn.execute(text(
                "SELECT fr.name, rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE rs.obs_date >= CURRENT_DATE - 1 "
                "ORDER BY rs.obs_date DESC LIMIT 20"
            )).fetchall()
            if rows:
                lines = [f"  {r[0]}: {r[1]} ({r[2]})" for r in rows]
                context_parts.append("Recent data:\n" + "\n".join(lines))
    except Exception as exc:
        log.debug("LLM-TQ: market briefing context query failed: {e}", e=str(exc))

    context_text = "\n\n".join(context_parts) if context_parts else "No recent data available."

    prompt = (
        f"Current date: {now.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"{context_text}\n\n"
        f"Write a concise, actionable market briefing covering:\n"
        f"1. Current regime state and key drivers\n"
        f"2. Notable moves in the last 24 hours\n"
        f"3. Key convergence signals or divergences\n"
        f"4. Actionable opportunities or risks\n\n"
        f"Be specific. No hedging language. State your confidence level."
    )
    return [("market_briefing", prompt, {"generated_at": now.isoformat()})]


def _gen_anomaly_detection(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Scan resolved_series for >3 sigma moves and explain them."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH stats AS (
                    SELECT feature_id,
                           AVG(value) AS mean_val,
                           STDDEV(value) AS std_val
                    FROM resolved_series
                    WHERE obs_date >= CURRENT_DATE - 252
                    GROUP BY feature_id
                    HAVING STDDEV(value) > 0
                ),
                latest AS (
                    SELECT DISTINCT ON (feature_id)
                           feature_id, value, obs_date
                    FROM resolved_series
                    WHERE obs_date >= CURRENT_DATE - 2
                    ORDER BY feature_id, obs_date DESC
                )
                SELECT fr.name, l.value, s.mean_val, s.std_val, l.obs_date,
                       ABS(l.value - s.mean_val) / s.std_val AS z_score
                FROM latest l
                JOIN stats s ON s.feature_id = l.feature_id
                JOIN feature_registry fr ON fr.id = l.feature_id
                WHERE ABS(l.value - s.mean_val) / s.std_val > 3
                ORDER BY z_score DESC
                LIMIT 5
            """)).fetchall()

            for row in rows:
                name, value, mean, std, obs_date, z = row
                prompt = (
                    f"Anomaly detected in {name}:\n"
                    f"  Current value: {value:.4f}\n"
                    f"  252-day mean: {mean:.4f}\n"
                    f"  252-day std: {std:.4f}\n"
                    f"  Z-score: {z:.2f}\n"
                    f"  Observation date: {obs_date}\n\n"
                    f"Explain this anomaly: what moved, possible causes, and "
                    f"whether this signals a regime change or is transient noise."
                )
                tasks.append(("anomaly_detection", prompt, {
                    "feature": name,
                    "z_score": float(z),
                    "value": float(value),
                    "obs_date": str(obs_date),
                }))
    except Exception as exc:
        log.debug("LLM-TQ: anomaly detection gen failed: {e}", e=str(exc))
    return tasks


def _gen_hypothesis_tasks(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate hypothesis generation tasks from unresearched candidates."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, statement FROM hypothesis_registry "
                "WHERE state = 'CANDIDATE' "
                "ORDER BY id DESC LIMIT 3"
            )).fetchall()

        for row in rows:
            hyp_id, statement = row[0], row[1]
            prompt = (
                f"Hypothesis (ID={hyp_id}):\n{statement}\n\n"
                f"Analyze this hypothesis:\n"
                f"1. Is the economic mechanism plausible?\n"
                f"2. What confounding variables could explain the pattern?\n"
                f"3. What additional data would strengthen or weaken it?\n"
                f"4. Suggest a specific, falsifiable test.\n"
                f"5. Rate confidence: LOW / MEDIUM / HIGH with reasoning."
            )
            tasks.append(("hypothesis_generation", prompt, {"hypothesis_id": hyp_id}))
    except Exception as exc:
        log.debug("LLM-TQ: hypothesis gen failed: {e}", e=str(exc))
    return tasks


def _gen_narrative_history(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate a daily market diary entry."""
    from datetime import date as date_cls

    today = date_cls.today().isoformat()
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            existing = conn.execute(text(
                "SELECT 1 FROM analytical_snapshots "
                "WHERE category = 'llm_task_narrative_history' "
                "AND as_of_date = :d LIMIT 1"
            ), {"d": today}).fetchone()
            if existing:
                return []
    except Exception as exc:
        log.debug("LLM-TQ: narrative history dedup check failed: {e}", e=str(exc))

    context_lines: list[str] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT fr.name, rs.value FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE rs.obs_date = CURRENT_DATE "
                "ORDER BY fr.name LIMIT 30"
            )).fetchall()
            context_lines = [f"  {r[0]}: {r[1]}" for r in rows]
    except Exception as exc:
        log.debug("LLM-TQ: narrative history data query failed: {e}", e=str(exc))

    data_block = "\n".join(context_lines) if context_lines else "No data for today yet."

    prompt = (
        f"Date: {today}\n\n"
        f"Today's data:\n{data_block}\n\n"
        f"Write a concise daily market diary entry (3-5 paragraphs) explaining:\n"
        f"1. What happened in markets today\n"
        f"2. What drove the moves (causes, not just descriptions)\n"
        f"3. How this fits the current macro regime\n"
        f"4. What to watch tomorrow\n\n"
        f"Write as a thoughtful journal entry, not a news report."
    )
    return [("narrative_history", prompt, {"date": today})]


def _gen_prediction_refinement(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Review active oracle predictions and suggest conviction updates."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, prediction_text, predicted_value, confidence, "
                "       target_date, created_at "
                "FROM oracle_predictions "
                "WHERE status = 'ACTIVE' "
                "AND target_date > CURRENT_DATE "
                "ORDER BY target_date ASC LIMIT 3"
            )).fetchall()

        for row in rows:
            pid, pred_text, pred_val, conf, target, created = row
            prompt = (
                f"Active prediction (ID={pid}):\n"
                f"  Text: {pred_text}\n"
                f"  Predicted value: {pred_val}\n"
                f"  Confidence: {conf}\n"
                f"  Target date: {target}\n"
                f"  Created: {created}\n\n"
                f"Given current market conditions, should this prediction's "
                f"conviction be RAISED, LOWERED, or MAINTAINED? What new "
                f"evidence has emerged since it was made? Provide a specific "
                f"updated confidence level (0-1)."
            )
            tasks.append(("prediction_refinement", prompt, {
                "prediction_id": pid,
                "current_confidence": float(conf) if conf else None,
            }))
    except Exception as exc:
        log.debug("LLM-TQ: prediction refinement gen failed: {e}", e=str(exc))
    return tasks


def _gen_correlation_discovery(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Test random feature pairs for non-obvious relationships."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        import random
        from sqlalchemy import text
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT name, family FROM feature_registry "
                "WHERE model_eligible = TRUE "
                "ORDER BY random() LIMIT 20"
            )).fetchall()

        features = [(r[0], r[1]) for r in rows]
        if len(features) < 2:
            return []

        random.shuffle(features)
        for i in range(len(features)):
            for j in range(i + 1, len(features)):
                if features[i][1] != features[j][1]:
                    f1, fam1 = features[i]
                    f2, fam2 = features[j]
                    prompt = (
                        f"Feature A: {f1} (family: {fam1})\n"
                        f"Feature B: {f2} (family: {fam2})\n\n"
                        f"These features are from different families. Could there "
                        f"be a non-obvious economic relationship between them? "
                        f"What mechanism would connect them? Is this worth testing "
                        f"empirically? What lag structure would you expect?"
                    )
                    tasks.append(("correlation_discovery", prompt, {
                        "feature_a": f1,
                        "feature_b": f2,
                    }))
                    return tasks
    except Exception as exc:
        log.debug("LLM-TQ: correlation discovery gen failed: {e}", e=str(exc))
    return tasks


def _gen_company_analysis(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Generate company analysis tasks — work through the NASDAQ 100 queue."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from intelligence.company_analyzer import (
            ANALYSIS_QUEUE,
            run_analysis_queue,
            _TICKER_NAMES,
        )
        from sqlalchemy import text as sa_text
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        with engine.connect() as conn:
            rows = conn.execute(sa_text(
                "SELECT ticker FROM company_profiles "
                "WHERE last_analyzed >= :cutoff"
            ), {"cutoff": cutoff}).fetchall()
        recently_analyzed = {row[0] for row in rows}

        batch_count = 0
        for ticker in ANALYSIS_QUEUE:
            if ticker in recently_analyzed:
                continue
            name = _TICKER_NAMES.get(ticker, ticker)

            prompt = (
                f"GRID Company Analysis Task: {name} ({ticker})\n\n"
                f"Running full influence pipeline — querying government contracts, "
                f"lobbying, congressional holdings, insider activity, export controls, "
                f"and actor network for {ticker}. This task triggers the "
                f"company_analyzer.analyze_company() pipeline and stores results "
                f"in company_profiles.\n\n"
                f"After analysis completes, summarize the key findings for {name}."
            )

            tasks.append(("company_analysis", prompt, {
                "ticker": ticker,
                "company": name,
                "action": "analyze_company",
            }))
            batch_count += 1
            if batch_count >= 5:
                break

        if tasks:
            log.info(
                "LLM-TQ company analysis: {n} tickers queued, {s} skipped (recent)",
                n=len(tasks), s=len(recently_analyzed),
            )
    except Exception as exc:
        log.debug("LLM-TQ: company analysis gen failed: {e}", e=str(exc))
    return tasks


def _gen_panama_papers_research(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Have the LLM research connections between known actors and offshore entities."""
    tasks: list[tuple[str, str, dict]] = []

    try:
        from sqlalchemy import text as sa_text

        with engine.connect() as conn:
            rows = conn.execute(sa_text("""
                SELECT DISTINCT
                    ss.ticker         AS actor_id,
                    ss.metadata->>'actor_name'     AS actor_name,
                    ss.metadata->>'entity_name'    AS entity_name,
                    ss.metadata->>'jurisdiction'   AS jurisdiction,
                    ss.metadata->>'match_type'     AS match_type,
                    ss.metadata->>'entity_status'  AS entity_status,
                    ss.metadata->>'leak_source'    AS leak_source,
                    ss.metadata->>'officer_name'   AS officer_name
                FROM signal_sources ss
                WHERE ss.source_type = 'offshore_leak'
                ORDER BY ss.signal_date DESC
                LIMIT 100
            """)).fetchall()

        for row in rows:
            actor_id = row[0] or ""
            actor_name = row[1] or actor_id
            entity_name = row[2] or "unknown entity"
            jurisdiction = row[3] or "unknown"
            match_type = row[4] or "unknown"
            entity_status = row[5] or ""
            leak_source = row[6] or ""
            officer_name = row[7] or ""

            research_key = f"{actor_id}:{entity_name}"
            if research_key in tq._researched_offshore:
                continue
            tq._researched_offshore.add(research_key)

            leak_db = "ICIJ Offshore Leaks"
            if leak_source:
                source_map = {
                    "Panama Papers": "Panama Papers",
                    "Pandora Papers": "Pandora Papers",
                    "Paradise Papers": "Paradise Papers",
                    "Bahamas Leaks": "Bahamas Leaks",
                    "Offshore Leaks": "Offshore Leaks",
                }
                for key, label in source_map.items():
                    if key.lower() in leak_source.lower():
                        leak_db = label
                        break

            prompt = (
                f"PANAMA PAPERS / ICIJ RESEARCH TASK\n"
                f"{'=' * 50}\n\n"
                f"Actor: {actor_name} (ID: {actor_id})\n"
                f"Match type: {match_type}\n"
                f"Officer name in leak: {officer_name}\n"
                f"Connected offshore entity: {entity_name}\n"
                f"Jurisdiction: {jurisdiction}\n"
                f"Entity status: {entity_status or 'unknown'}\n"
                f"Leak database: {leak_db}\n\n"
                f"RESEARCH QUESTIONS:\n"
                f"1. What was '{entity_name}' (jurisdiction: {jurisdiction}) "
                f"likely used for?\n\n"
                f"2. Is this a standard offshore structure for someone in "
                f"{actor_name}'s position, or does it raise red flags?\n\n"
                f"3. What other actors in GRID's network might be connected?\n\n"
                f"4. Implications for {actor_name}'s trading positions, "
                f"political roles, or fiduciary duties?\n\n"
                f"5. Should any active GRID theses involving {actor_name} be "
                f"re-evaluated?\n\n"
                f"Provide a RISK RATING: LOW / MEDIUM / HIGH / CRITICAL."
            )

            tasks.append(("panama_papers_research", prompt, {
                "actor_id": actor_id,
                "actor_name": actor_name,
                "entity_name": entity_name,
                "jurisdiction": jurisdiction,
                "match_type": match_type,
                "leak_source": leak_db,
                "action": "offshore_research",
            }))

            if len(tasks) >= 3:
                break

        # Phase 2: Cross-reference newly discovered actors
        try:
            with engine.connect() as conn:
                new_actors = conn.execute(sa_text("""
                    SELECT a.id, a.name
                    FROM actors a
                    WHERE a.created_at >= NOW() - INTERVAL '7 days'
                      AND NOT EXISTS (
                          SELECT 1 FROM signal_sources ss
                          WHERE ss.source_type = 'offshore_leak'
                            AND ss.ticker = a.id
                      )
                    ORDER BY a.created_at DESC
                    LIMIT 20
                """)).fetchall()

            for actor_row in new_actors:
                aid = actor_row[0]
                aname = actor_row[1]
                screen_key = f"offshore_screen:{aid}"
                if screen_key in tq._researched_offshore:
                    continue
                tq._researched_offshore.add(screen_key)

                try:
                    from ingestion.altdata.offshore_leaks import (
                        check_actor_in_offshore_leaks,
                        queue_offshore_investigation,
                    )
                    offshore_hits = check_actor_in_offshore_leaks(
                        engine, aname, actor_id=aid,
                    )
                    if offshore_hits:
                        queue_offshore_investigation(
                            engine, aname, aid, offshore_hits,
                        )
                        log.warning(
                            "New actor {name} found in offshore leaks — "
                            "investigation queued",
                            name=aname,
                        )
                except ImportError:
                    pass
                except Exception as exc:
                    log.debug(
                        "Offshore screen for new actor {n} failed: {e}",
                        n=aname, e=str(exc),
                    )

        except Exception as exc:
            log.debug("LLM-TQ: Panama Papers actor query failed: {e}", e=str(exc))

        if tasks:
            log.info(
                "LLM-TQ Panama Papers research: {n} tasks generated",
                n=len(tasks),
            )

    except Exception as exc:
        log.debug("Panama Papers research gen failed: {e}", e=str(exc))

    return tasks


def _gen_deep_forensics(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Deep forensic price move analysis for top tickers."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        tickers = ["AAPL", "NVDA", "MSFT", "TSLA", "GOOGL", "META", "AMZN",
                    "SPY", "QQQ", "BTC-USD", "ETH-USD"]
        for ticker in tickers[:10]:
            with engine.connect() as conn:
                rows = conn.execute(sa_text(
                    "SELECT signal_date, spot_price, put_call_ratio, iv_atm "
                    "FROM options_daily_signals WHERE ticker = :t "
                    "ORDER BY signal_date DESC LIMIT 5"
                ), {"t": ticker}).fetchall()

            if not rows:
                continue

            prices = [f"{r[0]}: ${r[1]:.2f} PCR={r[2]:.2f} IV={r[3]:.3f}" for r in rows if r[1]]
            prompt = (
                f"DEEP FORENSIC ANALYSIS: {ticker}\n\n"
                f"Recent price data:\n" + "\n".join(prices) + "\n\n"
                f"Analyze:\n"
                f"1. What drove each day's move?\n"
                f"2. How much was market-wide vs stock-specific?\n"
                f"3. Current implied expectation from options?\n"
                f"4. Biggest risk NOT priced in?\n"
                f"5. 2-month outlook?\n\n"
                f"Be specific with numbers."
            )
            tasks.append(("deep_forensic", prompt, {"ticker": ticker}))
    except Exception as exc:
        log.debug("LLM-TQ: deep forensics gen failed: {e}", e=str(exc))
    return tasks


def _gen_offshore_analysis(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Deep analysis of offshore networks from ICIJ data."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        import random

        tier = random.choice([1, 1, 2, 2, 3, 3, 4])

        if tier == 1:
            banks = [
                ("UBS", "UBS TRUSTEES (BAHAMAS) LTD.", 9731),
                ("Credit Suisse", "CREDIT SUISSE TRUST LIMITED", 8316),
                ("HSBC", "HSBC PRIVATE BANK (SUISSE) S.A.", 730),
            ]
            for bank_name, exact_name, count in banks[:3]:
                with engine.connect() as conn:
                    entities = conn.execute(sa_text(
                        "SELECT e.name, e.metadata->>'jurisdiction' "
                        "FROM actor_connections ac "
                        "JOIN actors e ON e.id = ac.actor_a AND e.category = 'icij_entity' "
                        "JOIN actors i ON i.id = ac.actor_b AND i.name = :n "
                        "WHERE ac.relationship = 'icij_intermediary_of' "
                        "LIMIT 20"
                    ), {"n": exact_name}).fetchall()

                entity_list = "\n".join(f"  - {r[0]} ({r[1]})" for r in entities)
                prompt = (
                    f"TIER 1 OFFSHORE ANALYSIS: {bank_name}\n\n"
                    f"This bank facilitated {count} offshore entities.\n"
                    f"Sample entities:\n{entity_list}\n\n"
                    f"Analyze structure types, jurisdictions, legitimacy, patterns, "
                    f"and rate suspicion level 1-10."
                )
                tasks.append(("offshore_tier1", prompt, {
                    "bank": bank_name, "entity_count": count, "tier": 1,
                }))

        elif tier == 2:
            with engine.connect() as conn:
                firms = conn.execute(sa_text(
                    "SELECT i.name, COUNT(DISTINCT ac.actor_a) as cnt "
                    "FROM actors i "
                    "JOIN actor_connections ac ON ac.actor_b = i.id "
                    "AND ac.relationship = 'icij_intermediary_of' "
                    "WHERE i.category = 'icij_intermediary' "
                    "GROUP BY i.name "
                    "ORDER BY cnt DESC LIMIT 5"
                )).fetchall()

            for firm_name, count in firms[:5]:
                prompt = (
                    f"TIER 2 OFFSHORE ANALYSIS: {firm_name}\n\n"
                    f"This intermediary created {count} shell entities.\n\n"
                    f"Research this firm: location, principals, reputation, "
                    f"sanctions/investigations, client types, Mossack Fonseca connection.\n"
                    f"Rate: legitimate vs enabler (1-10)."
                )
                tasks.append(("offshore_tier2", prompt, {
                    "firm": firm_name, "entity_count": count, "tier": 2,
                }))

        elif tier == 3:
            with engine.connect() as conn:
                people = conn.execute(sa_text(
                    "SELECT o.name, COUNT(DISTINCT ac.actor_a) as shells "
                    "FROM actor_connections ac "
                    "JOIN actors o ON o.id = ac.actor_b AND o.category = 'icij_officer' "
                    "WHERE ac.relationship = 'icij_officer_of' "
                    "AND o.name !~ '.*(Limited|Ltd|Corp|Bearer|Nominees|Services|Trust|Bank|S\\.A\\.).*' "
                    "AND LENGTH(o.name) > 8 "
                    "GROUP BY o.name "
                    "HAVING COUNT(DISTINCT ac.actor_a) >= 50 "
                    "ORDER BY RANDOM() LIMIT 3"
                )).fetchall()

            for person, shells in people:
                prompt = (
                    f"TIER 3 PERSON ANALYSIS: {person}\n\n"
                    f"Linked to {shells} offshore entities in ICIJ data.\n\n"
                    f"Research: background, reasons for {shells} shells, "
                    f"nominee vs beneficial owner, public records, "
                    f"publicly traded company connections, net worth.\n"
                    f"Confidence: confirmed/derived/estimated/rumored."
                )
                tasks.append(("offshore_tier3", prompt, {
                    "person": person, "shells": shells, "tier": 3,
                }))

        elif tier == 4:
            with engine.connect() as conn:
                tickers = conn.execute(sa_text(
                    "SELECT DISTINCT ticker FROM signal_sources "
                    "WHERE signal_type IN ('CONTRACT_AWARD', 'BUY', 'SELL', 'CLUSTER_BUY') "
                    "ORDER BY RANDOM() LIMIT 3"
                )).fetchall()

            for (ticker,) in tickers:
                prompt = (
                    f"TIER 4 CROSS-REFERENCE: {ticker} offshore exposure\n\n"
                    f"Investigate offshore structures, Panama/Paradise/Pandora Papers "
                    f"connections, effective tax rate, transfer pricing, lobbying on "
                    f"tax policy, board offshore connections.\n"
                    f"Label each finding: confirmed/derived/estimated/rumored/inferred."
                )
                tasks.append(("offshore_tier4", prompt, {
                    "ticker": ticker, "tier": 4,
                }))

    except Exception as exc:
        log.debug("LLM-TQ: offshore analysis gen failed: {e}", e=str(exc))
    return tasks


def _gen_sector_rotation(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Analyze sector rotation patterns from flow data."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        sectors = ["Technology", "Healthcare", "Energy", "Financials",
                   "Industrials", "Consumer Discretionary", "Utilities"]

        with engine.connect() as conn:
            for sector in sectors[:5]:
                rows = conn.execute(sa_text(
                    "SELECT fr.name, rs.obs_date, rs.value "
                    "FROM feature_registry fr "
                    "JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "WHERE fr.name LIKE :pat "
                    "AND rs.obs_date > CURRENT_DATE - 7 "
                    "ORDER BY rs.obs_date DESC LIMIT 10"
                ), {"pat": f"%{sector[:3].lower()}%"}).fetchall()

                data_str = "\n".join(f"  {r[0]}: {r[1]} = {r[2]:.2f}" for r in rows[:5])
                prompt = (
                    f"SECTOR ROTATION: {sector}\n\n"
                    f"Recent data:\n{data_str or '  Limited data available'}\n\n"
                    f"Analyze flow direction, drivers, subsectors, institutional "
                    f"positioning, contrarian signals, and top 3 names."
                )
                tasks.append(("sector_rotation", prompt, {"sector": sector}))
    except Exception as exc:
        log.debug("LLM-TQ: sector rotation gen failed: {e}", e=str(exc))
    return tasks


def _gen_signal_cross_validation(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Cross-validate multiple signal types for the same ticker."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        with engine.connect() as conn:
            tickers = conn.execute(sa_text(
                "SELECT ticker, COUNT(DISTINCT signal_type) as sig_types, "
                "array_agg(DISTINCT signal_type) as types "
                "FROM signal_sources "
                "WHERE signal_date > CURRENT_DATE - 7 "
                "GROUP BY ticker "
                "HAVING COUNT(DISTINCT signal_type) >= 2 "
                "ORDER BY sig_types DESC LIMIT 5"
            )).fetchall()

            for ticker, sig_count, sig_types in tickers[:5]:
                signals = conn.execute(sa_text(
                    "SELECT signal_type, signal_date, "
                    "LEFT(signal_value::text, 100) "
                    "FROM signal_sources "
                    "WHERE ticker = :t AND signal_date > CURRENT_DATE - 7 "
                    "ORDER BY signal_date DESC LIMIT 10"
                ), {"t": ticker}).fetchall()

                sig_str = "\n".join(f"  {r[0]} on {r[1]}: {r[2]}" for r in signals)
                prompt = (
                    f"SIGNAL CROSS-VALIDATION: {ticker}\n\n"
                    f"{sig_count} signal types in last 7 days:\n{sig_str}\n\n"
                    f"Analyze agreement/contradiction, reliability, directional bias, "
                    f"conviction (1-10), confirming signal, and trade recommendation."
                )
                tasks.append(("signal_cross_validation", prompt, {
                    "ticker": ticker, "signal_types": sig_count,
                }))
    except Exception as exc:
        log.debug("LLM-TQ: signal cross-validation gen failed: {e}", e=str(exc))
    return tasks


def _gen_earnings_preview(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Pre-analyze upcoming earnings for major tickers."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
        import random
        random.shuffle(tickers)

        for ticker in tickers[:7]:
            with engine.connect() as conn:
                opts = conn.execute(sa_text(
                    "SELECT put_call_ratio, iv_atm, iv_skew, max_pain, spot_price "
                    "FROM options_daily_signals "
                    "WHERE ticker = :t ORDER BY signal_date DESC LIMIT 1"
                ), {"t": ticker}).fetchone()

            if not opts:
                continue

            prompt = (
                f"EARNINGS PREVIEW: {ticker}\n\n"
                f"Current options positioning:\n"
                f"  Put/Call Ratio: {opts[0]:.2f}\n"
                f"  IV ATM: {opts[1]:.1%}\n"
                f"  IV Skew: {opts[2]:.3f}\n"
                f"  Max Pain: ${opts[3]:.2f}\n"
                f"  Spot: ${opts[4]:.2f}\n\n"
                f"Analyze implied move, skew direction, max pain vs spot, "
                f"historical surprise pattern, key metrics, and pre-earnings "
                f"trade idea with entry/exit criteria."
            )
            tasks.append(("earnings_preview", prompt, {"ticker": ticker}))
    except Exception as exc:
        log.debug("LLM-TQ: earnings preview gen failed: {e}", e=str(exc))
    return tasks


def _gen_alpha101_compute(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Run WorldQuant 101 Formulaic Alphas and have Qwen interpret signals."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from features.alpha101 import Alpha101Engine
        a101 = Alpha101Engine(engine)
        tickers = ["AAPL", "NVDA", "MSFT", "TSLA", "GOOGL", "META", "AMZN"]
        import random
        ticker = random.choice(tickers)

        prompt = (
            f"ALPHA101 QUANT ANALYSIS: {ticker}\n\n"
            f"Run the WorldQuant 101 Formulaic Alphas engine for {ticker}.\n\n"
            f"Analyze strongest factors, short vs medium term agreement, "
            f"cross-sectional rank, VWAP deviation, and combine into "
            f"directional call with confidence 1-10."
        )
        tasks.append(("alpha101_analysis", prompt, {"ticker": ticker}))
    except Exception as exc:
        log.debug("LLM-TQ: alpha101 gen failed: {e}", e=str(exc))
    return tasks


def _gen_strategy151_signals(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Run 151 Trading Strategies and have Qwen synthesize signals."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        import random
        categories = [
            ("momentum", "trend-following signals across timeframes"),
            ("mean_reversion", "overbought/oversold reversion signals"),
            ("volatility", "vol expansion/compression regime signals"),
            ("carry", "yield curve and dividend carry signals"),
            ("value", "relative value and factor tilt signals"),
        ]
        cat, desc = random.choice(categories)

        prompt = (
            f"STRATEGY151 SIGNAL SYNTHESIS: {cat.upper()}\n\n"
            f"Category: {desc}\n\n"
            f"Analyze current market conditions: firing signals, historical "
            f"win rate, optimal holding period, invalidation criteria, "
            f"top 3 tickers with most edge, expected return and max drawdown."
        )
        tasks.append(("strategy151_signal", prompt, {"category": cat}))
    except Exception as exc:
        log.debug("LLM-TQ: strategy151 gen failed: {e}", e=str(exc))
    return tasks


def _gen_sp500_insider_mapping(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Map S&P 500 insiders, board members, and all public information."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from sqlalchemy import text as sa_text
        from analysis.market_universe import SP500_TICKERS

        with engine.connect() as conn:
            recent = conn.execute(sa_text(
                "SELECT ticker FROM company_profiles "
                "WHERE last_analyzed > NOW() - INTERVAL '7 days'"
            )).fetchall()
        recent_tickers = {r[0] for r in recent}

        import random
        candidates = [t for t in SP500_TICKERS if t not in recent_tickers]
        random.shuffle(candidates)

        for ticker in candidates[:15]:
            prompt = (
                f"S&P 500 DEEP PROFILE: {ticker}\n\n"
                f"Build complete intelligence: executives, board interlocks, "
                f"insider trades, PAC donations, lobbying, offshore subsidiaries, "
                f"tax rate, SEC actions, short interest, accounting flags.\n"
                f"Label every finding: confirmed/derived/estimated/rumored/inferred."
            )
            tasks.append(("sp500_deep_profile", prompt, {"ticker": ticker}))

    except ImportError:
        import random
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                    "BRK-B", "JPM", "V", "UNH", "XOM", "JNJ", "PG", "MA",
                    "HD", "AVGO", "MRK", "PEP", "KO", "COST", "ABBV", "WMT",
                    "LLY", "BAC", "CSCO", "TMO", "CRM", "MCD", "ORCL"]
        random.shuffle(tickers)
        for ticker in tickers[:10]:
            prompt = (
                f"S&P 500 DEEP PROFILE: {ticker}\n\n"
                f"Build complete intelligence: executives, board interlocks, "
                f"insider trades, PAC donations, lobbying, offshore subsidiaries, "
                f"tax rate, SEC actions, short interest, accounting flags.\n"
                f"Label every finding: confirmed/derived/estimated/rumored/inferred."
            )
            tasks.append(("sp500_deep_profile", prompt, {"ticker": ticker}))
    except Exception as exc:
        log.debug("LLM-TQ: S&P 500 insider mapping gen failed: {e}", e=str(exc))
    return tasks


def _gen_expectation_tracking(
    engine: Any, tq: Any,
) -> list[tuple[str, str, dict]]:
    """Have Qwen generate market expectations for Mag 7+ tickers."""
    tasks: list[tuple[str, str, dict]] = []
    try:
        from intelligence.news_impact import MAG7_TICKERS, ensure_tables
        from sqlalchemy import text as sa_text

        ensure_tables(engine)

        with engine.connect() as conn:
            recent = conn.execute(sa_text(
                "SELECT DISTINCT ticker FROM news_impact_expectations "
                "WHERE created_at > NOW() - INTERVAL '6 hours'"
            )).fetchall()
        recent_tickers = {r[0] for r in recent}

        for ticker in MAG7_TICKERS:
            if ticker in recent_tickers:
                continue

            news_context = ""
            signal_context = ""
            try:
                with engine.connect() as conn:
                    news = conn.execute(sa_text(
                        "SELECT title, sentiment, confidence FROM news_articles "
                        "WHERE :t = ANY(tickers) AND published_at > NOW() - INTERVAL '7 days' "
                        "ORDER BY published_at DESC LIMIT 5"
                    ), {"t": ticker}).fetchall()
                    news_context = "\n".join(
                        f"  - [{r[1]}] {r[0]} (conf={r[2]:.1f})" for r in news
                    ) if news else "  No recent news"

                    sigs = conn.execute(sa_text(
                        "SELECT signal_type, signal_value, signal_date "
                        "FROM signal_sources WHERE ticker = :t "
                        "AND signal_date > CURRENT_DATE - 7 "
                        "ORDER BY signal_date DESC LIMIT 5"
                    ), {"t": ticker}).fetchall()
                    signal_context = "\n".join(
                        f"  - {r[0]} on {r[2]}: {str(r[1])[:60]}" for r in sigs
                    ) if sigs else "  No recent signals"
            except Exception as exc:
                log.debug("LLM-TQ: expectation tracking signal context failed for {t}: {e}", t=ticker, e=str(exc))

            prompt = f"""Analyze {ticker} and generate 3-5 market expectations.

RECENT NEWS:
{news_context}

RECENT SIGNALS:
{signal_context}

For each expectation, provide in this EXACT format (one per line):
EXPECT|<description>|<type>|<horizon>|<direction>|<magnitude_bps>|<baked_in_pct>|<deadline_YYYY-MM-DD or NONE>

Types: earnings, guidance, product_launch, regulation, macro_data, m_and_a, legal, geopolitical
Horizons: short (< 1 week), medium (1-8 weeks), long (> 8 weeks)
Directions: bullish, bearish
Magnitude: estimated basis points impact (e.g. 200 for a 2% move)
Baked_in: 0-100, how much is already in the price

Be specific and realistic."""

            tasks.append(("expectation_tracking", prompt, {
                "ticker": ticker,
                "action": "generate_expectations",
            }))

        if tasks:
            log.info("LLM-TQ expectation tracking: {n} tickers queued", n=len(tasks))
    except Exception as exc:
        log.debug("LLM-TQ: expectation tracking gen failed: {e}", e=str(exc))
    return tasks


def _handle_expectation_result(engine: Any, task_type: str, result: str, context: dict) -> None:
    """Parse Qwen's expectation output and store in DB."""
    if task_type != "expectation_tracking":
        return

    ticker = context.get("ticker", "")
    if not ticker or not result:
        return

    try:
        from intelligence.news_impact import Expectation, ExpectationTracker, ensure_tables
        import hashlib

        ensure_tables(engine)
        tracker = ExpectationTracker(engine)

        for line in result.split("\n"):
            line = line.strip()
            if not line.startswith("EXPECT|"):
                continue

            parts = line.split("|")
            if len(parts) < 8:
                continue

            _, desc, cat_type, horizon, direction, mag_str, baked_str, deadline_str = parts[:8]

            try:
                magnitude = float(mag_str)
                baked_in = float(baked_str)
            except (ValueError, TypeError):
                continue

            deadline = None
            if deadline_str.strip() != "NONE":
                try:
                    from datetime import date as dt_date
                    deadline = dt_date.fromisoformat(deadline_str.strip())
                except ValueError:
                    pass

            exp_id = hashlib.sha256(
                f"{ticker}:{desc[:50]}:{horizon}".encode()
            ).hexdigest()[:16]

            exp = Expectation(
                id=exp_id,
                ticker=ticker,
                description=desc.strip(),
                catalyst_type=cat_type.strip(),
                horizon=horizon.strip(),
                expected_direction=direction.strip(),
                expected_magnitude_bps=magnitude,
                baked_in_pct=min(100, max(0, baked_in)),
                deadline=deadline,
                status="active",
            )
            tracker.create_expectation(exp)

        log.info("Expectation tracking: parsed results for {t}", t=ticker)
    except Exception as exc:
        log.debug("Expectation result parsing failed: {e}", e=str(exc))


# ---------------------------------------------------------------------------
# FastAPI router
# ---------------------------------------------------------------------------

def build_router():
    """Build a FastAPI APIRouter with LLM task queue endpoints.

    Returns:
        APIRouter: Router with /api/v1/system/llm-status and llm-task routes.
    """
    from fastapi import APIRouter, Depends, HTTPException
    from pydantic import BaseModel, Field

    # Lazy import to avoid circular dependency
    from orchestration.llm_taskqueue import get_task_queue

    from api.auth import require_auth

    router = APIRouter(prefix="/api/v1/system", tags=["system"])

    class EnqueueRequest(BaseModel):
        task_type: str = Field(..., description="Task type (e.g. user_chat, trade_review)")
        prompt: str = Field(..., description="Prompt text for the LLM")
        context: dict = Field(default_factory=dict, description="Arbitrary metadata")
        priority: int = Field(default=3, ge=1, le=3, description="1=realtime, 2=scheduled, 3=background")

    class EnqueueResponse(BaseModel):
        task_id: str
        queue_depth: int
        priority: int

    class TaskResultResponse(BaseModel):
        task_id: str
        task_type: str
        priority: int
        status: str
        result: str | None = None
        error: str | None = None
        created_at: str
        completed_at: str | None = None

    @router.get("/llm-status")
    async def llm_status(_token: str = Depends(require_auth)):
        """Current LLM task queue status."""
        try:
            tq = get_task_queue()
            status = tq.get_status()
            if status["total_completed"] == 0:
                from sqlalchemy import text as sa_text
                engine = tq._engine
                with engine.connect() as conn:
                    row = conn.execute(sa_text(
                        "SELECT COUNT(*) FROM analytical_snapshots "
                        "WHERE category LIKE 'llm_task_%%' "
                        "AND created_at > NOW() - INTERVAL '1 hour'"
                    )).fetchone()
                    completed_1h = row[0] if row else 0

                    recent_rows = conn.execute(sa_text(
                        "SELECT category, created_at FROM analytical_snapshots "
                        "WHERE category LIKE 'llm_task_%%' "
                        "ORDER BY created_at DESC LIMIT 20"
                    )).fetchall()

                    recent_tasks = [
                        {
                            "type": r[0].replace("llm_task_", ""),
                            "completed_at": r[1].isoformat() if r[1] else None,
                            "has_result": True,
                        }
                        for r in recent_rows
                    ]

                    is_active = completed_1h > 0

                    status.update({
                        "total_completed": completed_1h,
                        "throughput_per_hour": completed_1h,
                        "recent_tasks": recent_tasks,
                        "running_task": {"type": "background", "note": "running in Hermes process"} if is_active else None,
                        "source": "db_snapshots",
                    })
            return status
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @router.post("/llm-task", response_model=EnqueueResponse)
    async def enqueue_task(
        req: EnqueueRequest,
        _token: str = Depends(require_auth),
    ):
        """Enqueue a custom LLM task."""
        try:
            tq = get_task_queue()
            task_id = tq.enqueue(
                task_type=req.task_type,
                prompt=req.prompt,
                context=req.context,
                priority=req.priority,
            )
            status = tq.get_status()
            return EnqueueResponse(
                task_id=task_id,
                queue_depth=status["queue_depth"],
                priority=req.priority,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @router.get("/llm-task/{task_id}", response_model=TaskResultResponse)
    async def get_task(task_id: str, _token: str = Depends(require_auth)):
        """Get the result of a completed LLM task."""
        tq = get_task_queue()
        task = tq.get_task_result(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found in history")
        return TaskResultResponse(
            task_id=task.id,
            task_type=task.task_type,
            priority=task.priority,
            status="completed" if task.result else ("error" if task.error else "pending"),
            result=task.result,
            error=task.error,
            created_at=task.created_at,
            completed_at=task.completed_at,
        )

    return router
