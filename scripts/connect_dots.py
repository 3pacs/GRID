"""GRID Intelligence — Deep Cross-Reference Engine.

Connects dots across all signal types to find:
1. Insider clusters (coordinated trading)
2. Congress-insider overlaps (information leakage)
3. Whale flow bias (smart money positioning)
4. Unusual options near business events (foreknowledge)
5. Multi-signal convergence (strongest conviction)
6. Hypothesis kill patterns (what fails and why)
7. Lead-lag relationships (predictive edges)
8. Most connected actors (power network)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_engine
from loguru import logger as log
from sqlalchemy import text


def main() -> dict:
    engine = get_engine()
    results = {}

    # 1. Insider CLUSTERS — multiple insiders same ticker same week
    log.info("DOT 1: Insider clusters...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, COUNT(DISTINCT actor) as insiders,
                       array_agg(DISTINCT actor ORDER BY actor) as names,
                       SUM(magnitude) as total_value,
                       MIN(signal_date) as first_date, MAX(signal_date) as last_date,
                       mode() WITHIN GROUP (ORDER BY direction) as dominant_dir
                FROM signal_data
                WHERE signal_type IN ('insider', 'quiverquant:insider')
                AND signal_date >= CURRENT_DATE - 30
                AND ticker IS NOT NULL AND magnitude > 0
                GROUP BY ticker, DATE_TRUNC('week', signal_date)
                HAVING COUNT(DISTINCT actor) >= 2
                ORDER BY total_value DESC
                LIMIT 20
            """)).fetchall()
            clusters = []
            for r in rows:
                c = {
                    "ticker": r[0], "insiders": r[1],
                    "names": list(r[2])[:5] if r[2] else [],
                    "total_value": float(r[3] or 0),
                    "first": str(r[4]), "last": str(r[5]),
                    "direction": r[6],
                }
                clusters.append(c)
                log.info("  CLUSTER: {t} — {n} insiders, ${v:,.0f}, dir={d}",
                         t=c["ticker"], n=c["insiders"], v=c["total_value"], d=c["direction"])
            results["insider_clusters"] = clusters
    except Exception as e:
        log.warning("Insider clusters: {e}", e=str(e))

    # 2. Congress-insider OVERLAP
    log.info("DOT 2: Congress-insider overlaps...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH congress AS (
                    SELECT ticker, actor, direction, signal_date, magnitude
                    FROM signal_data
                    WHERE signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                    AND signal_date >= CURRENT_DATE - 60
                    AND ticker IS NOT NULL
                ),
                insiders AS (
                    SELECT ticker, actor, direction, signal_date, magnitude
                    FROM signal_data
                    WHERE signal_type IN ('insider', 'quiverquant:insider')
                    AND signal_date >= CURRENT_DATE - 60
                    AND ticker IS NOT NULL AND magnitude > 10000
                )
                SELECT c.ticker,
                       c.actor as congress_member, c.direction as cong_dir, c.signal_date as cong_date,
                       i.actor as insider_name, i.direction as ins_dir, i.signal_date as ins_date,
                       i.magnitude as ins_value,
                       ABS(c.signal_date - i.signal_date) as days_apart
                FROM congress c
                JOIN insiders i ON c.ticker = i.ticker
                    AND ABS(c.signal_date - i.signal_date) <= 14
                    AND c.signal_date != i.signal_date
                ORDER BY i.magnitude DESC
                LIMIT 25
            """)).fetchall()
            overlaps = []
            for r in rows:
                o = {
                    "ticker": r[0],
                    "congress": str(r[1])[:30], "cong_dir": r[2], "cong_date": str(r[3]),
                    "insider": str(r[4])[:30], "ins_dir": r[5], "ins_date": str(r[6]),
                    "value": float(r[7] or 0), "days_apart": int(r[8]),
                }
                overlaps.append(o)
                log.info("  OVERLAP: {t} — Congress {cd} {cm} ({cdt}) | Insider {id} {im} ({idt}) ${v:,.0f} [{da}d]",
                         t=o["ticker"], cd=o["cong_dir"], cm=o["congress"],
                         cdt=o["cong_date"], id=o["ins_dir"], im=o["insider"],
                         idt=o["ins_date"], v=o["value"], da=o["days_apart"])
            results["congress_insider_overlaps"] = overlaps
    except Exception as e:
        log.warning("Congress-insider: {e}", e=str(e))

    # 3. Whale flow bias
    log.info("DOT 3: Whale flow bias...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker,
                    SUM(CASE WHEN direction IN ('buy', 'bullish', 'call') THEN magnitude ELSE 0 END) as bull,
                    SUM(CASE WHEN direction IN ('sell', 'bearish', 'put') THEN magnitude ELSE 0 END) as bear,
                    COUNT(*) as flows
                FROM signal_data
                WHERE signal_type IN ('whale_flow', 'unusual_options')
                AND signal_date >= CURRENT_DATE - 7
                AND ticker IS NOT NULL
                GROUP BY ticker
                HAVING COUNT(*) >= 50
                ORDER BY SUM(magnitude) DESC
                LIMIT 20
            """)).fetchall()
            whales = []
            for r in rows:
                bull = float(r[1] or 0)
                bear = float(r[2] or 0)
                ratio = bull / max(bear, 1)
                bias = "WHALE BULLISH" if ratio > 1.5 else "WHALE BEARISH" if ratio < 0.67 else "MIXED"
                w = {"ticker": r[0], "bull": bull, "bear": bear, "ratio": round(ratio, 2),
                     "bias": bias, "flows": r[3]}
                whales.append(w)
                log.info("  WHALE: {t} bull=${b:,.0f}M bear=${br:,.0f}M ratio={r:.1f}:1 [{bi}]",
                         t=w["ticker"], b=bull / 1e6, br=bear / 1e6, r=ratio, bi=bias)
            results["whale_bias"] = whales
    except Exception as e:
        log.warning("Whale bias: {e}", e=str(e))

    # 4. Unusual options near business events
    log.info("DOT 4: Unusual options near events...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT s.ticker, s.direction, COUNT(*) as unusual_count,
                       SUM(s.magnitude) as total_premium,
                       b.category, b.direction as event_dir, b.confidence,
                       LEFT(b.headline, 80) as headline
                FROM signal_data s
                JOIN business_events b ON s.ticker = ANY(b.tickers)
                WHERE s.signal_type = 'unusual_options'
                AND s.signal_date BETWEEN b.published_at::date - 5 AND b.published_at::date + 2
                AND s.magnitude > 100000
                GROUP BY s.ticker, s.direction, b.category, b.direction, b.confidence, b.headline
                HAVING COUNT(*) >= 2
                ORDER BY total_premium DESC
                LIMIT 20
            """)).fetchall()
            opts = []
            for r in rows:
                o = {"ticker": r[0], "opt_dir": r[1], "count": r[2],
                     "premium": float(r[3] or 0), "event": r[4], "event_dir": r[5],
                     "conf": float(r[6] or 0), "headline": r[7]}
                opts.append(o)
                log.info("  OPTIONS: {t} {n}x {d} ${p:,.0f}K -> [{e}] {ed}",
                         t=o["ticker"], n=o["count"], d=o["opt_dir"] or "?",
                         p=o["premium"] / 1000, e=o["event"], ed=o["event_dir"])
            results["unusual_options_near_events"] = opts
    except Exception as e:
        log.warning("Options-events: {e}", e=str(e))

    # 5. Multi-signal convergence
    log.info("DOT 5: Multi-signal convergence...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker,
                       COUNT(DISTINCT signal_type) as types,
                       COUNT(*) as total,
                       array_agg(DISTINCT signal_type ORDER BY signal_type) as type_list,
                       SUM(CASE WHEN direction IN ('buy','bullish','call') THEN 1 ELSE 0 END) as bull,
                       SUM(CASE WHEN direction IN ('sell','bearish','put') THEN 1 ELSE 0 END) as bear
                FROM signal_data
                WHERE signal_date >= CURRENT_DATE - 7
                AND ticker IS NOT NULL AND ticker != ''
                GROUP BY ticker
                HAVING COUNT(DISTINCT signal_type) >= 3
                ORDER BY COUNT(DISTINCT signal_type) DESC, COUNT(*) DESC
                LIMIT 30
            """)).fetchall()
            convergences = []
            for r in rows:
                bull = r[4] or 0
                bear = r[5] or 0
                bias = "BULLISH" if bull > bear * 2 else "BEARISH" if bear > bull * 2 else "MIXED"
                c = {"ticker": r[0], "types": r[1], "total": r[2],
                     "signal_types": list(r[3])[:8] if r[3] else [],
                     "bull": bull, "bear": bear, "bias": bias}
                convergences.append(c)
            results["convergence"] = convergences
            # Show top 10
            for c in convergences[:10]:
                log.info("  CONVERGE: {t} — {n} types, {tot} signals [{b}] types={ty}",
                         t=c["ticker"], n=c["types"], tot=c["total"],
                         b=c["bias"], ty=c["signal_types"][:5])
    except Exception as e:
        log.warning("Convergence: {e}", e=str(e))

    # 6. Hypothesis kill patterns
    log.info("DOT 6: Kill patterns...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT kill_reason, COUNT(*) as kills,
                       ROUND(AVG(confidence)::numeric, 3) as avg_conf,
                       ROUND(AVG(EXTRACT(EPOCH FROM (killed_at - created_at))/86400)::numeric, 1) as avg_life,
                       array_agg(DISTINCT pattern_type) as types
                FROM discovered_hypotheses
                WHERE kill_reason IS NOT NULL
                AND killed_at >= CURRENT_DATE - 30
                GROUP BY kill_reason
                ORDER BY kills DESC
            """)).fetchall()
            kills = []
            for r in rows:
                k = {"reason": r[0], "count": r[1], "avg_conf": float(r[2] or 0),
                     "avg_lifespan_days": float(r[3] or 0), "types": list(r[4]) if r[4] else []}
                kills.append(k)
                log.info("  KILL: {r} — {n} kills, conf={c:.3f}, life={l:.1f}d",
                         r=k["reason"], n=k["count"], c=k["avg_conf"], l=k["avg_lifespan_days"])
            results["kill_patterns"] = kills
    except Exception as e:
        log.warning("Kills: {e}", e=str(e))

    # 7. Lead-lag hypotheses
    log.info("DOT 7: Lead-lag edges...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT thesis, confidence, pattern_type
                FROM discovered_hypotheses
                WHERE pattern_type = 'lead_lag'
                AND status = 'active'
                AND confidence >= 0.55
                ORDER BY confidence DESC
                LIMIT 20
            """)).fetchall()
            leads = []
            for r in rows:
                leads.append({"thesis": r[0], "confidence": float(r[1] or 0)})
                log.info("  LEAD-LAG: [%.2f] %s", float(r[1] or 0), r[0][:100])
            results["lead_lag"] = leads
    except Exception as e:
        log.warning("Lead-lag: {e}", e=str(e))

    # 8. Most connected actors
    log.info("DOT 8: Power actors...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT actor, COUNT(*) as signals,
                       COUNT(DISTINCT ticker) as tickers,
                       COUNT(DISTINCT signal_type) as types,
                       array_agg(DISTINCT ticker ORDER BY ticker) as ticker_list
                FROM signal_data
                WHERE signal_date >= CURRENT_DATE - 30
                AND actor IS NOT NULL AND actor != ''
                AND actor NOT IN ('MACRO', 'unknown', 'qq_house_trading', 'qq_senate_trading')
                GROUP BY actor
                HAVING COUNT(DISTINCT ticker) >= 2
                ORDER BY COUNT(DISTINCT ticker) DESC, COUNT(*) DESC
                LIMIT 25
            """)).fetchall()
            actors = []
            for r in rows:
                a = {"name": r[0], "signals": r[1], "tickers": r[2],
                     "types": r[3], "ticker_list": list(r[4])[:8] if r[4] else []}
                actors.append(a)
                log.info("  ACTOR: {n} — {s} signals across {t} tickers",
                         n=a["name"][:30], s=a["signals"], t=a["tickers"])
            results["power_actors"] = actors
    except Exception as e:
        log.warning("Actors: {e}", e=str(e))

    # Summary
    log.info("=== DOT CONNECTION COMPLETE ===")
    for key, val in results.items():
        log.info("  {k}: {n} items", k=key, n=len(val) if isinstance(val, list) else "?")

    # Write results to JSON for the API
    output_path = Path(__file__).parent.parent / "output" / "dots_connected.json"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("Results written to {p}", p=output_path)

    return results


if __name__ == "__main__":
    main()
