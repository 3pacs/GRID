"""GRID Intelligence — Deep Cross-Reference Engine v2.

Goes deeper than v1: backtests signals, finds hidden chains,
tracks money flows through actor networks, identifies
asymmetric information edges.
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

    # 1. INSIDER CLUSTER -> PRICE MOVE backtesting
    # Did coordinated insider selling actually predict drops?
    log.info("DOT 1: Insider cluster -> price move backtest...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH clusters AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date as week,
                           COUNT(DISTINCT actor) as insiders,
                           mode() WITHIN GROUP (ORDER BY direction) as dominant_dir,
                           SUM(magnitude) as total_value
                    FROM signal_data
                    WHERE signal_type IN ('insider', 'quiverquant:insider')
                    AND signal_date >= CURRENT_DATE - 60
                    AND ticker IS NOT NULL AND magnitude > 10000
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                    HAVING COUNT(DISTINCT actor) >= 2
                ),
                price_after AS (
                    SELECT SPLIT_PART(series_id, ':', 2) as ticker,
                           obs_date,
                           value as price
                    FROM raw_series
                    WHERE series_id LIKE 'YF:%:close'
                    AND obs_date >= CURRENT_DATE - 60
                    AND value > 0
                )
                SELECT c.ticker, c.week, c.insiders, c.dominant_dir, c.total_value,
                       p1.price as price_at_cluster,
                       p2.price as price_2w_later,
                       CASE WHEN p1.price > 0 THEN
                           ROUND(((p2.price - p1.price) / p1.price * 100)::numeric, 2)
                       END as pct_move_2w
                FROM clusters c
                LEFT JOIN LATERAL (
                    SELECT price FROM price_after
                    WHERE ticker = c.ticker AND obs_date >= c.week
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                LEFT JOIN LATERAL (
                    SELECT price FROM price_after
                    WHERE ticker = c.ticker AND obs_date >= c.week + 14
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
                WHERE p1.price IS NOT NULL AND p2.price IS NOT NULL
                ORDER BY c.total_value DESC
                LIMIT 20
            """)).fetchall()
            backtests = []
            for r in rows:
                b = {
                    "ticker": r[0], "week": str(r[1]), "insiders": r[2],
                    "direction": r[3], "value": float(r[4] or 0),
                    "price_at": float(r[5] or 0), "price_2w": float(r[6] or 0),
                    "pct_move": float(r[7]) if r[7] is not None else None,
                }
                backtests.append(b)
                arrow = "v" if (b["pct_move"] or 0) < 0 else "^"
                log.info("  {t}: {n} insiders {d} ${v:,.0f} -> {pct}% {a} (${p1:.0f} -> ${p2:.0f})",
                         t=b["ticker"], n=b["insiders"], d=b["direction"],
                         v=b["value"], pct=b["pct_move"] or 0, a=arrow,
                         p1=b["price_at"], p2=b["price_2w"])
            results["insider_backtest"] = backtests
    except Exception as e:
        log.warning("Insider backtest: {e}", e=str(e))

    # 2. CONGRESS MEMBER TRACK RECORDS — who has the best returns?
    log.info("DOT 2: Congress member track records...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH congress_trades AS (
                    SELECT actor, ticker, direction, signal_date, magnitude
                    FROM signal_data
                    WHERE signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                    AND signal_date >= CURRENT_DATE - 90
                    AND ticker IS NOT NULL
                    AND actor NOT IN ('qq_house_trading', 'qq_senate_trading')
                    AND actor IS NOT NULL AND actor != ''
                ),
                with_prices AS (
                    SELECT ct.*,
                           p1.price as entry_price,
                           p2.price as price_30d
                    FROM congress_trades ct
                    LEFT JOIN LATERAL (
                        SELECT value as price
                        FROM raw_series
                        WHERE series_id = 'YF:' || ct.ticker || ':close'
                        AND obs_date >= ct.signal_date
                        AND value > 0
                        ORDER BY obs_date ASC LIMIT 1
                    ) p1 ON true
                    LEFT JOIN LATERAL (
                        SELECT value as price
                        FROM raw_series
                        WHERE series_id = 'YF:' || ct.ticker || ':close'
                        AND obs_date >= ct.signal_date + 30
                        AND value > 0
                        ORDER BY obs_date ASC LIMIT 1
                    ) p2 ON true
                    WHERE p1.price IS NOT NULL AND p2.price IS NOT NULL
                )
                SELECT actor,
                       COUNT(*) as trades,
                       COUNT(DISTINCT ticker) as tickers,
                       ROUND(AVG(CASE WHEN direction IN ('buy', 'purchase')
                           THEN (price_30d - entry_price) / NULLIF(entry_price, 0) * 100
                           ELSE (entry_price - price_30d) / NULLIF(entry_price, 0) * 100
                       END)::numeric, 2) as avg_return_30d,
                       SUM(CASE WHEN
                           (direction IN ('buy', 'purchase') AND price_30d > entry_price) OR
                           (direction IN ('sell', 'sale') AND price_30d < entry_price)
                       THEN 1 ELSE 0 END) as winners,
                       array_agg(DISTINCT ticker ORDER BY ticker) as traded_tickers
                FROM with_prices
                GROUP BY actor
                HAVING COUNT(*) >= 3
                ORDER BY AVG(CASE WHEN direction IN ('buy', 'purchase')
                    THEN (price_30d - entry_price) / NULLIF(entry_price, 0) * 100
                    ELSE (entry_price - price_30d) / NULLIF(entry_price, 0) * 100
                END) DESC NULLS LAST
                LIMIT 15
            """)).fetchall()
            congress = []
            for r in rows:
                win_rate = (r[4] / r[1] * 100) if r[1] > 0 else 0
                c = {
                    "member": r[0], "trades": r[1], "tickers": r[2],
                    "avg_return_30d": float(r[3]) if r[3] is not None else None,
                    "winners": r[4], "win_rate": round(win_rate, 1),
                    "traded": list(r[5])[:8] if r[5] else [],
                }
                congress.append(c)
                log.info("  {m}: {t} trades, {wr:.0f}% win rate, avg return {r}%, tickers={tk}",
                         m=c["member"][:25], t=c["trades"], wr=win_rate,
                         r=c["avg_return_30d"] or 0, tk=c["traded"][:5])
            results["congress_track_records"] = congress
    except Exception as e:
        log.warning("Congress records: {e}", e=str(e))

    # 3. WHALE FLOW REVERSALS — when did smart money flip?
    log.info("DOT 3: Whale flow reversals...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH weekly_flows AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date as week,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN magnitude ELSE 0 END) as bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN magnitude ELSE 0 END) as bear
                    FROM signal_data
                    WHERE signal_type IN ('whale_flow', 'unusual_options')
                    AND signal_date >= CURRENT_DATE - 30
                    AND ticker IS NOT NULL
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                ),
                with_bias AS (
                    SELECT *,
                           CASE WHEN bull > bear * 2 THEN 'BULL'
                                WHEN bear > bull * 2 THEN 'BEAR'
                                ELSE 'NEUTRAL' END as bias,
                           LAG(CASE WHEN bull > bear * 2 THEN 'BULL'
                                    WHEN bear > bull * 2 THEN 'BEAR'
                                    ELSE 'NEUTRAL' END)
                               OVER (PARTITION BY ticker ORDER BY week) as prev_bias
                    FROM weekly_flows
                    WHERE bull + bear > 1000000
                )
                SELECT ticker, week, prev_bias, bias, bull, bear
                FROM with_bias
                WHERE bias != prev_bias
                AND prev_bias IS NOT NULL
                AND bias != 'NEUTRAL' AND prev_bias != 'NEUTRAL'
                ORDER BY (bull + bear) DESC
                LIMIT 15
            """)).fetchall()
            reversals = []
            for r in rows:
                rv = {
                    "ticker": r[0], "week": str(r[1]),
                    "from": r[2], "to": r[3],
                    "bull": float(r[4] or 0), "bear": float(r[5] or 0),
                }
                reversals.append(rv)
                log.info("  FLIP: {t} {f} -> {to} week={w} bull=${b:,.0f}M bear=${br:,.0f}M",
                         t=rv["ticker"], f=rv["from"], to=rv["to"],
                         w=rv["week"], b=rv["bull"]/1e6, br=rv["bear"]/1e6)
            results["whale_reversals"] = reversals
    except Exception as e:
        log.warning("Whale reversals: {e}", e=str(e))

    # 4. SUPPLY CHAIN CONTAGION — which companies share suppliers/signals
    log.info("DOT 4: Supply chain signal contagion...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT s1.ticker as ticker_a, s2.ticker as ticker_b,
                       COUNT(*) as shared_days,
                       CORR(s1.magnitude, s2.magnitude) as signal_correlation,
                       array_agg(DISTINCT s1.signal_type) as shared_types
                FROM signal_data s1
                JOIN signal_data s2 ON s1.signal_date = s2.signal_date
                    AND s1.signal_type = s2.signal_type
                    AND s1.ticker < s2.ticker
                WHERE s1.signal_date >= CURRENT_DATE - 30
                AND s1.ticker IS NOT NULL AND s2.ticker IS NOT NULL
                AND s1.signal_type IN ('whale_flow', 'unusual_options', 'insider')
                AND s1.magnitude > 0 AND s2.magnitude > 0
                GROUP BY s1.ticker, s2.ticker
                HAVING COUNT(*) >= 10
                AND CORR(s1.magnitude, s2.magnitude) > 0.3
                ORDER BY CORR(s1.magnitude, s2.magnitude) DESC NULLS LAST
                LIMIT 20
            """)).fetchall()
            contagion = []
            for r in rows:
                c = {
                    "ticker_a": r[0], "ticker_b": r[1],
                    "shared_days": r[2],
                    "correlation": round(float(r[3]), 3) if r[3] else None,
                    "types": list(r[4])[:5] if r[4] else [],
                }
                contagion.append(c)
                log.info("  PAIR: {a} <-> {b} corr={c} shared={d}d types={t}",
                         a=c["ticker_a"], b=c["ticker_b"],
                         c=c["correlation"], d=c["shared_days"], t=c["types"])
            results["signal_contagion"] = contagion
    except Exception as e:
        log.warning("Contagion: {e}", e=str(e))

    # 5. MONEY TRAIL — wealth flows between connected actors
    log.info("DOT 5: Money trails through actor network...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT wf.from_actor, wf.to_entity, wf.implication,
                       SUM(wf.amount_estimate) as total_flow,
                       COUNT(*) as flow_count,
                       a1.category as from_category,
                       a2.category as to_category
                FROM wealth_flows wf
                LEFT JOIN actors a1 ON wf.from_actor = a1.id
                LEFT JOIN actors a2 ON wf.to_entity = a2.id
                WHERE wf.amount_estimate > 100000
                AND wf.flow_date >= CURRENT_DATE - 90
                GROUP BY wf.from_actor, wf.to_entity, wf.implication,
                         a1.category, a2.category
                HAVING SUM(wf.amount_estimate) > 1000000
                ORDER BY SUM(wf.amount_estimate) DESC
                LIMIT 20
            """)).fetchall()
            trails = []
            for r in rows:
                t = {
                    "from": r[0][:30] if r[0] else "?",
                    "to": r[1][:30] if r[1] else "?",
                    "implication": r[2],
                    "total": float(r[3] or 0),
                    "count": r[4],
                    "from_cat": r[5], "to_cat": r[6],
                }
                trails.append(t)
                log.info("  FLOW: {f} [{fc}] --${t:,.0f}M--> {to} [{tc}] ({imp})",
                         f=t["from"], fc=t["from_cat"] or "?",
                         t=t["total"]/1e6, to=t["to"],
                         tc=t["to_cat"] or "?", imp=t["implication"] or "?")
            results["money_trails"] = trails
    except Exception as e:
        log.warning("Money trails: {e}", e=str(e))

    # 6. DIVERGENCE SCANNER — insider vs whale vs congress direction mismatches
    log.info("DOT 6: Smart money divergences...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH actor_positions AS (
                    SELECT ticker,
                        -- Insider net direction
                        SUM(CASE WHEN signal_type IN ('insider', 'quiverquant:insider')
                            AND direction IN ('buy', 'purchase') THEN magnitude
                            WHEN signal_type IN ('insider', 'quiverquant:insider')
                            AND direction IN ('sell', 'sale') THEN -magnitude
                            ELSE 0 END) as insider_net,
                        -- Congress net direction
                        SUM(CASE WHEN signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                            AND direction IN ('buy', 'purchase') THEN 1
                            WHEN signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                            AND direction IN ('sell', 'sale') THEN -1
                            ELSE 0 END) as congress_net,
                        -- Whale net direction
                        SUM(CASE WHEN signal_type IN ('whale_flow', 'unusual_options')
                            AND direction IN ('buy', 'bullish', 'call') THEN magnitude
                            WHEN signal_type IN ('whale_flow', 'unusual_options')
                            AND direction IN ('sell', 'bearish', 'put') THEN -magnitude
                            ELSE 0 END) as whale_net,
                        COUNT(*) as total_signals
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - 14
                    AND ticker IS NOT NULL
                    GROUP BY ticker
                    HAVING COUNT(*) >= 10
                )
                SELECT ticker, insider_net, congress_net, whale_net, total_signals,
                    CASE
                        WHEN insider_net < 0 AND whale_net > 0 THEN 'INSIDER SELL / WHALE BUY'
                        WHEN insider_net > 0 AND whale_net < 0 THEN 'INSIDER BUY / WHALE SELL'
                        WHEN congress_net < 0 AND whale_net > 0 THEN 'CONGRESS SELL / WHALE BUY'
                        WHEN congress_net > 0 AND insider_net < 0 THEN 'CONGRESS BUY / INSIDER SELL'
                        ELSE NULL
                    END as divergence_type
                FROM actor_positions
                WHERE (insider_net < 0 AND whale_net > 0)
                   OR (insider_net > 0 AND whale_net < 0)
                   OR (congress_net != 0 AND SIGN(congress_net) != SIGN(insider_net) AND insider_net != 0)
                ORDER BY ABS(whale_net) DESC
                LIMIT 20
            """)).fetchall()
            divs = []
            for r in rows:
                d = {
                    "ticker": r[0],
                    "insider_net": float(r[1] or 0),
                    "congress_net": int(r[2] or 0),
                    "whale_net": float(r[3] or 0),
                    "signals": r[4],
                    "divergence": r[5],
                }
                divs.append(d)
                log.info("  DIVERGE: {t} [{div}] insider=${i:,.0f} congress={c} whale=${w:,.0f}M",
                         t=d["ticker"], div=d["divergence"] or "?",
                         i=d["insider_net"], c=d["congress_net"],
                         w=d["whale_net"]/1e6)
            results["smart_money_divergences"] = divs
    except Exception as e:
        log.warning("Divergences: {e}", e=str(e))

    # 7. EVENT CHAINS — A causes B causes C
    log.info("DOT 7: Event chains (A -> B -> C)...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT b1.category as event_a, b1.tickers[1] as ticker_a,
                       b2.category as event_b, b2.tickers[1] as ticker_b,
                       b1.published_at::date as date_a, b2.published_at::date as date_b,
                       (b2.published_at::date - b1.published_at::date) as days_between,
                       LEFT(b1.headline, 50) as headline_a,
                       LEFT(b2.headline, 50) as headline_b
                FROM business_events b1
                JOIN business_events b2
                    ON b1.tickers && b2.tickers
                    AND b2.published_at > b1.published_at
                    AND b2.published_at <= b1.published_at + INTERVAL '14 days'
                    AND b1.event_id != b2.event_id
                WHERE array_length(b1.tickers, 1) > 0
                AND array_length(b2.tickers, 1) > 0
                ORDER BY b1.published_at DESC
                LIMIT 20
            """)).fetchall()
            chains = []
            for r in rows:
                c = {
                    "event_a": r[0], "ticker_a": r[1],
                    "event_b": r[2], "ticker_b": r[3],
                    "date_a": str(r[4]), "date_b": str(r[5]),
                    "days": r[6],
                    "headline_a": r[7], "headline_b": r[8],
                }
                chains.append(c)
                log.info("  CHAIN: [{ea}] {ta} ({da}) -> [{eb}] {tb} ({db}) [{d}d]",
                         ea=c["event_a"], ta=c["ticker_a"] or "?", da=c["date_a"],
                         eb=c["event_b"], tb=c["ticker_b"] or "?", db=c["date_b"],
                         d=c["days"])
                log.info("    A: {a}", a=c["headline_a"])
                log.info("    B: {b}", b=c["headline_b"])
            results["event_chains"] = chains
    except Exception as e:
        log.warning("Event chains: {e}", e=str(e))

    # 8. HIDDEN GEMS — small caps with unusual signal density
    log.info("DOT 8: Hidden gem scanner...")
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH signal_density AS (
                    SELECT ticker,
                           COUNT(*) as signals,
                           COUNT(DISTINCT signal_type) as types,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN 1 ELSE 0 END) as bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN 1 ELSE 0 END) as bear,
                           MAX(magnitude) as max_mag,
                           MAX(confidence) as max_conf
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - 14
                    AND ticker IS NOT NULL AND ticker != ''
                    GROUP BY ticker
                ),
                with_mcap AS (
                    SELECT sd.*,
                           cp.sector,
                           cp.name as company_name
                    FROM signal_density sd
                    LEFT JOIN company_profiles cp ON cp.ticker = sd.ticker
                )
                SELECT ticker, company_name, sector, signals, types,
                       bull, bear, max_mag, max_conf,
                       CASE WHEN bull > bear * 2 THEN 'BULLISH'
                            WHEN bear > bull * 2 THEN 'BEARISH'
                            ELSE 'MIXED' END as bias
                FROM with_mcap
                WHERE signals BETWEEN 3 AND 500
                AND types >= 2
                AND (bull > bear * 2 OR bear > bull * 2)
                ORDER BY types DESC, signals DESC
                LIMIT 25
            """)).fetchall()
            gems = []
            for r in rows:
                g = {
                    "ticker": r[0], "name": r[1], "sector": r[2],
                    "signals": r[3], "types": r[4],
                    "bull": r[5], "bear": r[6],
                    "max_mag": float(r[7] or 0), "max_conf": float(r[8] or 0),
                    "bias": r[9],
                }
                gems.append(g)
                log.info("  GEM: {t} ({n}) [{s}] {sig} signals, {ty} types, {b} [{bi}]",
                         t=g["ticker"], n=(g["name"] or "?")[:25],
                         s=(g["sector"] or "?")[:15],
                         sig=g["signals"], ty=g["types"],
                         b="%d bull / %d bear" % (g["bull"], g["bear"]),
                         bi=g["bias"])
            results["hidden_gems"] = gems
    except Exception as e:
        log.warning("Hidden gems: {e}", e=str(e))

    # Summary
    log.info("=== V2 DOT CONNECTION COMPLETE ===")
    for key, val in results.items():
        log.info("  {k}: {n} items", k=key, n=len(val) if isinstance(val, list) else "?")

    output_path = Path(__file__).parent.parent / "output" / "dots_v2.json"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("Results written to {p}", p=output_path)

    return results


if __name__ == "__main__":
    main()
