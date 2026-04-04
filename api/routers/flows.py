"""Sector flow analysis API — serves the sector map with live data."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/flows", tags=["flows"])


@router.get("/sectors")
async def get_sectors(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Return the full sector map with live z-scores for each actor's features."""
    from analysis.sector_map import SECTOR_MAP, get_actor_influence

    engine = get_db_engine()

    # Get all z-scores from the signal snapshot
    z_map: dict[str, float] = {}
    val_map: dict[str, float] = {}
    try:
        from inference.live import LiveInference
        from api.dependencies import get_pit_store

        pit = get_pit_store()
        li = LiveInference(engine, pit)
        df = li.get_feature_snapshot()
        if not df.empty:
            # Compute z-scores inline (same as signals/snapshot endpoint)
            from datetime import date, timedelta

            with engine.connect() as conn:
                feat_rows = conn.execute(text(
                    "SELECT id, name FROM feature_registry WHERE model_eligible = TRUE"
                )).fetchall()
            name_to_id = {r[1]: r[0] for r in feat_rows}
            records = df.to_dict("records")
            feature_ids = [name_to_id[r["name"]] for r in records if r["name"] in name_to_id]

            if feature_ids:
                today = date.today()
                hist = pit.get_feature_matrix(
                    feature_ids=feature_ids,
                    start_date=today - timedelta(days=504),
                    end_date=today, as_of_date=today,
                    vintage_policy="LATEST_AS_OF",
                )
                if hist is not None and len(hist) > 20:
                    means = hist.mean()
                    stds = hist.std().replace(0, 1)
                    last = hist.ffill().iloc[-1]
                    id_to_name = {r[0]: r[1] for r in feat_rows}
                    for col in hist.columns:
                        name = id_to_name.get(col)
                        if name:
                            z = (last[col] - means[col]) / stds[col]
                            if z == z:  # not NaN
                                z_map[name] = round(float(z), 3)

            for r in records:
                val_map[r["name"]] = r.get("value")
    except Exception as exc:
        log.warning("Flow z-score computation failed: {e}", e=str(exc))

    # Get options data
    opts_map: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            opts = conn.execute(text(
                "SELECT ticker, put_call_ratio, iv_atm, max_pain, spot_price, total_oi "
                "FROM options_daily_signals "
                "WHERE signal_date = (SELECT MAX(signal_date) FROM options_daily_signals)"
            )).fetchall()
            for o in opts:
                opts_map[o[0]] = {
                    "pcr": o[1], "iv": o[2], "max_pain": o[3],
                    "spot": o[4], "oi": o[5],
                }
    except Exception as exc:
        log.debug("Flows: options data query failed: {e}", e=str(exc))

    # Build response with live data attached
    sectors = {}
    for sector_name, sector in SECTOR_MAP.items():
        actors = get_actor_influence(sector_name)

        # Attach live data to each actor
        for actor in actors:
            actor_z = []
            for feat in actor["features"]:
                z = z_map.get(feat)
                v = val_map.get(feat)
                if z is not None:
                    actor_z.append({"feature": feat, "z": z, "value": v})
            actor["live"] = actor_z
            actor["avg_z"] = round(sum(d["z"] for d in actor_z) / len(actor_z), 3) if actor_z else None

            # Attach options if ticker matches
            if actor.get("ticker") and actor["ticker"] in opts_map:
                actor["options"] = opts_map[actor["ticker"]]

        # Compute sector-level stress
        weighted_z = []
        for a in actors:
            if a["avg_z"] is not None:
                weighted_z.append(a["avg_z"] * a["influence"])

        sectors[sector_name] = {
            "etf": sector.get("etf"),
            "etf_z": z_map.get(sector.get("etf", "").lower()),
            "etf_options": opts_map.get(sector.get("etf", "")),
            "actors": actors,
            "sector_stress": round(sum(weighted_z) / sum(a["influence"] for a in actors if a["avg_z"] is not None), 3) if weighted_z else None,
            "subsectors": list(sector.get("subsectors", {}).keys()),
        }

    return {"sectors": sectors}


@router.get("/sectors/{sector_name}/detail")
async def get_sector_detail(
    sector_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return detailed subsector breakdown with per-actor price data and options."""
    from datetime import date, timedelta

    from analysis.sector_map import SECTOR_MAP

    sector = SECTOR_MAP.get(sector_name)
    if not sector:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Sector '{sector_name}' not found")

    engine = get_db_engine()
    today = date.today()
    lookback_30 = today - timedelta(days=30)
    lookback_504 = today - timedelta(days=504)

    # ── Gather feature z-scores and values ──────────────────────
    z_map: dict[str, float] = {}
    val_map: dict[str, float] = {}
    try:
        from inference.live import LiveInference
        from api.dependencies import get_pit_store

        pit = get_pit_store()
        li = LiveInference(engine, pit)
        df = li.get_feature_snapshot()
        if not df.empty:
            with engine.connect() as conn:
                feat_rows = conn.execute(text(
                    "SELECT id, name FROM feature_registry WHERE model_eligible = TRUE"
                )).fetchall()
            name_to_id = {r[1]: r[0] for r in feat_rows}
            records = df.to_dict("records")
            feature_ids = [name_to_id[r["name"]] for r in records if r["name"] in name_to_id]
            if feature_ids:
                hist = pit.get_feature_matrix(
                    feature_ids=feature_ids,
                    start_date=lookback_504,
                    end_date=today, as_of_date=today,
                    vintage_policy="LATEST_AS_OF",
                )
                if hist is not None and len(hist) > 20:
                    means = hist.mean()
                    stds = hist.std().replace(0, 1)
                    last = hist.ffill().iloc[-1]
                    id_to_name = {r[0]: r[1] for r in feat_rows}
                    for col in hist.columns:
                        name = id_to_name.get(col)
                        if name:
                            z = (last[col] - means[col]) / stds[col]
                            if z == z:  # not NaN
                                z_map[name] = round(float(z), 3)
            for r in records:
                val_map[r["name"]] = r.get("value")
    except Exception as exc:
        log.warning("Sector detail z-score computation failed: {e}", e=str(exc))

    # ── Options data ────────────────────────────────────────────
    opts_map: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            opts = conn.execute(text(
                "SELECT ticker, put_call_ratio, iv_atm, max_pain, spot_price, total_oi "
                "FROM options_daily_signals "
                "WHERE signal_date = (SELECT MAX(signal_date) FROM options_daily_signals)"
            )).fetchall()
            for o in opts:
                opts_map[o[0]] = {
                    "pcr": o[1], "iv": o[2], "max_pain": o[3],
                    "spot": o[4], "oi": o[5],
                }
    except Exception as exc:
        log.debug("Flows: sector detail options query failed: {e}", e=str(exc))

    # ── 30-day price changes for relative performance ───────────
    price_changes: dict[str, float] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT fr.name,
                    (SELECT rs2.value FROM resolved_series rs2
                     WHERE rs2.feature_id = fr.id
                     ORDER BY rs2.obs_date DESC LIMIT 1) as latest,
                    (SELECT rs3.value FROM resolved_series rs3
                     WHERE rs3.feature_id = fr.id
                       AND rs3.obs_date <= :d30
                     ORDER BY rs3.obs_date DESC LIMIT 1) as prev
                FROM feature_registry fr
                WHERE fr.name LIKE '%\_full' ESCAPE '\\'
                  AND EXISTS (
                      SELECT 1 FROM resolved_series rs4
                      WHERE rs4.feature_id = fr.id
                        AND rs4.obs_date >= :recent
                  )
            """), {"d30": lookback_30, "recent": today - timedelta(days=7)}).fetchall()
            for r in rows:
                if r[1] and r[2] and float(r[2]) != 0:
                    pct = (float(r[1]) - float(r[2])) / float(r[2])
                    price_changes[r[0]] = round(pct, 5)
    except Exception as exc:
        log.warning("Price change query failed: {e}", e=str(exc))

    # ETF 30d change (use lowercase etf name + _full or etf ticker in yfinance format)
    etf_ticker = sector.get("etf", "")
    etf_key = etf_ticker.lower()
    etf_change = price_changes.get(f"{etf_key}_full") or price_changes.get(etf_key)

    # ── Build subsector detail ──────────────────────────────────
    subsectors = {}
    for sub_name, sub in sector.get("subsectors", {}).items():
        sub_weight = sub.get("weight", 1.0)
        actor_details = []

        for actor in sub.get("actors", []):
            ticker = actor.get("ticker")
            influence = round(sub_weight * actor["weight"], 4)

            # Z-scores from features
            actor_z = []
            for feat in actor.get("features", []):
                z = z_map.get(feat)
                v = val_map.get(feat)
                if z is not None:
                    actor_z.append({"feature": feat, "z": z, "value": v})
            avg_z = round(sum(d["z"] for d in actor_z) / len(actor_z), 3) if actor_z else None

            # Price data from _full feature
            latest_price = None
            pct_30d = None
            rel_perf = None
            if ticker:
                tk = ticker.lower().replace("-", "_")
                full_key = f"{tk}_full"
                latest_price = val_map.get(full_key) or val_map.get(tk)
                pct_30d = price_changes.get(full_key)
                if pct_30d is not None and etf_change is not None:
                    rel_perf = round(pct_30d - etf_change, 5)

            # Options
            opts = opts_map.get(ticker) if ticker else None

            actor_details.append({
                "name": actor["name"],
                "ticker": ticker,
                "type": actor["type"],
                "weight": actor["weight"],
                "influence": influence,
                "description": actor.get("description", ""),
                "avg_z": avg_z,
                "live": actor_z,
                "latest_price": latest_price,
                "pct_30d": round(pct_30d, 5) if pct_30d is not None else None,
                "rel_perf_vs_etf": rel_perf,
                "options": opts,
            })

        # Sort actors within subsector by relative performance (outperformers first)
        actor_details.sort(
            key=lambda a: (a["rel_perf_vs_etf"] if a["rel_perf_vs_etf"] is not None else -999),
            reverse=True,
        )

        subsectors[sub_name] = {
            "weight": sub_weight,
            "actors": actor_details,
        }

    # ── SPY benchmark for relative strength ──────────────────────
    spy_change = price_changes.get("spy_full")

    # ── ETF spot price ─────────────────────────────────────────
    etf_price = val_map.get(f"{etf_key}_full") or val_map.get(etf_key) or (
        opts_map.get(etf_ticker, {}).get("spot")
    )

    # ── Sector metrics: insider + congressional + dark pool ────
    sector_tickers = []
    for sub in sector.get("subsectors", {}).values():
        for a in sub.get("actors", []):
            if a.get("ticker"):
                sector_tickers.append(a["ticker"])

    insider_activity: list[dict] = []
    congressional_activity: list[dict] = []
    dark_pool_signal = "neutral"
    etf_flow_5d: float | None = None

    # Each query runs independently so one missing table doesn't kill the rest
    if sector_tickers:
        placeholders = ", ".join(f":t{i}" for i in range(len(sector_tickers)))
        ticker_params = {f"t{i}": t for i, t in enumerate(sector_tickers)}

        try:
            with engine.connect() as conn:
                params = {**ticker_params, "d30": lookback_30}
                ins_rows = conn.execute(text(
                    "SELECT ticker, trade_date, insider_name, trade_type, shares, value "
                    "FROM insider_trades "
                    "WHERE ticker IN (" + placeholders + ") AND trade_date >= :d30 "
                    "ORDER BY value DESC NULLS LAST LIMIT 20"
                ), params).fetchall()
                for r in ins_rows:
                    insider_activity.append({
                        "ticker": r[0], "date": str(r[1]) if r[1] else None,
                        "name": r[2], "type": r[3],
                        "shares": r[4], "value": float(r[5]) if r[5] else None,
                    })
        except Exception as exc:
            log.warning("insider_trades query failed (non-fatal): {e}", e=str(exc))

        try:
            with engine.connect() as conn:
                params = {**ticker_params, "d60": today - timedelta(days=60)}
                cong_rows = conn.execute(text(
                    "SELECT ticker, disclosure_date, representative, transaction_type, amount "
                    "FROM congressional_trades "
                    "WHERE ticker IN (" + placeholders + ") AND disclosure_date >= :d60 "
                    "ORDER BY disclosure_date DESC LIMIT 20"
                ), params).fetchall()
                for r in cong_rows:
                    congressional_activity.append({
                        "ticker": r[0], "date": str(r[1]) if r[1] else None,
                        "representative": r[2], "type": r[3], "amount": r[4],
                    })
        except Exception as exc:
            log.warning("congressional_trades query failed (non-fatal): {e}", e=str(exc))

        try:
            with engine.connect() as conn:
                dp_rows = conn.execute(text(
                    "SELECT ticker, short_volume, total_volume "
                    "FROM dark_pool_weekly "
                    "WHERE ticker IN (" + placeholders + ") "
                    "AND report_date = (SELECT MAX(report_date) FROM dark_pool_weekly)"
                ), ticker_params).fetchall()
                total_short = sum(float(r[1] or 0) for r in dp_rows)
                total_vol = sum(float(r[2] or 0) for r in dp_rows)
                if total_vol > 0:
                    ratio = total_short / total_vol
                    dark_pool_signal = (
                        "accumulation" if ratio < 0.40
                        else "distribution" if ratio > 0.55
                        else "neutral"
                    )
        except Exception as exc:
            log.warning("dark_pool_weekly query failed (non-fatal): {e}", e=str(exc))

    try:
        with engine.connect() as conn:
            etf_flow_row = conn.execute(text(
                "SELECT SUM(flow_value) FROM etf_flows "
                "WHERE ticker = :etf AND flow_date >= :d5"
            ), {"etf": etf_ticker, "d5": today - timedelta(days=5)}).fetchone()
            if etf_flow_row and etf_flow_row[0] is not None:
                etf_flow_5d = float(etf_flow_row[0])
    except Exception as exc:
        log.warning("etf_flows query failed (non-fatal): {e}", e=str(exc))

    relative_strength_1m = None
    if etf_change is not None and spy_change is not None:
        relative_strength_1m = round(etf_change - spy_change, 5)

    # ── Intelligence: lever pullers + convergence + narrative ───
    lever_pullers: list[dict] = []
    convergence: list[dict] = []
    narrative: str = ""

    try:
        from intelligence.lever_pullers import get_lever_pullers
        all_lps = get_lever_pullers(engine)
        # Filter to those whose tickers overlap with this sector
        ticker_set = set(sector_tickers)
        for lp in all_lps:
            if lp.get("ticker") in ticker_set or lp.get("sector") == sector_name:
                lever_pullers.append(lp)
        lever_pullers = lever_pullers[:10]
    except Exception as exc:
        log.debug("Flows: lever pullers fetch failed: {e}", e=str(exc))

    try:
        from intelligence.trust_scorer import TrustScorer
        ts = TrustScorer(engine)
        conv_all = ts.get_convergence_alerts()
        ticker_set = set(sector_tickers)
        for c in conv_all:
            if c.get("ticker") in ticker_set:
                convergence.append(c)
        convergence = convergence[:10]
    except Exception as exc:
        log.debug("Flows: convergence signals fetch failed: {e}", e=str(exc))

    try:
        from ollama.client import ask_ollama
        sector_summary_parts = []
        for sub_name_k, sub_data in subsectors.items():
            top = sub_data["actors"][:3]
            names = ", ".join(
                f"{a['ticker'] or a['name']} ({'+' if (a.get('pct_30d') or 0) >= 0 else ''}{((a.get('pct_30d') or 0) * 100):.1f}%)"
                for a in top
            )
            sector_summary_parts.append(f"{sub_name_k}: {names}")
        prompt = (
            f"In 2-3 sentences, summarize the investment narrative for the {sector_name} sector. "
            f"Subsector breakdown: {'; '.join(sector_summary_parts)}. "
            f"ETF {etf_ticker} 30d change: {'+' if (etf_change or 0) >= 0 else ''}{((etf_change or 0) * 100):.1f}%. "
            f"Dark pool signal: {dark_pool_signal}."
        )
        llm_resp = ask_ollama(prompt)
        if llm_resp and not llm_resp.get("error"):
            narrative = (llm_resp.get("response") or llm_resp.get("text") or "")[:500]
    except Exception as exc:
        log.debug("Flows: LLM narrative generation failed: {e}", e=str(exc))

    # ── Attach per-actor insider/options signals to subsectors ──
    insider_tickers_buy = {r["ticker"] for r in insider_activity if r.get("type") in ("P", "Purchase", "Buy")}
    insider_tickers_sell = {r["ticker"] for r in insider_activity if r.get("type") in ("S", "Sale", "Sell")}
    for _sub_name, sub_data in subsectors.items():
        for actor in sub_data["actors"]:
            tk = actor.get("ticker")
            # Insider signal
            if tk in insider_tickers_buy:
                actor["insider_signal"] = "buy"
            elif tk in insider_tickers_sell:
                actor["insider_signal"] = "sell"
            else:
                actor["insider_signal"] = None
            # Options signal
            opts_d = actor.get("options")
            if opts_d and opts_d.get("pcr") is not None:
                pcr = opts_d["pcr"]
                actor["options_signal"] = (
                    "call_heavy" if pcr < 0.7
                    else "put_heavy" if pcr > 1.3
                    else "balanced"
                )
            else:
                actor["options_signal"] = None

    return {
        "sector": sector_name,
        "etf": etf_ticker,
        "price": etf_price,
        "change_1m": etf_change,
        "subsectors": subsectors,
        "etf_options": opts_map.get(etf_ticker),
        "sector_metrics": {
            "relative_strength_1m": relative_strength_1m,
            "etf_flow_5d": etf_flow_5d,
            "dark_pool_signal": dark_pool_signal,
            "congressional_activity": congressional_activity,
            "insider_activity": insider_activity,
        },
        "intelligence": {
            "lever_pullers": lever_pullers,
            "convergence": convergence,
            "narrative": narrative,
        },
    }


@router.get("/sector/{sector_name}")
async def get_sector_dive(
    sector_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Alias for sector detail — used by the SectorDive frontend view."""
    return await get_sector_detail(sector_name, _token)


@router.get("/sankey")
async def get_sankey_data(
    as_of: str | None = None,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return capital flow data formatted for Sankey visualization.

    Nodes: sectors, subsectors, actors (3 levels).
    Links: weighted by relative performance vs SPY (positive = inflow, negative = outflow).
    Includes historical snapshots for time scrubbing.
    """
    from datetime import date, timedelta
    from analysis.sector_map import SECTOR_MAP

    engine = get_db_engine()
    today = date.fromisoformat(as_of) if as_of else date.today()

    # Build nodes and links from sector map + live data
    nodes = []
    links = []
    node_idx = {}

    def get_node_id(name, level):
        key = f"{level}:{name}"
        if key not in node_idx:
            node_idx[key] = len(nodes)
            nodes.append({"id": node_idx[key], "name": name, "level": level})
        return node_idx[key]

    # Source node (market)
    market_id = get_node_id("Market", "root")

    # Get price changes for relative performance
    price_changes: dict[str, float] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT fr.name,
                    (SELECT rs2.value FROM resolved_series rs2
                     WHERE rs2.feature_id = fr.id
                     ORDER BY rs2.obs_date DESC LIMIT 1) as latest,
                    (SELECT rs3.value FROM resolved_series rs3
                     WHERE rs3.feature_id = fr.id
                       AND rs3.obs_date <= :d30
                     ORDER BY rs3.obs_date DESC LIMIT 1) as prev
                FROM feature_registry fr
                WHERE fr.name LIKE '%\_full' ESCAPE '\\'
                  AND fr.family IN ('equity', 'crypto', 'commodity', 'rates', 'credit')
                  AND EXISTS (
                      SELECT 1 FROM resolved_series rs4
                      WHERE rs4.feature_id = fr.id AND rs4.obs_date >= :recent
                  )
            """), {"d30": today - timedelta(days=30), "recent": today - timedelta(days=7)}).fetchall()
            for r in rows:
                if r[1] and r[2] and float(r[2]) != 0:
                    price_changes[r[0]] = round((float(r[1]) - float(r[2])) / float(r[2]), 5)
    except Exception as exc:
        log.debug("Flows: price changes query failed: {e}", e=str(exc))

    # SPY benchmark
    spy_change = price_changes.get("spy_full", 0)

    # Build Sankey from sector map
    for sector_name, sector in SECTOR_MAP.items():
        etf = sector.get("etf", "")
        etf_key = f"{etf.lower()}_full"
        sector_perf = price_changes.get(etf_key, 0) - spy_change

        sector_id = get_node_id(sector_name, "sector")

        # Market → Sector link (weighted by absolute flow)
        flow_value = max(0.1, abs(sector_perf) * 1000)
        links.append({
            "source": market_id if sector_perf >= 0 else sector_id,
            "target": sector_id if sector_perf >= 0 else market_id,
            "value": round(flow_value, 2),
            "flow_pct": round(sector_perf * 100, 2),
            "direction": "inflow" if sector_perf >= 0 else "outflow",
            "label": f"{sector_name}: {'+'if sector_perf>=0 else ''}{sector_perf*100:.1f}% vs SPY",
        })

        # Sector → Subsectors
        for sub_name, sub in sector.get("subsectors", {}).items():
            sub_id = get_node_id(f"{sector_name}/{sub_name}", "subsector")
            sub_weight = sub.get("weight", 0.5)

            # Subsector inherits sector flow, weighted
            sub_flow = flow_value * sub_weight
            links.append({
                "source": sector_id,
                "target": sub_id,
                "value": round(sub_flow, 2),
                "flow_pct": round(sector_perf * 100 * sub_weight, 2),
                "direction": "inflow" if sector_perf >= 0 else "outflow",
            })

            # Subsector → Actors
            for actor in sub.get("actors", []):
                ticker = actor.get("ticker")
                if not ticker:
                    continue
                actor_id = get_node_id(ticker, "actor")
                tk_key = f"{ticker.lower().replace('-','_')}_full"
                actor_perf = price_changes.get(tk_key, 0) - spy_change
                actor_flow = max(0.05, abs(actor_perf) * 1000) * actor.get("weight", 0.1)

                links.append({
                    "source": sub_id,
                    "target": actor_id,
                    "value": round(actor_flow, 2),
                    "flow_pct": round(actor_perf * 100, 2),
                    "direction": "inflow" if actor_perf >= 0 else "outflow",
                    "ticker": ticker,
                    "actor_name": actor.get("name"),
                })

    # Get historical snapshots for time slider
    snapshots = []
    try:
        with engine.connect() as conn:
            snap_rows = conn.execute(text(
                "SELECT snapshot_date, relative_strength, narrative "
                "FROM capital_flow_snapshots "
                "ORDER BY snapshot_date DESC LIMIT 30"
            )).fetchall()
            for r in snap_rows:
                snapshots.append({
                    "date": str(r[0]),
                    "relative_strength": r[1] if isinstance(r[1], dict) else {},
                    "narrative": (r[2] or "")[:200],
                })
    except Exception as exc:
        log.debug("Flows: sector analytical snapshots query failed: {e}", e=str(exc))

    # ── Actionable setups: tickers at thematic intersections ──
    setups = []
    opts_map: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            opts = conn.execute(text(
                "SELECT ticker, put_call_ratio, iv_atm, max_pain, spot_price "
                "FROM options_daily_signals "
                "WHERE signal_date = (SELECT MAX(signal_date) FROM options_daily_signals)"
            )).fetchall()
            for o in opts:
                opts_map[o[0]] = {"pcr": o[1], "iv": o[2], "max_pain": o[3], "spot": o[4]}
    except Exception as exc:
        log.debug("Flows: options map query failed: {e}", e=str(exc))

    # Score each actor by thematic relevance
    for sector_name, sector in SECTOR_MAP.items():
        for sub_name, sub in sector.get("subsectors", {}).items():
            for actor in sub.get("actors", []):
                ticker = actor.get("ticker")
                if not ticker:
                    continue
                tk_key = f"{ticker.lower().replace('-','_')}_full"
                perf = price_changes.get(tk_key, 0) - spy_change
                opts_data = opts_map.get(ticker)
                themes = []

                # Thematic tagging
                desc = (actor.get("description", "") + " " + sub_name + " " + sector_name).lower()
                if any(k in desc for k in ["defense", "military", "government", "dod"]):
                    themes.append("Defense Spending")
                if any(k in desc for k in ["ai", "semiconductor", "chip", "compute", "gpu"]):
                    themes.append("AI / Compute")
                if any(k in desc for k in ["energy", "oil", "gas", "lng"]):
                    themes.append("Energy Security")
                if any(k in desc for k in ["healthcare", "pharma", "biotech"]):
                    themes.append("Healthcare")
                if any(k in desc for k in ["fintech", "payment", "bank"]):
                    themes.append("Financials")
                if any(k in desc for k in ["cloud", "software", "saas"]):
                    themes.append("Cloud / Software")

                if not themes:
                    continue

                # Build setup with actionable info
                setup = {
                    "ticker": ticker,
                    "name": actor.get("name"),
                    "sector": sector_name,
                    "subsector": sub_name,
                    "themes": themes,
                    "perf_vs_spy": round(perf * 100, 2),
                    "influence": round(sub.get("weight", 0) * actor.get("weight", 0), 4),
                }

                # Options insight
                if opts_data:
                    setup["options"] = opts_data
                    pcr = opts_data.get("pcr", 1.0)
                    iv = opts_data.get("iv")
                    spot = opts_data.get("spot")
                    mp = opts_data.get("max_pain")

                    # Generate action
                    if perf < -0.03 and pcr < 0.8 and iv and iv < 0.4:
                        setup["action"] = f"Potential entry — pullback with bullish options flow and cheap IV"
                        setup["action_type"] = "BUY"
                    elif perf > 0.05 and pcr > 1.2:
                        setup["action"] = f"Caution — outperforming but heavy put buying signals hedging"
                        setup["action_type"] = "WATCH"
                    elif perf < -0.05 and pcr > 1.3:
                        setup["action"] = f"Bearish setup — underperforming with put accumulation"
                        setup["action_type"] = "AVOID"
                    elif iv and iv < 0.25:
                        setup["action"] = f"IV cheap ({iv*100:.0f}%) — options are historically inexpensive"
                        setup["action_type"] = "OPTIONS"
                    elif len(themes) >= 2:
                        setup["action"] = f"Multi-theme exposure ({', '.join(themes[:2])}) — monitor for catalyst"
                        setup["action_type"] = "WATCH"

                if "action" in setup:
                    setups.append(setup)

    # ── Physics scoring: wire dealer gamma, momentum, news energy ──
    _apply_physics_scores(engine, today, setups)

    # Sort by physics composite (highest force first), fallback to theme count + perf
    setups.sort(
        key=lambda s: (
            s.get("physics", {}).get("composite", 0),
            len(s["themes"]),
            abs(s.get("perf_vs_spy", 0)),
        ),
        reverse=True,
    )

    # Market posture
    inflow_count = sum(1 for l in links if l.get("direction") == "inflow" and l.get("source") == market_id)
    outflow_count = sum(1 for l in links if l.get("direction") == "outflow" and l.get("source") != market_id)
    posture = "RISK-ON" if inflow_count > outflow_count * 1.5 else "RISK-OFF" if outflow_count > inflow_count * 1.5 else "MIXED"

    return {
        "nodes": nodes,
        "links": links,
        "setups": setups[:15],
        "posture": posture,
        "snapshots": snapshots,
        "as_of": today.isoformat(),
        "spy_30d": round(spy_change * 100, 2),
        "node_count": len(nodes),
        "link_count": len(links),
    }


def _apply_physics_scores(engine, today, setups: list) -> None:
    """Enrich setups with dealer gamma, momentum, and news energy physics scores.

    Mutates each setup dict in place, adding a ``physics`` sub-dict.  Any
    component that fails (missing data, DB error) is silently skipped so the
    setup still shows with whatever physics data is available.
    """
    from datetime import date
    from api.dependencies import get_pit_store

    # -- 1) Dealer Gamma (per-ticker) ------------------------------------
    gex_by_ticker: dict[str, dict] = {}
    try:
        from physics.dealer_gamma import DealerGammaEngine
        dge = DealerGammaEngine(engine)
        tickers = {s["ticker"] for s in setups if s.get("ticker")}
        for ticker in tickers:
            try:
                result = dge.compute_gex_profile(ticker, today)
                if "error" not in result:
                    gex_by_ticker[ticker] = result
            except Exception as exc:
                log.debug("Flows: GEX profile failed for {t}: {e}", t=ticker, e=str(exc))
    except Exception as exc:
        log.debug("Dealer gamma engine unavailable: {e}", e=str(exc))

    # -- 2) News Momentum (market-wide) ----------------------------------
    momentum_result = None
    try:
        from physics.momentum import NewsMomentumAnalyzer
        pit = get_pit_store()
        nma = NewsMomentumAnalyzer(engine, pit)
        momentum_result = nma.analyze(today)
        if not momentum_result.available:
            momentum_result = None
    except Exception as exc:
        log.debug("News momentum unavailable: {e}", e=str(exc))

    # -- 3) News Energy (market-wide) ------------------------------------
    news_energy_result = None
    try:
        from physics.news_energy import NewsEnergyEngine
        pit = get_pit_store()
        nee = NewsEnergyEngine(engine, pit)
        news_energy_result = nee.analyze(today)
        if not news_energy_result.get("energy_by_source"):
            news_energy_result = None
    except Exception as exc:
        log.debug("News energy engine unavailable: {e}", e=str(exc))

    # -- 4) Score each setup ---------------------------------------------
    for setup in setups:
        ticker = setup.get("ticker")
        physics: dict = {}
        composite_parts: list[float] = []

        # GEX ---------------------------------------------------------------
        gex_data = gex_by_ticker.get(ticker) if ticker else None
        if gex_data:
            gex_norm = gex_data.get("gex_normalized", 0)
            regime = gex_data.get("regime", "NEUTRAL")
            physics["gex"] = round(gex_norm, 4)
            physics["gex_regime"] = regime
            if regime == "SHORT_GAMMA":
                physics["gex_interpretation"] = (
                    "Dealers short gamma \u2014 moves will be amplified"
                )
            elif regime == "LONG_GAMMA":
                physics["gex_interpretation"] = (
                    "Dealers long gamma \u2014 moves will be dampened"
                )
            else:
                physics["gex_interpretation"] = (
                    "Neutral gamma \u2014 no strong dealer hedging bias"
                )
            # Map to 0..1 score: short gamma = high force
            gex_score = min(1.0, max(0.0, (-gex_norm + 1) / 2))
            composite_parts.append(gex_score * 0.4)  # 40 % weight

        # Momentum -----------------------------------------------------------
        if momentum_result:
            trend = momentum_result.sentiment_trend
            direction = momentum_result.momentum_direction
            energy_state = momentum_result.energy_state
            velocity = momentum_result.details.get("momentum", {}).get("velocity", 0)

            physics["momentum_trend"] = trend
            physics["momentum_direction"] = direction
            physics["momentum_energy"] = energy_state

            interp_parts = []
            if direction == "accelerating":
                interp_parts.append("Accelerating")
            elif direction == "decelerating":
                interp_parts.append("Decelerating")
            else:
                interp_parts.append("Stable")

            if trend == "rising":
                interp_parts.append("uptrend")
            elif trend == "falling":
                interp_parts.append("downtrend")
            else:
                interp_parts.append("trend")

            interp_parts.append(
                "\u2014 momentum confirms direction"
                if direction == "accelerating"
                else "\u2014 momentum fading"
                if direction == "decelerating"
                else "\u2014 momentum neutral"
            )
            physics["momentum_interpretation"] = " ".join(interp_parts)

            # Score: accelerating + high energy = strongest
            mom_score = (
                (0.5 if direction == "accelerating" else 0.2 if direction == "decelerating" else 0.0)
                + (0.5 if energy_state == "high" else 0.25 if energy_state == "medium" else 0.0)
            )
            composite_parts.append(mom_score * 0.3)  # 30 % weight

        # News Energy --------------------------------------------------------
        if news_energy_result:
            total_energy = news_energy_result.get("total_news_energy", 0)
            coherence = news_energy_result.get("coherence", {}).get("coherence", 0)
            regime_eq = news_energy_result.get("regime_signal", {}).get("equilibrium", True)

            physics["news_energy"] = round(total_energy, 4)
            physics["news_coherence"] = round(coherence, 3)
            physics["news_regime_equilibrium"] = regime_eq

            if total_energy > 10:
                physics["news_interpretation"] = (
                    "Elevated news energy \u2014 catalyst-driven move likely"
                )
            elif total_energy > 3:
                physics["news_interpretation"] = (
                    "Building news energy \u2014 watch for continuation"
                )
            else:
                physics["news_interpretation"] = (
                    "Low news energy \u2014 intelligence streams quiet"
                )

            # Score: high energy + high coherence = high force
            energy_score = min(1.0, total_energy / 10.0) * 0.6 + min(1.0, coherence) * 0.4
            composite_parts.append(energy_score * 0.3)  # 30 % weight

        # Composite ----------------------------------------------------------
        composite = sum(composite_parts) if composite_parts else 0.0
        physics["composite"] = round(composite, 4)

        if composite > 0.6:
            physics["force_label"] = "HIGH FORCE"
        elif composite > 0.3:
            physics["force_label"] = "MODERATE"
        else:
            physics["force_label"] = "LOW FORCE"

        # -- Enhance action text with physics context -----------------------
        gex_regime = physics.get("gex_regime")
        mom_dir = physics.get("momentum_direction")
        news_e = physics.get("news_energy", 0)
        old_action = setup.get("action", "")

        if gex_regime == "SHORT_GAMMA" and mom_dir == "accelerating":
            setup["action"] = (
                f"High-force setup \u2014 dealers amplifying, momentum confirming. "
                f"{old_action}"
            )
        elif news_e and news_e > 5:
            setup["action"] = (
                f"Catalyst-driven \u2014 elevated news energy, watch for continuation. "
                f"{old_action}"
            )
        elif composite > 0.5:
            setup["action"] = (
                f"Physics confirm ({physics['force_label'].lower()}). {old_action}"
            )

        setup["physics"] = physics


@router.get("/gaps")
async def get_gaps(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Return data gap analysis across the sector map."""
    from analysis.research_agent import analyze_gaps
    engine = get_db_engine()
    return analyze_gaps(engine)


@router.post("/research")
async def run_research(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Run a full research sweep — gaps, hypotheses, LLM actor analysis."""
    from analysis.research_agent import run_full_research
    engine = get_db_engine()
    return run_full_research(engine)


@router.post("/fill-gaps")
async def fill_gaps(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Fill missing stock data from the sector map."""
    from analysis.research_agent import fill_missing_stocks
    engine = get_db_engine()
    return fill_missing_stocks(engine)


@router.post("/test-hypotheses")
async def test_hypotheses(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Run backtesting on all CANDIDATE hypotheses.

    Tests lead/lag relationships using lagged cross-correlation
    and updates hypothesis states to PASSED/FAILED/TESTING.
    """
    from analysis.hypothesis_tester import run_all_tests
    engine = get_db_engine()
    return run_all_tests(engine)


# ── Global Money Flow Map ─────────────────────────────────────────────

_money_map_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_MONEY_MAP_TTL: float = 900.0  # 15 minutes


@router.get("/money-map")
async def get_money_map(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Return the full global money flow map.

    Aggregates Fed balance sheet, banking credit, market prices, sector
    rotation, options positioning, dark pool signals, insider/congressional
    trades, and trust scorer convergence into a single hierarchical structure.

    Cached for 15 minutes.
    """
    import time
    now = time.time()

    if _money_map_cache["data"] is not None and (now - _money_map_cache["ts"]) < _MONEY_MAP_TTL:
        log.debug("Money map cache hit")
        return _money_map_cache["data"]

    from analysis.money_flow import build_flow_map
    engine = get_db_engine()
    result = build_flow_map(engine)

    _money_map_cache["data"] = result
    _money_map_cache["ts"] = now

    return result


# ── Sector drill-down endpoint ────────────────────────────────────────


@router.get("/sector/{name}")
async def get_sector_drill(
    name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Drill into a sector: subsectors, companies, actors, and flows.

    Returns subsectors with top companies (price, flow, signals),
    actors with influence scores, and aggregate flow totals.
    """
    from analysis.money_flow import get_sector_drill as _get_sector_drill

    engine = get_db_engine()
    return _get_sector_drill(engine, name)


# ── Company drill-down endpoint ───────────────────────────────────────


@router.get("/company/{ticker}")
async def get_company_drill(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Drill into a company: power players, actions, and connections.

    Returns actors (insiders, congressional holders, fund managers),
    their recent actions, dollar amounts, and trust scores.
    """
    from analysis.money_flow import get_company_drill as _get_company_drill

    engine = get_db_engine()
    return _get_company_drill(engine, ticker)


# ── Aggregated dollar flow endpoint ─────────────────────────────────────

_agg_flow_cache: dict[str, Any] = {"data": None, "ts": 0.0, "key": ""}
_AGG_FLOW_TTL: float = 300.0  # 5 minutes


@router.get("/aggregated")
async def get_aggregated_flows(
    sector: str | None = None,
    period: str = "weekly",
    days: int = 30,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return aggregated dollar flows across all sectors and actor tiers.

    Answers questions like "how much money moved into tech this week?"
    with real USD amounts from normalized signal sources.

    Query Parameters:
        sector: Optional sector name to include time series data for.
        period: 'daily' or 'weekly' for time series buckets (default 'weekly').
        days: Lookback window in days (default 30).

    Returns:
        Dict with by_sector, by_actor_tier, rotation_matrix, and optionally
        time_series (when sector is specified).
    """
    import time

    cache_key = f"{sector}:{period}:{days}"
    now = time.time()

    if (
        _agg_flow_cache["data"] is not None
        and _agg_flow_cache["key"] == cache_key
        and (now - _agg_flow_cache["ts"]) < _AGG_FLOW_TTL
    ):
        log.debug("Aggregated flow cache hit")
        return _agg_flow_cache["data"]

    from analysis.flow_aggregator import get_full_aggregation

    engine = get_db_engine()
    result = get_full_aggregation(engine, sector=sector, period=period, days=days)

    _agg_flow_cache["data"] = result
    _agg_flow_cache["ts"] = now
    _agg_flow_cache["key"] = cache_key

    return result


@router.get("/momentum/{ticker}")
async def get_flow_momentum(
    ticker: str,
    days: int = 30,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return dollar flow momentum for a specific ticker.

    Compares 5-day average net flow against 20-day average to detect
    smart money accumulation or distribution.

    Path Parameters:
        ticker: Stock ticker symbol.

    Query Parameters:
        days: Lookback window in days (default 30).
    """
    from analysis.flow_aggregator import compute_flow_momentum

    engine = get_db_engine()
    return compute_flow_momentum(engine, ticker, days=days)


# ---------------------------------------------------------------------------
# Expanded capital flows: 8-layer junction point flow map
# ---------------------------------------------------------------------------


@router.get("/flow-map-v2")
async def get_flow_map_v2(_token: str = Depends(require_auth)) -> dict:
    """8-layer junction point flow map with edges."""
    from analysis.money_flow_engine import build_flow_map

    engine = get_db_engine()
    flow_map = build_flow_map(engine)
    return flow_map.to_dict()


@router.get("/junction-points")
async def get_junction_points(_token: str = Depends(require_auth)) -> dict:
    """All junction points across 8 layers with current values."""
    from analysis.money_flow_engine import build_flow_map

    engine = get_db_engine()
    flow_map = build_flow_map(engine)

    junction_points = []
    layer_summaries = []

    for layer in flow_map.layers:
        for node in layer.nodes:
            junction_points.append({
                "id": node.id,
                "layer": layer.id,
                "label": node.label,
                "value": node.value,
                "change_1w": node.change_1w,
                "change_1m": node.change_1m,
                "confidence": node.confidence,
                "stress_z": node.z_score,
                "trend": _infer_trend(node),
                "updated_at": None,  # TODO: track in junction_point_readings
                "source": node.source,
            })

        layer_summaries.append({
            "id": layer.id,
            "label": layer.label,
            "order": layer.order,
            "aggregate_value": layer.total_value_usd,
            "aggregate_change_1m": layer.net_flow_1m,
            "dominant_confidence": layer.confidence,
            "stress_z": layer.stress_score,
            "regime": layer.regime,
            "node_count": len(layer.nodes),
        })

    return {
        "junction_points": junction_points,
        "layer_summaries": layer_summaries,
        "total_layers": len(flow_map.layers),
        "total_junction_points": len(junction_points),
    }


def _infer_trend(node) -> str:
    """Infer trend string from node changes."""
    if node.change_1m is None:
        return "unknown"
    if node.change_1w is not None and node.change_1m != 0:
        # Compare weekly rate to monthly rate
        weekly_rate = node.change_1w / max(abs(node.change_1m), 1)
        if weekly_rate > 0.4:
            return "accelerating"
        elif weekly_rate < -0.1:
            return "decelerating"
    if node.change_1m > 0:
        return "expanding"
    elif node.change_1m < 0:
        return "contracting"
    return "stable"


@router.get("/layers")
async def get_flow_layers(_token: str = Depends(require_auth)) -> dict:
    """Summary of all 8 junction point layers."""
    from analysis.money_flow_engine import build_flow_map

    engine = get_db_engine()
    flow_map = build_flow_map(engine)
    return {
        "layers": [layer.to_dict() for layer in flow_map.layers],
        "edges": [edge.to_dict() for edge in flow_map.edges],
        "global_liquidity_total": flow_map.global_liquidity_total,
        "global_liquidity_change_1m": flow_map.global_liquidity_change_1m,
        "global_policy_score": flow_map.global_policy_score,
    }


@router.get("/layers/{layer_id}")
async def get_flow_layer_detail(layer_id: str, _token: str = Depends(require_auth)) -> dict:
    """Detailed view of a single junction point layer."""
    from analysis.money_flow_engine import build_flow_map

    engine = get_db_engine()
    flow_map = build_flow_map(engine)

    layer = next((l for l in flow_map.layers if l.id == layer_id), None)
    if layer is None:
        return {"error": f"Layer '{layer_id}' not found", "available": [l.id for l in flow_map.layers]}

    # Get edges involving this layer
    layer_edges = [
        e.to_dict() for e in flow_map.edges
        if e.source_layer == layer_id or e.target_layer == layer_id
    ]

    return {
        "layer": layer.to_dict(),
        "edges": layer_edges,
        "inbound_edges": [e for e in layer_edges if e["target_layer"] == layer_id],
        "outbound_edges": [e for e in layer_edges if e["source_layer"] == layer_id],
    }


@router.get("/waterfall")
async def get_flow_waterfall(
    source: str = "fed_balance_sheet",
    _token: str = Depends(require_auth),
) -> dict:
    """Trace money from a source node through all layers."""
    from analysis.money_flow_engine import build_flow_map

    engine = get_db_engine()
    flow_map = build_flow_map(engine)

    # Find starting node
    start_node = None
    start_layer = None
    for layer in flow_map.layers:
        for node in layer.nodes:
            if node.id == source:
                start_node = node
                start_layer = layer
                break
        if start_node:
            break

    if start_node is None:
        all_nodes = [n.id for l in flow_map.layers for n in l.nodes]
        return {"error": f"Node '{source}' not found", "available_nodes": all_nodes}

    starting_value = start_node.value or 0
    chain = [{
        "layer": start_layer.id,
        "node": start_node.id,
        "label": start_node.label,
        "value": starting_value,
        "attenuation": 0.0,
        "confidence": start_node.confidence,
    }]

    # Follow outbound edges layer by layer
    visited_layers = {start_layer.id}
    current_value = starting_value

    # Sort layers by order
    sorted_layers = sorted(flow_map.layers, key=lambda l: l.order)

    for layer in sorted_layers:
        if layer.id in visited_layers:
            continue

        # Find edges from any visited layer to this layer
        relevant_edges = [
            e for e in flow_map.edges
            if e.source_layer in visited_layers and e.target_layer == layer.id
        ]

        if not relevant_edges:
            continue

        # Sum flow into this layer
        total_flow = sum(e.value_usd for e in relevant_edges)
        if total_flow <= 0 and current_value > 0:
            # Estimate attenuation
            total_flow = current_value * 0.3  # default 30% pass-through

        attenuation = 1.0 - (total_flow / current_value) if current_value > 0 else 1.0
        attenuation = max(0.0, min(1.0, attenuation))

        # Pick the node with highest edge flow
        best_edge = max(relevant_edges, key=lambda e: e.value_usd) if relevant_edges else None
        target_node_id = best_edge.target_node if best_edge else layer.nodes[0].id if layer.nodes else layer.id
        target_label = target_node_id
        for n in layer.nodes:
            if n.id == target_node_id:
                target_label = n.label
                break

        # Best confidence from edges
        edge_conf = best_edge.confidence if best_edge else "estimated"

        chain.append({
            "layer": layer.id,
            "node": target_node_id,
            "label": target_label,
            "value": round(total_flow, 2),
            "attenuation": round(attenuation, 3),
            "confidence": edge_conf,
        })

        visited_layers.add(layer.id)
        current_value = total_flow

    return {
        "source": source,
        "starting_value": starting_value,
        "chain": chain,
    }


@router.get("/orthogonality")
async def get_flow_orthogonality(_token: str = Depends(require_auth)) -> dict:
    """PCA decomposition and correlation matrix of junction point flows."""
    from analysis.money_flow_engine import build_flow_map
    import numpy as np

    engine = get_db_engine()
    flow_map = build_flow_map(engine)

    # Collect nodes with numeric values for PCA
    nodes_with_data = []
    for layer in flow_map.layers:
        for node in layer.nodes:
            if node.value is not None and node.change_1m is not None:
                nodes_with_data.append(node)

    if len(nodes_with_data) < 3:
        return {
            "components": [],
            "explained_variance": [],
            "correlation_matrix": {},
            "warning": "Insufficient data for PCA (need 3+ nodes with values)",
        }

    # Build feature matrix: [change_1m, z_score, value_normalized]
    labels = [n.id for n in nodes_with_data]
    values = np.array([n.value for n in nodes_with_data], dtype=float)
    changes = np.array([n.change_1m or 0 for n in nodes_with_data], dtype=float)
    zscores = np.array([n.z_score or 0 for n in nodes_with_data], dtype=float)

    # Normalize values to [0,1] range for comparability
    val_range = values.max() - values.min()
    if val_range > 0:
        values_norm = (values - values.min()) / val_range
    else:
        values_norm = np.zeros_like(values)

    # Feature matrix: each row is a junction point, columns are features
    X = np.column_stack([values_norm, changes / (np.abs(changes).max() or 1), zscores])

    # Center the data
    X_centered = X - X.mean(axis=0)

    # SVD-based PCA (no sklearn dependency needed)
    try:
        U, S, Vt = np.linalg.svd(X_centered, full_matrices=False)
        total_var = (S ** 2).sum()
        explained = [(s ** 2) / total_var for s in S] if total_var > 0 else [0.0] * len(S)

        # Project onto first 2 components
        pc1 = X_centered @ Vt[0]
        pc2 = X_centered @ Vt[1] if len(Vt) > 1 else np.zeros(len(labels))
    except Exception:
        pc1 = np.zeros(len(labels))
        pc2 = np.zeros(len(labels))
        explained = [0.0]

    # Simple K-means clustering (k=3)
    clusters = _simple_kmeans(np.column_stack([pc1, pc2]), k=min(3, len(labels)))

    components = []
    for i, label in enumerate(labels):
        node = nodes_with_data[i]
        components.append({
            "id": label,
            "label": node.label,
            "layer": node.layer,
            "pc1": round(float(pc1[i]), 4),
            "pc2": round(float(pc2[i]), 4),
            "cluster": int(clusters[i]),
            "value": node.value,
            "change_1m": node.change_1m,
        })

    # Correlation matrix (between junction point changes)
    correlation_matrix = {}
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            key = f"{labels[i]}|{labels[j]}"
            # Simple correlation proxy from z-score similarity
            corr = 1.0 - abs(zscores[i] - zscores[j]) / (abs(zscores[i]) + abs(zscores[j]) + 1e-9)
            correlation_matrix[key] = round(float(corr), 3)

    return {
        "components": components,
        "explained_variance": [round(float(e), 4) for e in explained],
        "correlation_matrix": correlation_matrix,
    }


def _simple_kmeans(data, k=3, max_iter=50):
    """Minimal K-means without sklearn."""
    import numpy as np

    n = len(data)
    if n <= k:
        return list(range(n))

    # Initialize with evenly spaced points
    indices = np.linspace(0, n - 1, k, dtype=int)
    centroids = data[indices].copy()
    labels = np.zeros(n, dtype=int)

    for _ in range(max_iter):
        # Assign
        for i in range(n):
            dists = [np.sum((data[i] - c) ** 2) for c in centroids]
            labels[i] = int(np.argmin(dists))
        # Update centroids
        new_centroids = []
        for c in range(k):
            mask = labels == c
            if mask.any():
                new_centroids.append(data[mask].mean(axis=0))
            else:
                new_centroids.append(centroids[c])
        new_centroids = np.array(new_centroids)
        if np.allclose(centroids, new_centroids):
            break
        centroids = new_centroids

    return labels.tolist()


# ══════════════════════════════════════════════════════════════════
# Image Generation Endpoints
# ══════════════════════════════════════════════════════════════════

@router.get("/generate-image/{image_type}")
async def generate_flow_image(
    image_type: str,
    style: str = "dark",
    model_tier: str = "fast",
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate an AI image from live flow data.

    Image types: flow_infographic, sector_heatmap, junction_dashboard,
    market_briefing, daily_pack.

    Styles: dark, light, cnbc, minimal.
    Model tiers: fast, standard, ultra.
    """
    from intelligence.image_gen import (
        generate_flow_infographic,
        generate_sector_heatmap,
        generate_junction_dashboard,
        generate_market_briefing_image,
        generate_daily_briefing_pack,
    )

    engine = get_db_engine()

    generators = {
        "flow_infographic": generate_flow_infographic,
        "sector_heatmap": generate_sector_heatmap,
        "junction_dashboard": generate_junction_dashboard,
        "market_briefing": generate_market_briefing_image,
    }

    if image_type == "daily_pack":
        results = generate_daily_briefing_pack(engine, style=style)
        return {
            "type": "daily_pack",
            "images": [r.to_dict() for r in results],
            "count": len(results),
        }

    gen_func = generators.get(image_type)
    if gen_func is None:
        return {
            "error": f"Unknown image type: {image_type}",
            "available": list(generators.keys()) + ["daily_pack"],
        }

    result = gen_func(engine, style=style, model_tier=model_tier)
    return {
        "type": image_type,
        "image": result.to_dict(),
    }


@router.post("/generate-image/custom")
async def generate_custom_image(
    prompt: str,
    style: str = "dark",
    model_tier: str = "fast",
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a custom AI image from a user prompt."""
    from intelligence.image_gen import generate_custom

    result = generate_custom(prompt=prompt, style=style, model_tier=model_tier)
    return {
        "type": "custom",
        "image": result.to_dict(),
    }


# ── CDS / Credit Risk ─────────────────────────────────────────────────

@router.get("/cds")
async def get_cds_dashboard(_token: str = Depends(require_auth)) -> dict:
    """CDS-equivalent credit risk dashboard from FRED OAS + ETF spreads."""
    from intelligence.cds_tracker import build_cds_dashboard, cds_to_dict

    engine = get_db_engine()
    dashboard = build_cds_dashboard(engine)
    return cds_to_dict(dashboard)


@router.get("/cds/history/{series_key}")
async def get_cds_history(
    series_key: str,
    days: int = 365,
    _token: str = Depends(require_auth),
) -> dict:
    """Historical spread data for a CDS proxy series."""
    from intelligence.cds_tracker import CDS_SERIES, _get_spread_history
    from datetime import date

    if series_key not in CDS_SERIES:
        return {"error": f"Unknown series: {series_key}", "available": list(CDS_SERIES.keys())}

    cfg = CDS_SERIES[series_key]
    history = _get_spread_history(get_db_engine(), cfg["id"], date.today(), lookback_days=days)

    return {
        "series_key": series_key,
        "label": cfg["label"],
        "description": cfg["desc"],
        "normal_range": cfg["normal_range"],
        "stress_threshold": cfg["stress_threshold"],
        "data": [{"date": d.isoformat(), "value": v} for d, v in history],
        "count": len(history),
    }


# -- Audio Briefing Endpoints -----------------------------------------------


@router.get("/briefing")
async def get_briefing(
    audio: bool = True,
    _token: str = Depends(require_auth),
) -> dict:
    """Generate a daily intelligence briefing (text + optional audio).

    Query params:
        audio: If true (default), also generates the MP3 audio file.

    Returns:
        Script text, audio URL, flow/credit/thesis summaries.
    """
    from intelligence.audio_briefing import (
        generate_briefing_audio,
        generate_briefing_script,
    )

    engine = get_db_engine()

    try:
        if audio:
            result = generate_briefing_audio(engine)
        else:
            result = generate_briefing_script(engine)
    except Exception as exc:
        log.error("Briefing generation failed: {e}", e=str(exc))
        return {"error": str(exc), "status": "FAILED"}

    return {
        "status": "SUCCESS",
        "briefing": result.to_dict(),
    }


@router.get("/briefing/audio")
async def get_briefing_audio(_token: str = Depends(require_auth)):
    """Stream the latest briefing audio file.

    Returns the most recent MP3 briefing as a streaming response.
    If no briefing exists, returns a 404-style JSON error.
    """
    from pathlib import Path

    from fastapi.responses import FileResponse

    from intelligence.audio_briefing import get_latest_briefing

    latest = get_latest_briefing()

    if latest is None or latest.audio_path is None:
        return {"error": "No briefing audio found. Generate one via GET /briefing first."}

    audio_path = Path(latest.audio_path)
    if not audio_path.exists():
        return {"error": "Audio file missing from disk.", "path": str(audio_path)}

    return FileResponse(
        path=str(audio_path),
        media_type="audio/mpeg",
        filename=audio_path.name,
    )


@router.get("/briefing/list")
async def list_briefings(_token: str = Depends(require_auth)) -> dict:
    """List all saved audio briefings, newest first.

    Returns filename, date, size, and whether a script transcript exists.
    """
    from intelligence.audio_briefing import list_all_briefings

    return {"briefings": list_all_briefings()}


@router.get("/briefing/audio/{filename}")
async def get_briefing_audio_by_name(
    filename: str,
    _token: str = Depends(require_auth),
):
    """Stream a specific briefing audio file by filename."""
    from pathlib import Path

    from fastapi.responses import FileResponse

    from intelligence.audio_briefing import get_briefing_by_filename

    briefing = get_briefing_by_filename(filename)
    if briefing is None or briefing.audio_path is None:
        return {"error": f"Briefing '{filename}' not found."}

    audio_path = Path(briefing.audio_path)
    if not audio_path.exists():
        return {"error": "Audio file missing from disk."}

    return FileResponse(
        path=str(audio_path),
        media_type="audio/mpeg",
        filename=audio_path.name,
    )


@router.get("/briefing/detail/{filename}")
async def get_briefing_detail(
    filename: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Get full briefing metadata (script, summaries) for a specific recording."""
    from intelligence.audio_briefing import get_briefing_by_filename

    briefing = get_briefing_by_filename(filename)
    if briefing is None:
        return {"error": f"Briefing '{filename}' not found."}

    return {"status": "SUCCESS", "briefing": briefing.to_dict()}
