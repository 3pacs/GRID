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
    except Exception:
        pass

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
    except Exception:
        pass

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

    return {
        "sector": sector_name,
        "etf": etf_ticker,
        "etf_change_30d": round(etf_change, 5) if etf_change is not None else None,
        "etf_options": opts_map.get(etf_ticker),
        "subsectors": subsectors,
    }


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
    except Exception:
        pass

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
    except Exception:
        pass

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
    except Exception:
        pass

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
            except Exception:
                pass
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
