"""Watchlist sub-router: AI overview and insider-edge endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.watchlist_helpers import (
    _cache_price_to_db,
    _fetch_live_price,
    _init_table,
    _resolve_feature_names,
)

router = APIRouter(tags=["watchlist"])


@router.get("/{ticker}/overview")
async def get_ticker_overview(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict:
    """AI-generated market overview for a watchlist ticker.

    Gathers price, options, regime, and sector context, then asks the LLM
    to produce a concise 3-5 sentence narrative.  Falls back to a rule-based
    overview when the LLM is unavailable.

    Returns:
        dict with keys: overview, key_levels, sentiment, generated_at,
        sector_path (for the capital-flow mini-chart).
    """
    from datetime import datetime, date

    _init_table()
    engine = get_db_engine()
    ticker_upper = ticker.strip().upper()

    # ── Gather data ──────────────────────────────────────────────
    price_info: dict = {}
    options_info: dict | None = None
    regime_info: dict | None = None
    sector_info: dict = {}
    related_features: list[dict] = []
    feature_names = _resolve_feature_names(ticker_upper)

    with engine.connect() as conn:
        # Price
        try:
            price_row = conn.execute(text(
                "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = ANY(:names) "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ), {"names": feature_names}).fetchone()
            if price_row:
                price_info = {"price": float(price_row[0]), "date": str(price_row[1]), "source": "grid"}
        except Exception:
            pass

        if not price_info:
            live = _fetch_live_price(ticker_upper)
            if live:
                price_info = {"price": live["price"], "pct_1d": live.get("pct_1d"), "source": "live"}
                _cache_price_to_db(engine, ticker_upper, live["price"], date.today())

        # Options (latest)
        try:
            opt_row = conn.execute(text(
                "SELECT signal_date, put_call_ratio, max_pain, iv_atm, iv_skew, "
                "spot_price, total_oi "
                "FROM options_daily_signals "
                "WHERE ticker = :ticker "
                "ORDER BY signal_date DESC LIMIT 1"
            ), {"ticker": ticker_upper}).fetchone()
            if opt_row:
                options_info = {
                    "date": str(opt_row[0]),
                    "put_call_ratio": opt_row[1],
                    "max_pain": opt_row[2],
                    "iv_atm": opt_row[3],
                    "iv_skew": opt_row[4],
                    "spot_price": opt_row[5],
                    "total_oi": opt_row[6],
                }
        except Exception:
            pass

        # Regime
        try:
            regime_row = conn.execute(text(
                "SELECT inferred_state, state_confidence, grid_recommendation "
                "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()
            if regime_row:
                regime_info = {
                    "state": regime_row[0],
                    "confidence": float(regime_row[1]) if regime_row[1] else None,
                    "posture": regime_row[2],
                }
        except Exception:
            pass

        # Related features (recent values for context)
        try:
            tk_lower = ticker_upper.lower().replace("-", "_")
            like_patterns = [f"{tk_lower}%"]
            tk_clean = tk_lower.lstrip("^").replace("=", "")
            if tk_clean != tk_lower:
                like_patterns.append(f"{tk_clean}%")
            if feature_names:
                canonical_base = feature_names[0].rsplit("_", 1)[0]
                pattern = f"{canonical_base}%"
                if pattern not in like_patterns:
                    like_patterns.append(pattern)

            feat_rows = conn.execute(
                text(
                    "SELECT fr.name, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE (" + " OR ".join(
                        f"fr.name LIKE :p{i}" for i in range(len(like_patterns))
                    ) + ") "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
                    "  WHERE rs2.feature_id = rs.feature_id"
                    ") "
                    "ORDER BY fr.name LIMIT 10"
                ),
                {f"p{i}": p for i, p in enumerate(like_patterns)},
            ).fetchall()
            related_features = [
                {"name": r[0], "value": float(r[1]) if r[1] is not None else None, "date": str(r[2])}
                for r in feat_rows
            ]
        except Exception:
            pass

    # ── Sector path (for capital-flow mini-chart) ────────────────
    try:
        from analysis.sector_map import SECTOR_MAP
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    if actor.get("ticker") == ticker_upper:
                        peers = sorted(
                            [
                                {"ticker": a["ticker"], "name": a.get("name", a["ticker"]),
                                 "weight": a.get("weight", 0)}
                                for a in sub.get("actors", [])
                                if a.get("ticker") and a["ticker"] != ticker_upper
                            ],
                            key=lambda p: p["weight"],
                            reverse=True,
                        )[:5]
                        sector_info = {
                            "sector": sector_name,
                            "sector_etf": sector.get("etf"),
                            "subsector": sub_name,
                            "subsector_weight": sub.get("weight", 0),
                            "actor_name": actor.get("name", ticker_upper),
                            "actor_weight": actor.get("weight", 0),
                            "influence": round(sub.get("weight", 0) * actor.get("weight", 0), 4),
                            "description": actor.get("description", ""),
                            "peers": peers,
                        }
                        break
                if sector_info:
                    break
            if sector_info:
                break
    except Exception:
        pass

    # ── Derive sentiment (rule-based) ────────────────────────────
    sentiment_score = 0
    if options_info and options_info.get("put_call_ratio") is not None:
        pcr = options_info["put_call_ratio"]
        if pcr < 0.7:
            sentiment_score += 1
        elif pcr > 1.3:
            sentiment_score -= 1
    if options_info and options_info.get("iv_atm") is not None:
        if options_info["iv_atm"] > 0.4:
            sentiment_score -= 1
    if regime_info and regime_info.get("state"):
        state = regime_info["state"].upper()
        if state == "GROWTH":
            sentiment_score += 1
        elif state in ("CRISIS", "FRAGILE"):
            sentiment_score -= 1

    sentiment = "bullish" if sentiment_score > 0 else "bearish" if sentiment_score < 0 else "neutral"

    # ── Key levels ───────────────────────────────────────────────
    key_levels: list[dict] = []
    if options_info and options_info.get("max_pain") is not None:
        key_levels.append({"label": "Max Pain", "value": options_info["max_pain"]})
    if options_info and options_info.get("spot_price") is not None:
        key_levels.append({"label": "Spot", "value": options_info["spot_price"]})
    if price_info.get("price") is not None:
        key_levels.append({"label": "Last", "value": price_info["price"]})

    # ── Build LLM prompt ─────────────────────────────────────────
    context_parts: list[str] = []
    if price_info.get("price"):
        context_parts.append(f"Current price: ${price_info['price']:.2f}")
    if options_info:
        pcr_val = options_info.get("put_call_ratio")
        iv_val = options_info.get("iv_atm")
        mp_val = options_info.get("max_pain")
        skew_val = options_info.get("iv_skew")
        context_parts.append(
            f"Options: P/C ratio {pcr_val:.2f}, IV ATM {iv_val*100:.1f}%, "
            f"max pain ${mp_val:.0f}, IV skew {skew_val:.2f}"
            if pcr_val is not None and iv_val is not None and mp_val is not None and skew_val is not None
            else "Options data available (partial)"
        )
    if sector_info:
        context_parts.append(
            f"Sector: {sector_info['sector']} / {sector_info['subsector']} — "
            f"{sector_info.get('description', '')}"
        )
    if regime_info:
        context_parts.append(
            f"Macro regime: {regime_info['state']} "
            f"(confidence {regime_info['confidence']*100:.0f}%)"
            if regime_info.get("confidence") else
            f"Macro regime: {regime_info['state']}"
        )
    if related_features:
        feat_summary = ", ".join(
            f"{f['name']}={f['value']:.4f}" for f in related_features[:5] if f.get("value") is not None
        )
        if feat_summary:
            context_parts.append(f"Related features: {feat_summary}")

    prompt_text = (
        f"Write a structured market overview for {ticker_upper}. "
        f"Return ONLY valid JSON (no markdown, no code fences) with this exact schema:\n"
        f'{{"sections": ['
        f'{{"title": "Price Action", "body": "1-2 sentences on current price and recent moves"}},'
        f'{{"title": "Options Flow", "body": "1-2 sentences on options positioning"}},'
        f'{{"title": "Sector Context", "body": "1-2 sentences on sector dynamics"}},'
        f'{{"title": "Risk & Levels", "body": "1-2 sentences on key risk levels to watch"}}'
        f'], "bottom_line": "One sentence: what to do right now"}}\n\n'
        f"Context:\n" + "\n".join(f"- {p}" for p in context_parts)
    )

    llm_system_prompt = (
        "You are a senior market analyst. Respond ONLY with valid JSON matching "
        "the requested schema. Be specific about numbers. No disclaimers."
    )

    # ── Call LLM (llama.cpp first, ollama fallback) ──────────────
    raw_llm_text: str | None = None
    try:
        from llamacpp.client import get_client as get_llamacpp
        llm = get_llamacpp()
        if llm.is_available:
            raw_llm_text = llm.chat(
                messages=[
                    {"role": "system", "content": llm_system_prompt},
                    {"role": "user", "content": prompt_text},
                ],
                temperature=0.3,
                num_predict=800,
            )
    except Exception as exc:
        log.debug("llama.cpp overview failed: {e}", e=str(exc))

    if raw_llm_text is None:
        try:
            from ollama.client import get_client as get_ollama
            llm_ollama = get_ollama()
            if llm_ollama.is_available:
                raw_llm_text = llm_ollama.chat(
                    messages=[
                        {"role": "system", "content": llm_system_prompt},
                        {"role": "user", "content": prompt_text},
                    ],
                    temperature=0.3,
                    num_predict=800,
                )
        except Exception as exc:
            log.debug("Ollama overview failed: {e}", e=str(exc))

    # ── Parse LLM JSON response ─────────────────────────────────
    import json as _json

    sections: list[dict] | None = None
    bottom_line: str | None = None

    if raw_llm_text is not None:
        cleaned = raw_llm_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
        try:
            parsed = _json.loads(cleaned)
            if isinstance(parsed, dict) and "sections" in parsed:
                sections = parsed["sections"]
                bottom_line = parsed.get("bottom_line")
        except (_json.JSONDecodeError, TypeError):
            log.debug("LLM returned non-JSON overview, falling back to single section")
            sections = [{"title": "Overview", "body": raw_llm_text}]

    # ── Rule-based fallback (structured sections) ────────────────
    if sections is None:
        sections = []
        if price_info.get("price"):
            price_body = f"{ticker_upper} is trading at ${price_info['price']:.2f}."
            if price_info.get("pct_1d") is not None:
                pct = price_info["pct_1d"]
                direction = "up" if pct >= 0 else "down"
                price_body += f" The stock is {direction} {abs(pct):.1f}% on the day."
            sections.append({"title": "Price Action", "body": price_body})
        if options_info and options_info.get("put_call_ratio") is not None:
            pcr = options_info["put_call_ratio"]
            opts_sent = "bearish" if pcr > 1.3 else "bullish" if pcr < 0.7 else "neutral"
            opts_body = f"Options positioning is {opts_sent} with a put/call ratio of {pcr:.2f}."
            if options_info.get("iv_atm"):
                opts_body += f" IV ATM sits at {options_info['iv_atm']*100:.1f}%."
            if options_info.get("iv_skew"):
                skew = options_info["iv_skew"]
                skew_desc = "elevated put demand" if skew > 1.3 else "complacent skew" if skew < 0.9 else "normal skew"
                opts_body += f" IV skew at {skew:.2f} indicates {skew_desc}."
            sections.append({"title": "Options Flow", "body": opts_body})
        if sector_info:
            sect_body = (
                f"Within {sector_info['sector']}/{sector_info['subsector']}, "
                f"this name carries {sector_info['influence']:.0%} influence weight."
            )
            if sector_info.get("description"):
                sect_body += f" {sector_info['description']}"
            sections.append({"title": "Sector Context", "body": sect_body})
        risk_parts: list[str] = []
        if regime_info:
            risk_parts.append(f"The macro regime is currently {regime_info['state']}.")
        if options_info and options_info.get("max_pain") is not None and options_info.get("spot_price") is not None:
            mp = options_info["max_pain"]
            spot = options_info["spot_price"]
            gap_pct = ((mp / spot) - 1) * 100 if spot else 0
            risk_parts.append(f"Max pain at ${mp:.0f} ({gap_pct:+.1f}% from spot).")
        if risk_parts:
            sections.append({"title": "Risk & Levels", "body": " ".join(risk_parts)})
        if not sections:
            sections.append({"title": "Overview", "body": f"No detailed data available for {ticker_upper} at this time."})
        bottom_line = f"Monitor {ticker_upper} — sentiment is {sentiment}."

    overview_text = " ".join(s["body"] for s in sections)
    return {
        "overview": overview_text,
        "sections": sections,
        "bottom_line": bottom_line,
        "key_levels": key_levels,
        "sentiment": sentiment,
        "sector_path": sector_info or None,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


# ══════════════════════════════════════════════════════════════════
# Insider Edge — aggregated intelligence for a single ticker
# ══════════════════════════════════════════════════════════════════

@router.get("/{ticker}/edge")
async def get_ticker_edge(
    ticker: str,
    user: dict = Depends(require_auth),
    engine=Depends(get_db_engine),
):
    """Return all intelligence signals for a ticker.

    Aggregates congressional trades, insider filings, dark pool,
    whale flow, prediction markets, smart money, lever pullers,
    investigation leads, and convergence into one response.
    """
    from datetime import date, datetime, timedelta, timezone

    ticker_upper = ticker.upper().strip()

    congressional: list[dict] = []
    insider: list[dict] = []
    dark_pool: dict | None = None
    whale_flow: list[dict] = []
    prediction_markets: list[dict] = []
    smart_money: list[dict] = []

    # 1. Trust scorer: congressional, insider, dark pool
    try:
        from intelligence.trust_scorer import get_insider_edge, detect_convergence

        edge_data = get_insider_edge(engine, ticker_upper)
        if edge_data:
            for sig in edge_data.get("congressional", []):
                meta = sig.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        import json; meta = json.loads(meta)
                    except Exception:
                        meta = {}
                congressional.append({
                    "member": sig.get("member", "Unknown"),
                    "action": sig.get("direction", "BUY"),
                    "amount": meta.get("amount", "N/A"),
                    "date": sig.get("date", ""),
                    "committee": meta.get("committee", "N/A"),
                    "trust_score": round(sig.get("trust_score", 0.5), 2),
                })
            for sig in edge_data.get("insider", []):
                meta = sig.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        import json; meta = json.loads(meta)
                    except Exception:
                        meta = {}
                insider.append({
                    "name": sig.get("insider", "Unknown"),
                    "title": meta.get("title", ""),
                    "action": sig.get("direction", "BUY"),
                    "shares": meta.get("shares", 0),
                    "value": meta.get("value", 0),
                    "date": sig.get("date", ""),
                    "cluster": meta.get("cluster", False),
                })
            dp_signals = edge_data.get("darkpool", [])
            if dp_signals:
                latest_dp = dp_signals[0]
                dp_meta = latest_dp.get("metadata") or {}
                if isinstance(dp_meta, str):
                    try:
                        import json; dp_meta = json.loads(dp_meta)
                    except Exception:
                        dp_meta = {}
                dark_pool = {
                    "volume_vs_avg": dp_meta.get("volume_vs_avg", 1.0),
                    "signal": (
                        "accumulation" if latest_dp.get("direction") == "BUY"
                        else "distribution"
                    ),
                    "date": latest_dp.get("date", ""),
                }
    except Exception as exc:
        log.warning("Edge: trust_scorer failed for {t}: {e}", t=ticker_upper, e=str(exc))

    # 2. Whale flow + prediction markets + smart money from signal_sources
    try:
        with engine.connect() as conn:
            lookback = date.today() - timedelta(days=14)
            whale_rows = conn.execute(text("""
                SELECT source_id, direction, signal_date, metadata
                FROM signal_sources
                WHERE ticker = :t AND source_type = 'scanner'
                  AND signal_date >= :lb
                ORDER BY signal_date DESC LIMIT 10
            """), {"t": ticker_upper, "lb": lookback}).fetchall()
            for r in whale_rows:
                meta = r[3] or {}
                if isinstance(meta, str):
                    try:
                        import json; meta = json.loads(meta)
                    except Exception:
                        meta = {}
                whale_flow.append({
                    "strike": meta.get("strike", 0),
                    "expiry": meta.get("expiry", ""),
                    "direction": str(r[1]),
                    "premium": meta.get("premium", 0),
                    "date": str(r[2]),
                })
            social_rows = conn.execute(text("""
                SELECT source_id, direction, signal_date, trust_score, metadata
                FROM signal_sources
                WHERE ticker = :t AND source_type = 'social'
                  AND signal_date >= :lb
                ORDER BY signal_date DESC LIMIT 10
            """), {"t": ticker_upper, "lb": lookback}).fetchall()
            for r in social_rows:
                meta = r[4] or {}
                if isinstance(meta, str):
                    try:
                        import json; meta = json.loads(meta)
                    except Exception:
                        meta = {}
                smart_money.append({
                    "source": meta.get("platform", "unknown"),
                    "user": str(r[0]),
                    "direction": str(r[1]),
                    "trust_score": round(float(r[3]) if r[3] else 0.5, 2),
                })
            pred_rows = conn.execute(text("""
                SELECT source_id, signal_date, metadata
                FROM signal_sources
                WHERE ticker = :t AND source_type IN ('prediction', 'polymarket')
                  AND signal_date >= :lb
                ORDER BY signal_date DESC LIMIT 5
            """), {"t": ticker_upper, "lb": lookback}).fetchall()
            for r in pred_rows:
                meta = r[2] or {}
                if isinstance(meta, str):
                    try:
                        import json; meta = json.loads(meta)
                    except Exception:
                        meta = {}
                prediction_markets.append({
                    "market": meta.get("market", str(r[0])),
                    "probability": meta.get("probability", 0.5),
                    "change_24h": meta.get("change_24h", 0.0),
                })
    except Exception as exc:
        log.warning("Edge: signal_sources query failed for {t}: {e}", t=ticker_upper, e=str(exc))

    # 3. Lever pullers
    lever_pullers: list[dict] = []
    try:
        from intelligence.lever_pullers import get_lever_context_for_ticker
        lp_data = get_lever_context_for_ticker(engine, ticker_upper)
        for puller in lp_data.get("active_pullers", []):
            lever_pullers.append({
                "name": puller.get("name", "Unknown"),
                "action": puller.get("latest_direction", "UNKNOWN"),
                "context": puller.get("motivation_summary", ""),
            })
    except Exception as exc:
        log.warning("Edge: lever_pullers failed for {t}: {e}", t=ticker_upper, e=str(exc))

    # 4. Actor network context
    try:
        from intelligence.actor_network import get_actor_context_for_ticker
        actor_data = get_actor_context_for_ticker(engine, ticker_upper)
        lp_names = {lp["name"].lower() for lp in lever_pullers}
        for actor in actor_data.get("actors", []):
            if actor.get("name", "").lower() not in lp_names:
                lever_pullers.append({
                    "name": actor.get("name", "Unknown"),
                    "action": "WATCHING",
                    "context": f"{actor.get('title', '')} — {actor.get('motivation', '')}",
                })
    except Exception as exc:
        log.warning("Edge: actor_network failed for {t}: {e}", t=ticker_upper, e=str(exc))

    # 5. Investigation leads
    leads: list[dict] = []
    try:
        with engine.connect() as conn:
            tbl_check = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'investigation_leads'
                )
            """)).scalar()
            if tbl_check:
                lead_rows = conn.execute(text("""
                    SELECT question, status, created_at
                    FROM investigation_leads
                    WHERE ticker = :t
                    ORDER BY created_at DESC LIMIT 10
                """), {"t": ticker_upper}).fetchall()
                for r in lead_rows:
                    leads.append({"question": str(r[0]), "status": str(r[1])})
    except Exception as exc:
        log.debug("Edge: investigation_leads not available: {e}", e=str(exc))

    # 6. Convergence detection
    convergence: dict = {"direction": "neutral", "source_count": 0, "confidence": 0.5}
    try:
        from intelligence.trust_scorer import detect_convergence
        conv_events = detect_convergence(engine, ticker=ticker_upper)
        if conv_events:
            best = conv_events[0]
            convergence = {
                "direction": best.get("direction", "neutral").lower(),
                "source_count": best.get("source_count", 0),
                "confidence": round(best.get("combined_confidence", 0.5), 2),
            }
    except Exception as exc:
        log.warning("Edge: convergence failed for {t}: {e}", t=ticker_upper, e=str(exc))

    # 7. Build edge_summary (rule-based)
    source_count = convergence["source_count"]
    direction = convergence["direction"]
    parts: list[str] = []
    if source_count >= 3:
        parts.append(f"{source_count} independent sources {direction}.")
    elif source_count > 0:
        parts.append(f"{source_count} source(s) leaning {direction}.")
    else:
        parts.append("Limited intelligence signals.")

    signal_descriptions: list[str] = []
    if congressional:
        actions = set(c["action"] for c in congressional)
        signal_descriptions.append(f"Congressional {'buy' if 'BUY' in actions else 'sell'}")
    if dark_pool:
        signal_descriptions.append(f"Dark pool {dark_pool['signal']}")
    if whale_flow:
        dirs = set(w["direction"] for w in whale_flow)
        signal_descriptions.append(f"Whale {'calls' if 'CALL' in dirs or 'BUY' in dirs else 'puts'}")
    if insider:
        actions = set(i["action"] for i in insider)
        signal_descriptions.append("Insider selling" if "SELL" in actions else "Insider buying")
    if signal_descriptions:
        parts.append(" + ".join(signal_descriptions) + ".")
    if insider and any(i["action"] == "SELL" for i in insider):
        cluster = any(i.get("cluster") for i in insider)
        parts.append(
            "Concern: cluster insider selling detected." if cluster
            else "Note: insider selling present (check if scheduled 10b5-1)."
        )
    if leads:
        active = sum(1 for lead in leads if lead["status"] == "investigating")
        if active:
            parts.append(f"{active} active investigation lead(s).")

    return {
        "ticker": ticker_upper,
        "congressional": congressional,
        "insider": insider,
        "dark_pool": dark_pool,
        "whale_flow": whale_flow,
        "prediction_markets": prediction_markets,
        "smart_money": smart_money,
        "lever_pullers": lever_pullers,
        "leads": leads,
        "convergence": convergence,
        "edge_summary": " ".join(parts),
    }
