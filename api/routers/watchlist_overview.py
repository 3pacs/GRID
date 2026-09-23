"""Watchlist sub-router: AI overview and insider-edge endpoints."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.watchlist_helpers import (
    _fetch_live_price,
    _resolve_feature_names,
)

# One definition of "is this failure a schema fault?", not two. #477 and #479
# spent two rounds removing exactly this bug class — a query naming a column
# the live schema lacks, swallowed at a level nobody reads — and the classifier
# they landed on lives in intelligence/entity_resolver.py. Importing it keeps
# the two call sites in step when the SQLSTATE list grows; re-deriving it here
# is how they drift. Module-level import is cheap: entity_resolver's own
# imports are stdlib + loguru + sqlalchemy.
from intelligence.entity_resolver import _log_query_failure

router = APIRouter(tags=["watchlist"])


def _round_or_none(value, digits: int = 2) -> float | None:
    """Round a nullable measurement without turning absence into a midpoint."""
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _edge_metadata(value) -> dict:
    """Decode stored JSON metadata without inventing a payload on failure."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _edge_within_days(value, days: int) -> bool:
    """Apply the trust scorer's per-source window to a persisted timestamp."""
    if isinstance(value, datetime):
        observed = value.date()
    elif isinstance(value, date):
        observed = value
    else:
        try:
            observed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except ValueError:
            return False
    return observed >= date.today() - timedelta(days=days)


def _edge_unavailable_response(ticker: str) -> dict:
    """A stable, explicit response when the persisted read model is absent."""
    return {
        "ticker": ticker,
        "status": "unavailable",
        "reason": "signal_sources_unavailable",
        "availability": {"signal_sources": {"status": "unavailable", "reason": "signal_sources_unavailable"}},
        "congressional": [], "insider": [], "dark_pool": None, "whale_flow": [],
        "prediction_markets": [], "smart_money": [], "lever_pullers": [], "leads": [],
        "convergence": {"direction": None, "signal_type": None, "source_count": 0,
                        "confidence": None, "status": "unavailable", "reason": "signal_sources_unavailable"},
        "edge_summary": "Intelligence data is currently unavailable.",
    }


def _edge_optional_rows(engine, statement, params, section: str, availability: dict) -> list:
    """Run one bounded persisted-data read without masking a failed section."""
    try:
        with engine.connect() as conn:
            # Transaction-local timeout; this is a SELECT so the edge route's
            # SQL capture remains read-only even when an optional table stalls.
            conn.execute(text("SELECT set_config('statement_timeout', '2500ms', true)"))
            rows = conn.execute(text(statement), params).fetchall()
        availability[section] = {"status": "available"}
        return rows
    except Exception as exc:
        log.warning("Edge: {section} read unavailable: {error}", section=section, error=str(exc))
        availability[section] = {"status": "unavailable", "reason": f"{section}_unavailable"}
        return []


@router.get("/{ticker}/overview")
def get_ticker_overview(
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
    from datetime import datetime

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
        except Exception as exc:
            # A failed SELECT aborts PostgreSQL's current transaction. Clear it
            # before independent options/regime/features reads on this connection.
            conn.rollback()
            _log_query_failure(f"Overview price query for {ticker_upper}", exc)

        if not price_info:
            live = _fetch_live_price(ticker_upper)
            if live:
                price_info = {"price": live["price"], "pct_1d": live.get("pct_1d"), "source": "live"}

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
        except Exception as exc:
            conn.rollback()
            _log_query_failure(f"Overview options query for {ticker_upper}", exc)

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
        except Exception as exc:
            conn.rollback()
            _log_query_failure("Overview regime query", exc)

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
        except Exception as exc:
            conn.rollback()
            _log_query_failure(f"Overview related-features query for {ticker_upper}", exc)

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
    except Exception as exc:
        log.debug("WatchlistOverview: sector path query failed: {e}", e=str(exc))

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
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.LOCAL)
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


@router.get("/{ticker}/quote")
def get_ticker_quote(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Fast, LLM-free price/options snapshot for a single ticker.

    Powers the stepdad.finance ticker_pulse widget. DB reads + rule-based
    sentiment so the home page populates instantly (the /overview narrative is
    far too slow for a tile). Prefers the cached GRID price; only falls back to
    a live fetch when nothing is stored.

    Returns: ticker, price, change_pct, put_call_ratio, max_pain, iv_atm,
    sentiment, source, as_of, stale (as_of more than 3 calendar days old).
    """
    from datetime import date

    engine = get_db_engine()
    ticker_upper = ticker.strip().upper()
    feature_names = _resolve_feature_names(ticker_upper)

    price: float | None = None
    change_pct: float | None = None
    as_of_date: date | None = None
    source = "grid"
    put_call_ratio = max_pain = iv_atm = None

    with engine.connect() as conn:
        try:
            # Two most recent closes so change_pct reflects the actual prior
            # session rather than always coming back null for GRID-sourced
            # prices (the live-fallback path was the only one that set it).
            # A puller with a lookback window re-inserts the same obs_date
            # under a new vintage_date on every run (uq_resolved_series_composite
            # is (feature_id, obs_date, vintage_date)), so a plain
            # `ORDER BY obs_date DESC LIMIT 2` can return two vintages of the
            # SAME day and turn change_pct into a same-day delta (often 0).
            # Pin to the single feature with the freshest row first, then
            # collapse to one (latest-vintage) value per calendar day.
            rows = conn.execute(text(
                "WITH winner AS ("
                "  SELECT rs.feature_id FROM resolved_series rs "
                "  JOIN feature_registry fr ON fr.id = rs.feature_id "
                "  WHERE fr.name = ANY(:names) "
                "  ORDER BY rs.obs_date DESC, rs.vintage_date DESC LIMIT 1"
                ") "
                "SELECT DISTINCT ON (rs.obs_date) rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "WHERE rs.feature_id = (SELECT feature_id FROM winner) "
                "ORDER BY rs.obs_date DESC, rs.vintage_date DESC LIMIT 2"
            ), {"names": feature_names}).fetchall()
            if rows:
                price = float(rows[0][0])
                as_of_date = rows[0][1]
                if len(rows) > 1 and rows[1][0]:
                    prev_close = float(rows[1][0])
                    if prev_close:
                        change_pct = round((price - prev_close) / prev_close, 5)
        except Exception as exc:
            conn.rollback()
            # A dead price query is why the ticker_pulse card silently fell
            # back to a live fetch for two months. At debug level, in
            # production, nothing recorded that it had happened at all.
            _log_query_failure(f"Quote price query for {ticker_upper}", exc)

        try:
            opt = conn.execute(text(
                "SELECT put_call_ratio, max_pain, iv_atm, signal_date "
                "FROM options_daily_signals WHERE ticker = :ticker "
                "ORDER BY signal_date DESC LIMIT 1"
            ), {"ticker": ticker_upper}).fetchone()
            if opt:
                put_call_ratio, max_pain, iv_atm = opt[0], opt[1], opt[2]
        except Exception as exc:
            conn.rollback()
            _log_query_failure(f"Quote options query for {ticker_upper}", exc)

    # Live fallback only when nothing is stored (kept off the hot path).
    if price is None:
        try:
            live = _fetch_live_price(ticker_upper)
            if live:
                price = live.get("price")
                change_pct = live.get("pct_1d")
                source = "live"
                if price is not None:
                    as_of_date = date.today()
        except Exception as exc:
            # Not a query: an outbound HTTP fetch. Always operational.
            log.warning(
                "Quote: live price fetch failed for {t}: {e}",
                t=ticker_upper, e=str(exc),
            )

    # Rule-based sentiment from options positioning (same logic as /overview).
    score = 0
    if put_call_ratio is not None:
        if put_call_ratio < 0.7:
            score += 1
        elif put_call_ratio > 1.3:
            score -= 1
    if iv_atm is not None and iv_atm > 0.4:
        score -= 1
    sentiment = "bullish" if score > 0 else "bearish" if score < 0 else "neutral"

    stale = (date.today() - as_of_date).days > 3 if as_of_date is not None else None

    return {
        "ticker": ticker_upper,
        "price": price,
        "change_pct": change_pct,
        "put_call_ratio": put_call_ratio,
        "max_pain": max_pain,
        "iv_atm": iv_atm,
        "sentiment": sentiment,
        "source": source,
        "as_of": str(as_of_date) if as_of_date is not None else None,
        "stale": stale,
    }


# ══════════════════════════════════════════════════════════════════
# Insider Edge — aggregated intelligence for a single ticker
# ══════════════════════════════════════════════════════════════════

@router.get("/{ticker}/edge")
def get_ticker_edge(
    ticker: str,
    user: dict = Depends(require_auth),
    engine=Depends(get_db_engine),
):
    """Return all intelligence signals for a ticker.

    Aggregates congressional trades, insider filings, dark pool,
    whale flow, prediction markets, smart money, lever pullers,
    investigation leads, and convergence into one response.
    """
    ticker_upper = ticker.upper().strip()

    congressional: list[dict] = []
    insider: list[dict] = []
    dark_pool: dict | None = None
    whale_flow: list[dict] = []
    prediction_markets: list[dict] = []
    smart_money: list[dict] = []
    availability: dict = {}

    # This endpoint is a read path.  The trust/lever/actor helpers initialize
    # tables (and the lever helper recomputes and persists profiles), so do not
    # call them from an authenticated GET.  These selects deliberately mirror
    # the trust scorer's windows and convergence calculation using persisted
    # signal_sources rows only.
    try:
        with engine.connect() as conn:
            core_rows = conn.execute(text("""
                SELECT source_type, source_id, signal_type, signal_date,
                       trust_score, metadata
                FROM signal_sources
                WHERE ticker = :t
                  AND source_type IN ('congressional', 'insider', 'darkpool')
                  AND signal_date >= NOW() - INTERVAL '45 days'
                ORDER BY signal_date DESC
            """), {"t": ticker_upper}).fetchall()
            for source_type, source_id, signal_type, signal_date, trust_score, metadata in core_rows:
                meta = _edge_metadata(metadata)
                signal = str(signal_type) if signal_type else "UNAVAILABLE"
                if source_type == "congressional" and _edge_within_days(signal_date, 45):
                    congressional.append({
                        "member": str(source_id), "action": signal,
                        "amount": meta.get("amount", "N/A"), "date": str(signal_date),
                        "committee": meta.get("committee", "N/A"),
                        "trust_score": _round_or_none(trust_score),
                    })
                elif source_type == "insider" and _edge_within_days(signal_date, 30):
                    insider.append({
                        "name": str(source_id), "title": meta.get("title", ""),
                        "action": signal, "shares": meta.get("shares"),
                        "value": meta.get("value"), "date": str(signal_date),
                        "cluster": meta.get("cluster", False),
                    })
                elif source_type == "darkpool" and _edge_within_days(signal_date, 7) and dark_pool is None:
                    dark_pool = {
                        "volume_vs_avg": meta.get("volume_vs_avg"),
                        "signal": "accumulation" if signal == "BUY" else "distribution" if signal == "SELL" else "unavailable",
                        "date": str(signal_date),
                    }

            lookback = date.today() - timedelta(days=14)
            whale_rows = conn.execute(text("""
                SELECT source_id, signal_type, signal_date, metadata
                FROM signal_sources
                WHERE ticker = :t AND source_type = 'scanner'
                  AND signal_date >= :lb
                ORDER BY signal_date DESC LIMIT 10
            """), {"t": ticker_upper, "lb": lookback}).fetchall()
            for r in whale_rows:
                meta = r[3] or {}
                if isinstance(meta, str):
                    try:
                            meta = json.loads(meta)
                    except Exception:
                        meta = {}
                whale_flow.append({
                    "strike": meta.get("strike"),
                    "expiry": meta.get("expiry", ""),
                    "direction": str(r[1]),
                    "premium": meta.get("premium"),
                    "date": str(r[2]),
                })
            social_rows = conn.execute(text("""
                SELECT source_id, signal_type, signal_date, trust_score, metadata
                FROM signal_sources
                WHERE ticker = :t AND source_type = 'social'
                  AND signal_date >= :lb
                ORDER BY signal_date DESC LIMIT 10
            """), {"t": ticker_upper, "lb": lookback}).fetchall()
            for r in social_rows:
                meta = r[4] or {}
                if isinstance(meta, str):
                    try:
                            meta = json.loads(meta)
                    except Exception:
                        meta = {}
                smart_money.append({
                    "source": meta.get("platform", "unknown"),
                    "user": str(r[0]),
                    "direction": str(r[1]),
                    "trust_score": _round_or_none(r[3]),
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
                            meta = json.loads(meta)
                    except Exception:
                        meta = {}
                prediction_markets.append({
                    "market": meta.get("market", str(r[0])),
                    "probability": meta.get("probability"),
                    "change_24h": meta.get("change_24h"),
                })
            convergence_rows = conn.execute(text("""
                SELECT source_type, source_id, signal_type, signal_date, trust_score
                FROM signal_sources
                WHERE ticker = :t
                  AND signal_date >= :lookback
                  AND outcome IN ('PENDING', 'CORRECT')
                ORDER BY signal_date DESC
            """), {"t": ticker_upper, "lookback": lookback}).fetchall()
    except Exception as exc:
        log.warning("Edge: signal_sources read unavailable for {t}: {e}", t=ticker_upper, e=str(exc))
        return _edge_unavailable_response(ticker_upper)

    availability["signal_sources"] = {"status": "available"}

    # Persisted profiles only: do not invoke helpers that rebuild or seed them.
    lever_rows = _edge_optional_rows(engine, """
        SELECT DISTINCT ON (lp.id) lp.name, s.signal_type, lp.motivation_model
        FROM lever_pullers lp
        JOIN signal_sources s ON s.source_type = lp.source_type
          AND lp.source_id = CASE
              WHEN s.source_type = 'options_flow' THEN regexp_replace(s.source_id, '_[0-9.]+$', '')
              WHEN s.source_type = 'quiverquant:house' THEN COALESCE(s.signal_value->>'Representative', s.source_id)
              WHEN s.source_type = 'quiverquant:senate' THEN COALESCE(s.signal_value->>'Senator', s.source_id)
              WHEN s.source_type = 'quiverquant:insider' THEN COALESCE(s.signal_value->>'Name', s.source_id)
              WHEN s.source_type = 'quiverquant:lobbying' THEN COALESCE(s.signal_value->>'Registrant', s.signal_value->>'Client', s.source_id)
              ELSE s.source_id END
        WHERE s.ticker = :t
        ORDER BY lp.id, s.signal_date DESC
        LIMIT 20
    """, {"t": ticker_upper}, "lever_pullers", availability)
    lever_pullers = [
        {"name": str(row[0]), "action": str(row[1]) if row[1] else None,
         "context": row[2] if row[2] else None}
        for row in lever_rows
    ]

    actor_rows = _edge_optional_rows(engine, """
        SELECT DISTINCT ON (a.id) a.name, a.title, a.motivation_model
        FROM actors a
        LEFT JOIN signal_sources s ON lower(s.source_id) LIKE '%' || lower(a.name) || '%' AND s.ticker = :t
        WHERE s.source_id IS NOT NULL
           OR a.known_positions @> CAST(:position AS JSONB)
        ORDER BY a.id, s.signal_date DESC NULLS LAST
        LIMIT 20
    """, {"t": ticker_upper, "position": json.dumps([{"ticker": ticker_upper}])}, "actor_context", availability)
    known_levers = {entry["name"].lower() for entry in lever_pullers}
    for name, title, motivation in actor_rows:
        if str(name).lower() not in known_levers:
            lever_pullers.append({
                "name": str(name), "action": "WATCHING",
                "context": " — ".join(str(value) for value in (title, motivation) if value) or None,
            })

    # 5. Investigation leads
    leads: list[dict] = []
    # Sleuth's persisted DDL has no ticker association; do not invent one.
    availability["investigation_leads"] = {"status": "unsupported", "reason": "no_ticker_association"}

    # 6. Convergence detection, equivalent to trust_scorer.detect_convergence
    # without its schema initializer.
    convergence: dict = {"direction": None, "signal_type": None, "source_count": 0,
                         "scored_source_count": 0, "confidence": None,
                         "confidence_basis": "unscored", "direction_basis": None,
                         "status": "none"}
    by_direction: dict[str, dict[str, float | None]] = {"BUY": {}, "SELL": {}}
    for source_type, _source_id, signal_type, _signal_date, trust_score in convergence_rows:
        if signal_type in by_direction and source_type not in by_direction[signal_type]:
            # A source can establish structural convergence before it is scored.
            # NULL trust contributes no numeric confidence; measured zero does.
            by_direction[signal_type][source_type] = float(trust_score) if trust_score is not None else None
    detected = next(
        ((signal_type, scores) for signal_type, scores in by_direction.items() if len(scores) >= 3),
        None,
    )
    if detected:
        signal_type, scores = detected
        scored = [score for score in scores.values() if score is not None]
        convergence = {
            "direction": "bullish" if signal_type == "BUY" else "bearish",
            "direction_basis": "inferred_from_signal_types",
            "signal_type": signal_type,
            "source_count": len(scores),
            "scored_source_count": len(scored),
            "confidence": _round_or_none(sum(scored) / len(scored)) if scored else None,
            "confidence_basis": "mean_trust_of_scored_sources" if scored else "unscored",
            "status": "detected",
        }

    # 7. Build edge_summary (rule-based)
    source_count = convergence.get("source_count") or 0
    direction = convergence.get("direction") or convergence.get("signal_type")
    parts: list[str] = []
    if source_count >= 3:
        parts.append(f"{source_count} independent sources {direction}." if direction else f"{source_count} independent sources, direction unresolved.")
    elif source_count > 0:
        parts.append(f"{source_count} source(s) leaning {direction}." if direction else f"{source_count} source(s), direction unresolved.")
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
        "status": "partial",
        "availability": availability,
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
