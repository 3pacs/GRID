"""Universal search endpoint — searches across all GRID registries."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/search", tags=["search"])

# ── Static view registry for navigable views ──────────────────────
_VIEWS = [
    {"name": "Dashboard",        "desc": "Main dashboard overview",           "action": "dashboard"},
    {"name": "Regime",           "desc": "Current market regime state",       "action": "regime"},
    {"name": "Signals",          "desc": "Live feature values",              "action": "signals"},
    {"name": "Journal",          "desc": "Decision log & outcomes",          "action": "journal"},
    {"name": "Models",           "desc": "Model registry & governance",      "action": "models"},
    {"name": "Discovery",        "desc": "Hypotheses & clustering",          "action": "discovery"},
    {"name": "Associations",     "desc": "Feature correlations & anomalies", "action": "associations"},
    {"name": "Backtest",         "desc": "Track record & paper trades",      "action": "backtest"},
    {"name": "Physics",          "desc": "Market dynamics verification",     "action": "physics"},
    {"name": "Options",          "desc": "Options flow & Greeks",            "action": "options"},
    {"name": "Heatmap",          "desc": "Sector & asset heatmap",           "action": "heatmap"},
    {"name": "Flows",            "desc": "Sector flows & influence",         "action": "flows"},
    {"name": "Money Flow",       "desc": "Global money flow visualization",  "action": "money-flow"},
    {"name": "Predictions",      "desc": "Oracle predictions & scoreboard",  "action": "predictions"},
    {"name": "Portfolio",        "desc": "Position analytics & allocation",  "action": "portfolio"},
    {"name": "Strategy",         "desc": "Regime-linked action plans",       "action": "strategy"},
    {"name": "Cross Reference",  "desc": "Government stats vs reality",      "action": "cross-reference"},
    {"name": "Actor Network",    "desc": "Financial power structure",        "action": "actor-network"},
    {"name": "Globe",            "desc": "Geographic intelligence view",     "action": "globe"},
    {"name": "Risk",             "desc": "Risk map & exposure",              "action": "risk"},
    {"name": "Thesis",           "desc": "Unified market thesis",            "action": "thesis"},
    {"name": "Trends",           "desc": "Momentum & rotation trends",       "action": "trends"},
    {"name": "Intelligence",     "desc": "Intelligence command center",      "action": "intelligence"},
    {"name": "Agents",           "desc": "Multi-agent deliberation",         "action": "agents"},
    {"name": "Briefings",        "desc": "AI market analysis reports",       "action": "briefings"},
    {"name": "Workflows",        "desc": "Data & compute pipelines",         "action": "workflows"},
    {"name": "Pipeline Health",  "desc": "Data pipeline health & freshness", "action": "pipeline-health"},
    {"name": "System",           "desc": "Logs, config & sources",           "action": "system"},
    {"name": "Settings",         "desc": "Connection & logout",              "action": "settings"},
    {"name": "Weights",          "desc": "Tune regime feature influence",    "action": "weights"},
    {"name": "Hyperspace",       "desc": "Distributed compute node",         "action": "hyperspace"},
    {"name": "Sector Dive",      "desc": "Deep-dive into a sector",         "action": "sector-dive"},
]


def _search_views(q: str) -> list[dict]:
    """Match query against static view names."""
    ql = q.lower()
    results = []
    for v in _VIEWS:
        if ql in v["name"].lower() or ql in v["desc"].lower() or ql in v["action"].lower():
            results.append({
                "type": "view",
                "title": v["name"],
                "subtitle": v["desc"],
                "action": v["action"],
                "param": None,
            })
    return results


def _search_tickers(engine, q: str) -> list[dict]:
    """Search watchlist tickers matching query."""
    results = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT ticker, company_name, sector, industry
                    FROM watchlist
                    WHERE LOWER(ticker) LIKE :q
                       OR LOWER(company_name) LIKE :q
                    ORDER BY ticker
                    LIMIT 10
                """),
                {"q": f"%{q.lower()}%"},
            ).fetchall()
            for r in rows:
                sector_info = ""
                if r.sector:
                    sector_info = r.sector
                    if r.industry:
                        sector_info += f"/{r.industry}"
                subtitle = r.company_name or r.ticker
                if sector_info:
                    subtitle += f" — {sector_info}"
                results.append({
                    "type": "ticker",
                    "title": r.ticker,
                    "subtitle": subtitle,
                    "action": "watchlist-analysis",
                    "param": r.ticker,
                })
    except Exception as e:
        log.debug(f"Ticker search skipped: {e}")
    return results


def _search_features(engine, q: str) -> list[dict]:
    """Search feature registry for matching feature names."""
    results = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT feature_name, family, source_id
                    FROM feature_registry
                    WHERE LOWER(feature_name) LIKE :q
                    ORDER BY feature_name
                    LIMIT 10
                """),
                {"q": f"%{q.lower()}%"},
            ).fetchall()
            for r in rows:
                subtitle = r.family or "unknown"
                if r.source_id:
                    subtitle += f" — {r.source_id}"
                results.append({
                    "type": "feature",
                    "title": r.feature_name,
                    "subtitle": subtitle,
                    "action": "signals",
                    "param": r.feature_name,
                })
    except Exception as e:
        log.debug(f"Feature search skipped: {e}")
    return results


def _search_hypotheses(engine, q: str) -> list[dict]:
    """Search hypotheses for matching statements."""
    results = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, statement, status, sharpe
                    FROM hypotheses
                    WHERE LOWER(statement) LIKE :q
                    ORDER BY created_at DESC
                    LIMIT 10
                """),
                {"q": f"%{q.lower()}%"},
            ).fetchall()
            for r in rows:
                status_str = (r.status or "unknown").upper()
                subtitle = status_str
                if r.sharpe is not None:
                    subtitle += f" — Sharpe {r.sharpe:.2f}"
                results.append({
                    "type": "hypothesis",
                    "title": r.statement[:80] if r.statement else f"Hypothesis #{r.id}",
                    "subtitle": subtitle,
                    "action": "discovery",
                    "param": str(r.id),
                })
    except Exception as e:
        log.debug(f"Hypothesis search skipped: {e}")
    return results


def _search_actors(engine, q: str) -> list[dict]:
    """Search actor network for matching actor names."""
    results = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT name, role, tier, organization
                    FROM actors
                    WHERE LOWER(name) LIKE :q
                       OR LOWER(organization) LIKE :q
                    ORDER BY name
                    LIMIT 10
                """),
                {"q": f"%{q.lower()}%"},
            ).fetchall()
            for r in rows:
                subtitle = r.role or ""
                if r.organization:
                    subtitle += f" of {r.organization}" if subtitle else r.organization
                if r.tier:
                    subtitle += f" — {r.tier} tier"
                results.append({
                    "type": "actor",
                    "title": r.name,
                    "subtitle": subtitle,
                    "action": "actor-network",
                    "param": r.name,
                })
    except Exception as e:
        log.debug(f"Actor search skipped: {e}")
    return results


def _search_sources(engine, q: str) -> list[dict]:
    """Search signal source catalog."""
    results = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_id, description
                    FROM source_catalog
                    WHERE LOWER(source_id) LIKE :q
                       OR LOWER(COALESCE(description, '')) LIKE :q
                    ORDER BY source_id
                    LIMIT 10
                """),
                {"q": f"%{q.lower()}%"},
            ).fetchall()
            for r in rows:
                results.append({
                    "type": "source",
                    "title": r.source_id,
                    "subtitle": r.description or "Signal source",
                    "action": "system",
                    "param": r.source_id,
                })
    except Exception as e:
        log.debug(f"Source search skipped: {e}")
    return results


@router.get("")
async def search_everything(
    q: str = Query(..., min_length=1, max_length=200, description="Search query"),
    _user: dict = Depends(require_auth),
    engine=Depends(get_db_engine),
) -> dict:
    """Universal search across all GRID registries.

    Searches tickers, features, hypotheses, actors, views, and sources.
    Returns grouped results sorted by relevance.
    """
    log.info(f"Universal search: q={q!r}")

    results = []

    # Views are searched in-memory (no DB)
    results.extend(_search_views(q))

    # DB-backed searches — each is independently resilient
    results.extend(_search_tickers(engine, q))
    results.extend(_search_features(engine, q))
    results.extend(_search_hypotheses(engine, q))
    results.extend(_search_actors(engine, q))
    results.extend(_search_sources(engine, q))

    return {"results": results}
