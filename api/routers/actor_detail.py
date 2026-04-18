"""Actor detail endpoint for SectorDive profile drawer.

Returns a rich per-node profile combining sector_map metadata,
institutional holdings, insider/congressional flow, price, and
any static activist/regulator/lineage context from flows.py.

The incoming ``actor_id`` may be:
    - a ticker (e.g. "WMT", "NVDA"),
    - a slug for a concept/person/regulator (e.g. "nelson_peltz", "fda"),
    - a synthetic commodity id (e.g. "commodity_cocoa").

All DB queries are parameterized and every branch degrades gracefully
so one missing table does not kill the response.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/actors", tags=["actors"])


# ── Helpers ──────────────────────────────────────────────────────────


def _slug(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=table_name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _lookup_sector_actor(actor_id: str) -> tuple[dict | None, str | None, str | None]:
    """Walk the sector_map looking for an actor matching this id.

    Returns (actor_dict, sector_name, subsector_name) or (None, None, None).
    Matching is ticker-exact OR slug-exact against the actor name.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception:
        return None, None, None

    aid = actor_id.strip()
    aid_upper = aid.upper()
    aid_slug = _slug(aid)

    for sector_name, sector in SECTOR_MAP.items():
        subs = sector.get("subsectors", {}) if isinstance(sector, dict) else {}
        for sub_name, sub in subs.items():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors", []) or []:
                tk = (actor.get("ticker") or "").upper()
                if tk and tk == aid_upper:
                    return actor, sector_name, sub_name
                if _slug(actor.get("name", "")) == aid_slug:
                    return actor, sector_name, sub_name
    return None, None, None


def _lookup_static_context(actor_id: str) -> dict[str, Any]:
    """Check flows.py hardcoded activist / regulator / commodity maps.

    Falls back to empty dict if flows.py doesn't yet export those maps
    (older deployments). The drawer will then rely on sector_map + db
    signals for enrichment.
    """
    try:
        from api.routers import flows as _flows
        _ACTIVIST_HOLDERS = getattr(_flows, "_ACTIVIST_HOLDERS", {}) or {}
        _REGULATOR_THREATS = getattr(_flows, "_REGULATOR_THREATS", {}) or {}
        _GLP1_PRESSURE = getattr(_flows, "_GLP1_PRESSURE", {}) or {}
        _SUPPLY_CHAIN = getattr(_flows, "_SUPPLY_CHAIN", {}) or {}
    except Exception:
        return {}

    ctx: dict[str, Any] = {}
    if actor_id in _ACTIVIST_HOLDERS:
        info = _ACTIVIST_HOLDERS[actor_id]
        ctx["activist"] = {
            "label": info["label"],
            "type": info["type"],
            "kind": info["kind"],
            "targets": info["targets"],
            "evidence": info["evidence"],
        }
    if actor_id in _REGULATOR_THREATS:
        info = _REGULATOR_THREATS[actor_id]
        ctx["regulator"] = {
            "label": info["label"],
            "targets": info["targets"],
            "evidence": info["evidence"],
        }
    if actor_id in _GLP1_PRESSURE:
        info = _GLP1_PRESSURE[actor_id]
        ctx["demand_destruction"] = {
            "label": info["label"],
            "targets": info["targets"],
            "evidence": info["evidence"],
        }
    if actor_id.startswith("commodity_"):
        cname = actor_id.replace("commodity_", "")
        consumers = [tk for tk, inputs in _SUPPLY_CHAIN.items() if cname in inputs]
        ctx["commodity"] = {"name": cname, "consumers": consumers}
    return ctx


def _ticker_price(engine: Any, ticker: str) -> dict[str, Any]:
    """Return latest price, 1d / 30d change and market cap if available."""
    today = date.today()
    d30 = today - timedelta(days=30)
    d1 = today - timedelta(days=2)

    sid = f"YF:{ticker}:close"
    params = {"sid": sid, "d30": d30, "d1": d1}
    out = {
        "price": None,
        "change_1d": None,
        "change_30d": None,
        "market_cap": None,
    }
    try:
        with engine.connect() as conn:
            latest = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "AND value > 0 AND value < 500000 "
                "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1"
            ), params).fetchone()
            prev_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "AND value > 0 AND value < 500000 AND obs_date <= :d30 "
                "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1"
            ), params).fetchone()
            prev_1 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "AND value > 0 AND value < 500000 AND obs_date <= :d1 "
                "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1"
            ), params).fetchone()
            if latest:
                out["price"] = float(latest[0])
            if latest and prev_30 and float(prev_30[0]) != 0:
                out["change_30d"] = round(
                    (float(latest[0]) - float(prev_30[0])) / float(prev_30[0]), 5
                )
            if latest and prev_1 and float(prev_1[0]) != 0:
                out["change_1d"] = round(
                    (float(latest[0]) - float(prev_1[0])) / float(prev_1[0]), 5
                )

            # Market cap: prefer ticker_metrics_daily (daily XBRL+close),
            # fall back to ticker_metadata if present.
            if _table_exists(conn, "ticker_metrics_daily"):
                row = conn.execute(text(
                    "SELECT market_cap_usd FROM ticker_metrics_daily "
                    "WHERE ticker = :t AND market_cap_usd IS NOT NULL "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"t": ticker}).fetchone()
                if row and row[0] is not None:
                    out["market_cap"] = float(row[0])
            if out["market_cap"] is None and _table_exists(conn, "ticker_metadata"):
                row = conn.execute(text(
                    "SELECT market_cap FROM ticker_metadata WHERE ticker = :t LIMIT 1"
                ), {"t": ticker}).fetchone()
                if row and row[0] is not None:
                    out["market_cap"] = float(row[0])
    except Exception as exc:
        log.debug("actor_detail: price lookup failed for {t}: {e}", t=ticker, e=str(exc))
    return out


def _ticker_signals(engine: Any, ticker: str) -> dict[str, Any]:
    """Gather insider, congress, options, and dark-pool snapshots for a ticker."""
    today = date.today()
    d30 = today - timedelta(days=30)
    d60 = today - timedelta(days=60)

    sigs: dict[str, Any] = {
        "insider_trades_30d": [],
        "congressional_trades_60d": [],
        "options_signal": None,
        "dark_pool_signal": None,
        "dealer_gamma": None,
        "sec_filings_90d": [],
        "chokepoint_crossings_30d": [],
        "trust_score": None,
    }
    try:
        with engine.connect() as conn:
            if _table_exists(conn, "insider_trades"):
                rows = conn.execute(text(
                    "SELECT trade_date, insider_name, trade_type, shares, value "
                    "FROM insider_trades WHERE ticker = :t AND trade_date >= :d30 "
                    "ORDER BY trade_date DESC LIMIT 20"
                ), {"t": ticker, "d30": d30}).fetchall()
                sigs["insider_trades_30d"] = [
                    {
                        "date": str(r[0]) if r[0] else None,
                        "name": r[1],
                        "type": r[2],
                        "shares": int(r[3]) if r[3] is not None else None,
                        "value": float(r[4]) if r[4] is not None else None,
                    }
                    for r in rows
                ]
            if _table_exists(conn, "congressional_trades"):
                rows = conn.execute(text(
                    "SELECT disclosure_date, representative, trade_type, amount "
                    "FROM congressional_trades WHERE ticker = :t "
                    "AND disclosure_date >= :d60 "
                    "ORDER BY disclosure_date DESC LIMIT 20"
                ), {"t": ticker, "d60": d60}).fetchall()
                sigs["congressional_trades_60d"] = [
                    {
                        "date": str(r[0]) if r[0] else None,
                        "representative": r[1],
                        "type": r[2],
                        "amount": r[3],
                    }
                    for r in rows
                ]
            if _table_exists(conn, "options_daily_signals"):
                row = conn.execute(text(
                    "SELECT put_call_ratio, iv_atm FROM options_daily_signals "
                    "WHERE ticker = :t "
                    "ORDER BY signal_date DESC LIMIT 1"
                ), {"t": ticker}).fetchone()
                if row:
                    pcr = row[0]
                    if pcr is None:
                        sigs["options_signal"] = None
                    elif pcr > 1.2:
                        sigs["options_signal"] = "bearish"
                    elif pcr < 0.7:
                        sigs["options_signal"] = "bullish"
                    else:
                        sigs["options_signal"] = "balanced"
            if _table_exists(conn, "dark_pool_weekly"):
                row = conn.execute(text(
                    "SELECT short_volume_ratio FROM dark_pool_weekly "
                    "WHERE ticker = :t ORDER BY week_ending DESC LIMIT 1"
                ), {"t": ticker}).fetchone()
                if row and row[0] is not None:
                    r = float(row[0])
                    sigs["dark_pool_signal"] = (
                        "accumulation" if r < 0.45
                        else "distribution" if r > 0.55
                        else "neutral"
                    )
    except Exception as exc:
        log.debug("actor_detail: signals query failed for {t}: {e}", t=ticker, e=str(exc))

    # SEC filings + chokepoint crossings via the trust scorer.
    try:
        from intelligence.trust_scorer import TrustScorer

        ts = TrustScorer(engine)
        sigs["sec_filings_90d"] = [
            {
                "date": s.get("signal_date"),
                "form": (s.get("metadata") or {}).get("source_filing"),
                "flow_type": (s.get("metadata") or {}).get("flow_type"),
                "amount_usd": (s.get("metadata") or {}).get("amount_usd"),
                "trust_delta": s.get("trust_delta"),
                "confidence": s.get("confidence"),
            }
            for s in ts._score_sec_filing(ticker)
        ][:20]
        sigs["chokepoint_crossings_30d"] = [
            {
                "date": s.get("signal_date"),
                "upstream": (s.get("metadata") or {}).get("upstream_id"),
                "input_type": (s.get("metadata") or {}).get("input_type"),
                "chokepoint_score": (s.get("metadata") or {}).get("chokepoint_score"),
                "trust_delta": s.get("trust_delta"),
                "confidence": s.get("confidence"),
            }
            for s in ts._score_chokepoint_crossing(ticker)
        ][:20]
        sigs["trust_score"] = ts.get_trust_score(ticker)
    except Exception as exc:
        log.debug("actor_detail: trust scorer enrichment failed for {t}: {e}",
                  t=ticker, e=str(exc))
    return sigs


def _top_holders(engine: Any, ticker: str, limit: int = 10) -> list[dict[str, Any]]:
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "institutional_holdings"):
                return []
            rows = conn.execute(text(
                "SELECT holder_name, shares_held, value_usd, report_date "
                "FROM institutional_holdings WHERE ticker = :t "
                "AND report_date = (SELECT MAX(report_date) FROM institutional_holdings "
                "                   WHERE ticker = :t) "
                "ORDER BY shares_held DESC NULLS LAST LIMIT :lim"
            ), {"t": ticker, "lim": limit}).fetchall()
            return [
                {
                    "filer": r[0],
                    "shares": int(r[1]) if r[1] is not None else None,
                    "value_usd": float(r[2]) if r[2] is not None else None,
                    "report_date": str(r[3]) if r[3] else None,
                }
                for r in rows
            ]
    except Exception as exc:
        log.debug("actor_detail: holders failed for {t}: {e}", t=ticker, e=str(exc))
    return []


def _connections_for(actor_id: str, sector_name: str | None) -> list[dict[str, Any]]:
    """Reuse the cached sector connection graph to pull neighbors of this node."""
    if not sector_name:
        return []
    try:
        from api.routers.flows import _build_sector_connections
        from analysis.sector_map import SECTOR_MAP

        sector = SECTOR_MAP.get(sector_name, {}) or {}
        subs_meta = sector.get("subsectors", {}) or {}
        # Cheap skeleton subsectors — _build_sector_connections just needs actor lists.
        subs = {
            name: {"weight": sub.get("weight", 1.0),
                   "actors": sub.get("actors", [])}
            for name, sub in subs_meta.items()
            if isinstance(sub, dict)
        }
        tickers: list[str] = []
        for sub in subs.values():
            for a in sub["actors"]:
                if a.get("ticker"):
                    tickers.append(a["ticker"])

        engine = get_db_engine()
        payload = _build_sector_connections(engine, sector_name, tickers, subs)
        node_label = {n["id"]: n.get("label", n["id"]) for n in payload.get("nodes", [])}
        out = []
        for e in payload.get("edges", []) or []:
            if e.get("source") == actor_id or e.get("target") == actor_id:
                other = e["target"] if e["source"] == actor_id else e["source"]
                out.append({
                    "target": other,
                    "target_label": node_label.get(other, other),
                    "type": e.get("type"),
                    "strength": e.get("strength"),
                    "evidence": e.get("evidence"),
                    "confidence": e.get("confidence"),
                })
        out.sort(key=lambda r: r.get("strength") or 0, reverse=True)
        return out
    except Exception as exc:
        log.debug("actor_detail: connections build failed for {a}: {e}",
                  a=actor_id, e=str(exc))
    return []


# ── Endpoint ────────────────────────────────────────────────────────


@router.get("/{actor_id}/trust-cog")
async def get_actor_trust_cog_endpoint(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """INTEL-2 — return the trust-vs-cog classification for one actor.

    Score is in [-1, +1] where +1 = pure trust signal source, -1 = pure cog.
    Returns the per-component breakdown plus the inputs that fed the score
    so the user can audit *why* an actor is classified as they are.

    Falls back to a 404-shaped payload if the actor is not in lever_pullers.
    """
    engine = get_db_engine()
    try:
        from intelligence.actor_trust_cog import get_actor_trust_cog
        result = get_actor_trust_cog(engine, actor_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("actor_trust_cog lookup failed for {a}: {e}", a=actor_id, e=str(exc))
        return {"actor_id": actor_id, "error": "lookup_failed", "detail": str(exc)}

    if result is None:
        return {
            "actor_id": actor_id,
            "found": False,
            "note": "actor not present in lever_pullers; run intelligence.actor_trust_cog.score_all_actors first",
        }
    return {"actor_id": actor_id, "found": True, **result}


@router.get("/{actor_id}/detail")
async def get_actor_detail_for_drawer(
    actor_id: str,
    sector: str | None = Query(None, description="Sector context for edges"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Rich detail payload for the SectorDive actor profile drawer.

    Accepts tickers, slugs, synthetic ids (``commodity_*``), or canvas
    graph node ids (``a:corp_KO``).
    """
    # Strip canvas graph node-id prefixes so "a:corp_KO" → "KO".
    for _pfx in ("a:corp_", "a:ticker_", "a:person_", "a:govt_", "a:org_", "a:fund_", "a:"):
        if actor_id.startswith(_pfx):
            actor_id = actor_id[len(_pfx):]
            break
    if not actor_id:
        return {"error": "actor_id required", "id": actor_id}

    engine = get_db_engine()
    actor, sector_from_map, subsector = _lookup_sector_actor(actor_id)
    ctx = _lookup_static_context(actor_id)
    sector_name = sector or sector_from_map

    # Normalize shape
    result: dict[str, Any] = {
        "id": actor_id,
        "label": None,
        "type": None,
        "description": None,
        "sector": sector_name,
        "subsector": subsector,
        "weight": None,
        "price": None,
        "change_1d": None,
        "change_30d": None,
        "market_cap": None,
        "signals": None,
        "holders_top10": [],
        "connections": _connections_for(actor_id, sector_name),
        "static_context": ctx,
        "recent_events": [],
        "affiliated_firms": [],
        "known_holdings": [],
        "pre_positioned_by_filers": [],
    }

    if actor is not None:
        result["label"] = actor.get("name")
        result["type"] = actor.get("type")
        result["description"] = actor.get("description")
        result["weight"] = actor.get("weight")
        ticker = actor.get("ticker")
    elif ctx.get("activist"):
        a = ctx["activist"]
        result["label"] = a["label"]
        result["type"] = a.get("type", "family_office")
        result["description"] = a.get("evidence")
        result["known_holdings"] = a.get("targets", [])
        ticker = None
    elif ctx.get("regulator"):
        r = ctx["regulator"]
        result["label"] = r["label"]
        result["type"] = "regulator"
        result["description"] = r.get("evidence")
        result["known_holdings"] = r.get("targets", [])
        ticker = None
    elif ctx.get("demand_destruction"):
        d = ctx["demand_destruction"]
        result["label"] = d["label"]
        result["type"] = "company"
        result["description"] = d.get("evidence")
        result["known_holdings"] = d.get("targets", [])
        ticker = actor_id.upper() if actor_id.isalpha() else None
    elif ctx.get("commodity"):
        c = ctx["commodity"]
        result["label"] = c["name"].replace("_", " ").title()
        result["type"] = "commodity"
        result["description"] = f"Primary input cost for {', '.join(c['consumers']) or 'none'}"
        result["known_holdings"] = c["consumers"]
        ticker = None
    else:
        # Unknown node — return a stub so the drawer can still render.
        result["label"] = actor_id.replace("_", " ").title()
        result["type"] = "unknown"
        ticker = actor_id.upper() if actor_id.isalpha() and 1 < len(actor_id) <= 6 else None

    # Ticker-specific enrichment.
    if ticker:
        result["id"] = ticker
        result["type"] = result["type"] or "company"
        price = _ticker_price(engine, ticker)
        result.update(price)
        result["signals"] = _ticker_signals(engine, ticker)
        result["holders_top10"] = _top_holders(engine, ticker, limit=10)

        # Pre-positioning cross-reference: filers that held both legs
        # of an M&A deal this ticker participated in before it was
        # announced. Only populated when holder_deal_overlap has rows
        # touching this ticker (degrades silently if the table or
        # intelligence module is missing on an older deployment).
        try:
            from intelligence.holder_deal_overlap import (
                fetch_overlaps_for_actor,
            )
            result["pre_positioned_by_filers"] = fetch_overlaps_for_actor(
                engine, ticker, limit=20
            )
        except Exception as exc:
            log.debug(
                "actor_detail: pre_positioned lookup failed for {t}: {e}",
                t=ticker, e=str(exc),
            )

    return result
