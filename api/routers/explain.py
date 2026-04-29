"""Hero endpoint: "why did this actor move?" — ranked evidence synthesis.

``GET /api/v1/actors/{actor_id}/explain?date=YYYY-MM-DD&window_days=5``

Given an actor and a pivot date, scans every intelligence lens
(insider trades, congressional trades, dark pool, options, capital
flows announcements, supply-shock attributions, chain contagion
predictions, corporate actions, news if available) within a window
centred on the pivot date, ranks the collected evidence by a
type-weight × recency score, and returns a deterministic narrative
citing the top-3 drivers. No LLM calls.

Every SQL query is parameterized and wrapped in try/except so one
missing table (or one permission error) will never kill the response —
the missing source is simply reported in ``provenance.sources_checked``
vs ``provenance.evidence_rows``.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1/actors", tags=["explain"])

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(identifier: str) -> str:
    """Return a safely quoted SQL identifier from a whitelisted name."""
    if not _IDENT_RE.fullmatch(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return '"' + identifier + '"'


# ── Configuration ────────────────────────────────────────────────────

# Per-type base strengths. These are the ranking prior — recency is
# applied multiplicatively on top. Values live in [0, 1].
TYPE_WEIGHTS: dict[str, float] = {
    "contagion_prediction": 0.85,        # pre-registered smoking gun
    "supply_shock_attribution": 0.80,    # historical lag correlation
    "announcement": 0.70,                # 8-K / earnings
    "corporate_action": 0.65,            # buybacks, splits, M&A
    "insider_trade": 0.55,               # Form 4
    "contagion_backtest": 0.50,          # retro-scored prediction hit
    "congressional_trade": 0.45,         # PTR disclosures
    "options_signal": 0.40,              # PCR / IV skew
    "dark_pool": 0.35,                   # short-vol distribution signal
    "news": 0.30,                        # headlines (if present)
}

_EXPLAIN_CACHE_TTL: float = 300.0  # 5 minutes
_explain_cache: TTLCache = TTLCache(ttl=_EXPLAIN_CACHE_TTL, max_size=256)


# ── Helpers ──────────────────────────────────────────────────────────


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=table_name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _parse_date(value: str | None) -> date:
    """Parse YYYY-MM-DD or return today."""
    if not value:
        return date.today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"Invalid date '{value}', expected YYYY-MM-DD"
        ) from exc


def _recency_weight(evt_date: date | None, pivot: date, window_days: int) -> float:
    """Linearly decay from 1.0 at the pivot to 0.5 at the window edge.

    Events outside the window get a floor of 0.5 so they still count but
    rank below their near-pivot peers.
    """
    if evt_date is None or window_days <= 0:
        return 0.75
    delta = abs((evt_date - pivot).days)
    if delta >= window_days:
        return 0.5
    return 1.0 - 0.5 * (delta / max(1, window_days))


def _resolve_label(engine: Any, actor_id: str) -> tuple[str, str | None, str | None, str | None]:
    """Return (label, type, sector, subsector).

    Best-effort — walks sector_map first (ticker or slug match), then
    ``supply_chain_nodes``. Falls back to a titled version of the id.
    """
    label = actor_id
    atype: str | None = None
    sector: str | None = None
    subsector: str | None = None

    try:
        from api.routers.actor_detail import _lookup_sector_actor
        actor, sec, sub = _lookup_sector_actor(actor_id)
        if actor is not None:
            label = actor.get("name") or label
            atype = actor.get("type") or atype
            sector = sec
            subsector = sub
    except Exception:
        pass

    if atype is None:
        try:
            with engine.connect() as conn:
                if _table_exists(conn, "supply_chain_nodes"):
                    row = conn.execute(
                        text("SELECT name, type FROM supply_chain_nodes WHERE id = :i LIMIT 1"),
                        {"i": actor_id},
                    ).fetchone()
                    if row:
                        label = row[0] or label
                        atype = row[1] or atype
        except Exception as exc:
            log.debug("explain: supply_chain_nodes lookup failed for {a}: {e}",
                      a=actor_id, e=str(exc))

    if atype is None:
        # Heuristic: short all-alpha → ticker, else slug/concept
        if actor_id.isalpha() and 1 < len(actor_id) <= 6:
            atype = "ticker"
        else:
            atype = "unknown"

    if atype == "ticker":
        label = label or actor_id.upper()
    else:
        label = label or actor_id.replace("_", " ").title()

    return label, atype, sector, subsector


def _price_at_or_before(conn: Any, sid: str, cutoff: date) -> float | None:
    try:
        row = conn.execute(
            text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "  AND value > 0 AND value < 500000 AND obs_date <= :d "
                "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1"
            ),
            {"sid": sid, "d": cutoff},
        ).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception as exc:
        log.debug("explain: price_at_or_before failed {sid} {d}: {e}",
                  sid=sid, d=cutoff, e=str(exc))
    return None


def _actual_move(engine: Any, actor_id: str, actor_type: str, pivot: date,
                 window_days: int) -> dict[str, Any]:
    """Compute the move we're trying to explain.

    Window is ``[pivot - window_days, pivot + window_days/2]``. If no
    price exists for this actor (non-ticker or stale), returns a stub.
    """
    start_d = pivot - timedelta(days=window_days)
    end_d = pivot + timedelta(days=max(1, window_days // 2))
    out: dict[str, Any] = {
        "start_date": str(start_d),
        "end_date": str(end_d),
        "start_price": None,
        "end_price": None,
        "pct": None,
    }
    if actor_type not in ("ticker", "company"):
        return out

    ticker = actor_id.upper()
    sid = f"YF:{ticker}:close"
    try:
        with engine.connect() as conn:
            start_px = _price_at_or_before(conn, sid, start_d)
            end_px = _price_at_or_before(conn, sid, end_d)
            if start_px is not None:
                out["start_price"] = round(start_px, 4)
            if end_px is not None:
                out["end_price"] = round(end_px, 4)
            if start_px and end_px and start_px != 0:
                out["pct"] = round((end_px - start_px) / start_px, 5)
    except Exception as exc:
        log.debug("explain: actual_move failed for {t}: {e}", t=ticker, e=str(exc))
    return out


# ── Evidence collectors ──────────────────────────────────────────────
#
# Each collector returns a list[dict] of evidence entries and increments
# the ``sources_checked`` counter in the caller's provenance dict. They
# must never raise — any failure is logged and an empty list returned.


def _collect_insider(conn: Any, ticker: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "insider_trades"):
        return []
    try:
        rows = conn.execute(
            text(
                "SELECT id, trade_date, insider_name, insider_title, "
                "       trade_type, shares, value "
                "FROM insider_trades "
                "WHERE ticker = :t AND trade_date BETWEEN :s AND :e "
                "ORDER BY COALESCE(value, 0) DESC, trade_date DESC "
                "LIMIT 20"
            ),
            {"t": ticker, "s": start, "e": end},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.insider failed for {t}: {e}", t=ticker, e=str(exc))
        return []

    out = []
    for r in rows:
        value = float(r[6]) if r[6] is not None else None
        direction = "sold" if (r[4] or "").upper().startswith("S") else "bought"
        usd = f"${value / 1e6:.1f}M" if value else "unknown size"
        name = r[2] or "Insider"
        title = f" ({r[3]})" if r[3] else ""
        out.append({
            "type": "insider_trade",
            "date": str(r[1]) if r[1] else None,
            "summary": f"{name}{title} {direction} {usd} on {r[1]}",
            "links": {"trade_id": int(r[0]) if r[0] is not None else None},
            "amount_usd": value,
        })
    return out


def _collect_congress(conn: Any, ticker: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "congressional_trades"):
        return []
    try:
        # Widen by ±3 days per spec
        s = start - timedelta(days=3)
        e = end + timedelta(days=3)
        rows = conn.execute(
            text(
                "SELECT id, disclosure_date, transaction_date, representative, "
                "       party, chamber, transaction_type, amount "
                "FROM congressional_trades "
                "WHERE ticker = :t AND disclosure_date BETWEEN :s AND :e "
                "ORDER BY disclosure_date DESC LIMIT 10"
            ),
            {"t": ticker, "s": s, "e": e},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.congress failed for {t}: {e}", t=ticker, e=str(exc))
        return []

    out = []
    for r in rows:
        rep = r[3] or "Representative"
        ttype = (r[6] or "").lower()
        action = "sold" if "sell" in ttype or ttype.startswith("s") else "bought"
        amount = r[7] or "undisclosed"
        out.append({
            "type": "congressional_trade",
            "date": str(r[1]) if r[1] else None,
            "summary": f"{rep} ({r[4] or '-'}) {action} {amount} disclosed {r[1]}",
            "links": {"trade_id": int(r[0]) if r[0] is not None else None},
        })
    return out


def _collect_dark_pool(conn: Any, ticker: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "dark_pool_weekly"):
        return []
    try:
        rows = conn.execute(
            text(
                "SELECT id, report_date, short_volume, total_volume, short_pct "
                "FROM dark_pool_weekly "
                "WHERE ticker = :t AND report_date BETWEEN :s AND :e "
                "ORDER BY report_date DESC LIMIT 5"
            ),
            {"t": ticker, "s": start, "e": end},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.dark_pool failed for {t}: {e}", t=ticker, e=str(exc))
        return []

    out = []
    for r in rows:
        short_pct = r[4]
        total = r[3]
        if short_pct is None and r[2] is not None and total:
            try:
                short_pct = float(r[2]) / float(total)
            except Exception:
                short_pct = None
        if short_pct is None:
            continue
        ratio = float(short_pct)
        if ratio > 0.55:
            signal = "distribution"
        elif ratio < 0.45:
            signal = "accumulation"
        else:
            signal = "neutral"
        out.append({
            "type": "dark_pool",
            "date": str(r[1]) if r[1] else None,
            "summary": (
                f"Dark pool showed {signal} signal week of {r[1]} "
                f"(short_vol/total = {ratio:.2f})"
            ),
            "links": {"dark_pool_id": int(r[0]) if r[0] is not None else None},
            "short_ratio": ratio,
        })
    return out


def _collect_options(conn: Any, ticker: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "options_daily_signals"):
        return []
    try:
        rows = conn.execute(
            text(
                "SELECT signal_date, put_call_ratio, iv_atm, iv_skew "
                "FROM options_daily_signals "
                "WHERE ticker = :t AND signal_date BETWEEN :s AND :e "
                "ORDER BY signal_date DESC LIMIT 5"
            ),
            {"t": ticker, "s": start, "e": end},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.options failed for {t}: {e}", t=ticker, e=str(exc))
        return []

    out = []
    for r in rows:
        pcr = r[1]
        if pcr is None:
            continue
        pcr_f = float(pcr)
        if pcr_f > 1.2:
            tone = "bearish"
        elif pcr_f < 0.7:
            tone = "bullish"
        else:
            tone = "balanced"
        out.append({
            "type": "options_signal",
            "date": str(r[0]) if r[0] else None,
            "summary": (
                f"Options flow {tone} on {r[0]}: PCR={pcr_f:.2f}"
                + (f", IV ATM={float(r[2]):.2f}" if r[2] is not None else "")
            ),
            "links": {},
            "pcr": pcr_f,
        })
    return out


def _collect_announcements(conn: Any, actor_id: str, start: date, end: date) -> list[dict]:
    """Capital flow 'announcement' rows AND corporate_actions."""
    results: list[dict] = []

    if _table_exists(conn, "capital_flows"):
        try:
            rows = conn.execute(
                text(
                    "SELECT id, fiscal_period, flow_type, direction, "
                    "       amount_usd, source_filing, counterparty_id "
                    "FROM capital_flows "
                    "WHERE actor_id = :a "
                    "  AND period_type = 'announcement' "
                    "  AND fiscal_period BETWEEN :s AND :e "
                    "ORDER BY fiscal_period DESC LIMIT 10"
                ),
                {"a": actor_id, "s": start, "e": end},
            ).fetchall()
        except Exception as exc:
            log.debug("explain.capital_flows failed for {a}: {e}",
                      a=actor_id, e=str(exc))
            rows = []
        for r in rows:
            amt = float(r[4]) if r[4] is not None else 0.0
            flow_type = r[2] or "flow"
            filing = r[5] or "announcement"
            cp = f" (counterparty {r[6]})" if r[6] else ""
            results.append({
                "type": "announcement",
                "date": str(r[1]) if r[1] else None,
                "summary": (
                    f"{filing}: {flow_type} {r[3]} ${amt / 1e6:.1f}M "
                    f"on {r[1]}{cp}"
                ),
                "links": {"flow_id": int(r[0]) if r[0] is not None else None},
            })

    if _table_exists(conn, "corporate_actions"):
        try:
            rows = conn.execute(
                text(
                    "SELECT id, announcement_date, action_type, "
                    "       value_usd, description "
                    "FROM corporate_actions "
                    "WHERE ticker = :t "
                    "  AND announcement_date BETWEEN :s AND :e "
                    "ORDER BY announcement_date DESC LIMIT 10"
                ),
                {"t": actor_id.upper(), "s": start, "e": end},
            ).fetchall()
        except Exception as exc:
            log.debug("explain.corp_actions failed for {a}: {e}",
                      a=actor_id, e=str(exc))
            rows = []
        for r in rows:
            amt = float(r[3]) if r[3] is not None else None
            label = f"{r[2] or 'corporate action'}"
            sz = f" ${amt / 1e6:.1f}M" if amt else ""
            desc = f" — {r[4]}" if r[4] else ""
            results.append({
                "type": "corporate_action",
                "date": str(r[1]) if r[1] else None,
                "summary": f"{label}{sz} announced {r[1]}{desc}",
                "links": {"action_id": int(r[0]) if r[0] is not None else None},
            })
    return results


def _collect_supply_shock(conn: Any, actor_id: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "supply_shock_attributions"):
        return []
    try:
        rows = conn.execute(
            text(
                "SELECT id, upstream_id, shock_date, shock_magnitude, "
                "       downstream_move_pct, lag_days, correlation, "
                "       evidence, method "
                "FROM supply_shock_attributions "
                "WHERE downstream_id = :a "
                "  AND shock_date BETWEEN :s AND :e "
                "ORDER BY ABS(COALESCE(correlation, 0)) DESC, shock_date DESC "
                "LIMIT 10"
            ),
            {"a": actor_id, "s": start, "e": end},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.supply_shock failed for {a}: {e}",
                  a=actor_id, e=str(exc))
        return []

    out = []
    for r in rows:
        shock_pct = float(r[3]) if r[3] is not None else None
        corr = float(r[6]) if r[6] is not None else None
        lag = int(r[5]) if r[5] is not None else None
        pct_s = f"{shock_pct * 100:+.1f}%" if shock_pct is not None else "moved"
        corr_s = f"correlation {corr:.2f}" if corr is not None else "no correlation"
        lag_s = f"lag={lag}d" if lag is not None else "lag=?"
        out.append({
            "type": "supply_shock_attribution",
            "date": str(r[2]) if r[2] else None,
            "source": r[1],
            "summary": (
                f"{r[1]} {pct_s} on {r[2]}; historical {lag_s} {corr_s} "
                f"with {actor_id.upper()}"
                + (f" — {r[7]}" if r[7] else "")
            ),
            "links": {
                "upstream": r[1],
                "attribution_id": int(r[0]) if r[0] is not None else None,
            },
            "correlation": corr,
        })
    return out


def _collect_contagion(conn: Any, actor_id: str, start: date, end: date) -> list[dict]:
    if not _table_exists(conn, "contagion_predictions"):
        return []
    try:
        # Postgres JSONB containment: ranked_impact has shape
        # {"tickers": [{"ticker": "AAPL", ...}], ...}. We use ::text ILIKE
        # as a cheap portable filter; correctness is double-checked below.
        rows = conn.execute(
            text(
                "SELECT id, shock_node, shock_type, magnitude, simulated_at, "
                "       summary, ranked_impact "
                "FROM contagion_predictions "
                "WHERE simulated_at::date BETWEEN :s AND :e "
                "  AND ranked_impact::text ILIKE :pat "
                "ORDER BY simulated_at DESC LIMIT 10"
            ),
            {"s": start, "e": end, "pat": f"%{actor_id}%"},
        ).fetchall()
    except Exception as exc:
        log.debug("explain.contagion failed for {a}: {e}",
                  a=actor_id, e=str(exc))
        return []

    out = []
    uid = actor_id.upper()
    lid = actor_id.lower()
    for r in rows:
        ri = r[6] or {}
        match = None
        if isinstance(ri, dict):
            for key in ("tickers", "ranked_tickers", "downstream", "impacts"):
                for impact in ri.get(key, []) or []:
                    if not isinstance(impact, dict):
                        continue
                    tk = (impact.get("ticker") or impact.get("id") or "")
                    if tk.upper() == uid or tk.lower() == lid:
                        match = impact
                        break
                if match:
                    break
        magnitude = float(r[3]) if r[3] is not None else None
        mag_pct = f"{magnitude * 100:+.0f}%" if magnitude is not None else "shock"
        margin_part = ""
        if match:
            m = match.get("margin_impact_pct") or match.get("impact_pct")
            if m is not None:
                try:
                    margin_part = f" → {actor_id.upper()} {float(m) * 100:+.1f}% margin"
                except Exception:
                    margin_part = ""
        sim_date = r[4].date() if hasattr(r[4], "date") else r[4]
        out.append({
            "type": "contagion_prediction",
            "date": str(sim_date) if sim_date else None,
            "summary": (
                f"Chain contagion prediction #{r[0]}: {r[1]} {mag_pct}"
                f"{margin_part} (pre-registered {sim_date})"
            ),
            "links": {"prediction_id": int(r[0]) if r[0] is not None else None},
        })
    return out


def _collect_news(conn: Any, ticker: str, start: date, end: date) -> list[dict]:
    """Best-effort news lookup — graceful if table absent.

    Handles both scalar ``ticker`` columns and ARRAY-typed ``tickers`` columns
    via the ``= ANY`` operator. Title falls back to LLM summary / summary.
    """
    candidates = ["news_articles", "news", "news_events"]
    for tbl in candidates:
        if not _table_exists(conn, tbl):
            continue
        try:
            cols_row = conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = :t"
                ),
                {"t": tbl},
            ).fetchall()
            col_type = {c[0]: (c[1] or "").lower() for c in cols_row}
            cols = set(col_type.keys())
            date_col = next(
                (c for c in ("published_at", "event_date", "article_date",
                             "date", "created_at") if c in cols),
                None,
            )
            title_col = next(
                (c for c in ("title", "headline", "llm_summary", "summary", "text")
                 if c in cols),
                None,
            )
            ticker_col = next(
                (c for c in ("ticker", "symbol", "primary_ticker", "tickers")
                 if c in cols),
                None,
            )
            if not (date_col and title_col and ticker_col):
                return []

            is_array = "array" in col_type.get(ticker_col, "")
            # Match both upper + lower case tickers
            params = {
                "t": ticker,
                "tl": ticker.lower(),
                "tu": ticker.upper(),
                "s": start,
                "e": end,
            }
            if is_array:
                where = (
                    "(" + _quote_ident(ticker_col)
                    + " && ARRAY[:t, :tl, :tu]::text[]) "
                    "AND " + _quote_ident(date_col)
                    + "::date BETWEEN :s AND :e"
                )
            else:
                where = (
                    "UPPER(" + _quote_ident(ticker_col) + ") = :tu "
                    "AND " + _quote_ident(date_col)
                    + "::date BETWEEN :s AND :e"
                )
            rows = conn.execute(
                text(
                    "SELECT " + _quote_ident(date_col) + ", "
                    + _quote_ident(title_col) + " FROM "
                    + _quote_ident(tbl) + " WHERE " + where + " "
                    "ORDER BY " + _quote_ident(date_col) + " DESC LIMIT 5"
                ),
                params,
            ).fetchall()
        except Exception as exc:
            log.debug("explain.news failed ({tbl}): {e}", tbl=tbl, e=str(exc))
            return []

        out = []
        for r in rows:
            raw = r[0]
            dt_str: str | None
            if hasattr(raw, "date"):
                dt_str = str(raw.date())
            else:
                dt_str = str(raw) if raw else None
            headline = (r[1] or "").strip()
            if not headline:
                continue
            out.append({
                "type": "news",
                "date": dt_str,
                "summary": f"News {dt_str}: {headline[:180]}",
                "links": {},
            })
        return out
    return []


# ── Ranking + narrative ──────────────────────────────────────────────


def _score(evidence: dict, pivot: date, window_days: int) -> float:
    base = TYPE_WEIGHTS.get(evidence.get("type", ""), 0.2)
    d = None
    raw = evidence.get("date")
    if raw:
        try:
            d = datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
        except Exception:
            d = None
    recency = _recency_weight(d, pivot, window_days)
    return round(base * recency, 4)


def _narrate(actor_label: str, actual_move: dict, top: list[dict],
             window_days: int) -> str:
    pct = actual_move.get("pct")
    if pct is not None:
        direction = "dropped" if pct < 0 else "gained"
        move_part = f"{actor_label} {direction} {abs(pct) * 100:.2f}% over {window_days} days."
    else:
        move_part = f"{actor_label} activity over the last {window_days} days."

    if not top:
        return (
            f"{move_part} No ranked evidence surfaced across the scanned "
            "intelligence lenses in this window."
        )

    pieces: list[str] = []
    for i, ev in enumerate(top[:3]):
        strength = int(round(ev.get("strength", 0) * 100))
        label = ev.get("type", "signal").replace("_", " ")
        pieces.append(f"{label} ({strength}%)")
    lead = pieces[0] if pieces else "unknown"
    reinforcers = (
        ", reinforced by " + " and ".join(pieces[1:])
        if len(pieces) > 1
        else ""
    )
    return f"{move_part} Most probable driver: {lead}{reinforcers}."


# ── Main builder ─────────────────────────────────────────────────────


def _build_explain(
    engine: Any,
    actor_id: str,
    pivot: date,
    window_days: int,
) -> dict[str, Any]:
    """Core pipeline — engine-in, dict-out. No auth, cache-safe."""
    label, atype, sector, subsector = _resolve_label(engine, actor_id)

    actual_move = _actual_move(engine, actor_id, atype, pivot, window_days)
    start = pivot - timedelta(days=window_days)
    end = pivot + timedelta(days=max(1, window_days // 2))

    # "ticker" covers explicit tickers; "company" covers sector_map entries
    # that were labelled as companies (the common case for SECTOR_MAP).
    is_ticker_like = atype in ("ticker", "company")
    ticker = actor_id.upper()
    sources_checked = 0
    all_evidence: list[dict] = []

    try:
        with engine.connect() as conn:
            # Ticker-keyed lenses
            if is_ticker_like:
                for fn in (_collect_insider, _collect_congress,
                           _collect_dark_pool, _collect_options):
                    sources_checked += 1
                    try:
                        all_evidence.extend(fn(conn, ticker, start, end))
                    except Exception as exc:
                        log.debug("explain collector {n} failed: {e}",
                                  n=fn.__name__, e=str(exc))

                sources_checked += 1
                try:
                    all_evidence.extend(_collect_news(conn, ticker, start, end))
                except Exception as exc:
                    log.debug("explain news failed: {e}", e=str(exc))

            # Actor-id keyed lenses (work for tickers and slugs)
            for fn in (_collect_announcements, _collect_supply_shock,
                       _collect_contagion):
                sources_checked += 1
                try:
                    all_evidence.extend(fn(conn, actor_id, start, end))
                except Exception as exc:
                    log.debug("explain collector {n} failed: {e}",
                              n=fn.__name__, e=str(exc))
    except Exception as exc:
        log.warning("explain: DB connection failure for {a}: {e}",
                    a=actor_id, e=str(exc))

    # Score + sort
    for ev in all_evidence:
        ev["strength"] = _score(ev, pivot, window_days)
    all_evidence.sort(key=lambda e: e.get("strength", 0), reverse=True)

    # Trim — keep top 25 to bound response size.
    trimmed = all_evidence[:25]
    summary = _narrate(label, actual_move, trimmed, window_days)

    return {
        "actor": {
            "id": actor_id,
            "label": label,
            "type": atype,
            "sector": sector,
            "subsector": subsector,
        },
        "window": {
            "start": str(start),
            "end": str(end),
            "pivot": str(pivot),
            "window_days": window_days,
        },
        "actual_move": actual_move,
        "evidence": trimmed,
        "summary": summary,
        "provenance": {
            "sources_checked": sources_checked,
            "evidence_rows": len(all_evidence),
            "window_days": window_days,
        },
    }


# ── Public endpoint ──────────────────────────────────────────────────


@router.get("/{actor_id}/explain")
async def get_actor_explain(
    actor_id: str,
    date: str | None = Query(None, description="Pivot date YYYY-MM-DD, default today"),
    window_days: int = Query(5, ge=1, le=30, description="± window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Hero query: rank evidence explaining a move for this actor on this date.

    Returns the top-25 evidence rows sorted by type-weight × recency plus a
    deterministic narrative citing the top-3 drivers.
    """
    if not actor_id:
        raise HTTPException(status_code=400, detail="actor_id required")

    pivot = _parse_date(date)
    cache_key = f"{actor_id}|{pivot}|{window_days}"
    cached = _explain_cache.get(cache_key)
    if cached is not None:
        return cached

    engine = get_db_engine()
    try:
        result = _build_explain(engine, actor_id, pivot, window_days)
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("explain: fatal error for {a}: {e}", a=actor_id, e=str(exc))
        raise HTTPException(status_code=500, detail="explain lookup failed")

    # 404 semantics: if we couldn't resolve to any sector / supply_chain / ticker
    # AND there's no evidence, treat as unknown.
    if (
        result["actor"]["type"] == "unknown"
        and result["provenance"]["evidence_rows"] == 0
        and result["actual_move"].get("pct") is None
    ):
        raise HTTPException(
            status_code=404,
            detail=f"No data for actor '{actor_id}' in window",
        )

    _explain_cache.set(cache_key, result)
    return result
