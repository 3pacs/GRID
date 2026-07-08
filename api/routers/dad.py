"""Dad-facing ticker lookup endpoints.

The workbook research corpus is produced as a DuckDB sidecar first. GRID can
later bridge the same tables into Postgres, but this route keeps the Dad UI
usable while the ingestion model is still evolving.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/dad", tags=["dad"])

DEFAULT_RESEARCH_DB = (
    "/data/agent-home/anikdang/dad-stock-analysis/work/"
    "extract-20260617/dad_stock_research.duckdb"
)
MAX_EVIDENCE_ROWS = 18
COMPACT_EVIDENCE_ROWS = 5
MAX_FILE_ROWS = 12
COMPACT_FILE_ROWS = 5
MAX_SNIPPET_CHARS = 360
SUMMARY_CACHE_TTL_SECONDS = 300
DAD_CACHE_VERSION = "dad-ticker-v2"
DEFAULT_CHART_POINTS = 220
MAX_CHART_POINTS = 800
FINVIZ_SOURCE_NAME = "finviz_fundamentals"
FINVIZ_BASE_URL = "https://finviz.com/quote.ashx"

FINVIZ_FIELD_MAP: dict[str, tuple[str, str, str]] = {
    "Price": ("price", "Price", "market"),
    "Target Price": ("target_price", "Target", "market"),
    "Market Cap": ("market_cap", "Market Cap", "company"),
    "Sales": ("sales", "Sales", "company"),
    "Income": ("income", "Income", "company"),
    "Sector": ("sector", "Sector", "company"),
    "Industry": ("industry", "Industry", "company"),
    "P/E": ("pe_ratio", "P/E", "valuation"),
    "Forward P/E": ("forward_pe", "Forward P/E", "valuation"),
    "PEG": ("peg", "PEG", "valuation"),
    "P/S": ("price_sales", "P/S", "valuation"),
    "P/B": ("price_book", "P/B", "valuation"),
    "EPS (ttm)": ("eps_ttm", "EPS", "quality"),
    "EPS next 5Y": ("eps_next_5y", "EPS 5Y", "quality"),
    "Dividend %": ("dividend_pct", "Dividend %", "income"),
    "ROE": ("roe", "ROE", "quality"),
    "ROA": ("roa", "ROA", "quality"),
    "ROI": ("roi", "ROI", "quality"),
    "Gross Margin": ("gross_margin", "Gross Margin", "quality"),
    "Oper. Margin": ("operating_margin", "Op Margin", "quality"),
    "Profit Margin": ("profit_margin", "Profit Margin", "quality"),
    "Debt/Eq": ("debt_equity", "Debt/Eq", "risk"),
    "LT Debt/Eq": ("lt_debt_equity", "LT Debt/Eq", "risk"),
    "Beta": ("beta", "Beta", "risk"),
    "ATR": ("atr", "ATR", "risk"),
    "RSI (14)": ("rsi_14", "RSI", "chart"),
    "SMA20": ("sma20", "SMA20", "chart"),
    "SMA50": ("sma50", "SMA50", "chart"),
    "SMA200": ("sma200", "SMA200", "chart"),
    "Perf YTD": ("perf_ytd", "YTD", "chart"),
    "Perf Year": ("perf_year", "1Y Perf", "chart"),
    "52W High": ("fifty_two_week_high", "52W High Gap", "chart"),
    "52W Low": ("fifty_two_week_low", "52W Low Gap", "chart"),
    "Short Float": ("short_float", "Short Float", "risk"),
    "Inst Own": ("institutional_ownership", "Inst Own", "ownership"),
    "Insider Own": ("insider_ownership", "Insider Own", "ownership"),
}

DAD_STAT_CATALOG = [
    {
        "id": "chart_quality",
        "label": "Chart Quality",
        "why": "Dad checks whether the long-term chart is up-and-right and whether it recovered from prior highs.",
        "keywords": ("historical", "price", "cagr", "52", "high", "low", "drawdown", "back to high", "trend"),
        "default_prompt": "Show 1Y, 5Y, 10Y trend, drawdown, recovery from high, and QQQ/SPY relative strength.",
    },
    {
        "id": "benchmark_fit",
        "label": "Benchmark Fit",
        "why": "He thinks in S&P/QQQ context: what sector or theme am I overweight, missing, or duplicating?",
        "keywords": ("qqq", "spy", "s&p", "nasdaq", "benchmark", "portfolio optimization", "weight", "sector"),
        "default_prompt": "Compare against QQQ/SPY and show whether it adds a new role or just repeats existing exposure.",
    },
    {
        "id": "prudent_entry",
        "label": "Prudent Entry",
        "why": "His buy decision wants zones: speculative, prudent, and back-up-the-truck levels.",
        "keywords": ("prudent", "valuation", "forward", "p/e", "pe", "target", "buy zone", "entry", "price"),
        "default_prompt": "Show valuation context, current price versus target zones, and reason not to buy yet.",
    },
    {
        "id": "options_yield",
        "label": "Options / Yield",
        "why": "He uses covered calls and spreads to define payoff, premium, stop, and take-profit.",
        "keywords": ("option", "covered", "call", "put", "premium", "expiry", "strike", "spread", "exercised"),
        "default_prompt": "Show premium, days to expiry, return if exercised, max loss, take-profit, and stop-loss.",
    },
    {
        "id": "portfolio_role",
        "label": "Portfolio Role",
        "why": "He wants to know what account, sleeve, and allocation role a name belongs in.",
        "keywords": ("portfolio", "allocation", "holding", "shares", "rebalance", "cost basis", "trust", "roth"),
        "default_prompt": "Show sleeve, target size, current exposure, cost basis evidence, and rebalance impact.",
    },
    {
        "id": "risk_control",
        "label": "Risk Control",
        "why": "The spreadsheet language repeatedly checks downside, distance from high, stop-loss, and drawdown.",
        "keywords": ("risk", "down", "drawdown", "stop", "loss", "debt", "downside", "from high"),
        "default_prompt": "Show what can break the thesis, downside level, debt/liquidity risk, and exit trigger.",
    },
]


def _research_db_path() -> Path:
    return Path(os.getenv("GRID_DAD_STOCK_RESEARCH_DB", DEFAULT_RESEARCH_DB)).expanduser()


def _perf_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 1)


def _timed(name: str, timings: dict[str, float], fn):
    start = time.perf_counter()
    try:
        return fn()
    finally:
        timings[name] = _perf_ms(start)


def _log_slow_ticker(ticker: str, timings: dict[str, float], *, route: str) -> None:
    total = round(sum(timings.values()), 1)
    if total >= 500:
        log.info(
            "Dad ticker timing {route} {ticker}: total={total}ms sections={sections}",
            route=route,
            ticker=ticker,
            total=total,
            sections=timings,
        )


def _normalize_ticker(ticker: str) -> str:
    match = re.search(r"\$?([A-Za-z0-9^.][A-Za-z0-9.^-]{0,11})", ticker or "")
    if not match:
        return ""
    cleaned = re.sub(r"[^A-Za-z0-9.^-]", "", match.group(1)).strip().upper()
    return cleaned.replace("-", ".")[:12]


def _shorten(value: Any, limit: int = MAX_SNIPPET_CHARS) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)].rstrip()}..."


def _keyword_hits(text: str) -> list[str]:
    checks = {
        "buy/rank language": r"\b(buy|rank|score|watch|target|upside|conviction)\b",
        "position language": r"\b(position|holding|shares|portfolio|allocation)\b",
        "valuation language": r"\b(cagr|eps|revenue|margin|fcf|ebitda|multiple|pe|peg|dcf)\b",
        "risk language": r"\b(risk|drawdown|downside|debt|loss|sell|stop)\b",
        "price language": r"\b(price|close|high|low|open|resistance|support)\b",
    }
    lower = text.lower()
    return [label for label, pattern in checks.items() if re.search(pattern, lower)]


def _downsample_points(points: list[dict[str, Any]], max_points: int | None = None) -> list[dict[str, Any]]:
    if not max_points or max_points <= 0 or len(points) <= max_points:
        return points
    if max_points == 1:
        return [points[-1]]
    step = (len(points) - 1) / (max_points - 1)
    sampled: list[dict[str, Any]] = []
    seen: set[int] = set()
    for idx in range(max_points):
        source_idx = round(idx * step)
        if source_idx in seen:
            continue
        seen.add(source_idx)
        sampled.append(points[source_idx])
    if sampled[-1] is not points[-1]:
        sampled[-1] = points[-1]
    return sampled


def _range_to_days(range_name: str | None) -> int:
    key = (range_name or "1Y").strip().upper()
    return {
        "1M": 31,
        "3M": 92,
        "6M": 183,
        "1Y": 365,
        "2Y": 730,
        "5Y": 1825,
        "10Y": 3650,
        "MAX": 3650,
    }.get(key, 365)


def _detail_urls(ticker: str) -> dict[str, str]:
    enc = ticker
    return {
        "stream": f"/api/v1/dad/ticker/{enc}/gold/stream",
        "evidence": f"/api/v1/dad/ticker/{enc}/evidence?limit=50&offset=0",
        "chart": f"/api/v1/dad/ticker/{enc}/chart?range=1Y&points={DEFAULT_CHART_POINTS}",
        "finviz": f"/api/v1/dad/ticker/{enc}/finviz",
        "options": f"/api/v1/dad/ticker/{enc}/options?days=90&limit=90",
    }


def _compact_finviz(finviz: dict[str, Any], *, include_fields: bool = False) -> dict[str, Any]:
    compact = {
        "status": finviz.get("status", "unavailable"),
        "source": finviz.get("source", "postgres"),
        "freshness": finviz.get("freshness", _freshness_state(None)),
        "latest_pull": finviz.get("latest_pull"),
        "latest_obs_date": finviz.get("latest_obs_date"),
        "field_count": finviz.get("field_count", 0),
        "rows_inserted": finviz.get("rows_inserted", 0),
        "live_refresh_requested": finviz.get("live_refresh_requested", False),
        "refresh_available": finviz.get("refresh_available", True),
        "stats": finviz.get("stats", []),
        "error": finviz.get("error"),
    }
    if include_fields:
        compact["fields"] = finviz.get("fields", {})
    return compact


def _compact_grid(
    grid: dict[str, Any],
    *,
    include_price_history: bool = False,
    max_points: int | None = None,
) -> dict[str, Any]:
    original_history = grid.get("price_history") or []
    price_history = _downsample_points(original_history, max_points) if include_price_history else []
    total_points = int(grid.get("price_points_total") or len(original_history))
    return {
        "status": grid.get("status", "missing"),
        "feature_names": grid.get("feature_names", []),
        "metrics": grid.get("metrics", {}),
        "freshness": grid.get("freshness", _freshness_state(None)),
        "source_freshness": grid.get("source_freshness", []),
        "features": grid.get("features", []),
        "price_history": price_history,
        "price_points_total": total_points,
        "price_points_returned": len(price_history),
    }


def _compact_signals(signals: dict[str, Any], *, include_rows: bool = False) -> dict[str, Any]:
    signal_rows = signals.get("signal_sources", []) or []
    tv_rows = signals.get("tradingview_signals", []) or []
    payload = {
        "signal_count": len(signal_rows),
        "tradingview_count": len(tv_rows),
        "regime": signals.get("regime"),
    }
    if include_rows:
        payload["signal_sources"] = signal_rows
        payload["tradingview_signals"] = tv_rows
    return payload


def _source_lane(file_name: str | None, sheet_name: str | None) -> str:
    text = f"{file_name or ''} {sheet_name or ''}".lower()
    if "anik trading log" in text or "akul trading log" in text or "akul anik" in text:
        return "anik_main"
    if any(token in text for token in (
        "options", "option", "covered call", "vertical spread", "bitcoin", "coinbase",
        "soxl", "tqqq", "tecl", "trading log", "expiry", "call put",
    )):
        return "gamble_sleeve"
    if any(token in text for token in (
        "cagr", "historical", "portfolio", "stock stats", "prudent", "monte carlo",
        "building a stock portfolio", "trading stocks", "portfolio visualizer",
    )):
        return "dad_method"
    return "workbook"


def _lane_counts(evidence_rows: list[dict[str, Any]], files: list[dict[str, Any]], sheets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lane_meta = {
        "dad_method": {
            "label": "Dad Method",
            "detail": "Framework files: CAGR, historical charts, portfolio role, prudent entry, and benchmark context.",
        },
        "anik_main": {
            "label": "Anik Main",
            "detail": "Your main account or Anik/Akul portfolio sheets. Do not treat as Dad's picks.",
        },
        "gamble_sleeve": {
            "label": "Gamble Sleeve",
            "detail": "Options, spreads, crypto, levered ETFs, and trading-log style evidence.",
        },
        "workbook": {
            "label": "Other Workbook",
            "detail": "Workbook evidence that needs manual classification.",
        },
    }
    counts: dict[str, dict[str, Any]] = {
        key: {"id": key, **meta, "evidence_rows": 0, "file_hits": 0, "sheet_hits": 0}
        for key, meta in lane_meta.items()
    }
    for row in evidence_rows:
        counts[_source_lane(row.get("file"), row.get("sheet"))]["evidence_rows"] += 1
    for row in files:
        counts[_source_lane(row.get("file"), None)]["file_hits"] += int(row.get("mentions") or 0)
    for row in sheets:
        counts[_source_lane(row.get("file"), row.get("sheet"))]["sheet_hits"] += int(row.get("mentions") or 0)
    return [row for row in counts.values() if row["evidence_rows"] or row["file_hits"] or row["sheet_hits"]]


def _dad_stat_cards(evidence_rows: list[dict[str, Any]], files: list[dict[str, Any]], sheets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    corpus = []
    for row in evidence_rows:
        corpus.append(" ".join(str(row.get(key) or "") for key in ("file", "sheet", "evidence_text", "row_context", "column_header")))
    for row in sheets:
        corpus.append(" ".join(str(row.get(key) or "") for key in ("file", "sheet")))
    for row in files:
        corpus.append(str(row.get("file") or ""))
    haystack = "\n".join(corpus).lower()

    cards: list[dict[str, Any]] = []
    for item in DAD_STAT_CATALOG:
        hits = [kw for kw in item["keywords"] if kw in haystack]
        state = "present" if hits else "needed"
        cards.append({
            "id": item["id"],
            "label": item["label"],
            "state": state,
            "why": item["why"],
            "hits": hits[:6],
            "prompt": item["default_prompt"],
        })
    return cards


def _tradingview_payload(ticker: str) -> dict[str, Any]:
    symbol = ticker.replace(".", "-")
    # Default to NASDAQ for US equities; TradingView's search still works from
    # the open URL if the actual venue differs.
    tv_symbol = f"NASDAQ:{symbol}"
    encoded = tv_symbol.replace(":", "%3A")
    return {
        "symbol": tv_symbol,
        "chart_url": f"https://www.tradingview.com/chart/?symbol={encoded}",
        "symbol_search_url": f"https://www.tradingview.com/symbols/{symbol}/",
        "webhook_note": "GRID can show TradingView webhook alerts already sent into /api/v1/tradingview/webhook.",
    }


def _parse_finviz_value(raw: str | None) -> float | str | None:
    """Parse Finviz's compact display values while preserving text fields."""
    if not raw or raw == "-":
        return None

    clean = raw.strip().replace(",", "")
    if not clean:
        return None
    if clean.endswith("%"):
        try:
            return float(clean[:-1])
        except ValueError:
            return raw.strip()

    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
    suffix = clean[-1]
    if suffix in multipliers:
        try:
            return float(clean[:-1]) * multipliers[suffix]
        except ValueError:
            return raw.strip()

    try:
        return float(clean)
    except ValueError:
        return raw.strip()


def _as_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _age_hours(value: Any) -> float | None:
    dt = _as_utc(value)
    if dt is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600)


def _freshness_state(value: Any, *, stale_hours: int = 48) -> dict[str, Any]:
    age = _age_hours(value)
    if age is None:
        return {"state": "missing", "age_hours": None, "label": "missing"}
    if age <= stale_hours:
        return {"state": "fresh", "age_hours": round(age, 2), "label": "fresh"}
    if age <= stale_hours * 2:
        return {"state": "aging", "age_hours": round(age, 2), "label": "aging"}
    return {"state": "stale", "age_hours": round(age, 2), "label": "stale"}


def _research_db_fingerprint(db_path: Path) -> tuple[str, float | None]:
    try:
        return str(db_path), db_path.stat().st_mtime if db_path.exists() else None
    except Exception:
        return str(db_path), None


def _ensure_summary_cache_table(engine: Any) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dad_ticker_summary_cache (
                    ticker             TEXT PRIMARY KEY,
                    payload_version    TEXT NOT NULL,
                    generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    research_db_path   TEXT,
                    research_db_mtime  DOUBLE PRECISION,
                    payload            JSONB NOT NULL,
                    timings            JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_dad_summary_cache_generated
                    ON dad_ticker_summary_cache (generated_at DESC)
            """))
    except Exception as exc:
        log.debug("Dad summary cache table unavailable: {e}", e=str(exc))


def _read_summary_cache(engine: Any, ticker: str, db_path: Path) -> dict[str, Any] | None:
    path, mtime = _research_db_fingerprint(db_path)
    try:
        _ensure_summary_cache_table(engine)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT payload, generated_at, timings "
                    "FROM dad_ticker_summary_cache "
                    "WHERE ticker = :ticker "
                    "AND payload_version = :version "
                    "AND research_db_path = :path "
                    "AND research_db_mtime IS NOT DISTINCT FROM :mtime "
                    f"AND generated_at >= NOW() - INTERVAL '{SUMMARY_CACHE_TTL_SECONDS} seconds' "
                    "LIMIT 1"
                ),
                {
                    "ticker": ticker,
                    "version": DAD_CACHE_VERSION,
                    "path": path,
                    "mtime": mtime,
                },
            ).fetchone()
    except Exception as exc:
        log.debug("Dad summary cache read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    if not row:
        return None
    payload = row[0]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    payload = dict(payload)
    payload["cache"] = {
        "hit": True,
        "generated_at": _as_utc(row[1]).isoformat() if _as_utc(row[1]) else str(row[1]),
        "ttl_seconds": SUMMARY_CACHE_TTL_SECONDS,
    }
    return payload


def _write_summary_cache(engine: Any, ticker: str, db_path: Path, payload: dict[str, Any], timings: dict[str, float]) -> None:
    path, mtime = _research_db_fingerprint(db_path)
    to_store = dict(payload)
    to_store["cache"] = {"hit": False, "ttl_seconds": SUMMARY_CACHE_TTL_SECONDS}
    try:
        _ensure_summary_cache_table(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO dad_ticker_summary_cache "
                    "(ticker, payload_version, generated_at, research_db_path, research_db_mtime, payload, timings) "
                    "VALUES (:ticker, :version, NOW(), :path, :mtime, CAST(:payload AS JSONB), CAST(:timings AS JSONB)) "
                    "ON CONFLICT (ticker) DO UPDATE SET "
                    "payload_version = EXCLUDED.payload_version, "
                    "generated_at = EXCLUDED.generated_at, "
                    "research_db_path = EXCLUDED.research_db_path, "
                    "research_db_mtime = EXCLUDED.research_db_mtime, "
                    "payload = EXCLUDED.payload, "
                    "timings = EXCLUDED.timings"
                ),
                {
                    "ticker": ticker,
                    "version": DAD_CACHE_VERSION,
                    "path": path,
                    "mtime": mtime,
                    "payload": json.dumps(to_store, default=str),
                    "timings": json.dumps(timings, default=str),
                },
            )
    except Exception as exc:
        log.debug("Dad summary cache write failed for {t}: {e}", t=ticker, e=str(exc))


def _ensure_finviz_source_id(engine: Any) -> int:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = :name"),
            {"name": FINVIZ_SOURCE_NAME},
        ).fetchone()
        if row:
            return int(row[0])

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO source_catalog "
                "(name, base_url, cost_tier, latency_class, pit_available, "
                "revision_behavior, trust_score, priority_rank, active) "
                "VALUES (:name, :url, 'FREE', 'EOD', FALSE, 'NEVER', 'MED', 45, TRUE) "
                "ON CONFLICT (name) DO UPDATE SET active = TRUE "
                "RETURNING id"
            ),
            {"name": FINVIZ_SOURCE_NAME, "url": FINVIZ_BASE_URL},
        ).fetchone()
        return int(row[0])


def _parse_finviz_snapshot_html(html: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    pattern = re.compile(
        r'class="snapshot-td2[^"]*cursor-pointer[^"]*"[^>]*>([^<]+)</td>'
        r'\s*<td[^>]*class="snapshot-td2[^"]*"[^>]*><b>([^<]*)</b>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        pairs[match.group(1).strip()] = match.group(2).strip()

    if pairs:
        return pairs

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        cells = [cell.get_text(" ", strip=True) for cell in soup.select("td.snapshot-td2")]
        for index in range(0, len(cells) - 1, 2):
            label = cells[index].strip()
            value = cells[index + 1].strip()
            if label and value:
                pairs[label] = value
    except Exception as exc:
        log.debug("Finviz snapshot BeautifulSoup parse failed: {e}", e=str(exc))

    return pairs


def _fetch_finviz_snapshot(ticker: str) -> dict[str, str]:
    import requests

    response = requests.get(
        FINVIZ_BASE_URL,
        params={"t": ticker},
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
        timeout=8,
    )
    response.raise_for_status()
    pairs = _parse_finviz_snapshot_html(response.text)
    if not pairs:
        raise RuntimeError("no Finviz snapshot table parsed")
    return pairs


def _read_finviz_rows(engine: Any, ticker: str) -> dict[str, Any]:
    rows: list[Any] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ON (rs.series_id) "
                    "  rs.series_id, rs.obs_date, rs.pull_timestamp, rs.value, rs.raw_payload "
                    "FROM raw_series rs "
                    "JOIN source_catalog sc ON sc.id = rs.source_id "
                    "WHERE sc.name = :source "
                    "AND rs.series_id LIKE :prefix "
                    "AND rs.pull_status = 'SUCCESS' "
                    "ORDER BY rs.series_id, rs.obs_date DESC, rs.pull_timestamp DESC"
                ),
                {"source": FINVIZ_SOURCE_NAME, "prefix": f"finviz.{ticker}.%"},
            ).fetchall()
    except Exception as exc:
        log.debug("Finviz Postgres read failed for {t}: {e}", t=ticker, e=str(exc))

    fields: dict[str, dict[str, Any]] = {}
    latest_pull: datetime | None = None
    latest_obs: date | None = None
    for row in rows:
        series_id, obs_date, pull_timestamp, value, payload = row
        field = str(series_id).split(".")[-1]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        payload = payload if isinstance(payload, dict) else {}
        pull_dt = _as_utc(pull_timestamp)
        fields[field] = {
            "field": field,
            "label": payload.get("label") or field.replace("_", " ").title(),
            "group": payload.get("group") or "other",
            "raw_value": payload.get("raw_value"),
            "parsed": payload.get("parsed"),
            "numeric_value": float(value) if value is not None else None,
            "obs_date": str(obs_date) if obs_date else None,
            "pull_timestamp": pull_dt.isoformat() if pull_dt else str(pull_timestamp),
        }
        if pull_dt and (latest_pull is None or pull_dt > latest_pull):
            latest_pull = pull_dt
        if obs_date and (latest_obs is None or obs_date > latest_obs):
            latest_obs = obs_date

    return {
        "fields": fields,
        "field_count": len(fields),
        "latest_pull": latest_pull,
        "latest_obs_date": latest_obs,
    }


def _store_finviz_snapshot(engine: Any, ticker: str, pairs: dict[str, str]) -> int:
    source_id = _ensure_finviz_source_id(engine)
    today = date.today()
    now = datetime.now(timezone.utc)
    inserted = 0

    with engine.begin() as conn:
        for finviz_label, (field_id, display_label, group) in FINVIZ_FIELD_MAP.items():
            raw_value = pairs.get(finviz_label)
            parsed = _parse_finviz_value(raw_value)
            if raw_value is None or parsed is None:
                continue

            numeric_value = parsed if isinstance(parsed, (int, float)) else 0.0
            payload = {
                "ticker": ticker,
                "field": field_id,
                "label": display_label,
                "group": group,
                "finviz_label": finviz_label,
                "raw_value": raw_value,
                "parsed": parsed,
                "source_url": f"{FINVIZ_BASE_URL}?t={ticker}",
                "scraped_at": now.isoformat(),
            }
            result = conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
                    "SELECT :series_id, :source_id, :obs_date, :pull_timestamp, :value, :payload, 'SUCCESS' "
                    "WHERE NOT EXISTS ("
                    "  SELECT 1 FROM raw_series "
                    "  WHERE source_id = :source_id "
                    "  AND series_id = :series_id "
                    "  AND obs_date = :obs_date "
                    "  AND pull_status = 'SUCCESS'"
                    ")"
                ),
                {
                    "series_id": f"finviz.{ticker}.{field_id}",
                    "source_id": source_id,
                    "obs_date": today,
                    "pull_timestamp": now,
                    "value": float(numeric_value),
                    "payload": json.dumps(payload),
                },
            )
            inserted += int(result.rowcount or 0)

        conn.execute(
            text("UPDATE source_catalog SET last_pull_at = NOW() WHERE id = :source_id"),
            {"source_id": source_id},
        )

    return inserted


def _finviz_stat_cards(fields: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    priority = [
        "price", "target_price", "market_cap", "pe_ratio", "forward_pe", "peg",
        "price_sales", "eps_ttm", "eps_next_5y", "roe", "gross_margin",
        "operating_margin", "debt_equity", "beta", "rsi_14", "sma50",
        "sma200", "perf_year", "short_float", "institutional_ownership",
    ]
    cards: list[dict[str, Any]] = []
    for field_id in priority:
        item = fields.get(field_id)
        if not item:
            continue
        cards.append({
            "id": field_id,
            "label": item["label"],
            "group": item["group"],
            "raw_value": item.get("raw_value"),
            "parsed": item.get("parsed"),
            "numeric_value": item.get("numeric_value"),
        })
    return cards


def _get_finviz_profile(engine: Any, ticker: str, *, refresh: bool = False) -> dict[str, Any]:
    stored = _read_finviz_rows(engine, ticker)
    freshness = _freshness_state(stored.get("latest_pull"), stale_hours=24)
    scraped = False
    scrape_error: str | None = None

    if refresh and freshness["state"] in {"missing", "aging", "stale"}:
        try:
            pairs = _fetch_finviz_snapshot(ticker)
            inserted = _store_finviz_snapshot(engine, ticker, pairs)
            scraped = True
            stored = _read_finviz_rows(engine, ticker)
            freshness = _freshness_state(stored.get("latest_pull"), stale_hours=24)
            stored["rows_inserted"] = inserted
        except Exception as exc:
            scrape_error = str(exc)
            log.debug("Finviz live scrape failed for {t}: {e}", t=ticker, e=scrape_error)

    fields = stored.get("fields", {})
    status = "ready" if fields else "unavailable"
    if fields and freshness["state"] in {"aging", "stale"}:
        status = "stale"

    return {
        "status": status,
        "source": "postgres+live" if scraped else "postgres",
        "freshness": freshness,
        "latest_pull": stored.get("latest_pull").isoformat() if stored.get("latest_pull") else None,
        "latest_obs_date": str(stored.get("latest_obs_date")) if stored.get("latest_obs_date") else None,
        "field_count": stored.get("field_count", 0),
        "rows_inserted": stored.get("rows_inserted", 0),
        "live_refresh_requested": refresh,
        "refresh_available": True,
        "stats": _finviz_stat_cards(fields),
        "fields": fields,
        "error": scrape_error,
    }


def _gold_from_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    if not summary:
        return {
            "verdict": "No workbook history yet",
            "score": 0,
            "tone": "neutral",
            "one_liner": "This ticker is not showing up in Dad's copied workbook corpus yet.",
        }

    mentions = int(summary.get("mentions") or 0)
    file_count = int(summary.get("file_count") or 0)
    sheet_count = int(summary.get("sheet_count") or 0)
    evidence_score = float(summary.get("evidence_score") or 0)
    score = min(100, round(evidence_score * 2.5 + file_count * 8 + sheet_count * 2 + min(mentions, 30)))

    if score >= 80 and file_count >= 3:
        verdict = "High workbook conviction"
        tone = "strong"
        one_liner = "Dad's workbooks mention this ticker repeatedly across files and sheets."
    elif score >= 45:
        verdict = "Known name in Dad's research"
        tone = "watch"
        one_liner = "This has enough workbook footprint to deserve a serious look."
    elif score >= 15:
        verdict = "Light workbook footprint"
        tone = "light"
        one_liner = "There is workbook evidence, but not enough to treat it as a core Dad name."
    else:
        verdict = "Trace evidence only"
        tone = "neutral"
        one_liner = "Only a small amount of workbook evidence showed up for this ticker."

    return {
        "verdict": verdict,
        "score": score,
        "tone": tone,
        "one_liner": one_liner,
    }


def _fit_signals(summary: dict[str, Any] | None, evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not summary:
        return []

    source_types = set(str(summary.get("source_types") or "").split(","))
    combined = " ".join(
        str(row.get("evidence_text") or "") + " " + str(row.get("row_context") or "")
        for row in evidence_rows
    )
    hits = _keyword_hits(combined)
    signals: list[dict[str, Any]] = []

    signals.append({
        "label": "Workbook depth",
        "state": "strong" if int(summary.get("file_count") or 0) >= 3 else "partial",
        "detail": (
            f"{summary.get('mentions', 0)} mentions across "
            f"{summary.get('file_count', 0)} files and {summary.get('sheet_count', 0)} sheets"
        ),
    })
    if "filename" in source_types:
        signals.append({
            "label": "Named at file level",
            "state": "strong",
            "detail": "The ticker appears in workbook or file names, not only individual cells.",
        })
    if "sheet_name" in source_types:
        signals.append({
            "label": "Named at sheet level",
            "state": "watch",
            "detail": "At least one sheet name carries the ticker or a close ticker token.",
        })
    if hits:
        signals.append({
            "label": "Dad-method language",
            "state": "watch",
            "detail": ", ".join(hits[:4]),
        })
    if not hits:
        signals.append({
            "label": "Context still thin",
            "state": "neutral",
            "detail": "Mentions were found, but the extractor has not classified clear decision language yet.",
        })
    return signals


def _source_freshness(engine: Any, source_names: list[str]) -> list[dict[str, Any]]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT sc.name, sc.last_pull_at, "
                    "  (SELECT rs.pull_timestamp FROM raw_series rs "
                    "   WHERE rs.source_id = sc.id "
                    "   ORDER BY rs.pull_timestamp DESC LIMIT 1) AS latest_raw_pull "
                    "FROM source_catalog sc "
                    "WHERE sc.name = ANY(:names) "
                    "ORDER BY sc.name"
                ),
                {"names": source_names},
            ).fetchall()
    except Exception as exc:
        log.debug("Dad source freshness read failed: {e}", e=str(exc))
        return []

    found = {row[0] for row in rows}
    freshness = []
    for row in rows:
        source, last_pull, latest_raw = row
        effective = latest_raw or last_pull
        freshness.append({
            "source": source,
            "last_pull": _as_utc(last_pull).isoformat() if _as_utc(last_pull) else None,
            "latest_raw_pull": _as_utc(latest_raw).isoformat() if _as_utc(latest_raw) else None,
            **_freshness_state(effective, stale_hours=48),
        })

    for missing in source_names:
        if missing not in found:
            freshness.append({
                "source": missing,
                "last_pull": None,
                "latest_raw_pull": None,
                **_freshness_state(None),
            })
    return freshness


def _grid_market_context(
    engine: Any,
    ticker: str,
    *,
    days: int = 365,
    include_price_history: bool = True,
    max_points: int | None = None,
    feature_limit: int = 20,
) -> dict[str, Any]:
    try:
        from api.routers.watchlist_helpers import _get_display_name, _interpret_feature, _resolve_feature_names
    except Exception as exc:
        log.debug("Watchlist helpers unavailable for Dad context: {e}", e=str(exc))
        return {"status": "unavailable", "price_history": [], "features": []}

    feature_names = _resolve_feature_names(ticker)
    prices: list[dict[str, Any]] = []
    features: list[dict[str, Any]] = []
    cutoff = date.today() - timedelta(days=max(1, min(days, 3650)))

    try:
        with engine.connect() as conn:
            price_rows = conn.execute(
                text(
                    "SELECT rs.obs_date, rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = ANY(:names) "
                    "AND rs.obs_date >= :cutoff "
                    "ORDER BY rs.obs_date"
                ),
                {"names": feature_names, "cutoff": cutoff},
            ).fetchall()
            prices = [{"date": str(row[0]), "value": float(row[1])} for row in price_rows]

            if not prices:
                raw_rows = conn.execute(
                    text(
                        "SELECT rs.obs_date, rs.value "
                        "FROM raw_series rs "
                        "JOIN source_catalog sc ON sc.id = rs.source_id "
                        "WHERE sc.name = 'yfinance' "
                        "AND rs.series_id = ANY(:series_ids) "
                        "AND rs.obs_date >= :cutoff "
                        "ORDER BY rs.obs_date"
                    ),
                    {
                        "series_ids": [f"yf_{ticker.lower()}_close", f"YF:{ticker}:close"],
                        "cutoff": cutoff,
                    },
                ).fetchall()
                prices = [{"date": str(row[0]), "value": float(row[1])} for row in raw_rows]

            like_patterns = [f"{ticker.lower()}%"]
            clean = ticker.lower().lstrip("^").replace("=", "")
            if clean != ticker.lower():
                like_patterns.append(f"{clean}%")
            if feature_names:
                canonical = feature_names[0].rsplit("_", 1)[0]
                pattern = f"{canonical}%"
                if pattern not in like_patterns:
                    like_patterns.append(pattern)

            feature_rows = conn.execute(
                text(
                    "SELECT fr.name, fr.family, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE (" + " OR ".join(
                        f"fr.name LIKE :p{i}" for i in range(len(like_patterns))
                    ) + ") "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
                    "  WHERE rs2.feature_id = rs.feature_id"
                    ") "
                    "ORDER BY fr.family, fr.name "
                    "LIMIT :feature_limit"
                ),
                {
                    **{f"p{i}": pattern for i, pattern in enumerate(like_patterns)},
                    "feature_limit": max(1, min(feature_limit, 50)),
                },
            ).fetchall()
    except Exception as exc:
        log.debug("Dad GRID market context failed for {t}: {e}", t=ticker, e=str(exc))

    latest_price = prices[-1]["value"] if prices else None
    for row in (feature_rows if "feature_rows" in locals() else []):
        value = float(row[2]) if row[2] is not None else None
        interpretation, signal = _interpret_feature(row[0], value, latest_price)
        features.append({
            "name": row[0],
            "display_name": _get_display_name(row[0]),
            "family": row[1],
            "value": value,
            "obs_date": str(row[3]),
            "interpretation": interpretation,
            "signal": signal,
        })

    metrics: dict[str, Any] = {}
    if prices:
        latest = prices[-1]["value"]
        first = prices[0]["value"]
        high = max(row["value"] for row in prices)
        low = min(row["value"] for row in prices)
        metrics = {
            "latest_price": latest,
            "first_price": first,
            "return_1y_pct": ((latest - first) / first * 100) if first else None,
            "high_52w": high,
            "low_52w": low,
            "pct_from_52w_high": ((latest - high) / high * 100) if high else None,
            "pct_above_52w_low": ((latest - low) / low * 100) if low else None,
            "obs_count": len(prices),
            "as_of": prices[-1]["date"],
        }

    return {
        "status": "ready" if prices or features else "missing",
        "feature_names": feature_names,
        "price_history": _downsample_points(prices, max_points) if include_price_history else [],
        "price_points_total": len(prices),
        "metrics": metrics,
        "features": features,
        "freshness": _freshness_state(metrics.get("as_of"), stale_hours=96),
    }


def _latest_options_context(engine: Any, ticker: str) -> dict[str, Any] | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT signal_date, put_call_ratio, max_pain, iv_skew, "
                    "total_oi, total_volume, spot_price, iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "ORDER BY signal_date DESC LIMIT 1"
                ),
                {"ticker": ticker},
            ).fetchone()
    except Exception as exc:
        log.debug("Dad options context failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    if not row:
        return None
    return {
        "date": str(row[0]),
        "put_call_ratio": row[1],
        "max_pain": row[2],
        "iv_skew": row[3],
        "total_oi": row[4],
        "total_volume": row[5],
        "spot_price": row[6],
        "iv_atm": row[7],
        "term_slope": row[8],
        "oi_concentration": row[9],
        "freshness": _freshness_state(row[0], stale_hours=96),
    }


def _options_history_context(engine: Any, ticker: str, *, days: int = 90, limit: int = 90) -> dict[str, Any]:
    bounded_days = max(1, min(days, 365))
    bounded_limit = max(1, min(limit, 250))
    cutoff = date.today() - timedelta(days=bounded_days)
    rows: list[dict[str, Any]] = []
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT signal_date, put_call_ratio, max_pain, iv_skew, "
                    "total_oi, total_volume, spot_price, iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT :limit"
                ),
                {"ticker": ticker, "cutoff": cutoff, "limit": bounded_limit},
            ).fetchall()
    except Exception as exc:
        log.debug("Dad options history failed for {t}: {e}", t=ticker, e=str(exc))
        result = []

    for row in result:
        rows.append({
            "date": str(row[0]),
            "put_call_ratio": row[1],
            "max_pain": row[2],
            "iv_skew": row[3],
            "total_oi": row[4],
            "total_volume": row[5],
            "spot_price": row[6],
            "iv_atm": row[7],
            "term_slope": row[8],
            "oi_concentration": row[9],
        })
    latest = rows[0] if rows else _latest_options_context(engine, ticker)
    return {
        "status": "ready" if rows or latest else "missing",
        "latest": latest,
        "history": list(reversed(rows)),
        "days": bounded_days,
        "limit": bounded_limit,
        "freshness": _freshness_state(latest.get("date") if latest else None, stale_hours=96),
    }


def _latest_signal_context(engine: Any, ticker: str) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    tv: list[dict[str, Any]] = []
    regime: dict[str, Any] | None = None

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, source_id, signal_date, signal_type, "
                    "signal_value, trust_score, created_at "
                    "FROM signal_sources "
                    "WHERE ticker = :ticker "
                    "ORDER BY signal_date DESC, created_at DESC "
                    "LIMIT 12"
                ),
                {"ticker": ticker},
            ).fetchall()
            for row in rows:
                payload = row[4]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                signals.append({
                    "source_type": row[0],
                    "source_id": row[1],
                    "date": str(row[2]),
                    "signal_type": row[3],
                    "signal_value": payload if isinstance(payload, dict) else {},
                    "trust_score": float(row[5] or 0),
                    "created_at": _as_utc(row[6]).isoformat() if _as_utc(row[6]) else None,
                })

            tv_rows = conn.execute(
                text(
                    "SELECT rs.pull_timestamp, rs.value, rs.raw_payload "
                    "FROM raw_series rs "
                    "JOIN source_catalog sc ON sc.id = rs.source_id "
                    "WHERE sc.name = 'TradingView' "
                    "AND rs.series_id LIKE :pattern "
                    "ORDER BY rs.pull_timestamp DESC LIMIT 10"
                ),
                {"pattern": f"tv_{ticker.lower()}%"},
            ).fetchall()
            for row in tv_rows:
                payload = row[2]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                tv.append({
                    "timestamp": _as_utc(row[0]).isoformat() if _as_utc(row[0]) else str(row[0]),
                    "signal_value": float(row[1]) if row[1] is not None else None,
                    **(payload if isinstance(payload, dict) else {}),
                })

            regime_row = conn.execute(
                text(
                    "SELECT inferred_state, state_confidence, grid_recommendation, "
                    "operator_confidence, decision_timestamp "
                    "FROM decision_journal "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                )
            ).fetchone()
            if regime_row:
                regime = {
                    "state": regime_row[0],
                    "confidence": float(regime_row[1] or 0),
                    "grid_recommendation": regime_row[2],
                    "operator_confidence": regime_row[3],
                    "as_of": _as_utc(regime_row[4]).isoformat() if _as_utc(regime_row[4]) else str(regime_row[4]),
                    "freshness": _freshness_state(regime_row[4], stale_hours=48),
                }
    except Exception as exc:
        log.debug("Dad signal context failed for {t}: {e}", t=ticker, e=str(exc))

    return {
        "signal_sources": signals,
        "tradingview_signals": tv,
        "regime": regime,
    }


def _num_field(finviz: dict[str, Any], field_id: str) -> float | None:
    item = finviz.get("fields", {}).get(field_id)
    if not item:
        return None
    value = item.get("parsed")
    if isinstance(value, (int, float)):
        return float(value)
    numeric = item.get("numeric_value")
    return float(numeric) if isinstance(numeric, (int, float)) else None


def _grid_decision_stack(
    summary: dict[str, Any] | None,
    gold: dict[str, Any],
    grid: dict[str, Any],
    finviz: dict[str, Any],
    options: dict[str, Any] | None,
    signals: dict[str, Any],
) -> dict[str, Any]:
    cards: list[dict[str, Any]] = []
    reasons: list[str] = []
    blockers: list[str] = []

    score = round(float(gold.get("score") or 0) * 0.35, 1)
    cards.append({
        "source": "Dad workbooks",
        "state": "strong" if summary and int(summary.get("file_count") or 0) >= 3 else "watch" if summary else "missing",
        "points": score,
        "detail": gold.get("one_liner") or "No workbook prior.",
    })

    metrics = grid.get("metrics", {})
    ret_1y = metrics.get("return_1y_pct")
    from_high = metrics.get("pct_from_52w_high")
    chart_points = 0.0
    if ret_1y is not None:
        if ret_1y >= 20:
            chart_points += 15
            reasons.append(f"GRID 1Y trend is strong at {ret_1y:.1f}%.")
        elif ret_1y > 0:
            chart_points += 8
            reasons.append(f"GRID 1Y trend is positive at {ret_1y:.1f}%.")
        else:
            chart_points -= 4
            blockers.append(f"GRID 1Y trend is negative at {ret_1y:.1f}%.")
    if from_high is not None:
        if from_high >= -10:
            chart_points += 5
        elif from_high <= -30:
            chart_points -= 5
            blockers.append(f"Price is {abs(from_high):.1f}% below its 52-week high.")
    if not metrics:
        blockers.append("GRID has no resolved price history for this ticker yet.")
    score += chart_points
    cards.append({
        "source": "GRID price history",
        "state": "strong" if chart_points >= 15 else "watch" if chart_points > 0 else "missing" if not metrics else "caution",
        "points": round(chart_points, 1),
        "detail": (
            f"1Y {ret_1y:.1f}%, {abs(from_high):.1f}% from 52W high"
            if ret_1y is not None and from_high is not None
            else "No GRID chart history."
        ),
    })

    finviz_points = 0.0
    forward_pe = _num_field(finviz, "forward_pe") or _num_field(finviz, "pe_ratio")
    roe = _num_field(finviz, "roe")
    debt_eq = _num_field(finviz, "debt_equity")
    margin = _num_field(finviz, "profit_margin") or _num_field(finviz, "operating_margin")
    eps_5y = _num_field(finviz, "eps_next_5y")
    if finviz.get("status") in {"ready", "stale"}:
        if forward_pe and 0 < forward_pe <= 35:
            finviz_points += 5
            reasons.append(f"Finviz valuation is reviewable: forward/ttm P/E {forward_pe:g}.")
        elif forward_pe and forward_pe > 60:
            finviz_points -= 4
            blockers.append(f"Finviz valuation is rich: P/E {forward_pe:g}.")
        if roe and roe >= 15:
            finviz_points += 5
        if margin and margin >= 10:
            finviz_points += 4
        if eps_5y and eps_5y >= 10:
            finviz_points += 3
        if debt_eq is not None and 0 <= debt_eq <= 1:
            finviz_points += 4
        elif debt_eq and debt_eq > 2:
            finviz_points -= 4
            blockers.append(f"Finviz debt/equity is elevated at {debt_eq:g}.")
    else:
        blockers.append("Finviz fundamentals are not in GRID for this ticker yet.")
    if finviz.get("freshness", {}).get("state") == "stale":
        finviz_points -= 5
        blockers.append("Finviz fundamentals are stale; refresh before making the call.")
    score += finviz_points
    cards.append({
        "source": "Finviz fundamentals",
        "state": "strong" if finviz_points >= 12 else "watch" if finviz_points > 0 else "missing" if finviz.get("status") == "unavailable" else "caution",
        "points": round(finviz_points, 1),
        "detail": f"{finviz.get('field_count', 0)} fields, {finviz.get('freshness', {}).get('label', 'unknown')}",
    })

    options_points = 0.0
    if options:
        options_points += 5
        pcr = options.get("put_call_ratio")
        if pcr and pcr > 1.2:
            options_points -= 2
            blockers.append(f"Options put/call ratio is elevated at {pcr:.2f}.")
    score += options_points
    cards.append({
        "source": "GRID options",
        "state": "watch" if options else "missing",
        "points": round(options_points, 1),
        "detail": f"Latest options date {options.get('date')}" if options else "No options_daily_signals row.",
    })

    signal_points = 0.0
    trusted = [row for row in signals.get("signal_sources", []) if row.get("trust_score", 0) >= 0.6]
    tv_alerts = signals.get("tradingview_signals", [])
    if trusted:
        signal_points += min(8, len(trusted) * 2)
    if tv_alerts:
        signal_points += min(6, len(tv_alerts) * 2)
    score += signal_points
    cards.append({
        "source": "GRID signals",
        "state": "strong" if signal_points >= 8 else "watch" if signal_points > 0 else "missing",
        "points": round(signal_points, 1),
        "detail": f"{len(trusted)} trusted signal rows, {len(tv_alerts)} TradingView alerts",
    })

    regime = signals.get("regime")
    if regime:
        rec = str(regime.get("grid_recommendation") or "").lower()
        if any(word in rec for word in ("risk", "hedge", "cash", "defensive", "reduce")):
            score -= 4
            blockers.append(f"Current GRID regime is cautious: {regime.get('grid_recommendation')}.")
        else:
            score += 2

    stale_sources = []
    for source in grid.get("source_freshness", []):
        if source.get("state") in {"stale", "missing"}:
            stale_sources.append(source.get("source"))
    if stale_sources:
        score -= min(12, len(stale_sources) * 3)
        blockers.append(f"Stale or missing source rows: {', '.join(stale_sources[:5])}.")

    score = max(0, min(100, round(score, 1)))
    if score >= 70:
        stance = "Deep review first"
        tone = "strong"
    elif score >= 45:
        stance = "Watchlist with checks"
        tone = "watch"
    elif score >= 25:
        stance = "Needs more evidence"
        tone = "caution"
    else:
        stance = "Do not surface hard yet"
        tone = "light"

    if not reasons and summary:
        reasons.append("Workbook prior exists, but GRID needs more current confirmation.")
    if not reasons:
        reasons.append("GRID does not have enough current evidence for this ticker yet.")

    return {
        "stance": stance,
        "tone": tone,
        "score": score,
        "cards": cards,
        "reasons": reasons[:5],
        "blockers": blockers[:6],
        "method": "Workbook prior plus GRID price, fundamentals, options, signal, regime, and freshness checks.",
    }


def _empty_grid_payload(message: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "message": message,
        "finviz": {
            "status": "unavailable",
            "source": "postgres",
            "freshness": _freshness_state(None),
            "field_count": 0,
            "stats": [],
            "fields": {},
            "error": message,
        },
        "grid": {
            "status": "unavailable",
            "price_history": [],
            "metrics": {},
            "features": [],
            "freshness": _freshness_state(None),
            "source_freshness": [],
        },
        "options": None,
        "signals": {"signal_sources": [], "tradingview_signals": [], "regime": None},
    }


def _load_grid_payload(
    ticker: str,
    *,
    refresh_finviz: bool = False,
    engine: Any | None = None,
    grid_days: int = 365,
    include_price_history: bool = False,
    chart_points: int | None = None,
    feature_limit: int = 8,
) -> dict[str, Any]:
    try:
        engine = engine or get_db_engine()
        finviz = _get_finviz_profile(engine, ticker, refresh=refresh_finviz)
        grid = _grid_market_context(
            engine,
            ticker,
            days=grid_days,
            include_price_history=include_price_history,
            max_points=chart_points,
            feature_limit=feature_limit,
        )
        grid["source_freshness"] = _source_freshness(
            engine,
            [
                "yfinance",
                FINVIZ_SOURCE_NAME,
                "TradingView",
                "Social_Smart_Money",
                "SEC_INSIDER",
                "Unusual_Whales",
            ],
        )
        options = _latest_options_context(engine, ticker)
        signals = _latest_signal_context(engine, ticker)
        return {
            "status": "ready",
            "finviz": finviz,
            "grid": grid,
            "options": options,
            "signals": signals,
        }
    except Exception as exc:
        log.debug("Dad GRID payload failed for {t}: {e}", t=ticker, e=str(exc))
        return _empty_grid_payload(str(exc))


def _connect_duckdb(db_path: Path):
    try:
        import duckdb
    except Exception as exc:  # pragma: no cover - depends on deploy env
        raise RuntimeError("duckdb is not installed in this GRID environment") from exc
    return duckdb.connect(str(db_path), read_only=True)


def _empty_workbook_context(ticker: str, db_path: Path, *, attached: bool, status: str, message: str) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "status": status,
        "message": message,
        "source": {"attached": attached, "db_path": str(db_path)},
        "summary": None,
        "workbook": {"files": [], "sheets": [], "evidence": []},
        "source_lanes": [],
        "dad_stats": _dad_stat_cards([], [], []),
        "fit_signals": [],
        "page": {"limit": 0, "offset": 0, "returned": 0},
    }


def _load_workbook_context(
    ticker: str,
    *,
    evidence_limit: int = MAX_EVIDENCE_ROWS,
    evidence_offset: int = 0,
    file_limit: int = MAX_FILE_ROWS,
) -> dict[str, Any]:
    db_path = _research_db_path()
    limit = max(0, min(evidence_limit, 100))
    offset = max(0, evidence_offset)
    files_limit = max(0, min(file_limit, 50))

    if not db_path.exists():
        return _empty_workbook_context(
            ticker,
            db_path,
            attached=False,
            status="unavailable",
            message="Workbook DuckDB database is not attached yet.",
        )

    try:
        conn = _connect_duckdb(db_path)
    except RuntimeError as exc:
        return _empty_workbook_context(
            ticker,
            db_path,
            attached=False,
            status="unavailable",
            message=str(exc),
        )

    try:
        summary_row = conn.execute(
            """
            SELECT ticker, mentions, file_count, sheet_count, evidence_score, source_types
            FROM ticker_summary
            WHERE ticker = ?
            """,
            [ticker],
        ).fetchone()
        summary = None
        if summary_row:
            summary = {
                "ticker": summary_row[0],
                "mentions": summary_row[1],
                "file_count": summary_row[2],
                "sheet_count": summary_row[3],
                "evidence_score": summary_row[4],
                "source_types": summary_row[5],
            }

        evidence_rows = conn.execute(
            """
            SELECT
              source_type,
              evidence_text,
              context_score,
              rel_path,
              sheet_name,
              cell_ref,
              row_context,
              col_header
            FROM ticker_best_evidence
            WHERE ticker = ?
            ORDER BY evidence_rank
            LIMIT ? OFFSET ?
            """,
            [ticker, limit, offset],
        ).fetchall()

        file_rows = conn.execute(
            """
            SELECT rel_path, mentions, score
            FROM ticker_file_summary
            WHERE ticker = ?
            ORDER BY score DESC, mentions DESC, rel_path
            LIMIT ?
            """,
            [ticker, files_limit],
        ).fetchall()

        sheet_rows = conn.execute(
            """
            SELECT rel_path, sheet_name, mentions, score
            FROM ticker_sheet_summary
            WHERE ticker = ?
            ORDER BY score DESC, mentions DESC, rel_path, sheet_name
            LIMIT ?
            """,
            [ticker, files_limit],
        ).fetchall()
    except Exception as exc:
        log.warning("Dad ticker workbook lookup failed for {ticker}: {error}", ticker=ticker, error=str(exc))
        return _empty_workbook_context(
            ticker,
            db_path,
            attached=True,
            status="unavailable",
            message="Workbook DuckDB database is present but not queryable yet.",
        ) | {"error": str(exc)}
    finally:
        conn.close()

    evidence = [
        {
            "source_type": row[0],
            "evidence_text": _shorten(row[1]),
            "context_score": float(row[2] or 0),
            "file": row[3],
            "sheet": row[4],
            "cell": row[5],
            "row_context": _shorten(row[6], 220),
            "column_header": _shorten(row[7], 120),
        }
        for row in evidence_rows
    ]
    files = [
        {"file": row[0], "mentions": int(row[1] or 0), "score": float(row[2] or 0)}
        for row in file_rows
    ]
    sheets = [
        {"file": row[0], "sheet": row[1], "mentions": int(row[2] or 0), "score": float(row[3] or 0)}
        for row in sheet_rows
    ]

    return {
        "ticker": ticker,
        "status": "ready" if summary else "not_found",
        "message": None,
        "source": {"attached": True, "db_path": str(db_path)},
        "summary": summary,
        "workbook": {"files": files, "sheets": sheets, "evidence": evidence},
        "source_lanes": _lane_counts(evidence, files, sheets),
        "dad_stats": _dad_stat_cards(evidence, files, sheets),
        "fit_signals": _fit_signals(summary, evidence),
        "page": {"limit": limit, "offset": offset, "returned": len(evidence)},
    }


def _response_risks_and_actions(
    workbook: dict[str, Any],
    grid_payload: dict[str, Any],
    decision_stack: dict[str, Any],
) -> tuple[list[str], list[str]]:
    summary = workbook.get("summary")
    risks = [
        "Workbook footprint is historical evidence, not a live buy/sell recommendation.",
        "Current price, fundamentals, news, and liquidity still need a fresh market check.",
    ]
    next_actions = [
        "Compare the workbook evidence against a current chart and fundamentals pass.",
        "Check whether Dad's workbook language is buy, watch, hold, or sell before acting.",
    ]

    if workbook.get("status") == "unavailable":
        risks.insert(0, workbook.get("message") or "Workbook research is unavailable.")
        next_actions.insert(0, "Attach or regenerate the workbook DuckDB research database.")
    elif summary:
        next_actions.insert(0, "Open the top workbook evidence rows and confirm the context.")
        if int(summary.get("file_count") or 0) <= 1:
            risks.append("Evidence is concentrated in one workbook, so treat it as low confidence.")
    else:
        risks.append("Regex extraction found no confident workbook footprint for this ticker.")
        next_actions.insert(0, "Try the company name or related ticker if this was renamed, delisted, or crypto-like.")

    if grid_payload["finviz"].get("status") in {"unavailable", "stale"}:
        next_actions.append("Use Refresh Finviz to populate or update cached fundamentals for this ticker.")
    for blocker in decision_stack.get("blockers", []):
        if blocker not in risks:
            risks.append(blocker)
    return risks, next_actions


def _assemble_dad_response(
    ticker: str,
    workbook: dict[str, Any],
    grid_payload: dict[str, Any],
    *,
    timings: dict[str, float] | None = None,
    compact: bool = True,
) -> dict[str, Any]:
    summary = workbook.get("summary")
    gold = _gold_from_summary(summary)
    decision_stack = _grid_decision_stack(
        summary,
        gold,
        grid_payload["grid"],
        grid_payload["finviz"],
        grid_payload["options"],
        grid_payload["signals"],
    )
    risks, next_actions = _response_risks_and_actions(workbook, grid_payload, decision_stack)
    timings = timings or {}
    return {
        "ticker": ticker,
        "status": workbook.get("status", "unavailable"),
        "payload_mode": "compact" if compact else "detail",
        "gold": gold,
        "decision_stack": decision_stack,
        "source": {
            "attached": bool(workbook.get("source", {}).get("attached")),
            "db_path": workbook.get("source", {}).get("db_path") if not compact else None,
        },
        "message": workbook.get("message"),
        "summary": summary,
        "workbook": workbook.get("workbook", {"files": [], "sheets": [], "evidence": []}),
        "source_lanes": workbook.get("source_lanes", []),
        "dad_stats": workbook.get("dad_stats", []),
        "finviz": _compact_finviz(grid_payload["finviz"], include_fields=not compact),
        "grid_data": _compact_grid(
            grid_payload["grid"],
            include_price_history=not compact,
            max_points=DEFAULT_CHART_POINTS if not compact else None,
        ),
        "options": grid_payload["options"],
        "signals": _compact_signals(grid_payload["signals"], include_rows=not compact),
        "tradingview": _tradingview_payload(ticker),
        "fit_signals": workbook.get("fit_signals", []),
        "risks": risks,
        "next_actions": next_actions,
        "detail_urls": _detail_urls(ticker),
        "hydration": {
            "compact": True,
            "evidence": "inline_sample" if compact else "complete",
            "chart": "detail_endpoint" if compact else "inline",
            "finviz": "summary" if compact else "complete",
            "options": "latest" if compact else "complete",
        },
        "performance": {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)},
        "cache": {"hit": False, "ttl_seconds": SUMMARY_CACHE_TTL_SECONDS},
    }


def _build_compact_dad_response(ticker: str, *, refresh_finviz: bool = False, use_cache: bool = True) -> dict[str, Any]:
    timings: dict[str, float] = {}
    engine = get_db_engine()
    db_path = _research_db_path()
    if use_cache and not refresh_finviz:
        cached = _timed("cache_read", timings, lambda: _read_summary_cache(engine, ticker, db_path))
        if cached:
            cached.setdefault("performance", {})["cache_read_ms"] = timings["cache_read"]
            return cached

    workbook = _timed(
        "workbook_compact",
        timings,
        lambda: _load_workbook_context(
            ticker,
            evidence_limit=COMPACT_EVIDENCE_ROWS,
            file_limit=COMPACT_FILE_ROWS,
        ),
    )
    grid_payload = _timed(
        "grid_compact",
        timings,
        lambda: _load_grid_payload(
            ticker,
            refresh_finviz=refresh_finviz,
            engine=engine,
            include_price_history=False,
            chart_points=None,
            feature_limit=8,
        ),
    )
    payload = _assemble_dad_response(ticker, workbook, grid_payload, timings=timings, compact=True)
    _timed("cache_write", timings, lambda: _write_summary_cache(engine, ticker, db_path, payload, timings))
    payload["performance"] = {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)}
    _log_slow_ticker(ticker, timings, route="gold_compact")
    return payload


def _sse_event(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"


def _build_evidence_payload(
    ticker: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    workbook = _timed(
        "workbook_evidence",
        timings,
        lambda: _load_workbook_context(
            ticker,
            evidence_limit=limit,
            evidence_offset=offset,
            file_limit=MAX_FILE_ROWS,
        ),
    )
    payload = {
        "ticker": ticker,
        "status": workbook.get("status", "unavailable"),
        "summary": workbook.get("summary"),
        "workbook": workbook.get("workbook", {"files": [], "sheets": [], "evidence": []}),
        "source_lanes": workbook.get("source_lanes", []),
        "dad_stats": workbook.get("dad_stats", []),
        "fit_signals": workbook.get("fit_signals", []),
        "page": workbook.get("page", {"limit": limit, "offset": offset, "returned": 0}),
        "performance": {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)},
    }
    return payload


def _build_chart_payload(
    ticker: str,
    *,
    range_name: str = "1Y",
    points: int = DEFAULT_CHART_POINTS,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    bounded_points = max(20, min(points, MAX_CHART_POINTS))
    engine = get_db_engine()
    grid = _timed(
        "grid_chart",
        timings,
        lambda: _grid_market_context(
            engine,
            ticker,
            days=_range_to_days(range_name),
            include_price_history=True,
            max_points=bounded_points,
            feature_limit=12,
        ),
    )
    grid["source_freshness"] = _timed(
        "source_freshness",
        timings,
        lambda: _source_freshness(
            engine,
            [
                "yfinance",
                FINVIZ_SOURCE_NAME,
                "TradingView",
                "Social_Smart_Money",
                "SEC_INSIDER",
                "Unusual_Whales",
            ],
        ),
    )
    signals = _timed("signals", timings, lambda: _latest_signal_context(engine, ticker))
    compact_grid = _compact_grid(
        grid,
        include_price_history=True,
        max_points=bounded_points,
    )
    return {
        "ticker": ticker,
        "status": compact_grid.get("status", "missing"),
        "payload_mode": "chart",
        "range": (range_name or "1Y").upper(),
        "points": bounded_points,
        "grid_data": compact_grid,
        "price_history": compact_grid.get("price_history", []),
        "metrics": compact_grid.get("metrics", {}),
        "features": compact_grid.get("features", []),
        "source_freshness": compact_grid.get("source_freshness", []),
        "tradingview_signals": signals.get("tradingview_signals", []),
        "regime": signals.get("regime"),
        "signals": _compact_signals(signals, include_rows=True),
        "performance": {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)},
    }


def _build_finviz_payload(
    ticker: str,
    *,
    refresh_finviz: bool = False,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    engine = get_db_engine()
    finviz = _timed(
        "finviz",
        timings,
        lambda: _compact_finviz(
            _get_finviz_profile(engine, ticker, refresh=refresh_finviz),
            include_fields=True,
        ),
    )
    return {
        "ticker": ticker,
        "status": finviz.get("status", "unavailable"),
        "payload_mode": "finviz",
        "finviz": finviz,
        "performance": {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)},
    }


def _build_options_payload(
    ticker: str,
    *,
    days: int = 90,
    limit: int = 90,
) -> dict[str, Any]:
    timings: dict[str, float] = {}
    engine = get_db_engine()
    options = _timed(
        "options",
        timings,
        lambda: _options_history_context(engine, ticker, days=days, limit=limit),
    )
    return {
        "ticker": ticker,
        "status": options.get("status", "missing"),
        "payload_mode": "options",
        "options": options,
        "latest": options.get("latest"),
        "history": options.get("history", []),
        "freshness": options.get("freshness", _freshness_state(None)),
        "performance": {"timings_ms": timings, "total_ms": round(sum(timings.values()), 1)},
    }


@router.get("/ticker/{ticker}/gold")
def get_dad_ticker_gold(
    ticker: str,
    refresh_finviz: bool = Query(
        default=False,
        description="Explicitly refresh the ticker's Finviz snapshot before returning cached rows.",
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the compact first-screen Dad ticker payload."""
    ticker_upper = _normalize_ticker(ticker)
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}
    return _build_compact_dad_response(
        ticker_upper,
        refresh_finviz=refresh_finviz,
        use_cache=not refresh_finviz,
    )


@router.get("/ticker/{ticker}/evidence")
def get_dad_ticker_evidence(
    ticker: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return paged workbook evidence rows and workbook source lanes."""
    ticker_upper = _normalize_ticker(ticker)
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}
    return _build_evidence_payload(ticker_upper, limit=limit, offset=offset)


@router.get("/ticker/{ticker}/chart")
def get_dad_ticker_chart(
    ticker: str,
    range: str = Query("1Y", description="Chart range: 1M, 3M, 6M, 1Y, 2Y, 5Y, 10Y, MAX."),
    points: int = Query(DEFAULT_CHART_POINTS, ge=20, le=MAX_CHART_POINTS),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return bounded price history and chart-adjacent GRID signals."""
    ticker_upper = _normalize_ticker(ticker)
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}
    return _build_chart_payload(ticker_upper, range_name=range, points=points)


@router.get("/ticker/{ticker}/finviz")
def get_dad_ticker_finviz(
    ticker: str,
    refresh_finviz: bool = Query(False),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full cached Finviz snapshot, optionally refreshing stale rows."""
    ticker_upper = _normalize_ticker(ticker)
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}
    return _build_finviz_payload(ticker_upper, refresh_finviz=refresh_finviz)


@router.get("/ticker/{ticker}/options")
def get_dad_ticker_options(
    ticker: str,
    days: int = Query(90, ge=1, le=365),
    limit: int = Query(90, ge=1, le=250),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return bounded options daily signal history for the ticker."""
    ticker_upper = _normalize_ticker(ticker)
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}
    return _build_options_payload(ticker_upper, days=days, limit=limit)


@router.get("/ticker/{ticker}/gold/stream")
def stream_dad_ticker_gold(
    ticker: str,
    refresh_finviz: bool = Query(False),
    _token: str = Depends(require_auth),
) -> StreamingResponse:
    """Stream compact payload first, then hydrate evidence, chart, Finviz, and options."""
    ticker_upper = _normalize_ticker(ticker)

    def generate():
        if not ticker_upper:
            yield _sse_event("error", {"ticker": "", "message": "Enter a ticker symbol."})
            return
        try:
            yield _sse_event("start", {"ticker": ticker_upper})
            yield _sse_event(
                "compact",
                _build_compact_dad_response(
                    ticker_upper,
                    refresh_finviz=refresh_finviz,
                    use_cache=not refresh_finviz,
                ),
            )
            yield _sse_event("evidence", _build_evidence_payload(ticker_upper, limit=50, offset=0))
            yield _sse_event("chart", _build_chart_payload(ticker_upper, range_name="1Y", points=DEFAULT_CHART_POINTS))
            yield _sse_event("finviz", _build_finviz_payload(ticker_upper, refresh_finviz=refresh_finviz))
            yield _sse_event("options", _build_options_payload(ticker_upper, days=90, limit=90))
            yield _sse_event("done", {"ticker": ticker_upper})
        except Exception as exc:
            log.exception("Dad ticker stream failed for {t}", t=ticker_upper)
            yield _sse_event("error", {"ticker": ticker_upper, "message": str(exc)})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _get_dad_ticker_gold_legacy(
    ticker: str,
    refresh_finviz: bool = Query(
        default=False,
        description="Explicitly refresh the ticker's Finviz snapshot before returning cached rows.",
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return Dad-facing workbook evidence for a ticker.

    This endpoint is evidence retrieval, not investment advice. It intentionally
    separates workbook memory from current market/fundamental checks.
    """
    ticker_upper = _normalize_ticker(ticker)
    db_path = _research_db_path()
    if not ticker_upper:
        return {"ticker": "", "status": "invalid", "message": "Enter a ticker symbol."}

    grid_payload = _load_grid_payload(ticker_upper, refresh_finviz=refresh_finviz)
    missing_gold = _gold_from_summary(None)
    missing_decision = _grid_decision_stack(
        None,
        missing_gold,
        grid_payload["grid"],
        grid_payload["finviz"],
        grid_payload["options"],
        grid_payload["signals"],
    )

    if not db_path.exists():
        return {
            "ticker": ticker_upper,
            "status": "unavailable",
            "gold": missing_gold,
            "decision_stack": missing_decision,
            "source": {"db_path": str(db_path), "attached": False},
            "message": "Workbook DuckDB database is not attached yet.",
            "workbook": {"files": [], "sheets": [], "evidence": []},
            "source_lanes": [],
            "dad_stats": _dad_stat_cards([], [], []),
            "finviz": grid_payload["finviz"],
            "grid_data": grid_payload["grid"],
            "options": grid_payload["options"],
            "signals": grid_payload["signals"],
            "tradingview": _tradingview_payload(ticker_upper),
            "fit_signals": [],
            "risks": [
                "Workbook research database has not been generated or mounted.",
                *missing_decision.get("blockers", []),
            ],
            "next_actions": [
                "Run the workbook DuckDB extraction and set GRID_DAD_STOCK_RESEARCH_DB if the path differs.",
                "Use Refresh Finviz when you need to populate or update cached fundamentals for this ticker.",
                "Use the GRID decision stack as a temporary market/fundamental view.",
            ],
        }

    try:
        conn = _connect_duckdb(db_path)
    except RuntimeError as exc:
        return {
            "ticker": ticker_upper,
            "status": "unavailable",
            "gold": missing_gold,
            "decision_stack": missing_decision,
            "source": {"db_path": str(db_path), "attached": False},
            "message": str(exc),
            "workbook": {"files": [], "sheets": [], "evidence": []},
            "source_lanes": [],
            "dad_stats": _dad_stat_cards([], [], []),
            "finviz": grid_payload["finviz"],
            "grid_data": grid_payload["grid"],
            "options": grid_payload["options"],
            "signals": grid_payload["signals"],
            "tradingview": _tradingview_payload(ticker_upper),
            "fit_signals": [],
            "risks": ["DuckDB is missing from the Python environment.", *missing_decision.get("blockers", [])],
            "next_actions": [
                "Install GRID requirements so duckdb is available.",
                "Use Refresh Finviz when you need to populate or update cached fundamentals for this ticker.",
                "Use the GRID decision stack as a temporary market/fundamental view.",
            ],
        }

    try:
        summary_row = conn.execute(
            """
            SELECT ticker, mentions, file_count, sheet_count, evidence_score, source_types
            FROM ticker_summary
            WHERE ticker = ?
            """,
            [ticker_upper],
        ).fetchone()
        summary = None
        if summary_row:
            summary = {
                "ticker": summary_row[0],
                "mentions": summary_row[1],
                "file_count": summary_row[2],
                "sheet_count": summary_row[3],
                "evidence_score": summary_row[4],
                "source_types": summary_row[5],
            }

        evidence_rows = conn.execute(
            """
            SELECT
              source_type,
              evidence_text,
              context_score,
              rel_path,
              sheet_name,
              cell_ref,
              row_context,
              col_header
            FROM ticker_best_evidence
            WHERE ticker = ?
            ORDER BY evidence_rank
            LIMIT ?
            """,
            [ticker_upper, MAX_EVIDENCE_ROWS],
        ).fetchall()

        file_rows = conn.execute(
            """
            SELECT rel_path, mentions, score
            FROM ticker_file_summary
            WHERE ticker = ?
            ORDER BY score DESC, mentions DESC, rel_path
            LIMIT ?
            """,
            [ticker_upper, MAX_FILE_ROWS],
        ).fetchall()

        sheet_rows = conn.execute(
            """
            SELECT rel_path, sheet_name, mentions, score
            FROM ticker_sheet_summary
            WHERE ticker = ?
            ORDER BY score DESC, mentions DESC, rel_path, sheet_name
            LIMIT ?
            """,
            [ticker_upper, MAX_FILE_ROWS],
        ).fetchall()
    except Exception as exc:
        log.warning("Dad ticker lookup failed for {ticker}: {error}", ticker=ticker_upper, error=str(exc))
        return {
            "ticker": ticker_upper,
            "status": "unavailable",
            "gold": missing_gold,
            "decision_stack": missing_decision,
            "source": {"db_path": str(db_path), "attached": True},
            "message": "Workbook DuckDB database is present but not queryable yet.",
            "workbook": {"files": [], "sheets": [], "evidence": []},
            "source_lanes": [],
            "dad_stats": _dad_stat_cards([], [], []),
            "finviz": grid_payload["finviz"],
            "grid_data": grid_payload["grid"],
            "options": grid_payload["options"],
            "signals": grid_payload["signals"],
            "tradingview": _tradingview_payload(ticker_upper),
            "fit_signals": [],
            "risks": [str(exc), *missing_decision.get("blockers", [])],
            "next_actions": [
                "Wait for extraction to finish, then refresh this ticker.",
                "Use Refresh Finviz when you need to populate or update cached fundamentals for this ticker.",
                "Use the GRID decision stack as a temporary market/fundamental view.",
            ],
        }
    finally:
        conn.close()

    evidence = [
        {
            "source_type": row[0],
            "evidence_text": _shorten(row[1]),
            "context_score": float(row[2] or 0),
            "file": row[3],
            "sheet": row[4],
            "cell": row[5],
            "row_context": _shorten(row[6], 220),
            "column_header": _shorten(row[7], 120),
        }
        for row in evidence_rows
    ]
    files = [
        {"file": row[0], "mentions": int(row[1] or 0), "score": float(row[2] or 0)}
        for row in file_rows
    ]
    sheets = [
        {"file": row[0], "sheet": row[1], "mentions": int(row[2] or 0), "score": float(row[3] or 0)}
        for row in sheet_rows
    ]

    status = "ready" if summary else "not_found"
    risks = [
        "Workbook footprint is historical evidence, not a live buy/sell recommendation.",
        "Current price, fundamentals, news, and liquidity still need a fresh market check.",
    ]
    if summary and int(summary.get("file_count") or 0) <= 1:
        risks.append("Evidence is concentrated in one workbook, so treat it as low confidence.")
    if not summary:
        risks.append("Regex extraction found no confident workbook footprint for this ticker.")

    next_actions = [
        "Compare the workbook evidence against a current chart and fundamentals pass.",
        "Check whether Dad's workbook language is buy, watch, hold, or sell before acting.",
    ]
    if summary:
        next_actions.insert(0, "Open the top workbook evidence rows and confirm the context.")
    else:
        next_actions.insert(0, "Try the company name or related ticker if this was renamed, delisted, or crypto-like.")
    if grid_payload["finviz"].get("status") in {"unavailable", "stale"}:
        next_actions.append("Use Refresh Finviz to populate or update cached fundamentals for this ticker.")

    gold = _gold_from_summary(summary)
    decision_stack = _grid_decision_stack(
        summary,
        gold,
        grid_payload["grid"],
        grid_payload["finviz"],
        grid_payload["options"],
        grid_payload["signals"],
    )
    for blocker in decision_stack.get("blockers", []):
        if blocker not in risks:
            risks.append(blocker)

    return {
        "ticker": ticker_upper,
        "status": status,
        "gold": gold,
        "decision_stack": decision_stack,
        "source": {"db_path": str(db_path), "attached": True},
        "summary": summary,
        "workbook": {
            "files": files,
            "sheets": sheets,
            "evidence": evidence,
        },
        "source_lanes": _lane_counts(evidence, files, sheets),
        "dad_stats": _dad_stat_cards(evidence, files, sheets),
        "finviz": grid_payload["finviz"],
        "grid_data": grid_payload["grid"],
        "options": grid_payload["options"],
        "signals": grid_payload["signals"],
        "tradingview": _tradingview_payload(ticker_upper),
        "fit_signals": _fit_signals(summary, evidence),
        "risks": risks,
        "next_actions": next_actions,
    }
