"""Read-only GRID database reads (pre-registration §2.1, §6, §9).

Three SELECTs, nothing else: candidate ``SEC_INSIDER`` BUY rows, market caps
from ``ticker_metrics_daily``, and insider-feed freshness. The engine comes
from ``paper_log.gex_levels.db.build_readonly_engine`` (every physical
connection pinned ``default_transaction_read_only = on``), verified with
``assert_read_only`` before the first query.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from sqlalchemy import text

from paper_log.trade_edge.config import CAP_LOOKBACK_DAYS, SERIES_PATTERN, SOURCE_NAME

CANDIDATES_SQL = """
SELECT r.series_id, r.obs_date, r.pull_timestamp, r.raw_payload
FROM raw_series r
JOIN source_catalog s ON s.id = r.source_id
WHERE s.name = :source
  AND r.series_id LIKE :pattern
  AND r.pull_status = 'SUCCESS'
  AND r.pull_timestamp >= :since
ORDER BY r.pull_timestamp, r.id
"""

MARKET_CAP_SQL = """
SELECT ticker, obs_date, market_cap_usd, source
FROM ticker_metrics_daily
WHERE ticker = ANY(:tickers)
  AND obs_date BETWEEN :lo AND :hi
  AND market_cap_usd > 0
ORDER BY ticker, obs_date DESC
"""

FRESHNESS_SQL = """
SELECT
  (SELECT max(r.pull_timestamp) FROM raw_series r JOIN source_catalog s ON s.id = r.source_id
    WHERE s.name = :source AND r.series_id LIKE :pattern AND r.pull_status = 'SUCCESS'
      AND r.pull_timestamp > now() - interval '30 days') AS latest_insider_buy_pull,
  (SELECT max(created_at) FROM insider_trades) AS latest_insider_trades_created_at,
  (SELECT count(*) FROM insider_trades WHERE created_at > now() - interval '24 hours')
    AS insider_trades_rows_24h
"""


def _payload(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (str, bytes)):
        try:
            return json.loads(raw)
        except ValueError:
            return {}
    return {}


def candidate_rows(conn, since: datetime) -> list[dict]:
    rows = conn.execute(
        text(CANDIDATES_SQL), {"source": SOURCE_NAME, "pattern": SERIES_PATTERN, "since": since}
    ).fetchall()
    out = []
    for series_id, obs_date, pulled, raw in rows:
        out.append({"series_id": series_id, "obs_date": obs_date, "pull_timestamp": pulled,
                    "payload": _payload(raw)})
    return out


def group_accessions(rows: Iterable[dict]) -> dict[str, dict]:
    """accession -> first ingest, filing date/URL, ticker and GRID's own lines."""
    out: dict[str, dict] = {}
    for row in rows:
        p = row["payload"]
        acc = (p.get("accession") or "").strip()
        if not acc:
            continue
        entry = out.setdefault(
            acc,
            {
                "accession": acc,
                "first_ingest_at": row["pull_timestamp"],
                "filing_date": None,
                "filing_url": "",
                "ticker": "",
                "db_lines": [],
            },
        )
        if row["pull_timestamp"] < entry["first_ingest_at"]:
            entry["first_ingest_at"] = row["pull_timestamp"]
        entry["filing_date"] = entry["filing_date"] or (p.get("filing_date") or None)
        entry["filing_url"] = entry["filing_url"] or (p.get("filing_url") or "")
        entry["ticker"] = entry["ticker"] or (p.get("ticker") or "")
        entry["db_lines"].append(
            {
                "insider_name": p.get("insider_name") or "",
                "trans_date": p.get("transaction_date") or (row["obs_date"].isoformat() if row["obs_date"] else None),
                "code": p.get("transaction_code") or "",
                "acq_disp": "A",  # §2.1 grid_db fallback: not carried by GRID
                "shares": _num(p.get("shares")),
                "price": _num(p.get("price")),
                "is_derivative": bool(p.get("is_derivative", False)),
                "equity_swap": False,
                "is_10b5_1": bool(p.get("is_10b5_1", False)),
                "security_title": "",
            }
        )
    return out


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def market_caps(conn, tickers: Iterable[str], on: date) -> dict[str, dict]:
    tickers = sorted({t for t in tickers if t})
    if not tickers:
        return {}
    rows = conn.execute(
        text(MARKET_CAP_SQL),
        {"tickers": tickers, "lo": on - timedelta(days=CAP_LOOKBACK_DAYS), "hi": on},
    ).fetchall()
    out: dict[str, dict] = {}
    for ticker, obs_date, mcap, src in rows:
        if ticker not in out:  # ordered newest first
            out[ticker] = {"market_cap_usd": float(mcap), "as_of": obs_date.isoformat(),
                           "source": f"grid:ticker_metrics_daily:{src}"}
    return out


def freshness(conn) -> dict:
    row = conn.execute(text(FRESHNESS_SQL), {"source": SOURCE_NAME, "pattern": SERIES_PATTERN}).fetchone()
    latest_pull, latest_created, rows_24h = row

    def iso(v):
        return v.isoformat() if v is not None else None

    return {
        "latest_insider_buy_pull": iso(latest_pull),
        "latest_insider_trades_created_at": iso(latest_created),
        "insider_trades_rows_24h": int(rows_24h or 0),
    }
