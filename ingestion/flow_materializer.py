"""
GRID Flow Materializer — transforms signal_sources and raw_series into
the dedicated query-friendly tables that the flows API expects.

The flows API (api/routers/flows.py) queries insider_trades,
congressional_trades, dark_pool_weekly, etf_flows, and
junction_point_readings directly. Those tables are materialized views
of data already stored in signal_sources (JSONB) and raw_series.

This module provides idempotent sync functions that read from the
source tables, parse the JSON payloads, and upsert into the
query-friendly target tables using ON CONFLICT DO UPDATE.

Entry point: sync_all(engine) runs all materializers and returns a
summary dict with row counts per table plus any errors encountered.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Amount range mapping (mirrors congressional.py / dollar_flows.py) ────

AMOUNT_RANGES: dict[str, tuple[int, int]] = {
    "A": (0, 1_000),
    "B": (1_001, 15_000),
    "C": (15_001, 50_000),
    "D": (50_001, 100_000),
    "E": (100_001, 250_000),
    "F": (250_001, 500_000),
    "G": (500_001, 1_000_000),
    "H": (1_000_001, 5_000_000),
    "I": (5_000_001, 25_000_000),
    "J": (25_000_001, 50_000_000),
}

# FRED series tracked for junction point readings
JUNCTION_SERIES: dict[str, str] = {
    "WALCL": "fed_balance_sheet",
    "RRPONTSYD": "reverse_repo",
    "WTREGEN": "treasury_general_account",
    "M2SL": "m2_money_supply",
    "TOTBKCR": "bank_credit",
    "H8B1023NCBCMG": "bank_credit_alt",
    "BAMLH0A0HYM2": "hy_spread",
    "BAMLC0A0CM": "ig_spread",
    "BOPGTB": "trade_balance",
    "UMCSENT": "consumer_sentiment",
}


# ── DDL — create target tables if missing ────────────────────────────────

_DDL_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS insider_trades (
        id          BIGSERIAL PRIMARY KEY,
        ticker      TEXT NOT NULL,
        trade_date  DATE NOT NULL,
        insider_name TEXT NOT NULL,
        trade_type  TEXT NOT NULL,
        shares      DOUBLE PRECISION,
        value       DOUBLE PRECISION,
        price       DOUBLE PRECISION,
        insider_title TEXT,
        is_unusual  BOOLEAN DEFAULT FALSE,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, trade_date, insider_name, trade_type)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker ON insider_trades (ticker, trade_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_insider_trades_value ON insider_trades (value DESC NULLS LAST)",
    """
    CREATE TABLE IF NOT EXISTS congressional_trades (
        id                BIGSERIAL PRIMARY KEY,
        ticker            TEXT NOT NULL,
        disclosure_date   DATE NOT NULL,
        representative    TEXT NOT NULL,
        transaction_type  TEXT NOT NULL,
        amount            TEXT,
        amount_midpoint   DOUBLE PRECISION,
        chamber           TEXT,
        party             TEXT,
        state             TEXT,
        created_at        TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, disclosure_date, representative, transaction_type)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_congressional_ticker ON congressional_trades (ticker, disclosure_date DESC)",
    """
    CREATE TABLE IF NOT EXISTS dark_pool_weekly (
        id           BIGSERIAL PRIMARY KEY,
        ticker       TEXT NOT NULL,
        report_date  DATE NOT NULL,
        short_volume DOUBLE PRECISION,
        total_volume DOUBLE PRECISION,
        trade_count  DOUBLE PRECISION,
        short_pct    DOUBLE PRECISION,
        spike_ratio  DOUBLE PRECISION,
        created_at   TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, report_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_dark_pool_ticker ON dark_pool_weekly (ticker, report_date DESC)",
    """
    CREATE TABLE IF NOT EXISTS etf_flows (
        id          BIGSERIAL PRIMARY KEY,
        ticker      TEXT NOT NULL,
        flow_date   DATE NOT NULL,
        flow_value  DOUBLE PRECISION NOT NULL,
        source      TEXT DEFAULT 'proxy',
        confidence  TEXT DEFAULT 'estimated',
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, flow_date, source)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_etf_flows_ticker ON etf_flows (ticker, flow_date DESC)",
    """
    CREATE TABLE IF NOT EXISTS junction_point_readings (
        id          BIGSERIAL PRIMARY KEY,
        series_key  TEXT NOT NULL,
        label       TEXT NOT NULL,
        obs_date    DATE NOT NULL,
        value       DOUBLE PRECISION,
        change_1d   DOUBLE PRECISION,
        change_1w   DOUBLE PRECISION,
        change_1m   DOUBLE PRECISION,
        z_score_2y  DOUBLE PRECISION,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (series_key, obs_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_junction_key ON junction_point_readings (series_key, obs_date DESC)",
]


def _ensure_tables(engine: Engine) -> None:
    """Create all materialized target tables if they do not exist."""
    with engine.begin() as conn:
        for stmt in _DDL_STATEMENTS:
            conn.execute(text(stmt.strip()))
    log.debug("flow_materializer: target tables ensured")


# ── Helpers ──────────────────────────────────────────────────────────────

def _parse_signal_value(raw: Any) -> dict:
    """Safely parse a signal_value field that may be JSONB, str, or None.

    Parameters:
        raw: The signal_value column value.

    Returns:
        Parsed dict (empty dict on failure).
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _midpoint_for_range(amount_range: str) -> float:
    """Compute midpoint dollar value from an amount range code or string.

    Parameters:
        amount_range: Code like 'A'-'J' or dollar string like '$1,001 - $15,000'.

    Returns:
        Midpoint as float, or 0.0 if unparseable.
    """
    if not amount_range:
        return 0.0
    code = amount_range.strip().upper()
    if code in AMOUNT_RANGES:
        lo, hi = AMOUNT_RANGES[code]
        return (lo + hi) / 2.0
    # Try parsing dollar range string
    import re
    nums = re.findall(r"[\d]+", amount_range.replace(",", ""))
    if len(nums) >= 2:
        try:
            return (float(nums[0]) + float(nums[1])) / 2.0
        except ValueError:
            pass
    return 0.0


# ── Sync: insider_trades ─────────────────────────────────────────────────

def sync_insider_trades(engine: Engine) -> int:
    """Read signal_sources WHERE source_type='insider', parse JSONB, upsert into insider_trades.

    The signal_value JSON contains: shares, price, value, insider_title,
    is_unusual_size (as stored by InsiderFilingsPuller._emit_signal).

    Returns:
        Number of rows upserted.
    """
    _ensure_tables(engine)
    rows_upserted = 0

    with engine.begin() as conn:
        src_rows = conn.execute(text(
            "SELECT ticker, signal_date, source_id, signal_type, signal_value "
            "FROM signal_sources WHERE source_type = 'insider' "
            "AND signal_type NOT LIKE '%CLUSTER%' "
            "ORDER BY signal_date DESC LIMIT 5000"
        )).fetchall()

        if not src_rows:
            log.info("flow_materializer: no insider signals found")
            return 0

        batch: list[dict] = []
        for r in src_rows:
            sv = _parse_signal_value(r[4])
            if not sv:
                log.debug("flow_materializer: skipping malformed insider row ticker={t}", t=r[0])
                continue
            batch.append({
                "ticker": r[0],
                "trade_date": r[1],
                "insider_name": r[2] or "",
                "trade_type": r[3] or "",
                "shares": _safe_float(sv.get("shares")),
                "value": _safe_float(sv.get("value")),
                "price_per_share": _safe_float(sv.get("price")),
                "insider_title": sv.get("insider_title", ""),
                "is_cluster_buy": bool(sv.get("is_unusual_size", False)),
            })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO insider_trades
                        (ticker, trade_date, insider_name, trade_type,
                         shares, value, price_per_share, insider_title, is_cluster_buy)
                    VALUES
                        (:ticker, :trade_date, :insider_name, :trade_type,
                         :shares, :value, :price_per_share, :insider_title, :is_cluster_buy)
                    ON CONFLICT (ticker, trade_date, insider_name, trade_type)
                    DO UPDATE SET
                        shares = EXCLUDED.shares,
                        value = EXCLUDED.value,
                        price_per_share = EXCLUDED.price_per_share,
                        insider_title = EXCLUDED.insider_title,
                        is_cluster_buy = EXCLUDED.is_cluster_buy
                """),
                batch,
            )
            rows_upserted = len(batch)

    log.info("flow_materializer: insider_trades upserted {n} rows", n=rows_upserted)
    return rows_upserted


# ── Sync: congressional_trades ───────────────────────────────────────────

def sync_congressional_trades(engine: Engine) -> int:
    """Read signal_sources WHERE source_type='congressional', parse JSONB,
    compute amount_midpoint, upsert into congressional_trades.

    The signal_value JSON contains: chamber, party, state, committee,
    amount_range, amount_midpoint, disclosure_date, disclosure_lag_days
    (as stored by CongressionalTradingPuller._emit_signal).

    Returns:
        Number of rows upserted.
    """
    _ensure_tables(engine)
    rows_upserted = 0

    with engine.begin() as conn:
        src_rows = conn.execute(text(
            "SELECT ticker, signal_date, source_id, signal_type, signal_value "
            "FROM signal_sources WHERE source_type = 'congressional' "
            "ORDER BY signal_date DESC LIMIT 5000"
        )).fetchall()

        if not src_rows:
            log.info("flow_materializer: no congressional signals found")
            return 0

        batch: list[dict] = []
        for r in src_rows:
            sv = _parse_signal_value(r[4])
            if not sv:
                log.debug("flow_materializer: skipping malformed congressional row ticker={t}", t=r[0])
                continue

            amount_range = sv.get("amount_range", "")
            stored_midpoint = _safe_float(sv.get("amount_midpoint"))
            midpoint = stored_midpoint if stored_midpoint > 0 else _midpoint_for_range(amount_range)

            disc_date_str = sv.get("disclosure_date", "")
            try:
                disc_date = date.fromisoformat(disc_date_str[:10]) if disc_date_str else r[1]
            except (ValueError, TypeError):
                disc_date = r[1]

            batch.append({
                "ticker": r[0],
                "disclosure_date": disc_date,
                "representative": r[2] or "",
                "transaction_type": r[3] or "",
                "amount": amount_range,
                "amount_midpoint": midpoint,
                "chamber": sv.get("chamber", ""),
                "party": sv.get("party", ""),
                "state": sv.get("state", ""),
            })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO congressional_trades
                        (ticker, disclosure_date, representative, transaction_type,
                         amount, amount_midpoint, chamber, party, state)
                    VALUES
                        (:ticker, :disclosure_date, :representative, :transaction_type,
                         :amount, :amount_midpoint, :chamber, :party, :state)
                    ON CONFLICT (ticker, disclosure_date, representative, transaction_type)
                    DO UPDATE SET
                        amount = EXCLUDED.amount,
                        amount_midpoint = EXCLUDED.amount_midpoint,
                        chamber = EXCLUDED.chamber,
                        party = EXCLUDED.party,
                        state = EXCLUDED.state
                """),
                batch,
            )
            rows_upserted = len(batch)

    log.info("flow_materializer: congressional_trades upserted {n} rows", n=rows_upserted)
    return rows_upserted


# ── Sync: dark_pool_weekly ───────────────────────────────────────────────

def sync_dark_pool_weekly(engine: Engine) -> int:
    """Read signal_sources WHERE source_type='darkpool' plus raw_series
    WHERE series_id LIKE 'DARKPOOL:%', aggregate by ticker and ISO week,
    compute short_pct and spike_ratio, upsert into dark_pool_weekly.

    spike_ratio = current week volume / 20-week rolling average volume.
    short_pct = short_volume / total_volume.

    Returns:
        Number of rows upserted.
    """
    _ensure_tables(engine)

    # Collect weekly data from raw_series (primary source)
    weekly: dict[tuple[str, date], dict[str, float]] = {}

    with engine.begin() as conn:
        rs_rows = conn.execute(text(
            "SELECT series_id, obs_date, value "
            "FROM raw_series WHERE series_id LIKE 'DARKPOOL:%' "
            "ORDER BY obs_date DESC LIMIT 50000"
        )).fetchall()

        for r in rs_rows:
            parts = str(r[0]).split(":")
            if len(parts) < 3:
                continue
            ticker = parts[1].upper()
            metric = parts[2].lower()  # 'volume' or 'trades'
            obs = r[1]
            # Align to ISO week start (Monday)
            week_start = obs - timedelta(days=obs.weekday())
            key = (ticker, week_start)
            entry = weekly.get(key, {"volume": 0.0, "trades": 0.0, "short_volume": 0.0})
            if metric == "volume":
                entry = {**entry, "volume": entry["volume"] + _safe_float(r[2])}
            elif metric == "trades":
                entry = {**entry, "trades": entry["trades"] + _safe_float(r[2])}
            weekly[key] = entry

        # Supplement with signal_sources darkpool entries
        sig_rows = conn.execute(text(
            "SELECT ticker, signal_date, signal_value "
            "FROM signal_sources WHERE source_type = 'darkpool' "
            "ORDER BY signal_date DESC LIMIT 10000"
        )).fetchall()

        for r in sig_rows:
            sv = _parse_signal_value(r[2])
            if not sv:
                continue
            ticker = (r[0] or "").upper()
            if not ticker:
                continue
            obs = r[1]
            week_start = obs - timedelta(days=obs.weekday()) if hasattr(obs, "weekday") else obs
            key = (ticker, week_start)
            entry = weekly.get(key, {"volume": 0.0, "trades": 0.0, "short_volume": 0.0})
            entry = {
                **entry,
                "volume": entry["volume"] + _safe_float(sv.get("volume")),
                "short_volume": entry["short_volume"] + _safe_float(sv.get("short_volume")),
            }
            weekly[key] = entry

        if not weekly:
            log.info("flow_materializer: no dark pool data found")
            return 0

        # Compute 20-week rolling average per ticker for spike_ratio
        by_ticker: dict[str, list[tuple[date, dict[str, float]]]] = {}
        for (ticker, wk), metrics in weekly.items():
            by_ticker.setdefault(ticker, []).append((wk, metrics))
        for ticker in by_ticker:
            by_ticker[ticker].sort(key=lambda x: x[0])

        batch: list[dict] = []
        for ticker, weeks in by_ticker.items():
            for i, (wk, metrics) in enumerate(weeks):
                total_vol = metrics["volume"]
                short_vol = metrics["short_volume"]
                short_pct = (short_vol / total_vol) if total_vol > 0 else None

                # 20-week lookback average
                lookback = weeks[max(0, i - 20):i]
                if lookback:
                    avg_vol = sum(w[1]["volume"] for w in lookback) / len(lookback)
                    spike = (total_vol / avg_vol) if avg_vol > 0 else None
                else:
                    spike = None

                batch.append({
                    "ticker": ticker,
                    "report_date": wk,
                    "short_volume": short_vol if short_vol > 0 else None,
                    "total_volume": total_vol if total_vol > 0 else None,
                    "trade_count": metrics["trades"] if metrics["trades"] > 0 else None,
                    "short_pct": round(short_pct, 4) if short_pct is not None else None,
                    "spike_ratio": round(spike, 2) if spike is not None else None,
                })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO dark_pool_weekly
                        (ticker, report_date, short_volume, total_volume,
                         trade_count, short_pct, spike_ratio)
                    VALUES
                        (:ticker, :report_date, :short_volume, :total_volume,
                         :trade_count, :short_pct, :spike_ratio)
                    ON CONFLICT (ticker, report_date)
                    DO UPDATE SET
                        short_volume = EXCLUDED.short_volume,
                        total_volume = EXCLUDED.total_volume,
                        trade_count = EXCLUDED.trade_count,
                        short_pct = EXCLUDED.short_pct,
                        spike_ratio = EXCLUDED.spike_ratio
                """),
                batch,
            )

    rows_upserted = len(batch) if weekly else 0
    log.info("flow_materializer: dark_pool_weekly upserted {n} rows", n=rows_upserted)
    return rows_upserted


# ── Sync: etf_flows ──────────────────────────────────────────────────────

def sync_etf_flows(engine: Engine) -> int:
    """Read raw_series WHERE series_id LIKE 'ETF_FLOW:%' for volume-based
    proxy data, upsert into etf_flows with source='proxy' and
    confidence='estimated'.

    The ETF flow series are stored by InstitutionalFlowsPuller as:
      ETF_FLOW:{ticker}:5d   — 5-day rolling dollar volume flow
      ETF_FLOW:{ticker}:20d  — 20-day rolling dollar volume flow
      ETF_FLOW:{ticker}:accel — flow acceleration

    We materialize the 5d series as the primary flow_value.

    Returns:
        Number of rows upserted.
    """
    _ensure_tables(engine)
    rows_upserted = 0

    with engine.begin() as conn:
        rs_rows = conn.execute(text(
            "SELECT series_id, obs_date, value "
            "FROM raw_series WHERE series_id LIKE :prefix "
            "AND series_id LIKE :suffix "
            "ORDER BY obs_date DESC LIMIT 20000"
        ), {"prefix": "ETF_FLOW:%", "suffix": "%:5d"}).fetchall()

        if not rs_rows:
            log.info("flow_materializer: no ETF flow series found")
            return 0

        batch: list[dict] = []
        for r in rs_rows:
            parts = str(r[0]).split(":")
            if len(parts) < 2:
                continue
            ticker = parts[1].upper()
            val = _safe_float(r[2])
            if val == 0.0:
                continue
            batch.append({
                "ticker": ticker,
                "flow_date": r[1],
                "flow_value": val,
                "source": "proxy",
                "confidence": "estimated",
            })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO etf_flows
                        (ticker, flow_date, flow_value, source, confidence)
                    VALUES
                        (:ticker, :flow_date, :flow_value, :source, :confidence)
                    ON CONFLICT (ticker, flow_date, source)
                    DO UPDATE SET
                        flow_value = EXCLUDED.flow_value,
                        confidence = EXCLUDED.confidence
                """),
                batch,
            )
            rows_upserted = len(batch)

    log.info("flow_materializer: etf_flows upserted {n} rows", n=rows_upserted)
    return rows_upserted


# ── Sync: junction_point_readings ────────────────────────────────────────

def sync_junction_points(engine: Engine) -> int:
    """Read latest values from raw_series for key FRED macro series,
    compute 1d/1w/1m changes and z-scores (vs 2-year history),
    upsert into junction_point_readings.

    Tracked series (from JUNCTION_SERIES): WALCL, RRPONTSYD, WTREGEN,
    M2SL, TOTBKCR/H8B1023NCBCMG, BAMLH0A0HYM2, BAMLC0A0CM, BOPGTB, UMCSENT.

    Returns:
        Number of rows upserted.
    """
    _ensure_tables(engine)
    rows_upserted = 0
    today = date.today()
    two_years_ago = today - timedelta(days=730)

    with engine.begin() as conn:
        batch: list[dict] = []

        for fred_id, label in JUNCTION_SERIES.items():
            # Fetch 2-year history for this series
            hist_rows = conn.execute(text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id = :sid "
                "AND obs_date >= :start "
                "ORDER BY obs_date ASC"
            ), {"sid": fred_id, "start": two_years_ago}).fetchall()

            if len(hist_rows) < 5:
                log.debug(
                    "flow_materializer: insufficient history for {s} ({n} rows)",
                    s=fred_id, n=len(hist_rows),
                )
                continue

            dates = [r[0] for r in hist_rows]
            values = [_safe_float(r[1]) for r in hist_rows]
            latest_val = values[-1]
            latest_date = dates[-1]

            # Compute changes by finding nearest observation to each offset
            change_1d = _compute_change(values, dates, latest_val, latest_date, days=1)
            change_1w = _compute_change(values, dates, latest_val, latest_date, days=7)
            change_1m = _compute_change(values, dates, latest_val, latest_date, days=30)

            # Z-score vs 2-year history
            mean_val = sum(values) / len(values)
            variance = sum((v - mean_val) ** 2 for v in values) / len(values)
            std_val = variance ** 0.5
            z_score = ((latest_val - mean_val) / std_val) if std_val > 0 else 0.0

            batch.append({
                "series_key": fred_id,
                "label": label,
                "obs_date": latest_date,
                "value": latest_val,
                "change_1d": round(change_1d, 6) if change_1d is not None else None,
                "change_1w": round(change_1w, 6) if change_1w is not None else None,
                "change_1m": round(change_1m, 6) if change_1m is not None else None,
                "z_score_2y": round(z_score, 4),
            })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO junction_point_readings
                        (series_key, label, obs_date, value,
                         change_1d, change_1w, change_1m, z_score_2y)
                    VALUES
                        (:series_key, :label, :obs_date, :value,
                         :change_1d, :change_1w, :change_1m, :z_score_2y)
                    ON CONFLICT (series_key, obs_date)
                    DO UPDATE SET
                        label = EXCLUDED.label,
                        value = EXCLUDED.value,
                        change_1d = EXCLUDED.change_1d,
                        change_1w = EXCLUDED.change_1w,
                        change_1m = EXCLUDED.change_1m,
                        z_score_2y = EXCLUDED.z_score_2y
                """),
                batch,
            )
            rows_upserted = len(batch)

    log.info("flow_materializer: junction_point_readings upserted {n} rows", n=rows_upserted)
    return rows_upserted


def _compute_change(
    values: list[float],
    dates: list[date],
    latest_val: float,
    latest_date: date,
    days: int,
) -> float | None:
    """Find the value closest to `days` ago and return the change vs latest.

    Parameters:
        values: Ordered list of observation values.
        dates: Corresponding observation dates (same length as values).
        latest_val: The most recent value.
        latest_date: The most recent observation date.
        days: How many days back to look.

    Returns:
        Absolute change (latest - prior), or None if no prior found.
    """
    target = latest_date - timedelta(days=days)
    best_idx = None
    best_dist = days + 30  # generous search window

    for i, d in enumerate(dates):
        dist = abs((d - target).days)
        if dist < best_dist:
            best_dist = dist
            best_idx = i

    if best_idx is not None and best_dist <= max(days, 7):
        return latest_val - values[best_idx]
    return None


# ── Orchestrator ─────────────────────────────────────────────────────────

def sync_all(engine: Engine) -> dict[str, Any]:
    """Run all five materialization sync functions.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.

    Returns:
        Summary dict with counts per table and any errors encountered.
    """
    results: dict[str, Any] = {"status": "SUCCESS", "errors": []}
    sync_funcs = {
        "insider_trades": sync_insider_trades,
        "congressional_trades": sync_congressional_trades,
        "dark_pool_weekly": sync_dark_pool_weekly,
        "etf_flows": sync_etf_flows,
        "junction_point_readings": sync_junction_points,
    }

    for table_name, func in sync_funcs.items():
        try:
            count = func(engine)
            results[table_name] = count
        except Exception as exc:
            log.error(
                "flow_materializer: {t} sync failed: {e}",
                t=table_name, e=str(exc),
            )
            results[table_name] = 0
            results["errors"].append({"table": table_name, "error": str(exc)})
            results["status"] = "PARTIAL"

    if len(results["errors"]) == len(sync_funcs):
        results["status"] = "FAILED"

    total = sum(results.get(t, 0) for t in sync_funcs)
    log.info(
        "flow_materializer: sync_all complete — {n} total rows, status={s}",
        n=total, s=results["status"],
    )
    return results
