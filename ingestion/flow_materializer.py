"""
GRID Flow Materializer — transforms signal_sources and raw_series into
the dedicated query-friendly tables that the flows API expects.

The flows API (api/routers/flows.py) queries insider_trades,
congressional_trades, dark_pool_weekly, etf_flows, and
junction_point_readings directly. Those tables are materialized views
of data already stored in signal_sources (JSONB) and raw_series.

This module provides idempotent sync functions that read from the
source tables, parse the JSON payloads, and upsert into the
query-friendly target tables using ON CONFLICT DO UPDATE
(congressional_trades is instead rebuilt atomically; see its sync).

Entry point: sync_all(engine) runs all materializers and returns a
summary dict with row counts per table plus any errors encountered.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.altdata.congressional import (
    _midpoint_amount,
    _normalize_member_name,
    _normalize_ticker,
    _normalize_txn_type,
    resolve_disclosure_date,
)

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
        price_per_share DOUBLE PRECISION,
        insider_title TEXT,
        filing_date DATE,
        is_cluster_buy BOOLEAN DEFAULT FALSE,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, trade_date, insider_name, trade_type)
    )
    """,
    # GD-FIX: filing_date already exists in prod (revision f1a2b3c4d5e6) but
    # was missing from this lazy-create fallback, so a database bootstrapped
    # from this file alone (fresh dev/test DB) would not have the column.
    "ALTER TABLE insider_trades ADD COLUMN IF NOT EXISTS filing_date DATE",
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
        committee         TEXT,
        created_at        TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ticker, disclosure_date, representative, transaction_type)
    )
    """,
    # GD-FIX: committee already exists in prod (revision f1a2b3c4d5e6) but
    # this fallback never declared it, and sync_congressional_trades never
    # selected it either (see below) — "Committee is always empty".
    "ALTER TABLE congressional_trades ADD COLUMN IF NOT EXISTS committee TEXT",
    # transaction_date / signal_source_id exist in prod (revision f1a2b3c4d5e6)
    # but this fallback never declared them and the sync never filled them.
    "ALTER TABLE congressional_trades ADD COLUMN IF NOT EXISTS transaction_date DATE",
    "ALTER TABLE congressional_trades ADD COLUMN IF NOT EXISTS signal_source_id INTEGER",
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


def _is_statement_timeout(exc: Exception) -> bool:
    """Return True when Postgres canceled a materializer query by timeout."""
    if isinstance(exc, OperationalError):
        orig = getattr(exc, "orig", None)
        if getattr(orig, "pgcode", None) == "57014":
            return True

    msg = str(exc).lower()
    return (
        "statement timeout" in msg
        or "canceling statement due to statement timeout" in msg
        or "querycanceled" in msg
    )


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


def _parse_filing_date(raw: Any, fallback: Any = None) -> Any:
    """Parse an insider-filing ``filing_date`` string into a ``date``.

    Never falls back to the transaction date: a missing or unparseable
    filing date means we do not know when the filing became public, and
    copying the trade date would fabricate a same-day disclosure that the
    source never asserted (GD-FIX).

    Parameters:
        raw: The ``filing_date`` value from a parsed ``signal_value`` dict
            (expected to be an ISO-ish date string, or empty/missing).
        fallback: Value to return when ``raw`` is missing or unparseable
            (defaults to ``None`` — leave the column NULL).

    Returns:
        A ``date`` on success, otherwise ``fallback``.
    """
    if not raw:
        return fallback
    try:
        return date.fromisoformat(str(raw)[:10])
    except (ValueError, TypeError):
        return fallback


def _cluster_windows_from_rows(
    rows: list[tuple[str, Any, Any]],
) -> dict[str, list[tuple[date, date]]]:
    """Build per-ticker cluster-buy date windows from CLUSTER_BUY signal rows.

    ``insider_filings.py::_detect_cluster_buys`` already does the honest
    work of finding multiple *distinct* insiders buying the same ticker
    within a window, and emits one CLUSTER_BUY row per detected cluster
    (source_id ``cluster_<ticker>``, signal_date = the cluster's last buy,
    signal_value carrying ``window_days``). Materializing that real signal
    is what makes ``insider_trades.is_cluster_buy`` mean what its name says,
    instead of the ``is_unusual_size`` flag it was mislabelled with (GD-FIX).

    Parameters:
        rows: ``(ticker, signal_date, signal_value)`` tuples for
            ``signal_type = 'CLUSTER_BUY'`` rows.

    Returns:
        Map of ticker -> list of ``(window_start, window_end)`` date ranges.
    """
    windows: dict[str, list[tuple[date, date]]] = {}
    for ticker, signal_date, signal_value in rows:
        sv = _parse_signal_value(signal_value)
        window_days = sv.get("window_days")
        try:
            window_days = int(window_days) if window_days is not None else 0
        except (ValueError, TypeError):
            window_days = 0
        if not ticker or signal_date is None:
            continue
        window_start = signal_date - timedelta(days=max(window_days, 0))
        windows.setdefault(ticker, []).append((window_start, signal_date))
    return windows


def _is_in_cluster_window(
    ticker: str,
    trade_date: Any,
    cluster_windows: dict[str, list[tuple[date, date]]],
) -> bool:
    """Return True if ``trade_date`` falls inside a detected cluster window for ``ticker``."""
    for start, end in cluster_windows.get(ticker, ()):
        if start <= trade_date <= end:
            return True
    return False


def _midpoint_for_range(amount_range: str) -> float:
    """Compute midpoint dollar value from an amount range code or string.

    Parameters:
        amount_range: Code like 'A'-'J', dollar string like '$1,001 - $15,000',
            or a bare band lower bound like '1001.0'.

    Returns:
        Midpoint as float, or 0.0 if unparseable.
    """
    if not amount_range:
        return 0.0
    # Shared with the puller: decimal-aware, so "1001.0" is not read as the
    # range 1001..0 (that bug stored a $1,001-$15,000 trade as $500.50).
    return _midpoint_amount(amount_range)


# ── Sync: insider_trades ─────────────────────────────────────────────────

def sync_insider_trades(engine: Engine) -> int:
    """Read signal_sources WHERE source_type='insider', parse JSONB, upsert into insider_trades.

    The signal_value JSON contains: shares, price, value, insider_title,
    filing_date, is_unusual_size (as stored by InsiderFilingsPuller._emit_signal).

    GD-FIX: two honesty fixes vs. the original materializer.
      1. ``filing_date`` — the column existed (revision f1a2b3c4d5e6) but this
         query never selected it, so it was NULL on every row. It is now
         parsed from signal_value and left NULL (never defaulted to the
         trade date) when the source didn't carry it.
      2. ``is_cluster_buy`` — this used to be set from ``is_unusual_size``
         (a single trade over $500K), which is a size flag, not a cluster
         signal. The real cluster detection already runs in
         ``insider_filings.py::_detect_cluster_buys`` and is stored as
         separate CLUSTER_BUY signal_sources rows; those are now read
         (instead of being filtered out) and used to flag only the trades
         that actually fall inside a detected multi-insider window.

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

        cluster_rows = conn.execute(text(
            "SELECT ticker, signal_date, signal_value "
            "FROM signal_sources WHERE source_type = 'insider' "
            "AND signal_type = 'CLUSTER_BUY' "
            "ORDER BY signal_date DESC LIMIT 5000"
        )).fetchall()
        cluster_windows = _cluster_windows_from_rows(
            [(r[0], r[1], r[2]) for r in cluster_rows]
        )

        batch: list[dict] = []
        for r in src_rows:
            sv = _parse_signal_value(r[4])
            if not sv:
                log.debug("flow_materializer: skipping malformed insider row ticker={t}", t=r[0])
                continue
            ticker = r[0]
            trade_date = r[1]
            batch.append({
                "ticker": ticker,
                "trade_date": trade_date,
                "insider_name": r[2] or "",
                "trade_type": r[3] or "",
                "shares": _safe_float(sv.get("shares")),
                "value": _safe_float(sv.get("value")),
                "price_per_share": _safe_float(sv.get("price")),
                "insider_title": sv.get("insider_title", ""),
                "filing_date": _parse_filing_date(sv.get("filing_date")),
                "is_cluster_buy": _is_in_cluster_window(ticker, trade_date, cluster_windows),
            })

        if batch:
            conn.execute(
                text("""
                    INSERT INTO insider_trades
                        (ticker, trade_date, insider_name, trade_type,
                         shares, value, price_per_share, insider_title,
                         filing_date, is_cluster_buy)
                    VALUES
                        (:ticker, :trade_date, :insider_name, :trade_type,
                         :shares, :value, :price_per_share, :insider_title,
                         :filing_date, :is_cluster_buy)
                    ON CONFLICT (ticker, trade_date, insider_name, trade_type)
                    DO UPDATE SET
                        shares = EXCLUDED.shares,
                        value = EXCLUDED.value,
                        price_per_share = EXCLUDED.price_per_share,
                        insider_title = EXCLUDED.insider_title,
                        filing_date = COALESCE(EXCLUDED.filing_date, insider_trades.filing_date),
                        is_cluster_buy = EXCLUDED.is_cluster_buy
                """),
                batch,
            )
            rows_upserted = len(batch)

    log.info("flow_materializer: insider_trades upserted {n} rows", n=rows_upserted)
    return rows_upserted


# ── Sync: congressional_trades ───────────────────────────────────────────

# QuiverQuant House/Senate rows are the live feed; ``congressional`` is the
# native puller (inactive since 2026-09-30), whose rows mirror them.
_CONGRESS_SOURCE_TYPES: tuple[str, ...] = (
    "quiverquant:house", "quiverquant:senate", "congressional",
)
# Disclosure bases that bound when a trade became public. ``statutory_bound``
# (trade + 45 days) is not one: late PTRs exist, so it can precede the real
# disclosure — the people_events pipeline ignores it for the same reason.
_CONGRESS_KNOWN_BASES: frozenset[str] = frozenset({"reported", "qq_last_modified"})
# Refuse a rebuild that would shrink the table below this share of its
# current size (a partial source read must never wipe good rows).
_CONGRESS_MIN_REBUILD_RATIO: float = 0.5


def build_congressional_rows(src_rows: Any) -> tuple[list[dict], dict[str, int]]:
    """Turn signal_sources rows into congressional_trades rows (pure).

    Each source row is ``(id, source_type, ticker, signal_date, source_id,
    signal_type, signal_value)``; ``signal_date`` is the transaction date.

    disclosure_date is the date the trade was public, never the trade date:
    a reported disclosure date, else QuiverQuant's ``last_modified`` when it
    is on/after the trade (an upper bound — PIT-safe). Rows with neither are
    skipped, not guessed. QuiverQuant rows win over native mirrors of the
    same (member, ticker, trade date, direction). When several trades share
    the table's unique key (ticker, disclosure_date, representative,
    transaction_type), the largest band is kept and the collision counted.

    Returns:
        ``(rows, skips)`` — rows ready to insert, and skip/collision counts.
    """
    skips: dict[str, int] = {}

    def skip(reason: str) -> None:
        skips[reason] = skips.get(reason, 0) + 1

    candidates: list[tuple[int, dict]] = []
    for sid, source_type, ticker, signal_date, source_id, signal_type, raw in src_rows:
        sv = _parse_signal_value(raw)
        txn_date = signal_date if isinstance(signal_date, date) else None
        if txn_date is None:
            try:
                txn_date = date.fromisoformat(str(signal_date)[:10])
            except (ValueError, TypeError):
                txn_date = None
        ticker = _normalize_ticker(ticker or "")
        if txn_date is None or not ticker:
            skip("missing_trade_date_or_ticker")
            continue

        if source_type == "congressional":
            basis = sv.get("disclosure_basis")
            if basis not in _CONGRESS_KNOWN_BASES:
                # No basis = pre-GD-FIX row whose disclosure_date is the trade date.
                skip("native_no_disclosure_bound" if basis is None else f"native_{basis}")
                continue
            disc_date, basis = resolve_disclosure_date(txn_date, sv.get("disclosure_date"))
            if basis != "reported":
                skip("native_no_disclosure_bound")
                continue
            member = source_id or ""
            txn_type = _normalize_txn_type(signal_type or "")
            amount = sv.get("amount_range") or ""
            chamber = sv.get("chamber") or ""
            precedence = 1
        else:
            disc_date, basis = resolve_disclosure_date(
                txn_date,
                sv.get("DisclosureDate") or sv.get("ReportDate") or sv.get("Filed"),
                sv.get("last_modified") or sv.get("LastModified"),
            )
            if basis not in _CONGRESS_KNOWN_BASES:
                skip("qq_no_disclosure_bound")
                continue
            member = sv.get("Representative") or sv.get("Senator") or sv.get("Name") or source_id or ""
            txn_type = _normalize_txn_type(sv.get("Transaction") or signal_type or "")
            amount = sv.get("Range") or sv.get("Amount") or ""
            chamber = "SENATE" if source_type.endswith("senate") else "HOUSE"
            precedence = 0

        member = str(member).strip()
        if not member or not txn_type:
            skip("missing_member_or_type")
            continue
        candidates.append((precedence, {
            "ticker": ticker,
            "disclosure_date": disc_date,
            "transaction_date": txn_date,
            "representative": member,
            "transaction_type": txn_type,
            "amount": str(amount),
            "amount_midpoint": _midpoint_amount(str(amount)),
            "chamber": chamber,
            "party": sv.get("party") or sv.get("Party") or "",
            "state": sv.get("state") or sv.get("State") or "",
            "committee": sv.get("committee") or sv.get("Committee") or "",
            "signal_source_id": int(sid) if sid is not None else None,
        }))

    # QuiverQuant first, so a native mirror of the same trade is dropped.
    candidates.sort(key=lambda c: c[0])
    seen_trades: set[tuple] = set()
    by_key: dict[tuple, dict] = {}
    for precedence, row in candidates:
        trade = (_normalize_member_name(row["representative"]), row["ticker"],
                 row["transaction_date"], row["transaction_type"], row["amount_midpoint"])
        if trade in seen_trades:
            skip("duplicate_trade" if precedence == 0 else "native_mirror_of_qq")
            continue
        seen_trades.add(trade)
        key = (row["ticker"], row["disclosure_date"], row["representative"], row["transaction_type"])
        kept = by_key.get(key)
        if kept is not None:
            skip("unique_key_collision")
            if (row["amount_midpoint"], row["transaction_date"]) <= (
                kept["amount_midpoint"], kept["transaction_date"]
            ):
                continue
        by_key[key] = row
    return list(by_key.values()), skips


def sync_congressional_trades(engine: Engine) -> int:
    """Rebuild congressional_trades from signal_sources in one transaction.

    The table is a derived view (this is its only writer). It is rebuilt
    rather than upserted because disclosure_date is part of its unique key:
    correcting a date in place would leave the old row behind. The rebuild
    is atomic (readers see old or new, never empty) and fails closed — an
    empty or sharply smaller build leaves the table untouched.

    History: rows written before this fix stored the transaction date as
    disclosure_date (lag 0 on every row, ~4 weeks of look-ahead for any
    point-in-time use) and QuiverQuant's band lower bound as the amount
    ("1001.0" -> midpoint $500.50). See build_congressional_rows.

    Returns:
        Number of rows written (0 when the rebuild was refused).
    """
    _ensure_tables(engine)

    with engine.begin() as conn:
        src_rows = conn.execute(text(
            "SELECT id, source_type, ticker, signal_date, source_id, signal_type, signal_value "
            "FROM signal_sources WHERE source_type IN :types"
        ).bindparams(bindparam("types", expanding=True)),
            {"types": list(_CONGRESS_SOURCE_TYPES)}).fetchall()

        rows, skips = build_congressional_rows(src_rows)
        existing = conn.execute(text("SELECT COUNT(*) FROM congressional_trades")).scalar() or 0
        if skips:
            log.info("flow_materializer: congressional skips {s}", s=skips)
        if not rows:
            log.warning("flow_materializer: no congressional rows built — table left unchanged")
            return 0
        if existing and len(rows) < existing * _CONGRESS_MIN_REBUILD_RATIO:
            log.error(
                "flow_materializer: congressional rebuild refused — {n} rows built vs {e} "
                "existing (below {r:.0%}); table left unchanged",
                n=len(rows), e=existing, r=_CONGRESS_MIN_REBUILD_RATIO,
            )
            return 0

        conn.execute(text("DELETE FROM congressional_trades"))
        conn.execute(
            text("""
                INSERT INTO congressional_trades
                    (ticker, disclosure_date, transaction_date, representative,
                     transaction_type, amount, amount_midpoint, chamber, party,
                     state, committee, signal_source_id)
                VALUES
                    (:ticker, :disclosure_date, :transaction_date, :representative,
                     :transaction_type, :amount, :amount_midpoint, :chamber, :party,
                     :state, :committee, :signal_source_id)
            """),
            rows,
        )

    log.info(
        "flow_materializer: congressional_trades rebuilt — {n} rows (was {e})",
        n=len(rows), e=existing,
    )
    return len(rows)


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
            "AND pull_status = 'SUCCESS' "
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
            "AND pull_status = 'SUCCESS' "
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
                "AND pull_status = 'SUCCESS' "
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
            log_fn = log.warning if _is_statement_timeout(exc) else log.error
            log_fn(
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
