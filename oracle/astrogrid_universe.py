"""Canonical AstroGrid scoring universe definitions.

AstroGrid answers can be broader than the liquid scoring universe, but the
learning loop should only score assets with explicit coverage rules. This file
keeps the scoring contract in one place so the API, seed corpus, and scorer all
use the same mapping and scoreability logic.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

from sqlalchemy import text

_MIN_SCOREABLE_HISTORY_POINTS = 60
_STALE_HISTORY_DAYS = 14

_ASTROGRID_SCOREABLE_UNIVERSE: list[dict[str, Any]] = [
    {
        "symbol": "BTC",
        "label": "Bitcoin",
        "group": "crypto",
        "asset_class": "crypto",
        "lookup_ticker": "BTC",
        "price_feature": "btc_full",
        "benchmark_symbol": "BTC",
    },
    {
        "symbol": "ETH",
        "label": "Ethereum",
        "group": "crypto",
        "asset_class": "crypto",
        "lookup_ticker": "ETH",
        "price_feature": "eth_full",
        "benchmark_symbol": "BTC",
    },
    {
        "symbol": "SOL",
        "label": "Solana",
        "group": "crypto",
        "asset_class": "crypto",
        "lookup_ticker": "SOL",
        "price_feature": "sol_full",
        "benchmark_symbol": "BTC",
    },
    {
        "symbol": "AAPL",
        "label": "Apple",
        "group": "equity",
        "asset_class": "equity",
        "lookup_ticker": "AAPL",
        "price_feature": "aapl_full",
        "benchmark_symbol": "QQQ",
    },
    {
        "symbol": "MSFT",
        "label": "Microsoft",
        "group": "equity",
        "asset_class": "equity",
        "lookup_ticker": "MSFT",
        "price_feature": "msft_full",
        "benchmark_symbol": "QQQ",
    },
    {
        "symbol": "GOOGL",
        "label": "Alphabet",
        "group": "equity",
        "asset_class": "equity",
        "lookup_ticker": "GOOGL",
        "price_feature": "googl_full",
        "benchmark_symbol": "QQQ",
    },
    {
        "symbol": "NVDA",
        "label": "NVIDIA",
        "group": "equity",
        "asset_class": "equity",
        "lookup_ticker": "NVDA",
        "price_feature": "nvda_full",
        "benchmark_symbol": "QQQ",
    },
    {
        "symbol": "META",
        "label": "Meta",
        "group": "equity",
        "asset_class": "equity",
        "lookup_ticker": "META",
        "price_feature": "meta_full",
        "benchmark_symbol": "QQQ",
    },
    {
        "symbol": "SPY",
        "label": "S&P 500",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "SPY",
        "price_feature": "spy_full",
        "benchmark_symbol": "SPY",
    },
    {
        "symbol": "QQQ",
        "label": "Nasdaq 100",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "QQQ",
        "price_feature": "qqq_full",
        "benchmark_symbol": "SPY",
    },
    {
        "symbol": "TLT",
        "label": "Long Bonds",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "TLT",
        "price_feature": "tlt_full",
        "benchmark_symbol": "SPY",
    },
    {
        "symbol": "DXY",
        "label": "Dollar Index",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "UUP",
        "price_feature": "dxy_index",
        "benchmark_symbol": "SPY",
    },
    {
        "symbol": "GLD",
        "label": "Gold",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "GLD",
        "price_feature": "gld_full",
        "benchmark_symbol": "SPY",
    },
    {
        "symbol": "CL",
        "label": "Crude Oil",
        "group": "macro",
        "asset_class": "macro",
        "lookup_ticker": "CL=F",
        "price_feature": "cl_close",
        "benchmark_symbol": "SPY",
    },
]


def get_astrogrid_scoreable_universe() -> list[dict[str, Any]]:
    return deepcopy(_ASTROGRID_SCOREABLE_UNIVERSE)


def scoreable_universe_by_symbol() -> dict[str, dict[str, Any]]:
    return {item["symbol"]: dict(item) for item in get_astrogrid_scoreable_universe()}


def enrich_astrogrid_scoreable_universe(conn) -> list[dict[str, Any]]:
    universe = get_astrogrid_scoreable_universe()
    feature_names = [item["price_feature"] for item in universe]
    rows = conn.execute(
        text(
            """
            SELECT
                fr.name,
                COUNT(rs.obs_date) AS row_count,
                MAX(rs.obs_date) AS latest_obs_date
            FROM feature_registry fr
            LEFT JOIN resolved_series rs ON rs.feature_id = fr.id
            WHERE fr.name = ANY(:feature_names)
            GROUP BY fr.name
            """
        ),
        {"feature_names": feature_names},
    ).fetchall()
    coverage = {
        row[0]: {
            "row_count": int(row[1] or 0),
            "latest_obs_date": row[2],
        }
        for row in rows
    }

    today = date.today()
    for item in universe:
        feature = item["price_feature"]
        info = coverage.get(feature, {})
        row_count = int(info.get("row_count") or 0)
        latest_obs_date = info.get("latest_obs_date")
        stale_days = (today - latest_obs_date).days if latest_obs_date else None

        if row_count >= _MIN_SCOREABLE_HISTORY_POINTS and (stale_days is None or stale_days <= _STALE_HISTORY_DAYS):
            status = "scoreable_now"
            reason = "canonical feature is materialized with sufficient recent history"
        elif row_count > 0:
            status = "degraded"
            reason = "canonical feature exists but needs more history or freshness for full scoring confidence"
        else:
            status = "unscored"
            reason = "canonical feature has no materialized history yet"

        item["scoreable_now"] = status == "scoreable_now"
        item["status"] = status
        item["reason_if_not"] = None if status == "scoreable_now" else reason
        item["history_points"] = row_count
        item["latest_obs_date"] = latest_obs_date.isoformat() if latest_obs_date else None
    return universe
