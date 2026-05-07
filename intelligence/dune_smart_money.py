"""Dune smart-money intelligence layer.

Reads the raw snapshots produced by ``ingestion.altdata.dune_puller`` and
surfaces three decision-support answers:

1. ``smart_money_leaderboard(token)``
     Top wallets by realized PnL on a token (last N days) with a
     "still holding" flag. Equivalent to the operator's prompt::

         "pull the top 20 wallets by realized pnl on $TOKEN in the
          last 30 days. show me which ones are still holding."

2. ``cex_flow_balance(token)``
     Net CEX inflows vs. outflows across all exchanges for a token over the
     last N days. Positive net = accumulation (coins leaving exchanges),
     negative net = distribution.

3. ``narrative_heat()``
     The 10 tokens with the biggest w/w % increase in unique new holders.

All three functions read from the immutable ``raw_series`` table (PIT
correct by construction) and never mutate it. Each returns a plain dict so
it can be served directly via an API route, streamed to the PWA, or
embedded in an LLM prompt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


_SERIES_PREFIX: str = "dune"
_DEFAULT_LOOKBACK_DAYS: int = 7


# ---------------------------------------------------------------------- #
# DTOs
# ---------------------------------------------------------------------- #


@dataclass(frozen=True)
class WalletPnL:
    """Single wallet PnL row in a smart-money leaderboard."""

    wallet: str
    realized_pnl_usd: float
    still_holding: bool
    balance_usd: float | None


@dataclass(frozen=True)
class CEXFlow:
    """Net CEX flow summary for a token."""

    token: str
    inflow_usd: float | None
    outflow_usd: float | None
    net_usd: float
    direction: str  # "accumulation" | "distribution"
    exchange_count: int | None
    as_of: date


@dataclass(frozen=True)
class HolderGrowth:
    """One token's w/w new-holder growth entry."""

    token: str
    new_holders: int | None
    prior_holders: int | None
    pct_change: float


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #


def _latest_payload(
    engine: Engine,
    series_id: str,
    as_of: date | None = None,
) -> tuple[date, dict[str, Any]] | None:
    """Fetch the most-recent raw_series payload for a series id.

    Parameters:
        engine: SQLAlchemy engine.
        series_id: Fully-qualified dune.* series id.
        as_of: Optional upper bound on observation date (for PIT queries).

    Returns:
        (obs_date, payload_dict) or None when no rows exist.
    """
    params: dict[str, Any] = {"sid": series_id}
    clause = ""
    if as_of is not None:
        clause = "AND obs_date <= :as_of"
        params["as_of"] = as_of

    sql = text(
        f"""
        SELECT obs_date, raw_payload
          FROM raw_series
         WHERE series_id = :sid
           AND pull_status = 'SUCCESS'
           {clause}
         ORDER BY obs_date DESC, pull_timestamp DESC
         LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, params).fetchone()
    if not row:
        return None

    obs_date, payload = row[0], row[1]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            log.warning("dune_smart_money: bad JSON in {s}", s=series_id)
            return None
    return obs_date, (payload or {})


def _safe_token(token: str) -> str:
    """Match the sanitizer used in the Dune puller."""
    out = token.lower()
    for ch in (" ", "-", ".", "/", "$", "#"):
        out = out.replace(ch, "_")
    return out.strip("_") or "unknown"


# ---------------------------------------------------------------------- #
# 1. Smart money leaderboard
# ---------------------------------------------------------------------- #


def smart_money_leaderboard(
    engine: Engine,
    token: str,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Return the top wallets by realized PnL on ``token`` with holdings.

    Parameters:
        engine: SQLAlchemy engine.
        token: Token symbol (e.g. "PEPE" or "$PEPE"; $ is stripped).
        as_of: Optional PIT cutoff for the lookup.

    Returns:
        Dict with ``token``, ``as_of``, ``lookback_days``, ``wallet_count``,
        ``still_holding``, and a ``leaderboard`` of WalletPnL dicts (sorted
        by realized PnL descending). Empty ``leaderboard`` means no snapshot
        exists yet; operators should run the puller.
    """
    token_clean = token.lstrip("$").upper()
    series_id = f"{_SERIES_PREFIX}.smart_money.{_safe_token(token_clean)}"

    res = _latest_payload(engine, series_id, as_of=as_of)
    if res is None:
        return {
            "token": token_clean,
            "as_of": None,
            "leaderboard": [],
            "wallet_count": 0,
            "still_holding": 0,
            "note": f"no snapshot in raw_series for {series_id}",
        }

    obs_date, payload = res
    wallets_raw = payload.get("wallets") or []
    leaderboard = [
        WalletPnL(
            wallet=str(w.get("wallet") or ""),
            realized_pnl_usd=float(w.get("realized_pnl_usd") or 0.0),
            still_holding=bool(w.get("still_holding")),
            balance_usd=(
                float(w["balance_usd"])
                if isinstance(w.get("balance_usd"), (int, float))
                else None
            ),
        )
        for w in wallets_raw
        if w.get("wallet")
    ]
    leaderboard.sort(key=lambda x: x.realized_pnl_usd, reverse=True)

    return {
        "token": token_clean,
        "as_of": obs_date.isoformat() if obs_date else None,
        "lookback_days": payload.get("lookback_days"),
        "wallet_count": len(leaderboard),
        "still_holding": sum(1 for w in leaderboard if w.still_holding),
        "leaderboard": [
            {
                "wallet": w.wallet,
                "realized_pnl_usd": w.realized_pnl_usd,
                "still_holding": w.still_holding,
                "balance_usd": w.balance_usd,
            }
            for w in leaderboard
        ],
    }


# ---------------------------------------------------------------------- #
# 2. CEX flow balance
# ---------------------------------------------------------------------- #


def cex_flow_balance(
    engine: Engine,
    token: str,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Return net CEX inflows vs. outflows for ``token`` (accumulation signal).

    Positive ``net_usd`` = coins leaving exchanges (accumulation).
    Negative ``net_usd`` = coins moving onto exchanges (distribution).

    Parameters:
        engine: SQLAlchemy engine.
        token: Token symbol (e.g. "PEPE" or "$PEPE").
        as_of: Optional PIT cutoff for the lookup.

    Returns:
        Dict with token, as_of, lookback_days, inflow_usd, outflow_usd,
        net_usd, direction, exchange_count. Missing data returns a sentinel
        dict with ``direction = "unknown"``.
    """
    token_clean = token.lstrip("$").upper()
    series_id = f"{_SERIES_PREFIX}.cex_flow.{_safe_token(token_clean)}"

    res = _latest_payload(engine, series_id, as_of=as_of)
    if res is None:
        return {
            "token": token_clean,
            "as_of": None,
            "direction": "unknown",
            "net_usd": 0.0,
            "note": f"no snapshot in raw_series for {series_id}",
        }

    obs_date, payload = res
    try:
        net_usd = float(payload.get("net_usd") or 0.0)
    except (TypeError, ValueError):
        net_usd = 0.0
    direction = payload.get("direction") or (
        "accumulation" if net_usd > 0 else "distribution"
    )
    return {
        "token": token_clean,
        "as_of": obs_date.isoformat() if obs_date else None,
        "lookback_days": payload.get("lookback_days"),
        "inflow_usd": payload.get("inflow_usd"),
        "outflow_usd": payload.get("outflow_usd"),
        "net_usd": net_usd,
        "direction": direction,
        "exchange_count": payload.get("exchange_count"),
    }


# ---------------------------------------------------------------------- #
# 3. Narrative heat
# ---------------------------------------------------------------------- #


def narrative_heat(
    engine: Engine,
    limit: int = 10,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Return the top tokens by w/w unique-new-holder growth.

    Reads the aggregate ``dune.narrative_heat`` snapshot written by the
    Dune puller. If the snapshot is absent, falls back to assembling a
    ranking from per-token ``dune.holder_growth.*`` series.

    Parameters:
        engine: SQLAlchemy engine.
        limit: Max number of tokens to return.
        as_of: Optional PIT cutoff for the lookup.

    Returns:
        Dict with as_of, tokens (list of HolderGrowth dicts sorted by
        pct_change descending, length <= ``limit``).
    """
    agg_series = f"{_SERIES_PREFIX}.narrative_heat"
    agg = _latest_payload(engine, agg_series, as_of=as_of)

    tokens: list[HolderGrowth] = []
    obs_date: date | None = None

    if agg is not None:
        obs_date, payload = agg
        for r in payload.get("top") or []:
            sym = str(r.get("token") or "").upper()
            if not sym:
                continue
            try:
                pct = float(r.get("pct_change") or 0.0)
            except (TypeError, ValueError):
                pct = 0.0
            tokens.append(
                HolderGrowth(
                    token=sym,
                    new_holders=r.get("new_holders"),
                    prior_holders=r.get("prior_holders"),
                    pct_change=pct,
                )
            )

    if not tokens:
        # Fallback: scan per-token series for the latest obs_date.
        params: dict[str, Any] = {"prefix": f"{_SERIES_PREFIX}.holder_growth."}
        clause = ""
        if as_of is not None:
            clause = "AND obs_date <= :as_of"
            params["as_of"] = as_of
        sql = text(
            f"""
            SELECT DISTINCT ON (series_id) series_id, obs_date, value, raw_payload
              FROM raw_series
             WHERE series_id LIKE :prefix || '%%'
               AND pull_status = 'SUCCESS'
               {clause}
             ORDER BY series_id, obs_date DESC, pull_timestamp DESC
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        for row in rows:
            payload = row[3]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            payload = payload or {}
            sym = str(payload.get("token") or row[0].rsplit(".", 1)[-1]).upper()
            try:
                pct = float(row[2] if row[2] is not None else 0.0)
            except (TypeError, ValueError):
                pct = 0.0
            if obs_date is None or (row[1] and row[1] > obs_date):
                obs_date = row[1]
            tokens.append(
                HolderGrowth(
                    token=sym,
                    new_holders=payload.get("new_holders"),
                    prior_holders=payload.get("prior_holders"),
                    pct_change=pct,
                )
            )

    tokens.sort(key=lambda x: x.pct_change, reverse=True)
    tokens = tokens[: max(0, int(limit))]

    return {
        "as_of": obs_date.isoformat() if obs_date else None,
        "tokens": [
            {
                "token": t.token,
                "new_holders": t.new_holders,
                "prior_holders": t.prior_holders,
                "pct_change": t.pct_change,
            }
            for t in tokens
        ],
    }


__all__ = [
    "CEXFlow",
    "HolderGrowth",
    "WalletPnL",
    "cex_flow_balance",
    "narrative_heat",
    "smart_money_leaderboard",
]
