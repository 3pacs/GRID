"""
GRID Intelligence — Government Contract Analysis.

Provides query functions for government contract data and cross-references
contract awards with insider/congressional trading to detect potential
information asymmetry.

Functions:
  - get_recent_contracts     — all contracts in last N days
  - get_contracts_for_ticker — contracts for a specific company
  - detect_contract_insider_overlap — insider/congressional trades preceding awards
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class ContractRecord:
    """A single government contract award."""
    award_id: str
    recipient_name: str
    ticker: str | None
    amount: float
    awarding_agency: str
    description: str
    award_date: str
    naics_code: str
    contract_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class InsiderContractOverlap:
    """A case where an insider or congressional trade preceded a contract award."""
    ticker: str
    contract_award_id: str
    contract_amount: float
    contract_date: str
    contract_agency: str
    trade_type: str            # 'insider' or 'congressional'
    trade_source_id: str       # member name or insider name
    trade_signal_type: str     # BUY / SELL
    trade_date: str
    days_before_contract: int
    suspicion_score: float     # 0-1, higher = more suspicious

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Query Functions ──────────────────────────────────────────────────────

def get_recent_contracts(engine: Engine, days: int = 30) -> list[ContractRecord]:
    """Fetch all government contract awards from the last N days.

    Parameters:
        engine: SQLAlchemy engine.
        days: Number of days to look back (default: 30).

    Returns:
        List of ContractRecord sorted by amount descending.
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE sc.name = 'USASPENDING_GOV' "
                "AND rs.obs_date >= :cutoff "
                "AND rs.pull_status = 'SUCCESS' "
                "ORDER BY rs.value DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    contracts: list[ContractRecord] = []
    for row in rows:
        payload = _parse_payload(row[3])
        if not payload:
            continue
        contracts.append(ContractRecord(
            award_id=payload.get("award_id", ""),
            recipient_name=payload.get("recipient_name", ""),
            ticker=payload.get("ticker"),
            amount=float(row[2]) if row[2] else 0.0,
            awarding_agency=payload.get("awarding_agency", ""),
            description=payload.get("description", ""),
            award_date=str(row[1]),
            naics_code=payload.get("naics_code", ""),
            contract_type=payload.get("contract_type", ""),
        ))

    log.info("Fetched {n} contracts in last {d} days", n=len(contracts), d=days)
    return contracts


def get_contracts_for_ticker(engine: Engine, ticker: str) -> list[ContractRecord]:
    """Fetch all stored government contracts for a specific ticker.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol (e.g. 'RTX', 'LMT').

    Returns:
        List of ContractRecord sorted by date descending.
    """
    ticker_upper = ticker.strip().upper()
    # series_id pattern includes ticker: GOV_CONTRACT:{agency}:{ticker}:{amount}
    pattern = f"GOV_CONTRACT:%:{ticker_upper}:%"

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE sc.name = 'USASPENDING_GOV' "
                "AND rs.series_id LIKE :pattern "
                "AND rs.pull_status = 'SUCCESS' "
                "ORDER BY rs.obs_date DESC"
            ),
            {"pattern": pattern},
        ).fetchall()

    contracts: list[ContractRecord] = []
    for row in rows:
        payload = _parse_payload(row[3])
        if not payload:
            continue
        contracts.append(ContractRecord(
            award_id=payload.get("award_id", ""),
            recipient_name=payload.get("recipient_name", ""),
            ticker=payload.get("ticker"),
            amount=float(row[2]) if row[2] else 0.0,
            awarding_agency=payload.get("awarding_agency", ""),
            description=payload.get("description", ""),
            award_date=str(row[1]),
            naics_code=payload.get("naics_code", ""),
            contract_type=payload.get("contract_type", ""),
        ))

    log.info("Found {n} contracts for {t}", n=len(contracts), t=ticker_upper)
    return contracts


def detect_contract_insider_overlap(
    engine: Engine,
    lookback_days: int = 90,
    pre_contract_window_days: int = 30,
) -> list[InsiderContractOverlap]:
    """Detect cases where insider or congressional trades preceded contract awards.

    Cross-references signal_sources entries of type 'insider' and 'congressional'
    with 'gov_contract' signals. Flags cases where a BUY trade for the same
    ticker occurred within pre_contract_window_days before a contract award.

    Parameters:
        engine: SQLAlchemy engine.
        lookback_days: How far back to search for contracts (default: 90).
        pre_contract_window_days: Max days a trade can precede a contract (default: 30).

    Returns:
        List of InsiderContractOverlap sorted by suspicion_score descending.
    """
    cutoff = date.today() - timedelta(days=lookback_days)

    with engine.connect() as conn:
        # Get recent contract awards from signal_sources
        contract_rows = conn.execute(
            text(
                "SELECT ticker, signal_date, signal_value "
                "FROM signal_sources "
                "WHERE source_type = 'gov_contract' "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

        if not contract_rows:
            log.info("No contract signals found in last {d} days", d=lookback_days)
            return []

        # For each contract, look for preceding insider/congressional trades
        overlaps: list[InsiderContractOverlap] = []

        for c_row in contract_rows:
            c_ticker = c_row[0]
            c_date = c_row[1]
            c_value = _parse_payload(c_row[2])
            if not c_value:
                continue

            c_amount = c_value.get("amount", 0)
            c_award_id = c_value.get("award_id", "")

            # Window: trades from (contract_date - window) to (contract_date - 1)
            window_start = c_date - timedelta(days=pre_contract_window_days)
            window_end = c_date - timedelta(days=1)

            trade_rows = conn.execute(
                text(
                    "SELECT source_type, source_id, signal_date, signal_type, signal_value "
                    "FROM signal_sources "
                    "WHERE ticker = :ticker "
                    "AND source_type IN ('insider', 'congressional') "
                    "AND signal_type = 'BUY' "
                    "AND signal_date BETWEEN :wstart AND :wend "
                    "ORDER BY signal_date DESC"
                ),
                {
                    "ticker": c_ticker,
                    "wstart": window_start,
                    "wend": window_end,
                },
            ).fetchall()

            for t_row in trade_rows:
                t_source_type = t_row[0]
                t_source_id = t_row[1]
                t_date = t_row[2]
                t_signal_type = t_row[3]
                days_before = (c_date - t_date).days

                # Suspicion score: closer to contract = more suspicious
                # Congressional trades are weighted higher (information advantage)
                time_factor = max(0.0, 1.0 - (days_before / pre_contract_window_days))
                type_weight = 0.8 if t_source_type == "congressional" else 0.6
                amount_factor = min(1.0, c_amount / 100_000_000)  # >$100M = max
                suspicion = round(
                    (time_factor * 0.5 + type_weight * 0.3 + amount_factor * 0.2),
                    3,
                )

                overlaps.append(InsiderContractOverlap(
                    ticker=c_ticker,
                    contract_award_id=c_award_id,
                    contract_amount=c_amount,
                    contract_date=str(c_date),
                    contract_agency=c_value.get("recipient_name", ""),
                    trade_type=t_source_type,
                    trade_source_id=t_source_id,
                    trade_signal_type=t_signal_type,
                    trade_date=str(t_date),
                    days_before_contract=days_before,
                    suspicion_score=suspicion,
                ))

    # Sort by suspicion descending
    overlaps.sort(key=lambda x: x.suspicion_score, reverse=True)

    log.info(
        "Contract-insider overlap detection: {n} overlaps from {c} contracts",
        n=len(overlaps),
        c=len(contract_rows),
    )
    return overlaps


# ── Helpers ──────────────────────────────────────────────────────────────

def _parse_payload(raw: Any) -> dict[str, Any] | None:
    """Parse a raw_payload or signal_value that may be JSON string or dict.

    Parameters:
        raw: JSON string, dict, or None.

    Returns:
        Parsed dict or None.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return None
    return None
