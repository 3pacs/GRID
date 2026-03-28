"""
GRID Intelligence — Legislative Trading Detection.

Cross-references congressional trading disclosures with active legislation
to identify committee members trading in sectors their committee is
actively considering.  This is the key insight: a member of the House
Financial Services Committee buying bank stocks the week before a hearing
on banking regulation is the kind of signal this module surfaces.

Entry points:
  get_upcoming_hearings        — hearings in the next N days with ticker mapping
  get_bills_affecting_ticker   — active bills that affect a given ticker
  detect_legislative_trading   — THE KEY FUNCTION: finds committee-member
                                 trades in sectors under their committee's
                                 jurisdiction during active legislation
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.legislation import (
    COMMITTEE_SECTOR_MAP,
    TOPIC_SECTOR_MAP,
    _tickers_for_committee,
)

# ── Data classes ─────────────────────────────────────────────────────────


@dataclass
class LegislativeHearing:
    """An upcoming committee hearing with sector/ticker impact."""

    hearing_id: str
    title: str
    chamber: str
    committees: list[str]
    date: str
    matched_topics: list[str]
    affected_tickers: list[str]
    days_until: int


@dataclass
class BillImpact:
    """A bill that affects a specific ticker or sector."""

    bill_id: str
    title: str
    status: str
    sponsor: str
    sponsor_party: str
    committees: list[str]
    obs_date: str
    policy_area: str
    matched_topics: list[str]
    affected_tickers: list[str]
    relevance_score: float  # how directly the bill affects the ticker


@dataclass
class LegislativeTradeAlert:
    """A committee member trading in a sector their committee oversees.

    This is the core intelligence output — it flags potentially informed
    trading by members of Congress.
    """

    member_name: str
    party: str
    state: str
    chamber: str
    committee: str
    ticker: str
    transaction_type: str  # BUY or SELL
    amount_range: str
    transaction_date: str
    disclosure_date: str
    disclosure_lag_days: int
    related_bills: list[str]          # bill IDs under consideration
    related_hearings: list[str]       # hearing IDs scheduled nearby
    committee_overlap: bool           # member sits on committee with jurisdiction
    severity: str                     # HIGH, MEDIUM, LOW
    explanation: str                  # human-readable why this is flagged


# ── Public functions ─────────────────────────────────────────────────────


def get_upcoming_hearings(engine: Engine, days: int = 14) -> list[dict[str, Any]]:
    """Return upcoming hearings with sector/ticker mapping.

    Queries raw_series for LEGISLATION:*:hearing entries with future
    obs_date values.

    Parameters:
        engine: SQLAlchemy engine.
        days: How many days ahead to look.

    Returns:
        List of hearing dicts with affected tickers.
    """
    today = date.today()
    future = today + timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND obs_date BETWEEN :today AND :future "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date ASC"
            ),
            {"pattern": "LEGISLATION:%:hearing", "today": today, "future": future},
        ).fetchall()

    hearings: list[dict[str, Any]] = []
    for row in rows:
        payload = row[2] if isinstance(row[2], dict) else json.loads(row[2]) if row[2] else {}
        obs_date = row[1]
        days_until = (obs_date - today).days if isinstance(obs_date, date) else 0

        hearings.append({
            **asdict(LegislativeHearing(
                hearing_id=payload.get("hearing_id", row[0]),
                title=payload.get("title", ""),
                chamber=payload.get("chamber", ""),
                committees=payload.get("committees", []),
                date=obs_date.isoformat() if isinstance(obs_date, date) else str(obs_date),
                matched_topics=payload.get("matched_topics", []),
                affected_tickers=payload.get("affected_tickers", []),
                days_until=days_until,
            )),
        })

    log.info("Legislative intel: {n} upcoming hearings in next {d} days", n=len(hearings), d=days)
    return hearings


def get_bills_affecting_ticker(engine: Engine, ticker: str) -> list[dict[str, Any]]:
    """Return active bills that affect a given ticker.

    Searches raw_series LEGISLATION entries where affected_tickers
    in the raw_payload contains the target ticker.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol to search for (e.g. 'NVDA').

    Returns:
        List of bill impact dicts sorted by relevance.
    """
    ticker_upper = ticker.strip().upper()

    # Search recent legislation entries (last 90 days)
    cutoff = date.today() - timedelta(days=90)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND obs_date >= :cutoff "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC"
            ),
            {"pattern": "LEGISLATION:%", "cutoff": cutoff},
        ).fetchall()

    bills: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for row in rows:
        payload = row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {}
        affected = payload.get("affected_tickers", [])

        if ticker_upper not in affected:
            continue

        bill_id = payload.get("bill_id", "")
        if not bill_id or bill_id in seen_ids:
            continue
        seen_ids.add(bill_id)

        # Relevance: direct topic match = 1.0, committee jurisdiction = 0.5
        topics = payload.get("matched_topics", [])
        relevance = 1.0 if topics else 0.5

        bills.append(asdict(BillImpact(
            bill_id=bill_id,
            title=payload.get("title", ""),
            status=payload.get("status", ""),
            sponsor=payload.get("sponsor", ""),
            sponsor_party=payload.get("sponsor_party", ""),
            committees=payload.get("committees", []),
            obs_date=row[1].isoformat() if isinstance(row[1], date) else str(row[1]),
            policy_area=payload.get("policy_area", ""),
            matched_topics=topics,
            affected_tickers=affected,
            relevance_score=relevance,
        )))

    # Sort by relevance then date
    bills.sort(key=lambda b: (-b["relevance_score"], b["obs_date"]), reverse=False)

    log.info(
        "Legislative intel: {n} bills affecting {t}",
        n=len(bills),
        t=ticker_upper,
    )
    return bills


def detect_legislative_trading(engine: Engine, days_back: int = 30) -> list[dict[str, Any]]:
    """Detect committee members trading in sectors their committee oversees.

    THIS IS THE KEY INSIGHT.

    Cross-references:
    1. Congressional trading disclosures (CONGRESS:* series)
    2. Active legislation (LEGISLATION:* series)
    3. Committee assignments from both sources

    Flags trades where:
    - The member sits on a committee that has jurisdiction over the sector
    - There is active legislation (bill or hearing) in that sector
    - The trade occurred within a window around the legislative activity

    Parameters:
        engine: SQLAlchemy engine.
        days_back: How far back to search for trades.

    Returns:
        List of LegislativeTradeAlert dicts sorted by severity.
    """
    cutoff = date.today() - timedelta(days=days_back)
    alerts: list[dict[str, Any]] = []

    # ── Step 1: Get recent congressional trades ──────────────────────────

    with engine.connect() as conn:
        trade_rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND obs_date >= :cutoff "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC"
            ),
            {"pattern": "CONGRESS:%", "cutoff": cutoff},
        ).fetchall()

    if not trade_rows:
        log.info("Legislative trading: no congressional trades in last {d} days", d=days_back)
        return []

    # ── Step 2: Get active legislation and hearings ──────────────────────

    legislation_cutoff = cutoff - timedelta(days=14)  # wider window for context
    legislation_future = date.today() + timedelta(days=14)

    with engine.connect() as conn:
        leg_rows = conn.execute(
            text(
                "SELECT series_id, obs_date, raw_payload "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND obs_date BETWEEN :cutoff AND :future "
                "AND pull_status = 'SUCCESS'"
            ),
            {"pattern": "LEGISLATION:%", "cutoff": legislation_cutoff, "future": legislation_future},
        ).fetchall()

    # Build committee -> active legislation index
    committee_legislation: dict[str, list[dict[str, Any]]] = defaultdict(list)
    # Build ticker -> active legislation index
    ticker_legislation: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in leg_rows:
        payload = row[2] if isinstance(row[2], dict) else json.loads(row[2]) if row[2] else {}
        leg_entry = {
            "series_id": row[0],
            "obs_date": row[1],
            "bill_id": payload.get("bill_id", payload.get("hearing_id", "")),
            "title": payload.get("title", ""),
            "committees": payload.get("committees", []),
            "affected_tickers": payload.get("affected_tickers", []),
            "status": payload.get("status", ""),
        }

        for committee in leg_entry["committees"]:
            committee_legislation[committee.lower()].append(leg_entry)

        for ticker in leg_entry["affected_tickers"]:
            ticker_legislation[ticker].append(leg_entry)

    if not leg_rows:
        log.info("Legislative trading: no active legislation found — cannot cross-reference")
        # Still check committee overlap even without specific bills
        pass

    # ── Step 3: Cross-reference trades with legislation ──────────────────

    for row in trade_rows:
        payload = row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {}

        member_name = payload.get("member_name", "")
        ticker = payload.get("ticker", "")
        committee = payload.get("committee", "")
        chamber = payload.get("chamber", "")
        party = payload.get("party", "")
        state = payload.get("state", "")
        txn_type = payload.get("transaction_type", "")
        amount_range = payload.get("amount_range", "")
        txn_date_str = payload.get("transaction_date", "")
        disc_date_str = payload.get("disclosure_date", "")

        if not member_name or not ticker:
            continue

        # Check 1: Does the member's committee have jurisdiction over this ticker?
        committee_overlap = False
        overlapping_committee = ""
        if committee:
            committee_tickers = _tickers_for_committee(committee)
            if ticker in committee_tickers:
                committee_overlap = True
                overlapping_committee = committee

        # Also check all known committee-sector mappings
        if not committee_overlap:
            for cname, ctickers in COMMITTEE_SECTOR_MAP.items():
                if ticker in ctickers and cname in committee.lower():
                    committee_overlap = True
                    overlapping_committee = committee
                    break

        # Check 2: Is there active legislation affecting this ticker?
        related_bills: list[str] = []
        related_hearings: list[str] = []

        if ticker in ticker_legislation:
            for leg in ticker_legislation[ticker]:
                leg_id = leg["bill_id"]
                if "hearing" in leg.get("series_id", ""):
                    related_hearings.append(leg_id)
                else:
                    related_bills.append(leg_id)

        # Check 3: Is there legislation in their committee?
        if committee:
            for leg in committee_legislation.get(committee.lower(), []):
                leg_id = leg["bill_id"]
                if leg_id not in related_bills and leg_id not in related_hearings:
                    if "hearing" in leg.get("series_id", ""):
                        related_hearings.append(leg_id)
                    else:
                        related_bills.append(leg_id)

        # ── Determine severity ───────────────────────────────────────────

        # HIGH: committee overlap + active legislation + large trade
        # MEDIUM: committee overlap OR active legislation
        # LOW: only generic sector match

        has_legislation = bool(related_bills or related_hearings)

        if committee_overlap and has_legislation:
            severity = "HIGH"
            explanation = (
                f"{member_name} ({party}-{state}) sits on {overlapping_committee}, "
                f"which has jurisdiction over {ticker}. "
                f"There are {len(related_bills)} active bill(s) and "
                f"{len(related_hearings)} hearing(s) affecting this ticker. "
                f"Member executed a {txn_type} of {amount_range}."
            )
        elif committee_overlap:
            severity = "MEDIUM"
            explanation = (
                f"{member_name} ({party}-{state}) sits on {overlapping_committee}, "
                f"which has jurisdiction over {ticker}. "
                f"No active legislation found in the current window, but "
                f"committee oversight creates an information advantage. "
                f"Member executed a {txn_type} of {amount_range}."
            )
        elif has_legislation:
            severity = "MEDIUM"
            explanation = (
                f"{member_name} ({party}-{state}) traded {ticker} while "
                f"{len(related_bills)} bill(s) and {len(related_hearings)} "
                f"hearing(s) are active for this sector. "
                f"Member executed a {txn_type} of {amount_range}."
            )
        else:
            # No overlap, no active legislation — skip (not suspicious)
            continue

        # Compute disclosure lag
        try:
            txn_date = date.fromisoformat(txn_date_str[:10]) if txn_date_str else row[1]
            disc_date = date.fromisoformat(disc_date_str[:10]) if disc_date_str else txn_date
            lag_days = (disc_date - txn_date).days if isinstance(disc_date, date) and isinstance(txn_date, date) else 0
        except (ValueError, TypeError):
            lag_days = 0

        alert = asdict(LegislativeTradeAlert(
            member_name=member_name,
            party=party,
            state=state,
            chamber=chamber,
            committee=committee,
            ticker=ticker,
            transaction_type=txn_type,
            amount_range=amount_range,
            transaction_date=txn_date_str,
            disclosure_date=disc_date_str,
            disclosure_lag_days=lag_days,
            related_bills=related_bills[:10],
            related_hearings=related_hearings[:10],
            committee_overlap=committee_overlap,
            severity=severity,
            explanation=explanation,
        ))
        alerts.append(alert)

    # Sort: HIGH first, then MEDIUM, then by date descending
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    alerts.sort(key=lambda a: (severity_order.get(a["severity"], 3), a["transaction_date"]))

    log.info(
        "Legislative trading: {n} alerts ({h} HIGH, {m} MEDIUM)",
        n=len(alerts),
        h=sum(1 for a in alerts if a["severity"] == "HIGH"),
        m=sum(1 for a in alerts if a["severity"] == "MEDIUM"),
    )

    return alerts


def get_legislation_summary(engine: Engine) -> dict[str, Any]:
    """Return a high-level summary of current legislative landscape.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Summary dict with counts, top sectors, and alerts.
    """
    upcoming = get_upcoming_hearings(engine, days=14)
    trade_alerts = detect_legislative_trading(engine, days_back=30)

    # Count affected tickers across all hearings
    ticker_counts: dict[str, int] = defaultdict(int)
    for h in upcoming:
        for t in h.get("affected_tickers", []):
            ticker_counts[t] += 1

    top_tickers = sorted(ticker_counts.items(), key=lambda x: -x[1])[:20]

    return {
        "upcoming_hearings_count": len(upcoming),
        "upcoming_hearings": upcoming[:10],
        "trade_alerts_count": len(trade_alerts),
        "high_severity_alerts": [a for a in trade_alerts if a["severity"] == "HIGH"],
        "medium_severity_alerts": [a for a in trade_alerts if a["severity"] == "MEDIUM"][:20],
        "most_legislated_tickers": [{"ticker": t, "count": c} for t, c in top_tickers],
    }
