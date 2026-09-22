"""
GRID Intelligence — Export Controls Analysis.

Provides query functions for export control data and revenue impact
assessment. For companies like NVIDIA, export controls to China have been
more material than the CHIPS Act (~25% of revenue at risk).

Functions:
  - get_recent_controls       — all export control actions in last N days
  - get_controls_for_ticker   — actions for a specific company
  - assess_revenue_impact     — estimate % of revenue at risk
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
class ExportControlRecord:
    """A single export control action."""
    document_number: str
    title: str
    abstract: str
    publication_date: str
    action_type: str
    action_description: str
    affected_ticker: str
    affected_company: str
    affected_sector: str
    restricted_countries: list[str]
    severity: int
    html_url: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RevenueImpactAssessment:
    """Revenue impact estimate from export controls."""
    ticker: str
    company_name: str
    sector: str
    china_revenue_pct: float
    estimated_revenue_at_risk_pct: float
    active_restrictions_count: int
    max_severity: int
    latest_action_date: str
    latest_action_title: str
    risk_level: str  # LOW / MEDIUM / HIGH / CRITICAL
    restricted_countries: list[str]
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Revenue exposure data (approximate, updated periodically) ───────────

# These are approximate China/HK revenue percentages from public filings.
# Kept in sync with the ingestion module but used for impact assessment.
_CHINA_REVENUE_ESTIMATES: dict[str, dict[str, Any]] = {
    "NVDA": {
        "name": "NVIDIA",
        "sector": "AI chips",
        "china_revenue_pct": 25.0,
        "total_revenue_b": 60.9,  # FY2024 approximate
        "notes": "China revenue was ~25% pre-restrictions; dropped with A800/H800 bans. "
                 "H20 variant still shipped until further tightening.",
    },
    "AMD": {
        "name": "AMD",
        "sector": "AI chips",
        "china_revenue_pct": 15.0,
        "total_revenue_b": 22.7,
        "notes": "MI250/MI300 AI accelerators subject to export thresholds. "
                 "Lower China exposure than NVDA but growing AI segment at risk.",
    },
    "INTC": {
        "name": "Intel",
        "sector": "semiconductors",
        "china_revenue_pct": 27.0,
        "total_revenue_b": 54.2,
        "notes": "Broad China exposure across PC, server, and foundry. "
                 "Gaudi AI accelerator subject to controls.",
    },
    "ASML": {
        "name": "ASML",
        "sector": "lithography",
        "china_revenue_pct": 29.0,
        "total_revenue_b": 27.6,
        "notes": "EUV banned to China since 2019 (Dutch govt). DUV restrictions "
                 "expanded 2023-2024. Equipment servicing also restricted.",
    },
    "LRCX": {
        "name": "Lam Research",
        "sector": "semiconductor equipment",
        "china_revenue_pct": 30.0,
        "total_revenue_b": 14.9,
        "notes": "Etch and deposition equipment for advanced nodes restricted. "
                 "Highest China revenue % among US semi-equipment makers.",
    },
    "AMAT": {
        "name": "Applied Materials",
        "sector": "semiconductor equipment",
        "china_revenue_pct": 28.0,
        "total_revenue_b": 26.5,
        "notes": "Broad semiconductor equipment portfolio. "
                 "Advanced node equipment banned, mature node equipment still shipped.",
    },
    "KLAC": {
        "name": "KLA Corporation",
        "sector": "semiconductor equipment",
        "china_revenue_pct": 25.0,
        "total_revenue_b": 10.5,
        "notes": "Process control and inspection equipment. "
                 "Advanced node tools restricted.",
    },
    "TSM": {
        "name": "TSMC",
        "sector": "foundry",
        "china_revenue_pct": 10.0,
        "total_revenue_b": 87.1,
        "notes": "Cannot manufacture advanced chips (<7nm) for Chinese customers. "
                 "Direct revenue impact lower but geopolitical risk high.",
    },
    "QCOM": {
        "name": "Qualcomm",
        "sector": "mobile chips",
        "china_revenue_pct": 62.0,
        "total_revenue_b": 38.3,
        "notes": "Highest China revenue % in semis. Mobile chips less restricted "
                 "than AI/HPC but licensing revenue at risk.",
    },
    "AVGO": {
        "name": "Broadcom",
        "sector": "networking chips",
        "china_revenue_pct": 35.0,
        "total_revenue_b": 35.8,
        "notes": "Networking and infrastructure chips. VMware acquisition "
                 "diversifies but hardware still China-exposed.",
    },
    "MU": {
        "name": "Micron",
        "sector": "memory",
        "china_revenue_pct": 25.0,
        "total_revenue_b": 25.1,
        "notes": "China banned Micron from critical infrastructure in 2023 "
                 "(retaliation). HBM for AI training subject to controls.",
    },
    "MRVL": {
        "name": "Marvell Technology",
        "sector": "infrastructure chips",
        "china_revenue_pct": 10.0,
        "total_revenue_b": 5.5,
        "notes": "Lower direct China exposure. Custom AI silicon growing "
                 "but primarily for US hyperscalers.",
    },
}


# ── Helper ───────────────────────────────────────────────────────────────

def _parse_payload(raw: Any) -> dict[str, Any] | None:
    """Parse a raw_payload column value into a dict.

    Parameters:
        raw: raw_payload value (str, dict, or None).

    Returns:
        Parsed dict or None.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


# ── Query Functions ──────────────────────────────────────────────────────

def get_recent_controls(
    engine: Engine, days: int = 90,
) -> list[ExportControlRecord]:
    """Fetch all export control actions from the last N days.

    Parameters:
        engine: SQLAlchemy engine.
        days: Number of days to look back (default: 90).

    Returns:
        List of ExportControlRecord sorted by date descending.
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE sc.name = 'BIS_EXPORT_CONTROLS' "
                "AND rs.obs_date >= :cutoff "
                "AND rs.pull_status = 'SUCCESS' "
                "ORDER BY rs.obs_date DESC, rs.value DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    records: list[ExportControlRecord] = []
    seen_docs: set[str] = set()

    for row in rows:
        payload = _parse_payload(row[3])
        if not payload:
            continue

        doc_num = payload.get("document_number", "")
        ticker = payload.get("affected_ticker", "")
        dedup_key = f"{doc_num}:{ticker}"
        if dedup_key in seen_docs:
            continue
        seen_docs.add(dedup_key)

        records.append(ExportControlRecord(
            document_number=doc_num,
            title=payload.get("title", ""),
            abstract=payload.get("abstract", ""),
            publication_date=str(row[1]),
            action_type=payload.get("action_type", ""),
            action_description=payload.get("action_description", ""),
            affected_ticker=ticker,
            affected_company=payload.get("affected_company", ""),
            affected_sector=payload.get("affected_sector", ""),
            restricted_countries=payload.get("restricted_countries", []),
            severity=int(row[2]) if row[2] else 0,
            html_url=payload.get("html_url", ""),
        ))

    log.info(
        "Fetched {n} export control records in last {d} days",
        n=len(records), d=days,
    )
    return records


def get_controls_for_ticker(
    engine: Engine, ticker: str,
) -> list[ExportControlRecord]:
    """Fetch all stored export control actions for a specific ticker.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol (e.g. 'NVDA', 'ASML').

    Returns:
        List of ExportControlRecord sorted by date descending.
    """
    ticker_upper = ticker.strip().upper()
    pattern = f"EXPORT_CONTROL:{ticker_upper}:%"

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT series_id, obs_date, value, raw_payload "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE sc.name = 'BIS_EXPORT_CONTROLS' "
                "AND rs.series_id LIKE :pattern "
                "AND rs.pull_status = 'SUCCESS' "
                "ORDER BY rs.obs_date DESC"
            ),
            {"pattern": pattern},
        ).fetchall()

    records: list[ExportControlRecord] = []
    for row in rows:
        payload = _parse_payload(row[3])
        if not payload:
            continue

        records.append(ExportControlRecord(
            document_number=payload.get("document_number", ""),
            title=payload.get("title", ""),
            abstract=payload.get("abstract", ""),
            publication_date=str(row[1]),
            action_type=payload.get("action_type", ""),
            action_description=payload.get("action_description", ""),
            affected_ticker=payload.get("affected_ticker", ""),
            affected_company=payload.get("affected_company", ""),
            affected_sector=payload.get("affected_sector", ""),
            restricted_countries=payload.get("restricted_countries", []),
            severity=int(row[2]) if row[2] else 0,
            html_url=payload.get("html_url", ""),
        ))

    log.info(
        "Found {n} export control actions for {t}",
        n=len(records), t=ticker_upper,
    )
    return records


def assess_revenue_impact(
    engine: Engine, ticker: str,
) -> dict[str, Any]:
    """Estimate percentage of revenue at risk from export controls.

    Combines static China revenue exposure data with active restriction
    count and severity to produce a risk assessment.

    For context: NVDA China revenue was ~25% of total before the Oct 2022
    controls. Post-controls, they created the A800/H800 variants (still
    restricted later), then the H20 (further restricted in 2024).

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.

    Returns:
        Dict with revenue impact assessment fields. Returns a minimal
        dict with risk_level='UNKNOWN' if the ticker is not tracked.
    """
    ticker_upper = ticker.strip().upper()

    # Get baseline exposure data
    baseline = _CHINA_REVENUE_ESTIMATES.get(ticker_upper)
    if not baseline:
        return {
            "ticker": ticker_upper,
            "risk_level": "UNKNOWN",
            "notes": f"{ticker_upper} not tracked in export controls module. "
                     "Add to EXPORT_CONTROL_TICKERS if relevant.",
            "active_restrictions_count": 0,
            "estimated_revenue_at_risk_pct": 0.0,
        }

    # Fetch active restrictions from the last year
    controls = get_controls_for_ticker(engine, ticker_upper)
    # Filter to last 365 days for "active" count
    one_year_ago = date.today() - timedelta(days=365)
    recent_controls = [
        c for c in controls
        if c.publication_date >= one_year_ago.isoformat()
    ]

    active_count = len(recent_controls)
    max_severity = max(
        (c.severity for c in recent_controls), default=0,
    )
    latest_date = recent_controls[0].publication_date if recent_controls else ""
    latest_title = recent_controls[0].title if recent_controls else ""

    # Collect all restricted countries
    all_countries: list[str] = []
    for c in recent_controls:
        for country in c.restricted_countries:
            if country not in all_countries:
                all_countries.append(country)

    # Estimate revenue at risk:
    # Start with baseline China revenue %
    base_pct = baseline["china_revenue_pct"]

    # Adjust based on severity and count of active restrictions
    # More restrictions and higher severity = more revenue actually blocked
    if max_severity >= 8:
        # Severe restrictions: assume most China revenue is at risk
        at_risk_pct = base_pct * 0.85
    elif max_severity >= 6:
        # Moderate restrictions: partial impact
        at_risk_pct = base_pct * 0.5
    elif max_severity >= 4:
        # Mild restrictions: license requirements, some still gets through
        at_risk_pct = base_pct * 0.25
    elif active_count > 0:
        at_risk_pct = base_pct * 0.1
    else:
        # No active restrictions found in DB (may be pre-data)
        # Still report baseline exposure as potential risk
        at_risk_pct = base_pct * 0.05

    # Multiple active restrictions compound the effect
    if active_count >= 5:
        at_risk_pct = min(base_pct, at_risk_pct * 1.2)
    elif active_count >= 3:
        at_risk_pct = min(base_pct, at_risk_pct * 1.1)

    at_risk_pct = round(min(base_pct, at_risk_pct), 1)

    # Classify risk level
    if at_risk_pct >= 20:
        risk_level = "CRITICAL"
    elif at_risk_pct >= 10:
        risk_level = "HIGH"
    elif at_risk_pct >= 5:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    assessment = RevenueImpactAssessment(
        ticker=ticker_upper,
        company_name=baseline["name"],
        sector=baseline["sector"],
        china_revenue_pct=base_pct,
        estimated_revenue_at_risk_pct=at_risk_pct,
        active_restrictions_count=active_count,
        max_severity=max_severity,
        latest_action_date=latest_date,
        latest_action_title=latest_title,
        risk_level=risk_level,
        restricted_countries=all_countries,
        notes=baseline.get("notes", ""),
    )

    log.info(
        "Revenue impact for {t}: {pct}% at risk ({level}), "
        "{n} active restrictions",
        t=ticker_upper,
        pct=at_risk_pct,
        level=risk_level,
        n=active_count,
    )

    return assessment.to_dict()
