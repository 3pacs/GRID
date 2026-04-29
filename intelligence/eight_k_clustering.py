"""CAT-61 — 8-K unusual clustering + item category tracker.

Companies file 8-Ks to disclose material events. Each 8-K has a bolded
"Item" reference that categorizes the event (Item 1.01, 2.02, 5.02,
etc.). Individual 8-Ks are usually benign, but CLUSTERS — when a
company files 3+ 8-Ks within a 30-day window — almost always precede
bad news.

This module:

  1. Scans the sec_filings table for 8-K filings by item category
  2. Detects cluster events (3+ in 30 days for one ticker)
  3. Scores cluster severity by item category mix
  4. Emits a ClusterAlert per flagged ticker

Item category severity weights (higher = more concerning):

  1.01 Entry into material definitive agreement   — 0.4 (M&A, partnerships)
  1.02 Termination of material agreement          — 0.8 (contract loss)
  2.02 Results of operations                       — 0.5 (earnings/guidance)
  2.04 Triggering events of financial obligations  — 1.0 (default trigger)
  2.06 Material impairments                        — 0.9 (write-downs)
  3.01 Notice of delisting                         — 1.0 (exchange red flag)
  3.02 Unregistered sales of equity                — 0.6 (dilution)
  4.01 Changes in registrant's certifying accountant — 0.9 (auditor change)
  4.02 Non-reliance on previously issued financials — 1.0 (restatement)
  5.02 Departure of directors or officers          — 0.5 (per-officer)
  5.07 Submission of matters to vote of securityholders — 0.2 (routine)
  7.01 Regulation FD disclosure                    — 0.3 (routine)
  8.01 Other events                                 — 0.2 (catch-all)

Why this matters (Tier A catalog #61): cluster analysis beats individual
8-K monitoring because a single high-item filing is often already priced
in. Multiple filings within a tight window signal genuine operational
stress or an unfolding disclosure cascade.

All analysis functions are pure — no DB I/O. A thin wrapper reads
sec_filings and feeds the analyzer.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Item category severity map ────────────────────────────────────────────

ITEM_SEVERITY: dict[str, float] = {
    "1.01": 0.4,  # Entry into material agreement
    "1.02": 0.8,  # Termination of material agreement
    "1.03": 0.7,  # Bankruptcy or receivership
    "2.01": 0.5,  # Completion of acquisition/disposition
    "2.02": 0.5,  # Results of operations (earnings/guidance)
    "2.03": 0.6,  # Creation of direct financial obligation
    "2.04": 1.0,  # Triggering events of financial obligations (default)
    "2.05": 0.7,  # Costs associated with exit/disposal activities
    "2.06": 0.9,  # Material impairments
    "3.01": 1.0,  # Notice of delisting
    "3.02": 0.6,  # Unregistered sales of equity (dilution)
    "3.03": 0.5,  # Material modification of rights of security holders
    "4.01": 0.9,  # Auditor change
    "4.02": 1.0,  # Non-reliance on prior financials (restatement)
    "5.01": 0.5,  # Change in control
    "5.02": 0.5,  # Officer/director departure
    "5.03": 0.3,  # Amendments to articles of incorporation
    "5.07": 0.2,  # Annual meeting vote results (routine)
    "7.01": 0.3,  # Reg FD disclosure (routine)
    "8.01": 0.2,  # Other events (catch-all)
    "9.01": 0.1,  # Financial statements and exhibits
}

# Cluster detection window
_CLUSTER_WINDOW_DAYS = 30
# Minimum filings to flag as a cluster
_MIN_CLUSTER_SIZE = 3
# Severity thresholds on the composite score
_SEVERITY_WARN = 0.5
_SEVERITY_ELEVATED = 1.0
_SEVERITY_CRITICAL = 2.0


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class EightKFiling:
    """One 8-K filing record."""

    ticker: str
    filed_date: date
    item_codes: list[str]       # e.g. ['2.02', '9.01']
    accession: str | None = None
    form_type: str = "8-K"


@dataclass(frozen=True)
class ClusterAlert:
    """One cluster event flagged by the detector."""

    ticker: str
    window_start: date
    window_end: date
    filing_count: int
    unique_items: list[str]
    composite_severity: float
    severity_label: str         # 'warn' / 'elevated' / 'critical'
    top_item: str | None
    top_item_severity: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "filing_count": self.filing_count,
            "unique_items": list(self.unique_items),
            "composite_severity": round(self.composite_severity, 4),
            "severity_label": self.severity_label,
            "top_item": self.top_item,
            "top_item_severity": round(self.top_item_severity, 4),
        }


# ── Pure-function analysis ────────────────────────────────────────────────


def score_filing(filing: EightKFiling) -> float:
    """Score a single 8-K by its item category severities (max)."""
    if not filing.item_codes:
        return 0.0
    return max(
        ITEM_SEVERITY.get(code, 0.2) for code in filing.item_codes
    )


def classify_severity(composite: float) -> str:
    """Map a composite severity score to a label."""
    if composite >= _SEVERITY_CRITICAL:
        return "critical"
    if composite >= _SEVERITY_ELEVATED:
        return "elevated"
    if composite >= _SEVERITY_WARN:
        return "warn"
    return "neutral"


def detect_clusters(
    filings: list[EightKFiling],
    *,
    window_days: int = _CLUSTER_WINDOW_DAYS,
    min_cluster_size: int = _MIN_CLUSTER_SIZE,
) -> list[ClusterAlert]:
    """Scan a list of 8-K filings and return ticker-level cluster alerts.

    For each ticker, checks every filing date as a potential cluster
    center and counts filings within ``window_days``. Returns one alert
    per ticker (the highest-severity window found).
    """
    by_ticker: dict[str, list[EightKFiling]] = defaultdict(list)
    for f in filings:
        by_ticker[f.ticker].append(f)

    alerts: list[ClusterAlert] = []
    for ticker, ticker_filings in by_ticker.items():
        if len(ticker_filings) < min_cluster_size:
            continue
        # Sort by date
        sorted_filings = sorted(ticker_filings, key=lambda x: x.filed_date)

        # Sliding-window pass: for each filing, find the window ending
        # there with the most filings + highest severity.
        best_alert: ClusterAlert | None = None

        for i, center in enumerate(sorted_filings):
            window_end = center.filed_date
            window_start = window_end - timedelta(days=window_days)
            window_filings = [
                f for f in sorted_filings
                if window_start <= f.filed_date <= window_end
            ]
            if len(window_filings) < min_cluster_size:
                continue

            # Compute composite severity as sum of per-filing severity
            # (sum, not mean — a dense cluster of bad news accumulates)
            composite = sum(score_filing(f) for f in window_filings)

            # Collect unique item codes
            unique_items: list[str] = []
            seen: set[str] = set()
            for f in window_filings:
                for code in f.item_codes:
                    if code not in seen:
                        unique_items.append(code)
                        seen.add(code)

            # Top item by severity
            top_item = None
            top_sev = 0.0
            for code in unique_items:
                sev = ITEM_SEVERITY.get(code, 0.2)
                if sev > top_sev:
                    top_sev = sev
                    top_item = code

            alert = ClusterAlert(
                ticker=ticker,
                window_start=window_start,
                window_end=window_end,
                filing_count=len(window_filings),
                unique_items=unique_items,
                composite_severity=composite,
                severity_label=classify_severity(composite),
                top_item=top_item,
                top_item_severity=top_sev,
            )
            if best_alert is None or alert.composite_severity > best_alert.composite_severity:
                best_alert = alert

        if best_alert is not None:
            alerts.append(best_alert)

    # Sort by composite severity descending
    alerts.sort(key=lambda a: -a.composite_severity)
    return alerts


# ── DB wrapper ───────────────────────────────────────────────────────────


def _read_recent_filings(
    engine: Engine,
    *,
    lookback_days: int = 90,
) -> list[EightKFiling]:
    """Read recent 8-K filings from sec_filings table.

    Expects columns (ticker, filing_date, form_type, item_codes TEXT[])
    where item_codes is a postgres array of strings. Non-fatal on missing
    table or missing column.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT ticker, filing_date, item_codes, accession_number
                    FROM sec_filings
                    WHERE form_type = '8-K'
                      AND filing_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                      AND ticker IS NOT NULL
                    ORDER BY filing_date ASC
                    """
                ),
                {"days": int(lookback_days)},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("8k filings read failed: {e}", e=str(exc))
        return []

    filings: list[EightKFiling] = []
    for r in rows:
        ticker = (r[0] or "").strip().upper()
        if not ticker:
            continue
        filed = r[1]
        item_codes = list(r[2] or [])
        filings.append(EightKFiling(
            ticker=ticker,
            filed_date=filed,
            item_codes=[str(c) for c in item_codes],
            accession=str(r[3]) if r[3] else None,
        ))
    return filings


def scan_for_clusters(
    engine: Engine,
    *,
    lookback_days: int = 90,
) -> list[ClusterAlert]:
    """Read 8-K filings from the DB and return cluster alerts."""
    filings = _read_recent_filings(engine, lookback_days=lookback_days)
    alerts = detect_clusters(filings)
    if alerts:
        log.warning(
            "8-K clusters: {n} tickers flagged ({c} critical, {e} elevated)",
            n=len(alerts),
            c=sum(1 for a in alerts if a.severity_label == "critical"),
            e=sum(1 for a in alerts if a.severity_label == "elevated"),
        )
    return alerts
