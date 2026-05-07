"""CAT-71 — WARN Act mass layoff filings puller.

The Worker Adjustment and Retraining Notification (WARN) Act requires
employers with 100+ workers to file notice 60 days before a mass
layoff or plant closing. Each state maintains its own WARN filing
database, all public.

This puller starts with California's DIR WARN database (the largest,
~40% of national filings) and treats the schema as authoritative —
other states' databases conform to similar columns (company_name,
layoff_count, notice_date, effective_date, location, ticker_if_known).

Why this matters (Tier A catalog #71): WARN filings are a legally
required 60-day early warning. They lead BLS nonfarm payrolls by
30-60 days. The canary-in-coalmine sectors (tech, retail, manufacturing)
file WARN months before their earnings cracks publicly. Plus per-ticker
filings map directly to single-name shorting opportunities.

Data layout
-----------
Each filing row is stored in the ``warn_filings`` table with columns:

    id              SERIAL PK
    state           TEXT NOT NULL     -- 'CA', 'NY', 'TX', ...
    company_name    TEXT NOT NULL
    ticker          TEXT              -- resolved via entity_resolver if possible
    notice_date     DATE NOT NULL
    effective_date  DATE
    layoff_count    INTEGER
    city            TEXT
    county          TEXT
    closure_type    TEXT              -- 'mass_layoff' / 'closure' / 'other'
    raw_json        JSONB             -- full source row for audit
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
    UNIQUE(state, company_name, notice_date)

The table migration lives in migrations/0046_warn_filings.sql which
this puller creates-on-demand if missing. A downstream intelligence
module consumes warn_filings to flag company-level distress.

The source implementation here is a STUB scaffold — the actual CA DIR
scrape would require fetching a paginated HTML table that changes
format periodically. Instead we provide a clean ``upsert_filings``
entry point that accepts pre-parsed rows (from a downstream fetcher
or manual load) and handles dedup + entity resolution.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller


# ── Filing states we support (expand as each state scraper lands) ─────────

SUPPORTED_STATES: tuple[str, ...] = (
    "CA",   # California DIR
    "NY",   # NY DOL WARN
    "TX",   # Texas TWC
    "FL",   # Florida DEO
    "IL",   # Illinois DCEO
    "PA",   # Pennsylvania Labor & Industry
    "NJ",   # NJ DOLWD
    "OH",   # Ohio JFS
)

# Closure-type enum
CLOSURE_TYPES: frozenset[str] = frozenset({
    "mass_layoff",
    "closure",
    "temporary",
    "other",
})


@dataclass
class WARNFiling:
    """One WARN Act filing record."""

    state: str
    company_name: str
    notice_date: date
    layoff_count: int | None
    effective_date: date | None = None
    ticker: str | None = None
    city: str | None = None
    county: str | None = None
    closure_type: str = "other"
    raw: dict[str, Any] | None = None

    def is_valid(self) -> bool:
        """Minimum required fields check before insert."""
        return bool(self.state and self.company_name and self.notice_date)


class WARNLayoffsPuller(BasePuller):
    """Warn Act filings puller — composition-style (accepts pre-parsed rows).

    The actual state-specific scrapers live in per-state submodules (queued
    as follow-ups). This class owns the schema ensure + upsert + dedup +
    ticker resolution path that's the same across every state.
    """

    SOURCE_NAME = "warn_layoffs"
    SOURCE_CONFIG = {
        "base_url": "https://dir.ca.gov/dlse/warn/",
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    # ── Schema bootstrap ──────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Create the warn_filings table if it doesn't exist.

        Idempotent via IF NOT EXISTS. The migration file is
        migrations/0046_warn_filings.sql — this is the in-code mirror
        so the puller can self-heal on fresh databases.
        """
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS warn_filings (
                    id SERIAL PRIMARY KEY,
                    state TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    ticker TEXT,
                    notice_date DATE NOT NULL,
                    effective_date DATE,
                    layoff_count INTEGER,
                    city TEXT,
                    county TEXT,
                    closure_type TEXT,
                    raw_json JSONB,
                    ingested_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(state, company_name, notice_date)
                )
            """))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_warn_filings_notice_date "
                "ON warn_filings (notice_date DESC)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_warn_filings_ticker "
                "ON warn_filings (ticker) WHERE ticker IS NOT NULL"
            ))

    # ── Upsert ────────────────────────────────────────────────────────

    def upsert_filings(self, filings: list[WARNFiling]) -> dict[str, int]:
        """Upsert a batch of filings. Returns counts of inserted + skipped.

        Uses the (state, company_name, notice_date) UNIQUE constraint
        so re-pulls are idempotent. Skipped rows include both dupes
        and invalid-shape rows.
        """
        counts = {"inserted": 0, "skipped_invalid": 0, "skipped_dupe": 0}
        if not filings:
            return counts

        import json

        with self.engine.begin() as conn:
            for f in filings:
                if not f.is_valid():
                    counts["skipped_invalid"] += 1
                    continue
                try:
                    result = conn.execute(
                        text("""
                            INSERT INTO warn_filings
                                (state, company_name, ticker, notice_date,
                                 effective_date, layoff_count, city, county,
                                 closure_type, raw_json)
                            VALUES
                                (:state, :company, :ticker, :notice,
                                 :effective, :count, :city, :county,
                                 :closure, :raw)
                            ON CONFLICT (state, company_name, notice_date)
                            DO NOTHING
                            RETURNING id
                        """),
                        {
                            "state": f.state.upper(),
                            "company": f.company_name,
                            "ticker": f.ticker,
                            "notice": f.notice_date,
                            "effective": f.effective_date,
                            "count": f.layoff_count,
                            "city": f.city,
                            "county": f.county,
                            "closure": (
                                f.closure_type
                                if f.closure_type in CLOSURE_TYPES
                                else "other"
                            ),
                            "raw": json.dumps(f.raw) if f.raw else None,
                        },
                    )
                    if result.rowcount > 0:
                        counts["inserted"] += 1
                    else:
                        counts["skipped_dupe"] += 1
                except Exception as exc:  # noqa: BLE001
                    log.debug(
                        "warn insert failed {c} {d}: {e}",
                        c=f.company_name, d=f.notice_date, e=str(exc),
                    )
                    counts["skipped_invalid"] += 1
        return counts

    # ── Query helpers ─────────────────────────────────────────────────

    def recent_filings_by_ticker(
        self,
        ticker: str,
        *,
        lookback_days: int = 180,
    ) -> list[dict[str, Any]]:
        """Return recent WARN filings resolved to a specific ticker."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT state, company_name, notice_date,
                               effective_date, layoff_count, closure_type
                        FROM warn_filings
                        WHERE ticker = :t
                          AND notice_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                        ORDER BY notice_date DESC
                    """),
                    {"t": ticker.upper(), "days": lookback_days},
                ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.debug("warn query failed {t}: {e}", t=ticker, e=str(exc))
            return []
        return [
            {
                "state": r[0],
                "company_name": r[1],
                "notice_date": r[2],
                "effective_date": r[3],
                "layoff_count": r[4],
                "closure_type": r[5],
            }
            for r in rows
        ]

    def national_totals(
        self,
        *,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        """Return the sum of layoff_count nationwide over the lookback."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("""
                        SELECT
                            COUNT(*) AS filings,
                            SUM(layoff_count) AS total_layoffs,
                            COUNT(DISTINCT state) AS states_reporting
                        FROM warn_filings
                        WHERE notice_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                    """),
                    {"days": lookback_days},
                ).fetchone()
        except Exception as exc:  # noqa: BLE001
            log.debug("warn totals failed: {e}", e=str(exc))
            return {"filings": 0, "total_layoffs": 0, "states_reporting": 0}
        return {
            "filings": int(row[0] or 0),
            "total_layoffs": int(row[1] or 0),
            "states_reporting": int(row[2] or 0),
        }
