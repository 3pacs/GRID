"""godview.buyback_pillar — issuer-level MODELED quiet-window God View pillar.

Implements ONLY what is measurable or explicitly modeled (operator
direction, 2026-09-18):

(a) a MODELED 10b5-1/10b-18-style quiet-window calendar per issuer,
    derived from earnings dates present in ``earnings_calendar`` (a
    lazily-created, untracked table --
    ``ingestion/altdata/earnings_calendar.py::_ensure_earnings_table``,
    not in ``schema.sql`` or any Alembic migration; grepped 2026-09-18,
    this is the only earnings/catalyst-dated table found anywhere in this
    codebase). When an issuer has no earnings date in that table, its
    window is unavailable -- there is nothing to derive it from, and this
    module never invents one.
(b) NOTHING ELSE. No dollar amounts, no "% of market in blackout"
    literal -- those require issuer-level repurchase EXECUTION disclosures
    (10-Q/10-K share-repurchase tables via EDGAR), which do not exist
    anywhere in this database. See ``MISSING_INPUT`` below.

**The window is a documented ASSUMPTION, not an SEC-mandated rule for
issuers.** Rule 10b5-1's 2022 amendments impose a cooling-off period on
DIRECTORS AND OFFICERS, quoted from SEC Chair Gensler's statement
(https://www.sec.gov/newsroom/speeches-statements/gensler-insider-trading-20221214,
fetched 2026-09-18):

    "90 days or two days after the release of financial statements,
    whichever is longer, but no more than 120 days"

but the SAME statement is explicit that issuers themselves got no such
mandate:

    "we are not adopting a cooling-off period for issuers"

So there is no SEC rule this module can cite for an ISSUER's own
quiet-window calendar. What it models instead is the common corporate
governance PRACTICE of issuers self-imposing a trading blackout around
earnings (a Rule 10b-18 "safe harbor" compliance convention, not a
10b5-1 timing mandate): ``WINDOW_BEFORE_DAYS`` (14 calendar days before
the earnings date) through ``WINDOW_AFTER_DAYS`` (2 calendar days after)
-- explicit, disclosed, and stated in every row's ``source_ref`` as an
assumption, never presented as a measured fact or an SEC requirement.

``provenance`` is always ``'modeled'``; ``availability_basis`` is always
``'unknown'`` (this is a model, not a published data feed with a release
schedule to observe against or infer from).

**Missing input, named exactly** (operator direction): issuer-level
repurchase EXECUTION data (10-Q/10-K share-repurchase tables via EDGAR)
does not exist in this database. The work that prevents, precisely: any
dollar or share buyback figure, and any "% of [issuer/market] in
blackout" statistic -- this pillar can say WHEN a modeled quiet window
applies to an issuer, and nothing about what that issuer is actually
buying or not buying during it.

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)

PILLAR_NAME = "buyback_blackouts"

WINDOW_STATUS_QUIET = "quiet_window"
WINDOW_BEFORE_DAYS = 14
WINDOW_AFTER_DAYS = 2

_SEC_STATEMENT_URL = "https://www.sec.gov/newsroom/speeches-statements/gensler-insider-trading-20221214"
MODELING_ASSUMPTION_NOTE = (
    f"modeled quiet window = earnings_date -{WINDOW_BEFORE_DAYS}d to +{WINDOW_AFTER_DAYS}d "
    "(common issuer self-imposed Rule 10b-18 compliance PRACTICE, not an SEC-mandated period -- "
    'the SEC\'s own Rule 10b5-1 statement is explicit: "we are not adopting a cooling-off period '
    f'for issuers" ({_SEC_STATEMENT_URL}); the 90-120 day cooling-off period that statement DOES '
    "describe applies to directors/officers, not issuers, and is not what this window models)"
)
MISSING_INPUT = (
    "issuer-level repurchase execution data (10-Q/10-K share-repurchase tables via EDGAR) "
    "does not exist in this database; no dollar or share buyback figure is ever computed here"
)


def compute_window(earnings_date: date) -> tuple[date, date]:
    """Modeled quiet-window boundaries around one earnings date."""
    return (
        earnings_date - timedelta(days=WINDOW_BEFORE_DAYS),
        earnings_date + timedelta(days=WINDOW_AFTER_DAYS),
    )


def window_dates(window_start: date, window_end: date) -> list[date]:
    """Every calendar date in [window_start, window_end], inclusive."""
    n_days = (window_end - window_start).days
    return [window_start + timedelta(days=i) for i in range(n_days + 1)]


@dataclass(frozen=True)
class MaterializationResult:
    status: str
    generation_id: str
    rows_written: int = 0
    issuers_with_data: int = 0
    issuers_discovered: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


def _table_exists(conn: Connection, table_name: str) -> bool:
    try:
        row = conn.execute(text("SELECT to_regclass(:n)").bindparams(n=table_name)).fetchone()
        return bool(row and row[0])
    except Exception:  # noqa: BLE001
        return False


def _read_earnings_dates(conn: Connection, as_of: date) -> dict[str, list[date]]:
    """ticker -> ascending list of known earnings dates, PIT-gated on pull_timestamp.

    Only earnings_calendar rows this system could actually have known
    about by ``as_of`` (pull_timestamp <= as_of, end of day) are used --
    a future earnings date recorded well after as_of would be lookahead.
    """
    as_of_end = datetime.combine(as_of, datetime.max.time())
    rows = conn.execute(
        text(
            "SELECT ticker, earnings_date FROM earnings_calendar "
            "WHERE pull_timestamp <= :cutoff ORDER BY ticker, earnings_date"
        ),
        {"cutoff": as_of_end},
    ).fetchall()
    out: dict[str, list[date]] = {}
    for ticker, earnings_date in rows:
        out.setdefault(ticker, []).append(earnings_date)
    return out


def _existing_calendar_dates(conn: Connection, ticker: str) -> set[date]:
    rows = conn.execute(
        text("SELECT calendar_date FROM issuer_buyback_blackout_windows WHERE ticker = :t"),
        {"t": ticker},
    ).fetchall()
    return {r[0] for r in rows}


def materialize_buyback_pillar(engine: Engine, *, as_of: date | None = None) -> MaterializationResult:
    """Materialize modeled quiet-window rows as one atomic generation.

    Same transactional/idempotent shape as the other pillars. Never
    invents a window for an issuer with no earnings_calendar row.
    """
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            if not _table_exists(conn, "earnings_calendar"):
                raise _EmptyUpstream("earnings_calendar table does not exist")

            earnings_by_ticker = _read_earnings_dates(conn, as_of)
            if not earnings_by_ticker:
                raise _EmptyUpstream("earnings_calendar has no rows known as of this as_of")

            rows_to_insert: list[dict[str, Any]] = []
            issuers_with_data: set[str] = set()

            for ticker, earnings_dates in earnings_by_ticker.items():
                existing = _existing_calendar_dates(conn, ticker)
                for earnings_date in earnings_dates:
                    window_start, window_end = compute_window(earnings_date)
                    source_ref = f"{MODELING_ASSUMPTION_NOTE}; earnings_date={earnings_date.isoformat()}"
                    for calendar_date in window_dates(window_start, window_end):
                        if calendar_date in existing:
                            continue
                        rows_to_insert.append(
                            {
                                "ticker": ticker,
                                "calendar_date": calendar_date,
                                "window_status": WINDOW_STATUS_QUIET,
                                "earnings_date_used": earnings_date,
                                "window_start": window_start,
                                "window_end": window_end,
                                "provenance": "modeled",
                                "availability_basis": "unknown",
                                "generation_id": generation_id,
                                "coverage_fraction": None,
                                "source_ref": source_ref,
                            }
                        )
                        existing.add(calendar_date)
                        issuers_with_data.add(ticker)

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO issuer_buyback_blackout_windows (
                            ticker, calendar_date, window_status, earnings_date_used,
                            window_start, window_end, provenance, availability_basis,
                            generation_id, coverage_fraction, source_ref
                        ) VALUES (
                            :ticker, :calendar_date, :window_status, :earnings_date_used,
                            :window_start, :window_end, :provenance, :availability_basis,
                            :generation_id, :coverage_fraction, :source_ref
                        )
                        ON CONFLICT (ticker, calendar_date) DO NOTHING
                        """
                    ),
                    row,
                )

            coverage_fraction = (
                len(issuers_with_data) / len(earnings_by_ticker) if earnings_by_ticker else None
            )
            record_generation(
                conn, pillar=PILLAR_NAME, generation_id=generation_id,
                status=STATUS_COMPLETE, row_count=len(rows_to_insert),
                coverage_fraction=coverage_fraction,
            )

        status = "SUCCESS" if rows_to_insert else "SUCCESS_NOOP"
        return MaterializationResult(
            status=status, generation_id=generation_id, rows_written=len(rows_to_insert),
            issuers_with_data=len(issuers_with_data), issuers_discovered=len(earnings_by_ticker),
            message=f"{len(rows_to_insert)} new row(s) across {len(issuers_with_data)} issuer(s)",
        )
    except _EmptyUpstream as exc:
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message=str(exc))
    except Exception as exc:  # noqa: BLE001
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="FAILED", generation_id=generation_id, message=str(exc))


class _EmptyUpstream(Exception):
    pass


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    try:
        with engine.begin() as conn:
            record_generation(conn, pillar=PILLAR_NAME, generation_id=generation_id, status=STATUS_FAILED, failure_reason=reason)
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class PillarReadResult:
    state: str  # "never_configured" | "materializer_failed" | "ok"
    rows: list[dict[str, Any]] = field(default_factory=list)
    issuers_with_data: int = 0
    generation_id: str | None = None
    generation_published_at: Any = None


def read_buyback_pillar(conn: Connection, as_of: date) -> PillarReadResult:
    """Every issuer with a modeled quiet-window row covering exactly ``as_of``."""
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed")
        return PillarReadResult(state="never_configured")

    rows = conn.execute(
        text(
            "SELECT ticker, calendar_date, window_status, earnings_date_used, "
            "window_start, window_end, provenance, availability_basis, "
            "generation_id, coverage_fraction, source_ref "
            "FROM issuer_buyback_blackout_windows WHERE calendar_date = :d "
            "ORDER BY ticker"
        ),
        {"d": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    return PillarReadResult(
        state="ok", rows=rows_out, issuers_with_data=len(rows_out),
        generation_id=generation["generation_id"], generation_published_at=generation["published_at"],
    )
