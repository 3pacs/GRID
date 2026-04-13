"""Capital-flow rollup derivations.

Two derived views off the base ``capital_flows`` table:

1. **TTM rows** — for every (actor, flow_type, counterparty, latest
   quarter end) where the trailing four quarterly periods are all
   present, sum them and write a row with ``period_type='ttm'`` and
   ``source_filing='ttm_rollup'``.

2. **Announcement folding** — every ``period_type='announcement'`` row
   carries an event-level capital flow (M&A, buyback announcement,
   debt issuance press-release, etc.). To make those announcements
   show up in the corresponding annual aggregates without losing the
   original event row, we create a *separate* annual row tagged
   ``source_filing='announcement_rolled'``. The dedup CTE in
   ``api/routers/capital_flow.py`` already prefers SEC 10-* filings
   over rolled rows over seeds, so the rolled row is invisible when a
   real 10-K exists for the same period and only fills the gap when
   no SEC row is available yet.

Both functions are idempotent (ON CONFLICT DO UPDATE / DELETE+INSERT)
and use parameterised SQL only. Designed to run daily after the XBRL
ingestor inside the Hermes operator.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Tag values written by this module. Reused by tests + the dedup CTE.
TTM_SOURCE_FILING: str = "ttm_rollup"
TTM_CONFIDENCE: str = "derived"
ROLLED_SOURCE_FILING: str = "announcement_rolled"
ROLLED_CONFIDENCE: str = "derived"

# Required number of trailing quarters to compute a TTM bucket.
TTM_WINDOW_QUARTERS: int = 4


# ── TTM rollup ───────────────────────────────────────────────────────


# We compute TTM in a single set-based statement: window over four
# trailing quarters per (actor, flow_type, direction, counterparty),
# require an exact count of 4 quarters present, and INSERT...ON
# CONFLICT DO UPDATE so re-runs refresh totals as new quarters land.
#
# We use the migration 0024 functional unique index
# ``capital_flows_dedup_nullable_cp_key`` which keys on
# ``(actor_id, fiscal_period, period_type, flow_type,
#   COALESCE(NULLIF(counterparty_id,''), '__none__'), source_filing)``.
# Postgres ON CONFLICT can target a unique index by repeating its
# expression list — so we restate the COALESCE/NULLIF here verbatim.
_TTM_UPSERT_SQL = text(
    """
    -- Dedup base quarterly rows by natural key. The base table can
    -- have multiple source_filing variants for the same logical
    -- (actor, fp, flow_type, cp) — SEC 10-Q + seed + corporate-action
    -- backfill — and we need exactly one row per natural key before
    -- we sum over the window, otherwise the same quarter gets counted
    -- twice and the ON CONFLICT target sees duplicate rows.
    --
    -- Picks one row per natural key with the same priority order the
    -- API dedup CTE uses: SEC 10-* > 8-* > seed > other, then
    -- confidence, then most-recent as_of.
    WITH q_ranked AS (
        SELECT
            actor_id,
            fiscal_period,
            flow_type,
            direction,
            COALESCE(NULLIF(counterparty_id, ''), '__none__') AS cp_key,
            counterparty_id,
            currency,
            amount_usd,
            ROW_NUMBER() OVER (
                PARTITION BY actor_id, fiscal_period, flow_type, direction,
                             COALESCE(NULLIF(counterparty_id, ''), '__none__')
                ORDER BY
                    CASE
                        WHEN source_filing LIKE '10-%' THEN 1
                        WHEN source_filing LIKE '20-%' THEN 2
                        WHEN source_filing LIKE '8-%'  THEN 3
                        WHEN source_filing LIKE 'seed%' THEN 5
                        ELSE 4
                    END,
                    CASE confidence
                        WHEN 'confirmed' THEN 1
                        WHEN 'derived'   THEN 2
                        WHEN 'estimated' THEN 3
                        WHEN 'rumored'   THEN 4
                        WHEN 'inferred'  THEN 5
                        ELSE 6
                    END,
                    as_of DESC NULLS LAST,
                    id DESC
            ) AS rk
        FROM capital_flows
        WHERE period_type = 'quarter'
          AND amount_usd IS NOT NULL
    ),
    q AS (
        SELECT actor_id, fiscal_period, flow_type, direction, cp_key,
               counterparty_id, currency, amount_usd
        FROM q_ranked
        WHERE rk = 1
    ),
    windowed AS (
        SELECT
            actor_id,
            flow_type,
            direction,
            cp_key,
            counterparty_id,
            currency,
            fiscal_period,
            amount_usd,
            COUNT(*) OVER w AS n_quarters,
            SUM(amount_usd) OVER w AS ttm_amount,
            MIN(fiscal_period) OVER w AS earliest_in_window
        FROM q
        WINDOW w AS (
            PARTITION BY actor_id, flow_type, direction, cp_key
            ORDER BY fiscal_period
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        )
    ),
    ttm AS (
        SELECT
            actor_id,
            fiscal_period,
            flow_type,
            direction,
            counterparty_id,
            -- Use the latest quarter's currency. In practice all four
            -- trailing quarters for one actor share a reporting
            -- currency, so this is unambiguous.
            currency,
            ttm_amount AS amount_usd
        FROM windowed
        WHERE n_quarters = :window
          -- Reject windows with gaps: 4 trailing quarters span ~9
          -- months from oldest start-of-quarter to newest end-of-
          -- quarter. We allow up to 320 days to absorb fiscal-year
          -- offset oddities while still catching missing-quarter holes.
          AND fiscal_period - earliest_in_window <= 320
    )
    INSERT INTO capital_flows (
        actor_id, fiscal_period, period_type, flow_type, direction,
        amount_usd, counterparty_id, source_filing, confidence,
        currency, as_of
    )
    SELECT
        actor_id,
        fiscal_period,
        'ttm',
        flow_type,
        direction,
        amount_usd,
        counterparty_id,
        :source_filing,
        :confidence,
        COALESCE(currency, 'USD'),
        NOW()
    FROM ttm
    ON CONFLICT (
        actor_id,
        fiscal_period,
        period_type,
        flow_type,
        (COALESCE(NULLIF(counterparty_id, ''), '__none__')),
        source_filing
    ) DO UPDATE SET
        amount_usd = EXCLUDED.amount_usd,
        direction  = EXCLUDED.direction,
        confidence = EXCLUDED.confidence,
        currency   = EXCLUDED.currency,
        as_of      = NOW()
    """
)


def compute_ttm(engine: Engine) -> int:
    """Build trailing-twelve-month rollup rows from quarterly data.

    Returns the number of TTM rows written/refreshed.
    """
    with engine.begin() as conn:
        result = conn.execute(
            _TTM_UPSERT_SQL,
            {
                "window": TTM_WINDOW_QUARTERS,
                "source_filing": TTM_SOURCE_FILING,
                "confidence": TTM_CONFIDENCE,
            },
        )
        rowcount = result.rowcount or 0
    log.info("capital_flow_rollups.compute_ttm: {n} ttm rows", n=rowcount)
    return int(rowcount)


# ── Announcement folding ─────────────────────────────────────────────


# For each announcement row, find the fiscal year it falls into and
# emit/refresh an annual row tagged ``announcement_rolled``. The dedup
# CTE in api/routers/capital_flow.py orders SEC 10-* > others > seed,
# so this row only surfaces when no real 10-K row exists for the same
# (actor, year, flow_type, counterparty).
#
# The fiscal-year end-date we attribute to is the December 31st of the
# announcement year. This is a deliberate simplification: the API
# layer formats periods as ``FYxxxx`` so the day-of-month is cosmetic
# and using calendar-year-end keeps the rolled row from accidentally
# colliding with a real 10-K row (which usually ends on a non-Dec-31
# date for actors with fiscal years offset from the calendar).
#
# We sum announcement amounts inside the same year so two M&A
# announcements in the same year for the same target/flow-type roll
# into a single annual rolled row.
_ROLL_UPSERT_SQL = text(
    """
    -- Group ALL announcements for an (actor, year, flow_type, cp_key)
    -- into a single rolled row. We pick the most-common counterparty
    -- spelling and currency via MAX as a deterministic tiebreaker so
    -- the conflict-target key (which keys on cp_key, NOT
    -- counterparty_id) sees exactly one row per group.
    WITH ann AS (
        SELECT
            actor_id,
            (date_trunc('year', fiscal_period) + INTERVAL '1 year - 1 day')::date
                AS fy_end,
            flow_type,
            -- Direction is functionally determined by flow_type but
            -- we still pick one deterministically for the INSERT.
            MAX(direction) AS direction,
            COALESCE(NULLIF(counterparty_id, ''), '__none__') AS cp_key,
            MAX(NULLIF(counterparty_id, '')) AS counterparty_id,
            MAX(currency) AS currency,
            SUM(amount_usd) AS amount_usd
        FROM capital_flows
        WHERE period_type = 'announcement'
          AND amount_usd IS NOT NULL
          AND fiscal_period IS NOT NULL
        GROUP BY
            actor_id,
            fy_end,
            flow_type,
            cp_key
    )
    INSERT INTO capital_flows (
        actor_id, fiscal_period, period_type, flow_type, direction,
        amount_usd, counterparty_id, source_filing, confidence,
        currency, as_of
    )
    SELECT
        actor_id,
        fy_end,
        'annual',
        flow_type,
        direction,
        amount_usd,
        counterparty_id,
        :source_filing,
        :confidence,
        COALESCE(currency, 'USD'),
        NOW()
    FROM ann
    ON CONFLICT (
        actor_id,
        fiscal_period,
        period_type,
        flow_type,
        (COALESCE(NULLIF(counterparty_id, ''), '__none__')),
        source_filing
    ) DO UPDATE SET
        amount_usd = EXCLUDED.amount_usd,
        direction  = EXCLUDED.direction,
        confidence = EXCLUDED.confidence,
        currency   = EXCLUDED.currency,
        as_of      = NOW()
    """
)


def fold_announcements(engine: Engine) -> int:
    """Aggregate announcement rows into annual ``announcement_rolled`` rows.

    The original ``period_type='announcement'`` rows are NEVER mutated.
    Instead a parallel annual row is written/refreshed with
    ``source_filing='announcement_rolled'`` so the dedup CTE in the
    API layer can decide precedence:

        SEC 10-K  >  announcement_rolled  >  seed

    Returns the number of annual rolled rows written/refreshed.
    """
    with engine.begin() as conn:
        result = conn.execute(
            _ROLL_UPSERT_SQL,
            {
                "source_filing": ROLLED_SOURCE_FILING,
                "confidence": ROLLED_CONFIDENCE,
            },
        )
        rowcount = result.rowcount or 0
    log.info(
        "capital_flow_rollups.fold_announcements: {n} rolled rows",
        n=rowcount,
    )
    return int(rowcount)


# ── Orchestrator ─────────────────────────────────────────────────────


def run_all(engine: Engine) -> dict[str, Any]:
    """Run every rollup. Returns a stats dict for telemetry."""
    stats: dict[str, Any] = {}
    try:
        stats["ttm_rows"] = compute_ttm(engine)
    except Exception as exc:
        log.error("compute_ttm failed: {e}", e=str(exc))
        stats["ttm_rows"] = 0
        stats["ttm_error"] = str(exc)
    try:
        stats["rolled_rows"] = fold_announcements(engine)
    except Exception as exc:
        log.error("fold_announcements failed: {e}", e=str(exc))
        stats["rolled_rows"] = 0
        stats["rolled_error"] = str(exc)
    stats["completed_at"] = date.today().isoformat()
    return stats
