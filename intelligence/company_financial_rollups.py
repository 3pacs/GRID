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

Durable TTM recompute tracking (fable-daily-intel-sql-tasks, 2026-09-20
follow-up — replaces the earlier fixed ``TTM_LOOKBACK_DAYS=3`` window).

**Corrected-row signal, established from code, not assumed:** the only
writer of ``period_type='quarter'`` rows is
``ingestion/altdata/sec_xbrl_financials.py::_write_rows`` (confirmed by
grepping every ``INSERT INTO capital_flows`` site;
``ingestion/altdata/corporate_actions_parser.py`` only ever writes
``period_type='announcement'``, and ``scripts/load_supply_capital_seed.py``
is an offline one-off seed script, not part of the daily ingest path).
``_write_rows`` DELETEs the exact
``(actor_id, fiscal_period, period_type, flow_type, source_filing)`` row
(if any) and then plain-INSERTs a fresh one with ``as_of = NOW()`` on
*every* call — there is no ``ON CONFLICT DO NOTHING`` short-circuit. That
means ``as_of`` moves forward on a re-ingested/corrected quarterly row
exactly the same way it does on a brand-new one: a correction to an OLD
fiscal period is indistinguishable, at the ``as_of`` level, from a new
row. So a single scalar **watermark** — the maximum ``as_of`` among
``period_type='quarter'`` rows already processed by a successful
``compute_ttm`` call — is sufficient to detect both cases; no per-actor
content fingerprint is needed (that fallback would only be required if
corrections did NOT move any monotonic column, which is not what the code
shows).

**Persistence:** the watermark is a single ISO-8601 string (or ``None``
before the first successful run), small enough to live directly on
``OperatorState.capital_flow_ttm_watermark`` (scripts/hermes_health.py),
serialised/hydrated the same "only if currently unset" way as the other
daily-intel ledger fields — no new table, no migration. A tiny table was
considered and rejected: there is exactly one scalar of state for the
whole rollup (not one row per actor), so a table would only add a
migration and a query for no correctness benefit over the existing
``OperatorState`` snapshot path every other piece of daily-intel ledger
state already uses.

**Advance-after-write:** ``compute_ttm`` runs its UPSERT inside a single
``engine.begin()`` transaction and only computes/returns the new
watermark value (``MAX(as_of)`` over the exact set of quarter rows the
UPSERT just considered) after that transaction has committed
successfully. If the UPSERT raises, the transaction rolls back and the
exception propagates BEFORE any new watermark is computed or returned —
the caller (``run_all`` / the daily-intel task) never sees an updated
value to persist, so a failed run leaves the persisted watermark exactly
where it was and the next call recomputes the identical actor set. See
``compute_ttm``'s own docstring for the exact contract.
"""

from __future__ import annotations

from datetime import date
from typing import Any, NamedTuple

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
    -- Bound the recompute to actors with a quarterly row newer than the
    -- persisted watermark (fable-daily-intel-sql-tasks, 2026-09-20
    -- follow-up — see the module docstring and compute_ttm's docstring
    -- for the durable-tracking design). This is what keeps the
    -- ROW_NUMBER/window computation below off the full ~310k-row quarter
    -- table on every call; an actor with no quarterly row newer than
    -- :watermark already has a correct TTM row from a previous run.
    -- :watermark IS NULL means "no watermark persisted yet" (first run /
    -- explicit full-recompute) and matches every quarter row.
    WITH changed_actors AS (
        SELECT DISTINCT actor_id
        FROM capital_flows
        WHERE period_type = 'quarter'
          AND amount_usd IS NOT NULL
          AND (
            CAST(:watermark AS timestamptz) IS NULL
            OR as_of > CAST(:watermark AS timestamptz)
          )
    ),
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
    q_ranked AS (
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
          -- Still needs each changed actor's FULL quarterly history (not
          -- just the new row) to sum a correct trailing-4-quarter window
          -- — only the ACTOR SET is bounded, not the per-actor lookback.
          AND actor_id IN (SELECT actor_id FROM changed_actors)
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

# Companion query: the new watermark to persist after a successful
# compute_ttm run is MAX(as_of) over the exact same predicate the
# changed_actors CTE above used — i.e. "how far did this run actually
# look". Run inside the SAME transaction as the UPSERT (see compute_ttm)
# so it reflects a consistent snapshot with what was just written.
_TTM_NEW_WATERMARK_SQL = text(
    """
    SELECT MAX(as_of) FROM capital_flows
    WHERE period_type = 'quarter'
      AND amount_usd IS NOT NULL
      AND (
        CAST(:watermark AS timestamptz) IS NULL
        OR as_of > CAST(:watermark AS timestamptz)
      )
    """
)


class TtmResult(NamedTuple):
    """``compute_ttm``'s return value.

    ``watermark`` is the value the CALLER should persist next (e.g. onto
    ``OperatorState.capital_flow_ttm_watermark``) — see ``compute_ttm``'s
    docstring for the advance-after-write contract. It is:
      * the new ``MAX(as_of)`` this run considered, as an ISO-8601 string,
        when at least one quarter row qualified;
      * the INPUT ``watermark`` unchanged when nothing qualified (nothing
        to advance past).
    ``compute_ttm`` never returns a value at all when the UPSERT raises —
    the exception propagates instead, so a failed run cannot produce a
    watermark to advance past.
    """
    rows_written: int
    watermark: str | None


def compute_ttm(engine: Engine, watermark: str | None = None) -> TtmResult:
    """Build trailing-twelve-month rollup rows from quarterly data.

    ``watermark`` is an ISO-8601 timestamp string — the durable, restart-
    safe tracking cursor described in the module docstring — or ``None``.
    An actor is included in this run's recompute when it has a
    ``period_type='quarter'`` row with ``as_of`` strictly greater than
    ``watermark`` (or unconditionally when ``watermark`` is ``None``: no
    watermark persisted yet, i.e. the first-ever run, or an explicit
    caller-requested full recompute — e.g. a manual backfill via
    ``scripts/run_capital_flow_rollups.py`` after a bulk correction).
    Because ``as_of`` moves forward on every write from the XBRL ingestor
    — new row OR corrected row, regardless of which fiscal period it
    corrects (see the module docstring's "corrected-row signal" finding)
    — this single scalar watermark catches both a plain gap (an actor
    idle beyond any fixed lookback window) and a late correction to an
    OLD fiscal period, with no per-actor state needed.

    **Advance-after-write contract**: the UPSERT and the "what's the new
    watermark" query both run inside ONE ``engine.begin()`` transaction.
    If the UPSERT raises (e.g. a cancelled statement), the transaction
    rolls back and the exception propagates out of this function BEFORE
    any watermark is computed — the caller never receives (and therefore
    can never persist) an advanced watermark for a run that didn't
    actually commit its TTM rows. A retry with the SAME unchanged
    watermark therefore recomputes exactly the same actor set. On
    success, the returned ``TtmResult.watermark`` is safe to persist
    immediately — the TTM rows it corresponds to are already committed.

    Returns a ``TtmResult(rows_written, watermark)``.
    """
    with engine.begin() as conn:
        result = conn.execute(
            _TTM_UPSERT_SQL,
            {
                "window": TTM_WINDOW_QUARTERS,
                "source_filing": TTM_SOURCE_FILING,
                "confidence": TTM_CONFIDENCE,
                "watermark": watermark,
            },
        )
        rowcount = result.rowcount or 0
        new_watermark_row = conn.execute(
            _TTM_NEW_WATERMARK_SQL, {"watermark": watermark},
        ).fetchone()

    new_max_as_of = new_watermark_row[0] if new_watermark_row else None
    if new_max_as_of is not None:
        effective_watermark = (
            new_max_as_of.isoformat()
            if hasattr(new_max_as_of, "isoformat")
            else str(new_max_as_of)
        )
    else:
        # Nothing qualified this run (no quarter row newer than
        # `watermark`) — keep the watermark exactly where it was; there
        # is nothing new to advance past.
        effective_watermark = watermark

    log.info(
        "capital_flow_rollups.compute_ttm: {n} ttm rows (watermark {w} -> {nw})",
        n=rowcount, w=watermark, nw=effective_watermark,
    )
    return TtmResult(rows_written=int(rowcount), watermark=effective_watermark)


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


def run_all(engine: Engine, ttm_watermark: str | None = None) -> dict[str, Any]:
    """Run every rollup. Returns a stats dict for telemetry.

    ``ttm_watermark`` is the persisted durable-tracking cursor (see the
    module docstring) — pass ``state.capital_flow_ttm_watermark`` from
    ``OperatorState``, or ``None`` before the first successful run.

    ``compute_ttm`` and ``fold_announcements`` are attempted
    INDEPENDENTLY — a failure in one does not skip the other. This
    mirrors production evidence (docs/handoffs/2026-09-20/
    fable-daily-intel-sql-tasks.md) that a cancelled ``compute_ttm``
    statement still let ``fold_announcements`` complete and write its
    rolled rows in the same cycle.

    This function itself never raises. The returned dict's ``"ok"`` key
    is ``True`` only when BOTH sub-steps succeeded — the caller (see
    ``scripts/hermes_operator.py::_daily_intel_capital_flow_rollups``)
    MUST check it and raise/propagate a failure when it is ``False``, so
    a partial failure (e.g. ``compute_ttm`` cancelled but
    ``fold_announcements`` fine) is recorded as a genuine task FAILURE by
    the daily-intel ledger, never as ``done``/``done_late``.

    ``stats["ttm_watermark"]`` is the value the caller should persist
    next: the new watermark ``compute_ttm`` returned on success, or the
    INPUT ``ttm_watermark`` unchanged when ``compute_ttm`` failed (its
    transaction never committed, so there is nothing new to advance
    past — see ``compute_ttm``'s docstring).
    """
    stats: dict[str, Any] = {"ttm_watermark_in": ttm_watermark}
    try:
        ttm_result = compute_ttm(engine, ttm_watermark)
        stats["ttm_rows"] = ttm_result.rows_written
        stats["ttm_watermark"] = ttm_result.watermark
        stats["ttm_ok"] = True
    except Exception as exc:
        log.error("compute_ttm failed: {e}", e=str(exc))
        stats["ttm_rows"] = 0
        stats["ttm_error"] = str(exc)
        stats["ttm_ok"] = False
        # Never advance past a failed write (see compute_ttm's
        # advance-after-write contract) — the next call must recompute
        # the identical actor set.
        stats["ttm_watermark"] = ttm_watermark
    try:
        stats["rolled_rows"] = fold_announcements(engine)
        stats["fold_ok"] = True
    except Exception as exc:
        log.error("fold_announcements failed: {e}", e=str(exc))
        stats["rolled_rows"] = 0
        stats["rolled_error"] = str(exc)
        stats["fold_ok"] = False
    stats["completed_at"] = date.today().isoformat()
    stats["ok"] = bool(stats["ttm_ok"] and stats["fold_ok"])
    return stats
