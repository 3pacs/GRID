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

Durable TTM recompute tracking — per-actor content fingerprint
(fable-daily-intel-sql-tasks, 2026-09-20 SECOND follow-up. Supersedes the
first follow-up's scalar ``as_of`` watermark, which the controller
established is NOT commit-order safe — see "Why the scalar watermark was
replaced" below).

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
*every* call — there is no ``ON CONFLICT DO NOTHING`` short-circuit.

**Why the scalar watermark was replaced:** the first follow-up read that
finding as "as_of moves forward on every write, so a single scalar
high-water-mark over as_of is sufficient." That is false in the presence
of real concurrency. In PostgreSQL, ``NOW()`` (what ``_write_rows`` binds
into ``as_of``) is the *transaction start* time, not the commit time. A
writer transaction that STARTS before a ``compute_ttm`` run takes its
snapshot but COMMITS after it carries an ``as_of`` that is older than the
watermark the run persists — ``as_of > watermark`` then skips that row
FOREVER, not just for one cycle. A second, narrower hole: two rows with
the EXACT same ``as_of`` (a same-instant double-write, or two actors
corrected in the same transaction) fall on either side of a strict ``>``
depending only on which one happened to set the watermark first. Neither
hole is about "how stale" a row is — both are about commit ORDER, which
``as_of`` does not encode. Proved with two independent PostgreSQL
connections/transactions (real concurrency, not sequential seeding) in
``tests/test_capital_flow_rollups_pg.py``
(``test_late_committing_earlier_as_of_is_recomputed`` and
``test_equal_as_of_timestamps_do_not_skip_the_second_actor``). A third,
unrelated hole in the old design: it only ever *added* TTM rows — an
actor whose quarterly rows were deleted or reclassified (no new row, no
``as_of`` movement at all) kept a stale ``ttm`` row forever, with nothing
in ``compute_ttm`` that would ever remove it.

**What replaced it — a per-actor quarter-set fingerprint (Design B from
the controller's brief).** ``capital_flows_ttm_state`` (migration
``capital_flow_ttm_state_20260920``) holds one row per actor:
``(actor_id, quarter_fingerprint, computed_at)``. Every ``compute_ttm``
call recomputes, for EVERY actor with at least one
``period_type='quarter'`` row (a full aggregate scan of the quarter
table — see "Cost" below), an ``md5(string_agg(...))`` fingerprint over
these columns of each qualifying quarterly row, in this deterministic
order: ``fiscal_period, flow_type, direction, counterparty_id (coalesced
to '__none__'), amount_usd, currency, source_filing, confidence``. An
actor is "dirty" (recomputed this run) exactly when that live fingerprint
differs (``IS DISTINCT FROM``, so NULL-safe) from what is stored for it —
including an actor that now has ZERO quarterly rows (live fingerprint is
NULL) when it previously had a non-NULL one stored, and an actor with no
stored row at all (first time seen). This is commit-order safe BY
CONSTRUCTION: it compares committed table content on each run, never a
timestamp, so it cannot matter whether a competing writer's transaction
started before or after this run's snapshot — only whether its write had
already committed BY THE TIME this run's query executed. A late commit
that this run's query missed is, by definition, still uncommitted content
as far as this run is concerned; the NEXT run's query will see it and
flag the actor dirty then. Two equal-``as_of`` rows are just two more
bytes in (or out of) the same string_agg — ``>`` vs ``>=`` never enters
into it.

**Why not also PostgreSQL snapshot-visibility tracking (Design A,
``pg_visible_in_snapshot``):** the brief's Option A is real and
commit-order safe for INSERTs, but it does not see deletes or
reclassifications (a `DELETE` or an `UPDATE ... SET period_type` leaves no
new, not-yet-visible ``xmin`` to catch) — cases 3(a)/3(b)/3(c) below.
Since Option B (the fingerprint) already covers inserts, corrections,
deletions, and reclassifications with ONE mechanism and no second code
path to keep in sync, Option A would only add a second dependency
(PostgreSQL 13+, confirmed satisfied — production griddb runs PostgreSQL
15 per ``docs/SERVER-SERVICES.md``) for a case the fingerprint already
covers. Not used.

**Cost, disclosed:** unlike the scalar watermark (which could restrict
even the "is anything dirty" check to rows newer than a cursor), the
fingerprint must read every ``period_type='quarter'`` row on every call
to know whether anything changed — there is no cheaper way to detect a
content change than reading the content. This is a single
``GROUP BY actor_id`` aggregate scan (no per-row round trips, no window
function over the full table — that part of the original bounding still
only runs over the DIRTY actors' full history, same as before). At the
~310k-row scale referenced in the first follow-up's docs this is a
sub-second sequential scan for a once-daily job; if the table grows large
enough for this to matter, the natural follow-up is materializing
``current_fp`` incrementally (e.g. a trigger-maintained fingerprint column
on ``capital_flows`` itself) rather than reverting to a commit-order-unsafe
shortcut.

**Stale TTM rows are now deleted, not left to survive as current
(closes the third hole above):** for every dirty actor, any of its
existing ``period_type='ttm', source_filing='ttm_rollup'`` rows whose
``(flow_type, direction, counterparty_id, fiscal_period)`` group is NOT
present in this run's freshly computed 4-quarter windows is DELETED in
the same statement/transaction as the write — covers a quarter row
deleted without replacement, a quarter row reclassified to a different
``period_type`` (its group simply stops qualifying), and an actor whose
every quarter row is gone (every one of its stale groups is removed).
See ``tests/test_capital_flow_rollups_pg.py``'s case-3 tests.

**What the fingerprint does NOT cover (the one honest remaining gap):**
it is scoped to ``period_type='quarter'`` rows and to the eight columns
listed above. A change to any OTHER column of an existing quarter row
(e.g. hand-editing ``id`` — not reachable via SQL — or a direct
``UPDATE`` that touches only some column outside that list, such as a
free-text audit note column if one is ever added) would not move the
fingerprint and would not be detected. In practice this is NOT reachable
through the one real write path (`_write_rows` always DELETEs and
re-INSERTs the full row, touching every fingerprinted column identically
either way), so this gap only matters for a HYPOTHETICAL future writer
that mutates a quarter row in place outside those eight columns, or an
operator hand-editing the table directly with SQL. No trigger is used
here (Design C from the brief), so there is no "triggers disabled"
exception to state — the whole mechanism is a plain query run by
``compute_ttm`` itself, always active.

**Persistence / the ``watermark`` parameter is now vestigial:**
``compute_ttm``'s per-actor dirty state lives ENTIRELY in
``capital_flows_ttm_state`` and is written in the SAME transaction as the
TTM rows it governs — there is no second, caller-owned cursor for
correctness to depend on any more. The ``watermark`` parameter and
``TtmResult.watermark`` / ``stats["ttm_watermark"]`` are kept, unused for
gating, purely so existing callers (``scripts/hermes_operator.py``,
``scripts/run_capital_flow_rollups.py``, ``OperatorState.
capital_flow_ttm_watermark``) do not need to change: the returned value is
now just "the wall-clock time this run completed", persisted the same way
as before, read for display/telemetry only. This also makes the old
"crash between DB commit and operator-state persistence" scenario
strictly harmless in a stronger sense than before: since nothing outside
this function's own transaction is needed to gate the next run, a crash
before ``OperatorState.capital_flow_ttm_watermark`` gets written changes
nothing about what the next run recomputes — see
``test_replay_after_crash_before_watermark_persist_is_a_harmless_noop`` in
``tests/test_capital_flow_rollups_pg.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
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


# One statement, one snapshot, one transaction:
#   1. current_fp      — fingerprint every actor's CURRENT quarter rows.
#   2. changed_actors   — actors whose fingerprint differs from the
#                          durably stored one (capital_flows_ttm_state),
#                          NULL-safe, including "had rows before, has
#                          none now" and "never seen before".
#   3. q_ranked/q/windowed/ttm — the same trailing-4-quarter rollup
#                          logic as before, scoped to dirty actors only.
#   4. deleted_stale    — removes any of a dirty actor's existing ttm
#                          rows whose group no longer qualifies (case 3).
#   5. state_upsert     — durably records the fingerprint just computed
#                          for every dirty actor, in the SAME statement/
#                          transaction as the ttm write and the stale
#                          delete, so a crash can never see one without
#                          the others.
#   6. The outer INSERT — writes/refreshes the qualifying ttm rows.
#
# All three data-modifying CTEs (deleted_stale, state_upsert, and the
# outer INSERT) run against the ONE query-level snapshot PostgreSQL takes
# for this whole statement — see "Data-Modifying Statements in WITH" in
# the PostgreSQL docs: they are always executed to completion exactly
# once, whether or not the primary query's output reads them. Computing
# current_fp/changed_actors only ONCE per call (not once per DML
# statement) is what keeps state_upsert from racing a concurrent write
# that lands between two separate statements — a second `conn.execute()`
# call for the state upsert would re-read a possibly-different snapshot
# and could durably record a fingerprint that this call's OWN ttm write
# never actually matched.
_TTM_UPSERT_SQL = text(
    """
    WITH current_fp AS (
        SELECT
            actor_id,
            md5(string_agg(
                fiscal_period::text || '|' || flow_type || '|' || direction || '|' ||
                COALESCE(NULLIF(counterparty_id, ''), '__none__') || '|' ||
                amount_usd::text || '|' || COALESCE(currency, '') || '|' ||
                source_filing || '|' || confidence,
                ',' ORDER BY fiscal_period, flow_type, direction,
                             COALESCE(NULLIF(counterparty_id, ''), '__none__'),
                             source_filing, confidence
            )) AS fp
        FROM capital_flows
        WHERE period_type = 'quarter'
          AND amount_usd IS NOT NULL
        GROUP BY actor_id
    ),
    -- Dirty actor set: FULL OUTER JOIN so an actor with rows now but no
    -- stored state (never seen), and an actor with stored state but no
    -- rows now (everything deleted), are BOTH caught — a plain (inner or
    -- left) join would silently miss one side.
    changed_actors AS (
        SELECT
            COALESCE(c.actor_id, s.actor_id) AS actor_id,
            c.fp AS new_fingerprint
        FROM current_fp c
        FULL OUTER JOIN capital_flows_ttm_state s ON s.actor_id = c.actor_id
        WHERE c.fp IS DISTINCT FROM s.quarter_fingerprint
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
          -- just the changed row) to sum a correct trailing-4-quarter
          -- window — only the ACTOR SET is bounded to dirty actors, not
          -- the per-actor lookback.
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
    ),
    -- Case 3: a dirty actor's existing ttm row/group that no longer has
    -- a qualifying 4-quarter window (quarter deleted, reclassified away,
    -- or the whole actor's quarterly history gone) does not survive as
    -- if it were still current — delete it here, in the same statement.
    deleted_stale AS (
        DELETE FROM capital_flows cf
        USING changed_actors ca
        WHERE cf.actor_id = ca.actor_id
          AND cf.period_type = 'ttm'
          AND cf.source_filing = :source_filing
          AND NOT EXISTS (
              SELECT 1 FROM ttm t
              WHERE t.actor_id = cf.actor_id
                AND t.flow_type = cf.flow_type
                AND t.direction = cf.direction
                AND COALESCE(NULLIF(t.counterparty_id, ''), '__none__')
                    = COALESCE(NULLIF(cf.counterparty_id, ''), '__none__')
                AND t.fiscal_period = cf.fiscal_period
          )
        RETURNING cf.id
    ),
    -- Durably record the fingerprint just computed for every dirty actor
    -- — including NULL for an actor with no quarter rows left, so a
    -- future re-insertion is still detected as a change from NULL, and a
    -- repeat with no rows stays stable (NULL IS NOT DISTINCT FROM NULL).
    state_upsert AS (
        INSERT INTO capital_flows_ttm_state (actor_id, quarter_fingerprint, computed_at)
        SELECT actor_id, new_fingerprint, NOW()
        FROM changed_actors
        ON CONFLICT (actor_id) DO UPDATE SET
            quarter_fingerprint = EXCLUDED.quarter_fingerprint,
            computed_at = EXCLUDED.computed_at
        RETURNING actor_id
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


class TtmResult(NamedTuple):
    """``compute_ttm``'s return value.

    ``watermark`` is now purely informational (the wall-clock time this
    run completed, ISO-8601) — kept only so existing callers that persist
    it (``OperatorState.capital_flow_ttm_watermark`` via
    ``scripts/hermes_operator.py``) do not need to change. It plays NO
    role in deciding which actors get recomputed; that durable state
    lives entirely in ``capital_flows_ttm_state`` and is written in the
    SAME transaction as the ttm rows themselves — see the module
    docstring's "Persistence / the watermark parameter is now vestigial"
    section. ``compute_ttm`` never returns a value at all when the UPSERT
    raises — the exception propagates instead, so a failed run cannot
    produce a watermark to advance past (its transaction rolled back, so
    ``capital_flows_ttm_state`` is exactly where it was too).
    """
    rows_written: int
    watermark: str | None


def compute_ttm(engine: Engine, watermark: str | None = None) -> TtmResult:
    """Build trailing-twelve-month rollup rows from quarterly data.

    Which actors get recomputed is decided ENTIRELY by comparing a
    content fingerprint of each actor's current ``period_type='quarter'``
    rows against the fingerprint durably stored for it in
    ``capital_flows_ttm_state`` from the last successful run — see the
    module docstring for the full design and why it replaced a scalar
    ``as_of`` watermark (commit-order unsafe: PostgreSQL's ``NOW()`` is
    transaction-START time, so a late-committing writer can carry an
    ``as_of`` a watermark already passed, and equal ``as_of`` values have
    the same hole under strict ``>``).

    ``watermark`` is accepted and returned for backward-compatible call
    signatures only — see ``TtmResult.watermark``'s docstring. It has NO
    effect on which actors are recomputed.

    **Transaction contract (unchanged in spirit from the first
    follow-up):** the fingerprint comparison, the stale-ttm delete, the
    per-actor state upsert, and the ttm row write all run inside ONE
    ``engine.begin()`` transaction, built from a SINGLE SQL statement so
    they all see the same query snapshot (see the SQL's own comment for
    why a second statement would race). If the UPSERT raises, the whole
    transaction rolls back and the exception propagates out of this
    function — neither the ttm rows nor ``capital_flows_ttm_state`` move,
    so a retry (with any watermark, since it is now unused) recomputes
    exactly the same dirty set.

    Returns a ``TtmResult(rows_written, watermark)``.
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

    # Informational only (see TtmResult docstring) — always populated on
    # success so existing callers/tests that expect a non-None value on a
    # successful run keep working.
    effective_watermark = datetime.now(timezone.utc).isoformat()

    log.info(
        "capital_flow_rollups.compute_ttm: {n} ttm rows written/refreshed "
        "(watermark param {w} -> informational {nw}; dirty-actor gating is "
        "now fingerprint-based, see capital_flows_ttm_state)",
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

    ``ttm_watermark`` is accepted for backward-compatible call signatures
    only — ``compute_ttm``'s dirty-actor gating no longer uses it (see
    that function's docstring and the module docstring's "Persistence /
    the watermark parameter is now vestigial" section). It is passed
    through unchanged to ``compute_ttm`` and the returned
    ``stats["ttm_watermark"]`` is the new informational value on success,
    or the INPUT ``ttm_watermark`` unchanged when ``compute_ttm`` failed.

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
        # docstring) — the next call must recompute the identical dirty
        # set, which is guaranteed by capital_flows_ttm_state never
        # having moved (same rolled-back transaction).
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
