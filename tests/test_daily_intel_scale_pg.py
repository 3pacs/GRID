"""Representative-scale timing harness for the daily-intel SQL tasks
(fable-daily-intel-sql-tasks, 2026-09-20, task 2).

Only runs when BOTH ``GRID_TEST_DB_URL`` and ``GRID_SCALE_TESTS=1`` are set
— skipped otherwise, including when a default local Postgres happens to be
reachable, because this seeds well over a million rows and must never run
against a database nobody explicitly opted in for.

    GRID_TEST_DB_URL=postgresql://user:pass@host:5432/scratch_db \\
    GRID_SCALE_TESTS=1 DB_PASSWORD=x PYTHONUTF8=1 \\
    python -m pytest tests/test_daily_intel_scale_pg.py -q -s

(``-s`` is required to see the printed timing table and EXPLAIN output.)

Production numbers this harness is sized off (read-only, 2026-09-20):
``capital_flows`` ~470k rows total — quarter 309,915 / annual 162,298 /
ttm 4,200 / announcement 105 — ~238MB. ``fundamental_divergence`` 36k
rows. ``raw_series`` is >1e9 rows overall but the divergence loaders only
ever touch the price series of the universe (``YF:{TICKER}:close``).

Budgets (imported from scripts/hermes_operator.py so this test tracks the
real, currently-configured values rather than a second hand-typed copy):
``DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S`` (90s), and
``DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S`` (60s). The DB statement
timeout (120s) is reproduced on this test's own engine exactly the way
db.py's get_engine() sets it — ``-c statement_timeout=120000`` — so a
runaway statement here gets cancelled the same way production would
cancel it, rather than silently hanging past what production allows.

Seeding
-------
* ~5,000 synthetic TTM actors (``scale_ttm_00000``..``scale_ttm_04999``),
  ~60 ``period_type='quarter'`` rows each (6 flow types x 10 fiscal
  quarters) — realistic direction (revenue in; cogs/opex/capex/dividends/
  buybacks out) and currency spread (~1 in 12 actors is an all-non-USD
  IFRS-style filer). ≈300,000 rows total — matches production's
  quarter-row count.
* The REAL production ticker universe (``analysis.sector_map.SECTOR_MAP``
  via ``fundamental_divergence._load_universe()``, ~1,268 tickers as of
  2026-09-20 — the ~1,500 target in the brief) gets ``period_type='annual'``
  rows (revenue/cogs/dividends/buybacks x enough fiscal years to land
  close to ~160,000 rows) and ``raw_series`` daily ``YF:{TICKER}:close``
  SUCCESS rows (comfortably over ``MIN_PRICE_OBS`` and
  ``PRICE_LOOKBACK_DAYS``). Using the REAL universe (not synthetic
  tickers) means ``snapshot_all`` end-to-end in this harness exercises
  the actual production code path (``_load_universe()`` takes no
  arguments — it always reads ``SECTOR_MAP``), not a stand-in.
* A modest ~150 ``period_type='announcement'`` rows (production has only
  105 — no need to synthesize more than that order of magnitude).

All bulk loads use ``COPY ... FROM STDIN`` via psycopg2 (chunked to bound
memory), never per-row round trips, to keep seeding itself well under the
2-3 minute budget the task allows for it.

Every seeded row is tagged with a distinctive ``source_filing`` (or, for
``raw_series``, a throwaway ``source_catalog`` row created just for this
test) so cleanup is exact regardless of whatever else lives in the
disposable database.
"""
from __future__ import annotations

import io
import os
import time
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from intelligence.company_financial_rollups import compute_ttm, fold_announcements
from intelligence.fundamental_divergence import (
    MIN_PRICE_OBS,
    PRICE_LOOKBACK_DAYS,
    _load_batch_fundamentals,
    _load_batch_price_cagrs,
    _load_universe,
    snapshot_all,
)
from scripts.hermes_operator import (
    DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S,
    DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S,
)

_GRID_TEST_DB_URL = os.environ.get("GRID_TEST_DB_URL")
_SCALE_TESTS_ON = os.environ.get("GRID_SCALE_TESTS") == "1"

pytestmark = pytest.mark.skipif(
    not (_GRID_TEST_DB_URL and _SCALE_TESTS_ON),
    reason=(
        "representative-scale seeding (well over 1M rows) only runs when "
        "GRID_TEST_DB_URL and GRID_SCALE_TESTS=1 are both explicitly set"
    ),
)

STATEMENT_TIMEOUT_MS = int(os.environ.get("GRID_DB_STATEMENT_TIMEOUT_MS", "120000"))
STATEMENT_TIMEOUT_S = STATEMENT_TIMEOUT_MS / 1000.0

# ── Seed sizing ─────────────────────────────────────────────────────

N_TTM_ACTORS = 5000
QUARTER_ENDS = [
    date(2023, 12, 31), date(2024, 3, 31), date(2024, 6, 30), date(2024, 9, 30),
    date(2024, 12, 31), date(2025, 3, 31), date(2025, 6, 30), date(2025, 9, 30),
    date(2025, 12, 31), date(2026, 3, 31),
]  # 10 trailing fiscal quarters
QUARTER_FLOW_TYPES = [
    ("revenue", "in"), ("cogs", "out"), ("opex", "out"),
    ("capex", "out"), ("dividends", "out"), ("buybacks", "out"),
]  # 6 types x 10 quarters = 60 rows/actor
NON_USD_CURRENCIES = ["EUR", "GBP", "JPY", "CAD"]

ANNUAL_FLOW_TYPES = ["revenue", "cogs", "dividends", "buybacks"]  # 4 types
TARGET_ANNUAL_ROWS = 160_000

N_PRICE_DAYS = PRICE_LOOKBACK_DAYS + 60  # > lookback, comfortably > MIN_PRICE_OBS
assert N_PRICE_DAYS + 1 > MIN_PRICE_OBS, "seeded price history must clear MIN_PRICE_OBS"
_DAILY_RATE = 0.0005

N_ANNOUNCEMENT_ROWS = 150

_QUARTER_SOURCE_FILING = "10-Q scale_test 20260920"
_ANNUAL_SOURCE_FILING = "10-K scale_test 20260920"
_ANNOUNCEMENT_SOURCE_FILING = "8-K scale_test 20260920"


# ── Engine ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def scale_engine() -> Engine:
    engine = create_engine(
        _GRID_TEST_DB_URL,
        pool_size=5,
        max_overflow=5,
        connect_args={"options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"},
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            row = conn.execute(
                text("SELECT to_regclass('public.capital_flows')"),
            ).fetchone()
            if not row or not row[0]:
                pytest.skip("capital_flows table missing on GRID_TEST_DB_URL")
    except Exception as exc:
        pytest.skip(f"GRID_TEST_DB_URL not reachable: {exc}")
    yield engine
    engine.dispose()


# ── COPY helpers ─────────────────────────────────────────────────────


def _copy_field(v) -> str:
    if v is None:
        return "\\N"
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    s = str(v)
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def _copy_rows(engine: Engine, table: str, columns: list[str], rows, chunk_size: int = 100_000) -> int:
    """Bulk-load ``rows`` (iterable of tuples) into ``table`` via
    ``COPY ... FROM STDIN``, chunked so memory stays bounded on very
    large seeds (the raw_series price load in particular)."""
    total = 0
    cols_sql = ", ".join(columns)
    buf = io.StringIO()
    n_in_buf = 0

    def _flush():
        nonlocal buf, n_in_buf, total
        if n_in_buf == 0:
            return
        buf.seek(0)
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.copy_expert(
                f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT text)", buf,
            )
            raw.commit()
        finally:
            raw.close()
        total += n_in_buf
        buf = io.StringIO()
        n_in_buf = 0

    for row in rows:
        buf.write("\t".join(_copy_field(v) for v in row))
        buf.write("\n")
        n_in_buf += 1
        if n_in_buf >= chunk_size:
            _flush()
    _flush()
    return total


# ── Seeders ──────────────────────────────────────────────────────────


def _seed_quarter_rows(engine: Engine) -> int:
    as_of = datetime.now(timezone.utc)

    def _rows():
        for i in range(N_TTM_ACTORS):
            actor_id = f"scale_ttm_{i:05d}"
            currency = NON_USD_CURRENCIES[i % len(NON_USD_CURRENCIES)] if i % 12 == 0 else "USD"
            for fp in QUARTER_ENDS:
                for flow_type, direction in QUARTER_FLOW_TYPES:
                    base = 1_000_000.0 * (1 + (i % 97))
                    scale = {
                        "revenue": 1.0, "cogs": 0.55, "opex": 0.18,
                        "capex": 0.09, "dividends": 0.04, "buybacks": 0.05,
                    }[flow_type]
                    amount = round(base * scale * (1.0 + 0.01 * QUARTER_ENDS.index(fp)), 2)
                    yield (
                        actor_id, fp, "quarter", flow_type, direction,
                        amount, None, _QUARTER_SOURCE_FILING, "confirmed",
                        currency, as_of,
                    )

    return _copy_rows(
        engine, "capital_flows",
        ["actor_id", "fiscal_period", "period_type", "flow_type", "direction",
         "amount_usd", "counterparty_id", "source_filing", "confidence",
         "currency", "as_of"],
        _rows(),
    )


def _seed_annual_rows(engine: Engine, tickers: list[str]) -> int:
    as_of = datetime.now(timezone.utc)
    n_years = max(4, round(TARGET_ANNUAL_ROWS / (len(ANNUAL_FLOW_TYPES) * len(tickers))))
    year_ends = [date(y, 12, 31) for y in range(2026 - n_years, 2026)]

    def _rows():
        for ti, ticker in enumerate(tickers):
            base = 500_000_000.0 * (1 + (ti % 211))
            for yi, fp in enumerate(year_ends):
                growth = (1.0 + 0.06) ** yi  # steady 6%/yr revenue growth
                for flow_type in ANNUAL_FLOW_TYPES:
                    direction = "in" if flow_type == "revenue" else "out"
                    scale = {
                        "revenue": 1.0, "cogs": 0.6, "dividends": 0.03, "buybacks": 0.04,
                    }[flow_type]
                    amount = round(base * scale * growth, 2)
                    yield (
                        ticker, fp, "annual", flow_type, direction,
                        amount, None, _ANNUAL_SOURCE_FILING, "confirmed",
                        "USD", as_of,
                    )

    return _copy_rows(
        engine, "capital_flows",
        ["actor_id", "fiscal_period", "period_type", "flow_type", "direction",
         "amount_usd", "counterparty_id", "source_filing", "confidence",
         "currency", "as_of"],
        _rows(),
        chunk_size=50_000,
    )


def _seed_announcement_rows(engine: Engine) -> int:
    as_of = datetime.now(timezone.utc)

    def _rows():
        for i in range(N_ANNOUNCEMENT_ROWS):
            actor_id = f"scale_ttm_{i % 50:05d}"
            fp = date(2020 + (i % 5), 1 + (i % 12), 15)
            amount = round(1_000_000.0 * (1 + i), 2)
            yield (
                actor_id, fp, "announcement", "acquisitions", "out",
                amount, f"scale_cp_{i % 7}", _ANNOUNCEMENT_SOURCE_FILING,
                "derived", "USD", as_of,
            )

    return _copy_rows(
        engine, "capital_flows",
        ["actor_id", "fiscal_period", "period_type", "flow_type", "direction",
         "amount_usd", "counterparty_id", "source_filing", "confidence",
         "currency", "as_of"],
        _rows(),
    )


def _seed_source_catalog_row(engine: Engine) -> int:
    name = f"scale_test_yf_{int(time.time())}"
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO source_catalog (
                    name, base_url, cost_tier, latency_class, pit_available,
                    revision_behavior, trust_score, priority_rank, active
                ) VALUES (
                    :name, 'https://example.invalid/scale-test', 'FREE', 'EOD',
                    FALSE, 'NEVER', 'MED', 999, TRUE
                )
                RETURNING id
                """,
            ).bindparams(name=name),
        ).fetchone()
        return int(row[0])


def _seed_price_rows(engine: Engine, tickers: list[str], source_id: int, as_of: date) -> int:
    def _rows():
        for ti, ticker in enumerate(tickers):
            series_id = f"YF:{ticker}:close"
            base = 50.0 + (ti % 400)
            for d in range(N_PRICE_DAYS, -1, -1):
                obs_date = as_of - timedelta(days=d)
                value = round(base * (1.0 + _DAILY_RATE) ** (N_PRICE_DAYS - d), 4)
                yield (
                    series_id, source_id, obs_date, as_of, value, "SUCCESS",
                )

    return _copy_rows(
        engine, "raw_series",
        ["series_id", "source_id", "obs_date", "pull_timestamp", "value", "pull_status"],
        _rows(),
        chunk_size=200_000,
    )


# ── EXPLAIN ANALYZE reconstructions (read-only; no DML) ───────────────

_FINGERPRINT_EXPLAIN_SQL = text(
    """
    EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
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
    WHERE period_type = 'quarter' AND amount_usd IS NOT NULL
    GROUP BY actor_id
    """
)

_TTM_WINDOW_EXPLAIN_SQL = text(
    """
    EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
    WITH q_ranked AS (
        SELECT
            actor_id, fiscal_period, flow_type, direction,
            COALESCE(NULLIF(counterparty_id, ''), '__none__') AS cp_key,
            counterparty_id, currency, amount_usd,
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
                    as_of DESC NULLS LAST, id DESC
            ) AS rk
        FROM capital_flows
        WHERE period_type = 'quarter'
          AND amount_usd IS NOT NULL
          AND actor_id = ANY(:actor_ids)
    ),
    q AS (
        SELECT actor_id, fiscal_period, flow_type, direction, cp_key,
               counterparty_id, currency, amount_usd
        FROM q_ranked WHERE rk = 1
    ),
    windowed AS (
        SELECT
            actor_id, flow_type, direction, cp_key, counterparty_id, currency,
            fiscal_period, amount_usd,
            COUNT(*) OVER w AS n_quarters,
            SUM(amount_usd) OVER w AS ttm_amount,
            MIN(fiscal_period) OVER w AS earliest_in_window
        FROM q
        WINDOW w AS (
            PARTITION BY actor_id, flow_type, direction, cp_key
            ORDER BY fiscal_period
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        )
    )
    SELECT actor_id, fiscal_period, flow_type, direction, currency, ttm_amount
    FROM windowed
    WHERE n_quarters = 4 AND fiscal_period - earliest_in_window <= 320
    """
)


# ── The harness ──────────────────────────────────────────────────────


def test_representative_scale_timings(scale_engine: Engine):
    engine = scale_engine
    tickers = sorted({st.ticker for st in _load_universe()})
    assert tickers, (
        "analysis.sector_map.SECTOR_MAP produced no tickers — "
        "_load_universe() is what snapshot_all uses internally, so an "
        "empty universe here would make the 'snapshot_all end to end' "
        "measurement meaningless (0 rows, near-0 time)."
    )

    as_of = date(2026, 9, 20)
    timings: dict[str, float] = {}
    source_id: int | None = None

    try:
        t0 = time.perf_counter()
        n_q = _seed_quarter_rows(engine)
        n_a = _seed_annual_rows(engine, tickers)
        n_ann = _seed_announcement_rows(engine)
        source_id = _seed_source_catalog_row(engine)
        n_p = _seed_price_rows(engine, tickers, source_id, as_of)
        seed_elapsed = time.perf_counter() - t0
        print(
            f"\n[seed] {n_q} quarter rows, {n_a} annual rows ({len(tickers)} "
            f"tickers), {n_ann} announcement rows, {n_p} raw_series rows "
            f"in {seed_elapsed:.1f}s"
        )
        assert seed_elapsed < 240, (
            f"seeding took {seed_elapsed:.1f}s, over the 2-3 minute budget "
            f"the task allows for it (some slack given to 240s here)"
        )

        # (a) compute_ttm first run — every actor first-time-seen.
        t0 = time.perf_counter()
        result_a = compute_ttm(engine)
        timings["ttm_first_run"] = time.perf_counter() - t0

        # (b) compute_ttm steady state — nothing dirty.
        t0 = time.perf_counter()
        result_b = compute_ttm(engine)
        timings["ttm_steady_state"] = time.perf_counter() - t0

        # (c) compute_ttm after changing 50 actors.
        changed_actor_ids = [f"scale_ttm_{i:05d}" for i in range(50)]
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE capital_flows
                    SET amount_usd = amount_usd * 1.05
                    WHERE actor_id = ANY(:ids)
                      AND period_type = 'quarter'
                      AND fiscal_period = :fp
                    """,
                ).bindparams(ids=changed_actor_ids, fp=QUARTER_ENDS[-1]),
            )
        t0 = time.perf_counter()
        result_c = compute_ttm(engine)
        timings["ttm_after_50_changed"] = time.perf_counter() - t0

        # (d) fold_announcements.
        t0 = time.perf_counter()
        fold_announcements(engine)
        timings["fold_announcements"] = time.perf_counter() - t0

        # (e) divergence: batched loaders + snapshot_all end to end.
        with engine.connect() as conn:
            t0 = time.perf_counter()
            fund_cache = _load_batch_fundamentals(conn, tickers)
            timings["divergence_load_batch_fundamentals"] = time.perf_counter() - t0

            t0 = time.perf_counter()
            price_cache = _load_batch_price_cagrs(conn, tickers, as_of)
            timings["divergence_load_batch_price_cagrs"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        snapshot_result = snapshot_all(engine, as_of=as_of)
        timings["divergence_snapshot_all"] = time.perf_counter() - t0

        # ── EXPLAIN (ANALYZE, BUFFERS) — printed for the coordinator ────
        with engine.connect() as conn:
            plan_rows = conn.execute(_FINGERPRINT_EXPLAIN_SQL).fetchall()
            print("\n[EXPLAIN] fingerprint aggregate (current_fp):")
            for r in plan_rows:
                print("  " + r[0])

            plan_rows = conn.execute(
                _TTM_WINDOW_EXPLAIN_SQL.bindparams(actor_ids=changed_actor_ids),
            ).fetchall()
            print("\n[EXPLAIN] TTM window query (dirty-actor scope, 50 actors):")
            for r in plan_rows:
                print("  " + r[0])

        # ── Compact timing table ─────────────────────────────────────
        print("\n[timing table]")
        print(f"{'phase':38s} {'seconds':>10s}  budget")
        print(f"{'ttm_first_run':38s} {timings['ttm_first_run']:10.2f}  "
              f"< {DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S}s")
        print(f"{'ttm_steady_state':38s} {timings['ttm_steady_state']:10.2f}  "
              f"< {DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S}s")
        print(f"{'ttm_after_50_changed':38s} {timings['ttm_after_50_changed']:10.2f}  "
              f"< {DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S}s")
        print(f"{'fold_announcements':38s} {timings['fold_announcements']:10.2f}  "
              f"< {DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S}s")
        print(f"{'divergence_load_batch_fundamentals':38s} "
              f"{timings['divergence_load_batch_fundamentals']:10.2f}  "
              f"< {DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S}s")
        print(f"{'divergence_load_batch_price_cagrs':38s} "
              f"{timings['divergence_load_batch_price_cagrs']:10.2f}  "
              f"< {DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S}s")
        print(f"{'divergence_snapshot_all':38s} "
              f"{timings['divergence_snapshot_all']:10.2f}  "
              f"< {DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S}s")
        print(f"(result_a rows_written={result_a.rows_written}, "
              f"result_b rows_written={result_b.rows_written}, "
              f"result_c rows_written={result_c.rows_written}, "
              f"fund_cache non-null={sum(1 for v in fund_cache.values() if v)}, "
              f"price_cache non-null={sum(1 for v in price_cache.values() if v is not None)}, "
              f"snapshot_result={snapshot_result})")

        # ── Assertions — under budget with headroom ─────────────────
        assert timings["ttm_first_run"] < DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S, (
            f"compute_ttm first run took {timings['ttm_first_run']:.1f}s, "
            f"over the {DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S}s rollups "
            f"budget — see this test's EXPLAIN ANALYZE output above for "
            f"where the time is going and propose the smallest index "
            f"(added to migrations/versions/capital_flow_ttm_state_"
            f"20260920.py) before raising the budget."
        )
        assert timings["ttm_steady_state"] < DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S
        assert timings["ttm_after_50_changed"] < DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S
        assert timings["fold_announcements"] < DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S
        assert timings["divergence_load_batch_fundamentals"] < DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S
        assert timings["divergence_load_batch_price_cagrs"] < DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S
        assert timings["divergence_snapshot_all"] < DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S

        # Every individual statement must also stay under the DB's own
        # statement_timeout — a phase that got cancelled would have raised
        # already, but this restates the ceiling explicitly for the
        # printed table's sake.
        for name, secs in timings.items():
            assert secs < STATEMENT_TIMEOUT_S, (
                f"{name} took {secs:.1f}s, at or over the {STATEMENT_TIMEOUT_S}s "
                f"statement_timeout ceiling itself — should have raised a "
                f"QueryCanceled before getting here"
            )

        # steady state must be dramatically cheaper than a full recompute —
        # sanity check that fingerprint gating is actually doing its job,
        # not just "happens to be under budget too".
        assert timings["ttm_steady_state"] < timings["ttm_first_run"], (
            "steady-state compute_ttm (nothing dirty) was not faster than "
            "the first full recompute — fingerprint-based dirty-actor "
            "gating may not be short-circuiting correctly at this scale"
        )
    finally:
        # ── Cleanup — every seeded row, nothing else ──────────────────
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM fundamental_divergence WHERE as_of = :d").bindparams(d=as_of),
            )
            conn.execute(
                text(
                    "DELETE FROM capital_flows WHERE source_filing IN (:q, :a, :ann)",
                ).bindparams(q=_QUARTER_SOURCE_FILING, a=_ANNUAL_SOURCE_FILING, ann=_ANNOUNCEMENT_SOURCE_FILING),
            )
            conn.execute(
                text("DELETE FROM capital_flows_ttm_state WHERE actor_id LIKE 'scale_ttm_%'"),
            )
            if source_id is not None:
                conn.execute(
                    text("DELETE FROM raw_series WHERE source_id = :sid").bindparams(sid=source_id),
                )
                conn.execute(
                    text("DELETE FROM source_catalog WHERE id = :sid").bindparams(sid=source_id),
                )
