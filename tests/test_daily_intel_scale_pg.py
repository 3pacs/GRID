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
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import importlib

# `contracts/__init__` re-exports the *function* `emit`, so `import contracts.emit as x`
# binds that function, not the module; resolve the module explicitly.
contracts_emit = importlib.import_module("contracts.emit")
import intelligence.fundamental_divergence as fd
from intelligence.company_financial_rollups import compute_ttm, fold_announcements
from intelligence.fundamental_divergence import (
    MIN_PRICE_OBS,
    PRICE_LOOKBACK_DAYS,
    _load_batch_fundamentals,
    _load_batch_price_cagrs,
    _load_universe,
    _table_exists,
    compute_divergence,
    snapshot_all,
)
from scripts.hermes_operator import (
    DAILY_INTEL_CAPITAL_FLOW_ROLLUPS_BUDGET_S,
    DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S,
)

_CONTRACTS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts" / "migrations" / "20260411_contracts_infrastructure.sql"
)


def _apply_contracts_migration(engine: Engine) -> None:
    """Apply the CREATE TABLE/INDEX statements from
    ``scripts/migrations/20260411_contracts_infrastructure.sql`` against
    the scratch DB so the real ``contracts.emit`` path (used by the
    emitter measurement below) has ``contracts_audit`` to write into.
    Idempotent — every statement in that file is ``IF NOT EXISTS``."""
    sql_text = _CONTRACTS_MIGRATION_PATH.read_text()
    with engine.begin() as conn:
        for stmt in sql_text.split(";"):
            stmt = stmt.strip()
            if not stmt or stmt.upper() in ("BEGIN", "COMMIT"):
                continue
            conn.execute(text(stmt))


def _emit_engine_matches_scratch_db(emit_engine: Engine, scratch_engine: Engine) -> bool:
    """True when ``contracts.emit``'s own config-resolved engine
    (``contracts.emit._get_engine()`` -> ``api.dependencies.get_db_engine()``,
    built from ``DB_*`` config env vars, NOT the ``GRID_TEST_DB_URL`` engine
    the rest of this harness uses) points at the same physical database as
    the scratch DB. The emitter writes through its own engine/connection
    pool by design (see snapshot_all's "SYNTH-26" comment) — this harness
    must not run the real emit path unless that separate engine is
    confirmed to land on the same disposable database, or it risks writing
    audit rows/pg_notify traffic to whatever DB is otherwise configured."""
    e_url, s_url = emit_engine.url, scratch_engine.url
    return (
        (e_url.host or "") == (s_url.host or "")
        and (e_url.port or None) == (s_url.port or None)
        and (e_url.database or "") == (s_url.database or "")
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


def test_representative_scale_timings(scale_engine: Engine, monkeypatch: pytest.MonkeyPatch):
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
    emitter_run_start: datetime | None = None
    contracts_audit_present = False

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

        # (e2) core snapshot_all — batched loads + batched upsert only.
        # ``_emit_divergence_signal`` is monkeypatched to a counting
        # no-op so this isolates the SQL-task-owned code path from the
        # pre-existing per-event contracts.emit fanout measured
        # separately below (fable-daily-intel-sql-tasks, 2026-09-20
        # follow-up).
        core_emit_calls = {"n": 0}

        def _noop_emit_divergence_signal(r):
            core_emit_calls["n"] += 1

        with monkeypatch.context() as m:
            m.setattr(fd, "_emit_divergence_signal", _noop_emit_divergence_signal)
            t0 = time.perf_counter()
            snapshot_result = snapshot_all(engine, as_of=as_of)
            timings["divergence_snapshot_all_core"] = time.perf_counter() - t0

        # (e3) emitter — the real per-event contracts.emit path, run once
        # with contracts_audit present on the scratch DB. contracts.emit
        # resolves its OWN engine from DB_* config
        # (contracts.emit._get_engine() -> api.dependencies.get_db_engine()),
        # separate from the GRID_TEST_DB_URL engine this harness otherwise
        # uses — NOT overridden here, so this only runs the real path when
        # that separately-configured engine is confirmed to point at the
        # same scratch database (coordinator's setup responsibility: DB_*
        # config env vars matching GRID_TEST_DB_URL). This is RTT-bound by
        # design (audit INSERT + pg_notify + connection/transaction
        # overhead per emitted SignalFired — see snapshot_all's "SYNTH-26"
        # comment in intelligence/fundamental_divergence.py) and is NOT
        # held to the 60s divergence budget; it is reported for
        # visibility only. Statement counts are not measured through this
        # harness's own engine — that engine is not the one the emitter
        # actually uses, so any count taken there would not reflect the
        # emitter's real cost.
        emitter_events = [
            r for r in compute_divergence(engine, as_of=as_of)
            if r["classification"] in ("long_candidate", "short_candidate")
        ]
        emitter_audit_rows = 0
        timings["divergence_emitter"] = 0.0
        emitter_ran = False
        if emitter_events:
            _apply_contracts_migration(engine)
            with engine.connect() as conn:
                contracts_audit_present = _table_exists(conn, "public.contracts_audit")
            if contracts_audit_present:
                emit_engine = contracts_emit._get_engine()
                if _emit_engine_matches_scratch_db(emit_engine, engine):
                    emitter_run_start = datetime.now(timezone.utc)
                    t0 = time.perf_counter()
                    for r in emitter_events:
                        fd._emit_divergence_signal(r)
                    timings["divergence_emitter"] = time.perf_counter() - t0
                    emitter_ran = True
                    with engine.connect() as conn:
                        emitter_audit_rows = conn.execute(
                            text(
                                "SELECT COUNT(*) FROM contracts_audit WHERE "
                                "producer_module = :pm AND emitted_at >= :start",
                            ).bindparams(
                                pm="intelligence.fundamental_divergence",
                                start=emitter_run_start,
                            ),
                        ).scalar_one()
                else:
                    print(
                        "\n[emitter] contracts.emit's own configured engine "
                        "does not point at the scratch DB (GRID_TEST_DB_URL) "
                        "— emitter measurement skipped to avoid writing to "
                        "the wrong database. Point DB_* config env vars at "
                        "the same scratch DB to enable it."
                    )
            else:
                print(
                    "\n[emitter] contracts_audit could not be created on "
                    "the scratch DB — emitter measurement skipped"
                )

        per_signal_ms = (
            (timings["divergence_emitter"] * 1000.0 / len(emitter_events))
            if emitter_ran and emitter_events else 0.0
        )
        print(
            f"\nemitter: {len(emitter_events)} events, "
            f"{timings['divergence_emitter']:.2f} seconds, "
            f"{per_signal_ms:.1f} ms per signal, "
            f"audit_rows={emitter_audit_rows} (ran={emitter_ran})"
        )

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
        print(f"{'divergence_snapshot_all_core':38s} "
              f"{timings['divergence_snapshot_all_core']:10.2f}  "
              f"< {DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S}s")
        print(f"{'divergence_emitter':38s} "
              f"{timings['divergence_emitter']:10.2f}  n/a (RTT-bound, not budgeted)")
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
        assert timings["divergence_snapshot_all_core"] < DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S, (
            f"core snapshot_all (emitter no-op'd) took "
            f"{timings['divergence_snapshot_all_core']:.1f}s, over "
            f"the {DAILY_INTEL_FUNDAMENTAL_DIVERGENCE_BUDGET_S}s divergence "
            f"budget. snapshot_all's write phase batches the whole universe "
            f"into one multi-row INSERT ... ON CONFLICT statement per "
            f"fundamental_divergence._DIVERGENCE_UPSERT_CHUNK_SIZE-row chunk "
            f"(fable-daily-intel-sql-tasks, 2026-09-20 follow-up) — a "
            f"regression here almost certainly means a per-ticker statement "
            f"crept back into snapshot_all or compute_divergence; check for a "
            f"conn.execute() call inside a per-row loop before raising the "
            f"budget."
        )
        if emitter_ran:
            assert emitter_audit_rows == len(emitter_events), (
                f"expected exactly 1 contracts_audit row per emitted signal "
                f"(the pre-existing per-event audit/notify design documented "
                f"in snapshot_all's 'SYNTH-26' comment, which #587 does not "
                f"change), got {emitter_audit_rows} audit rows for "
                f"{len(emitter_events)} events. This assertion documents "
                f"the per-event write shape, not a performance budget — the "
                f"emitter's elapsed time itself is reported above but not "
                f"asserted against any bound (it is RTT-bound, over its own "
                f"separately-configured engine, not this harness's)."
            )

        # Every individual statement must also stay under the DB's own
        # statement_timeout — a phase that got cancelled would have raised
        # already, but this restates the ceiling explicitly for the
        # printed table's sake. ``divergence_emitter`` is excluded: it is
        # the wall time of hundreds of individually-cheap, individually
        # under-timeout statements summed over an RTT-bound loop, not one
        # statement subject to the ceiling itself.
        for name, secs in timings.items():
            if name == "divergence_emitter":
                continue
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
            if contracts_audit_present and emitter_run_start is not None:
                conn.execute(
                    text(
                        "DELETE FROM contracts_audit WHERE producer_module = "
                        ":pm AND emitted_at >= :start",
                    ).bindparams(
                        pm="intelligence.fundamental_divergence",
                        start=emitter_run_start,
                    ),
                )
            if source_id is not None:
                conn.execute(
                    text("DELETE FROM raw_series WHERE source_id = :sid").bindparams(sid=source_id),
                )
                conn.execute(
                    text("DELETE FROM source_catalog WHERE id = :sid").bindparams(sid=source_id),
                )
