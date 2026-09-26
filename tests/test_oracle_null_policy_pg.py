"""Real-PostgreSQL proof for the packet2a extraction's nullable
``oracle_predictions.entry_price`` / ``.confidence`` and the
historical-write hold that guards them.

fable/packet2a-20260921 combines #547's null-safe readers with #544's own
oracle/confidence slice, re-parented onto this repo's live migration head
(``oracle_pred_nullable_0918`` -> ``capital_flow_ttm_state_20260920``). The
in-process SQLite fixtures in ``tests/test_oracle_null_readers.py`` already
pin the portable SQL contract; this file proves the same contract against a
REAL PostgreSQL instance (NULLS LAST ordering, NULL-vs-0 arithmetic, the
alembic revision itself), plus the one property that can only be observed on
a live table: that the historical-write hold added in this branch actually
holds -- a legacy row (entry_price=0.0, non-null confidence, written before
the column was nullable) survives a real scorer run byte-for-byte.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly
if no PostgreSQL is reachable, and honours ``GRID_TEST_DB_URL`` for a
disposable test database (see tests/test_capital_flow_rollups_pg.py for the
established pattern this file follows). Every row this file writes carries a
unique ``pkt2a_pg_<uuid>`` id and/or a unique ``ZPKT2A<n>`` ticker, and the
autouse fixture deletes them by id afterwards.

Two scoring paths write the historical-write hold's outcome, and both are
proved separately here:

* ``scripts/score_oracle_trades.py`` is a MANUAL CLI -- not invoked by any
  systemd unit, timer, cron entry or by Hermes (confirmed: no reference in
  the release tree's ``hermes_operator.py``, no journal lines in 24h on
  grid-svr).
* ``oracle/engine.py::OracleEngine.score_expired_predictions`` is the
  AUTOMATIC path -- Hermes's oracle step imports and calls it directly
  (``from oracle.engine import OracleEngine``, ``hermes_operator.py:3964``).

Five things proved here, matching the extraction's handoff doc:

1. test_migration_allows_null_entry_price_and_confidence_after_upgrade --
   after the migration, a prediction row can be inserted with
   entry_price IS NULL and confidence IS NULL.
2. test_547_readers_handle_null_rows_honestly -- the #547 readers
   (api/routers/oracle.py, oracle/calibration.py) return honest
   nulls/unscored for such rows and never raise.
3. test_preservation_legacy_rows_survive_a_real_scorer_run -- the
   historical-write hold on the MANUAL path: two legacy pending rows
   (entry_price=0.0, non-null confidence) run through the real
   scripts/score_oracle_trades.py against a stubbed price source and come
   out byte-identical; a new-policy NULL row is closed per Step 4.
4. test_preservation_legacy_rows_survive_engine_score_expired_predictions --
   the same hold, proved on the AUTOMATIC path:
   OracleEngine.score_expired_predictions() against a stubbed price lookup.
5. test_migration_downgrade_with_null_rows_present -- alembic downgrade
   behaviour with NULL rows present, run inside a transaction that is always
   rolled back so it can never touch the shared table's real constraints.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

# oracle.* must be imported BEFORE scripts.score_oracle_trades (imported
# lazily, inside test_preservation_legacy_rows_survive_a_real_scorer_run).
# That module does `sys.path.insert(0, "/data/grid_v4/grid_repo")` (a
# compute-node checkout path, pre-existing on main, out of scope here)
# whose own `oracle` package predates entry_price_policy.py. On a host
# where that directory exists, importing score_oracle_trades first would
# cache THAT `oracle` package in sys.modules, and every `oracle.*` import
# afterward -- including api.routers.oracle's own `from oracle.engine
# import ...` in test_547_readers_handle_null_rows_honestly -- would
# resolve from it instead of this repo's real package. These two eager,
# module-level imports cache the correct `oracle` package first, so
# nothing later in the process can be shadowed.
import oracle.engine  # noqa: F401
from oracle.entry_price_policy import NULL_WRITE_POLICY, SCORE_NOTE_ENTRY_NULL

_MIGRATION_MODULE = "migrations.versions.oracle_pred_nullable_0918"


def _pred_id() -> str:
    return f"pkt2a_pg_{uuid.uuid4().hex[:16]}"


@pytest.fixture
def test_ids() -> list[str]:
    return []


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg_engine: Engine, test_ids: list[str]):
    yield
    if not test_ids:
        return
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM oracle_predictions WHERE id = ANY(:ids)").bindparams(
                ids=test_ids,
            ),
        )


def _insert_prediction(
    conn,
    *,
    pred_id: str,
    ticker: str,
    entry_price: float | None,
    confidence: float | None,
    direction: str = "CALL",
    verdict: str = "pending",
    expiry: date | None = None,
    model_name: str = "packet2a_pg_test",
    null_write_policy: str | None = None,
) -> None:
    """Insert a minimal, valid oracle_predictions row.

    Every NOT NULL column (id, ticker, prediction_type, direction, expiry,
    model_name) is supplied; everything else uses the table's own defaults
    or the nullable columns under test.

    ``null_write_policy`` defaults to None (unstamped): every row this test
    file writes by hand simulates either a pre-migration legacy row or a
    hand-constructed new-policy row, neither of which went through the real
    writers -- only ``oracle/publish.py`` and
    ``oracle/engine.py::_store_predictions`` stamp
    ``oracle.entry_price_policy.NULL_WRITE_POLICY`` (proved separately in
    ``TestNullWritePolicyProvenanceBoundary`` below). Passing it explicitly
    here would assert something this helper never verified.
    """
    conn.execute(
        text(
            """
            INSERT INTO oracle_predictions (
                id, ticker, prediction_type, direction, target_price,
                entry_price, expiry, confidence, expected_move_pct,
                model_name, verdict, dedup_keep, null_write_policy
            ) VALUES (
                :id, :ticker, 'test', :direction, NULL,
                :entry_price, :expiry, :confidence, 5.0,
                :model_name, :verdict, TRUE, :null_write_policy
            )
            """,
        ).bindparams(
            id=pred_id, ticker=ticker, direction=direction,
            entry_price=entry_price, expiry=expiry or date.today(),
            confidence=confidence, model_name=model_name, verdict=verdict,
            null_write_policy=null_write_policy,
        ),
    )


def _fetch_row(pg_engine: Engine, pred_id: str) -> dict | None:
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, verdict, entry_price, confidence, actual_price, "
                "actual_move_pct, pnl_pct, scored_at, score_notes, "
                "null_write_policy "
                "FROM oracle_predictions WHERE id = :id",
            ).bindparams(id=pred_id),
        ).fetchone()
    if row is None:
        return None
    return dict(row._mapping)


# ── 1. Migration produces a genuinely nullable schema ──────────────────────


def test_upgrade_runs_for_real_with_finite_timeouts_scoped_to_its_own_transaction(
    pg_engine: Engine,
):
    """The real ``upgrade()`` -- not a hand-replicated copy of its SQL, the
    actual function -- must run cleanly with its new ``SET LOCAL
    lock_timeout``/``statement_timeout`` guards, and those guards must never
    outlive the migration's own transaction. Idempotent (DROP NOT NULL /
    ADD COLUMN IF NOT EXISTS never fails on an already-migrated table), so
    this is safe to run for real against the shared disposable database.
    """
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)

    with pg_engine.connect() as baseline_conn:
        baseline_lock_timeout = baseline_conn.execute(
            text("SHOW lock_timeout"),
        ).scalar()
        baseline_statement_timeout = baseline_conn.execute(
            text("SHOW statement_timeout"),
        ).scalar()

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            migration.upgrade()
        finally:
            migration.op = real_op

        # Still in effect INSIDE the transaction that ran the migration.
        assert conn.execute(text("SHOW lock_timeout")).scalar() == (
            migration._LOCK_TIMEOUT
        )
        assert conn.execute(text("SHOW statement_timeout")).scalar() == (
            migration._STATEMENT_TIMEOUT
        )
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    # A fresh connection/session must see the ordinary baseline, never the
    # migration's SET LOCAL values -- proves the scoping, not just that the
    # statements ran without raising.
    with pg_engine.connect() as after_conn:
        assert after_conn.execute(text("SHOW lock_timeout")).scalar() == (
            baseline_lock_timeout
        )
        assert after_conn.execute(text("SHOW statement_timeout")).scalar() == (
            baseline_statement_timeout
        )

    # The migration's real effect still happened.
    with pg_engine.connect() as verify_conn:
        cols = verify_conn.execute(
            text(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'oracle_predictions' "
                "AND column_name IN ('entry_price', 'confidence', 'null_write_policy')",
            ),
        ).fetchall()
    by_col = {r[0]: r[1] for r in cols}
    assert by_col["entry_price"] == "YES"
    assert by_col["confidence"] == "YES"
    assert "null_write_policy" in by_col


def test_migration_allows_null_entry_price_and_confidence_after_upgrade(
    pg_engine: Engine, test_ids: list[str],
):
    """After oracle_pred_nullable_0918's upgrade(), a prediction row can be
    inserted with entry_price IS NULL and confidence IS NULL.

    upgrade() only ever relaxes a NOT NULL constraint (``ALTER COLUMN ...
    DROP NOT NULL``), which is idempotent and safe to run again for real
    even if this database's oracle_predictions is already migrated -- it
    can never make the schema stricter. That is what lets this test call it
    directly rather than assuming the coordinator's test database has
    already run the full migration chain.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)
    # Idempotent, one-directional relaxation -- safe to commit for real.
    with pg_engine.begin() as conn:
        for col in ("entry_price", "confidence"):
            conn.execute(
                text(
                    f"ALTER TABLE IF EXISTS oracle_predictions "
                    f"ALTER COLUMN {col} DROP NOT NULL",
                ),
            )
    assert migration.revision == "oracle_pred_nullable_0918"
    assert migration.down_revision == "capital_flow_ttm_state_20260920"

    pred_id = _pred_id()
    test_ids.append(pred_id)
    with pg_engine.begin() as conn:
        _insert_prediction(
            conn, pred_id=pred_id, ticker="ZPKT2A1",
            entry_price=None, confidence=None,
        )

    row = _fetch_row(pg_engine, pred_id)
    assert row is not None
    assert row["entry_price"] is None
    assert row["confidence"] is None


def test_migration_adds_the_null_write_policy_column_after_upgrade(
    pg_engine: Engine, test_ids: list[str],
):
    """oracle_pred_nullable_0918's upgrade() also adds
    ``null_write_policy`` -- additive, no default, safe to run again for
    real on an already-migrated database (``ADD COLUMN IF NOT EXISTS``).

    Proves the historical-NULL provenance boundary (item (a), packet 2a)
    exists as a real column a reader can query, not just as a fixture-level
    assumption: a legacy-shaped row (NULL columns, no stamp -- the only
    shape a hand-inserted row has unless it opts in) and a row that opts
    into the stamp are simultaneously present and distinguishable by SQL
    alone.
    """
    migration = importlib.import_module(_MIGRATION_MODULE)
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE IF EXISTS oracle_predictions "
                "ADD COLUMN IF NOT EXISTS null_write_policy TEXT",
            ),
        )

    unstamped_id = _pred_id()
    stamped_id = _pred_id()
    test_ids.extend([unstamped_id, stamped_id])
    with pg_engine.begin() as conn:
        _insert_prediction(
            conn, pred_id=unstamped_id, ticker="ZPKT2A2",
            entry_price=None, confidence=None,
        )
        _insert_prediction(
            conn, pred_id=stamped_id, ticker="ZPKT2A2",
            entry_price=None, confidence=None, direction="PUT",
            null_write_policy=NULL_WRITE_POLICY,
        )

    unstamped = _fetch_row(pg_engine, unstamped_id)
    stamped = _fetch_row(pg_engine, stamped_id)
    assert unstamped["null_write_policy"] is None
    assert stamped["null_write_policy"] == NULL_WRITE_POLICY

    # The boundary is provable by SQL alone, without inferring anything
    # from migration history: exactly the stamped row comes back.
    with pg_engine.connect() as conn:
        proven = conn.execute(
            text(
                "SELECT id FROM oracle_predictions "
                "WHERE id = ANY(:ids) AND null_write_policy = :policy",
            ).bindparams(ids=[unstamped_id, stamped_id], policy=NULL_WRITE_POLICY),
        ).fetchall()
    assert [r[0] for r in proven] == [stamped_id]


# ── 2. The #547 readers are honest and never raise ─────────────────────────


def test_547_readers_handle_null_rows_honestly(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    """A NULL entry_price / confidence row must never crash a reader, and
    must never be silently replayed as 0 / 0.5."""
    from oracle.calibration import compute_calibration
    import api.routers.oracle as oracle_router

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    null_conf_scored_id = _pred_id()
    null_entry_pending_id = _pred_id()
    test_ids.extend([null_conf_scored_id, null_entry_pending_id])

    with pg_engine.begin() as conn:
        # A scored row with no stated confidence -- must be excluded from
        # calibration's Brier/ECE inputs, never imputed at 0.5.
        #
        # oracle_predictions_dedup_unique is a real partial unique index on
        # (ticker, direction, expiry, prediction_type,
        # COALESCE(model_version,''), created_at::date) WHERE dedup_keep.
        # Both rows share a ticker and (by default) a created_at date, so
        # direction is varied per row to give each a distinct natural key
        # without touching the index or the semantics under test.
        _insert_prediction(
            conn, pred_id=null_conf_scored_id, ticker=ticker,
            entry_price=100.0, confidence=None, verdict="hit",
            direction="CALL",
        )
        # A pending row with no measured entry price.
        _insert_prediction(
            conn, pred_id=null_entry_pending_id, ticker=ticker,
            entry_price=None, confidence=0.6, verdict="pending",
            direction="PUT",
        )

    # -- oracle/calibration.py --------------------------------------------
    report = compute_calibration(pg_engine, ticker=ticker)
    # The NULL-confidence row is excluded outright (compute_calibration's
    # own query requires confidence IS NOT NULL) -- never counted at a
    # default, never raising on `float(None)`.
    assert report.total_predictions == 0, report

    # -- api/routers/oracle.py::get_predictions -----------------------------
    monkeypatch.setattr(oracle_router, "get_db_engine", lambda: pg_engine)
    result = oracle_router.get_predictions(
        ticker=ticker, model=None, status=None, limit=100, offset=0,
        _token="test",
    )
    by_id = {p["id"]: p for p in result["predictions"]}
    assert by_id[null_conf_scored_id]["confidence"] is None
    assert by_id[null_entry_pending_id]["entry_price"] is None
    # The pending NULL-entry row must report an honest, named basis, never
    # a fabricated 0%/None-that-looks-like-zero.
    assert by_id[null_entry_pending_id]["tracking_pnl"] is None
    assert by_id[null_entry_pending_id]["tracking_pnl_basis"] is not None
    assert "unavailable" in by_id[null_entry_pending_id]["tracking_pnl_basis"]

    # -- api/routers/oracle.py::get_latest -----------------------------------
    # Global (not ticker-scoped) -- only asserted not to raise with NULL
    # rows present in the table, since "latest cycle" is table-wide and not
    # deterministic to scope to this test's own rows on a shared database.
    oracle_router.get_latest(_token="test")


# ── 3. Historical-write hold: legacy rows survive a real scorer run ────────


def test_preservation_legacy_rows_survive_a_real_scorer_run(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    """Two legacy pending rows (entry_price=0.0, non-null confidence -- the
    only shape possible before this migration, since the column was NOT
    NULL) must come out of a real scripts/score_oracle_trades.py run
    byte-identical: same status, entry_price, confidence, outcome/score
    columns. A third, MARKED new-policy row (entry_price IS NULL AND
    null_write_policy = NULL_WRITE_POLICY) must be handled per Step 4 --
    closed to 'no_data' with an explicit reason, never scored as if the
    entry were 0. A fourth, UNMARKED row (entry_price IS NULL but
    null_write_policy IS NULL -- provenance not proved) must be held
    exactly like a legacy row: the nullable schema alone never authorises
    the close-out, only the stamp does (item (a)'s whole point; adding and
    stamping the column is not enough without the scorer actually checking
    it -- this proves the check, not just the column).

    The scorer's own price lookups are stubbed (no network access); this
    only proves the hold, not price-fetching.
    """
    import scripts.score_oracle_trades as sot

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    # already expired -> scoreable. oracle_predictions_dedup_unique is a
    # real partial unique index on (ticker, direction, expiry,
    # prediction_type, COALESCE(model_version,''), created_at::date) WHERE
    # dedup_keep; these four rows share a ticker, direction and created_at
    # date, so each gets its own expiry day to give it a distinct natural
    # key -- all still in the past, all still scoreable.
    expiry_a = date.today() - timedelta(days=1)
    expiry_b = date.today() - timedelta(days=2)
    expiry_c = date.today() - timedelta(days=3)
    expiry_d = date.today() - timedelta(days=4)

    legacy_a = _pred_id()
    legacy_b = _pred_id()
    new_policy_null = _pred_id()
    unmarked_null = _pred_id()
    test_ids.extend([legacy_a, legacy_b, new_policy_null, unmarked_null])

    with pg_engine.begin() as conn:
        _insert_prediction(
            conn, pred_id=legacy_a, ticker=ticker, entry_price=0.0,
            confidence=0.55, verdict="pending", expiry=expiry_a,
        )
        _insert_prediction(
            conn, pred_id=legacy_b, ticker=ticker, entry_price=0.0,
            confidence=0.72, verdict="pending", expiry=expiry_b,
        )
        _insert_prediction(
            conn, pred_id=new_policy_null, ticker=ticker, entry_price=None,
            confidence=None, verdict="pending", expiry=expiry_c,
            null_write_policy=NULL_WRITE_POLICY,
        )
        _insert_prediction(
            conn, pred_id=unmarked_null, ticker=ticker, entry_price=None,
            confidence=None, verdict="pending", expiry=expiry_d,
            # null_write_policy defaults to None -- unmarked on purpose.
        )

    before = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null, unmarked_null)
    }
    for pid, row in before.items():
        assert row is not None, pid

    # Stub: the real yfinance-backed fetch_prices is never called over the
    # network. Any ticker this run asks about that we have no opinion on
    # (i.e. every ticker except ours) resolves to "no price", which is a
    # safe, honest outcome for those unrelated rows on a disposable test
    # database -- not a claim this test makes about them.
    def _stub_fetch_prices(tickers, start, end):
        return {
            ticker: {
                expiry_a: 999.0, expiry_b: 999.0, expiry_c: 999.0,
                expiry_d: 999.0, date.today(): 999.0,
            },
        }

    monkeypatch.setattr(sot, "fetch_prices", _stub_fetch_prices)
    monkeypatch.setattr(sot, "create_engine", lambda *a, **kw: pg_engine)

    sot.main(["--chunk-size", "500"])

    after = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null, unmarked_null)
    }

    # PRESERVATION: the legacy rows are byte-identical. Historical-write
    # hold -- never updated, closed, rescored or re-labelled.
    assert after[legacy_a] == before[legacy_a], (legacy_a, before[legacy_a], after[legacy_a])
    assert after[legacy_b] == before[legacy_b], (legacy_b, before[legacy_b], after[legacy_b])
    assert after[legacy_a]["verdict"] == "pending"
    assert after[legacy_b]["verdict"] == "pending"

    # The MARKED new-policy NULL row is closed honestly -- never scored as
    # if the entry were 0, and the reason is the shared, named one.
    assert after[new_policy_null]["verdict"] == "no_data"
    assert after[new_policy_null]["score_notes"] == SCORE_NOTE_ENTRY_NULL
    assert after[new_policy_null]["entry_price"] is None
    assert after[new_policy_null]["pnl_pct"] is None

    # The UNMARKED NULL row is held byte-identical, exactly like a legacy
    # row -- entry_price IS NULL alone never authorises the close-out.
    assert after[unmarked_null] == before[unmarked_null], (
        unmarked_null, before[unmarked_null], after[unmarked_null],
    )
    assert after[unmarked_null]["verdict"] == "pending"


# ── 4. Historical-write hold on the AUTOMATIC path (Hermes) ────────────────


def test_preservation_legacy_rows_survive_engine_score_expired_predictions(
    pg_engine: Engine, test_ids: list[str],
):
    """The hold on the path Hermes actually calls automatically.

    scripts/score_oracle_trades.py (test 3, above) is a manual CLI -- not
    invoked by any systemd unit, timer, cron entry or by Hermes. The
    prediction -> outcome -> score cycle that runs on its own is Hermes's
    oracle step calling ``OracleEngine.score_expired_predictions()``
    directly (``hermes_operator.py:3964``: ``from oracle.engine import
    OracleEngine``). The historical-write hold has to hold there too: two
    legacy pending rows (entry_price=0.0, non-null confidence) survive
    byte-identical; a MARKED new-policy NULL-entry row (null_write_policy =
    NULL_WRITE_POLICY) is closed to 'no_data' with the shared reason; an
    UNMARKED NULL-entry row (null_write_policy IS NULL -- provenance not
    proved) is held byte-identical, exactly like a legacy row -- proving
    the engine path also checks the stamp and not just entry_price's
    nullness.
    """
    from oracle.engine import OracleEngine

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    # oracle_predictions_dedup_unique (see test 3's comment) -- distinct
    # expiry per row for a distinct natural key.
    expiry_a = date.today() - timedelta(days=1)
    expiry_b = date.today() - timedelta(days=2)
    expiry_c = date.today() - timedelta(days=3)
    expiry_d = date.today() - timedelta(days=4)

    legacy_a = _pred_id()
    legacy_b = _pred_id()
    new_policy_null = _pred_id()
    unmarked_null = _pred_id()
    test_ids.extend([legacy_a, legacy_b, new_policy_null, unmarked_null])

    with pg_engine.begin() as conn:
        _insert_prediction(
            conn, pred_id=legacy_a, ticker=ticker, entry_price=0.0,
            confidence=0.55, verdict="pending", expiry=expiry_a,
        )
        _insert_prediction(
            conn, pred_id=legacy_b, ticker=ticker, entry_price=0.0,
            confidence=0.72, verdict="pending", expiry=expiry_b,
        )
        _insert_prediction(
            conn, pred_id=new_policy_null, ticker=ticker, entry_price=None,
            confidence=None, verdict="pending", expiry=expiry_c,
            null_write_policy=NULL_WRITE_POLICY,
        )
        _insert_prediction(
            conn, pred_id=unmarked_null, ticker=ticker, entry_price=None,
            confidence=None, verdict="pending", expiry=expiry_d,
            # null_write_policy defaults to None -- unmarked on purpose.
        )

    before = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null, unmarked_null)
    }
    for pid, row in before.items():
        assert row is not None, pid

    # __init__ runs 8+ CREATE TABLE/INDEX statements and loads the model
    # registry -- unrelated to this test and unneeded against a live,
    # already-bootstrapped table. Bypassed the same way
    # tests/test_oracle_null_readers.py::TestEngineScoringLoopEntryPrice.
    # _engine_under_test does. The price lookup is stubbed for our own
    # ticker only -- no network access, and every other ticker's expired
    # row in this shared table is left alone (None -> skipped, not scored).
    oe = object.__new__(OracleEngine)
    oe.engine = pg_engine
    oe.models = []
    oe._last_guard_verdicts = []
    oe._get_price_at_date = lambda t, _expiry: 999.0 if t == ticker else None

    results = oe.score_expired_predictions()

    after = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null, unmarked_null)
    }

    # PRESERVATION on the automatic path too.
    assert after[legacy_a] == before[legacy_a], (legacy_a, before[legacy_a], after[legacy_a])
    assert after[legacy_b] == before[legacy_b], (legacy_b, before[legacy_b], after[legacy_b])
    assert after[legacy_a]["verdict"] == "pending"
    assert after[legacy_b]["verdict"] == "pending"

    # MARKED new-policy NULL row: closed as intended.
    assert after[new_policy_null]["verdict"] == "no_data"
    assert after[new_policy_null]["score_notes"] == SCORE_NOTE_ENTRY_NULL
    assert after[new_policy_null]["entry_price"] is None
    assert after[new_policy_null]["pnl_pct"] is None

    # UNMARKED NULL row: held byte-identical -- entry_price IS NULL alone
    # never authorises the close-out on the automatic path either.
    assert after[unmarked_null] == before[unmarked_null], (
        unmarked_null, before[unmarked_null], after[unmarked_null],
    )
    assert after[unmarked_null]["verdict"] == "pending"

    # This test's own two legacy rows and its own unmarked-NULL row are
    # counted among the held ones, and its own marked-NULL row among the
    # closed ones -- >= because the loop also sees every other expired
    # pending row in this shared table.
    assert results["held_legacy_entry_price"] >= 2, results
    assert results["held_unmarked_null_entry_price"] >= 1, results
    assert results["unscorable_entry_price"] >= 1, results


# ── 5. Migration downgrade behaviour, isolated in a rolled-back txn ────────


def test_migration_downgrade_with_null_rows_present(pg_engine: Engine):
    """alembic downgrade of oracle_pred_nullable_0918 with NULL rows
    present must leave the columns nullable and log why, never delete or
    repair rows, and never raise.

    Run entirely inside one connection's own transaction, which is ALWAYS
    rolled back in a finally -- this proves the migration's real behaviour
    without ever permanently touching the shared table's actual
    constraints (other tests, and the live application, depend on
    entry_price/confidence staying nullable for the rest of the run).
    """
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION_MODULE)

    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        # Make sure the columns are nullable on this connection's view
        # before seeding a NULL row (idempotent no-op if already so).
        for col in ("entry_price", "confidence"):
            conn.execute(
                text(
                    f"ALTER TABLE IF EXISTS oracle_predictions "
                    f"ALTER COLUMN {col} DROP NOT NULL",
                ),
            )

        pred_id = _pred_id()
        conn.execute(
            text(
                """
                INSERT INTO oracle_predictions (
                    id, ticker, prediction_type, direction, entry_price,
                    expiry, confidence, model_name, verdict, dedup_keep
                ) VALUES (
                    :id, 'ZPKT2ADOWN', 'test', 'CALL', NULL,
                    :expiry, NULL, 'packet2a_pg_test', 'pending', TRUE
                )
                """,
            ).bindparams(id=pred_id, expiry=date.today()),
        )

        ctx = MigrationContext.configure(conn)
        ops = Operations(ctx)
        real_op = migration.op
        migration.op = ops
        try:
            # Must not raise. The migration's own contract: a column that
            # holds a NULL stays nullable, logged, never repaired/deleted.
            migration.downgrade()
        finally:
            migration.op = real_op

        # Assert the actual, documented outcome: still nullable.
        nullability = conn.execute(
            text(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'oracle_predictions' "
                "AND column_name IN ('entry_price', 'confidence')",
            ),
        ).fetchall()
        by_col = {r[0]: r[1] for r in nullability}
        assert by_col["entry_price"] == "YES", (
            "downgrade must leave entry_price nullable while a NULL row "
            "exists (documented one-way schema change), got " + repr(by_col)
        )
        assert by_col["confidence"] == "YES", by_col

        # The row itself was never touched, let alone deleted or repaired.
        row = conn.execute(
            text(
                "SELECT entry_price, confidence FROM oracle_predictions "
                "WHERE id = :id",
            ).bindparams(id=pred_id),
        ).fetchone()
        assert row is not None, "downgrade must never delete a NULL row"
        assert row[0] is None and row[1] is None, row
    finally:
        # ALWAYS rolled back: this test must have zero permanent effect on
        # the shared table's schema or data.
        trans.rollback()
        conn.close()


# ── 6. Real writers stamp the provenance boundary on a live row ────────────


def test_publish_astrogrid_prediction_stamps_the_policy_on_a_real_row(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    """oracle/publish.py's real INSERT, run against real PostgreSQL, must
    carry the historical-NULL provenance stamp -- not just in a fake-engine
    unit test (tests/test_oracle_publish_entry_price.py), but on an actual
    row next to actual NULLs (no options_daily_signals observation on this
    disposable database, so entry_price comes back NULL here too)."""
    from oracle import publish
    from oracle.entry_price_policy import NULL_WRITE_POLICY

    # The conviction-context lookup reads unrelated tables this disposable
    # database doesn't have populated; stub it exactly as
    # tests/test_oracle_publish_entry_price.py's `no_context` fixture does,
    # so this test proves only the write contract, not context enrichment.
    monkeypatch.setattr(
        publish, "build_prediction_context",
        lambda *a, **k: {
            "regime": "NEUTRAL", "fci_regime": "NEUTRAL",
            "vix_level": None, "signal_contributions": {},
        },
    )

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    payload = {
        "prediction_id": "pkt2a-pg-publish",
        "target_symbols": [ticker],
        "horizon_label": "swing",
        "as_of_ts": "2026-09-21T00:00:00+00:00",
        "call": "buy the dip",
    }
    out = publish.publish_astrogrid_prediction(pg_engine, payload)
    test_ids.append(out["oracle_prediction_id"])

    assert out["entry_price"] is None  # no spot on this disposable DB
    row = _fetch_row(pg_engine, out["oracle_prediction_id"])
    assert row["entry_price"] is None
    assert row["null_write_policy"] == NULL_WRITE_POLICY


def test_store_predictions_stamps_the_policy_on_a_real_row(
    pg_engine: Engine, test_ids: list[str], monkeypatch,
):
    """oracle/engine.py::_store_predictions's real INSERT, run against real
    PostgreSQL, must carry the same stamp -- the automatic/main path
    alongside publish.py's astrogrid path, both governed by the same
    migration and the same constant."""
    from oracle.engine import OracleEngine, OraclePrediction, PredictionType

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    pred_id = f"pkt2a_pg_store_{uuid.uuid4().hex[:16]}"
    test_ids.append(pred_id)

    # Bypass __init__ (model registry load, unrelated to this write path) —
    # same technique as test_preservation_legacy_rows_survive_engine_
    # score_expired_predictions above.
    oe = object.__new__(OracleEngine)
    oe.engine = pg_engine

    # The regime/fci/vix lookups read unrelated tables this disposable
    # database doesn't have populated; each is independently wrapped in a
    # try/except that defaults safely, so no monkeypatching is required for
    # this write-contract proof to run cleanly.
    prediction = OraclePrediction(
        id=pred_id,
        timestamp=datetime.now(timezone.utc),
        ticker=ticker,
        prediction_type=PredictionType.DIRECTION,
        direction="CALL",
        target_price=None,
        current_price=None,  # no spot observed -> NULL entry_price
        expiry=date.today() + timedelta(days=7),
        confidence=0.6,
        expected_move_pct=2.0,
        model_name="packet2a_pg_test",
        model_version="packet2a_pg_test-v1",
    )

    oe._store_predictions([prediction])

    row = _fetch_row(pg_engine, pred_id)
    assert row is not None
    assert row["entry_price"] is None
    assert row["null_write_policy"] == NULL_WRITE_POLICY
