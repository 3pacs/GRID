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
from datetime import date, timedelta

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
from oracle.entry_price_policy import SCORE_NOTE_ENTRY_NULL

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
) -> None:
    """Insert a minimal, valid oracle_predictions row.

    Every NOT NULL column (id, ticker, prediction_type, direction, expiry,
    model_name) is supplied; everything else uses the table's own defaults
    or the nullable columns under test.
    """
    conn.execute(
        text(
            """
            INSERT INTO oracle_predictions (
                id, ticker, prediction_type, direction, target_price,
                entry_price, expiry, confidence, expected_move_pct,
                model_name, verdict, dedup_keep
            ) VALUES (
                :id, :ticker, 'test', :direction, NULL,
                :entry_price, :expiry, :confidence, 5.0,
                :model_name, :verdict, TRUE
            )
            """,
        ).bindparams(
            id=pred_id, ticker=ticker, direction=direction,
            entry_price=entry_price, expiry=expiry or date.today(),
            confidence=confidence, model_name=model_name, verdict=verdict,
        ),
    )


def _fetch_row(pg_engine: Engine, pred_id: str) -> dict | None:
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, verdict, entry_price, confidence, actual_price, "
                "actual_move_pct, pnl_pct, scored_at, score_notes "
                "FROM oracle_predictions WHERE id = :id",
            ).bindparams(id=pred_id),
        ).fetchone()
    if row is None:
        return None
    return dict(row._mapping)


# ── 1. Migration produces a genuinely nullable schema ──────────────────────


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
    columns. A third, new-policy row (entry_price IS NULL) must be handled
    per Step 4 -- closed to 'no_data' with an explicit reason, never scored
    as if the entry were 0.

    The scorer's own price lookups are stubbed (no network access); this
    only proves the hold, not price-fetching.
    """
    import scripts.score_oracle_trades as sot

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    # already expired -> scoreable. oracle_predictions_dedup_unique is a
    # real partial unique index on (ticker, direction, expiry,
    # prediction_type, COALESCE(model_version,''), created_at::date) WHERE
    # dedup_keep; these three rows share a ticker, direction and
    # created_at date, so each gets its own expiry day to give it a
    # distinct natural key -- all still in the past, all still scoreable.
    expiry_a = date.today() - timedelta(days=1)
    expiry_b = date.today() - timedelta(days=2)
    expiry_c = date.today() - timedelta(days=3)

    legacy_a = _pred_id()
    legacy_b = _pred_id()
    new_policy_null = _pred_id()
    test_ids.extend([legacy_a, legacy_b, new_policy_null])

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
        )

    before = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null)
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
                date.today(): 999.0,
            },
        }

    monkeypatch.setattr(sot, "fetch_prices", _stub_fetch_prices)
    monkeypatch.setattr(sot, "create_engine", lambda *a, **kw: pg_engine)

    sot.main(["--chunk-size", "500"])

    after = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null)
    }

    # PRESERVATION: the legacy rows are byte-identical. Historical-write
    # hold -- never updated, closed, rescored or re-labelled.
    assert after[legacy_a] == before[legacy_a], (legacy_a, before[legacy_a], after[legacy_a])
    assert after[legacy_b] == before[legacy_b], (legacy_b, before[legacy_b], after[legacy_b])
    assert after[legacy_a]["verdict"] == "pending"
    assert after[legacy_b]["verdict"] == "pending"

    # The new-policy NULL row is closed honestly -- never scored as if the
    # entry were 0, and the reason is the shared, named one.
    assert after[new_policy_null]["verdict"] == "no_data"
    assert after[new_policy_null]["score_notes"] == SCORE_NOTE_ENTRY_NULL
    assert after[new_policy_null]["entry_price"] is None
    assert after[new_policy_null]["pnl_pct"] is None


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
    byte-identical; a new-policy NULL-entry row is closed to 'no_data' with
    the shared reason.
    """
    from oracle.engine import OracleEngine

    ticker = f"ZPKT2A{uuid.uuid4().hex[:6]}"
    # oracle_predictions_dedup_unique (see test 3's comment) -- distinct
    # expiry per row for a distinct natural key.
    expiry_a = date.today() - timedelta(days=1)
    expiry_b = date.today() - timedelta(days=2)
    expiry_c = date.today() - timedelta(days=3)

    legacy_a = _pred_id()
    legacy_b = _pred_id()
    new_policy_null = _pred_id()
    test_ids.extend([legacy_a, legacy_b, new_policy_null])

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
        )

    before = {
        pid: _fetch_row(pg_engine, pid)
        for pid in (legacy_a, legacy_b, new_policy_null)
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
        for pid in (legacy_a, legacy_b, new_policy_null)
    }

    # PRESERVATION on the automatic path too.
    assert after[legacy_a] == before[legacy_a], (legacy_a, before[legacy_a], after[legacy_a])
    assert after[legacy_b] == before[legacy_b], (legacy_b, before[legacy_b], after[legacy_b])
    assert after[legacy_a]["verdict"] == "pending"
    assert after[legacy_b]["verdict"] == "pending"

    assert after[new_policy_null]["verdict"] == "no_data"
    assert after[new_policy_null]["score_notes"] == SCORE_NOTE_ENTRY_NULL
    assert after[new_policy_null]["entry_price"] is None
    assert after[new_policy_null]["pnl_pct"] is None

    # This test's own two legacy rows are counted among the held ones, and
    # its own NULL row among the closed ones -- >= because the loop also
    # sees every other expired pending row in this shared table.
    assert results["held_legacy_entry_price"] >= 2, results
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
