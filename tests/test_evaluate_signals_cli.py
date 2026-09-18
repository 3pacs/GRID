"""Tests for scripts/evaluate_signals.py (workstream W3d) — the real,
never-scheduled caller of evaluation/signal_outcomes.py::evaluate_signal.

Fake-engine tests (no database) exercise: dry-run-by-default (prints a
summary, writes nothing), the two-flag persist gate, and the
`griddb_`-prefix database safety check. A DB-gated section at the bottom
runs the same CLI against a real, disposable PostgreSQL when
``GRID_TEST_DB_URL`` is set — skipped with an explicit reason otherwise.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

# config.py's Settings() unconditionally rejects an empty DB_PASSWORD (by
# design — see config.py::_check_db_password's docstring) even for a code
# path, like this one, that never actually uses config.py's DB_URL (the
# CLI is always handed an explicit engine — a fake one in these tests, or
# the real one from db.get_engine() at the __main__ entry point). A dev
# sandbox with no .env sourced has no other way to satisfy that validator
# just to import the (transitively imported, real-app) candidate-name
# rule — see evaluation/prices.py::_default_candidate_names. This does not
# affect which database anything here actually talks to.
os.environ.setdefault("DB_PASSWORD", "test-only-placeholder-unused")
os.environ.setdefault("ENVIRONMENT", "development")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import evaluate_signals as cli  # noqa: E402
from evaluation.signal_outcomes import EVALUATION_VERSION  # noqa: E402
from tests.test_evaluation_prices import FakeConnection, FakeEngine, _FakeResult  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]

D0 = date(2026, 1, 5)
EXIT_DATE = date(2026, 1, 10)  # D0 + 5 calendar days == default --horizon-days


def _make_signal_sources_engine(*, db_name="griddb_test", extra_rows=None):
    """A fake engine that additionally answers signal_sources SELECTs and
    signal_evaluations INSERTs, on top of the feature_registry/
    source_catalog/resolved_series support FakeConnection already has."""

    signal_rows = extra_rows if extra_rows is not None else [
        # id, source_type, ticker, signal_date, signal_type
        (1, "congressional", "AAPL", D0, "BUY"),
    ]
    inserted: list[dict] = []

    class _Conn(FakeConnection):
        def execute(self, query, params=None):
            sql = str(query)
            params = params or {}

            if "FROM signal_sources" in sql:
                rows = signal_rows
                if params.get("source_type") is not None:
                    rows = [r for r in rows if r[1] == params["source_type"]]
                if params.get("date_from") is not None:
                    rows = [r for r in rows if r[3] >= params["date_from"]]
                if params.get("date_to") is not None:
                    rows = [r for r in rows if r[3] <= params["date_to"]]
                rows = rows[: params.get("limit", len(rows))]
                return _FakeResult(rows)

            if "INSERT INTO signal_evaluations" in sql:
                key = (params["signal_source_id"], params["evaluation_version"], params["horizon_days"])
                existing_keys = {
                    (r["signal_source_id"], r["evaluation_version"], r["horizon_days"]) for r in inserted
                }

                class _InsertResult:
                    rowcount = 0 if key in existing_keys else 1

                if key not in existing_keys:
                    inserted.append(dict(params))
                return _InsertResult()

            return super().execute(query, params)

    conn = _Conn(
        feature_rows=[(42, "aapl_close")],
        source_rows=[(1, "yfinance")],
        resolved_rows=[
            {"feature_id": 42, "obs_date": D0, "value": 100.0, "release_date": D0, "vintage_date": D0},
            {
                "feature_id": 42,
                "obs_date": EXIT_DATE,
                "value": 103.0,
                "release_date": EXIT_DATE,
                "vintage_date": EXIT_DATE,
            },
        ],
    )
    engine = FakeEngine(conn, db_name=db_name)
    return engine, inserted


def _args(**overrides):
    parser = cli.build_arg_parser()
    args = parser.parse_args([])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


# ---------------------------------------------------------------------------
# Dry run is the default: prints a summary, writes nothing
# ---------------------------------------------------------------------------


def test_dry_run_by_default_writes_nothing():
    engine, inserted = _make_signal_sources_engine()
    args = _args(origin_tag="live")
    out = io.StringIO()

    result = cli.run(engine, args, out=out)

    assert result["dry_run"] is True
    assert result["persisted_count"] is None
    assert inserted == []  # nothing written
    printed = json.loads(out.getvalue())
    assert printed["cohort_summary"]["n_total"] == 1
    assert printed["evaluation_version"] == EVALUATION_VERSION


def test_dry_run_summary_reflects_the_evaluated_outcome():
    engine, _inserted = _make_signal_sources_engine()
    args = _args()
    out = io.StringIO()
    result = cli.run(engine, args, out=out)
    # BUY, 100 -> 103 over the default horizon is a +3% CORRECT call.
    assert result["cohort_summary"]["n_correct"] == 1


def test_null_ticker_rows_are_skipped_and_counted():
    rows = [(1, "congressional", None, D0, "BUY")]
    engine, _inserted = _make_signal_sources_engine(extra_rows=rows)
    args = _args()
    out = io.StringIO()
    result = cli.run(engine, args, out=out)
    assert result["n_skipped_null_ticker"] == 1
    assert result["cohort_summary"]["n_total"] == 0


# ---------------------------------------------------------------------------
# --persist requires the explicit acknowledgement flag
# ---------------------------------------------------------------------------


def test_persist_without_acknowledgement_refuses():
    engine, inserted = _make_signal_sources_engine()
    args = _args(persist=True, ack_writes=False)
    with pytest.raises(cli.RefusalError):
        cli.run(engine, args, out=io.StringIO())
    assert inserted == []


def test_persist_with_acknowledgement_writes_one_row():
    engine, inserted = _make_signal_sources_engine()
    args = _args(persist=True, ack_writes=True, origin_tag="synthetic")
    result = cli.run(engine, args, out=io.StringIO())
    assert result["persisted_count"] == 1
    assert len(inserted) == 1
    assert inserted[0]["origin_tag"] == "synthetic"


def test_persist_is_idempotent_on_rerun():
    engine, inserted = _make_signal_sources_engine()
    args = _args(persist=True, ack_writes=True)
    cli.run(engine, args, out=io.StringIO())
    result2 = cli.run(engine, args, out=io.StringIO())
    assert result2["persisted_count"] == 0  # ON CONFLICT DO NOTHING, second time
    assert len(inserted) == 1


# ---------------------------------------------------------------------------
# Database-name safety check
# ---------------------------------------------------------------------------


def test_refuses_non_griddb_prefixed_database_by_default():
    engine, _inserted = _make_signal_sources_engine(db_name="grid")
    args = _args()
    with pytest.raises(cli.RefusalError):
        cli.run(engine, args, out=io.StringIO())


def test_allow_any_db_overrides_the_prefix_check():
    engine, _inserted = _make_signal_sources_engine(db_name="grid")
    args = _args(allow_any_db=True)
    result = cli.run(engine, args, out=io.StringIO())
    assert result["db_name"] == "grid"


def test_griddb_prefixed_database_is_allowed_by_default():
    engine, _inserted = _make_signal_sources_engine(db_name="griddb_test")
    args = _args()
    result = cli.run(engine, args, out=io.StringIO())
    assert result["db_name"] == "griddb_test"


# ---------------------------------------------------------------------------
# It never touches signal_sources.trust_score/outcome or writes anywhere
# other than signal_evaluations.
# ---------------------------------------------------------------------------


def test_source_never_mentions_writing_signal_sources():
    source = (REPO_ROOT / "scripts" / "evaluate_signals.py").read_text(encoding="utf-8")
    assert "UPDATE signal_sources" not in source
    assert "DELETE FROM signal_sources" not in source
    # The old meter is only ever mentioned in prose (this script never
    # imports or calls it) — check for an actual import statement, not
    # just the substring, which the docstring itself also contains.
    assert "import trust_scorer" not in source
    assert "from intelligence.trust_scorer" not in source
    assert "from intelligence import trust_scorer" not in source


# ---------------------------------------------------------------------------
# DB-gated: real, disposable PostgreSQL via GRID_TEST_DB_URL
# ---------------------------------------------------------------------------


def _db_url() -> str | None:
    return os.environ.get("GRID_TEST_DB_URL")


@pytest.fixture(scope="module")
def bootstrapped_engine():
    """Bootstrap a disposable test DB: schema.sql, then `alembic stamp` the
    schema.sql baseline revision and `alembic upgrade head` to pick up
    everything after it (including signal_evaluations_0918), exactly the
    two-step sequence migrations/versions/7e4dfecce247_baseline_schema_from_schema_sql.py's
    own docstring documents for a fresh deployment. Idempotent: if
    signal_evaluations already exists, the bootstrap is skipped.
    """
    db_url = _db_url()
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set — DB-gated CLI test skipped (waiting on the lead's disposable DB)")

    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")

    url = make_url(db_url)

    with engine.connect() as conn:
        already_bootstrapped = conn.execute(
            text(
                "SELECT to_regclass('public.signal_evaluations') IS NOT NULL"
            )
        ).scalar()

    if not already_bootstrapped:
        import db as grid_db

        grid_db.apply_schema(str(REPO_ROOT / "schema.sql"))

        env = dict(os.environ)
        env.update(
            {
                "DB_HOST": url.host or "localhost",
                "DB_PORT": str(url.port or 5432),
                "DB_NAME": url.database or "",
                "DB_USER": url.username or "",
                "DB_PASSWORD": url.password or "",
                "ENVIRONMENT": "development",
            }
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "stamp", "7e4dfecce247"],
            cwd=str(REPO_ROOT),
            env=env,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=str(REPO_ROOT),
            env=env,
            check=True,
            capture_output=True,
        )

    yield engine
    engine.dispose()


@pytest.fixture
def seeded_signal(bootstrapped_engine):
    """Insert two feature_registry/resolved_series rows and one
    signal_sources row for a throwaway ticker, cleaned up afterwards."""
    from sqlalchemy import text

    engine = bootstrapped_engine
    ticker = "ZQXTEST"
    feature_name = f"{ticker.lower()}_close"

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO feature_registry (name, family, description, "
                "transformation, normalization, missing_data_policy, "
                "eligible_from_date, model_eligible) "
                "VALUES (:name, 'equity', 'test fixture', 'raw', 'RAW', "
                "'FORWARD_FILL', '2020-01-01', TRUE) "
                "ON CONFLICT (name) DO NOTHING"
            ),
            {"name": feature_name},
        )
        feature_id = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": feature_name},
        ).scalar()

        source_id = conn.execute(
            text("SELECT id FROM source_catalog WHERE LOWER(name) = LOWER('yfinance')")
        ).scalar()
        assert source_id is not None, "yfinance must be seeded in source_catalog by schema.sql"

        conn.execute(
            text("DELETE FROM resolved_series WHERE feature_id = :fid"), {"fid": feature_id}
        )
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                "VALUES (:fid, :d0, :d0, :d0, 100.0, :sid), "
                "(:fid, :d1, :d1, :d1, 103.0, :sid)"
            ),
            {"fid": feature_id, "d0": D0, "d1": EXIT_DATE, "sid": source_id},
        )

        conn.execute(
            text("DELETE FROM signal_sources WHERE source_type = 'w3d_cli_test' AND ticker = :ticker"),
            {"ticker": ticker},
        )
        signal_id = conn.execute(
            text(
                "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type) "
                "VALUES ('w3d_cli_test', 'fixture', :ticker, :d0, 'BUY') RETURNING id"
            ),
            {"ticker": ticker, "d0": D0},
        ).scalar()

    yield ticker, signal_id

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM signal_evaluations WHERE signal_source_id = :sid"), {"sid": signal_id})
        conn.execute(text("DELETE FROM signal_sources WHERE id = :sid"), {"sid": signal_id})
        conn.execute(text("DELETE FROM resolved_series WHERE feature_id = :fid"), {"fid": feature_id})
        conn.execute(text("DELETE FROM feature_registry WHERE name = :name"), {"name": feature_name})


@pytest.mark.integration
def test_cli_dry_run_then_persist_against_real_db(bootstrapped_engine, seeded_signal):
    ticker, signal_id = seeded_signal
    engine = bootstrapped_engine

    dry_args = _args(source_type="w3d_cli_test", origin_tag="live")
    dry_result = cli.run(engine, dry_args, out=io.StringIO())
    assert dry_result["dry_run"] is True
    assert dry_result["cohort_summary"]["n_total"] == 1
    assert dry_result["cohort_summary"]["n_correct"] == 1

    from sqlalchemy import text

    with engine.connect() as conn:
        count_before = conn.execute(
            text("SELECT COUNT(*) FROM signal_evaluations WHERE signal_source_id = :sid"),
            {"sid": signal_id},
        ).scalar()
    assert count_before == 0

    persist_args = _args(source_type="w3d_cli_test", origin_tag="live", persist=True, ack_writes=True)
    persist_result = cli.run(engine, persist_args, out=io.StringIO())
    assert persist_result["persisted_count"] == 1

    with engine.connect() as conn:
        count_after = conn.execute(
            text("SELECT COUNT(*) FROM signal_evaluations WHERE signal_source_id = :sid"),
            {"sid": signal_id},
        ).scalar()
    assert count_after == 1

    # Idempotent re-run: same evaluation_version + horizon_days -> no new row.
    persist_result_2 = cli.run(engine, persist_args, out=io.StringIO())
    assert persist_result_2["persisted_count"] == 0

    with engine.connect() as conn:
        count_final = conn.execute(
            text("SELECT COUNT(*) FROM signal_evaluations WHERE signal_source_id = :sid"),
            {"sid": signal_id},
        ).scalar()
    assert count_final == 1
