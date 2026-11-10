"""The oracle's registry signals are scoped to the ticker it predicts, and its
schema bootstrap takes no table lock when the columns already exist.

* ``ModelFactory.get_signals_for_model(..., ticker=...)`` returns that ticker's
  rows plus market-wide rows (no ticker), never another ticker's. Before
  2026-11-06 the oracle called it without a ticker, so every registry model fed
  every ticker's signals into each ticker's prediction.
* ``OracleEngine._gather_signals_from_registry`` passes its ticker through.
* ``OracleEngine._ensure_tables`` runs ``ALTER TABLE oracle_predictions ADD
  COLUMN`` only for a missing column: the ALTER takes ACCESS EXCLUSIVE even when
  the column exists, and on 2026-11-06 it timed out behind a reader and killed
  the oracle cycle.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateSchema, DropSchema

from oracle.engine import OracleEngine
from oracle.model_factory import ModelFactory, ModelSpec

_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS signal_registry (
    id              BIGSERIAL PRIMARY KEY,
    signal_id       TEXT NOT NULL,
    source_module   TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    ticker          TEXT,
    direction       TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    z_score         DOUBLE PRECISION,
    confidence      DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from      TIMESTAMPTZ NOT NULL,
    valid_until     TIMESTAMPTZ,
    freshness_hours DOUBLE PRECISION,
    metadata        JSONB,
    provenance      TEXT,
    UNIQUE (signal_id, valid_from)
)
"""


@pytest.fixture
def admission_engine(pg_engine):
    """Admission proofs use only a disposable database and a private schema."""
    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("Model admission proof requires a database ending in _test")
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT current_database()")).scalar_one().endswith("_test")
    schema = "oracle_admission_" + uuid.uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    engine = create_engine(pg_engine.url, connect_args={
        "options": "-c search_path=" + schema + " -c statement_timeout=5000",
    })
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE oracle_models (
                    name TEXT PRIMARY KEY, version TEXT, description TEXT,
                    signal_families JSONB, weight DOUBLE PRECISION DEFAULT 1,
                    predictions_made INTEGER DEFAULT 0, hits INTEGER DEFAULT 0,
                    misses INTEGER DEFAULT 0, partials INTEGER DEFAULT 0,
                    cumulative_pnl DOUBLE PRECISION DEFAULT 0,
                    sharpe DOUBLE PRECISION DEFAULT 0, last_updated TIMESTAMPTZ,
                    active BOOLEAN DEFAULT TRUE
                )
            """))
            conn.execute(text(_REGISTRY_DDL))
        factory = ModelFactory.__new__(ModelFactory)
        factory.engine = engine
        factory._ensure_columns()
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


@pytest.fixture
def registry_rows(admission_engine):
    """Three active rows from one throwaway source: AAPL, MSFT, market-wide."""
    source = f"test_ticker_scope:{uuid.uuid4().hex[:12]}"
    valid_from = datetime.now(timezone.utc) - timedelta(hours=1)
    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('m', TRUE)"))
        for ticker in ("AAPL", "MSFT", None):
            conn.execute(
                text(
                    "INSERT INTO signal_registry (signal_id, source_module, signal_type, "
                    "ticker, direction, value, z_score, confidence, valid_from) "
                    "VALUES (:sid, :src, 'test', :t, 'bullish', 1.0, 1.0, 0.8, :vf)"
                ),
                {"sid": f"{source}:{ticker}", "src": source, "t": ticker, "vf": valid_from},
            )
    return source


def _factory(pg_engine, source: str) -> ModelFactory:
    factory = ModelFactory.__new__(ModelFactory)
    factory.engine = pg_engine
    factory.get_model_spec = lambda name: ModelSpec(name=name, signal_sources=[source])
    return factory


def test_ticker_scope_keeps_own_and_market_wide_rows(admission_engine, registry_rows, monkeypatch):
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    rows = _factory(admission_engine, registry_rows).get_signals_for_model(
        "m", datetime.now(timezone.utc), ticker="AAPL",
    )
    assert sorted(r["ticker"] or "" for r in rows) == ["", "AAPL"]


def test_no_ticker_keeps_the_unscoped_listing(admission_engine, registry_rows, monkeypatch):
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    rows = _factory(admission_engine, registry_rows).get_signals_for_model(
        "m", datetime.now(timezone.utc),
    )
    assert sorted(r["ticker"] or "" for r in rows) == ["", "AAPL", "MSFT"]


def test_gather_signals_from_registry_passes_the_ticker():
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = MagicMock()
    factory = MagicMock()
    factory.get_signals_for_model.return_value = []
    with patch("oracle.model_factory.ModelFactory", return_value=factory):
        eng._gather_signals_from_registry("NVDA", SimpleNamespace(name="m1"))
    assert factory.get_signals_for_model.call_args.kwargs["ticker"] == "NVDA"


class _RecordingConn:
    def __init__(self, columns: set[str]) -> None:
        self.columns = columns
        self.statements: list[str] = []

    def execute(self, stmt, *args, **kwargs):
        sql = str(stmt)
        self.statements.append(sql)
        if "information_schema.columns" in sql:
            return iter([(c,) for c in self.columns])
        if "FROM pg_index" in sql:
            return SimpleNamespace(fetchone=lambda: None)
        return MagicMock()


def _bootstrap(columns: set[str]) -> list[str]:
    conn = _RecordingConn(columns)
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = engine
    eng._ensure_tables()
    return [s for s in conn.statements if "ALTER TABLE oracle_predictions" in s]


def test_bootstrap_skips_alter_when_columns_exist():
    assert _bootstrap({"dedup_keep", "null_write_policy", "recording_policy", "horizon_days"}) == []


def test_bootstrap_adds_only_the_missing_column():
    alters = _bootstrap({"dedup_keep", "null_write_policy", "recording_policy"})
    assert len(alters) == 1
    assert "horizon_days" in alters[0]


def _oracle(engine):
    oracle = OracleEngine.__new__(OracleEngine)
    oracle.engine = engine
    return oracle


def _real_factory(engine):
    factory = ModelFactory.__new__(ModelFactory)
    factory.engine = engine
    return factory


def test_registry_admits_true_only_and_never_seeds_empty_defaults(admission_engine):
    oracle = _oracle(admission_engine)
    factory = _real_factory(admission_engine)
    assert oracle._load_models() == []
    assert factory.list_active_models() == []
    with admission_engine.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM oracle_models")).scalar_one() == 0
        conn.execute(text("""
            INSERT INTO oracle_models (name, active) VALUES
            ('enabled', TRUE), ('disabled', FALSE), ('unknown', NULL),
            ('timeseries_enhanced', FALSE)
        """))
    assert [model.name for model in oracle._load_models()] == ["enabled"]
    assert [model.name for model in factory.list_active_models()] == ["enabled"]
    assert not factory.get_model_spec("unknown").active
    with admission_engine.begin() as conn:
        conn.execute(text("UPDATE oracle_models SET active = FALSE"))
    assert oracle._load_models() == []
    assert factory.list_active_models() == []
    with admission_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM oracle_models")).scalar_one() == 4


@pytest.mark.parametrize("active", [False, None])
def test_current_registry_blocks_stale_spec_signals(admission_engine, registry_rows, active):
    # _factory deliberately returns an old active=True spec. The signal read
    # must still observe retirement in its own database snapshot.
    with admission_engine.begin() as conn:
        conn.execute(text("UPDATE oracle_models SET active = :active WHERE name = 'm'"), {"active": active})
    assert _factory(admission_engine, registry_rows).get_signals_for_model(
        "m", datetime.now(timezone.utc), ticker="AAPL",
    ) == []


def test_startup_default_migration_preserves_retirement_and_unknown(admission_engine):
    from oracle.model_factory import migrate_default_models

    with admission_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO oracle_models (name, active, created_by) VALUES
            ('flow_momentum', FALSE, 'evolver'), ('news_energy', NULL, 'fixture')
        """))
    factory = _real_factory(admission_engine)
    with patch("oracle.model_factory.ModelFactory", return_value=factory):
        migrate_default_models(admission_engine)
        migrate_default_models(admission_engine)
    with admission_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT name, active, created_by, signal_sources FROM oracle_models ORDER BY name
        """)).fetchall()
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("flow_momentum", False, "evolver"), ("news_energy", None, "fixture"),
    ]
    assert all(row[3] for row in rows)
    assert _oracle(admission_engine)._load_models() == []


def test_missing_admission_column_does_not_activate_legacy_rows(admission_engine):
    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name) VALUES ('legacy')"))
        conn.execute(text("ALTER TABLE oracle_models DROP COLUMN active"))
    factory = _real_factory(admission_engine)
    factory._ensure_columns()
    assert not factory.get_model_spec("legacy").active
    assert factory.list_active_models() == []
    factory.create_model(ModelSpec(name="explicit-new"))
    assert [spec.name for spec in factory.list_active_models()] == ["explicit-new"]


def test_empty_roster_stops_timesfm_and_all_input_fallbacks(admission_engine):
    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('timeseries_enhanced', FALSE)"))
    oracle = _oracle(admission_engine)
    oracle.models = [SimpleNamespace(name="timeseries_enhanced")]
    oracle._oracle_one_ticker = MagicMock(side_effect=AssertionError("No inactive model inference"))
    oracle._get_active_tickers = MagicMock(side_effect=AssertionError("No empty-roster fallback"))
    assert oracle.generate_predictions() == []
    oracle._oracle_one_ticker.assert_not_called()
    oracle._get_active_tickers.assert_not_called()
    assert oracle.models == []
    assert oracle._last_store_counts["predictions"] == 0


def test_event_reconciliation_reloads_retired_roster(admission_engine):
    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('m', TRUE)"))
    oracle = _oracle(admission_engine)
    oracle.models = oracle._load_models()

    def reconcile():
        _real_factory(admission_engine).retire_model("m")
        return {"mode": "event_driven"}

    oracle._reconcile_event_driven_weights = reconcile
    assert oracle.evolve_weights() == {"mode": "event_driven"}
    assert oracle.models == []


def test_run_cycle_reloads_after_both_evolvers(admission_engine):
    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('m', TRUE)"))
    oracle = _oracle(admission_engine)
    oracle.models = oracle._load_models()
    oracle.score_expired_predictions = lambda: {}
    oracle.evolve_weights = lambda: {}
    oracle._get_leaderboard = lambda: []
    observed = []
    oracle.generate_predictions = lambda *a, **kw: observed.extend(oracle.models) or []

    def retire():
        _real_factory(admission_engine).retire_model("m")
        return {}

    with patch("oracle.model_evolver.ModelEvolver") as evolver, patch("oracle.trace_evolver.TraceEvolver") as trace:
        evolver.return_value.evolve_cycle.return_value = {}
        trace.return_value.evolve_cycle.side_effect = retire
        result = oracle.run_cycle(["SYNTH"])
    assert observed == []
    assert result["new_predictions"] == 0


def test_locked_admission_serializes_retirement(admission_engine):
    with admission_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO oracle_models (name, active) VALUES
            ('enabled', TRUE), ('disabled', FALSE), ('unknown', NULL)
        """))
    names = ["missing", "unknown", "disabled", "enabled", "enabled"]
    with admission_engine.begin() as writer:
        assert ModelFactory.active_model_names(writer, names, lock=True) == {"enabled"}
        with admission_engine.connect() as retiring:
            retiring.execute(text("SET LOCAL lock_timeout = '100ms'"))
            with pytest.raises(OperationalError) as exc:
                retiring.execute(text("UPDATE oracle_models SET active = FALSE WHERE name = 'enabled'"))
            assert exc.value.orig.pgcode == "55P03"
            retiring.rollback()
    _real_factory(admission_engine).retire_model("enabled")
    with admission_engine.begin() as writer:
        assert ModelFactory.active_model_names(writer, names, lock=True) == set()


def test_ensemble_drops_model_retired_during_vote_assembly(admission_engine):
    from oracle.engine import EnsemblePredictor

    with admission_engine.begin() as conn:
        conn.execute(text("INSERT INTO oracle_models (name, active) VALUES ('m', TRUE)"))
    predictor = EnsemblePredictor.__new__(EnsemblePredictor)
    predictor.engine = admission_engine
    predictor.factory = _real_factory(admission_engine)
    predictor.factory.get_signals_for_model = lambda *a, **kw: [{}] * 3
    predictor._get_hit_rate = lambda *a, **kw: 0.5
    predictor._get_bucket_weight = lambda *a, **kw: 1.0

    def aggregate(*a):
        _real_factory(admission_engine).retire_model("m")
        return SimpleNamespace(direction="bullish", strength=1.0, confidence=0.8,
                               coherence=1.0, signal_count=3)

    predictor.aggregator = SimpleNamespace(aggregate=aggregate)
    with patch("intelligence.liquidity_regime.classify_current_regime", return_value=None), \
            patch("oracle.regime_router.RegimeRouter", side_effect=RuntimeError("unused fixture")):
        result = predictor.predict("SYNTH")
    assert result.model_count == 0
    assert result.model_votes == []
    assert result.confidence == 0
    assert result.regime_router_weights == {}
