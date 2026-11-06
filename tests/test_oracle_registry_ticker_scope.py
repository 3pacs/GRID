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
from sqlalchemy import text

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
def registry_rows(pg_engine):
    """Three active rows from one throwaway source: AAPL, MSFT, market-wide."""
    source = f"test_ticker_scope:{uuid.uuid4().hex[:12]}"
    valid_from = datetime.now(timezone.utc) - timedelta(hours=1)
    with pg_engine.begin() as conn:
        conn.execute(text(_REGISTRY_DDL))
        for ticker in ("AAPL", "MSFT", None):
            conn.execute(
                text(
                    "INSERT INTO signal_registry (signal_id, source_module, signal_type, "
                    "ticker, direction, value, z_score, confidence, valid_from) "
                    "VALUES (:sid, :src, 'test', :t, 'bullish', 1.0, 1.0, 0.8, :vf)"
                ),
                {"sid": f"{source}:{ticker}", "src": source, "t": ticker, "vf": valid_from},
            )
    yield source
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM signal_registry WHERE source_module = :s"), {"s": source})


def _factory(pg_engine, source: str) -> ModelFactory:
    factory = ModelFactory.__new__(ModelFactory)
    factory.engine = pg_engine
    factory.get_model_spec = lambda name: ModelSpec(name=name, signal_sources=[source])
    return factory


def test_ticker_scope_keeps_own_and_market_wide_rows(pg_engine, registry_rows, monkeypatch):
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    rows = _factory(pg_engine, registry_rows).get_signals_for_model(
        "m", datetime.now(timezone.utc), ticker="AAPL",
    )
    assert sorted(r["ticker"] or "" for r in rows) == ["", "AAPL"]


def test_no_ticker_keeps_the_unscoped_listing(pg_engine, registry_rows, monkeypatch):
    monkeypatch.setenv("GRID_SIGNAL_REGISTRY", "1")
    rows = _factory(pg_engine, registry_rows).get_signals_for_model(
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
    assert _bootstrap({"dedup_keep", "null_write_policy", "horizon_days"}) == []


def test_bootstrap_adds_only_the_missing_column():
    alters = _bootstrap({"dedup_keep", "null_write_policy"})
    assert len(alters) == 1
    assert "horizon_days" in alters[0]
