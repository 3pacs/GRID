"""Opt-in proof on a disposable loopback database, never the configured GRID DB.

GRID_WING_IV_TEST_DSN must name a grid_wing_iv_test_* database at 127.0.0.1
with an explicit port. A random schema is created and dropped in that DB only.
"""
import importlib
import json
import os
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from tests.test_options_capture_batch import SESSION_NOW, _run, puller as _capture_fixture

capture_puller = _capture_fixture


@pytest.mark.integration
def test_nullable_migration_and_actual_producer_upsert_preserve_legacy(capture_puller, monkeypatch):
    dsn = os.environ.get("GRID_WING_IV_TEST_DSN")
    if not dsn:
        pytest.skip("GRID_WING_IV_TEST_DSN required for disposable PostgreSQL proof")
    url = make_url(dsn)
    if (url.drivername != "postgresql+psycopg2" or url.host != "127.0.0.1"
            or not url.port or url.query or not (url.database or "").startswith("grid_wing_iv_test_")):
        pytest.fail("Requires explicit 127.0.0.1:port / grid_wing_iv_test_* and no query overrides")
    engine = create_engine(url, connect_args={
        "host": "127.0.0.1", "hostaddr": "127.0.0.1", "port": url.port, "dbname": url.database,
    })
    schema = "wing_iv_" + uuid4().hex
    revision = importlib.import_module("migrations.versions.wing_iv_proxy_20261116")
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            conn.exec_driver_sql(f'SET LOCAL search_path TO "{schema}"')
            conn.exec_driver_sql("""CREATE TABLE options_daily_signals (
                ticker TEXT, signal_date DATE, put_call_ratio DOUBLE PRECISION,
                max_pain DOUBLE PRECISION, iv_skew DOUBLE PRECISION, total_oi BIGINT,
                total_volume BIGINT, near_expiry DATE, spot_price DOUBLE PRECISION,
                iv_atm DOUBLE PRECISION, iv_25d_put DOUBLE PRECISION, iv_25d_call DOUBLE PRECISION,
                term_structure_slope DOUBLE PRECISION, oi_concentration DOUBLE PRECISION,
                UNIQUE(ticker, signal_date))""")
            conn.exec_driver_sql("""CREATE TABLE resolved_series (
                feature_id INTEGER, obs_date DATE, release_date DATE, vintage_date DATE,
                value DOUBLE PRECISION, source_priority_used INTEGER,
                UNIQUE(feature_id, obs_date, vintage_date))""")
            conn.exec_driver_sql("""CREATE TABLE feature_registry (
                id SERIAL, name TEXT UNIQUE, family TEXT, description TEXT,
                transformation TEXT, transformation_version INTEGER, lag_days INTEGER,
                normalization TEXT, missing_data_policy TEXT, eligible_from_date DATE,
                model_eligible BOOLEAN)""")
            conn.execute(text("INSERT INTO options_daily_signals (ticker, signal_date, iv_25d_put, iv_25d_call) VALUES ('SPY', :d, .9, .8)"), {"d": SESSION_NOW.date()})
            conn.exec_driver_sql("INSERT INTO resolved_series (value) VALUES (.9)")
            monkeypatch.setattr(revision, "op", Operations(MigrationContext.configure(conn)))
            revision.upgrade()
            revision.upgrade()  # additive, safe to retry
            legacy = conn.exec_driver_sql("SELECT iv_25d_put, iv_25d_call, iv_m90_put_v1, wing_iv_metadata FROM options_daily_signals").one()
            assert legacy == (.9, .8, None, None)
            assert conn.exec_driver_sql("SELECT value, options_iv_metadata FROM resolved_series").one() == (.9, None)

            result, _ = _run(capture_puller, [90, 100, 110])
            assert result["status"] == "SUCCESS"
            statement, params = next((s, p) for s, p in capture_puller.engine.calls if "INSERT INTO options_daily_signals" in s)
            conn.execute(text(statement), params)
            row = conn.exec_driver_sql("SELECT iv_25d_put, iv_25d_call, iv_m90_put_v1, iv_m110_call_v1, wing_iv_metadata FROM options_daily_signals").one()
            assert row[:4] == (.9, .8, .2, .2)
            assert row[4] == json.loads(params["wing_meta"])

            from ingestion.options import OptionsPuller
            publisher = OptionsPuller.__new__(OptionsPuller)
            publisher.source_id = 7
            metadata = row[4]
            signals = {"iv_m90_put_v1": ("vol", "Put IV proxy v1", .2)}
            assert publisher._push_to_resolved(conn, "SPY", str(SESSION_NOW.date()), signals, wing_metadata=metadata) == 1
            # Later captures cannot attach their metadata to the first saved value.
            assert publisher._push_to_resolved(conn, "SPY", str(SESSION_NOW.date()), {
                "iv_m90_put_v1": ("vol", "Put IV proxy v1", .8),
            }, wing_metadata={**metadata, "capture_batch_id": "later"}) == 0
            value = conn.exec_driver_sql("SELECT value, options_iv_metadata FROM resolved_series WHERE feature_id IS NOT NULL").one()
            assert value == (.2, metadata)

            # Surfacer replaces daily-signal context without a capture receipt.
            from scripts import pull_surfacer_options as surfacer
            df = pd.DataFrame({"strike": [100], "openInterest": [10],
                               "volume": [1], "impliedVolatility": [.2]})
            stock = SimpleNamespace(options=[str(SESSION_NOW.date()+timedelta(days=30))],
                                    option_chain=lambda *a: SimpleNamespace(calls=df, puts=df))
            monkeypatch.setattr(surfacer, "_require_yfinance", lambda: SimpleNamespace(Ticker=lambda *a: stock))
            monkeypatch.setattr(surfacer, "_spot_price", lambda *a: 105)
            with conn.connection.cursor() as cursor:
                assert surfacer._pull_one(cursor, "SPY", 7, str(SESSION_NOW.date()))["status"] == "done"
            assert conn.exec_driver_sql("SELECT iv_25d_put, iv_m90_put_v1, wing_iv_metadata FROM options_daily_signals").one() == (.9, None, None)
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        engine.dispose()
