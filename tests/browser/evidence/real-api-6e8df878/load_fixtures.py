"""Load fixture data through the tree's own code paths (no raw ad hoc SQL
where a loader/materializer already exists), against the disposable DB.

1. God View CFTC materializer: build_cot_records() + parse_cot_record()
   (mirrors tests/godview/test_cftc_pillar_db.py::_insert_raw_series) then
   materialize_cftc_pillar() -> one generation.
2. feature_registry / resolved_series / signal_sources fixture for ticker
   TEST1 (mirrors tests/test_evaluate_signals_cli.py::seeded_signal, but
   left in place -- not cleaned up -- so the real-API journey checks have
   backing data for TEST1).
"""
from __future__ import annotations

import os
from datetime import date

from sqlalchemy import create_engine, text

import config

engine = create_engine(config.settings.DB_URL)

# ---------------------------------------------------------------------
# 1. God View CFTC pillar: one generation via the real materializer
# ---------------------------------------------------------------------
from godview.cftc_pillar import CFTC_PILLAR_CONTRACTS, materialize_cftc_pillar, parse_cot_record
from ingestion.altdata.cftc_cot import _build_series_id
from tests.fixtures.godview.cftc_fixture import build_cot_records

FIXTURE_CONTRACT_KEY = "ES_FIXTURE"
FIXTURE_CONTRACT_CODE = "ES"

def _ensure_source_catalog_row(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row:
        return row[0]
    return conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank, active) "
            "VALUES (:n, 'https://example.invalid', 'FREE', 'WEEKLY', TRUE, 'RARE', 'HIGH', 99, TRUE) "
            "RETURNING id"
        ),
        {"n": name},
    ).scalar()


records = build_cot_records(n_weeks=8)
with engine.begin() as conn:
    source_id = _ensure_source_catalog_row(conn, "dbproof_cftc_fixture")
    for record in records:
        parsed = parse_cot_record(record)
        if parsed is None:
            continue
        for metric, value in parsed.metrics.items():
            conn.execute(
                text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
                    "VALUES (:sid, :src, :od, :val, 'SUCCESS') ON CONFLICT DO NOTHING"
                ),
                {
                    "sid": _build_series_id(FIXTURE_CONTRACT_KEY, metric),
                    "src": source_id,
                    "od": parsed.report_date,
                    "val": value,
                },
            )

as_of_date = date.fromisoformat(records[-1]["report_date_as_yyyy_mm_dd"][:10])
contracts = {
    FIXTURE_CONTRACT_KEY: {
        "contract_code": FIXTURE_CONTRACT_CODE,
        "contract_name": "E-mini S&P 500 (fixture)",
        "asset_class": "equity_index",
    }
}
result = materialize_cftc_pillar(engine, as_of=as_of_date, contracts=contracts)
print("GODVIEW_MATERIALIZE_RESULT status=%s generation_id=%s as_of=%s" % (
    result.status, getattr(result, "generation_id", None), as_of_date))

# ---------------------------------------------------------------------
# 2. feature_registry / resolved_series / signal_sources for TEST1
# ---------------------------------------------------------------------
TICKER = "TEST1"
FEATURE_NAME = f"{TICKER.lower()}_close"
D0 = date(2026, 1, 5)
EXIT_DATE = date(2026, 1, 10)

with engine.begin() as conn:
    conn.execute(
        text(
            "INSERT INTO feature_registry (name, family, description, "
            "transformation, normalization, missing_data_policy, "
            "eligible_from_date, model_eligible) "
            "VALUES (:name, 'equity', 'dbproof fixture', 'raw', 'RAW', "
            "'FORWARD_FILL', '2020-01-01', TRUE) "
            "ON CONFLICT (name) DO NOTHING"
        ),
        {"name": FEATURE_NAME},
    )
    feature_id = conn.execute(
        text("SELECT id FROM feature_registry WHERE name = :name"), {"name": FEATURE_NAME}
    ).scalar()

    source_id2 = conn.execute(
        text("SELECT id FROM source_catalog WHERE LOWER(name) = LOWER('yfinance')")
    ).scalar()
    assert source_id2 is not None, "yfinance must be seeded in source_catalog by schema.sql"

    conn.execute(text("DELETE FROM resolved_series WHERE feature_id = :fid"), {"fid": feature_id})
    conn.execute(
        text(
            "INSERT INTO resolved_series "
            "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
            "VALUES (:fid, :d0, :d0, :d0, 100.0, :sid), "
            "(:fid, :d1, :d1, :d1, 103.0, :sid)"
        ),
        {"fid": feature_id, "d0": D0, "d1": EXIT_DATE, "sid": source_id2},
    )

    conn.execute(
        text("DELETE FROM signal_sources WHERE source_type = 'dbproof_test' AND ticker = :ticker"),
        {"ticker": TICKER},
    )
    signal_id = conn.execute(
        text(
            "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type) "
            "VALUES ('dbproof_test', 'fixture', :ticker, :d0, 'BUY') RETURNING id"
        ),
        {"ticker": TICKER, "d0": D0},
    ).scalar()

print("SIGNAL_FIXTURE_OK ticker=%s feature_id=%s signal_id=%s" % (TICKER, feature_id, signal_id))
