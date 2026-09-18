"""DB-gated API contract tests for GET /api/v1/godview/pillars/cftc.

Exercises api/routers/godview_pillars.py::get_cftc_pillar directly against a
real (scratch) database via godview_pg_engine, bypassing FastAPI's HTTP
layer and auth dependency -- the router function itself takes no request
object, so it is called like any other function under test.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from godview.cftc_pillar import materialize_cftc_pillar
from tests.fixtures.godview.cftc_fixture import build_cot_records

pytestmark = pytest.mark.integration


def _insert_raw_series_on_schedule(conn, contract_key: str, records: list[dict], *, source_id: int) -> None:
    """Insert each record with an explicit pull_timestamp near ITS OWN release_date.

    Mimics a normal weekly puller run (rather than a one-shot historical
    backfill via NOW()), so every resulting row classifies as
    availability_basis='observed_acquisition' -- see
    godview/cftc_pillar.py::classify_availability_basis. Relying on
    raw_series.pull_timestamp's NOW() default here would instead backfill
    every row at the real test-run time, weeks/years after each report's
    schedule-inferred release_date, and (correctly, per Slice A) classify
    all of them 'inferred_schedule' -- excluded from a strict-PIT read by
    default and irrelevant to what this file is testing.
    """
    from godview.cftc_pillar import compute_release_date, parse_cot_record

    for record in records:
        parsed = parse_cot_record(record)
        if parsed is None:
            continue
        release_date, _ = compute_release_date(parsed.report_date)
        pull_timestamp = (
            datetime.combine(release_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=20)
            if release_date is not None
            else datetime.now(timezone.utc)
        )
        for metric, value in parsed.metrics.items():
            conn.execute(
                text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, "
                    "pull_timestamp, pull_status) "
                    "VALUES (:sid, :src, :od, :val, :pts, 'SUCCESS')"
                ),
                {
                    "sid": f"cftc.{contract_key}.{metric}",
                    "src": source_id,
                    "od": parsed.report_date,
                    "val": value,
                    "pts": pull_timestamp,
                },
            )


def _ensure_source_catalog_row(conn, name: str) -> int:
    row = conn.execute(text("SELECT id FROM source_catalog WHERE name = :n"), {"n": name}).fetchone()
    if row is not None:
        return row[0]
    row = conn.execute(
        text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, "
            "pit_available, revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'https://example.invalid', 'FREE', 'WEEKLY', TRUE, 'NEVER', 'HIGH', 30) "
            "RETURNING id"
        ),
        {"n": name},
    ).fetchone()
    return row[0]


def test_api_router_never_500s_when_the_table_is_absent(godview_pg_engine, monkeypatch):
    """Simulate a database that lacks cftc_positioning_daily -- the router must
    degrade to the unavailable state, not raise. Creating a second, table-less
    database is out of scope for this lane, so the missing-table branch is
    exercised the same way the route itself decides to take it: by making
    ``_table_exists`` report False, exactly as it would against a real DB
    that predates this migration."""
    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: godview_pg_engine)
    monkeypatch.setattr(router_module, "_table_exists", lambda conn, name: False)

    result = router_module.get_cftc_pillar(as_of=date.today(), _token="test")

    assert result["available"] is False
    assert result["status"] == "unavailable"


def test_api_router_returns_available_generation_end_to_end(godview_pg_engine, monkeypatch):
    engine = godview_pg_engine
    contract_key = f"APITEST_{uuid.uuid4().hex[:8]}"
    contract_code = f"A{uuid.uuid4().hex[:6]}"
    contracts = {contract_key: {"contract_code": contract_code, "contract_name": "API Test", "asset_class": "test"}}
    records = build_cot_records(n_weeks=10)

    with engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"CFTC_COT_API_TEST_{uuid.uuid4().hex[:8]}")
        _insert_raw_series_on_schedule(conn, contract_key, records, source_id=sid)

    last_report_date = date.fromisoformat(records[-1]["report_date_as_yyyy_mm_dd"][:10])
    result = materialize_cftc_pillar(engine, as_of=last_report_date, contracts=contracts)
    assert result.status == "SUCCESS"

    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: engine)
    monkeypatch.setattr(router_module, "CFTC_PILLAR_CONTRACTS", contracts)

    as_of = last_report_date + timedelta(days=3)  # the Friday release
    response = router_module.get_cftc_pillar(as_of=as_of, _token="test")

    assert response["available"] is True
    assert response["pillar"] == "cftc_positioning"
    assert response["include_inferred"] is False
    assert contract_code in response["fields"]
    assert response["fields"][contract_code]["total_open_interest"]["provenance"] == "measured"
    assert response["fields"][contract_code]["total_open_interest"]["availability_basis"] == "observed_acquisition"
    assert response["fields"][contract_code]["total_open_interest"]["availability_basis_note"] is None
    assert response["fields"][contract_code]["z_score_1y"]["availability"] in ("available", "unavailable")


def test_api_router_exposes_availability_basis_and_admits_inferred_rows_when_flagged(
    godview_pg_engine, monkeypatch
):
    """A backfilled (weeks-late) pull -> inferred_schedule: excluded by default,
    admitted and labelled once ?include_inferred=true is passed through."""
    engine = godview_pg_engine
    contract_key = f"APIBACK_{uuid.uuid4().hex[:8]}"
    contract_code = f"K{uuid.uuid4().hex[:6]}"
    contracts = {contract_key: {"contract_code": contract_code, "contract_name": "API Backfill", "asset_class": "test"}}
    report_date = date(2026, 1, 6)  # a Tuesday
    records = build_cot_records(n_weeks=1, start_date=report_date)
    release_date = report_date + timedelta(days=3)
    backfill_pull = datetime.combine(release_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(weeks=10)

    with engine.begin() as conn:
        sid = _ensure_source_catalog_row(conn, f"CFTC_COT_API_BACKFILL_{uuid.uuid4().hex[:8]}")
        for record in records:
            from godview.cftc_pillar import parse_cot_record

            parsed = parse_cot_record(record)
            for metric, value in parsed.metrics.items():
                conn.execute(
                    text(
                        "INSERT INTO raw_series (series_id, source_id, obs_date, value, "
                        "pull_timestamp, pull_status) "
                        "VALUES (:sid, :src, :od, :val, :pts, 'SUCCESS')"
                    ),
                    {
                        "sid": f"cftc.{contract_key}.{metric}",
                        "src": sid,
                        "od": parsed.report_date,
                        "val": value,
                        "pts": backfill_pull,
                    },
                )

    result = materialize_cftc_pillar(engine, as_of=backfill_pull.date(), contracts=contracts)
    assert result.status == "SUCCESS"

    import api.routers.godview_pillars as router_module

    monkeypatch.setattr(router_module, "get_db_engine", lambda: engine)
    monkeypatch.setattr(router_module, "CFTC_PILLAR_CONTRACTS", contracts)

    as_of = backfill_pull.date() + timedelta(days=1)

    default_response = router_module.get_cftc_pillar(as_of=as_of, _token="test")
    assert contract_code not in default_response["fields"]

    inferred_response = router_module.get_cftc_pillar(as_of=as_of, include_inferred=True, _token="test")
    assert inferred_response["include_inferred"] is True
    assert contract_code in inferred_response["fields"]
    field = inferred_response["fields"][contract_code]["total_open_interest"]
    assert field["availability_basis"] == "inferred_schedule"
    assert field["availability_basis_note"] == "availability inferred from schedule; record revised/backfilled"
