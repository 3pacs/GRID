"""Shared catalog rows are judged by their owner's own series (SERIES_ANCHORS).

``finra_dix`` writes ``finra:dix_sp500`` under the FINRA_SHORT_VOLUME
source_catalog row (id 4679 on griddb), and SmartScheduler bumps that row's
``last_pull_at`` for every DIX run. From 2026-11-12 the short-volume puller
wrote nothing for obs 11-12, 11-13 and 11-18..11-20 (PR #1325), while every
source-level check judged the row by "newest row / last_pull_at for the
source" and read it fresh for 8 days.

These tests run the real ``store.observations`` SQL on an in-memory SQLite
``raw_series`` shaped like production, with the 2026-11-20 state: the
short-volume series' newest observation is obs 2026-11-17 (landed 11-18 by a
Hermes repair), DIX landed obs 11-19 that morning, and ``last_pull_at`` is
DIX's bump.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

from alerts.cadence import cadence_for_source, is_stale
from ingestion import source_freshness as sf
from ingestion.smart_scheduler import PULLER_REGISTRY, catalog_name_for

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FINRA = "FINRA_SHORT_VOLUME"
FINRA_ID = 4679
OTHER_ID = 7
MARKET_RATIO = "finra:short_volume_agg:market_ratio"
SPY = "finra:short_volume:SPY"
DIX = "finra:dix_sp500"

# Friday 2026-11-20, 14:00Z: the third trade date in a row the short-volume
# puller skipped.
NOW = datetime(2026, 11, 20, 14, 0, tzinfo=timezone.utc)
AS_OF = NOW.date()
SHORT_VOLUME_LANDED = datetime(2026, 11, 18, 7, 50)   # obs 11-17, via Hermes repair
DIX_LANDED = datetime(2026, 11, 20, 7, 30)            # obs 11-19, that morning


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc)


@pytest.fixture()
def db():
    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("last_pull_at", DateTime),
    )
    raw = Table(
        "raw_series", md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(catalog.insert(), [
            {"id": FINRA_ID, "name": FINRA, "last_pull_at": DIX_LANDED},
            {"id": OTHER_ID, "name": "SOMEONE_ELSE", "last_pull_at": None},
        ])

    def add(series_id, obs_date, landed, *, status="SUCCESS", source_id=FINRA_ID, value=0.5):
        with engine.begin() as conn:
            conn.execute(raw.insert(), {
                "series_id": series_id, "source_id": source_id, "obs_date": obs_date,
                "pull_timestamp": landed, "value": value, "raw_payload": "{}",
                "pull_status": status,
            })

    return engine, add


def _dix_only_day(add) -> None:
    """The 2026-11-20 state: short-volume series last landed 11-18, DIX today."""
    for d, landed in ((date(2026, 11, 16), datetime(2026, 11, 17, 8, 0)),
                      (date(2026, 11, 17), SHORT_VOLUME_LANDED)):
        add(MARKET_RATIO, d, landed)
        add(SPY, d, landed)
    for d, landed in ((date(2026, 11, 18), datetime(2026, 11, 19, 7, 30)),
                      (date(2026, 11, 19), DIX_LANDED)):
        add(DIX, d, landed)


def _anchor():
    anchor = sf.anchor_for(FINRA)
    assert anchor is not None
    return anchor


def _read(engine) -> sf.AnchoredFreshness:
    with engine.connect() as conn:
        return sf.read_anchored_freshness(conn, _anchor(), as_of=AS_OF)


# ── the acceptance case ──────────────────────────────────────────────────


def test_dix_only_day_reads_stale_for_the_short_volume_puller(db):
    engine, add = db
    _dix_only_day(add)

    anchored = _read(engine)

    assert anchored.error is None
    assert anchored.landed_at == _utc(SHORT_VOLUME_LANDED)
    assert {r.series_id: r.obs_date for r in anchored.readings} == {
        MARKET_RATIO: date(2026, 11, 17),
        SPY: date(2026, 11, 17),
    }
    cadence = cadence_for_source(FINRA)
    assert is_stale(anchored.landed_at, NOW, cadence)


def test_the_old_rule_read_the_same_day_fresh(db):
    """Pins the bug: newest row or last_pull_at for the source is DIX's."""
    engine, add = db
    _dix_only_day(add)
    with engine.connect() as conn:
        newest_row = conn.exec_driver_sql(
            "SELECT MAX(pull_timestamp) FROM raw_series "
            "WHERE source_id = ? AND pull_status = 'SUCCESS'", (FINRA_ID,),
        ).scalar()
    old_effective = _utc(datetime.fromisoformat(str(newest_row)))
    assert old_effective == _utc(DIX_LANDED)
    assert not is_stale(old_effective, NOW, cadence_for_source(FINRA))


def test_pipeline_status_on_a_dix_only_day_is_not_healthy(db):
    """pipeline-health: DIX's rows and runs no longer make the row healthy."""
    engine, add = db
    _dix_only_day(add)
    run_rows = [
        # (source_id, puller_name, last_run_at, last_rows_at, runs_7d, status, rows, error)
        (FINRA_ID, "smart:finra_dix", DIX_LANDED, DIX_LANDED, 6, "SUCCESS", 1, None),
        (FINRA_ID, "smart:finra_short_volume", datetime(2026, 11, 20, 9, 0), None, 6,
         "SUCCESS", 0, "NO_NEW_DATA: run completed, 0 rows written"),
    ]
    runs = sf.merge_catalog_runs(run_rows, {FINRA.lower(): FINRA_ID})
    fresh_hours = sf.catalog_freshness_hours(FINRA)

    old = sf.classify_catalog_source(
        now=NOW, fresh_hours=fresh_hours, last_write_at=_utc(DIX_LANDED), run=runs[FINRA_ID],
    )
    new = sf.classify_catalog_source(
        now=NOW, fresh_hours=fresh_hours,
        last_write_at=_read(engine).landed_at, run=sf.anchored_run(runs[FINRA_ID]),
    )

    assert old == sf.PIPELINE_HEALTHY  # what the DIX row said before
    assert new == sf.PIPELINE_QUIET    # the owner ran and wrote nothing


# ── how anchors are read ─────────────────────────────────────────────────


def test_fresh_short_volume_series_read_fresh(db):
    engine, add = db
    _dix_only_day(add)
    landed = datetime(2026, 11, 20, 8, 5)
    add(MARKET_RATIO, date(2026, 11, 19), landed)
    add(SPY, date(2026, 11, 19), landed)

    anchored = _read(engine)

    assert anchored.landed_at == _utc(landed)
    assert not is_stale(anchored.landed_at, NOW, cadence_for_source(FINRA))


def test_stalest_anchor_decides(db):
    """Per-symbol rows landing without the aggregate (or the reverse) is stale."""
    engine, add = db
    _dix_only_day(add)
    add(SPY, date(2026, 11, 19), datetime(2026, 11, 20, 8, 5))  # producer wrote, aggregate did not

    anchored = _read(engine)

    assert anchored.stalest.series_id == MARKET_RATIO
    assert anchored.landed_at == _utc(SHORT_VOLUME_LANDED)
    assert is_stale(anchored.landed_at, NOW, cadence_for_source(FINRA))


def test_an_anchor_with_no_observation_is_never(db):
    engine, add = db
    add(SPY, date(2026, 11, 19), datetime(2026, 11, 20, 8, 5))
    add(DIX, date(2026, 11, 19), DIX_LANDED)

    anchored = _read(engine)

    assert anchored.stalest.series_id == MARKET_RATIO
    assert anchored.landed_at is None
    assert is_stale(anchored.landed_at, NOW, cadence_for_source(FINRA))


def test_only_this_rows_accepted_observations_count(db):
    """FAILED rows, another source's rows and future obs dates are not observations."""
    engine, add = db
    _dix_only_day(add)
    today = datetime(2026, 11, 20, 9, 0)
    add(MARKET_RATIO, date(2026, 11, 20), today, status="FAILED", value=0.0)
    add(SPY, date(2026, 11, 19), today, source_id=OTHER_ID)
    add(MARKET_RATIO, date(2026, 11, 23), today)  # obs after as_of

    anchored = _read(engine)

    assert {r.series_id: r.obs_date for r in anchored.readings} == {
        MARKET_RATIO: date(2026, 11, 17),
        SPY: date(2026, 11, 17),
    }
    assert anchored.landed_at == _utc(SHORT_VOLUME_LANDED)


def test_a_failed_read_is_reported_and_never_fresh():
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("statement timeout")

    anchored = sf.read_anchored_freshness(conn, _anchor(), as_of=AS_OF)

    assert anchored.error is not None and "statement timeout" in anchored.error
    assert anchored.landed_at is None
    assert anchored.as_dict()["error"] == anchored.error


def test_judged_by_record_names_each_anchor(db):
    engine, add = db
    _dix_only_day(add)

    record = _read(engine).as_dict()

    assert record["owner"] == "finra_short_volume"
    assert [s["series_id"] for s in record["series"]] == [MARKET_RATIO, SPY]
    assert record["series"][0]["obs_date"] == "2026-11-17"
    assert record["series"][0]["landed_at"].startswith("2026-11-18T07:50")


# ── pull_log attribution ─────────────────────────────────────────────────


def test_dix_runs_are_guests_of_the_short_volume_row():
    assert sf.guest_pull_log_names(_anchor()) == frozenset({"smart:finra_dix"})
    assert "smart:finra_dix" in sf.GUEST_PULL_LOG_NAMES
    assert "smart:finra_short_volume" not in sf.GUEST_PULL_LOG_NAMES


def test_merge_drops_guest_runs():
    run_rows = [
        (FINRA_ID, "smart:finra_dix", DIX_LANDED, DIX_LANDED, 6, "FAILED", 0, "TIMEOUT: 600s"),
        (FINRA_ID, "smart:finra_short_volume", datetime(2026, 11, 20, 6, 0), None, 6,
         "SUCCESS", 0, "NO_NEW_DATA: run completed, 0 rows written"),
    ]

    merged = sf.merge_catalog_runs(run_rows, {FINRA.lower(): FINRA_ID})[FINRA_ID]

    assert merged.last_status == "SUCCESS"
    assert merged.last_run_at == _utc(datetime(2026, 11, 20, 6, 0))
    assert merged.last_success_with_rows_at is None
    assert merged.runs_7d == 6


def test_anchored_run_leaves_write_evidence_to_the_anchors():
    run = sf.CatalogRunSummary(
        last_run_at=NOW, last_success_with_rows_at=NOW, last_status="SUCCESS",
        last_rows_inserted=12_000, last_error=None, runs_7d=5,
    )
    assert sf.anchored_run(run).last_success_with_rows_at is None
    assert sf.anchored_run(run).last_status == "SUCCESS"
    assert sf.anchored_run(None) is None


# ── the anchor map cannot drift from the writers ─────────────────────────


def test_anchor_owner_writes_under_the_anchored_row():
    by_name = {e["name"]: e for e in PULLER_REGISTRY}
    for anchor in sf.SERIES_ANCHORS:
        assert anchor.owner in by_name, anchor
        assert catalog_name_for(anchor.owner).lower() == anchor.catalog_name.lower()
        assert sf.guest_pull_log_names(anchor), "an anchor is only needed on a shared row"


def test_finra_anchor_series_are_the_short_volume_writers_own():
    from ingestion.altdata import finra_dix, finra_short_volume, finra_short_volume_agg

    assert _anchor().series == (
        finra_short_volume_agg.AGG_MARKET_RATIO,
        finra_short_volume.FINRAShortVolumePuller.series_id("spy"),
    )
    assert finra_short_volume_agg.FINRAShortVolumeAggPuller.SOURCE_NAME == FINRA
    # DIX shares the row but is not one of its anchors.
    assert finra_dix.FINRADixPuller.SOURCE_NAME == FINRA
    assert finra_dix.DIX_SERIES not in _anchor().series
