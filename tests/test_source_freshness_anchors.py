"""Shared catalog rows are judged by trade-date coverage of their owner's own series.

``finra_dix`` writes ``finra:dix_sp500`` under the FINRA_SHORT_VOLUME
source_catalog row (id 4679 on griddb), and SmartScheduler bumps that row's
``last_pull_at`` for every DIX run. From 2026-11-12 the short-volume puller
wrote nothing for obs 11-12, 11-13 and 11-18..11-20 (PR #1325), while every
source-level check judged the row by "newest row / last_pull_at for the
source" and read it fresh for 8 days.

The row is now fresh only when each anchor series' newest accepted
observation is dated on or after the newest trade date due by now: an NYSE
session on the hash-pinned XNYS calendar whose FINRA file is out (~18:00 ET)
plus one 24 h scheduler cadence. When the observation landed is reported,
never judged (Codex review 6009985487: obs 2026-11-10 re-landed at 17:40Z on
11-20 read HEALTHY under the landing rule).

The store reads run the real ``store.observations`` SQL on an in-memory
SQLite ``raw_series`` shaped like production. The due dates use the real
pinned calendar (``docs/reference/calendar-manifests``).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
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
SHORT_VOLUME_LANDED = datetime(2026, 11, 18, 7, 50)   # obs 11-17, via Hermes repair
DIX_LANDED = datetime(2026, 11, 20, 7, 30)            # obs 11-19, that morning


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc)


def _z(y: int, m: int, d: int, hh: int, mm: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


def _anchor() -> sf.SeriesAnchor:
    anchor = sf.anchor_for(FINRA)
    assert anchor is not None
    return anchor


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
    """The 2026-11-20 state: short-volume series' newest obs is 11-17; DIX landed today."""
    for d, landed in ((date(2026, 11, 16), datetime(2026, 11, 17, 8, 0)),
                      (date(2026, 11, 17), SHORT_VOLUME_LANDED)):
        add(MARKET_RATIO, d, landed)
        add(SPY, d, landed)
    for d, landed in ((date(2026, 11, 18), datetime(2026, 11, 19, 7, 30)),
                      (date(2026, 11, 19), DIX_LANDED)):
        add(DIX, d, landed)


def _read(engine, now: datetime = NOW) -> sf.AnchoredFreshness:
    with engine.connect() as conn:
        return sf.read_anchored_freshness(conn, _anchor(), now=now)


# ── the due trade date: release clock + pinned XNYS calendar ──────────────


@pytest.mark.parametrize(
    ("now", "due", "case"),
    [
        (NOW, date(2026, 11, 18), "incident day: 11-19's file is not due until 11-20 23:00Z"),
        (_z(2026, 11, 18, 20), date(2026, 11, 16), "normal late arrival: 11-17's pull may land until 23:00Z"),
        (_z(2026, 11, 18, 23, 30), date(2026, 11, 17), "11-17 is due once a cadence has passed"),
        (_z(2026, 11, 21, 10), date(2026, 11, 19), "Saturday morning: Friday's file is not due yet"),
        (_z(2026, 11, 22, 10), date(2026, 11, 20), "Sunday: Friday is the newest due session"),
        (_z(2026, 11, 23, 10), date(2026, 11, 20), "Monday morning: still Friday"),
        (_z(2026, 11, 12, 23, 30), date(2026, 11, 11), "Veterans Day is an NYSE session"),
        (_z(2026, 11, 27, 12), date(2026, 11, 25), "day after Thanksgiving: the holiday is not due"),
        (_z(2026, 11, 28, 23, 30), date(2026, 11, 27), "the early-close day still has a file"),
        (_z(2026, 7, 6, 12), date(2026, 7, 2), "observed Independence Day (Fri 07-03) is not a session"),
        (_z(2026, 7, 16, 22, 30), date(2026, 7, 15), "summer: 18:00 EDT is 22:00Z"),
        (_z(2026, 7, 16, 21, 30), date(2026, 7, 14), "summer: before 22:00Z, the day before"),
    ],
)
def test_due_trade_date(now, due, case):
    got, calendar = sf.expected_obs_date(_anchor(), now)
    assert got == due, case
    assert calendar.startswith("pinned XNYS")


def test_a_date_past_the_pinned_calendar_is_refused():
    with pytest.raises(ValueError, match="outside the pinned XNYS calendar"):
        sf.expected_obs_date(_anchor(), _z(2028, 1, 5, 12))


# ── the acceptance cases ─────────────────────────────────────────────────


def test_dix_only_day_reads_stale_for_the_short_volume_puller(db):
    engine, add = db
    _dix_only_day(add)

    anchored = _read(engine)

    assert anchored.error is None
    assert anchored.expected_obs_date == date(2026, 11, 18)
    assert anchored.newest_obs_date == date(2026, 11, 17)
    assert not anchored.covered
    assert anchored.landed_at == _utc(SHORT_VOLUME_LANDED)  # reported, not judged


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


def test_an_old_trade_date_landed_again_is_not_current(db):
    """Codex's witness: obs 2026-11-10 re-landed at 17:40Z on 11-20, no owner run."""
    engine, add = db
    relanded = datetime(2026, 11, 20, 17, 40)
    add(MARKET_RATIO, date(2026, 11, 10), relanded)
    add(SPY, date(2026, 11, 10), relanded)
    now = _z(2026, 11, 20, 18)

    anchored = _read(engine, now)

    assert anchored.landed_at == _utc(relanded)
    assert anchored.newest_obs_date == date(2026, 11, 10)
    assert not anchored.covered
    status = sf.classify_anchored_source(
        now=now, fresh_hours=sf.catalog_freshness_hours(FINRA), anchored=anchored, run=None,
    )
    assert status == sf.PIPELINE_STALE
    # The landing rule this replaces called the same reading healthy.
    assert sf.classify_catalog_source(
        now=now, fresh_hours=sf.catalog_freshness_hours(FINRA), last_write_at=_utc(relanded), run=None,
    ) == sf.PIPELINE_HEALTHY


def test_pipeline_status_on_a_dix_only_day_is_stale_not_healthy(db):
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
    new = sf.classify_anchored_source(
        now=NOW, fresh_hours=fresh_hours, anchored=_read(engine), run=runs[FINRA_ID],
    )

    assert old == sf.PIPELINE_HEALTHY  # what the DIX row said before
    # A trade date is due and missing: neither healthy nor "quiet", although
    # the owner's last run said it found nothing new.
    assert new == sf.PIPELINE_STALE


def test_missing_coverage_keeps_the_owners_failure_and_stop_states(db):
    engine, add = db
    _dix_only_day(add)
    anchored = _read(engine)
    failed = sf.CatalogRunSummary(
        last_run_at=_utc(datetime(2026, 11, 20, 9)), last_success_with_rows_at=None,
        last_status="FAILED", last_rows_inserted=0, last_error="TIMEOUT: 60s", runs_7d=4,
    )
    assert sf.classify_anchored_source(
        now=NOW, fresh_hours=48, anchored=anchored, run=failed,
    ) == sf.PIPELINE_FAILING
    long_after = NOW + timedelta(days=10)
    assert sf.classify_anchored_source(
        now=long_after, fresh_hours=48, anchored=_read(engine, long_after), run=None,
    ) == sf.PIPELINE_STOPPED


# ── how anchors are read ─────────────────────────────────────────────────


def test_covered_series_read_fresh_whatever_the_landing_age(db):
    engine, add = db
    _dix_only_day(add)
    landed = datetime(2026, 11, 19, 8, 5)
    add(MARKET_RATIO, date(2026, 11, 18), landed)
    add(SPY, date(2026, 11, 18), landed)

    anchored = _read(engine)

    assert anchored.covered
    assert sf.classify_anchored_source(
        now=NOW, fresh_hours=48, anchored=anchored, run=None,
    ) == sf.PIPELINE_HEALTHY


def test_friday_data_covers_the_weekend(db):
    engine, add = db
    landed = datetime(2026, 11, 21, 8, 0)
    add(MARKET_RATIO, date(2026, 11, 20), landed)
    add(SPY, date(2026, 11, 20), landed)

    for now in (_z(2026, 11, 21, 12), _z(2026, 11, 22, 12), _z(2026, 11, 23, 12)):
        assert _read(engine, now).covered, now
    assert not _read(engine, _z(2026, 11, 24, 23, 30)).covered  # Monday 11-23 is due


def test_thanksgiving_is_not_due(db):
    engine, add = db
    landed = datetime(2026, 11, 26, 8, 0)
    add(MARKET_RATIO, date(2026, 11, 25), landed)
    add(SPY, date(2026, 11, 25), landed)

    assert _read(engine, _z(2026, 11, 27, 12)).covered
    assert not _read(engine, _z(2026, 11, 28, 23, 30)).covered  # Friday 11-27 is due


def test_stalest_anchor_decides(db):
    """Per-symbol rows landing without the aggregate (or the reverse) is not covered."""
    engine, add = db
    _dix_only_day(add)
    add(SPY, date(2026, 11, 18), datetime(2026, 11, 19, 8, 5))  # producer wrote, aggregate did not

    anchored = _read(engine)

    assert anchored.stalest.series_id == MARKET_RATIO
    assert anchored.newest_obs_date == date(2026, 11, 17)
    assert not anchored.covered


def test_an_anchor_with_no_observation_is_never_covered(db):
    engine, add = db
    add(SPY, date(2026, 11, 19), datetime(2026, 11, 20, 8, 5))
    add(DIX, date(2026, 11, 19), DIX_LANDED)

    anchored = _read(engine)

    assert anchored.stalest.series_id == MARKET_RATIO
    assert anchored.newest_obs_date is None
    assert anchored.landed_at is None
    assert not anchored.covered


def test_only_this_rows_accepted_observations_count(db):
    """FAILED rows, another source's rows and future obs dates are not observations."""
    engine, add = db
    _dix_only_day(add)
    today = datetime(2026, 11, 20, 9, 0)
    add(MARKET_RATIO, date(2026, 11, 19), today, status="FAILED", value=0.0)
    add(SPY, date(2026, 11, 19), today, source_id=OTHER_ID)
    add(MARKET_RATIO, date(2026, 11, 23), today)  # obs after the UTC date

    anchored = _read(engine)

    assert {r.series_id: r.obs_date for r in anchored.readings} == {
        MARKET_RATIO: date(2026, 11, 17),
        SPY: date(2026, 11, 17),
    }
    assert not anchored.covered


def test_a_failed_read_is_reported_and_never_covered():
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("statement timeout")

    anchored = sf.read_anchored_freshness(conn, _anchor(), now=NOW)

    assert anchored.error is not None and "statement timeout" in anchored.error
    assert anchored.expected_obs_date == date(2026, 11, 18)
    assert not anchored.covered
    assert anchored.as_dict()["error"] == anchored.error


def test_an_unreadable_calendar_is_reported_and_never_covered(db):
    engine, add = db
    for d in (date(2027, 12, 30), date(2027, 12, 31)):
        add(MARKET_RATIO, d, datetime(2028, 1, 1, 8))
        add(SPY, d, datetime(2028, 1, 1, 8))

    anchored = _read(engine, _z(2028, 1, 5, 12))

    assert anchored.expected_obs_date is None
    assert anchored.error is not None and "outside the pinned XNYS calendar" in anchored.error
    assert anchored.newest_obs_date == date(2027, 12, 31)
    assert not anchored.covered


def test_judged_by_record(db):
    engine, add = db
    _dix_only_day(add)

    record = _read(engine).as_dict()

    assert record["owner"] == "finra_short_volume"
    assert record["rule"] == "trade_date_coverage"
    assert record["covered"] is False
    assert record["expected_obs_date"] == "2026-11-18"
    assert record["newest_obs_date"] == "2026-11-17"
    assert record["landed_at"].startswith("2026-11-18T07:50")
    assert record["calendar"].startswith("pinned XNYS")
    assert "18:00 ET" in record["release"]
    assert [s["series_id"] for s in record["series"]] == [MARKET_RATIO, SPY]


def test_alert_line_names_the_missing_trade_date(db):
    from alerts.health_alerter import _format_stale_source

    engine, add = db
    _dix_only_day(add)
    anchored = _read(engine)
    line = _format_stale_source({
        "source": FINRA, "last_pull": anchored.landed_at.isoformat(), "cadence": "TRADE_DATE",
        "age_hours": 54.2, "judged_by": anchored.as_dict(),
        "last_attempt_status": "SUCCESS", "last_attempt_at": "2026-11-20T09:00:00+00:00",
    })

    assert "newest trade date 2026-11-17, due 2026-11-18" in line
    assert "newest landed 2026-11-18 07:50Z, 2.3d ago" in line
    assert "last success" not in line
    assert "last attempt SUCCESS" in line


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
        # Due one owner cadence after release.
        assert anchor.release.due_after == timedelta(hours=float(by_name[anchor.owner]["freq_h"]))


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


def test_finra_release_clock_matches_the_schedulers_cutoff():
    import inspect

    from ingestion import smart_scheduler

    clock = _anchor().release
    assert (clock.local_time.hour, clock.local_time.minute, clock.tz) == (18, 0, "America/New_York")
    source = inspect.getsource(smart_scheduler._finra_short_volume_trade_date)
    assert 'ZoneInfo("America/New_York")' in source and "now_et.hour < 18" in source
