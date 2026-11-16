"""GDELT 2.0 bulk loader (ingestion/altdata/gdelt_bulk.py): fixture export zips, a fake fetcher.

The database tests run in their own schema (dropped afterwards) of the
``pg_engine`` database and skip when no PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
import io
import json
import zipfile
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
from sqlalchemy import create_engine, text

from ingestion.altdata import gdelt_bulk as gb

UTC = timezone.utc
DAY = date(2026, 11, 15)
SCHEMA = "gdelt_bulk_test"


# --- fixtures: export files -------------------------------------------------------------


def export_row(event_id: int, stamp: datetime, *, sqldate: str | None = None, quad: int = 1,
               a1: str = "USA", a2: str = "", tone: float = -2.0, goldstein: float = 1.0,
               mentions: int = 3, ncols: int = gb.EXPORT_COLUMNS) -> list[str]:
    row = [""] * ncols
    row[gb.COL_GLOBALEVENTID] = str(event_id)
    row[gb.COL_SQLDATE] = sqldate or stamp.strftime("%Y%m%d")
    row[gb.COL_ACTOR1COUNTRYCODE] = a1
    row[gb.COL_ACTOR2COUNTRYCODE] = a2
    row[gb.COL_EVENTROOTCODE] = "04"
    row[gb.COL_QUADCLASS] = str(quad)
    row[gb.COL_GOLDSTEINSCALE] = str(goldstein)
    row[gb.COL_NUMMENTIONS] = str(mentions)
    row[gb.COL_NUMSOURCES] = "1"
    row[gb.COL_NUMARTICLES] = str(mentions)
    row[gb.COL_AVGTONE] = str(tone)
    if ncols > gb.COL_DATEADDED:
        row[gb.COL_DATEADDED] = gb.format_stamp(stamp)
        row[-1] = f"https://example.test/{event_id}"
    return row


def export_zip(stamp: datetime, rows: list[list[str]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{gb.format_stamp(stamp)}.export.CSV", "".join("\t".join(r) + "\n" for r in rows))
    return buf.getvalue()


def lastupdate(newest: datetime) -> bytes:
    s = gb.format_stamp(newest)
    return (f"100 abc {gb.BASE_URL.replace('https', 'http')}{s}.export.CSV.zip\n"
            f"100 def http://data.gdeltproject.org/gdeltv2/{s}.mentions.CSV.zip\n"
            f"100 ghi http://data.gdeltproject.org/gdeltv2/{s}.gkg.csv.zip\n").encode()


class FakeGdelt:
    """Serves export zips by URL and counts every request."""

    def __init__(self, newest: datetime) -> None:
        self.files: dict[str, gb.Fetched] = {}
        self.calls: list[str] = []
        self.newest = newest
        self.fetched_at = newest + timedelta(hours=3)

    def add_day(self, day: date, *, per_file: int = 2, first_id: int | None = None,
                published_offset: timedelta = timedelta(minutes=-11), skip: set[int] = frozenset(),
                last_modified: bool = True) -> None:
        next_id = first_id if first_id is not None else int(day.strftime("%Y%m%d")) * 1000
        for i, stamp in enumerate(gb.day_stamps(day)):
            if i in skip:
                continue
            rows = []
            for j in range(per_file):
                rows.append(export_row(next_id, stamp, quad=(j % 4) + 1, a1="USA" if j % 2 == 0 else "CHN",
                                       a2="RUS" if j == 1 else "", tone=-1.0 - j, goldstein=float(j)))
                next_id += 1
            self.put(stamp, export_zip(stamp, rows),
                     stamp + published_offset if last_modified else None)

    def put(self, stamp: datetime, body: bytes, published: datetime | None, status: int = 200) -> None:
        self.files[gb.export_url(stamp)] = gb.Fetched(
            status, body, format_datetime(published, usegmt=True) if published else None, self.fetched_at)

    def __call__(self, url: str) -> gb.Fetched:
        self.calls.append(url)
        if url == gb.LASTUPDATE_URL:
            return gb.Fetched(200, lastupdate(self.newest), None, self.fetched_at)
        return self.files.get(url) or gb.Fetched(404, b"", None, self.fetched_at)

    def export_calls(self) -> list[str]:
        return [c for c in self.calls if c != gb.LASTUPDATE_URL]


# --- pure pieces ------------------------------------------------------------------------


def test_stamps_are_utc_on_the_15_minute_grid():
    assert gb.parse_stamp("20261115234500") == datetime(2026, 11, 15, 23, 45, tzinfo=UTC)
    for bad in ("20261115234000", "2026111523450", "20261115234501", "abcdefghijklmn"):
        with pytest.raises(ValueError):
            gb.parse_stamp(bad)
    stamps = gb.day_stamps(DAY)
    assert len(stamps) == 96 and stamps[0].hour == 0 and stamps[-1] == datetime(2026, 11, 15, 23, 45, tzinfo=UTC)
    assert gb.export_url(stamps[1]) == "https://data.gdeltproject.org/gdeltv2/20261115001500.export.CSV.zip"


def test_lastupdate_names_the_newest_export():
    assert gb.parse_lastupdate(lastupdate(datetime(2026, 11, 16, 9, 45, tzinfo=UTC)).decode()) == \
        datetime(2026, 11, 16, 9, 45, tzinfo=UTC)
    with pytest.raises(gb.GdeltBulkError):
        gb.parse_lastupdate("<html>busy</html>")


def test_publication_instant_is_last_modified_else_the_later_fetch_instant():
    fetched = datetime(2026, 11, 16, 2, 0, tzinfo=UTC)
    when, basis = gb.published_at("Sun, 15 Nov 2026 23:33:33 GMT", fetched)
    assert (when, basis) == (datetime(2026, 11, 15, 23, 33, 33, tzinfo=UTC), "last_modified")
    assert gb.published_at(None, fetched) == (fetched, "fetched_at")
    assert gb.published_at("not a date", fetched) == (fetched, "fetched_at")


def test_parse_export_counts_cameo_countries_and_rejects_bad_rows():
    stamp = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    rows = [
        export_row(1, stamp, quad=1, a1="USA", a2="CHN", tone=-2.0, goldstein=2.0, mentions=4),
        export_row(2, stamp, quad=3, a1="IRN", a2="USA", tone=-4.0, goldstein=-6.0, mentions=6),
        export_row(3, stamp, quad=4, a1="US", tone=0.0, goldstein=-2.0, sqldate="20251115"),  # 'US' is not CAMEO
        export_row(2, stamp, quad=1),                       # duplicate id: first row kept
        export_row(4, stamp, ncols=58),                     # GDELT 1.0 width: rejected
        export_row(5, stamp, sqldate="2026-11-15"),         # unreadable SQLDATE: rejected
        export_row(6, stamp + gb.SLOT),                     # DATEADDED of another file: kept, counted
    ]
    parsed = gb.parse_export(export_zip(stamp, rows), stamp)
    assert [r["global_event_id"] for r in parsed.rows] == [1, 2, 3, 6]
    assert parsed.rejected == 2 and parsed.dateadded_mismatch == 1
    assert parsed.rows[2]["event_date"] == date(2025, 11, 15) and parsed.rows[0]["date_added"] == stamp
    agg = parsed.aggregate
    assert (agg.events, agg.conflict, agg.cooperation, agg.mentions) == (4, 2, 2, 4 + 6 + 3 + 3)
    assert agg.countries == {"us": 3, "cn": 1, "ru": 0, "ir": 1, "tw": 0, "ua": 0}
    values = gb.feature_values(agg)
    assert values["gdelt_avg_tone"] == pytest.approx((-2.0 - 4.0 + 0.0 - 2.0) / 4)
    assert values["gdelt_events_us"] == 3.0 and set(values) == set(gb.FEATURES)


def test_aggregates_add_up_across_files_and_survive_json():
    stamp = datetime(2026, 11, 15, 0, 0, tzinfo=UTC)
    a = gb.parse_export(export_zip(stamp, [export_row(1, stamp, a1="UKR", tone=-1.0)]), stamp).aggregate
    b = gb.parse_export(export_zip(stamp, [export_row(2, stamp, a1="TWN", tone=-3.0)]), stamp).aggregate
    total = gb.FileAggregate.from_json(json.loads(json.dumps(a.__dict__)))
    total.add(b)
    assert total.events == 2 and total.countries["ua"] == 1 and total.countries["tw"] == 1
    assert gb.feature_values(total)["gdelt_avg_tone"] == pytest.approx(-2.0)


def test_an_html_page_served_as_a_file_is_an_error_not_an_empty_file():
    with pytest.raises(gb.GdeltBulkError):
        gb.parse_export(b"<html>rate limited</html>", datetime(2026, 11, 15, tzinfo=UTC))


def test_a_day_with_no_tone_has_no_average_rather_than_zero():
    values = gb.feature_values(gb.FileAggregate())
    assert "gdelt_avg_tone" not in values and "gdelt_avg_goldstein" not in values
    assert values["gdelt_total_events"] == 0.0


# --- database ---------------------------------------------------------------------------

_CORE_DDL = (
    """CREATE TABLE source_catalog (
        id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, base_url TEXT NOT NULL,
        cost_tier TEXT NOT NULL, latency_class TEXT NOT NULL, pit_available BOOLEAN NOT NULL DEFAULT FALSE,
        revision_behavior TEXT NOT NULL, trust_score TEXT NOT NULL, priority_rank INTEGER NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE)""",
    """CREATE TABLE feature_registry (
        id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, family TEXT NOT NULL, subfamily TEXT,
        description TEXT NOT NULL, transformation TEXT NOT NULL,
        transformation_version INTEGER NOT NULL DEFAULT 1, lag_days INTEGER NOT NULL DEFAULT 0,
        normalization TEXT NOT NULL, missing_data_policy TEXT NOT NULL, eligible_from_date DATE NOT NULL,
        model_eligible BOOLEAN NOT NULL DEFAULT FALSE)""",
    """CREATE TABLE resolved_series (
        id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
        value DOUBLE PRECISION NOT NULL, source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
        conflict_flag BOOLEAN NOT NULL DEFAULT FALSE, conflict_detail JSONB,
        resolution_version INTEGER NOT NULL DEFAULT 1)""",
    "CREATE UNIQUE INDEX uq_rs ON resolved_series (feature_id, obs_date, vintage_date)",
)


@pytest.fixture
def engine(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {SCHEMA}"))
    eng = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={SCHEMA}"})
    migration = importlib.import_module("migrations.versions.gdelt_bulk_loader_20261116")
    with eng.begin() as conn:
        for stmt in _CORE_DDL + migration.DDL:
            conn.execute(text(stmt))
    yield eng
    eng.dispose()
    with pg_engine.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))


def scalar(eng, sql, **params):
    with eng.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def features(eng):
    with eng.connect() as conn:
        return {r[0]: (r[1], r[2], r[3], r[4]) for r in conn.execute(text(
            "SELECT f.name, r.obs_date, r.release_date, r.vintage_date, r.value FROM resolved_series r "
            "JOIN feature_registry f ON f.id = r.feature_id"))}


def legacy_day(eng, day: date) -> None:
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO gdelt_daily_summary (summary_date, total_events) VALUES (:d, 7)"),
                     {"d": day})


NEWEST = datetime(2026, 11, 16, 0, 30, tzinfo=UTC)


def test_a_full_day_loads_and_is_dated_by_its_last_publication(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)

    assert s.status == "ok" and s.first_day == DAY.isoformat() and s.days_finalised == [DAY.isoformat()]
    assert len(fake.export_calls()) == 96 and s.events_inserted == 192
    assert scalar(engine, "SELECT count(*) FROM gdelt_events WHERE date_added IS NOT NULL") == 192
    assert scalar(engine, "SELECT count(*) FROM gdelt_export_files WHERE status = 'loaded'") == 96
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM gdelt_daily_summary WHERE summary_date = :d"), {"d": DAY}).mappings().one()
    assert (row["total_events"], row["day_basis"], row["files_loaded"], row["files_missing"]) == \
        (192, "dateadded_v2", 96, 0)
    assert row["published_at"] == datetime(2026, 11, 15, 23, 34, tzinfo=UTC)
    assert row["country_events"]["USA"] == 96 and row["country_events"]["RUS"] == 96

    got = features(engine)
    assert set(got) == set(gb.FEATURES)
    # Every point: observed on D, known on D+1 although D's last file went up on D (never release = obs).
    assert {v[:3] for v in got.values()} == {(DAY, DAY + timedelta(days=1), DAY + timedelta(days=1))}
    assert got["gdelt_total_events"][3] == 192.0 and got["gdelt_events_cn"][3] == 96.0
    assert scalar(engine, "SELECT count(*) FROM gdelt_daily_summary WHERE summary_date < :d", d=DAY) == 1


def test_a_file_published_after_midnight_moves_the_whole_day_to_the_next_date(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    last = gb.day_stamps(DAY)[-1]
    rows = [export_row(9_000_001, last)]
    fake.put(last, export_zip(last, rows), last + timedelta(minutes=98))  # measured 2026-11-15
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.late_days == [DAY.isoformat()]
    assert {v[1:3] for v in features(engine).values()} == {(DAY + timedelta(days=1),) * 2}


def test_a_file_published_two_days_late_moves_the_day_to_that_date(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    last = gb.day_stamps(DAY)[-1]
    fake.put(last, export_zip(last, [export_row(9_000_001, last)]), last + timedelta(hours=25))
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.late_days == [DAY.isoformat()]
    assert {v[1:3] for v in features(engine).values()} == {(DAY + timedelta(days=2),) * 2}


def test_release_day_is_never_the_observed_day():
    d = date(2026, 11, 15)
    assert gb.release_day(d, datetime(2026, 11, 15, 23, 34, tzinfo=UTC)) == date(2026, 11, 16)
    assert gb.release_day(d, datetime(2026, 11, 14, 23, 0, tzinfo=UTC)) == date(2026, 11, 16)
    assert gb.release_day(d, datetime(2026, 11, 17, 0, 5, tzinfo=UTC)) == date(2026, 11, 17)


def test_without_last_modified_the_fetch_instant_dates_the_day(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY, last_modified=False)
    gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert scalar(engine, "SELECT count(*) FROM gdelt_export_files WHERE published_basis = 'fetched_at'") == 96
    assert {v[1] for v in features(engine).values()} == {fake.fetched_at.date()}


def test_rerun_fetches_nothing_and_writes_nothing(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    before = [scalar(engine, f"SELECT count(*) FROM {t}") for t in
              ("gdelt_events", "gdelt_export_files", "gdelt_daily_summary", "resolved_series")]
    fake.calls.clear()
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert fake.export_calls() == [] and s.days_finalised == [] and s.first_day is None
    after = [scalar(engine, f"SELECT count(*) FROM {t}") for t in
             ("gdelt_events", "gdelt_export_files", "gdelt_daily_summary", "resolved_series")]
    assert before == after


def test_a_budget_stop_resumes_where_it_left_off_and_fetches_each_file_once(engine):
    legacy_day(engine, DAY - timedelta(days=2))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY - timedelta(days=1))
    fake.add_day(DAY)
    ticks = iter(range(10_000))
    loader = gb.GdeltBulkLoader(engine, fetch=fake, clock=lambda: float(next(ticks)))
    first = loader.run(write=True, budget_seconds=130)   # one clock tick per fetch check
    assert first.status == "budget" and first.days_finalised == [(DAY - timedelta(days=1)).isoformat()]
    second = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert second.status == "ok" and second.days_finalised == [DAY.isoformat()]
    urls = fake.export_calls()
    assert len(urls) == len(set(urls)) == 192
    assert scalar(engine, "SELECT count(*) FROM gdelt_daily_summary WHERE day_basis = 'dateadded_v2'") == 2


def test_a_recent_404_waits_and_an_old_one_is_recorded_missing(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY, skip={95})
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.status == "waiting" and s.days_finalised == [] and s.files_missing == 0
    assert scalar(engine, "SELECT count(*) FROM gdelt_export_files") == 95
    assert scalar(engine, "SELECT count(*) FROM resolved_series") == 0

    fake.newest = NEWEST + timedelta(days=1)
    fake.calls.clear()
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None, until=DAY)
    assert fake.export_calls() == [gb.export_url(gb.day_stamps(DAY)[95])]
    assert s.files_missing == 1 and s.days_finalised == [DAY.isoformat()]
    assert scalar(engine, "SELECT files_missing FROM gdelt_daily_summary WHERE summary_date = :d", d=DAY) == 1
    assert scalar(engine, "SELECT total_events FROM gdelt_daily_summary WHERE summary_date = :d", d=DAY) == 190


def test_events_already_stored_are_not_inserted_twice_but_still_counted_in_the_day(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    first_id = int(DAY.strftime("%Y%m%d")) * 1000
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO gdelt_events (global_event_id, event_date) VALUES (:i, :d)"),
                     {"i": first_id, "d": DAY})
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.events_parsed == 192 and s.events_inserted == 191
    assert features(engine)["gdelt_total_events"][3] == 192.0


def test_an_existing_point_for_the_same_vintage_is_kept_and_reported(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    loader = gb.GdeltBulkLoader(engine, fetch=fake)
    with engine.begin() as conn:
        src, ids = loader._registry(conn)
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) VALUES (:f, :d, :k, :k, 1.0, :s)"),
                     {"f": ids["gdelt_conflict_count"], "d": DAY, "k": DAY + timedelta(days=1), "s": src})
    s = loader.run(write=True, budget_seconds=None)
    assert s.features_written == 11 and s.features_skipped_existing == 1
    assert features(engine)["gdelt_conflict_count"][3] == 1.0


def test_dry_run_writes_nothing(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=False, budget_seconds=None)
    assert s.files_loaded == 96 and s.events_parsed == 192 and s.days_finalised == []
    for table in ("gdelt_events", "gdelt_export_files", "resolved_series", "feature_registry"):
        assert scalar(engine, f"SELECT count(*) FROM {table}") == 0


def test_a_bad_file_stops_the_run_and_records_nothing_for_it(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    fake.put(gb.day_stamps(DAY)[10], b"<html>busy</html>", None)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.status == "error" and "20261115023000" in s.detail
    assert scalar(engine, "SELECT count(*) FROM gdelt_export_files") == 10
    assert scalar(engine, "SELECT count(*) FROM gdelt_daily_summary WHERE day_basis IS NOT NULL") == 0


def test_a_server_error_stops_the_run(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    fake.put(gb.day_stamps(DAY)[3], b"", None, status=503)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.status == "error" and s.files_missing == 0
    assert scalar(engine, "SELECT count(*) FROM gdelt_export_files") == 3


def test_a_second_runner_is_locked_out(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(NEWEST)
    with engine.connect() as holder:
        holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": gb.LOCK_KEY})
        s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": gb.LOCK_KEY})
        holder.commit()
    assert s.status == "locked" and fake.calls == []


def test_legacy_days_are_never_refetched(engine):
    legacy_day(engine, DAY)
    fake = FakeGdelt(NEWEST)
    fake.add_day(DAY)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, since=DAY, budget_seconds=None)
    assert fake.export_calls() == [] and s.first_day is None
    assert scalar(engine, "SELECT total_events FROM gdelt_daily_summary WHERE summary_date = :d", d=DAY) == 7


def test_today_is_not_finalised_before_its_last_file(engine):
    legacy_day(engine, DAY - timedelta(days=1))
    fake = FakeGdelt(datetime(2026, 11, 15, 23, 30, tzinfo=UTC))
    fake.add_day(DAY)
    s = gb.GdeltBulkLoader(engine, fetch=fake).run(write=True, budget_seconds=None)
    assert s.first_day is None and fake.export_calls() == []
