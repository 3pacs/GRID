"""scripts/rebuild_gdelt_history.py: plan (read-only) and apply (snapshot, copy, delete, rebuild).

Fixture GDELT 1.0 daily zips and GDELT 2.0 15-minute zips, a fake fetcher,
and an isolated schema of the ``pg_engine`` database (skipped without one).
"""

from __future__ import annotations

import csv
import gzip
import importlib
import io
import zipfile
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
from sqlalchemy import create_engine, text

from ingestion.altdata import gdelt_bulk as gb
from scripts import rebuild_gdelt_history as rb
from tests.test_gdelt_bulk import _CORE_DDL, export_row, export_zip

UTC = timezone.utc
SCHEMA = "gdelt_rebuild_test"
D1, D2, D3 = date(2025, 6, 10), date(2025, 6, 11), date(2025, 6, 12)


def v1_row(event_id: int, day: date, *, a1: str = "USA", quad: int = 1, ncols: int = gb.V1_EXPORT_COLUMNS,
           dateadded: str | None = None) -> list[str]:
    row = [""] * ncols
    row[gb.COL_GLOBALEVENTID] = str(event_id)
    row[gb.COL_SQLDATE] = day.strftime("%Y%m%d")
    row[gb.COL_ACTOR1COUNTRYCODE] = a1
    row[gb.COL_QUADCLASS] = str(quad)
    row[gb.COL_GOLDSTEINSCALE] = "1.0"
    row[gb.COL_NUMMENTIONS] = "2"
    row[gb.COL_AVGTONE] = "-3.0"
    if ncols > gb.V1_COL_DATEADDED:
        row[gb.V1_COL_DATEADDED] = dateadded or day.strftime("%Y%m%d")
    return row


def v1_zip(day: date, rows: list[list[str]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{day:%Y%m%d}.export.CSV", "".join("\t".join(r) + "\n" for r in rows))
    return buf.getvalue()


class Fake:
    """URL -> (status, body, Last-Modified); records (url, head) calls."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[int, bytes, datetime | None]] = {}
        self.calls: list[tuple[str, bool]] = []

    def v1(self, day: date, rows: list[list[str]], published: datetime | None) -> None:
        self.files[rb.V1_URL.format(day=f"{day:%Y%m%d}")] = (200, v1_zip(day, rows), published)

    def __call__(self, url: str, head_only: bool) -> gb.Fetched:
        self.calls.append((url, head_only))
        status, body, published = self.files.get(url, (404, b"", None))
        return gb.Fetched(status, b"" if head_only else body,
                          format_datetime(published, usegmt=True) if published else None,
                          datetime(2026, 11, 16, 12, tzinfo=UTC))


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


# --- pure ------------------------------------------------------------------------------


def test_v1_daily_file_aggregates_cameo_countries_and_rejects_other_widths():
    rows = [v1_row(1, D1, a1="USA", quad=3), v1_row(2, D1, a1="RUS"), v1_row(2, D1),
            v1_row(3, D1, ncols=gb.EXPORT_COLUMNS), v1_row(4, D1, dateadded="20250609")]
    parsed = gb.parse_daily_v1(v1_zip(D1, rows), D1)
    assert parsed.rows == [] and parsed.rejected == 1 and parsed.dateadded_mismatch == 1
    agg = parsed.aggregate
    assert (agg.events, agg.conflict, agg.countries["us"], agg.countries["ru"]) == (3, 1, 2, 1)


def test_a_day_is_dated_by_the_remote_last_modified_never_the_archive(tmp_path):
    (tmp_path / f"{D1:%Y%m%d}.export.CSV.zip").write_bytes(v1_zip(D1, [v1_row(1, D1)]))
    fake = Fake()
    fake.v1(D1, [v1_row(99, D1)], at(D2, 7))           # the remote body is not fetched: archive wins
    rec = rb.build_day(D1, tmp_path, fake)
    assert rec["status"] == "ok" and rec["origin"] == "archive" and rec["release_date"] == D2.isoformat()
    assert rec["values"]["gdelt_total_events"] == 1.0 and fake.calls == [(rb.V1_URL.format(day="20250610"), True)]


def test_a_re_uploaded_file_dates_the_day_by_the_re_upload():
    fake = Fake()
    fake.v1(D1, [v1_row(1, D1)], datetime(2026, 4, 9, 3, tzinfo=UTC))
    assert rb.build_day(D1, None, fake)["release_date"] == "2026-04-09"


def test_without_last_modified_the_day_is_left_out():
    fake = Fake()
    fake.v1(D1, [v1_row(1, D1)], None)
    assert rb.build_day(D1, None, fake)["status"] == "undated"


def test_a_day_with_no_daily_file_is_built_from_all_96_v2_files_or_left_out():
    fake = Fake()
    for i, stamp in enumerate(gb.day_stamps(D1)):
        fake.files[gb.export_url(stamp)] = (200, export_zip(stamp, [export_row(i + 1, stamp)]),
                                            stamp + timedelta(minutes=-11 if i < 95 else 30))
    rec = rb.build_day(D1, None, fake)
    assert rec["status"] == "ok" and rec["source"] == gb.DAY_BASIS and rec["files"] == 96
    assert rec["values"]["gdelt_total_events"] == 96.0 and rec["release_date"] == D2.isoformat()
    del fake.files[gb.export_url(gb.day_stamps(D1)[40])]
    assert rb.build_day(D1, None, fake)["status"] == "missing"


# --- database --------------------------------------------------------------------------


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


def seed_legacy(eng) -> dict[str, int]:
    """Legacy same-day rows on D1..D3 (country zeros), a legacy article count, and one loader row (D+1)."""
    with eng.begin() as conn:
        src, ids = gb.GdeltBulkLoader(eng)._registry(conn)
        old = conn.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, revision_behavior, "
            "trust_score, priority_rank) VALUES ('GDELT', 'x', 'FREE', 'EOD', 'NEVER', 'MED', 9) RETURNING id"
        )).scalar_one()
        art = conn.execute(text(
            "INSERT INTO feature_registry (name, family, description, transformation, normalization, "
            "missing_data_policy, eligible_from_date) VALUES ('gdelt_article_count', 'sentiment', 'x', 'RAW', "
            "'ZSCORE', 'FORWARD_FILL', '2020-01-01') RETURNING id")).scalar_one()
        for d in (D1, D2, D3):
            for name in ("gdelt_total_events", "gdelt_events_us"):
                conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, "
                                  "value, source_priority_used) VALUES (:f, :d, :d, :d, :v, :s)"),
                             {"f": ids[name], "d": d, "v": 7.0 if name == "gdelt_total_events" else 0.0, "s": src})
            conn.execute(text("INSERT INTO gdelt_daily_summary (summary_date, total_events) VALUES (:d, 7)"),
                         {"d": d})
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) VALUES (:f, :d, :d, :d, 76725, :s)"),
                     {"f": art, "d": D1, "s": old})
        # a loader point (2026-03-20, dated D+1) and its summary row: outside the legacy set
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) VALUES (:f, '2026-03-20', '2026-03-21', '2026-03-21', 5, :s)"),
                     {"f": ids["gdelt_total_events"], "s": src})
        conn.execute(text("INSERT INTO gdelt_daily_summary (summary_date, total_events, day_basis) "
                          "VALUES ('2026-03-20', 5, 'dateadded_v2')"))
    return ids


def fake_history() -> Fake:
    fake = Fake()
    fake.v1(D1, [v1_row(1, D1, a1="USA"), v1_row(2, D1, a1="IRN", quad=4)], at(D2, 7))
    fake.v1(D2, [v1_row(3, D2)], at(D3, 7))
    # D3: no file at GDELT and no 2.0 files -> left out
    return fake


def rows(eng, sql, **params):
    with eng.connect() as conn:
        return [tuple(r) for r in conn.execute(text(sql), params)]


def test_plan_reads_only_the_same_day_rows_and_writes_nothing(engine):
    seed_legacy(engine)
    before = rows(engine, "SELECT count(*) FROM resolved_series")
    p = rb.plan(engine, archive=None, first=D1, last=D3, fetch=fake_history())
    assert p["legacy"]["rows"] == 7 and p["legacy"]["summary_rows"] == 3
    assert p["legacy"]["by_feature"] == {"gdelt_article_count": 1, "gdelt_events_us": 3, "gdelt_total_events": 3}
    r = p["report"]
    assert r["days_ok"] == 2 and r["days_left_out"] == {D3.isoformat(): "missing"}
    assert r["points_to_write"] == 24 and r["release_lag_days"]["counts"] == {"1": 2}
    assert rows(engine, "SELECT count(*) FROM resolved_series") == before


def test_apply_snapshots_copies_deletes_and_rebuilds_dated_after_the_day(engine, tmp_path):
    ids = seed_legacy(engine)
    p = rb.plan(engine, archive=None, first=D1, last=D3, fetch=fake_history())
    out = rb.apply(engine, p, run_tag="t-1", snapshot_dir=tmp_path)

    assert (out["legacy_rows_copied"], out["legacy_rows_deleted"]) == (7, 7)
    assert (out["legacy_summary_copied"], out["summary_rows_written"], out["points_written"]) == (3, 2, 24)
    with gzip.open(out["snapshot"]["resolved_series"]["path"], "rt") as fh:
        assert len(list(csv.DictReader(fh))) == 7
    assert rows(engine, f"SELECT count(*), min(run_tag) FROM {rb.RS_LEGACY_TABLE}") == [(7, "t-1")]
    assert rows(engine, f"SELECT count(*) FROM {rb.SUMMARY_LEGACY_TABLE}") == [(3,)]

    got = rows(engine, "SELECT obs_date, release_date, vintage_date, value FROM resolved_series "
                       "WHERE feature_id = :f ORDER BY obs_date", f=ids["gdelt_events_ir"])
    assert got == [(D1, D2, D2, 1.0), (D2, D3, D3, 0.0)]          # the country zeros are real counts now
    assert rows(engine, "SELECT count(*) FROM resolved_series WHERE release_date <= obs_date") == [(0,)]
    assert rows(engine, "SELECT count(*) FROM resolved_series r JOIN feature_registry f ON f.id = r.feature_id "
                        "WHERE f.name = 'gdelt_article_count'") == [(0,)]
    # the loader's point and summary row are untouched; D3 has no summary row now
    assert rows(engine, "SELECT value FROM resolved_series WHERE obs_date = '2026-03-20'") == [(5.0,)]
    assert rows(engine, "SELECT summary_date, day_basis, files_loaded FROM gdelt_daily_summary ORDER BY 1") == [
        (D1, "dateadded_v1", 1), (D2, "dateadded_v1", 1), (date(2026, 3, 20), "dateadded_v2", None)]


def test_apply_refuses_when_the_legacy_set_changed_or_it_already_ran(engine, tmp_path):
    ids = seed_legacy(engine)
    p = rb.plan(engine, archive=None, first=D1, last=D3, fetch=fake_history())
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                          "source_priority_used) SELECT :f, '2025-06-13', '2025-06-13', '2025-06-13', 1, "
                          "source_priority_used FROM resolved_series LIMIT 1"), {"f": ids["gdelt_avg_tone"]})
    with pytest.raises(rb.RebuildRefused, match="legacy set changed"):
        rb.apply(engine, p, run_tag="t", snapshot_dir=tmp_path / "a")
    assert not (tmp_path / "a").exists()

    p = rb.plan(engine, archive=None, first=D1, last=D3, fetch=fake_history())
    rb.apply(engine, p, run_tag="t", snapshot_dir=tmp_path / "b")
    with pytest.raises(rb.RebuildRefused):
        rb.apply(engine, p, run_tag="t", snapshot_dir=tmp_path / "c")


def test_a_failure_inside_the_transaction_leaves_the_live_tables_as_they_were(engine, tmp_path):
    seed_legacy(engine)
    p = rb.plan(engine, archive=None, first=D1, last=D3, fetch=fake_history())
    p["days"][0]["release_date"] = p["days"][0]["day"]          # a same-day point must never be written
    before = rows(engine, "SELECT id, release_date FROM resolved_series ORDER BY id")
    with pytest.raises(rb.RebuildRefused, match="not after the observed day"):
        rb.apply(engine, p, run_tag="t", snapshot_dir=tmp_path)
    assert rows(engine, "SELECT id, release_date FROM resolved_series ORDER BY id") == before
    assert rows(engine, "SELECT to_regclass(:t)", t=rb.RS_LEGACY_TABLE) == [(None,)]


def test_cli_requires_a_run_tag_and_a_snapshot_dir_for_apply(capsys):
    with pytest.raises(SystemExit):
        rb.main(["apply", "--plan", "p.json"])
    assert rb.main(["plan", "--out", "x.json", "--last", "2026-03-20"], engine=object()) == 2
