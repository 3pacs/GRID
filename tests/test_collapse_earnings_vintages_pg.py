"""DB-gated tests for scripts/collapse_earnings_vintages.py.

The collapse must delete only exact repeats of the previous vintage of the
same (series_id, obs_date): the first vintage and every genuine revision
survive (so as-of reads are unchanged), other series/sources are untouched,
and a row referenced by astrogrid.price_close_receipt is never deleted.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.integration


@pytest.fixture
def engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL not set")
    root = create_engine(url)
    try:
        with root.connect() as c:
            c.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"GRID_TEST_DB_URL unreachable: {exc}")
    schema = f"collapse_{uuid.uuid4().hex[:10]}"
    ag = f"{schema}_ag"
    with root.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
    eng = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, "
            "source_id INT NOT NULL, obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL, "
            "value DOUBLE PRECISION, pull_status TEXT NOT NULL)"))
        # The script names astrogrid.price_close_receipt explicitly; give the
        # test a private stand-in schema and point the name at it.
        c.execute(text(f'CREATE SCHEMA "{ag}"'))
        c.execute(text(f'CREATE TABLE "{ag}".price_close_receipt (raw_series_id BIGINT REFERENCES raw_series(id))'))
    try:
        yield eng, ag
    finally:
        eng.dispose()
        with root.begin() as c:
            c.execute(text(f'DROP SCHEMA "{ag}" CASCADE'))
            c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        root.dispose()


def _insert(conn, sid, src, od, ts, value, status="SUCCESS") -> int:
    return conn.execute(text(
        "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
        "VALUES (:s, :src, :od, :ts, :v, :st) RETURNING id"),
        {"s": sid, "src": src, "od": od, "ts": ts, "v": value, "st": status}).scalar()


def _rows(conn, sid):
    return [tuple(r) for r in conn.execute(text(
        "SELECT value, pull_timestamp FROM raw_series WHERE series_id = :s ORDER BY pull_timestamp"),
        {"s": sid}).fetchall()]


def test_collapse_keeps_first_and_revisions_only(engine, monkeypatch):
    import scripts.collapse_earnings_vintages as cev

    eng, ag = engine
    for name in ("_COUNT_SQL", "_DELETE_SQL"):
        monkeypatch.setattr(cev, name, text(str(getattr(cev, name)).replace(
            "astrogrid.price_close_receipt", f'"{ag}".price_close_receipt')))
    monkeypatch.setattr(cev, "_price_receipt_table_exists", lambda conn: True)

    t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
    od = date(2025, 4, 29)
    with eng.begin() as c:
        # value run: 1.0 1.0 1.0 1.2 1.2 1.0  -> keep 1.0(first) 1.2 1.0
        ids = [_insert(c, "EARNINGS:AMD:actual", 351, od, t0 + timedelta(hours=h), v)
               for h, v in enumerate([1.0, 1.0, 1.0, 1.2, 1.2, 1.0])]
        _insert(c, "EARNINGS:AMD:actual", 351, od, t0 + timedelta(hours=9), 0.0, status="FAILED")
        for h in range(3):  # other source and other series prefix: untouched
            _insert(c, "EARNINGS:AMD:actual", 999, od, t0 + timedelta(hours=h), 5.0)
            _insert(c, "PRICE:AMD", 351, od, t0 + timedelta(hours=h), 5.0)
        # a referenced repeat is never deleted
        c.execute(text(f'INSERT INTO "{ag}".price_close_receipt VALUES (:i)'), {"i": ids[2]})

    dry = cev.collapse(eng, 351)
    assert (dry["rows_scanned"], dry["repeat_rows"], dry["deleted"]) == (6, 2, 0)
    with eng.connect() as c:
        assert len(_rows(c, "EARNINGS:AMD:actual")) == 6 + 1 + 3  # dry run deleted nothing

    applied = cev.collapse(eng, 351, apply=True)
    assert applied["deleted"] == 2 and not applied["errors"]
    with eng.connect() as c:
        kept = [v for v, ts in _rows(c, "EARNINGS:AMD:actual")
                if ts < t0 + timedelta(hours=6)]
        src351 = c.execute(text(
            "SELECT value FROM raw_series WHERE source_id = 351 AND series_id = 'EARNINGS:AMD:actual' "
            "AND pull_status = 'SUCCESS' ORDER BY pull_timestamp")).scalars().all()
        assert src351 == [1.0, 1.0, 1.2, 1.0]  # first, referenced repeat, revision, revision back
        assert c.execute(text("SELECT count(*) FROM raw_series WHERE source_id = 999")).scalar() == 3
        assert c.execute(text("SELECT count(*) FROM raw_series WHERE series_id = 'PRICE:AMD'")).scalar() == 3
        assert c.execute(text("SELECT count(*) FROM raw_series WHERE pull_status = 'FAILED'")).scalar() == 1
        assert kept  # sanity

    again = cev.collapse(eng, 351, apply=True)
    assert again["deleted"] == 0
