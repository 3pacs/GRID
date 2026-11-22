"""Every ``store/pit.py`` read fences ``vintage_date <= as_of``, not only ``release_date``.

Knowable-at for a ``resolved_series`` row is the later of ``release_date`` and
``vintage_date``. The resolver stamps both with the pull date, but nothing in
the schema enforces that: the TwelveData bridge
(``scripts/backfill_resolved_series_from_ticker_metrics.sql``, E1-V7c) writes
``release_date = obs_date`` and ``vintage_date = ticker_metrics_daily.as_of``
(the fetch time). Before this fence ``get_pit`` and ``PITWindowCache`` served
such a row to every ``as_of`` in ``[obs_date, fetch date)`` -- a value GRID did
not yet hold -- and LATEST_AS_OF picked it over an honest earlier vintage.
``get_source_snapshot`` already fenced both dates.

Runs in a scratch schema on the shared ``pg_engine`` (``GRID_TEST_DB_URL``);
skips when no PostgreSQL is reachable. The ``assert_no_lookahead`` checks at
the bottom need no database.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from store.pit import PITStore, PITWindowCache

_DDL = (
    """
    CREATE TABLE feature_registry (
        id    SERIAL PRIMARY KEY,
        name  TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE resolved_series (
        id                    BIGSERIAL PRIMARY KEY,
        feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date              DATE NOT NULL,
        release_date          DATE NOT NULL,
        vintage_date          DATE NOT NULL,
        value                 DOUBLE PRECISION NOT NULL,
        source_priority_used  INTEGER NOT NULL DEFAULT 1
    )
    """,
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """
    CREATE TABLE resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason        TEXT NOT NULL,
        run_tag       TEXT NOT NULL,
        UNIQUE (feature_id, obs_date, vintage_date)
    )
    """,
)

# Cell A (2026-04-01): only a backdated-release row, fetched 2026-05-17
#   (the TwelveData bridge shape: release = obs, vintage = fetch date).
# Cell B (2026-03-30): an honest vintage on 03-31, then a backdated-release
#   re-fetch on 05-17 with another value.
# Cell C (2026-03-27): two honest vintages (release = vintage), the baseline
#   both policies must keep answering exactly as before.
A, B, C = date(2026, 4, 1), date(2026, 3, 30), date(2026, 3, 27)
FETCH = date(2026, 5, 17)
_ROWS = (
    (A, A, FETCH, 200.0),
    (B, date(2026, 3, 31), date(2026, 3, 31), 100.0),
    (B, B, FETCH, 201.0),
    (C, date(2026, 3, 28), date(2026, 3, 28), 50.0),
    (C, date(2026, 4, 2), date(2026, 4, 2), 51.0),
)
AS_OF = date(2026, 4, 15)  # after every release_date, before the 05-17 fetch


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"pit_vfence_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            fid = conn.execute(
                text("INSERT INTO feature_registry (name) VALUES ('aapl_full') RETURNING id")
            ).scalar()
            for od, rd, vd, val in _ROWS:
                conn.execute(
                    text(
                        "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                        "VALUES (:f, :od, :rd, :vd, :v)"
                    ),
                    {"f": fid, "od": od, "rd": rd, "vd": vd, "v": val},
                )
        yield engine, fid
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _cells(df: pd.DataFrame) -> dict[date, tuple[float, date]]:
    return {r.obs_date: (r.value, r.vintage_date) for r in df.itertuples()}


def _readers(engine: Engine, fid: int):
    """The SQL read and the in-memory window, answering the same question."""
    window = PITWindowCache.load(engine, [fid], date(2026, 3, 1), date(2026, 5, 31), date(2026, 5, 31),
                                 allow_db_fallback=False)

    def window_read(as_of: date, policy: str) -> pd.DataFrame:
        return window.get_pit([fid], as_of, policy, start_date=date(2026, 3, 1))

    def sql_read(as_of: date, policy: str) -> pd.DataFrame:
        return PITStore(engine).get_pit([fid], as_of, policy, start_date=date(2026, 3, 1))

    return {"sql": sql_read, "window": window_read}


@pytest.mark.parametrize("reader", ["sql", "window"])
def test_latest_as_of_never_serves_a_vintage_written_after_as_of(scratch, reader):
    engine, fid = scratch
    read = _readers(engine, fid)[reader]
    cells = _cells(read(AS_OF, "LATEST_AS_OF"))
    assert A not in cells  # GRID first held it on 05-17
    assert cells[B] == (100.0, date(2026, 3, 31))  # the honest vintage, not the 05-17 re-fetch
    assert cells[C] == (51.0, date(2026, 4, 2))
    assert all(v <= AS_OF for _, v in cells.values())


@pytest.mark.parametrize("reader", ["sql", "window"])
def test_first_release_never_serves_a_vintage_written_after_as_of(scratch, reader):
    engine, fid = scratch
    read = _readers(engine, fid)[reader]
    cells = _cells(read(AS_OF, "FIRST_RELEASE"))
    assert A not in cells
    assert cells[B] == (100.0, date(2026, 3, 31))
    assert cells[C] == (50.0, date(2026, 3, 28))


@pytest.mark.parametrize("reader", ["sql", "window"])
def test_backdated_release_row_is_served_from_its_vintage_on(scratch, reader):
    engine, fid = scratch
    read = _readers(engine, fid)[reader]
    before = _cells(read(FETCH.replace(day=16), "LATEST_AS_OF"))
    on = _cells(read(FETCH, "LATEST_AS_OF"))
    assert A not in before
    assert on[A] == (200.0, FETCH)
    assert on[B] == (201.0, FETCH)  # the later vintage wins once it is held
    first = _cells(read(FETCH, "FIRST_RELEASE"))
    assert first[B] == (100.0, date(2026, 3, 31))
    assert first[A] == (200.0, FETCH)


def test_window_snapshot_matches_sql_on_backdated_release_rows(scratch, tmp_path):
    engine, fid = scratch
    window = PITWindowCache.load(engine, [fid], date(2026, 3, 1), date(2026, 5, 31), AS_OF,
                                 allow_db_fallback=False)
    # The bounded window read itself leaves out vintages after its last as_of.
    assert FETCH not in set(window._rows["vintage_date"])
    loaded = PITWindowCache.read_snapshot(window.write_snapshot(tmp_path))
    for policy in ("LATEST_AS_OF", "FIRST_RELEASE"):
        for as_of in (date(2026, 3, 31), date(2026, 4, 1), date(2026, 4, 2), AS_OF):
            sql = PITStore(engine).get_pit([fid], as_of, policy, start_date=date(2026, 3, 1))
            mem = loaded.get_pit([fid], as_of, policy, start_date=date(2026, 3, 1))
            pd.testing.assert_frame_equal(sql.reset_index(drop=True), mem.reset_index(drop=True),
                                          check_dtype=False)


def test_get_feature_matrix_inherits_the_fence(scratch):
    engine, fid = scratch
    matrix = PITStore(engine).get_feature_matrix([fid], date(2026, 3, 1), date(2026, 5, 31), AS_OF,
                                                 vintage_policy="LATEST_AS_OF")
    assert pd.Timestamp(A) not in matrix.index
    assert matrix.loc[pd.Timestamp(B), fid] == 100.0


# -- assert_no_lookahead (no database) --------------------------------------


def _frame(release: date, vintage: date) -> pd.DataFrame:
    return pd.DataFrame([{"feature_id": 1, "obs_date": date(2026, 4, 1), "value": 1.0,
                          "release_date": release, "vintage_date": vintage}])


def test_assert_no_lookahead_rejects_a_vintage_after_as_of():
    df = _frame(release=date(2026, 4, 1), vintage=FETCH)
    with pytest.raises(ValueError, match="LOOKAHEAD VIOLATION"):
        PITStore(None).assert_no_lookahead(df, AS_OF)  # type: ignore[arg-type]
    assert df.empty  # tainted frame is emptied, as for a late release_date


def test_assert_no_lookahead_accepts_known_rows_and_release_only_frames():
    PITStore(None).assert_no_lookahead(_frame(date(2026, 4, 1), date(2026, 4, 2)), AS_OF)  # type: ignore[arg-type]
    release_only = _frame(date(2026, 4, 1), FETCH).drop(columns=["vintage_date"])
    PITStore(None).assert_no_lookahead(release_only, AS_OF)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="LOOKAHEAD VIOLATION"):
        PITStore(None).assert_no_lookahead(_frame(FETCH, FETCH), AS_OF)  # type: ignore[arg-type]
