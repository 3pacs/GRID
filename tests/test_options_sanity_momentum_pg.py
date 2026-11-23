"""PostgreSQL contract: the CROSS_ASSET momentum read takes one vintage per date, the newest.

``trading/options_recommender.py::_sanity_cross_asset`` computes momentum
from the newest 6 stored sessions of ``{ticker}_close``. The read used to be
``JOIN feature_registry ... WHERE fr.name = :fname ORDER BY rs.obs_date DESC
LIMIT 6``. ``{t}_close`` features hold several vintages of one date (griddb
2026-11-23: cl_close has a same-day vintage and a next-day vintage on most
dates, with different values). That statement counted each vintage as a
session, so its 6 rows covered 3-4 dates, and which vintage of a date came
first or last was left to the query plan. The read now binds the feature id
and takes ``DISTINCT ON (obs_date) ... ORDER BY obs_date DESC, vintage_date
DESC``, as ``trading.signal_executor._get_latest_prices`` does.

The seed gives the two newest dates two vintages with different values,
inserted newest vintage first (so heap order is not vintage order). The
newer vintage of the newest date moves momentum from 0% to +3%, which flips
the PUT verdict under a GROWTH journal state, so a read that took the other
vintage fails here.

Skips without ``GRID_TEST_DB_URL`` (a local throwaway database); fails
instead when ``REQUIRE_BOUND_ID_READS_PG=1`` (CI, the bound-id reads step).
"""

from __future__ import annotations

import ast
import os
import pathlib
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_BOUND_ID_READS_PG"
REPO = pathlib.Path(__file__).resolve().parent.parent

D0 = date.today() - timedelta(days=1)


def _d(n: int) -> date:
    return D0 - timedelta(days=n)


# obs_date -> [(vintage_date, value), ...], in insertion order (newest vintage first).
SEED: dict[date, list[tuple[date, float]]] = {
    _d(0): [(_d(0) + timedelta(days=1), 103.0), (_d(0), 100.0)],
    _d(1): [(_d(1) + timedelta(days=1), 101.0), (_d(1), 101.5)],
    _d(2): [(_d(2), 102.0)],
    _d(3): [(_d(3), 99.0)],
    _d(4): [(_d(4), 98.0)],
    _d(5): [(_d(5), 100.0)],
    _d(6): [(_d(6), 50.0)],  # outside the 6 newest dates
    _d(7): [(_d(7), 50.0)],
}

# The statement before 2026-11-23, verbatim, with the dates selected.
LEGACY_DATED = """
    SELECT rs.value, rs.obs_date
    FROM resolved_series rs
    JOIN feature_registry fr ON rs.feature_id = fr.id
    WHERE fr.name = :fname
    ORDER BY rs.obs_date DESC
    LIMIT 6
"""


@pytest.fixture(scope="module")
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("options momentum contract requires a local disposable test database")

    schema = "opt_momentum_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE resolved_series (id SERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, "
            "obs_date DATE NOT NULL, release_date DATE, vintage_date DATE NOT NULL, "
            "value DOUBLE PRECISION, UNIQUE (feature_id, obs_date, vintage_date))"
        ))
        conn.execute(text("CREATE INDEX ON resolved_series (feature_id, obs_date DESC)"))
        conn.execute(text(
            "CREATE TABLE decision_journal (id SERIAL PRIMARY KEY, decision_timestamp TIMESTAMPTZ, "
            "inferred_state TEXT, grid_recommendation TEXT)"
        ))
        fid = conn.execute(text(
            "INSERT INTO feature_registry (name) VALUES ('cl_close') RETURNING id"
        )).scalar_one()
        conn.execute(text("INSERT INTO feature_registry (name) VALUES ('zzz_close')"))  # no rows
        for obs, vintages in SEED.items():
            for vintage, value in vintages:
                conn.execute(text(
                    "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                    "VALUES (:f, :o, :v, :v, :val)"
                ), {"f": fid, "o": obs, "v": vintage, "val": value})
        conn.execute(text(
            "INSERT INTO decision_journal (decision_timestamp, inferred_state, grid_recommendation) "
            "VALUES (:ts, 'GROWTH', 'HOLD')"
        ), {"ts": datetime.now(timezone.utc)})
    engine.fid = fid
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _shipped_momentum_sql() -> str:
    """The momentum statement as it ships in ``_sanity_cross_asset``."""
    tree = ast.parse((REPO / "trading/options_recommender.py").read_text(encoding="utf-8"))
    fn = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_sanity_cross_asset"]
    assert len(fn) == 1
    found = [n.value for n in ast.walk(fn[0])
             if isinstance(n, ast.Constant) and isinstance(n.value, str) and "resolved_series" in n.value]
    assert len(found) == 1, found
    return found[0]


def _cross_asset(pg, ticker: str, direction: str) -> dict[str, str]:
    from trading.options_recommender import OptionsRecommender

    rec = SimpleNamespace(ticker=ticker, direction=direction)
    return OptionsRecommender._sanity_cross_asset(None, rec, pg)


def test_momentum_read_is_newest_vintage_of_each_of_six_dates(pg):
    with pg.connect() as conn:
        rows = [tuple(r) for r in conn.execute(text(_shipped_momentum_sql()), {"fid": pg.fid}).fetchall()]
    assert rows == [
        (103.0, _d(0)),  # vintage d0+1, not the same-day 100.0
        (101.0, _d(1)),  # vintage d1+1 (the lower value): newest vintage, not max
        (102.0, _d(2)),
        (99.0, _d(3)),
        (98.0, _d(4)),
        (100.0, _d(5)),
    ]


def test_legacy_read_counted_vintages_as_sessions(pg):
    """Why the read changed: 6 rows of the old statement covered 4 dates."""
    with pg.connect() as conn:
        rows = conn.execute(text(LEGACY_DATED), {"fname": "cl_close"}).fetchall()
    assert len(rows) == 6
    assert sorted({r[1] for r in rows}, reverse=True) == [_d(0), _d(1), _d(2), _d(3)]


def test_cross_asset_momentum_uses_newest_vintage(pg):
    # (103.0 - 100.0) / 100.0 over 5 sessions. The same-day vintage of d0
    # (100.0) would read +0.00%; the old 4-date window read +1.01% or +4.04%.
    call = _cross_asset(pg, "CL", "CALL")
    assert call == {"status": "PASS", "reason": "Momentum (+3.00%) and regime aligned with CALL"}
    # GROWTH contradicts a PUT, and +3% is a sharp rally for one (> +2%):
    # FAIL. The same-day vintage would have passed it (momentum aligned).
    put = _cross_asset(pg, "cl", "PUT")
    assert put == {"status": "FAIL", "reason": "Momentum (+3.00%) and regime both contradict PUT"}


@pytest.mark.parametrize("ticker", ["ZZZ", "QQQ"])
def test_cross_asset_without_rows_keeps_zero_momentum(pg, ticker):
    """Registered with no rows (ZZZ) or not registered (QQQ): momentum reads 0, as before."""
    assert _cross_asset(pg, ticker, "PUT") == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=+0.00%, news_aligned=False",
    }
