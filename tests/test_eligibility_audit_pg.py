"""normalization/eligibility_audit.py against real PostgreSQL (#1301, Codex HOLD on #1340).

Proves the two reports keep their semantics apart:

* the point-in-time health half reads ``resolved_series`` with store/pit.py's
  canonical as-of rule: obs, release and vintage dates fenced to ``as_of``,
  retractions by the end of ``as_of``'s UTC day removed before the latest-vintage
  pick. A revision or late row with a vintage after ``as_of`` does not change the
  as-of result; a retraction on or before ``as_of`` hides the row (a retracted
  revision falls back to the earlier vintage) and one after ``as_of`` does not;
  on resolver-shaped rows (``release_date = vintage_date``) the cells equal
  ``PITStore.get_pit(..., "LATEST_AS_OF")`` exactly;
* future-dated rows show up only in the current-store contamination report and
  never move the health decision;
* NaN, infinite and overflow-sized values fail the variance check without
  aborting the all-feature scan.

Runs in a scratch schema on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``)
and skips when no PostgreSQL is reachable; CI runs it in its own step and fails on
any skip. The retractions table is created without the production append-only
trigger so a fixture can hold retractions dated in the past.
"""

from __future__ import annotations

import math
import statistics
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from normalization import eligibility_audit as EA
from store.pit import PITStore

_DDL = (
    """
    CREATE TABLE feature_registry (
        id             SERIAL PRIMARY KEY,
        name           TEXT NOT NULL UNIQUE,
        family         TEXT NOT NULL DEFAULT 'vol',
        model_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        deprecated_at  TIMESTAMPTZ
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
        UNIQUE (feature_id, obs_date, vintage_date),
        FOREIGN KEY (feature_id, obs_date, vintage_date)
            REFERENCES resolved_series (feature_id, obs_date, vintage_date)
    )
    """,
)

AS_OF = date(2026, 11, 2)
HISTORY_DAYS = 300  # > 252, the daily minimum


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"eligibility_audit_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _features(engine: Engine, names: list[str], *, eligible: frozenset[str] = frozenset()) -> dict[str, int]:
    with engine.begin() as conn:
        return {
            n: conn.execute(
                text("INSERT INTO feature_registry (name, model_eligible) VALUES (:n, :e) RETURNING id"),
                {"n": n, "e": n in eligible},
            ).scalar_one()
            for n in names
        }


def _row(fid: int, obs: date, value: float, *, release: date | None = None,
         vintage: date | None = None) -> dict:
    vintage = vintage or obs
    return {"f": fid, "o": obs, "r": release or vintage, "v": vintage, "val": value}


def _insert(engine: Engine, rows: list[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                "VALUES (:f, :o, :r, :v, :val)"
            ),
            rows,
        )


def _retract(engine: Engine, fid: int, obs: date, vintage: date, at: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, retracted_at, reason, run_tag) "
                "VALUES (:f, :o, :v, :at, 'test', 'test')"
            ),
            {"f": fid, "o": obs, "v": vintage, "at": at},
        )


def _history(fid: int, *, end: date = AS_OF - timedelta(days=1), days: int = HISTORY_DAYS,
             offset: float = 0.0) -> list[dict]:
    """Same-day-known daily history ending at ``end`` (release = vintage = obs)."""
    return [
        _row(fid, end - timedelta(days=k), 10.0 + offset + (k % 17) * 0.5)
        for k in range(days)
    ]


def _health(engine: Engine, as_of: date) -> dict[str, EA.FeatureHealthStats]:
    return {s.name: s for s in EA.read_feature_health(engine, as_of=as_of)}


def _assert_same_health(got: EA.FeatureHealthStats, want: EA.FeatureHealthStats) -> None:
    assert (got.n_obs, got.last_obs, got.n_distinct, got.n_non_finite) == (
        want.n_obs, want.last_obs, want.n_distinct, want.n_non_finite)
    assert got.value_std == pytest.approx(want.value_std, rel=1e-12)


def _assert_cells(stats: EA.FeatureHealthStats, cells: dict[date, float]) -> None:
    """The as-of statistics are exactly those of ``cells`` (obs_date -> value)."""
    values = list(cells.values())
    assert stats.n_obs == len(values)
    assert stats.last_obs == max(cells)
    assert stats.n_distinct == len(set(values))
    if len(values) > 1:
        assert stats.value_std == pytest.approx(statistics.stdev(values), rel=1e-12)
    else:
        assert stats.value_std is None


def _pit_store_cells(engine: Engine, fid: int, as_of: date) -> dict[date, float]:
    df = PITStore(engine).get_pit([fid], as_of, "LATEST_AS_OF")
    return {r.obs_date: float(r.value) for r in df.itertuples()}


# ── point-in-time health ─────────────────────────────────────────────────


def test_revision_with_vintage_after_as_of_does_not_change_the_as_of_result(scratch):
    ids = _features(scratch, ["vix_spot"])
    fid = ids["vix_spot"]
    _insert(scratch, _history(fid))
    before = _health(scratch, AS_OF)["vix_spot"]
    report_before = EA.audit_feature_eligibility(scratch, as_of=AS_OF)
    assert report_before["to_enable"] == [{"id": fid, "name": "vix_spot"}]

    _insert(scratch, [
        # a revision resolved after as_of (release = vintage, the resolver's shape)
        _row(fid, AS_OF - timedelta(days=5), 1e6, vintage=AS_OF + timedelta(days=3)),
        # a revision that keeps a release date <= as_of but was written after as_of
        _row(fid, AS_OF - timedelta(days=6), 2e6, release=AS_OF - timedelta(days=6),
             vintage=AS_OF + timedelta(days=1)),
        # an observation dated as_of that only arrived the next day
        _row(fid, AS_OF, 3e6, vintage=AS_OF + timedelta(days=1)),
    ])

    _assert_same_health(_health(scratch, AS_OF)["vix_spot"], before)
    assert EA.audit_feature_eligibility(scratch, as_of=AS_OF) == report_before

    # The rows are real: once their vintages are known they count.
    later = _health(scratch, AS_OF + timedelta(days=3))["vix_spot"]
    assert later.n_obs == before.n_obs + 1 and later.last_obs == AS_OF
    assert later.value_std > 1e5 > before.value_std


def test_retraction_on_or_before_as_of_hides_the_row_and_after_does_not(scratch):
    fid = _features(scratch, ["rev_feat"])["rev_feat"]
    d1, d2, d3, d4 = (AS_OF - timedelta(days=n) for n in (10, 9, 8, 7))
    v2 = AS_OF - timedelta(days=2)
    _insert(scratch, [
        _row(fid, d1, 1.0), _row(fid, d2, 2.0), _row(fid, d3, 3.0), _row(fid, d4, 4.0),
        _row(fid, d3, 100.0, vintage=v2),  # a revision known from AS_OF - 2
    ])
    utc = timezone.utc
    # On the as_of day (last second): hides the revision at as_of; d3 falls back to 3.0.
    _retract(scratch, fid, d3, v2, datetime.combine(AS_OF, datetime.min.time(), utc) + timedelta(seconds=86399))
    # Before as_of: d4's only vintage is hidden, so the cell has no value.
    _retract(scratch, fid, d4, d4, datetime.combine(AS_OF - timedelta(days=3), datetime.min.time(), utc)
             + timedelta(hours=12))
    # The first instant after as_of's UTC day: does not hide d1 at as_of.
    _retract(scratch, fid, d1, d1, datetime.combine(AS_OF + timedelta(days=1), datetime.min.time(), utc))

    expected = {
        AS_OF: {d1: 1.0, d2: 2.0, d3: 3.0},
        AS_OF - timedelta(days=1): {d1: 1.0, d2: 2.0, d3: 100.0},  # before the revision's retraction day
        AS_OF - timedelta(days=4): {d1: 1.0, d2: 2.0, d3: 3.0, d4: 4.0},  # revision not yet known
        AS_OF + timedelta(days=1): {d2: 2.0, d3: 3.0},  # d1's retraction now applies
    }
    for as_of, cells in expected.items():
        _assert_cells(_health(scratch, as_of)["rev_feat"], cells)
        # Resolver-shaped rows: the same cells PITStore.get_pit serves.
        assert _pit_store_cells(scratch, fid, as_of) == cells, as_of


def test_as_of_cells_equal_pit_store_on_resolver_shaped_rows(scratch):
    ids = _features(scratch, ["a_full", "b_full", "c_full"])
    rows: list[dict] = []
    for j, fid in enumerate(ids.values()):
        for k in range(40):
            obs = AS_OF - timedelta(days=60 - k)
            rows.append(_row(fid, obs, k + j * 0.1, vintage=obs + timedelta(days=1)))
            if k % 3 == 0:
                rows.append(_row(fid, obs, k + 0.25 + j, vintage=obs + timedelta(days=4)))
            if k % 7 == 0:
                rows.append(_row(fid, obs, k + 0.5 + j, vintage=obs + timedelta(days=30)))
    _insert(scratch, rows)
    a = ids["a_full"]
    _retract(scratch, a, AS_OF - timedelta(days=60), AS_OF - timedelta(days=56),
             datetime(2026, 10, 1, 15, tzinfo=timezone.utc))
    _retract(scratch, a, AS_OF - timedelta(days=53), AS_OF - timedelta(days=52),
             datetime(2026, 10, 20, tzinfo=timezone.utc))

    for offset in range(-40, 5, 3):
        as_of = AS_OF + timedelta(days=offset)
        health = _health(scratch, as_of)
        for name, fid in ids.items():
            cells = _pit_store_cells(scratch, fid, as_of)
            if cells:
                _assert_cells(health[name], cells)
            else:
                assert health[name].n_obs == 0 and health[name].last_obs is None


# ── contamination is a separate report ───────────────────────────────────


def test_future_dated_rows_show_up_only_in_the_contamination_section(scratch):
    ids = _features(scratch, ["weather_tmax_fcst", "clean_feat"])
    w, c = ids["weather_tmax_fcst"], ids["clean_feat"]
    _insert(scratch, _history(w) + _history(c, offset=3.0))
    health_before = _health(scratch, AS_OF)
    report_before = EA.audit_feature_eligibility(scratch, as_of=AS_OF)
    assert EA.audit_future_dated_rows(scratch, as_of=AS_OF)["features"] == []

    stored = AS_OF - timedelta(days=1)
    future = [_row(w, AS_OF + timedelta(days=k), 50.0 + k, vintage=stored) for k in range(1, 6)]
    future += [
        _row(w, date(2100, 12, 31), 7.0, vintage=stored),                       # far-future placeholder
        _row(w, AS_OF + timedelta(days=10), 8.0, vintage=AS_OF + timedelta(days=4)),  # written after as_of
        # a forecast for a past date, written 17 days before its observation date
        _row(w, AS_OF - timedelta(days=3), 999.0, vintage=AS_OF - timedelta(days=20)),
    ]
    _insert(scratch, future)
    _retract(scratch, w, AS_OF + timedelta(days=1), stored, datetime(2026, 11, 4, tzinfo=timezone.utc))

    # Health and the decision are untouched by every one of those rows.
    health_after = _health(scratch, AS_OF)
    for name in ids:
        _assert_same_health(health_after[name], health_before[name])
    assert EA.audit_feature_eligibility(scratch, as_of=AS_OF) == report_before
    assert {"id": w, "name": "weather_tmax_fcst"} in report_before["to_enable"]

    contamination = EA.audit_future_dated_rows(scratch, as_of=AS_OF)
    assert contamination["section"] == "current_store_contamination"
    assert contamination["applied"] is False
    assert contamination["features"] == [{
        "id": w,
        "name": "weather_tmax_fcst",
        "current_eligible": False,
        "rows_after_as_of": 7,
        "obs_dates_after_as_of": 7,
        "after_as_of_stored_by_as_of": 6,
        "observed_after_vintage": 8,
        "retracted_rows": 1,
        "first_obs_after_as_of": (AS_OF + timedelta(days=1)).isoformat(),
        "max_obs_date": "2100-12-31",
    }]


# ── variance and refusal ─────────────────────────────────────────────────


def test_non_finite_values_fail_variance_without_aborting_the_scan(scratch):
    bad_feeds = ("nan_feat", "inf_feat", "huge_feat")
    ids = _features(scratch, ["ok_feat", *bad_feeds], eligible=frozenset(bad_feeds))
    rows: list[dict] = []
    for name, bad in (("ok_feat", None), ("nan_feat", math.nan), ("inf_feat", math.inf), ("huge_feat", 1e200)):
        hist = _history(ids[name])
        if bad is not None:
            hist[10]["val"] = bad
        rows += hist
    _insert(scratch, rows)

    health = _health(scratch, AS_OF)
    assert health["ok_feat"].n_non_finite == 0 and math.isfinite(health["ok_feat"].value_std)
    for name in bad_feeds:
        assert health[name].n_non_finite == 1, name
        assert health[name].n_obs == HISTORY_DAYS
    report = EA.audit_feature_eligibility(scratch, as_of=AS_OF)  # one STDDEV over 1e200 would raise
    assert report["to_enable"] == [{"id": ids["ok_feat"], "name": "ok_feat"}]
    disabled = {d["name"]: d["reasons"] for d in report["to_disable"]}
    assert set(disabled) == set(bad_feeds)
    for name, reasons in disabled.items():
        assert len(reasons) == 1 and reasons[0].startswith("non_finite_variance"), (name, reasons)


def test_the_audit_refuses_without_the_retractions_table(scratch):
    _features(scratch, ["vix_spot"])
    with scratch.begin() as conn:
        conn.execute(text("DROP TABLE resolved_series_retractions"))
    for fn in (EA.audit_feature_eligibility, EA.audit_future_dated_rows):
        with pytest.raises(RuntimeError, match="retraction"):
            fn(scratch, as_of=AS_OF)
