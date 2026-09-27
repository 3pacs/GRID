"""PostgreSQL contract for the SQL added with scripts/run_analytics_snapshots.py.

Runs against a disposable schema on GRID_TEST_DB_URL (skips without it; the
CI step fails on any skip). Covers every statement this change added or
modified:

* the readiness gate (G1-G5) in ``check_readiness``;
* ``ClusterDiscovery._get_eligible_feature_ids`` with and without a
  sector restriction (``CAST(:restrict AS INTEGER[])``);
* ``latest_snapshot_result`` (global vs per-sector rows);
* ``resolve_cross_asset_features`` (correlation-matrix fail-closed ids);
* ``resolve_sector_feature_ids`` and ``summarize_options_scans``;
* one end-to-end gated ``run`` that writes a provenance-tagged snapshot.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, text

from scripts import run_analytics_snapshots as job

AS_OF = date(2026, 9, 27)
RUN_TAG = "reresolve_20260927"


@pytest.fixture
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the analytics snapshots PostgreSQL contract")
    admin = create_engine(url)
    schema = "analytics_snap_" + uuid.uuid4().hex[:12]
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE feature_registry ("
            " id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE,"
            " family TEXT NOT NULL DEFAULT 'equity',"
            " model_eligible BOOLEAN NOT NULL DEFAULT FALSE,"
            " deprecated_at TIMESTAMPTZ)"
        ))
        conn.execute(text(
            "CREATE TABLE resolved_series ("
            " id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),"
            " obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,"
            " value DOUBLE PRECISION NOT NULL,"
            " UNIQUE (feature_id, obs_date, vintage_date))"
        ))
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _feature(engine, name, eligible=True, deprecated=False) -> int:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO feature_registry (name, model_eligible, deprecated_at) "
                "VALUES (:n, :e, CASE WHEN :d THEN NOW() END) RETURNING id"
            ),
            {"n": name, "e": eligible, "d": deprecated},
        ).scalar()


def _series(engine, fid, last_obs: date, n: int = 5) -> None:
    with engine.begin() as conn:
        for i in range(n):
            d = last_obs - timedelta(days=i)
            conn.execute(
                text(
                    "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                    "VALUES (:f, :d, :d, :d, :v)"
                ),
                {"f": fid, "d": d, "v": 100.0 + i},
            )


def _retractions_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE resolved_series_retractions ("
            " id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,"
            " vintage_date DATE NOT NULL, retracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
            " reason TEXT NOT NULL, run_tag TEXT NOT NULL)"
        ))


def _retract(engine, fid, obs: date, age: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, retracted_at, reason, run_tag) "
                "VALUES (:f, :d, :d, NOW() - CAST(:age AS INTERVAL), 'no_clean_raw', :tag)"
            ),
            {"f": fid, "d": obs, "age": age, "tag": RUN_TAG},
        )


def _checks(gate):
    return {c["check"]: c["ok"] for c in gate["checks"]}


def _flag(tmp_path, content=RUN_TAG):
    p = tmp_path / "analytics.ready"
    p.write_text(content + "\n", encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# Readiness gate
# ---------------------------------------------------------------------------

def test_gate_fails_closed_without_retractions_table(pg, tmp_path):
    spy = _feature(pg, "spy_full")
    _series(pg, spy, AS_OF - timedelta(days=1))
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG, flag_path=_flag(tmp_path),
                               settle_minutes=60, max_resolver_lag_days=4)
    checks = _checks(gate)
    assert gate["ok"] is False
    assert checks["G1_retractions_table"] is False
    assert checks["G2_run_tag_rows"] is False
    # G5 is still evaluated and reported (with PR #683's PIT reader it also
    # fails here, because that reader requires the retractions table).
    assert "G5_resolver_fresh" in checks


def test_gate_requires_settled_tagged_rows_flag_and_fresh_resolver(pg, tmp_path):
    spy = _feature(pg, "spy_full")
    _series(pg, spy, AS_OF - timedelta(days=1))
    _retractions_table(pg)

    # Table present but no rows for the tag.
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG, flag_path=_flag(tmp_path),
                               settle_minutes=60, max_resolver_lag_days=4)
    assert _checks(gate)["G1_retractions_table"] is True
    assert _checks(gate)["G2_run_tag_rows"] is False
    assert gate["ok"] is False

    # Rows present but the newest is 5 minutes old: insert still settling.
    _retract(pg, spy, AS_OF - timedelta(days=1), "5 minutes")
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG, flag_path=_flag(tmp_path),
                               settle_minutes=60, max_resolver_lag_days=4)
    assert _checks(gate)["G2_run_tag_rows"] is True
    assert _checks(gate)["G3_settled"] is False
    assert gate["ok"] is False

    # Settled (settle window shorter than the row's age) -> all green.
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG, flag_path=_flag(tmp_path),
                               settle_minutes=1, max_resolver_lag_days=4)
    assert gate["ok"] is True, gate

    # Wrong flag content or missing flag -> blocked.
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG, flag_path=_flag(tmp_path, "other"),
                               settle_minutes=1, max_resolver_lag_days=4)
    assert _checks(gate)["G4_ready_flag"] is False and gate["ok"] is False
    gate = job.check_readiness(pg, AS_OF, run_tag=RUN_TAG,
                               flag_path=str(tmp_path / "absent.flag"),
                               settle_minutes=1, max_resolver_lag_days=4)
    assert _checks(gate)["G4_ready_flag"] is False and gate["ok"] is False

    # Resolver refresh not landed -> blocked.
    gate = job.check_readiness(pg, AS_OF + timedelta(days=10), run_tag=RUN_TAG,
                               flag_path=_flag(tmp_path), settle_minutes=1,
                               max_resolver_lag_days=4)
    assert _checks(gate)["G5_resolver_fresh"] is False and gate["ok"] is False


# ---------------------------------------------------------------------------
# Clustering eligibility (sector restriction)
# ---------------------------------------------------------------------------

def test_cluster_eligibility_restrict_to_sector_ids(pg):
    from discovery.clustering import ClusterDiscovery
    from store.pit import PITStore

    recent = date.today() - timedelta(days=3)
    a = _feature(pg, "nvda_full")
    b = _feature(pg, "amd_full")
    c = _feature(pg, "xom_full")
    d = _feature(pg, "old_full")                      # no recent data
    e = _feature(pg, "dep_full", deprecated=True)     # deprecated
    f = _feature(pg, "inel_full", eligible=False)     # not eligible
    for fid in (a, b, c, e, f):
        _series(pg, fid, recent)
    _series(pg, d, date.today() - timedelta(days=500))

    cd = ClusterDiscovery(db_engine=pg, pit_store=PITStore(pg))
    assert cd._get_eligible_feature_ids() == sorted([a, b, c])
    assert cd._get_eligible_feature_ids(restrict_to=[a, b, d, e, f]) == sorted([a, b])
    assert cd._get_eligible_feature_ids(restrict_to=[]) == []


# ---------------------------------------------------------------------------
# Snapshot reads used by /discovery/results/* and /associations/clusters
# ---------------------------------------------------------------------------

def test_latest_snapshot_result_separates_global_and_sector_rows(pg, monkeypatch):
    from api.routers import discovery
    from store.snapshots import AnalyticalSnapshotStore

    store = AnalyticalSnapshotStore(db_engine=pg)
    store.save_snapshot("clustering", {"best_k": 3}, as_of_date=AS_OF - timedelta(days=1))
    store.save_snapshot("clustering", {"best_k": 4}, as_of_date=AS_OF)
    store.save_snapshot("clustering_sector", {"best_k": 2}, as_of_date=AS_OF,
                        subcategory="Technology")
    monkeypatch.setattr(discovery, "get_db_engine", lambda: pg)

    glob = discovery.latest_snapshot_result("clustering", None)
    assert glob["result"] == {"best_k": 4}
    assert glob["as_of_date"] == AS_OF.isoformat()
    tech = discovery.latest_snapshot_result("clustering_sector", "Technology")
    assert tech["result"] == {"best_k": 2}
    assert discovery.latest_snapshot_result("clustering_sector", "Energy") is None
    # A per-sector row is never served as the global answer.
    assert discovery.latest_snapshot_result("clustering_sector", None) is None

    listing = discovery.list_sector_clustering_results(_token="t")
    assert listing["sectors"] == [
        {"sector": "Technology", "as_of_date": AS_OF.isoformat(), "snapshots": 1}
    ]


# ---------------------------------------------------------------------------
# Correlation-matrix ids fail closed per feature
# ---------------------------------------------------------------------------

def test_resolve_cross_asset_features_fails_closed_per_feature(pg):
    from api.routers.discovery import CROSS_ASSET_FEATURES, resolve_cross_asset_features

    names = [n for n, _ in CROSS_ASSET_FEATURES]
    ok_ids = {_feature(pg, n) for n in names[:-3]}
    _feature(pg, names[-3], deprecated=True)
    _feature(pg, names[-2], eligible=False)
    # names[-1] is not in the registry at all.

    usable, id_to_name, excluded = resolve_cross_asset_features(pg)
    assert set(usable) == ok_ids
    assert set(id_to_name.values()) == set(names[:-3])
    assert {e["feature"]: e["reason"] for e in excluded} == {
        names[-3]: "deprecated",
        names[-2]: "not_model_eligible",
        names[-1]: "not_in_registry",
    }


def test_resolve_sector_feature_ids_matches_exact_names_only(pg):
    a = _feature(pg, "c_full")
    _feature(pg, "c_something_else")
    b = _feature(pg, "xlf_iv_atm")
    names = job.sector_candidate_names(
        {"Financials": {"etf": "XLF", "subsectors": {"banks": {"actors": [{"ticker": "C"}]}}}}
    )["Financials"]["names"]
    assert job.resolve_sector_feature_ids(pg, names) == sorted([a, b])
    assert job.resolve_sector_feature_ids(pg, []) == []


# ---------------------------------------------------------------------------
# Options scan summary
# ---------------------------------------------------------------------------

def _scans_table(engine):
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE options_mispricing_scans ("
            " id BIGSERIAL PRIMARY KEY, ticker TEXT NOT NULL, scan_date DATE NOT NULL,"
            " score DOUBLE PRECISION NOT NULL, payoff_multiple DOUBLE PRECISION NOT NULL,"
            " direction TEXT NOT NULL, thesis TEXT NOT NULL, signals JSONB,"
            " strikes DOUBLE PRECISION[], expiry DATE, spot_price DOUBLE PRECISION,"
            " iv_atm DOUBLE PRECISION, confidence TEXT NOT NULL,"
            " is_100x BOOLEAN NOT NULL DEFAULT FALSE,"
            " UNIQUE (ticker, scan_date, direction))"
        ))


def _scan(engine, ticker, sd, score, is100=False):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO options_mispricing_scans (ticker, scan_date, score, payoff_multiple,"
                " direction, thesis, confidence, is_100x, expiry, iv_atm)"
                " VALUES (:t, :sd, :s, 3.0, 'CALL', 'x', 'LOW', :h, :sd, NULL)"
            ),
            {"t": ticker, "sd": sd, "s": score, "h": is100},
        )


def test_summarize_options_scans_latest_and_stale(pg):
    assert job.summarize_options_scans(pg, AS_OF)["status"] == "skipped"  # table missing
    _scans_table(pg)
    assert job.summarize_options_scans(pg, AS_OF)["status"] == "skipped"  # no rows
    _scan(pg, "OLD", AS_OF - timedelta(days=3), 9.0)
    _scan(pg, "AAA", AS_OF - timedelta(days=1), 6.0)
    _scan(pg, "BBB", AS_OF - timedelta(days=1), 7.5, is100=True)
    _scan(pg, "FUT", AS_OF + timedelta(days=1), 9.9)  # after as_of: ignored

    out = job.summarize_options_scans(pg, AS_OF)
    assert out["status"] == "ok"
    assert out["scan_date"] == (AS_OF - timedelta(days=1)).isoformat()
    assert out["opportunities"] == 2 and out["100x"] == 1
    assert [r["ticker"] for r in out["top"]] == ["BBB", "AAA"]
    assert out["top"][0]["iv_atm"] is None

    stale = job.summarize_options_scans(pg, AS_OF + timedelta(days=10))
    assert stale["status"] == "skipped" and "stale" in stale["reason"]


# ---------------------------------------------------------------------------
# End to end: gated run writes a provenance-tagged snapshot
# ---------------------------------------------------------------------------

def test_gated_run_writes_provenance_tagged_snapshot(pg, tmp_path):
    spy = _feature(pg, "spy_full")
    _series(pg, spy, AS_OF - timedelta(days=1))
    _retractions_table(pg)
    _retract(pg, spy, AS_OF - timedelta(days=1), "2 hours")
    _scans_table(pg)
    _scan(pg, "AAA", AS_OF, 6.0)
    flag = _flag(tmp_path)

    def gate(engine, as_of):
        return job.check_readiness(engine, as_of, run_tag=RUN_TAG, flag_path=flag,
                                   settle_minutes=60, max_resolver_lag_days=4)

    summary = job.run(pg, AS_OF, steps=("options_scan",), gate_fn=gate)
    assert summary["status"] == "ok", summary
    snap_id = summary["steps"]["options_scan"]["snapshot_id"]
    with pg.connect() as conn:
        row = conn.execute(
            text("SELECT category, subcategory, as_of_date, payload FROM analytical_snapshots WHERE id = :i"),
            {"i": snap_id},
        ).fetchone()
    assert row[0] == "options_scan" and row[1] is None and row[2] == AS_OF
    prov = row[3]["provenance"]
    assert prov["job"] == "run_analytics_snapshots"
    assert prov["as_of_date"] == AS_OF.isoformat()
    assert prov["readiness_gate"] == {"ok": True, "run_tag": RUN_TAG}
    assert prov["source"] == "options_mispricing_scans"

    # Not ready -> nothing written.
    with pg.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM analytical_snapshots")).scalar()
    missing_flag = str(tmp_path / "nope")
    summary = job.run(
        pg, AS_OF, steps=("options_scan",),
        gate_fn=lambda e, a: job.check_readiness(e, a, run_tag=RUN_TAG, flag_path=missing_flag,
                                                 settle_minutes=60, max_resolver_lag_days=4),
    )
    assert summary["status"] == "not_ready" and summary["steps"] == {}
    with pg.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM analytical_snapshots")).scalar() == before
