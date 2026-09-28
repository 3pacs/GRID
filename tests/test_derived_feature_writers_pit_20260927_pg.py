"""Real-PostgreSQL proof for the derived-feature writers' PIT fix.

GRID-RERESOLVE-PLAN-20260927, "DERIVED FEATURES": the only two writers of
stored derived features -- ``scripts/compute_derived_features.py`` (writes
``resolved_series`` directly) and ``scripts/fill_missing_features.py::
compute_derived_features`` (writes ``raw_series`` ``COMPUTED:*`` rows) --
loaded their inputs across *every* vintage in ``resolved_series`` with no
vintage policy and no retraction filter, and the first one stamped new rows
``vintage_date = release_date = obs_date`` (backdated). Since PR #683
(``resolved_series_retractions``), running either job unchanged would
compute from arbitrary vintages -- including contaminated ones kept as
superseded history -- and, for the first writer, store the result as if it
had been known on the observation date itself.

This proves, against real Postgres, that both writers now:

* refuse to run at all when ``resolved_series_retractions`` does not exist;
* read inputs through ``store.pit.PITStore.get_pit`` with vintage_policy
  "LATEST_AS_OF" as of the computation time, so a retracted vintage is
  never used and the *remaining* (non-retracted) vintage is picked instead;
* (``compute_derived_features.py`` only) stamp new ``resolved_series`` rows
  with ``vintage_date = release_date =`` the computation day, never the
  observation date, and never overwrite an existing vintage.

Runs in a throwaway schema (random name, dropped afterwards) holding a
minimal ``feature_registry``, ``resolved_series`` (production's composite
unique key), ``raw_series`` and ``source_catalog``, plus
``resolved_series_retractions`` built exactly to PR #683's shape where a
test needs it installed.

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips
when no PostgreSQL is reachable (the CI step fails on a skip).
"""

from __future__ import annotations

from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from scripts import compute_derived_features as cdf
from scripts import fill_missing_features as fmf

_FEATURE_REGISTRY_DDL = """
CREATE TABLE feature_registry (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    family         TEXT,
    deprecated_at  TIMESTAMPTZ
)
"""

# schema.sql's resolved_series, trimmed to what these writers touch.
_RESOLVED_SERIES_DDL = """
CREATE TABLE resolved_series (
    id                    BIGSERIAL PRIMARY KEY,
    feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
    obs_date              DATE NOT NULL,
    release_date          DATE NOT NULL,
    vintage_date          DATE NOT NULL,
    value                 DOUBLE PRECISION NOT NULL,
    source_priority_used  INTEGER NOT NULL,
    conflict_flag         BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail       JSONB,
    resolution_version    INTEGER NOT NULL DEFAULT 1
)
"""
_RESOLVED_SERIES_KEY = (
    "CREATE UNIQUE INDEX uq_resolved_series_composite "
    "ON resolved_series (feature_id, obs_date, vintage_date)"
)

_SOURCE_CATALOG_DDL = """
CREATE TABLE source_catalog (
    id                 SERIAL PRIMARY KEY,
    name               TEXT NOT NULL UNIQUE,
    base_url           TEXT,
    cost_tier          TEXT,
    latency_class      TEXT,
    pit_available      BOOLEAN,
    revision_behavior  TEXT,
    trust_score        TEXT,
    priority_rank      INTEGER,
    active             BOOLEAN NOT NULL DEFAULT TRUE
)
"""

# schema.sql's raw_series, trimmed (no QUARANTINED check needed here).
_RAW_SERIES_DDL = """
CREATE TABLE raw_series (
    id              BIGSERIAL PRIMARY KEY,
    series_id       TEXT NOT NULL,
    source_id       INTEGER NOT NULL REFERENCES source_catalog(id),
    obs_date        DATE NOT NULL,
    pull_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    value           DOUBLE PRECISION NOT NULL,
    raw_payload     JSONB,
    pull_status     TEXT NOT NULL
)
"""

# PR #683's schema.sql addition, reproduced exactly (FK + append-only guard).
_RETRACTIONS_DDL = """
CREATE TABLE resolved_series_retractions (
    id            BIGSERIAL PRIMARY KEY,
    feature_id    INTEGER NOT NULL,
    obs_date      DATE NOT NULL,
    vintage_date  DATE NOT NULL,
    retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason        TEXT NOT NULL CHECK (btrim(reason) <> ''),
    run_tag       TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
    CONSTRAINT uq_resolved_series_retractions_key
        UNIQUE (feature_id, obs_date, vintage_date) INCLUDE (retracted_at),
    CONSTRAINT fk_resolved_series_retractions_row
        FOREIGN KEY (feature_id, obs_date, vintage_date)
        REFERENCES resolved_series (feature_id, obs_date, vintage_date)
)
"""
_RETRACTIONS_GUARD_FN = """
CREATE OR REPLACE FUNCTION resolved_series_retractions_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION
        'resolved_series_retractions is append-only (% refused)', TG_OP;
END;
$$ LANGUAGE plpgsql;
"""
_RETRACTIONS_GUARD_TRIGGER = """
CREATE TRIGGER trg_resolved_series_retractions_guard
    BEFORE INSERT OR UPDATE OR DELETE ON resolved_series_retractions
    FOR EACH ROW
    EXECUTE FUNCTION resolved_series_retractions_guard();
"""

D_OBS = date(2024, 1, 10)
V1 = date(2024, 1, 15)  # clean vintage
V2 = date(2024, 1, 20)  # contaminated, later vintage


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"derived_pit_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_FEATURE_REGISTRY_DDL))
            conn.execute(text(_RESOLVED_SERIES_DDL))
            conn.execute(text(_RESOLVED_SERIES_KEY))
            conn.execute(text(_SOURCE_CATALOG_DDL))
            conn.execute(text(_RAW_SERIES_DDL))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _install_retractions_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_RETRACTIONS_DDL))
        conn.execute(text(_RETRACTIONS_GUARD_FN))
        conn.execute(text(_RETRACTIONS_GUARD_TRIGGER))


def _feature(engine: Engine, name: str, family: str = "commodity") -> int:
    with engine.begin() as conn:
        return conn.execute(
            text("INSERT INTO feature_registry (name, family) VALUES (:n, :f) RETURNING id"),
            {"n": name, "f": family},
        ).scalar_one()


def _resolved(engine: Engine, fid: int, obs: date, vintage: date, value: float) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                "VALUES (:f, :o, :v, :v, :val, 1)"
            ),
            {"f": fid, "o": obs, "v": vintage, "val": value},
        )


def _retract(engine: Engine, fid: int, obs: date, vintage: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, reason, run_tag) "
                "VALUES (:f, :o, :v, 'no_clean_raw', 'test_run')"
            ),
            {"f": fid, "o": obs, "v": vintage},
        )


def _db_today(engine: Engine) -> date:
    with engine.connect() as conn:
        return conn.execute(text("SELECT (now() AT TIME ZONE 'UTC')::date")).scalar_one()


def _count_resolved(engine: Engine, fid: int) -> int:
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT COUNT(*) FROM resolved_series WHERE feature_id = :f"), {"f": fid},
        ).scalar_one()


def _seed_copper_gold_resolved(engine: Engine) -> tuple[int, int, int, int]:
    """feature_registry + resolved_series for compute_derived_features.py's
    "copper"/"gold" inputs and its "copper_gold_ratio"/"copper_gold_slope"
    targets. copper carries a clean V1 and a later, contaminated V2."""
    copper = _feature(engine, "copper")
    gold = _feature(engine, "gold")
    ratio_fid = _feature(engine, "copper_gold_ratio")
    slope_fid = _feature(engine, "copper_gold_slope")
    _resolved(engine, copper, D_OBS, V1, 4.0)     # clean: ratio should use this
    _resolved(engine, copper, D_OBS, V2, 999.0)   # contaminated latest vintage
    _resolved(engine, gold, D_OBS, V1, 2.0)
    return copper, gold, ratio_fid, slope_fid


# ---------------------------------------------------------------------------
# compute_derived_features.py (writes resolved_series directly)
# ---------------------------------------------------------------------------

def test_cdf_refuses_to_run_without_retractions_table(scratch, monkeypatch):
    copper, gold, ratio_fid, slope_fid = _seed_copper_gold_resolved(scratch)
    monkeypatch.setattr(cdf, "get_engine", lambda: scratch)

    cdf.run(family_filter="commodity", dry_run=False)

    # Refused before ever computing or writing anything for the target.
    assert _count_resolved(scratch, ratio_fid) == 0


def test_cdf_computes_and_stamps_computation_time_not_obs_date(scratch, monkeypatch):
    _install_retractions_table(scratch)
    copper, gold, ratio_fid, slope_fid = _seed_copper_gold_resolved(scratch)
    monkeypatch.setattr(cdf, "get_engine", lambda: scratch)
    today = _db_today(scratch)
    assert today != D_OBS  # sanity: computation day differs from obs_date

    cdf.run(family_filter="commodity", dry_run=False)

    with scratch.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT obs_date, release_date, vintage_date, value "
                "FROM resolved_series WHERE feature_id = :f"
            ),
            {"f": ratio_fid},
        ).fetchall()
    assert len(rows) == 1
    obs, release, vintage, value = rows[0]
    assert obs == D_OBS
    # Retraction wasn't even needed here (V2 predates the fix's read of "the
    # clean vintage" only when retracted -- see the retraction test below);
    # this test is about the stamp, so just check it landed and is sane.
    assert release == today
    assert vintage == today
    assert release != obs and vintage != obs  # never backdated to obs_date


def test_cdf_honors_retraction_of_contaminated_latest_vintage(scratch, monkeypatch):
    _install_retractions_table(scratch)
    copper, gold, ratio_fid, slope_fid = _seed_copper_gold_resolved(scratch)
    _retract(scratch, copper, D_OBS, V2)  # hide the contaminated 999.0 vintage
    monkeypatch.setattr(cdf, "get_engine", lambda: scratch)

    cdf.run(family_filter="commodity", dry_run=False)

    with scratch.connect() as conn:
        value = conn.execute(
            text("SELECT value FROM resolved_series WHERE feature_id = :f"),
            {"f": ratio_fid},
        ).scalar_one()
    # copper's remaining (non-retracted) vintage is V1 = 4.0; gold = 2.0.
    assert value == pytest.approx(2.0)  # 4.0 / 2.0, NOT 999.0 / 2.0


def test_cdf_without_retraction_would_use_contaminated_latest_vintage(scratch, monkeypatch):
    """Contrast case: with the table present but nothing retracted, LATEST_AS_OF
    picks copper's latest vintage as usual -- the contaminated 999.0 -- which is
    exactly why the retraction in the test above is what fixes the read."""
    _install_retractions_table(scratch)
    copper, gold, ratio_fid, slope_fid = _seed_copper_gold_resolved(scratch)
    monkeypatch.setattr(cdf, "get_engine", lambda: scratch)

    cdf.run(family_filter="commodity", dry_run=False)

    with scratch.connect() as conn:
        value = conn.execute(
            text("SELECT value FROM resolved_series WHERE feature_id = :f"),
            {"f": ratio_fid},
        ).scalar_one()
    assert value == pytest.approx(999.0 / 2.0)


def test_cdf_never_overwrites_an_existing_vintage(scratch, monkeypatch):
    _install_retractions_table(scratch)
    copper, gold, ratio_fid, slope_fid = _seed_copper_gold_resolved(scratch)
    monkeypatch.setattr(cdf, "get_engine", lambda: scratch)
    today = _db_today(scratch)
    # A row already on file for this exact (feature, obs_date, vintage=today)
    # key, as if an earlier run today (or the resolver) already wrote it.
    _resolved(scratch, ratio_fid, D_OBS, today, -12345.0)

    cdf.run(family_filter="commodity", dry_run=False)

    with scratch.connect() as conn:
        rows = conn.execute(
            text("SELECT value FROM resolved_series WHERE feature_id = :f AND obs_date = :o"),
            {"f": ratio_fid, "o": D_OBS},
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == pytest.approx(-12345.0)  # untouched, not overwritten


# ---------------------------------------------------------------------------
# fill_missing_features.py::compute_derived_features (writes raw_series
# COMPUTED:* rows, resolver picks them up later)
# ---------------------------------------------------------------------------
#
# get_series() (unchanged by this fix) keeps its pre-existing 400-day
# window (``rs.obs_date >= CURRENT_DATE - :days`` before, now a filter on
# the PIT result). D_OBS/V1/V2 above are fixed 2024 dates, far outside that
# window by the time this runs -- so this section uses its own obs_date/
# vintages computed relative to "now".

FMF_D_OBS = date.today() - timedelta(days=30)
FMF_V1 = FMF_D_OBS + timedelta(days=2)   # clean vintage
FMF_V2 = FMF_D_OBS + timedelta(days=5)   # contaminated, later vintage


def _seed_copper_gold_futures(engine: Engine) -> tuple[int, int]:
    copper = _feature(engine, "copper_futures_close")
    gold = _feature(engine, "gold_futures_close")
    _resolved(engine, copper, FMF_D_OBS, FMF_V1, 4.0)
    _resolved(engine, copper, FMF_D_OBS, FMF_V2, 999.0)
    _resolved(engine, gold, FMF_D_OBS, FMF_V1, 2.0)
    return copper, gold


def _computed_ratio_value(engine: Engine) -> float | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'COMPUTED:copper_gold_ratio' AND obs_date = :o"
            ),
            {"o": FMF_D_OBS},
        ).fetchone()
    return float(row[0]) if row else None


def test_fmf_refuses_to_run_without_retractions_table(scratch):
    _seed_copper_gold_futures(scratch)

    results = fmf.compute_derived_features(scratch)

    assert results == []
    assert _computed_ratio_value(scratch) is None


def test_fmf_honors_retraction_of_contaminated_latest_vintage(scratch):
    _install_retractions_table(scratch)
    copper, gold = _seed_copper_gold_futures(scratch)
    _retract(scratch, copper, FMF_D_OBS, FMF_V2)

    results = fmf.compute_derived_features(scratch)

    assert any(r.get("feature") == "copper_gold_ratio" for r in results)
    assert _computed_ratio_value(scratch) == pytest.approx(2.0)  # 4.0 / 2.0


def test_fmf_without_retraction_would_use_contaminated_latest_vintage(scratch):
    _install_retractions_table(scratch)
    copper, gold = _seed_copper_gold_futures(scratch)

    fmf.compute_derived_features(scratch)

    assert _computed_ratio_value(scratch) == pytest.approx(999.0 / 2.0)
