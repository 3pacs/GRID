"""GD5 acceptance test 13: the people_events-backed look-ahead canary (PostgreSQL).

E1-style canary for ``analysis.people_density`` on a real ``people_events``
table, built from the real migration DDL (people_events_20260927 ->
security_master_20260927 -> people_events_v2_20261001) in a throwaway schema:

1. ``load_events(engine, as_of, ...)`` never returns a row with
   ``known_at > as_of`` -- including rows whose ``event_time`` is before
   ``as_of`` (traded before, public after) -- and never an echo.
2. Appending rows known after ``as_of`` changes no feature at any decision
   ``<= as_of`` (A, C, S, D_self, D_peer), byte for byte.
3. Leak self-test: with the store's PIT filter regressed to ``event_time``
   (monkeypatched), ``load_events``' own guard refuses the leaked rows, and
   a pipeline that also takes availability to be the trade date makes the
   append-future canary trip -- so the canary is proven able to fail -- while
   the real path stays byte-identical.

Fixture rules mirror the E1 ``pg_scratch`` fixture (PR #762,
``evals/e1/conftest.py`` / ``pg_safety.py``): the URL comes only from
``GRID_TEST_DB_URL`` (no default -- a default would point at production on
grid-svr); a production database name is refused before any connection;
with ``E1_REQUIRE_PG=1`` (set by the CI step) an unreachable PostgreSQL is a
failure, not a skip. This file is the fallback home named by the GD5 brief
while #762 is unmerged; once E1 is on main it moves to
``evals/e1/test_people_density_canary.py`` (owner-approved E1 bump).
"""

from __future__ import annotations

import importlib
import os
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from analysis import people_density as P
from store import people_events as pe_store
from store.people_events import PeopleEvent, upsert_event

UTC = timezone.utc
AS_OF = datetime(2024, 6, 28, 20, 0, tzinfo=UTC)  # a Friday, 16:00 New York
CHANNELS = ("form4", "congress")
PRODUCTION_DB_NAMES = frozenset({"grid", "griddb", "grid_obsidian", "grid_v4", "gridprod", "postgres"})


def check_scratch_target(url: str) -> str:
    """Same rule as evals/e1/pg_safety.py: a *_test name is fine; a production name or any other grid* name is refused."""
    name = (make_url(url).database or "").strip().lower()
    if not name:
        raise RuntimeError("GRID_TEST_DB_URL names no database")
    if name.endswith("_test"):
        return name
    if name in PRODUCTION_DB_NAMES or name.startswith("grid"):
        raise RuntimeError(f"refusing scratch schemas in {name!r}: point GRID_TEST_DB_URL at a *_test database")
    return name


@pytest.mark.parametrize("url, ok", [
    ("postgresql://u:p@localhost/griddb_test", True),
    ("postgresql://u:p@localhost/griddb", False),
    ("postgresql://u:p@localhost/grid", False),
    ("postgresql://u:p@localhost/grid_v4", False),
    ("postgresql://u:p@localhost/postgres", False),
])
def test_scratch_target_refuses_production_names(url, ok):
    if ok:
        assert check_scratch_target(url)
    else:
        with pytest.raises(RuntimeError):
            check_scratch_target(url)


def _run_migration(engine) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    # The store targets the v2 schema (people_events_v2_20261001: partial
    # unique index, versioning columns, TEXT security_id FK onto
    # security_master), so the scratch table is built from the whole chain.
    with engine.connect() as conn:
        trans = conn.begin()
        for name in ("migrations.versions.people_events_20260927",
                     "migrations.versions.security_master_20260927",
                     "migrations.versions.people_events_v2_20261001"):
            migration = importlib.import_module(name)
            real_op = migration.op
            migration.op = Operations(MigrationContext.configure(conn))
            try:
                migration.upgrade()
            finally:
                migration.op = real_op
        trans.commit()


@pytest.fixture()
def pe_scratch():
    require = os.environ.get("E1_REQUIRE_PG") == "1"
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        if require:
            pytest.fail("E1_REQUIRE_PG=1 but GRID_TEST_DB_URL is unset")
        pytest.skip("GRID_TEST_DB_URL unset (set E1_REQUIRE_PG=1 to make this a failure)")
    try:
        check_scratch_target(url)
    except RuntimeError as exc:
        pytest.fail(str(exc))
    try:
        admin = create_engine(url, pool_pre_ping=True)
        with admin.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - environment dependent
        if require:
            pytest.fail(f"E1_REQUIRE_PG=1 but PostgreSQL is unreachable: {type(exc).__name__}")
        pytest.skip("PostgreSQL not available (set E1_REQUIRE_PG=1 to make this a failure)")
    schema = f"gd5_canary_{uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(admin.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        _run_migration(engine)
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


# --- synthetic people_events world -----------------------------------------------------------


def _event(i: int, *, known_at: datetime, event_time: datetime | None = None, channel: str = "form4",
           issuer: int = 1, actor: int = 1, direction: str = "buy", plan: bool | None = False,
           echo_of: int | None = None, tag: str = "base") -> PeopleEvent:
    code = None
    if channel == "form4":
        code = "P" if direction == "buy" else "S"
    return PeopleEvent(
        channel=channel, dedup_key=f"{tag}|{i}", event_time=event_time or known_at - timedelta(days=2),
        known_at=known_at, known_at_basis="filing",
        actor_id=f"{actor:010d}" if channel == "form4" else f"B{actor:06d}",
        actor_id_basis="owner_cik" if channel == "form4" else "bioguide",
        actor_type="insider" if channel == "form4" else "member", source="synthetic",
        entity_cik=str(7000 + issuer), direction=direction, transaction_code=code,
        provenance={} if plan is None else {"attrs": {"is_10b5_1": plan}}, echo_of=echo_of,
    )


def _world(rng: np.random.Generator, n: int, lo: datetime, hi: datetime, tag: str, start: int = 0) -> list[PeopleEvent]:
    span = (hi - lo).total_seconds()
    out = []
    for i in range(start, start + n):
        known = lo + timedelta(seconds=float(rng.uniform(1, span)))
        channel = str(rng.choice(CHANNELS))
        out.append(_event(
            i, known_at=known, channel=channel, issuer=int(rng.integers(0, 10)), actor=int(rng.integers(0, 25)),
            direction=str(rng.choice(["buy", "sell"])), plan=bool(rng.random() < 0.3), tag=tag,
        ))
    return out


MEMBERS = pd.DataFrame({
    "entity_id": [P.entity_id_for_cik(7000 + i) for i in range(10)],
    "sector": ["S0"] * 5 + ["S1"] * 5,
    "valid_from": [date(2015, 1, 1)] * 10, "valid_to": [None] * 10,
})
ENTITIES = sorted(MEMBERS["entity_id"])
DECISIONS = P.weekly_decisions(date(2022, 1, 7), AS_OF.date())
COVERAGE = [P.CoverageSpan(c, pd.Timestamp("2015-01-01", tz="UTC")) for c in CHANNELS]


def _features(events: pd.DataFrame) -> dict[str, bytes]:
    out = {}
    for name in ("A_insider_buy_w90", "A_multi_mc1_w30", "A_congress_w90"):
        a = P.density_A(events, P.DECLARED_SPECS[name], ENTITIES, DECISIONS, coverage=COVERAGE)
        out[name] = a.to_numpy().tobytes()
        out[name + ":D_self"] = P.d_self(a).to_numpy().tobytes()
        out[name + ":D_peer"] = P.d_peer(a, MEMBERS).to_numpy().tobytes()
    spec_c = P.DensitySpec("C_canary", tuple(P.ChannelRule(c) for c in CHANNELS), 90, 45.0)
    out["C"] = P.channel_count_C(events, spec_c, ENTITIES, DECISIONS, coverage=COVERAGE).to_numpy().tobytes()
    out["S"] = P.signed_S(events, P.DECLARED_SPECS["S_insider_w90"], ENTITIES, DECISIONS,
                          coverage=COVERAGE).to_numpy().tobytes()
    return out


def _future_rows(rng: np.random.Generator) -> list[PeopleEvent]:
    """Rows nobody may use at AS_OF: traded before AS_OF but filed after, and wholly later acts.

    Includes Form 4 sells with no 10b5-1 determination (S would refuse them if
    they leaked in) and echoes of base rows.
    """
    rows = _world(rng, 150, AS_OF + timedelta(microseconds=1), AS_OF + timedelta(days=200), "future", 10_000)
    for i in range(10_200, 10_260):
        rows.append(_event(i, known_at=AS_OF + timedelta(hours=1 + i % 50), event_time=AS_OF - timedelta(days=5),
                           issuer=i % 10, actor=100 + i % 7, direction="sell", plan=None, tag="late-filed"))
    return rows


def _seed(engine, rows) -> None:
    for row in rows:
        upsert_event(engine, row)


def test_load_events_is_pit_and_features_ignore_rows_known_after_as_of(pe_scratch):
    rng = np.random.default_rng(13)
    base = _world(rng, 600, datetime(2020, 1, 1, tzinfo=UTC), AS_OF, "base")
    _seed(pe_scratch, base)
    upsert_event(pe_scratch, _event(90_000, known_at=AS_OF - timedelta(days=3), echo_of=None, tag="orig"))
    with pe_scratch.connect() as conn:
        orig_id = conn.execute(text("SELECT id FROM people_events WHERE dedup_key = 'orig|90000'")).scalar()
    upsert_event(pe_scratch, _event(90_001, known_at=AS_OF - timedelta(days=2), actor=999, echo_of=orig_id, tag="echo"))

    before_frame = P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False)
    assert len(before_frame) == len(base) + 1
    assert before_frame.attrs["versioned_store_gap"] is False  # v2 table read through read_event_versions
    assert (before_frame["known_at"] <= pd.Timestamp(AS_OF)).all()
    assert not before_frame["dedup_key"].str.startswith("echo").any()
    before = _features(before_frame)
    assert any(np.frombuffer(v, dtype=float).any() for v in before.values())  # not vacuous

    future = _future_rows(rng)
    _seed(pe_scratch, future)
    with pe_scratch.connect() as conn:
        total = conn.execute(text("SELECT count(*) FROM people_events")).scalar()
    assert total == len(base) + 2 + len(future)

    after_frame = P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False)
    assert not after_frame["dedup_key"].str.startswith(("future", "late-filed")).any()
    assert P.event_set_sha256(after_frame) == P.event_set_sha256(before_frame)
    after = _features(after_frame)
    for key in before:
        assert after[key] == before[key], key


def test_ticker_only_rows_resolve_through_security_master_as_of_known_at(pe_scratch):
    from intelligence.security_master import ensure_tables

    ensure_tables(pe_scratch)
    with pe_scratch.begin() as conn:
        conn.execute(text("INSERT INTO security_master (entity_id, name, source) VALUES ('sm_0000007777', 'X', 'test')"))
        conn.execute(text(
            "INSERT INTO security_identifiers (entity_id, id_scheme, id_value, valid_from, source) "
            "VALUES ('sm_0000007777', 'ticker', 'ABC', '2024-01-01', 'test')"))
    ticker_row = PeopleEvent(
        channel="form4", dedup_key="tkr|1", event_time=AS_OF - timedelta(days=9), known_at=AS_OF - timedelta(days=7),
        known_at_basis="filing", actor_id="JANE DOE", actor_id_basis="normalized_name", actor_type="insider",
        source="quiverquant", entity_ticker="ABC", direction="buy", transaction_code="P",
    )
    early = PeopleEvent(**{**ticker_row.__dict__, "dedup_key": "tkr|0", "known_at": datetime(2023, 6, 1, tzinfo=UTC),
                           "event_time": datetime(2023, 5, 30, tzinfo=UTC)})
    upsert_event(pe_scratch, ticker_row)
    upsert_event(pe_scratch, early)  # ABC was not yet mapped in 2023: unresolved, dropped, counted
    frame = P.load_events(pe_scratch, AS_OF, ("form4",))
    assert list(frame["entity_id"]) == ["sm_0000007777"]
    assert frame.attrs["dropped"]["unresolved_entity"] == 1


def test_leak_self_test_canary_trips_on_a_store_and_reader_keyed_on_event_time(pe_scratch, monkeypatch):
    rng = np.random.default_rng(31)
    _seed(pe_scratch, _world(rng, 400, datetime(2021, 1, 1, tzinfo=UTC), AS_OF, "base"))
    honest_before = _features(P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False))

    # Deliberately leaky pipeline: the store's PIT filter regressed from
    # known_at to event_time, and availability is taken to be the trade date.
    leaky_sql = pe_store._READ_SQL_TEMPLATE.replace("WHERE known_at <= :as_of", "WHERE event_time <= :as_of")
    assert leaky_sql != pe_store._READ_SQL_TEMPLATE
    monkeypatch.setattr(pe_store, "_READ_SQL_TEMPLATE", leaky_sql)
    # The v2 store is read through read_event_versions: regress it the same way.
    leaky_history = pe_store._HISTORY_SQL_TEMPLATE.replace("WHERE known_at <= :known_by",
                                                           "WHERE event_time <= :known_by")
    assert leaky_history != pe_store._HISTORY_SQL_TEMPLATE
    monkeypatch.setattr(pe_store, "_HISTORY_SQL_TEMPLATE", leaky_history)

    def leaky_read() -> pd.DataFrame:
        return P.events_frame(
            [e for c in CHANNELS for e in pe_store.read_events(pe_scratch, AS_OF, channel=c, exclude_echoes=True)]
        )

    def leaky_features() -> dict[str, bytes]:
        frame = leaky_read()
        return _features(frame.assign(known_at=frame["event_time"]))

    leaky_before = leaky_features()
    # Traded before AS_OF, filed after it.
    late = [_event(20_000 + i, known_at=AS_OF + timedelta(days=3), event_time=AS_OF - timedelta(days=4),
                   issuer=i % 10, actor=300 + i, direction="buy", tag="late") for i in range(40)]
    _seed(pe_scratch, late)

    # 1) The leaky store hands back exactly the late-filed rows, and
    #    load_events' own guard refuses them.
    assert (leaky_read()["known_at"] > pd.Timestamp(AS_OF)).sum() == len(late)
    with pytest.raises(AssertionError, match="known_at > as_of"):
        P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False)
    # 2) The append-future canary trips on the leaky pipeline ...
    assert leaky_features()["A_insider_buy_w90"] != leaky_before["A_insider_buy_w90"]
    # 3) ... and stays silent on the real one.
    monkeypatch.undo()
    assert _features(P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False)) == honest_before


def test_superseded_version_stays_visible_to_earlier_decisions(pe_scratch):
    """v2: a version superseded before AS_OF still counts for decisions before its supersession."""
    first = _event(70_000, known_at=datetime(2024, 3, 1, 2, tzinfo=UTC), tag="ver")
    upsert_event(pe_scratch, first)
    with pe_scratch.begin() as conn:
        conn.execute(text("UPDATE people_events SET superseded_at = :t WHERE dedup_key = 'ver|70000'"),
                     {"t": datetime(2024, 5, 1, tzinfo=UTC)})
    upsert_event(pe_scratch, _event(70_000, known_at=datetime(2024, 5, 1, tzinfo=UTC), tag="ver",
                                    direction="sell"))
    frame = P.load_events(pe_scratch, AS_OF, CHANNELS, resolve_tickers=False)
    ver = frame[frame["dedup_key"] == "ver|70000"].sort_values("known_at")
    assert len(ver) == 2
    assert ver.iloc[0]["visible_until"] == pd.Timestamp("2024-05-01", tz="UTC")
    assert pd.isna(ver.iloc[1]["visible_until"])
    early = P.load_events(pe_scratch, datetime(2024, 4, 1, tzinfo=UTC), CHANNELS, resolve_tickers=False)
    ver_early = early[early["dedup_key"] == "ver|70000"]
    assert len(ver_early) == 1 and pd.isna(ver_early.iloc[0]["visible_until"])  # later end is masked
