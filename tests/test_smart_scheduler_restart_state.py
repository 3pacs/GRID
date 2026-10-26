"""SmartScheduler restart state survives a Hermes restart (stale-sources audit 2026-09-29).

Before this fix, restart state was rebuilt only from
``source_catalog.last_pull_at`` keyed by ``name.lower()``: 48 of the 92
registry names match no catalog row that way, so every restart made them
look "never run", they sorted ahead of genuinely overdue sources, and the
real ones (bls/eia/cboe/aaii/finra_short_volume/sec_ftd) starved.

These tests cover:
  * the explicit registry -> source_catalog map, checked against each
    puller class's own SOURCE_NAME (AST only -- no puller imports);
  * a simulated restart on a real (SQLite) database: after one tick, a
    fresh SmartScheduler has nothing due, and repeated restart+tick cycles
    run each job exactly once;
  * failure streak / cooldown restoration, the shared-catalog-row
    bootstrap guard, and the pull_log row shape.
"""

from __future__ import annotations

import ast
import pathlib
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

import ingestion.smart_scheduler as ss
from ingestion.smart_scheduler import (
    PULLER_REGISTRY,
    REGISTRY_CATALOG_NAMES,
    SMART_PULL_LOG_PREFIX,
    SmartScheduler,
    catalog_name_for,
)

REPO = pathlib.Path(__file__).resolve().parents[1]

# Registry entries whose puller class defines no SOURCE_NAME attribute; the
# expected catalog row is what the class's own code writes / resolves.
_NO_SOURCE_NAME_ATTR = {
    "coingecko": "coingecko",          # writes features directly; catalog row "coingecko"
    "social_sentiment": "SocialSentiment",  # hand-rolled INSERT INTO source_catalog
    "wiki_history": "WikiHistory",          # hand-rolled INSERT INTO source_catalog
    "sec_ftd": "SEC_FTD",                   # adapter over SECFTDPuller.SOURCE_NAME
}


def _class_source_name(mod: str, cls: str) -> str | None:
    path = REPO / (mod.replace(".", "/") + ".py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for stmt in node.body:
                if isinstance(stmt, ast.Assign):
                    targets = [getattr(t, "id", None) for t in stmt.targets]
                    value = stmt.value
                elif isinstance(stmt, ast.AnnAssign):
                    targets = [getattr(stmt.target, "id", None)]
                    value = stmt.value
                else:
                    continue
                if "SOURCE_NAME" in targets and isinstance(value, ast.Constant):
                    return str(value.value)
            return None
    raise AssertionError(f"class {cls} not found in {mod}")


@pytest.mark.parametrize("entry", PULLER_REGISTRY, ids=lambda e: e["name"])
def test_every_registry_entry_maps_to_its_writers_catalog_row(entry) -> None:
    name = entry["name"]
    if name in _NO_SOURCE_NAME_ATTR:
        expected = _NO_SOURCE_NAME_ATTR[name]
    else:
        expected = _class_source_name(entry["mod"], entry["cls"])
        assert expected, f"{name}: {entry['cls']} has no SOURCE_NAME; add it to _NO_SOURCE_NAME_ATTR"
    assert catalog_name_for(name).lower() == expected.lower(), (
        f"registry entry {name!r} writes source_catalog row {expected!r} but "
        f"REGISTRY_CATALOG_NAMES maps it to {catalog_name_for(name)!r}"
    )


def test_catalog_map_has_no_stale_or_redundant_keys() -> None:
    names = {p["name"] for p in PULLER_REGISTRY}
    assert set(REGISTRY_CATALOG_NAMES) <= names, set(REGISTRY_CATALOG_NAMES) - names
    for key, value in REGISTRY_CATALOG_NAMES.items():
        assert key.lower() != value.lower(), f"{key}: redundant identity mapping"


def test_the_starved_sources_from_the_audit_are_now_keyed_correctly() -> None:
    # The six the audit saw run 0x on 2026-09-28 already lower() to their
    # catalog rows; the 14x re-runners are the ones that needed the map.
    for name, row in {
        "bls": "BLS", "eia": "EIA", "cboe": "CBOE", "aaii_sentiment": "AAII_Sentiment",
        "finra_short_volume": "FINRA_SHORT_VOLUME", "sec_ftd": "SEC_FTD",
        "lunar": "LUNAR_EPHEMERIS", "solar": "NOAA_SWPC", "planetary": "PLANETARY_EPHEMERIS",
        "congressional": "CONGRESS_TRADING", "fed_liquidity": "FRED",
        "smart_money": "Social_Smart_Money", "prediction_odds": "Polymarket",
        "social_attention": "Wikipedia_Attention", "fed_speeches": "FedSpeeches",
        "ag_commodity_futures": "YFINANCE_COMMODITY_FUTURES", "crucix_bridge": "Crucix",
        "options": "YFINANCE_OPTIONS",
    }.items():
        assert catalog_name_for(name).lower() == row.lower()


# ── Simulated restart on a real database ─────────────────────────────────

RUN_COUNTS: dict[str, int] = {}


class _FakePuller:
    """Stand-in puller: counts runs per registry entry via its own name."""

    def __init__(self, db_engine) -> None:
        self.db_engine = db_engine

    def pull_all(self, tag: str) -> dict:
        RUN_COUNTS[tag] = RUN_COUNTS.get(tag, 0) + 1
        return {"status": "SUCCESS", "rows_inserted": 3}


class _FailingPuller(_FakePuller):
    def pull_all(self, tag: str) -> dict:
        RUN_COUNTS[tag] = RUN_COUNTS.get(tag, 0) + 1
        raise RuntimeError("upstream 503")


def _entry(name: str, freq_h: float, cls: str = "_FakePuller") -> dict:
    return {
        "name": name, "mod": __name__, "cls": cls, "method": "pull_all",
        "freq_h": freq_h, "timeout_s": 10, "kwargs": {"tag": name},
    }


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )

    @event.listens_for(engine, "connect")
    def _now(dbapi_conn, _rec) -> None:  # Postgres NOW() stand-in
        dbapi_conn.create_function(
            "NOW", 0, lambda: datetime.now(timezone.utc).isoformat(sep=" ")
        )

    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT UNIQUE, "
            "last_pull_at TIMESTAMP)"
        ))
        conn.execute(text(
            "CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "puller_name TEXT NOT NULL, source_id INTEGER, started_at TIMESTAMP NOT NULL, "
            "completed_at TIMESTAMP, status TEXT NOT NULL CHECK (status IN "
            "('RUNNING','SUCCESS','PARTIAL','FAILED')), rows_inserted INTEGER DEFAULT 0, "
            "rows_expected INTEGER, error_message TEXT, node_name TEXT)"
        ))
    return engine


def _add_catalog(engine, cid: int, name: str, last_pull_at: datetime | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO source_catalog (id, name, last_pull_at) VALUES (:i, :n, :t)"),
            {"i": cid, "n": name, "t": last_pull_at},
        )


@pytest.fixture
def registry(monkeypatch):
    RUN_COUNTS.clear()
    entries: list[dict] = []
    catalog_map: dict[str, str] = {}
    monkeypatch.setattr(ss, "PULLER_REGISTRY", entries)
    monkeypatch.setattr(ss, "REGISTRY_CATALOG_NAMES", catalog_map)
    monkeypatch.setattr(ss, "MAX_PULLERS_PER_TICK", 50)
    monkeypatch.setattr(SmartScheduler, "_warn_registry_divergence", lambda self: None)
    return entries, catalog_map


def test_restart_does_not_rerun_jobs_that_already_ran(registry) -> None:
    entries, catalog_map = registry
    engine = _engine()
    # Mismatched registry name -> catalog row (the 48-entry bug class),
    # an exact match, and one with no catalog row at all.
    entries += [_entry("lunar_like", 24), _entry("bls_like", 168), _entry("uncatalogued", 24)]
    catalog_map["lunar_like"] = "LUNAR_LIKE_EPHEMERIS"
    _add_catalog(engine, 1, "LUNAR_LIKE_EPHEMERIS", None)
    _add_catalog(engine, 2, "bls_like", None)

    first = SmartScheduler(engine)
    assert {p["name"] for p in first._get_due_pullers()} == {"lunar_like", "bls_like", "uncatalogued"}
    summary = first.tick()
    assert summary["succeeded"] == 3
    assert RUN_COUNTS == {"lunar_like": 1, "bls_like": 1, "uncatalogued": 1}

    # Simulated restart: brand-new process state, same database.
    restarted = SmartScheduler(engine)
    assert restarted._get_due_pullers() == []
    assert {n: s["state_source"] for n, s in restarted._state.items()} == {
        "lunar_like": "pull_log", "bls_like": "pull_log", "uncatalogued": "pull_log",
    }
    restarted.tick()
    assert RUN_COUNTS == {"lunar_like": 1, "bls_like": 1, "uncatalogued": 1}

    # The mapped catalog row got bumped (previously a silent no-op).
    with engine.connect() as conn:
        bumped = dict(conn.execute(text("SELECT name, last_pull_at FROM source_catalog")).fetchall())
    assert bumped["LUNAR_LIKE_EPHEMERIS"] is not None
    assert bumped["bls_like"] is not None


def test_fourteen_restarts_in_five_hours_run_each_job_once(registry) -> None:
    """The 2026-09-28 pattern: 14 grid-hermes restarts between 17:03 and 22:06Z."""
    entries, catalog_map = registry
    engine = _engine()
    for i in range(6):
        entries.append(_entry(f"mismatch_{i}", 24))
        catalog_map[f"mismatch_{i}"] = f"CATALOG_{i}"
        _add_catalog(engine, 10 + i, f"CATALOG_{i}", None)
    entries.append(_entry("weekly_real", 168))
    _add_catalog(engine, 99, "weekly_real", datetime.now(timezone.utc) - timedelta(hours=200))

    for _restart in range(14):
        SmartScheduler(engine).tick()

    assert RUN_COUNTS == {**{f"mismatch_{i}": 1 for i in range(6)}, "weekly_real": 1}


def test_failure_streak_and_cooldown_survive_restart(registry) -> None:
    entries, _ = registry
    engine = _engine()
    entries.append(_entry("flaky", 1, cls="_FailingPuller"))

    sched = SmartScheduler(engine)
    sched.tick()
    assert RUN_COUNTS == {"flaky": 1}

    restarted = SmartScheduler(engine)
    state = restarted._state["flaky"]
    assert state["consecutive_fails"] == 1
    assert state["cooldown_until"] > datetime.now(timezone.utc) + timedelta(minutes=25)
    assert restarted._get_due_pullers() == []  # still backing off
    restarted.tick()
    assert RUN_COUNTS == {"flaky": 1}

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT puller_name, status, error_message, rows_inserted FROM pull_log"
        )).fetchall()
    assert [(r[0], r[1]) for r in rows] == [(SMART_PULL_LOG_PREFIX + "flaky", "FAILED")]
    assert "upstream 503" in rows[0][2]


def test_bootstrap_uses_exclusively_owned_catalog_row(registry) -> None:
    """First activation (no pull_log history yet) must not stampede either."""
    entries, catalog_map = registry
    engine = _engine()
    entries.append(_entry("mismatch", 24))
    catalog_map["mismatch"] = "OWNED_ROW"
    _add_catalog(engine, 1, "OWNED_ROW", datetime.now(timezone.utc) - timedelta(hours=2))

    sched = SmartScheduler(engine)
    assert sched._state["mismatch"]["state_source"] == "source_catalog"
    assert sched._get_due_pullers() == []


def test_shared_catalog_row_does_not_bootstrap_other_jobs(registry) -> None:
    """repo_market (168h) must not be deferred forever by FRED's 12h bumps."""
    entries, catalog_map = registry
    engine = _engine()
    entries += [_entry("fred_like", 12), _entry("repo_like", 168), _entry("curve_like", 24)]
    catalog_map["repo_like"] = "fred_like"
    catalog_map["curve_like"] = "fred_like"
    _add_catalog(engine, 1, "fred_like", datetime.now(timezone.utc) - timedelta(hours=1))

    sched = SmartScheduler(engine)
    due = {p["name"] for p in sched._get_due_pullers()}
    assert "fred_like" not in due  # the row's own entry still bootstraps
    assert due == {"repo_like", "curve_like"}

    sched.tick()
    assert SmartScheduler(engine)._get_due_pullers() == []


def test_skipped_runs_are_not_logged_and_timeouts_log_as_failed() -> None:
    engine = _engine()
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = engine
    sched._catalog_ids = {}
    now = datetime.now(timezone.utc)
    sched._log_run("a", now, {"status": "SKIPPED", "reason": "thread limit"})
    sched._log_run("b", now, {"status": "TIMEOUT", "error": "Exceeded 120s timeout"})
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT puller_name, status, error_message FROM pull_log")).fetchall()
    assert rows == [(SMART_PULL_LOG_PREFIX + "b", "FAILED", "TIMEOUT: Exceeded 120s timeout")]


def test_state_load_failure_degrades_to_empty_state() -> None:
    class _Broken:
        def connect(self):
            raise RuntimeError("db down")

    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = _Broken()
    sched._state = {}
    sched._load_state_from_db()  # must not raise
    assert sched._state == {}
