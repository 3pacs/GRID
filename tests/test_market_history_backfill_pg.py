"""Disposable-PostgreSQL proof for scripts/market_history_backfill.py.

Runs only when GRID_TEST_DB_URL points at a local database whose name contains
"test". Each test builds its own schema, with source and feature ids mirroring
griddb (R1: yfinance=2, binance=184, TIINGO=524; btc_full=677, eth_full=675,
spy_full=2791), and drops it afterwards. The unique index is the real
uq_raw_series_composite. Providers are fakes that go through the importer's
real HTTP and parsing code; nothing touches the network.

Covered: read-only planning, exact bounded writes, acquisition/availability
timestamps, idempotent re-apply, provider failure and resume, interruption and
resume, whole-chunk rollback on a database failure, revisions (stop, or append
only), plan drift, duplicate and unfinished bars, single-writer locking,
refusal guards, validation against existing identities, coverage-gap targeting,
and proof that the real Resolver never promotes the namespace.
"""

from __future__ import annotations

import importlib
import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from binance_close_contract import is_completed_canonical_close
from normalization.entity_map import EntityMap
from normalization.resolver import Resolver
from price_close_contract import is_policy_capture

mhb = importlib.import_module("scripts.market_history_backfill")

DAY_MS = 86_400_000
WINDOW = (date(2025, 1, 1), date(2025, 3, 31))
BTC, ETH, SPY = (mhb.SERIES[k].series_id for k in ("BTCUSDT", "ETHUSDT", "SPY"))


def _ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _days(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def _crypto_closes(base: int) -> dict[date, str]:
    return {d: f"{base + i}.{i % 100:02d}" for i, d in enumerate(_days(date(2024, 11, 1), date(2025, 6, 30)))}


def _spy_closes() -> dict[date, float]:
    out = {}
    for i, d in enumerate(_days(date(2024, 11, 1), date(2025, 6, 30))):
        out[d] = 580.0 + (i % 37) * 0.25 + 0.00390625  # exactly representable binary fractions
    return out


class _Resp:
    def __init__(self, status: int, body: object) -> None:
        self.status_code = status
        self.text = body if isinstance(body, str) else json.dumps(body)


class FakeBinanceSession:
    def __init__(self) -> None:
        self.closes = {"BTCUSDT": _crypto_closes(90000), "ETHUSDT": _crypto_closes(3000)}
        self.hook = None  # (symbol, start, end) -> _Resp | None; may raise
        self.extra: dict[tuple[str, date], list] = {}
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        sym = params["symbol"]
        start = datetime.fromtimestamp(params["startTime"] / 1000, timezone.utc).date()
        end = datetime.fromtimestamp(params["endTime"] / 1000, timezone.utc).date()
        if self.hook is not None:
            forced = self.hook(sym, start, end)
            if forced is not None:
                return forced
        rows = []
        for d in _days(start, end):
            if d in self.closes[sym]:
                c = self.closes[sym][d]
                rows.append([_ms(d), c, str(float(c) + 50), str(float(c) - 50), c, "12.5",
                             _ms(d) + DAY_MS - 1, "1000000.0", 777, "6", "500000", "0"])
        rows.extend(self.extra.get((sym, start), []))
        return _Resp(200, rows)


class FakeYahoo:
    def __init__(self) -> None:
        self.closes = _spy_closes()
        self.hook = None
        self.phantom: dict[date, float] = {date(2025, 1, 9): 587.5}  # Yahoo shows the Carter closure

    def download(self, ticker, start=None, end=None, **kwargs):
        s, e = date.fromisoformat(start), date.fromisoformat(end)
        if self.hook is not None:
            self.hook(ticker, s, e)
        rows = {d: v for d, v in self.closes.items() if s <= d < e and mhb.is_nyse_session(d)}
        rows.update({d: v for d, v in self.phantom.items() if s <= d < e})
        rows = dict(sorted(rows.items()))
        idx = pd.DatetimeIndex([pd.Timestamp(d) for d in rows])
        frame = pd.DataFrame({"Open": list(rows.values()), "High": [v + 2 for v in rows.values()],
                              "Low": [v - 2 for v in rows.values()], "Close": list(rows.values()),
                              "Adj Close": [v - 7.5 for v in rows.values()],
                              "Volume": [5.0e7] * len(rows)}, index=idx)
        frame.columns = pd.MultiIndex.from_product([frame.columns, [ticker]])
        return frame


SCHEMA_SQL = Path(__file__).resolve().parents[1] / "schema.sql"
BINANCE_LAST_PULL = datetime(2026, 9, 22, 8, 53, 12, tzinfo=timezone.utc)  # R1: griddb binance.last_pull_at


def _schema_statement(pattern: str) -> str:
    """One statement from schema.sql, so the fixture tracks the declared DDL and its CHECKs."""
    body = SCHEMA_SQL.read_text(encoding="utf-8")
    match = re.search(pattern + r".*?;", body, re.S)
    assert match, pattern
    return match.group(0).replace("IF NOT EXISTS ", "")


@pytest.fixture
def world():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("history backfill proof requires a local disposable *test* database")
    schema = "history_backfill_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.execute(text(_schema_statement(r"CREATE TABLE IF NOT EXISTS source_catalog \(")))
            conn.execute(text(_schema_statement(r"CREATE TABLE IF NOT EXISTS raw_series \(")))
            conn.execute(text(_schema_statement(r"CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_series_composite")))
            conn.execute(text(_schema_statement(r"CREATE INDEX IF NOT EXISTS idx_raw_series_series_obs")))
            conn.execute(text("""
                CREATE TABLE feature_registry (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, family TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE resolved_series (
                    id BIGSERIAL PRIMARY KEY,
                    feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
                    obs_date DATE NOT NULL, release_date DATE NOT NULL,
                    vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL,
                    source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
                    conflict_flag BOOLEAN NOT NULL DEFAULT FALSE, conflict_detail TEXT,
                    UNIQUE (feature_id, obs_date, vintage_date))
            """))
            conn.execute(text("""
                INSERT INTO source_catalog (id, name, base_url, cost_tier, latency_class, pit_available,
                                            revision_behavior, trust_score, priority_rank, active, last_pull_at)
                VALUES (2, 'yfinance', 'yahoo', 'FREE', 'EOD', FALSE, 'NEVER', 'HIGH', 2, TRUE, NULL),
                       (184, 'binance', 'https://data.binance.vision', 'FREE', 'EOD', FALSE, 'NEVER', 'HIGH', 10,
                        TRUE, :binance_last),
                       (524, 'TIINGO', 'tiingo', 'PAID', 'EOD', TRUE, 'RARE', 'HIGH', 6, TRUE, NULL)
            """), {"binance_last": BINANCE_LAST_PULL})
            conn.execute(text("SELECT setval(pg_get_serial_sequence('source_catalog', 'id'), 3000)"))
            conn.execute(text("""
                INSERT INTO feature_registry (id, name, family) VALUES
                    (677, 'btc_full', 'crypto'), (675, 'eth_full', 'crypto'), (2791, 'spy_full', 'equity')
            """))
        binance = FakeBinanceSession()
        yahoo = FakeYahoo()
        providers = {
            "BTCUSDT": mhb.BinanceKlinesProvider(binance, sleep=lambda s: None, max_attempts=1),
            "ETHUSDT": mhb.BinanceKlinesProvider(binance, sleep=lambda s: None, max_attempts=1),
            "SPY": mhb.YahooDailyProvider(yahoo.download, sleep=lambda s: None),
        }
        _seed_existing_identities(engine, binance, yahoo)
        yield {"engine": engine, "binance": binance, "yahoo": yahoo, "providers": providers}
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _insert(conn, series_id: str, source_id: int, obs: date, pulled: datetime, value: float,
            payload: dict | None = None) -> None:
    conn.execute(text("""
        INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
        VALUES (:sid, :src, :od, :ts, :val, CAST(:p AS jsonb), 'SUCCESS')
    """), {"sid": series_id, "src": source_id, "od": obs, "ts": pulled, "val": value,
           "p": None if payload is None else json.dumps(payload)})


def _seed_existing_identities(engine: Engine, binance: FakeBinanceSession, yahoo: FakeYahoo) -> None:
    """Shape of griddb per R1b: legacy BINANCE archive ends 2024-12-31; SPY has
    post-day-end captures for some sessions and same-day-only captures for others."""
    legacy_pull = datetime(2026, 3, 26, 5, 1, 17, tzinfo=timezone.utc)
    with engine.begin() as conn:
        for sym, sid in (("BTCUSDT", "BINANCE:BTCUSDT:close"), ("ETHUSDT", "BINANCE:ETHUSDT:close")):
            for d in _days(date(2024, 12, 1), date(2024, 12, 31)):
                _insert(conn, sid, 184, d, legacy_pull, float(binance.closes[sym][d]))
        for d in _days(date(2025, 1, 1), date(2025, 3, 31)):
            if not mhb.is_nyse_session(d):
                continue
            v = yahoo.closes[d]
            same_day = datetime.combine(d, time(13, 33), timezone.utc)
            _insert(conn, "YF:SPY:close", 2, d, same_day, v - 3.0)  # intraday provisional value
            if d.month == 1 or (d.month == 3 and d.day % 2 == 0):
                _insert(conn, "YF:SPY:close", 2, d, datetime.combine(d + timedelta(days=1), time(13, 31),
                                                                   timezone.utc), v)
            # A second provider under the same series id never counts as yfinance coverage.
            _insert(conn, "YF:SPY:close", 524, d, datetime.combine(d + timedelta(days=2), time(4), timezone.utc), v)


def _plan(w, keys=("BTCUSDT", "ETHUSDT", "SPY"), **kw):
    return mhb.build_plan(w["engine"], list(keys), *WINDOW, providers=w["providers"], **kw)


def _apply(w, plan, **kw):
    kw.setdefault("run_id", "hbf-test")
    kw.setdefault("max_rows", plan["total_targets"])
    return mhb.apply_plan(w["engine"], plan, expect_digest=plan["plan_digest"], providers=w["providers"], **kw)


def _ns_rows(engine: Engine, series_id: str | None = None):
    sql = "SELECT id, series_id, obs_date, value, pull_timestamp, raw_payload FROM raw_series " \
          "WHERE series_id IN (:a, :b, :c)"
    params = {"a": BTC, "b": ETH, "c": SPY}
    with engine.connect() as conn:
        rows = conn.execute(text(sql + " ORDER BY series_id, obs_date, id"), params).fetchall()
    return [r for r in rows if series_id is None or r[1] == series_id]


def _count_all(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT count(*) FROM raw_series")).scalar()


def _spy_gap_sessions() -> list[date]:
    return [d for d in _days(*WINDOW) if mhb.is_nyse_session(d)
            and not (d.month == 1 or (d.month == 3 and d.day % 2 == 0))]


# --------------------------------------------------------------------------

def test_plan_is_read_only_and_targets_only_real_gaps(world):
    with world["engine"].begin() as conn:  # griddb has YF:SPY:close rows on non-session dates (R1b)
        _insert(conn, "YF:SPY:close", 2, date(2025, 1, 9), datetime(2026, 3, 26, 4, 34, tzinfo=timezone.utc), 587.5)
    before = _count_all(world["engine"])
    plan = _plan(world)
    assert _count_all(world["engine"]) == before
    assert plan["status"] == "READY", plan["blocking_anomalies"]
    btc, eth, spy = (plan["series"][s] for s in (BTC, ETH, SPY))
    assert len(btc["targets"]) == 90 and len(eth["targets"]) == 90
    assert btc["validation"] == {"compared": 31, "agreeing": 31, "max_abs_diff_agreeing": 0.0,  # legacy Dec-2024
                                 "basis_conflicts": 0, "unvalidated_coverage": 0}
    assert [t["obs_date"] for t in spy["targets"]] == [d.isoformat() for d in _spy_gap_sessions()]
    assert {t["reason"] for t in spy["targets"]} == {"no_final_capture"}
    assert spy["covered_by_existing_final_capture"] == spy["expected_dates"] - len(_spy_gap_sessions())
    assert spy["validation"]["basis_conflicts"] == 0 and spy["validation"]["compared"] > 20
    phantom = [n for n in spy["notes"] if n["kind"] == "unexpected_provider_date"]
    assert [n["obs_date"] for n in phantom] == ["2025-01-09"]
    stray = [n for n in spy["notes"] if n["kind"] == "coverage_on_non_session_date"]
    assert [n["obs_date"] for n in stray] == ["2025-01-09"]
    assert spy["validation"]["unvalidated_coverage"] == 0 and "2025-01-09" not in [t["obs_date"] for t in spy["targets"]]
    assert plan["total_targets"] == 180 + len(_spy_gap_sessions())
    json.dumps(plan)  # serialisable as the approval artifact


def test_apply_writes_exact_scope_with_honest_timestamps_and_verify_passes(world, tmp_path):
    plan = _plan(world)
    ledger = mhb.Ledger(tmp_path / "ledger.jsonl")
    started = datetime.now(timezone.utc)
    result = _apply(world, plan, ledger=ledger)
    finished = datetime.now(timezone.utc)
    assert result.exit_code == mhb.EXIT_OK, result.message
    assert result.inserted == plan["total_targets"]
    rows = _ns_rows(world["engine"])
    assert len(rows) == plan["total_targets"]
    planned = {(sid, t["obs_date"]): float(t["value"]) for sid, s in plan["series"].items() for t in s["targets"]}
    for rid, sid, obs, value, pulled, payload in rows:
        assert planned[(sid, obs.isoformat())] == value
        acquired = datetime.fromisoformat(payload["acquired_at_utc"])
        assert started <= acquired <= pulled <= finished
        assert pulled >= mhb.utc_day_end(obs) + timedelta(days=365)  # never back-dated
        assert payload["backfill_contract"] == mhb.BACKFILL_CONTRACT
        assert payload["run_id"] == "hbf-test" and payload["plan_digest"] == plan["plan_digest"]
        assert not is_completed_canonical_close(payload, obs, pulled)
        assert not is_policy_capture(payload, obs, pulled)
    report = mhb.verify_plan(world["engine"], plan, "hbf-test")
    assert report["ok"], report
    with world["engine"].connect() as conn:
        sources = {r[0]: r[1:] for r in conn.execute(text("""
            SELECT name, id, active, pit_available, priority_rank, last_pull_at FROM source_catalog
        """)).fetchall()}
        by_source = dict(conn.execute(text("""
            SELECT source_id, max(pull_timestamp) FROM raw_series
            WHERE series_id IN (:a, :b, :c) GROUP BY source_id
        """), {"a": BTC, "b": ETH, "c": SPY}).fetchall())
    for name in ("binance_history_backfill_v1", "yfinance_history_backfill_v1"):
        sid, active, pit, rank, last_pull = sources[name]
        assert (active, pit, rank) == (False, False, 99)
        assert last_pull == by_source[sid]  # heartbeat = newest row actually written, not "now"
    assert set(by_source) == {sources["binance_history_backfill_v1"][0], sources["yfinance_history_backfill_v1"][0]}
    assert sources["binance"][4] == BINANCE_LAST_PULL  # the forward source heartbeat is untouched
    assert sources["yfinance"][4] is None
    events = [json.loads(line)["event"] for line in (tmp_path / "ledger.jsonl").read_text().splitlines()]
    assert events[0] == "apply_start" and events[-1] == "apply_done"
    months_with_targets = sum(len({t["obs_date"][:7] for t in s["targets"]}) for s in plan["series"].values())
    assert months_with_targets == 3 + 3 + 2  # BTC, ETH Jan-Mar; SPY January is fully covered
    assert events.count("chunk_committed") == months_with_targets


def test_reapply_is_idempotent(world):
    plan = _plan(world)
    assert _apply(world, plan).inserted == plan["total_targets"]
    again = _apply(world, plan)
    assert again.exit_code == mhb.EXIT_OK
    assert again.inserted == 0 and again.skipped_present == plan["total_targets"]
    assert len(_ns_rows(world["engine"])) == plan["total_targets"]
    replanned = _plan(world)  # a fresh plan now finds nothing to do
    assert replanned["total_targets"] == 0 and replanned["status"] == "READY"


def test_real_resolver_never_promotes_namespace_rows(world):
    plan = _plan(world)
    assert _apply(world, plan).exit_code == mhb.EXIT_OK
    engine = world["engine"]
    control_obs = date(2025, 2, 1)
    control_pull = datetime.now(timezone.utc) - timedelta(minutes=5)
    evidence = {"close_contract": "binance_utc_close_v1", "endpoint_host": "data-api.binance.vision",
                "interval": "1d", "time_zone": "UTC", "open_time_ms": _ms(control_obs),
                "close_time_ms": _ms(control_obs) + DAY_MS - 1, "captured_at_utc": control_pull.isoformat()}
    with engine.begin() as conn:
        _insert(conn, "binance.BTCUSDT.close", 184, control_obs, control_pull, 12345.0, evidence)
    entity_map = EntityMap(engine)
    for sid in (BTC, ETH, SPY):
        assert entity_map.get_feature_id(sid) is None
    assert entity_map.get_feature_id("binance.BTCUSDT.close") == 677
    summary = Resolver(engine).resolve_pending(workers=1, since=datetime.now(timezone.utc) - timedelta(hours=1))
    assert summary["errors"] == 0
    with engine.connect() as conn:
        resolved = conn.execute(text("SELECT feature_id, obs_date, value FROM resolved_series")).fetchall()
    # Only the marked forward-contract control row resolves; none of the
    # 180+ namespace rows (same features, same dates) do.
    assert [(r[0], r[1], r[2]) for r in resolved] == [(677, control_obs, 12345.0)]


def test_provider_failure_mid_run_keeps_committed_chunks_and_resume_completes(world, tmp_path):
    plan = _plan(world)
    world["binance"].hook = lambda sym, s, e: _Resp(500, "") if (sym, s) == ("BTCUSDT", date(2025, 2, 1)) else None
    first = _apply(world, plan, ledger=mhb.Ledger(tmp_path / "l.jsonl"))
    assert first.exit_code == mhb.EXIT_INCOMPLETE
    rows = _ns_rows(world["engine"])
    assert {r[2].month for r in rows} == {1} and {r[1] for r in rows} == {BTC}  # only BTC January committed
    assert len(rows) == 31
    world["binance"].hook = None
    second = _apply(world, plan, ledger=mhb.Ledger(tmp_path / "l.jsonl"))
    assert second.exit_code == mhb.EXIT_OK
    assert second.skipped_present == 31 and second.inserted == plan["total_targets"] - 31
    assert mhb.verify_plan(world["engine"], plan)["ok"]
    events = [json.loads(x)["event"] for x in (tmp_path / "l.jsonl").read_text().splitlines()]
    assert "chunk_failed" in events and events[-1] == "apply_done"


def test_interruption_releases_lock_and_resume_completes(world):
    plan = _plan(world)

    def interrupt(sym, s, e):
        if (sym, s) == ("ETHUSDT", date(2025, 2, 1)):
            raise KeyboardInterrupt

    world["binance"].hook = interrupt
    with pytest.raises(KeyboardInterrupt):
        _apply(world, plan)
    with world["engine"].connect() as conn:
        assert conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": mhb.ADVISORY_LOCK_KEY}).scalar()
        conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": mhb.ADVISORY_LOCK_KEY})
    committed = _ns_rows(world["engine"])
    assert len(committed) == 90 + 31  # BTC fully, ETH January
    world["binance"].hook = None
    assert _apply(world, plan).exit_code == mhb.EXIT_OK
    assert len(_ns_rows(world["engine"])) == plan["total_targets"]
    assert mhb.verify_plan(world["engine"], plan)["ok"]


def test_database_failure_mid_chunk_rolls_back_the_whole_chunk(world):
    plan = _plan(world)
    seen = {"n": 0}

    def fail_third_spy_insert(series_id, obs):
        if series_id == SPY and obs.month == 2:
            seen["n"] += 1
            if seen["n"] == 3:
                raise RuntimeError("simulated connection loss")

    result = _apply(world, plan, insert_hook=fail_third_spy_insert)
    assert result.exit_code == mhb.EXIT_INCOMPLETE
    spy_rows = _ns_rows(world["engine"], SPY)
    assert all(r[2].month != 2 for r in spy_rows)  # none of February's inserts survived
    assert _apply(world, plan).exit_code == mhb.EXIT_OK
    assert mhb.verify_plan(world["engine"], plan)["ok"]


def test_revision_stops_by_default_and_is_append_only_when_recorded(world):
    plan = _plan(world)
    target = date(2025, 2, 10)
    with world["engine"].begin() as conn:
        _insert(conn, BTC, 184, target, datetime.now(timezone.utc), 1.0,
                {"backfill_contract": mhb.BACKFILL_CONTRACT, "run_id": "older"})
    first = _apply(world, plan)
    assert first.exit_code == mhb.EXIT_REVISION
    btc = _ns_rows(world["engine"], BTC)
    assert {r[2].month for r in btc} == {1, 2} and len([r for r in btc if r[2].month == 2]) == 1  # Jan + the old row
    second = _apply(world, plan, record_revisions=True, run_id="hbf-rev")
    assert second.exit_code == mhb.EXIT_OK and second.revisions_recorded == 1
    rows = [r for r in _ns_rows(world["engine"], BTC) if r[2] == target]
    assert [r[3] for r in rows] == [1.0, float(world["binance"].closes["BTCUSDT"][target])]  # old row untouched
    assert rows[1][5]["revision_of_raw_ids"] == [rows[0][0]]
    # The provider later revises a bar that is already stored: a new plan
    # detects it and blocks instead of silently adding a second value.
    revised = date(2025, 3, 3)
    world["binance"].closes["BTCUSDT"][revised] = f"{float(world['binance'].closes['BTCUSDT'][revised]) + 12.34:.2f}"
    blocked = _plan(world, keys=("BTCUSDT",))
    assert blocked["status"] == "BLOCKED" and "namespace_revision" in blocked["blocking_anomalies"]


def test_plan_drift_stops_before_writing_the_drifted_chunk(world):
    plan = _plan(world, keys=("BTCUSDT",))
    drifted = date(2025, 3, 5)
    # A valid bar whose close moved after approval (not a malformed or missing bar).
    world["binance"].closes["BTCUSDT"][drifted] = f"{float(world['binance'].closes['BTCUSDT'][drifted]) + 7.77:.2f}"
    result = _apply(world, plan)
    assert result.exit_code == mhb.EXIT_PLAN_DRIFT
    months = {r[2].month for r in _ns_rows(world["engine"], BTC)}
    assert months == {1, 2}


def test_duplicate_bars_dedupe_or_block(world):
    closes = world["binance"].closes["BTCUSDT"]
    d = date(2025, 1, 15)
    same = [_ms(d), closes[d], str(float(closes[d]) + 50), str(float(closes[d]) - 50), closes[d], "12.5",
            _ms(d) + DAY_MS - 1, "1.0", 1, "1", "1", "0"]
    world["binance"].extra[("BTCUSDT", date(2025, 1, 1))] = [same]
    plan = _plan(world, keys=("BTCUSDT",))
    assert plan["status"] == "READY" and len(plan["series"][BTC]["targets"]) == 90
    conflicting = list(same)
    conflicting[4] = conflicting[1] = "99999.99"
    conflicting[2] = "100049.99"
    world["binance"].extra[("BTCUSDT", date(2025, 1, 1))] = [conflicting]
    blocked = _plan(world, keys=("BTCUSDT",))
    assert blocked["status"] == "BLOCKED" and "provider_duplicate_conflict" in blocked["blocking_anomalies"]
    with pytest.raises(mhb.BackfillRefused, match="not READY"):
        _apply(world, blocked)
    assert _ns_rows(world["engine"]) == []


def test_unfinished_and_out_of_window_bars_are_never_written(world):
    today = datetime.now(timezone.utc).date()
    open_bar = [_ms(today), "1", "2", "0.5", "1.5", "1", _ms(today) + DAY_MS - 1, "1", 1, "1", "1", "0"]
    stray = [_ms(date(2025, 4, 2)), "1", "2", "0.5", "1.5", "1", _ms(date(2025, 4, 2)) + DAY_MS - 1,
             "1", 1, "1", "1", "0"]
    world["binance"].extra[("BTCUSDT", date(2025, 3, 1))] = [open_bar, stray]
    plan = _plan(world, keys=("BTCUSDT",))
    kinds = sorted(n["kind"] for n in plan["series"][BTC]["notes"])
    assert kinds == ["out_of_window_bar_ignored", "unfinished_bar_rejected"]
    assert _apply(world, plan).exit_code == mhb.EXIT_OK
    dates = [r[2] for r in _ns_rows(world["engine"], BTC)]
    assert min(dates) == WINDOW[0] and max(dates) == WINDOW[1] and len(dates) == 90


def test_single_writer_lock(world):
    plan = _plan(world, keys=("ETHUSDT",))
    holder = world["engine"].connect()
    try:
        assert holder.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": mhb.ADVISORY_LOCK_KEY}).scalar()
        result = _apply(world, plan)
        assert result.exit_code == mhb.EXIT_LOCKED
        assert _ns_rows(world["engine"]) == []
    finally:
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": mhb.ADVISORY_LOCK_KEY})
        holder.close()
    assert _apply(world, plan).exit_code == mhb.EXIT_OK


def test_guards_refuse_before_any_write(world):
    plan = _plan(world, keys=("BTCUSDT",))
    before = _count_all(world["engine"])
    with pytest.raises(mhb.BackfillRefused, match="max-rows"):
        _apply(world, plan, max_rows=plan["total_targets"] - 1)
    tampered = json.loads(json.dumps(plan))
    tampered["series"][BTC]["targets"][5]["value_text"] = "1.00"
    with pytest.raises(mhb.BackfillRefused, match="digest"):
        mhb.apply_plan(world["engine"], tampered, expect_digest=plan["plan_digest"], run_id="x",
                       max_rows=10_000, providers=world["providers"])
    with pytest.raises(mhb.BackfillRefused, match="digest"):
        mhb.apply_plan(world["engine"], plan, expect_digest="0" * 64, run_id="x",
                       max_rows=10_000, providers=world["providers"])
    stale_now = datetime.fromisoformat(plan["created_at_utc"]) + timedelta(hours=73)
    with pytest.raises(mhb.BackfillRefused, match="older than"):
        _apply(world, plan, now_utc=stale_now)
    with pytest.raises(mhb.BackfillRefused, match="forward-owned"):
        mhb.build_plan(world["engine"], ["BTCUSDT"], date(2026, 9, 1), date(2026, 9, 23),
                       providers=world["providers"])
    assert _count_all(world["engine"]) == before


def test_basis_conflicts_block_by_default_and_are_only_filled_or_accepted_explicitly(world):
    yahoo = world["yahoo"]
    with world["engine"].begin() as conn:
        # 2025-01-15: the only post-day-end capture is a dividend-adjusted value
        # (what scripts/fill_missing_features.py writes with auto_adjust=True).
        conn.execute(text("""
            UPDATE raw_series SET value = value * 0.985
            WHERE series_id = 'YF:SPY:close' AND source_id = 2 AND obs_date = DATE '2025-01-15'
              AND pull_timestamp >= TIMESTAMPTZ '2025-01-16 00:00+00'
        """))
        # 2025-01-16: an EARLIER adjusted post-day-end capture sits beside the raw one;
        # one agreeing capture is enough, whichever came first.
        _insert(conn, "YF:SPY:close", 2, date(2025, 1, 16), datetime(2025, 1, 17, 1, tzinfo=timezone.utc),
                yahoo.closes[date(2025, 1, 16)] * 0.985)
        conn.execute(text("""
            UPDATE raw_series SET value = value + 1
            WHERE series_id = 'BINANCE:ETHUSDT:close' AND obs_date = DATE '2024-12-20'
        """))
    plan = _plan(world, keys=("ETHUSDT", "SPY"))
    assert plan["status"] == "BLOCKED"
    anomalies = {(a["kind"], a["obs_date"]): a for s in plan["series"].values() for a in s["anomalies"]}
    assert set(anomalies) == {("validation_mismatch", "2025-01-15"), ("validation_mismatch", "2024-12-20")}
    assert anomalies[("validation_mismatch", "2025-01-15")]["nearest_ratio_to_provider"] == pytest.approx(0.985)
    assert "2025-01-15" not in [t["obs_date"] for t in plan["series"][SPY]["targets"]]

    filled = _plan(world, keys=("SPY",), fill_basis_conflicts=True)
    assert filled["status"] == "READY"
    reasons = {t["obs_date"]: t["reason"] for t in filled["series"][SPY]["targets"]}
    assert reasons["2025-01-15"] == "basis_conflict" and "2025-01-16" not in reasons
    both = _plan(world, keys=("ETHUSDT", "SPY"), fill_basis_conflicts=True, allow_validation_mismatch=True)
    assert both["status"] == "READY"  # the out-of-window ETH overlap conflict needs the explicit acceptance
    assert both["plan_digest"] != filled["plan_digest"]
    allowed = _plan(world, keys=("ETHUSDT", "SPY"), allow_validation_mismatch=True)
    assert allowed["status"] == "READY" and "2025-01-15" not in [t["obs_date"] for t in allowed["series"][SPY]["targets"]]
    assert _apply(world, filled).exit_code == mhb.EXIT_OK
    with world["engine"].connect() as conn:  # the disagreeing capture itself is never modified
        kept = conn.execute(text("""
            SELECT count(*) FROM raw_series WHERE series_id = 'YF:SPY:close' AND obs_date = DATE '2025-01-15'
        """)).scalar()
    assert kept == 3


def test_altered_dedicated_source_row_is_refused(world):
    plan = _plan(world, keys=("BTCUSDT",))
    assert plan["status"] == "READY"
    assert plan["write_sources"] == {"binance_history_backfill_v1": mhb.WRITE_SOURCE_SPECS["binance_history_backfill_v1"]}
    with world["engine"].begin() as conn:
        conn.execute(text("""
            INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available,
                                        revision_behavior, trust_score, priority_rank, active)
            VALUES ('binance_history_backfill_v1', 'x', 'FREE', 'EOD', FALSE, 'NEVER', 'HIGH', 99, TRUE)
        """))
    with pytest.raises(mhb.BackfillRefused, match="different attributes"):
        _apply(world, plan)
    assert _ns_rows(world["engine"]) == []
    replanned = _plan(world, keys=("BTCUSDT",))
    assert replanned["status"] == "BLOCKED" and "write_source_attribute_mismatch" in replanned["blocking_anomalies"]


def test_existing_final_capture_is_coverage_but_same_day_capture_is_not(world):
    with world["engine"].begin() as conn:
        for d in _days(date(2025, 1, 1), date(2025, 1, 5)):  # archive-style rows pulled long after the day
            _insert(conn, "BINANCE:BTCUSDT:close", 184, d, datetime(2026, 3, 26, tzinfo=timezone.utc),
                    float(world["binance"].closes["BTCUSDT"][d]))
        provisional = datetime.combine(date(2025, 1, 6), time(15), timezone.utc)  # same UTC day
        _insert(conn, "binance.BTCUSDT.close", 184, date(2025, 1, 6), provisional, 1.0)
    plan = _plan(world, keys=("BTCUSDT",))
    targets = [t["obs_date"] for t in plan["series"][BTC]["targets"]]
    assert targets[0] == "2025-01-06" and len(targets) == 85
    assert plan["series"][BTC]["covered_by_existing_final_capture"] == 5
