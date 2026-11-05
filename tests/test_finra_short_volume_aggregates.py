"""FINRA daily short-volume aggregates: compute, write, backfill, register.

The per-symbol ``finra:short_volume:<SYM>`` series (~12k a day) have no
feature mapping and no reader outside ingestion.
``ingestion/altdata/finra_short_volume_agg.py`` writes the market-wide and
SPY/QQQ/IWM short-volume ratios from the same file, under
``finra:short_volume_agg:``, without touching the sha256-pinned producer. These tests reuse the in-memory FakeEngine from
``tests/test_source_finra_short_volume.py``: no database, no network.
"""
from __future__ import annotations

import importlib
import json
import re
from datetime import date
from pathlib import Path

import pytest
import requests

from ingestion.altdata.finra_short_volume import FINRAShortVolumePuller, parse_daily_short_volume_file
from ingestion.altdata.finra_short_volume_agg import (
    AGG_MARKET_RATIO,
    FINRAShortVolumeAggPuller,
    compute_daily_aggregates,
)
from normalization.entity_map import SEED_MAPPINGS
from tests.test_source_finra_short_volume import FakeEngine, _load

REPO = Path(__file__).resolve().parents[1]
D = date(2026, 9, 16)

# good.txt sums, over all 8 facility rows:
SHORT_ALL = 123456 + 54321 + 10000 + 98765 + 20000 + 555555 + 111111 + 30000
TOTAL_ALL = 500000 + 150000 + 60000 + 400000 + 90000 + 2000000 + 300000 + 120000
SHORT_SPY, TOTAL_SPY = 555555 + 111111 + 30000, 2000000 + 300000 + 120000


def _puller(engine: FakeEngine | None = None) -> FINRAShortVolumeAggPuller:
    """Build the subclass without BasePuller.__init__ (no real DB)."""
    p = FINRAShortVolumeAggPuller.__new__(FINRAShortVolumeAggPuller)
    p.engine = engine or FakeEngine()
    p.source_id = 7
    return p


def _agg_rows(engine: FakeEngine) -> dict[str, dict]:
    return {
        r["series_id"]: r for r in engine.store["rows"]
        if r["series_id"].startswith("finra:short_volume_agg:")
    }


# ── compute_daily_aggregates ──────────────────────────────────────────────


def test_market_and_etf_ratios_sum_every_facility_row() -> None:
    aggs = compute_daily_aggregates(parse_daily_short_volume_file(_load("good.txt"))["rows"])

    assert set(aggs) == {AGG_MARKET_RATIO, "finra:short_volume_agg:spy_ratio"}
    market = aggs[AGG_MARKET_RATIO]
    assert market["obs_date"] == D
    assert market["value"] == pytest.approx(SHORT_ALL / TOTAL_ALL)
    assert market["payload"]["rows"] == 8 and market["payload"]["symbols"] == 3
    spy = aggs["finra:short_volume_agg:spy_ratio"]
    assert spy["value"] == pytest.approx(SHORT_SPY / TOTAL_SPY)
    assert spy["payload"]["rows"] == 3


def test_no_rows_or_zero_volume_writes_nothing_rather_than_zero() -> None:
    assert compute_daily_aggregates([]) == {}
    row = {"date": D, "symbol": "QQQ", "market": "N", "short_volume": 0.0,
           "short_exempt_volume": 0.0, "total_volume": 0.0}
    assert compute_daily_aggregates([row]) == {}


def test_a_file_spanning_two_dates_is_refused() -> None:
    base = {"symbol": "SPY", "market": "N", "short_volume": 1.0,
            "short_exempt_volume": 0.0, "total_volume": 2.0}
    rows = [{**base, "date": D}, {**base, "date": date(2026, 9, 17)}]
    assert compute_daily_aggregates(rows) == {}


def test_aggregate_namespace_never_collides_with_a_symbol_series() -> None:
    p = _puller()
    assert not AGG_MARKET_RATIO.startswith("finra:short_volume:")
    assert p.series_id("AGG") != AGG_MARKET_RATIO


# ── pull ──────────────────────────────────────────────────────────────────


def test_pull_writes_aggregates_once_alongside_symbol_rows() -> None:
    engine = FakeEngine()
    p = _puller(engine)
    p._fetch_raw_text = lambda trade_date, url=None, **kw: _load("good.txt")

    first = p.pull("2026-09-16")
    second = p.pull("2026-09-16")

    assert first["rows_inserted"] == 3 and first["aggregates_inserted"] == 2
    assert second["rows_inserted"] == 0 and second["aggregates_inserted"] == 0
    aggs = _agg_rows(engine)
    assert set(aggs) == {AGG_MARKET_RATIO, "finra:short_volume_agg:spy_ratio"}
    assert aggs[AGG_MARKET_RATIO]["value"] == pytest.approx(SHORT_ALL / TOTAL_ALL)
    assert json.loads(aggs[AGG_MARKET_RATIO]["raw_payload"])["scope"] == "all symbols"
    assert aggs[AGG_MARKET_RATIO]["pull_status"] == "SUCCESS"


def test_pull_dry_run_counts_aggregates_and_writes_nothing() -> None:
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kw: _load("good.txt")

    result = p.pull("2026-09-16", dry_run=True)

    assert result["aggregates_would_insert"] == 2
    assert p.engine.store["rows"] == []


# ── backfill_aggregates ───────────────────────────────────────────────────


def _http_error(status: int) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    return requests.HTTPError(f"HTTP {status}", response=resp)


def test_backfill_writes_only_missing_aggregates_and_never_symbol_rows() -> None:
    engine = FakeEngine()
    p = _puller(engine)

    def _fetch(trade_date, url=None, **kw):
        if trade_date == date(2026, 9, 17):
            raise _http_error(404)  # no file published that day
        return _load("good.txt").replace("20260916", trade_date.strftime("%Y%m%d"))

    p._fetch_raw_text = _fetch
    # 09-16 already has its market ratio (e.g. a later pull); SPY is missing.
    p._insert_raw(conn=engine.begin(), series_id=AGG_MARKET_RATIO, obs_date=D,
                  value=0.5, raw_payload={})

    result = p.backfill_aggregates("2026-09-16", "2026-09-20")  # Wed..Sun

    assert result == {
        "status": "SUCCESS", "aggregates_inserted": 3,
        "dates_written": 2, "dates_complete": 0, "dates_skipped": 1, "dates_failed": 0,
    }
    assert not [r for r in engine.store["rows"] if r["series_id"].startswith("finra:short_volume:")]
    by_key = {(r["series_id"], r["obs_date"]) for r in engine.store["rows"]}
    assert (AGG_MARKET_RATIO, date(2026, 9, 18)) in by_key
    assert ("finra:short_volume_agg:spy_ratio", D) in by_key
    stored_16 = [r for r in engine.store["rows"] if r["series_id"] == AGG_MARKET_RATIO and r["obs_date"] == D]
    assert len(stored_16) == 1 and stored_16[0]["value"] == 0.5  # existing row untouched

    again = p.backfill_aggregates("2026-09-16", "2026-09-18")
    assert again["aggregates_inserted"] == 0 and again["dates_complete"] == 2


def test_backfill_counts_a_real_failure_and_keeps_going() -> None:
    p = _puller()

    def _fetch(trade_date, url=None, **kw):
        if trade_date == D:
            raise _http_error(500)
        return _load("good.txt").replace("20260916", trade_date.strftime("%Y%m%d"))

    p._fetch_raw_text = _fetch
    result = p.backfill_aggregates("2026-09-16", "2026-09-17")

    assert result["status"] == "FAILED"
    assert result["dates_failed"] == 1 and result["dates_written"] == 1


# ── mapping + registration ────────────────────────────────────────────────


@pytest.fixture(scope="module")
def rev():
    return importlib.import_module("migrations.versions.finra_short_volume_agg_20261105")


def test_every_aggregate_series_maps_to_its_registered_feature(rev) -> None:
    for name, raw in rev.REGISTERED_FEATURES.items():
        assert SEED_MAPPINGS[raw] == name
    mapped = {k for k in SEED_MAPPINGS if k.startswith("finra:short_volume_agg:")}
    assert mapped == set(rev.REGISTERED_FEATURES.values())
    assert not [k for k in SEED_MAPPINGS if k.startswith("finra:short_volume:")]


def test_puller_writes_exactly_the_registered_series(rev) -> None:
    aggs = compute_daily_aggregates([
        {"date": D, "symbol": s, "market": "N", "short_volume": 1.0,
         "short_exempt_volume": 0.0, "total_volume": 2.0}
        for s in ("SPY", "QQQ", "IWM", "AAPL")
    ])
    assert set(aggs) == set(rev.REGISTERED_FEATURES.values())


def test_migration_rows_and_graph(rev) -> None:
    names = re.findall(r"\(\s*'(finra_short_volume_ratio_[a-z]+)'\s*,\s*'alternative'", rev.FINRA_RATIO_FEATURES)
    assert names == list(rev.REGISTERED_FEATURES)
    assert rev.down_revision == "ghost_features_20261105"
    sql = " ".join(rev.FINRA_RATIO_FEATURES.split())
    assert "CROSS JOIN LATERAL" in sql and "r.pull_status = 'SUCCESS'" in sql
    assert sql.endswith("ON CONFLICT (name) DO NOTHING")
    children = [
        p.name for p in (REPO / "migrations" / "versions").glob("*.py")
        if re.search(r'^down_revision[^=]*=\s*"ghost_features_20261105"', p.read_text(), re.M)
    ]
    assert children == ["finra_short_volume_agg_20261105.py"]
    with pytest.raises(RuntimeError, match="refused"):
        rev.downgrade()


# ── scheduler wiring + producer pin ───────────────────────────────────────


def test_scheduler_runs_the_aggregating_subclass() -> None:
    from ingestion.smart_scheduler import PULLER_REGISTRY

    (entry,) = [e for e in PULLER_REGISTRY if e["name"] == "finra_short_volume"]
    assert (entry["mod"], entry["cls"], entry["method"]) == (
        "ingestion.altdata.finra_short_volume_agg", "FINRAShortVolumeAggPuller", "pull_recent")
    assert issubclass(FINRAShortVolumeAggPuller, FINRAShortVolumePuller)
    assert FINRAShortVolumeAggPuller.SOURCE_NAME == FINRAShortVolumePuller.SOURCE_NAME


def test_pinned_producer_is_byte_for_byte_unchanged() -> None:
    import hashlib

    from ingestion import smart_scheduler

    producer = REPO / "ingestion" / "altdata" / "finra_short_volume.py"
    assert hashlib.sha256(producer.read_bytes()).hexdigest() == smart_scheduler._FINRA_SELECTED_PINS["producer"]


def test_aggregate_write_failure_keeps_the_symbol_result() -> None:
    p = _puller()
    p._fetch_raw_text = lambda trade_date, url=None, **kw: _load("good.txt")

    def _boom(*a, **k):
        raise RuntimeError("db went away")

    p._insert_missing_aggregates = _boom
    result = p.pull("2026-09-16")

    assert result["status"] == "SUCCESS" and result["rows_inserted"] == 3
    assert result["aggregates_inserted"] == 0 and "db went away" in result["aggregates_error"]
    assert "_fetch_raw_text" in p.__dict__  # the test's own override is put back
