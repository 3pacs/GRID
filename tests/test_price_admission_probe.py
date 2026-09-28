"""GD4 price-admission basis probe: synthetic data only (SQLite; no real prices)."""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from datetime import date, datetime, timedelta
from fractions import Fraction
from pathlib import Path

import pytest
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine

from analysis import panel_insider_density as v1
from analysis import price_admission_probe as gd4

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TIINGO, YFINANCE, TD_SPLITS, YF_ADJ = 524, 2, 1035, 4678
PULL = datetime(2026, 4, 7, 10, 0)
LO, HI = gd4.DEFAULT_WINDOW


def _sessions(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _series(start=date(2019, 1, 2), n=60, base=41.237, split_at=None, split=2.0, dividends=(), drift=0.0137):
    """Synthetic raw close and adjusted close (adj = close x factor for later actions)."""
    days = _sessions(start, n)
    close = {}
    for i, d in enumerate(days):
        c = base + drift * ((i * 7) % 11)
        if split_at is not None and i >= split_at:
            c /= split
        close[d] = round(c, 4)
    factor = {}
    f = 1.0
    for i in range(len(days) - 1, -1, -1):
        factor[days[i]] = f
        if split_at is not None and i == split_at:
            f /= split
        if i in dividends:
            f *= 1 - 0.37 / close[days[i - 1]]
    adj = {d: close[d] * factor[d] for d in days}
    return close, adj


def _rows(values: dict, pulls=(PULL,)) -> list[gd4.Row]:
    return [gd4.Row(d, v, p) for p in pulls for d, v in values.items()]


# --- pure checks ------------------------------------------------------------------------------------


def test_window_refuses_the_holdout_period():
    gd4.check_window(LO, HI)
    assert HI == date(2019, 12, 31) and LO == date(2011, 11, 2)
    for bad in (date(2020, 1, 1), date(2024, 6, 3)):
        with pytest.raises(gd4.ProbeRefused, match="holdout"):
            gd4.check_window(LO, bad)
    with pytest.raises(gd4.ProbeRefused):
        gd4.check_window(date(2019, 1, 2), date(2018, 1, 2))


def test_refused_sources_cover_the_preregistered_list_and_the_yfinance_family():
    assert v1.REFUSED_PRICE_SOURCES <= gd4.REFUSED_SOURCES
    for name in ("yfinance", "YF", "KAGGLE_BULK", "yfinance_adj", "yfinance_adjusted_extended", "", "Kaggle_x"):
        assert gd4.is_refused_source(name), name
    for name in ("TIINGO", "tiingo", "TWELVEDATA"):
        assert not gd4.is_refused_source(name)


def test_multi_valued_dates_are_found_and_float_roundtrip_is_not():
    d1, d2 = date(2019, 3, 1), date(2019, 3, 4)
    rows = [gd4.Row(d1, 10.0, PULL), gd4.Row(d1, 10.0 * (1 + 1e-12), PULL + timedelta(days=1)),
            gd4.Row(d2, 11.0, PULL), gd4.Row(d2, 11.5, PULL + timedelta(days=1))]
    values, multi = gd4.collapse_vintages(rows)
    assert set(values) == {d1, d2}
    assert multi == ["2019-03-04"]


def test_split_ratios_and_labels():
    assert gd4.nice_ratio(0.5) == Fraction(1, 2) and gd4.split_label(Fraction(1, 2)) == "2-for-1"
    assert gd4.nice_ratio(10.0) == Fraction(10, 1) and gd4.split_label(Fraction(10, 1)) == "1-for-10"
    assert gd4.split_label(gd4.nice_ratio(2 / 3 * 1.003)) == "3-for-2"
    assert gd4.nice_ratio(0.985) is None  # a dividend-sized step is never a split
    assert gd4.nice_ratio(0.61) is None  # not near any small-integer ratio


def test_factor_steps_classify_splits_distributions_and_anomalies():
    close, adj = _series(split_at=20, split=2.0, dividends=(10, 40))
    steps = gd4.factor_steps(close, adj)
    assert [s["label"] for s in steps["implied_splits"]] == ["2-for-1"]
    assert steps["distribution_steps"] == 2 and steps["anomalous_steps"] == []
    days = sorted(close)
    broken = dict(adj)
    for d in days[30:]:
        broken[d] = adj[d] * 1.45  # an unexplained basis break (step 0.69: no split ratio)
    assert gd4.factor_steps(close, broken)["anomalous_steps"][0]["date"] == days[30].isoformat()
    upward = dict(adj)
    for d in days[45:]:
        upward[d] = adj[d] * 0.97  # factor rising forward in time without a reverse split
    assert gd4.factor_steps(close, upward)["anomalous_steps"]


def _assess(close, adj, *, statuses=None, td=(), close_pulls=(PULL,), adj_rows=None):
    return gd4.assess_ticker(
        "AAA", "TIINGO", _rows(close, close_pulls), adj_rows if adj_rows is not None else _rows(adj),
        statuses=statuses or {"YF:AAA:close": {"SUCCESS": len(close)}}, other_sources=["TIINGO", "yfinance"],
        td_splits=td, calendar=sorted(close))


def test_assess_admits_a_clean_series_and_reports_no_prices():
    close, adj = _series(split_at=20, dividends=(10,))
    rec = _assess(close, adj, close_pulls=(PULL, PULL + timedelta(days=3)))
    assert rec["admitted"] and rec["reasons"] == []
    assert all(v is True for v in rec["checks"].values())
    assert rec["other_sources_on_series_id"] == ["yfinance"]
    assert rec["pull_batches"]["count"] == 2 and rec["pull_batches"]["in_april_2026"] == 2
    assert rec["coverage"]["missing_sessions"] == 0
    dumped = json.dumps(rec)
    for v in list(close.values())[:10] + list(adj.values())[:10]:
        assert repr(v) not in dumped and f"{v:.4f}" not in dumped


@pytest.mark.parametrize("case, reason", [
    ("multi", "multi_valued_dates"), ("quarantined", "quarantined_batch_rows"),
    ("no_adj", "no_adjusted_series"), ("td_unmatched", "split_inconsistent"), ("break", "split_inconsistent"),
    ("nothing", "no_source_series"),
])
def test_assess_refuses_each_failed_rule(case, reason):
    close, adj = _series(split_at=20, dividends=(10,))
    kw = {}
    if case == "multi":
        d = sorted(adj)[5]
        kw["adj_rows"] = _rows(adj) + [gd4.Row(d, adj[d] * 1.02, PULL + timedelta(days=1))]
    elif case == "quarantined":
        kw["statuses"] = {"YF:AAA:close": {"SUCCESS": 60, "QUARANTINED": 3}}
    elif case == "no_adj":
        kw["adj_rows"] = []
    elif case == "td_unmatched":
        kw["td"] = [(sorted(close)[40], 0.25)]
    elif case == "break":
        adj = {d: v * (1.45 if i > 30 else 1.0) for i, (d, v) in enumerate(sorted(adj.items()))}
    elif case == "nothing":
        close, adj = {}, {}
    rec = _assess(close, adj, **kw)
    assert not rec["admitted"] and reason in rec["reasons"]


def test_twelvedata_split_matched_within_days_passes():
    close, adj = _series(split_at=20, split=2.0)
    split_day = sorted(close)[20]
    rec = _assess(close, adj, td=[(split_day + timedelta(days=1), 0.5)])
    assert rec["admitted"] and rec["twelvedata_splits"]["td_splits_in_window"] == 1


def test_event_coverage_is_issuer_level():
    issuers = [{"ticker": "AAA", "admitted_purchase_events_2012_2019": 5},
               {"ticker": "BBB", "admitted_purchase_events_2012_2019": 3},
               {"ticker": "CCC", "admitted_purchase_events_2012_2019": 0}]
    cov = gd4.event_coverage(issuers, ["AAA", "CCC", "XLK"])
    assert (cov.issuers, cov.issuers_price_admitted, cov.events, cov.events_price_admitted) == (3, 2, 8, 5)
    assert cov.issuers_with_events == 2 and cov.issuers_with_events_price_admitted == 1
    assert cov.event_fraction == pytest.approx(5 / 8) and cov.not_admitted == ["BBB"]


# --- manifest schema vs the harness -----------------------------------------------------------------


def test_manifest_keys_match_the_harness_price_manifest_on_main():
    names = tuple(f.name for f in dataclasses.fields(v1.PriceManifest))
    assert names == gd4.MANIFEST_KEYS


def test_manifest_keys_match_the_v2_v3_harness_once_merged():
    v2 = pytest.importorskip("analysis.panel_insider_density_v2")
    names = tuple(f.name for f in dataclasses.fields(v2.PriceManifest))
    assert names == gd4.MANIFEST_KEYS + gd4.MANIFEST_OPTIONAL_KEYS


# --- end to end on SQLite --------------------------------------------------------------------------


def _db(tmp_path=None):
    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table("raw_series", md, Column("series_id", String), Column("source_id", Integer),
                Column("obs_date", Date), Column("pull_timestamp", DateTime), Column("value", Float),
                Column("raw_payload", Text), Column("pull_status", String))
    md.create_all(engine)
    rows = []

    def put(ticker, close, adj, source=TIINGO, status="SUCCESS", pull=PULL):
        for fld, vals in (("close", close), ("adj_close", adj)):
            rows.extend({"series_id": f"YF:{ticker}:{fld}", "source_id": source, "obs_date": d,
                         "pull_timestamp": pull, "value": v, "raw_payload": "{}", "pull_status": status}
                        for d, v in vals.items())

    xc, xa = _series(base=77.123, dividends=(15, 45))
    put("XLK", xc, xa)
    put("XLK", xc, xa, pull=PULL + timedelta(days=2))
    put("XLK", {d: v * 1.7 for d, v in xc.items()}, {}, source=YFINANCE)  # other source: never read
    ac, aa = _series(base=13.377, split_at=25)
    put("AAA", ac, aa)
    # holdout-period rows that would break AAA if they were ever read
    late = _sessions(date(2020, 1, 2), 5)
    put("AAA", {d: 99.0 for d in late}, {d: 1.0 for d in late})
    put("AAA", {d: 98.0 for d in late}, {d: 2.0 for d in late}, pull=PULL + timedelta(days=9))
    bc, ba = _series(base=22.222)
    put("BBB", bc, ba)
    put("BBB", {d: v * 1.01 for d, v in list(bc.items())[:3]}, {}, pull=PULL + timedelta(days=1))
    cc, ca = _series(base=31.313)
    put("CCC", cc, ca)
    put("CCC", {d: v * 3 for d, v in list(cc.items())[:4]}, {}, source=TIINGO, status="QUARANTINED")
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": TIINGO, "name": "TIINGO"}, {"id": YFINANCE, "name": "yfinance"},
                                     {"id": TD_SPLITS, "name": "TWELVEDATA_SPLITS"},
                                     {"id": YF_ADJ, "name": "yfinance_adjusted_extended"}])
        c.execute(raw.insert(), rows)
        c.execute(raw.insert(), [{"series_id": "TWELVEDATA_SPLITS:AAA:ratio", "source_id": TD_SPLITS,
                                  "obs_date": sorted(ac)[25], "pull_timestamp": PULL, "value": 0.5,
                                  "raw_payload": "{}", "pull_status": "SUCCESS"}])
    return engine


def test_run_probe_end_to_end_on_sqlite():
    engine = _db()
    with engine.connect() as conn:
        with pytest.raises(gd4.ProbeRefused):
            gd4.run_probe(conn, ["AAA"], benchmark="XLK", source="yfinance", lo=LO, hi=HI)
        with pytest.raises(gd4.ProbeRefused):
            gd4.run_probe(conn, ["AAA"], benchmark="XLK", source="TIINGO", lo=LO, hi=date(2020, 1, 1))
        with pytest.raises(gd4.ProbeRefused):
            gd4.read_rows(conn, "YF:AAA:close", TIINGO, LO, date(2020, 6, 1))
        probe = gd4.run_probe(conn, ["AAA", "BBB", "CCC", "ZZZ"], benchmark="XLK", source="tiingo", lo=LO, hi=HI)
    rec = probe["records"]
    assert probe["source"] == {"name": "TIINGO", "id": TIINGO}
    assert rec["XLK"]["admitted"] and rec["XLK"]["other_sources_on_series_id"] == ["yfinance"]
    assert rec["XLK"]["pull_batches"]["count"] == 2
    assert rec["AAA"]["admitted"] and rec["AAA"]["coverage"]["last_date"] < "2020-01-01"
    assert [s["label"] for s in rec["AAA"]["factor_steps"]["implied_splits"]] == ["2-for-1"]
    assert rec["AAA"]["twelvedata_splits"]["td_splits_in_window"] == 1
    assert rec["BBB"]["reasons"] == ["multi_valued_dates"]
    assert rec["CCC"]["reasons"] == ["quarantined_batch_rows"]
    assert rec["ZZZ"]["reasons"] == ["no_source_series"]
    assert probe["calendar_sessions"] == 60


def test_cli_writes_report_manifest_and_hashes(tmp_path, monkeypatch):
    engine = _db()
    import scripts.run_real_panel_scan as rps
    from scripts import run_price_admission_probe as cli

    monkeypatch.setattr(rps, "read_only_engine", lambda *a, **k: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    tickers = tmp_path / "issuers.json"
    tickers.write_text(json.dumps({"issuers": [{"ticker": t, "admitted_purchase_events_2012_2019": n}
                                               for t, n in (("AAA", 4), ("BBB", 2), ("CCC", 1), ("ZZZ", 3))]}))
    out = tmp_path / "probe"
    cli.main(["probe", "--tickers-file", str(tickers), "--code-sha", "c" * 40, "--out", str(out)])
    report = json.loads((out / "probe_report.json").read_text())
    manifest_path = out / "price_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert tuple(manifest) == tuple(sorted(gd4.MANIFEST_KEYS))
    assert manifest["admitted"] == ["AAA", "XLK"] and manifest["source"] == "TIINGO"
    assert manifest["series_template"] == "YF:{ticker}:adj_close" and manifest["basis"] == gd4.BASIS_ADJUSTED
    assert manifest["probe_report_sha256"] == v1.data_sha256(out / "probe_report.json")
    assert report["summary"]["admitted_excluding_benchmark"] == 1 and report["summary"]["promotion_allowed"] is False
    loaded = v1.PriceManifest.from_file(manifest_path)  # the harness on main accepts it
    assert loaded.admitted == ("AAA", "XLK") and loaded.benchmark == "XLK"
    try:
        from analysis import panel_insider_density_v2 as v2
    except ImportError:
        v2 = None
    if v2 is not None:
        assert v2.PriceManifest.from_file(manifest_path).listed_from == ()
    lines = (out / "sha256s.txt").read_text().splitlines()
    assert sorted(line.split()[1] for line in lines) == ["price_manifest.json", "probe_report.json"]
    for line in lines:
        h, name = line.split()
        assert v1.data_sha256(out / name) == h
    with pytest.raises(FileExistsError):
        cli.main(["probe", "--tickers-file", str(tickers), "--code-sha", "c" * 40, "--out", str(out)])
    cov_path = tmp_path / "coverage.json"
    cli.main(["coverage", "--issuers", str(tickers), "--manifest", str(manifest_path), "--out", str(cov_path)])
    cov = json.loads(cov_path.read_text())
    assert (cov["events"], cov["events_price_admitted"], cov["issuers_price_admitted"]) == (10, 4, 1)
    with pytest.raises(SystemExit):
        cli.main(["probe", "--tickers-file", str(tickers), "--code-sha", "c", "--out", str(tmp_path / "x"),
                  "--end", "2020-03-31"])


def test_no_manifest_when_the_benchmark_fails():
    report = {"benchmark_admitted": False, "benchmark": "XLK", "source": {"name": "TIINGO"}}
    with pytest.raises(gd4.ProbeRefused, match="benchmark"):
        gd4.build_manifest(report, "a" * 64)
    assert not Path("price_manifest.json").exists()
