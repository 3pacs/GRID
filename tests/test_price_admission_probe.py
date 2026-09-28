"""VS1 v6 price-admission probe (GD4 + §2.3): synthetic data only (SQLite, fake vendor files; no real prices)."""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from argparse import Namespace
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

import pytest
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v6 as v6
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TIINGO, YFINANCE, TD_SPLITS, KAGGLE = 524, 2, 1035, 522
PULL = datetime(2026, 4, 7, 10, 0)
LATER = datetime(2026, 9, 28, 9, 0)
SNAPSHOT = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
LO, HI = gd4.DEFAULT_WINDOW
N = 300  # sessions per synthetic series: more than the cross-check's N = 250 pairs


def _sessions(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


CAL = _sessions(date(2018, 1, 2), N)


def _series(start=date(2018, 1, 2), n=N, base=41.237, split_at=None, split=2.0, dividends=()):
    """Synthetic raw close and adjusted close (adj = close x factor for later actions)."""
    days = _sessions(start, n)
    close = {}
    for i, d in enumerate(days):
        c = base * (1 + 0.013 * (((i * 7) % 11) - 5) / 5)
        if split_at is not None and i >= split_at:
            c /= split
        close[d] = round(c, 4)
    factor, f = {}, 1.0
    for i in range(len(days) - 1, -1, -1):
        factor[days[i]] = f
        if split_at is not None and i == split_at:
            f /= split
        if i in dividends:
            f *= 1 - 0.37 / close[days[i - 1]]
    adj = {d: close[d] * factor[d] for d in days}
    return close, adj


def _td(close, adj, *, scale=1.0, bad_days=()):
    """TwelveData's view of the same instrument: raw = close, adjusted = adj up to a constant."""
    all_ = {d.isoformat(): round(v * scale, 5) for d, v in adj.items()}
    none = {d.isoformat(): v for d, v in close.items()}
    for d in bad_days:  # a bad print in both TwelveData series: its factor is unchanged, its return is off
        all_[d.isoformat()] *= 1.02
        none[d.isoformat()] *= 1.02
    return {"all": all_, "none": none,
            "receipts": {"all": {"outcome": "ok"}, "none": {"outcome": "ok"}}, "refused_holdout_dates": 0}


def _rows(values: dict, pull=PULL) -> list[gd4.Row]:
    return [gd4.Row(d, v, pull) for d, v in values.items()]


META = {"receipt": {"outcome": "ok"}, "meta": {"ticker": "AAA", "name": "Advanced Micro Devices",
                                              "exchangeCode": "NASDAQ", "startDate": "1983-03-21",
                                              "endDate": "2026-09-26"}}


def _assess(close, adj, *, td="same", meta=META, sec_name="Advanced Micro Devices Inc", vint_close=None,
            vint_adj=None, sel_close=None, sel_adj=None, statuses=None, others=None, calendar=CAL, **kw):
    return gd4.assess_ticker(
        "AAA",
        vintages_close=vint_close if vint_close is not None else _rows(close),
        vintages_adj=vint_adj if vint_adj is not None else _rows(adj),
        selected_close=sel_close if sel_close is not None else _rows(close),
        selected_adj=sel_adj if sel_adj is not None else _rows(adj),
        statuses=statuses or {"YF:AAA:close": {"SUCCESS": len(close)}},
        other_source_rows=others or {}, calendar=calendar,
        td=_td(close, adj) if td == "same" else td, meta=meta, sec_name=sec_name, **kw)


# --- rule constants come from the merged v6 harness ------------------------------------------------


def test_rule_is_the_v6_harness_rule():
    assert gd4.STUDY == "vs1-v6" and gd4.PREREG_BODY_SHA256 == v6.PREREG_BODY_SHA256
    assert (gd4.PRICE_SOURCE, gd4.PRICE_SOURCE_ID, gd4.SERIES_TEMPLATE, gd4.BASIS, gd4.BENCHMARK) == (
        "TIINGO", 524, "YF:{ticker}:adj_close", "split+dividend adjusted", "XLK")
    rule = gd4.CROSSCHECK
    assert (rule.min_share_within, rule.tolerance, rule.min_pairs, rule.max_excluded_share, rule.factor_rel_tol) == (
        0.99, 0.0010, 250, 0.10, 1e-4)
    assert gd4.SPLICE_TOL == 1e-4 and v6.NAME_JACCARD_MIN == 0.5
    from scripts.run_vs1_v2_insider_density import PRICE_WARMUP_DAYS

    assert LO == v1.window_bounds("discovery")[0].date() - timedelta(days=PRICE_WARMUP_DAYS) == date(2011, 11, 2)
    assert HI == date(2019, 12, 31)


def test_window_and_source_refusals():
    gd4.check_window(LO, HI)
    for bad in (date(2020, 1, 1), date(2024, 6, 3)):
        with pytest.raises(gd4.ProbeRefused, match="holdout"):
            gd4.check_window(LO, bad)
    with pytest.raises(gd4.ProbeRefused):
        gd4.check_window(date(2019, 1, 2), date(2018, 1, 2))
    assert v1.REFUSED_PRICE_SOURCES <= gd4.REFUSED_SOURCES
    gd4.check_source("TIINGO")
    gd4.check_source("tiingo")
    for name in ("yfinance", "YF", "KAGGLE_BULK", "yfinance_adjusted_extended", "", "TWELVEDATA", "polygon"):
        with pytest.raises(gd4.ProbeRefused):
            gd4.check_source(name)


# --- GD4 basis checks (unchanged) ------------------------------------------------------------------


def test_multi_valued_dates_are_found_and_float_roundtrip_is_not():
    d1, d2 = date(2019, 3, 1), date(2019, 3, 4)
    rows = [gd4.Row(d1, 10.0, PULL), gd4.Row(d1, 10.0 * (1 + 1e-12), LATER),
            gd4.Row(d2, 11.0, PULL), gd4.Row(d2, 11.5, LATER)]
    values, multi = gd4.collapse_vintages(rows)
    assert set(values) == {d1, d2} and multi == ["2019-03-04"]


def test_split_ratios_and_factor_steps():
    assert gd4.nice_ratio(0.5) == Fraction(1, 2) and gd4.split_label(Fraction(1, 2)) == "2-for-1"
    assert gd4.split_label(gd4.nice_ratio(10.0)) == "1-for-10"
    assert gd4.nice_ratio(0.985) is None and gd4.nice_ratio(0.61) is None
    close, adj = _series(split_at=20, dividends=(10, 40))
    steps = gd4.factor_steps(close, adj)
    assert [s["label"] for s in steps["implied_splits"]] == ["2-for-1"]
    assert steps["distribution_steps"] == 2 and steps["anomalous_steps"] == []
    days = sorted(close)
    broken = {d: v * (1.45 if i >= 30 else 1.0) for i, (d, v) in enumerate(sorted(adj.items()))}
    assert gd4.factor_steps(close, broken)["anomalous_steps"][0]["date"] == days[30].isoformat()


def test_calendar_gaps_and_pull_batches():
    cal = CAL[:10]
    assert gd4.calendar_gaps(cal[:3] + cal[4:9], cal)["missing_sessions"] == 1
    assert gd4.calendar_gaps(cal[2:6], cal)["missing_sessions"] == 0  # outside the span is not a gap
    assert gd4.pull_batch(datetime(2026, 4, 7, 23, 30, tzinfo=timezone(timedelta(hours=-5)))) == "2026-04-08"
    assert gd4.pull_batch(PULL) == "2026-04-07" and gd4.pull_batch(None) == "none"
    d = cal[0]
    labels = gd4.batch_labels([gd4.Row(d, 1.0, PULL)], [gd4.Row(d, 1.0, LATER)])
    assert labels == {d.isoformat(): "2026-04-07|2026-09-28"}


# --- the v6 admission record -------------------------------------------------------------------------


def test_clean_series_is_admitted_with_listed_from_and_no_prices_in_the_record():
    close, adj = _series(split_at=150, dividends=(40, 200))
    rec = _assess(close, adj, others={"KAGGLE_BULK": {"SUCCESS": 12}, "yfinance": {"QUARANTINED": 3}})
    assert rec["admitted"] and rec["reasons"] == []
    assert rec["listed_from"] == "1983-03-21" and rec["entity"]["check"] == "match"
    cc = rec["crosscheck"]
    assert cc["passed"] and cc["pairs"] >= 250 and cc["excluded_adjustment_pairs"] == 3  # 1 split + 2 dividends
    assert rec["splice"]["boundaries"] == 0 and rec["checks"]["no_gaps"] is True
    assert rec["source_filtering"]["other_source_rows_total"] == 15  # counted, not disqualifying
    dumped = json.dumps(rec)
    for v in list(close.values())[:20] + list(adj.values())[:20]:
        assert repr(v) not in dumped and f"{v:.4f}" not in dumped


def test_twelvedata_disagreement_and_missing_twelvedata_fail():
    close, adj = _series()
    bad = _td(close, adj, bad_days=CAL[10:40:3])  # 10 isolated bad prints -> 20 pairs off: share < 99%
    rec = _assess(close, adj, td=bad)
    assert not rec["admitted"] and rec["reasons"] == ["crosscheck_disagreement"]
    two = _td(close, adj, bad_days=CAL[10:11])  # one bad print (2 pairs) still passes at N ~ 299
    assert _assess(close, adj, td=two)["admitted"]
    none = _assess(close, adj, td={"receipts": {}, "refused_holdout_dates": 0})
    assert "twelvedata_not_fetched" in none["reasons"] and "crosscheck_too_few_pairs" in none["reasons"]
    gone = _assess(close, adj, td={"receipts": {"all": {"outcome": "unavailable"}, "none": {"outcome": "unavailable"}}})
    assert "twelvedata_unavailable" in gone["reasons"] and not gone["admitted"]


def test_scaled_twelvedata_adjustment_passes_and_short_history_fails():
    close, adj = _series()
    assert _assess(close, adj, td=_td(close, adj, scale=0.87))["admitted"]  # returns, not levels, are compared
    c2, a2 = _series(start=CAL[-200], n=200)
    rec = _assess(c2, a2)
    assert not rec["admitted"] and rec["reasons"] == ["crosscheck_too_few_pairs"]


def test_adjustment_dominated_series_fails():
    close, adj = _series(dividends=tuple(range(5, N, 8)))  # a distribution every 8 sessions: > 10% of pairs
    rec = _assess(close, adj)
    assert rec["crosscheck"]["reason"] == "adjustment_dominated" and not rec["admitted"]


@pytest.mark.parametrize("case, reason", [
    ("multi", "multi_valued_dates"), ("quarantined", "quarantined_rows"), ("no_adj", "no_adjusted_series"),
    ("break", "split_inconsistent"), ("gap", "calendar_gaps"), ("nothing", "no_source_series"),
    ("no_meta", "no_meta"), ("meta_404", "no_meta"), ("mismatch", "entity_mismatch"), ("no_sec_name", "entity_mismatch"),
])
def test_each_failed_rule_refuses(case, reason):
    close, adj = _series(split_at=150, dividends=(40,))
    kw = {}
    if case == "multi":
        d = sorted(adj)[5]
        kw["vint_adj"] = _rows(adj) + [gd4.Row(d, adj[d] * 1.02, LATER)]
    elif case == "quarantined":
        kw["statuses"] = {"YF:AAA:close": {"SUCCESS": N, "QUARANTINED": 3}}
    elif case == "no_adj":
        kw["sel_adj"] = []
    elif case == "break":
        adj = {d: v * (1.45 if i > 30 else 1.0) for i, (d, v) in enumerate(sorted(adj.items()))}
        kw["td"] = _td(close, adj)
    elif case == "gap":
        missing = sorted(close)[77]
        kw["sel_close"] = [r for r in _rows(close) if r.obs_date != missing]
    elif case == "nothing":
        close, adj = {}, {}
    elif case == "no_meta":
        kw["meta"] = None
    elif case == "meta_404":
        kw["meta"] = {"receipt": {"outcome": "unavailable"}, "meta": None}
    elif case == "mismatch":
        kw["sec_name"] = "ITT Educational Services Inc"
    elif case == "no_sec_name":
        kw["sec_name"] = None
    rec = _assess(close, adj, **kw)
    assert not rec["admitted"] and reason in rec["reasons"], rec["reasons"]


def test_splice_boundary_fails_unless_twelvedata_shows_the_same_step():
    close, adj = _series()
    k = 120
    days = sorted(close)
    # an older vintage (April) spliced to a re-adjusted one (September): the old rows lack a later
    # dividend's adjustment, so the TIINGO factor steps by about 0.6% at the batch boundary
    sel_adj = ([gd4.Row(d, adj[d] / 0.994, PULL) for d in days[:k]] + [gd4.Row(d, adj[d], LATER) for d in days[k:]])
    sel_close = [gd4.Row(d, close[d], PULL) for d in days[:k]] + [gd4.Row(d, close[d], LATER) for d in days[k:]]
    rec = _assess(close, adj, sel_adj=sel_adj, sel_close=sel_close, vint_adj=sel_adj, vint_close=sel_close)
    assert rec["splice"]["boundaries"] == 1 and rec["splice"]["failed"] == [[days[k - 1].isoformat(), days[k].isoformat()]]
    assert "splice_failed" in rec["reasons"] and not rec["admitted"]
    # the same step at a real ex-date that TwelveData also shows: corroborated, passes the splice check
    adj_true = {d: (adj[d] * 0.994 if i < k else adj[d]) for i, d in enumerate(days)}  # a real ex-date at k
    true_adj = [gd4.Row(d, adj_true[d], PULL if i < k else LATER) for i, d in enumerate(days)]
    ok = _assess(close, adj_true, sel_adj=true_adj, sel_close=sel_close, vint_adj=true_adj, vint_close=sel_close,
                 td=_td(close, adj_true))
    assert ok["splice"]["boundaries"] == 1 and ok["factor_steps"]["distribution_steps"] == 1
    assert ok["splice"]["passed"] and ok["admitted"], ok["reasons"]
    # a boundary with a flat factor passes without TwelveData
    flat_adj = [gd4.Row(d, adj[d], PULL if i < k else LATER) for i, d in enumerate(days)]
    assert _assess(close, adj, sel_adj=flat_adj, sel_close=sel_close)["splice"]["passed"]


def test_benchmark_skips_the_entity_check_and_carries_no_listed_from():
    close, adj = _series(base=77.1)
    rec = _assess(close, adj, meta=None, sec_name=None, is_benchmark=True)
    assert rec["admitted"] and rec["entity"]["check"] == "not_applicable_benchmark" and rec["listed_from"] is None


def test_low_price_sessions_are_counted():
    close, adj = _series(base=0.61)
    rec = _assess(close, adj)
    assert rec["low_price"]["sessions_close_below_1usd"] == N


def test_interval_counts_report_what_c1_and_start_date_blank():
    close, adj = _series()
    inside = [i >= 50 and not 100 <= i < 110 for i in range(N)]
    got = gd4.interval_counts(inside, CAL, CAL[70].isoformat(), adj)
    assert got["inside_interval"] == N - 60 and got["outside_interval"] == 60
    assert got["before_start_date"] == 70 and got["closes_used"] == N - 70 - 10 and got["closes_blanked"] == 80
    assert gd4.interval_counts(None, CAL, None, adj) is None


# --- C1 interval from the submissions table (v2.ticker_mask via v6.interval_close_mask) -------------


def _submissions(path: Path) -> Path:
    rows = [
        ("0001-1", "2017-11-15", "111", "4", "OLD"),     # names another ticker: before the interval
        ("0001-2", "2018-02-01", "111", "4", "AAA"),     # first filing naming a current ticker
        ("0001-3", "2018-06-01", "111", "4", "ZZZ"),     # interrupted
        ("0001-4", "2018-09-04", "111", "4/A", "NYSE: AAA"),  # back inside
        ("0002-1", "2018-03-01", "222", "4", "NONE"),    # names no ticker: never inside
    ]
    lines = ["accession_number,filing_date,issuer_cik,document_type,issuer_ticker"]
    lines += [",".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def test_c1_interval_matches_the_harness_ticker_rule(tmp_path):
    issuers = [{"ticker": "AAA", "cik": 111, "current_tickers": ["AAA"]},
               {"ticker": "BBB", "cik": 222, "current_tickers": ["BBB"]}]
    interval = gd4.C1Interval.from_submissions(_submissions(tmp_path / "subs.csv"), issuers)
    mask = interval.mask(CAL)
    a = dict(zip(CAL, mask["AAA"]))
    assert not a[date(2018, 1, 31)] and a[date(2018, 2, 2)]
    assert not a[date(2018, 6, 4)] and not a[date(2018, 8, 31)] and a[date(2018, 9, 5)]
    assert not mask["BBB"].any()
    assert len(interval.receipt_sha256) == 64


# --- end to end: SQLite + vendor files + CLI ----------------------------------------------------------


def _db():
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

    series = {"XLK": _series(base=77.123, dividends=(15, 145)), "AAA": _series(base=13.377, split_at=125),
              "BBB": _series(base=22.222), "CCC": _series(base=31.313), "DDD": _series(base=18.5),
              "EEE": _series(base=44.4)}
    put("XLK", *series["XLK"])
    put("XLK", *series["XLK"], pull=PULL + timedelta(days=2))
    put("XLK", {d: v * 1.7 for d, v in series["XLK"][0].items()}, {}, source=YFINANCE)  # other source: counted
    put("AAA", *series["AAA"])
    put("AAA", {d: 5.0 for d in list(series["AAA"][0])[:40]}, {}, source=KAGGLE)
    late = _sessions(date(2020, 1, 2), 5)  # holdout rows that would break AAA if they were ever read
    put("AAA", {d: 99.0 for d in late}, {d: 1.0 for d in late})
    put("AAA", {d: 98.0 for d in late}, {d: 2.0 for d in late}, pull=PULL + timedelta(days=9))
    put("BBB", *series["BBB"])
    put("BBB", {d: v * 1.01 for d, v in list(series["BBB"][0].items())[:3]}, {}, pull=PULL + timedelta(days=1))
    put("CCC", *series["CCC"])
    put("CCC", {d: v * 3 for d, v in list(series["CCC"][0].items())[:4]}, {}, status="QUARANTINED")
    put("DDD", *series["DDD"])
    put("EEE", *series["EEE"])
    # a vintage pulled after the snapshot: invisible to every read, so EEE stays clean
    put("EEE", {d: v * 1.5 for d, v in series["EEE"][0].items()}, {}, pull=datetime(2026, 9, 20, 9, 0))
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": TIINGO, "name": "TIINGO"}, {"id": YFINANCE, "name": "yfinance"},
                                     {"id": TD_SPLITS, "name": "TWELVEDATA_SPLITS"}, {"id": KAGGLE, "name": "KAGGLE_BULK"}])
        c.execute(raw.insert(), rows)
    return engine, series


class _Vendors:
    """Writes TwelveData and Tiingo-metadata files through ``price_admission_fetch`` with a fake transport."""

    def __init__(self, series, tmp_path: Path, *, td_missing=("ZZZ",), td_bad=("DDD",), names=None):
        self.series, self.td_missing, self.td_bad = series, set(td_missing), set(td_bad)
        self.names = names or {}
        self.td_dir, self.meta_dir = tmp_path / "td", tmp_path / "meta"

    def __call__(self, url, params, headers):
        if url == fetch.TD_USAGE_URL:
            return 200, json.dumps({"daily_usage": 0, "plan_daily_limit": 800, "plan_limit": 8,
                                    "plan_category": "basic"}).encode()
        if url == fetch.TD_URL:
            sym = params["symbol"]
            if sym in self.td_missing or sym not in self.series:
                return 200, json.dumps({"code": 400, "message": "symbol not found", "status": "error"}).encode()
            close, adj = self.series[sym]
            vals = adj if params["adjust"] == "all" else close
            if sym == "XLK":  # the benchmark's TwelveData history starts at the window start (plan check)
                vals = {LO: vals[min(vals)], **vals}
            bad = sym in self.td_bad
            values = [{"datetime": d.isoformat(), "close": f"{v * (1.03 if bad and i % 3 == 0 else 1.0):.5f}"}
                      for i, (d, v) in enumerate(sorted(vals.items()))]
            return 200, json.dumps({"meta": {"symbol": sym}, "values": values, "status": "ok"}).encode()
        t = url.rsplit("/", 1)[-1]
        if t not in self.series:
            return 404, b'{"detail":"Not found."}'
        return 200, json.dumps({"ticker": t, "name": self.names.get(t, f"{t} Systems Inc"), "exchangeCode": "NYSE",
                                "startDate": "2018-03-01" if t == "AAA" else "2000-01-03",
                                "endDate": "2026-09-26"}).encode()

    def write(self, tickers):
        fetch.fetch_twelvedata(tickers, self.td_dir, benchmark="XLK", key="k", http_get=self, sleep=lambda s: None,
                               spacing_s=0)
        fetch.fetch_tiingo_meta(tickers + ["XLK"], self.meta_dir, key="k", http_get=self, sleep=lambda s: None)
        return gd4.VendorFiles(self.td_dir, self.meta_dir)


TICKERS = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ZZZ"]
NAMES = {"AAA": "AAA Systems Inc", "BBB": "BBB Systems Inc", "CCC": "CCC Systems Inc", "DDD": "DDD Systems Inc",
         "EEE": "Entirely Other Co", "FFF": "FFF Systems Inc", "ZZZ": "ZZZ Systems Inc"}


def test_run_probe_end_to_end_on_sqlite(tmp_path):
    engine, series = _db()
    vendors = _Vendors(series, tmp_path).write(TICKERS)
    with engine.connect() as conn:
        with pytest.raises(gd4.ProbeRefused):
            gd4.run_probe(conn, ["AAA"], benchmark="XLK", source="yfinance", lo=LO, hi=HI, as_of_ts=SNAPSHOT,
                          vendors=vendors)
        with pytest.raises(gd4.ProbeRefused):
            gd4.run_probe(conn, ["AAA"], benchmark="XLK", source="TIINGO", lo=LO, hi=date(2020, 1, 1),
                          as_of_ts=SNAPSHOT, vendors=vendors)
        with pytest.raises(gd4.ProbeRefused):
            gd4.read_selected(conn, "YF:AAA:close", LO, date(2020, 6, 1), SNAPSHOT)
        probe = gd4.run_probe(conn, TICKERS, benchmark="XLK", source="tiingo", lo=LO, hi=HI, as_of_ts=SNAPSHOT,
                              vendors=vendors, sec_names=NAMES)
    rec = probe["records"]
    assert probe["source"] == {"name": "TIINGO", "id": TIINGO} and probe["calendar_sessions"] == N
    assert rec["XLK"]["admitted"] and rec["XLK"]["source_filtering"]["other_source_rows"] == {"yfinance": {"SUCCESS": N}}
    assert rec["AAA"]["admitted"] and rec["AAA"]["coverage"]["last_date"] < "2020-01-01"
    assert rec["AAA"]["listed_from"] == "2018-03-01"
    assert rec["AAA"]["source_filtering"]["other_source_rows"] == {"KAGGLE_BULK": {"SUCCESS": 40}}
    # BBB's re-pulled dates are multi-valued, and the re-pull is a batch boundary with an unexplained step
    assert rec["BBB"]["reasons"] == ["multi_valued_dates", "splice_failed"]
    assert rec["CCC"]["reasons"] == ["quarantined_rows"]
    assert rec["DDD"]["reasons"] == ["crosscheck_disagreement"]
    assert rec["EEE"]["reasons"] == ["entity_mismatch"]  # the post-snapshot vintage was never read
    assert rec["FFF"]["reasons"][0] == "no_source_series" and "no_meta" in rec["FFF"]["reasons"]
    assert "twelvedata_unavailable" in rec["ZZZ"]["reasons"]


def test_cli_writes_reports_and_a_manifest_the_v6_harness_accepts(tmp_path, monkeypatch):
    engine, series = _db()
    _Vendors(series, tmp_path).write(TICKERS)
    import scripts.run_real_panel_scan as rps
    from scripts import run_price_admission_probe as cli
    from scripts import run_vs1_v2_insider_density as harness_cli

    monkeypatch.setattr(rps, "read_only_engine", lambda *a, **k: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    ciks = {t: 100 + i for i, t in enumerate(TICKERS)}
    issuers = tmp_path / "issuers.json"
    issuers.write_text(json.dumps({"issuers": [
        {"ticker": t, "cik": ciks[t], "current_tickers": [t], "admitted_purchase_events_2012_2019": i + 1}
        for i, t in enumerate(TICKERS)]}))
    sic = tmp_path / "issuer_sic_map.jsonl"
    sic.write_text("".join(json.dumps({"cik": ciks[t], "sic": 7372, "name": NAMES[t], "tickers": [t],
                                       "former_names": [], "http_status": 200}) + "\n" for t in TICKERS))
    subs = tmp_path / "subs.csv"
    subs.write_text("accession_number,filing_date,issuer_cik,document_type,issuer_ticker\n"
                    + "".join(f"{ciks[t]}-1,2018-06-01,{ciks[t]},4,{t}\n" for t in TICKERS))
    out = tmp_path / "probe"
    base = ["probe", "--tickers-file", str(issuers), "--twelvedata-dir", str(tmp_path / "td"),
            "--tiingo-meta-dir", str(tmp_path / "meta"), "--code-sha", "c" * 40]
    cli.main(base + ["--sic-map", str(sic), "--unpinned-sic-map", "--submissions", str(subs), "--out", str(out),
                     "--as-of-ts", SNAPSHOT.isoformat()])
    report = json.loads((out / "probe_report.json").read_text())
    cross = json.loads((out / "crosscheck_report.json").read_text())
    meta = json.loads((out / "tiingo_meta_report.json").read_text())
    manifest = json.loads((out / "price_manifest.json").read_text())
    assert manifest["admitted"] == ["AAA", "XLK"] and manifest["listed_from"] == {"AAA": "2018-03-01"}
    assert tuple(manifest) == tuple(sorted(f.name for f in dataclasses.fields(v6.PriceManifest)))
    assert manifest["probe_report_sha256"] == v1.data_sha256(out / "probe_report.json")
    assert manifest["crosscheck_report_sha256"] == v1.data_sha256(out / "crosscheck_report.json")
    assert manifest["tiingo_meta_report_sha256"] == v1.data_sha256(out / "tiingo_meta_report.json")
    loaded = v6.PriceManifest.from_file(out / "price_manifest.json")
    assert loaded.listed_from == (("AAA", "2018-03-01"),) and loaded.benchmark == "XLK"
    # the v6 CLI's own check of the three report hashes accepts the probe's files
    args = Namespace(h=v6, price_manifest=str(out / "price_manifest.json"), probe_report=str(out / "probe_report.json"),
                     crosscheck_report=str(out / "crosscheck_report.json"),
                     tiingo_meta_report=str(out / "tiingo_meta_report.json"))
    assert harness_cli._manifest(args).admitted == ("AAA", "XLK")
    bad = Namespace(**{**vars(args), "crosscheck_report": str(out / "tiingo_meta_report.json")})
    with pytest.raises(SystemExit, match="cross-check"):
        harness_cli._manifest(bad)
    assert report["window"] == "discovery" and report["prereg"]["study"] == "vs1-v6"
    assert report["snapshot_as_of_ts"] == SNAPSHOT.isoformat() and report["summary"]["promotion_allowed"] is False
    assert report["summary"]["not_admitted_by_reason"]["entity_mismatch"] == 1
    assert report["summary"]["source_filtering"]["other_source_rows_by_source"] == {"KAGGLE_BULK": 40, "yfinance": N}
    assert report["summary"]["c1_interval"]["tickers"] == len(TICKERS)
    assert report["tickers"]["AAA"]["c1"]["first_inside"] == "2018-06-04"
    assert cross["summary"]["twelvedata_unavailable"] == ["FFF", "ZZZ"] and cross["rule"]["min_pairs"] == 250
    assert cross["summary"]["twelvedata_end_date_exclusive_tickers"] == 0
    assert meta["summary"]["entity_mismatch"] == ["EEE"] and meta["summary"]["no_meta"] == ["FFF", "ZZZ"]
    for line in (out / "sha256s.txt").read_text().splitlines():
        h, name = line.split()
        assert v1.data_sha256(out / name) == h
    for text_ in (json.dumps(report), json.dumps(cross), json.dumps(meta)):
        for v in list(series["AAA"][1].values())[:20]:
            assert repr(v) not in text_ and f"{v:.4f}" not in text_
    with pytest.raises(FileExistsError):
        cli.main(base + ["--out", str(out)])
    with pytest.raises(SystemExit):
        cli.main(base + ["--out", str(tmp_path / "x"), "--end", "2020-03-31"])
    with pytest.raises(SystemExit):
        cli.main(base + ["--out", str(tmp_path / "y"), "--source", "KAGGLE_BULK"])
    cov_path = tmp_path / "coverage.json"
    cli.main(["coverage", "--issuers", str(issuers), "--manifest", str(out / "price_manifest.json"),
              "--out", str(cov_path)])
    cov = json.loads(cov_path.read_text())
    assert (cov["events"], cov["events_price_admitted"], cov["issuers_price_admitted"]) == (28, 1, 1)


def test_no_manifest_when_the_benchmark_fails():
    report = {"benchmark_admitted": False, "benchmark": "XLK", "source": {"name": "TIINGO"}}
    with pytest.raises(gd4.ProbeRefused, match="benchmark"):
        gd4.build_manifest(report, "a" * 64)


def test_event_coverage_is_issuer_level():
    issuers = [{"ticker": "AAA", "admitted_purchase_events_2012_2019": 5},
               {"ticker": "BBB", "admitted_purchase_events_2012_2019": 3},
               {"ticker": "CCC", "admitted_purchase_events_2012_2019": 0}]
    cov = gd4.event_coverage(issuers, ["AAA", "CCC", "XLK"])
    assert (cov.issuers, cov.issuers_price_admitted, cov.events, cov.events_price_admitted) == (3, 2, 8, 5)
    assert cov.event_fraction == pytest.approx(5 / 8) and cov.not_admitted == ["BBB"]
