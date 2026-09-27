"""CFTC COT identity by ``cftc_contract_market_code`` (god-view slice G1).

The legacy puller matched Socrata rows on a market-name substring and kept
the first row per date, so ``cftc.SP500.*`` switched between E-mini, micro,
dividend-index and consolidated S&P futures week to week. These tests pin:

* one series identity per market code, from a captured Socrata response
  (2026-09-15 and 2026-09-22, every market whose name contains "S&P 500" or
  "GOLD") — a similar-named market cannot enter another code's series;
* fail closed: missing code, incomplete metrics, conflicting rows -> skip;
* scheduled publication time (Friday 15:30 ET, holiday-shifted) stored per row;
* consumers read only code-keyed ids and are unavailable without them.
"""

from __future__ import annotations

import json
import pathlib
import re
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata import cftc_cot
from ingestion.altdata.cftc_cot import CFTCCOTPuller, parse_market_records
from ingestion.altdata.cftc_markets import (
    COT_METRICS,
    LEGACY_CONTRACT_KEYS,
    MARKETS,
    compute_release,
    series_id,
    series_id_for_root,
)

REPO = pathlib.Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "fixtures" / "cftc" / "socrata_6dca_sp500_gold_20260915_20260922.json"


@pytest.fixture(scope="module")
def socrata_rows() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _rows_for(rows: list[dict], code: str) -> list[dict]:
    return [r for r in rows if r["cftc_contract_market_code"] == code]


# ── fake engine (same SQL surface as tests/test_raw_series_duplicate_guards) ─


def _make_engine(existing: set[tuple[str, date]] | None = None, source_id: int = 7):
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx
    existing = existing or set()
    inserted: list[dict] = []

    def execute(stmt, params=None):
        sql = str(stmt)
        res = MagicMock()
        if "SELECT id FROM source_catalog" in sql:
            res.fetchone.return_value = (source_id,)
        elif "SELECT DISTINCT obs_date FROM raw_series" in sql:
            res.fetchall.return_value = [(d,) for s, d in existing if s == params["sid"]]
        elif "SELECT MAX(obs_date) FROM raw_series" in sql:
            ds = [d for s, d in existing if s == params["sid"]]
            res.fetchone.return_value = (max(ds),) if ds else (None,)
        elif "INSERT INTO raw_series" in sql:
            inserted.append(dict(params))
            res.rowcount = 1
        else:  # pragma: no cover
            raise AssertionError(f"Unexpected SQL: {sql}")
        return res

    conn.execute.side_effect = execute
    return engine, inserted


def _pull(engine, code, records, **kw):
    puller = CFTCCOTPuller(engine)
    fetch = MagicMock(return_value=records)
    with patch.object(cftc_cot, "_RATE_LIMIT_DELAY", 0.0), \
            patch.object(puller, "_fetch_market", fetch):
        result = puller.pull_market(code, **kw)
    return result, fetch


# ── identity ────────────────────────────────────────────────────────────


def test_fixture_really_contains_similar_named_markets(socrata_rows):
    sp = {r["cftc_contract_market_code"] for r in socrata_rows
          if "S&P 500" in r["market_and_exchange_names"].upper()}
    gold = {r["cftc_contract_market_code"] for r in socrata_rows
            if "GOLD" in r["market_and_exchange_names"].upper()}
    assert {"13874A", "13874U", "43874A", "43874Q", "13874W", "13874+"} <= sp
    assert {"088691", "088695", "180LM9"} <= gold


def test_similar_named_markets_parse_to_separate_identities(socrata_rows):
    emini = parse_market_records("13874A", socrata_rows)
    micro = parse_market_records("13874U", socrata_rows)

    assert [r.report_date for r in emini.reports] == [date(2026, 9, 15), date(2026, 9, 22)]
    assert {r.market_name for r in emini.reports} == {"E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"}
    assert {r.market_name for r in micro.reports} == {"MICRO E-MINI S&P 500 INDEX - CHICAGO MERCANTILE EXCHANGE"}
    for rep in emini.reports:
        src = next(r for r in _rows_for(socrata_rows, "13874A")
                   if r["report_date_as_yyyy_mm_dd"].startswith(rep.report_date.isoformat()))
        assert rep.metrics["total_open_interest"] == float(src["open_interest_all"])
    assert [r.metrics for r in emini.reports] != [r.metrics for r in micro.reports]
    # every other S&P/gold market in the response was refused by code
    assert all(s["reason"] == "market_code_mismatch" for s in emini.skipped)
    assert len(emini.skipped) == len(socrata_rows) - 2


def test_mixed_name_week_cannot_enter_series(socrata_rows):
    """Even if the API ignored the code filter and returned every S&P/gold
    market, only the configured code's rows are written, under its own id."""
    engine, inserted = _make_engine()
    result, _ = _pull(engine, "13874A", socrata_rows, start_date="2026-09-01")

    assert result["status"] == "PARTIAL"  # other markets skipped, logged
    assert len(inserted) == 2 * len(COT_METRICS)
    assert {p["sid"] for p in inserted} == {f"cftc.13874A.{m}" for m in COT_METRICS}
    oi = {p["od"]: p["val"] for p in inserted if p["sid"] == "cftc.13874A.total_open_interest"}
    assert oi == {date(2026, 9, 15): 2446519.0, date(2026, 9, 22): 1890653.0}
    for p in inserted:
        payload = json.loads(p["payload"])
        assert payload["market_code"] == "13874A"
        assert payload["root"] == "ES"
        assert payload["identity"] == "cftc_contract_market_code"
        assert payload["market_name"] == "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"
        assert payload["report_date"] == p["od"].isoformat()
        assert payload["release_is_floor"] is True


def test_gold_is_comex_only(socrata_rows):
    engine, inserted = _make_engine()
    _pull(engine, "088691", socrata_rows, start_date="2026-09-01")
    names = {json.loads(p["payload"])["market_name"] for p in inserted}
    assert names == {"GOLD - COMMODITY EXCHANGE INC."}
    net = {p["od"]: p["val"] for p in inserted if p["sid"].endswith("net_speculative")}
    src = _rows_for(socrata_rows, "088691")
    assert sorted(net.values()) == sorted(
        float(r["noncomm_positions_long_all"]) - float(r["noncomm_positions_short_all"]) for r in src
    )


def test_series_id_only_accepts_tracked_market_codes():
    assert series_id("13874A", "net_speculative") == "cftc.13874A.net_speculative"
    assert series_id_for_root("ZN", "total_open_interest") == "cftc.043602.total_open_interest"
    for bad in ("SP500", "ES", "13874U", ""):
        with pytest.raises(ValueError):
            series_id(bad, "net_speculative")
    with pytest.raises(ValueError):
        series_id("13874A", "made_up_metric")


def test_new_ids_never_collide_with_legacy_keys():
    assert not set(MARKETS) & LEGACY_CONTRACT_KEYS
    assert len({m.root for m in MARKETS.values()}) == len(MARKETS)
    # Treasury markets are tracked by code (the legacy name keys died on the
    # 2022-02-08 CFTC rename, e.g. "10-YEAR ..." -> "UST 10Y NOTE").
    assert {"042601", "044601", "043602", "020601"} <= set(MARKETS)


# ── fail closed ─────────────────────────────────────────────────────────


def test_missing_code_skips_without_substitute(socrata_rows):
    others = [r for r in socrata_rows if r["cftc_contract_market_code"] != "13874A"]
    for records in ([], others):
        engine, inserted = _make_engine()
        result, _ = _pull(engine, "13874A", records, start_date="2026-09-01")
        assert result["status"] == "SKIPPED"
        assert inserted == []
        assert "not in report" in result["errors"][0]


def test_unknown_or_legacy_key_is_refused():
    engine, inserted = _make_engine()
    result, fetch = _pull(engine, "SP500", [], start_date="2026-09-01")
    assert result["status"] == "FAILED"
    fetch.assert_not_called()
    assert inserted == []


def test_incomplete_metrics_row_is_skipped_not_zero_filled(socrata_rows):
    row = dict(_rows_for(socrata_rows, "13874A")[0])
    del row["comm_positions_short_all"]
    out = parse_market_records("13874A", [row])
    assert out.reports == []
    assert out.skipped[0]["reason"] == "incomplete_metrics"


def test_conflicting_rows_same_date_drop_that_date(socrata_rows):
    a, b = _rows_for(socrata_rows, "13874A")
    a2 = dict(a, open_interest_all="1")
    out = parse_market_records("13874A", [a, a2, b, dict(b)])
    assert [r.report_date for r in out.reports] == [date(2026, 9, 22)]  # identical dup collapsed
    assert out.skipped == [{"reason": "conflicting_rows_same_report_date",
                            "report_date": "2026-09-15", "n": 2}]


def test_fetch_error_writes_no_zero_marker_row():
    engine, inserted = _make_engine()
    puller = CFTCCOTPuller(engine)
    with patch.object(puller, "_fetch_market", side_effect=OSError("boom")):
        result = puller.pull_market("13874A", start_date="2026-09-01")
    assert result["status"] == "FAILED"
    assert inserted == []


def test_existing_dates_are_not_reinserted(socrata_rows):
    existing = {(f"cftc.13874A.{m}", date(2026, 9, 15)) for m in COT_METRICS}
    engine, inserted = _make_engine(existing)
    _pull(engine, "13874A", _rows_for(socrata_rows, "13874A"), start_date="2026-09-01")
    assert {p["od"] for p in inserted} == {date(2026, 9, 22)}


# ── scheduled run never performs the full backfill ──────────────────────


def test_scheduled_run_bootstraps_recent_window_only():
    engine, _ = _make_engine()
    result, fetch = _pull(engine, "13874A", [])
    start = fetch.call_args.args[1]
    assert result["mode"] == "bootstrap"
    assert date.today() - start == timedelta(days=cftc_cot._BOOTSTRAP_LOOKBACK_DAYS)


def test_scheduled_run_is_incremental_when_history_exists():
    last = date(2026, 9, 15)
    engine, _ = _make_engine({(f"cftc.13874A.{m}", last) for m in COT_METRICS})
    result, fetch = _pull(engine, "13874A", [])
    assert result["mode"] == "incremental"
    assert fetch.call_args.args[1] == last - timedelta(days=7)


def test_dry_run_writes_nothing(socrata_rows):
    engine, inserted = _make_engine()
    result, _ = _pull(engine, "13874A", _rows_for(socrata_rows, "13874A"),
                      start_date="2026-09-01", dry_run=True)
    assert inserted == []
    assert result["rows_would_insert"] == 2 * len(COT_METRICS)
    engine.begin.assert_not_called()


def test_backfill_script_defaults_to_dry_run():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "backfill_cftc_market_codes", REPO / "scripts" / "backfill_cftc_market_codes.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    args = mod.parse_args([])
    assert args.dry_run is True
    assert args.start == date(2006, 1, 1)
    assert mod.parse_args(["--execute"]).dry_run is False
    with pytest.raises(SystemExit):
        mod.parse_args(["--codes", "SP500"])

    puller = MagicMock()
    mod.run(puller, mod.parse_args(["--codes", "13874A"]))
    puller.pull_all.assert_called_once_with(
        market_codes=["13874A"], start_date=date(2006, 1, 1), dry_run=True)


# ── publication time ────────────────────────────────────────────────────


def _utc(*a) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


@pytest.mark.parametrize("report,expected_utc,shifted", [
    # normal week, EDT: Friday 15:30 ET = 19:30Z
    (date(2026, 9, 22), _utc(2026, 9, 25, 19, 30), False),
    # normal week, EST: 20:30Z
    (date(2026, 12, 1), _utc(2026, 12, 4, 20, 30), False),
    # Monday holiday before the report date does not shift (Labor Day)
    (date(2026, 9, 8), _utc(2026, 9, 11, 19, 30), False),
    # Thanksgiving Thu -> Monday (CFTC 2026 schedule: Nov 30*)
    (date(2026, 11, 24), _utc(2026, 11, 30, 20, 30), True),
    # Veterans Day Wed -> Monday (Nov 16*)
    (date(2026, 11, 10), _utc(2026, 11, 16, 20, 30), True),
    # Juneteenth on the Friday -> Monday (Jun 22*)
    (date(2026, 6, 16), _utc(2026, 6, 22, 19, 30), True),
    # July 4 observed Fri Jul 3 -> Monday (Jul 6*)
    (date(2026, 6, 30), _utc(2026, 7, 6, 19, 30), True),
    # New Year's Day across the year boundary (Jan 5*)
    (date(2025, 12, 30), _utc(2026, 1, 5, 20, 30), True),
])
def test_release_time(report, expected_utc, shifted):
    rel = compute_release(report)
    assert rel.release_at == expected_utc
    assert rel.holiday_shifted is shifted
    assert rel.reason is None


def test_monday_or_wednesday_report_date_now_has_a_computed_release_time():
    """#682 fix: a Monday/Wednesday report date (the CFTC's known holiday-shift

    pattern) now resolves via ``CONFIRMED_HOLIDAY_RELEASES`` or the
    conservative fallback instead of always returning ``None``.
    """
    confirmed = compute_release(date(2025, 11, 10))  # Monday; in the confirmed table
    assert confirmed.release_at is not None
    assert confirmed.holiday_shifted is True

    fallback = compute_release(date(2009, 11, 9))  # Monday; not in the confirmed table
    assert fallback.release_at is not None
    assert fallback.holiday_shifted is True


def test_report_date_with_no_rule_at_all_has_no_release_time():
    """A report date that is neither Tuesday, Monday, nor Wednesday (never

    observed in real CFTC data) still has no computable release.
    """
    rel = compute_release(date(2018, 12, 27))  # a synthetic Thursday
    assert rel.release_at is None
    assert "not a Tuesday, Monday, or Wednesday" in rel.reason
    assert rel.to_payload()["release_at"] is None


def test_payload_carries_release_for_holiday_week(socrata_rows):
    row = dict(_rows_for(socrata_rows, "13874A")[0], report_date_as_yyyy_mm_dd="2026-11-24T00:00:00.000")
    engine, inserted = _make_engine()
    _pull(engine, "13874A", [row], start_date="2026-11-01")
    payload = json.loads(inserted[0]["payload"])
    assert payload["report_date"] == "2026-11-24"
    assert payload["release_at"] == "2026-11-30T20:30:00+00:00"
    assert payload["release_at_et"] == "2026-11-30T15:30:00-05:00"
    assert payload["release_holiday_shifted"] is True


# ── consumers ───────────────────────────────────────────────────────────


def test_cot_extremes_unavailable_without_code_keyed_ids():
    """Legacy mixed-market rows exist; code-keyed rows do not -> unavailable."""
    from intelligence.cot_extremes import scan_all_extremes, scan_extremes_report
    from tests.test_raw_reads_migration import _engine, _row

    today = date.today()
    rows = [_row(f"cftc.{k}.{m}", today - timedelta(weeks=w), 1000.0 + w * (i + 1), h=w)
            for i, k in enumerate(("SP500", "GOLD"))
            for m in ("net_speculative", "noncommercial_long", "noncommercial_short")
            for w in range(1, 120)]
    eng = _engine(rows)

    report = scan_extremes_report(eng)
    assert report["available"] is False
    assert report["status"] == "unavailable"
    assert report["extremes"] == []
    assert {u["reason"] for u in report["unavailable"]} == {"no code-keyed rows"}
    assert scan_all_extremes(eng) == []


def test_cot_extremes_reads_code_keyed_ids_and_refuses_legacy_keys():
    from intelligence import cot_extremes

    today = date.today()
    history = [(today - timedelta(weeks=k), float(k)) for k in range(100, 0, -1)]
    history.append((today, 500.0))
    seen: list[str] = []

    def fake_read(engine, sid, **kw):
        seen.append(sid)
        return history

    with patch.object(cot_extremes, "_read_series_history", side_effect=fake_read):
        rep = cot_extremes.scan_extremes_report(MagicMock(), metrics=["net_speculative"])
        legacy = cot_extremes.scan_extremes_report(
            MagicMock(), contracts=["SP500", "GOLD"], metrics=["net_speculative"])

    assert seen == [f"cftc.{c}.net_speculative" for c in MARKETS]
    assert rep["available"] is True
    es = next(e for e in rep["extremes"] if e.market_code == "13874A")
    assert es.contract == "ES" and es.severity == "extreme"
    assert es.to_dict()["market_code"] == "13874A"
    assert legacy["available"] is False
    assert all("legacy" in u["reason"] for u in legacy["unavailable"])


def test_cot_extremes_short_history_is_unavailable_not_scored():
    from intelligence import cot_extremes

    short = [(date.today() - timedelta(weeks=k), float(k)) for k in range(8, 0, -1)]
    with patch.object(cot_extremes, "_read_series_history", return_value=short):
        rep = cot_extremes.scan_extremes_report(
            MagicMock(), contracts=["13874A"], metrics=["net_speculative"])
    assert rep["available"] is False
    assert rep["unavailable"][0]["reason"].startswith("insufficient history")


def test_thesis_scorer_cftc_reads_code_keyed_ids_and_is_no_data_without_them():
    from analysis.thesis_scorer import _score_cftc_positioning

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    sids: list[str] = []

    def execute(stmt, params=None):
        sids.append(params["sid"])
        res = MagicMock()
        res.fetchone.return_value = None
        return res

    conn.execute.side_effect = execute
    verdict = _score_cftc_positioning(engine, 0.5)
    assert verdict["status"] == "no_data"
    assert set(sids) == {series_id_for_root(r, "net_speculative") for r in ("ES", "GC", "CL", "VX")}


# ── guard: analytical code never reads the legacy mixed-market ids ─────

_LEGACY_ID = re.compile(
    r"cftc\.(" + "|".join(sorted(LEGACY_CONTRACT_KEYS, key=len, reverse=True)) + r")\.[a-z_]+"
)
_DYNAMIC_ID = re.compile(r"f[\"']cftc\.\{")
_SCANNED = ("api", "intelligence", "analysis", "physics", "oracle", "trading",
            "valuation", "features", "alerts", "store", "godview", "normalization")


def test_no_analytical_reader_uses_legacy_cftc_ids():
    offenders: list[str] = []
    for root in _SCANNED:
        base = REPO / root
        if not base.exists():
            continue
        for f in base.rglob("*.py"):
            text = f.read_text(encoding="utf-8", errors="ignore")
            for pat in (_LEGACY_ID, _DYNAMIC_ID):
                for m in pat.finditer(text):
                    line = text.count("\n", 0, m.start()) + 1
                    offenders.append(f"{f.relative_to(REPO).as_posix()}:{line}: {m.group(0)}")
    assert not offenders, (
        "Read CFTC series through ingestion.altdata.cftc_markets.series_id / "
        "series_id_for_root (cftc.<market_code>.<metric>); the legacy "
        "name-keyed ids mix markets:\n" + "\n".join(offenders)
    )


# ── GRID task A1 (coordinator follow-up): a missed scheduler week must ──
# still be recovered by the puller's own forward fetch, since
# ingestion/smart_scheduler.py's cftc_cot gate (_cftc_cot_is_due) is now
# fail-closed to a Friday/holiday-release + 1-day retry window and will
# NOT fire again mid-week if that window is missed entirely (e.g. grid-svr
# down through the whole window). The gate can only be safe to leave that
# way if the next successful run still picks up every report published
# since the last success, not just the latest one.


def test_incremental_start_ignores_gap_size_and_rewinds_to_last_stored_date():
    """CFTCCOTPuller._incremental_start must key off the oldest per-metric
    stored date minus the fixed overlap -- not "since the last scheduled
    tick" -- so a scheduler gap of any length (one missed Friday, or a
    month of downtime) is closed by the very next successful run.
    """
    puller = CFTCCOTPuller.__new__(CFTCCOTPuller)
    stale = date(2026, 8, 7)  # ~7 weeks before a hypothetical "now"
    puller._get_latest_date = lambda series_id: stale

    start, mode = puller._incremental_start("13874A")

    assert mode == "incremental"
    assert start == stale - timedelta(days=cftc_cot._INCREMENTAL_OVERLAP_DAYS)


def test_fetch_market_page_query_has_no_upper_date_bound():
    """The Socrata $where clause is an open-ended ">= start_date" with no
    end date, so one call from an old `start_date` returns every report
    published since then -- an arbitrary number of missed weeks, not just
    the most recent one. This is what makes the scheduler's fail-closed
    Friday/Saturday-only gate (which never fires mid-week to "catch up")
    safe: the eventual next Friday run still backfills the gap.
    """
    puller = CFTCCOTPuller.__new__(CFTCCOTPuller)
    captured: dict = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        captured["params"] = params
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        resp.raise_for_status.return_value = None
        return resp

    with patch.object(cftc_cot.requests, "get", side_effect=fake_get):
        puller._fetch_market_page("13874A", date(2026, 8, 7), offset=0)

    where = captured["params"]["$where"]
    assert "report_date_as_yyyy_mm_dd >= '2026-08-07'" in where
    assert "<=" not in where and " < " not in where
