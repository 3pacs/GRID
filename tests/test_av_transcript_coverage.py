"""Offline contract tests. Run with --noconftest; no provider/DB credentials."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.av_transcript_plan import TranscriptPeriod, TranscriptState, verified_periods
from ingestion.earnings_events_puller import EarningsEventsPuller


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Network access forbidden in offline tests")
    monkeypatch.setattr("socket.socket.connect", denied)


def fact(year=2026, fp="Q3", end="2026-06-27", filed="2026-07-31", accession="a"):
    return dict(fiscal_year=year, fiscal_period=fp, period_end=end, filed=filed, accession=accession)


def filing(p, form="10-Q"):
    return dict(norm_form=form, accession=p["accession"],
                period=date.fromisoformat(p["period_end"]), filed=date.fromisoformat(p["filed"]))


@pytest.mark.parametrize("p,form,quarter", [
    (fact(), "10-Q", "2026Q3"),  # Apple June quarter, not calendar Q2.
    (fact(2027, "Q1", "2026-04-26", "2026-05-28"), "10-Q", "2027Q1"),
    (fact(2026, "FY", "2026-01-31", "2026-03-15"), "10-K", "2026Q4"),
    (fact(2026, "Q1", "2026-03-31", "2026-04-30"), "10-Q", "2026Q1"),
])
def test_fiscal_not_calendar(p, form, quarter):
    result = verified_periods([p], [filing(p, form)], date(2026, 11, 7))
    assert result[0].quarter == quarter
    assert result[0].period_end == date.fromisoformat(p["period_end"])


@pytest.mark.parametrize("change", [
    {"fiscal_year": None}, {"fiscal_year": True}, {"fiscal_year": "bad"},
    {"fiscal_year": 2099}, {"fiscal_period": "Q9"}, {"fiscal_period": "FY"},
    {"period_end": "2025-06-27"}, {"accession": "comparative"},
    {"filed": "2026-08-01"}, {"period_end": "bad"},
])
def test_unverified_metadata_is_not_requested(change):
    p = fact()
    assert verified_periods([{**p, **change}], [filing(p)], date(2026, 11, 7)) == []


def test_conflicting_labels_fail_closed():
    p = fact()
    assert verified_periods([p, {**p, "fiscal_period": "Q2"}], [filing(p)], date(2026, 11, 7)) == []


def test_future_filing_is_not_known():
    p = fact(filed="2026-12-01")
    assert verified_periods([p], [filing(p)], date(2026, 11, 7)) == []


def test_amendments_use_original_stable_date():
    p = fact()
    amended = {**p, "filed": "2026-08-01", "accession": "b"}
    result = verified_periods([amended, p], [filing(amended), filing(p)], date(2026, 11, 7))
    assert len(result) == 1 and result[0].filed == date(2026, 7, 31)


def test_persisted_rotation_including_empty_and_failed_attempts(tmp_path):
    path = tmp_path / "state.db"
    keys = ["AAA|7973|1", "AAA|7973|2", "ZZZ|7973|1"]
    attempted = []
    for n in range(7):
        # New process each time; reservation itself advances, regardless of response.
        state = TranscriptState(path, 5, None)
        attempted.append(state.reserve(keys, now=1000+n*20)[0])
    assert attempted == keys * 2 + keys[:1]


def test_changed_universe_resume_and_wrap(tmp_path):
    state = TranscriptState(tmp_path / "state.db", 5, None)
    assert state.reserve(["AAA", "BBB"], 1000)[0] == "AAA"
    assert state.reserve(["AAC", "CCC"], 1020)[0] == "AAC"
    assert state.reserve(["CCC"], 1040)[0] == "CCC"
    assert state.reserve(["AAA", "CCC"], 1060)[0] == "AAA"


def test_daily_budget_pacing_and_utc_rollover(tmp_path):
    state = TranscriptState(tmp_path / "state.db", 120, 2)
    assert state.reserve(["a", "b"], 1000) == ("a", 0)
    assert state.reserve(["a", "b"], 1000) == ("b", .5)
    assert TranscriptState(state.path, 120, 2).reserve(["a"], 1001) is None
    assert state.reserve(["a"], 86400) == ("a", 0)


def test_never_charge_tomorrow_slot_to_today(tmp_path):
    state = TranscriptState(tmp_path / "state.db", 5, None)
    assert state.reserve(["a"], 86399) == ("a", 0)
    assert state.reserve(["b"], 86399) is None


def test_concurrent_reservations_honor_shared_budget(tmp_path):
    path = tmp_path / "state.db"
    TranscriptState(path, 120, 7)
    def reserve(_):
        return TranscriptState(path, 120, 7).reserve(["a", "b", "c"], 1000)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = [r for r in pool.map(reserve, range(20)) if r]
    assert len(results) == 7
    assert sorted(r[1] for r in results) == [n*.5 for n in range(7)]


@pytest.mark.parametrize("rate", [0, -1, float("nan"), float("inf")])
def test_invalid_rate_fails_closed(tmp_path, rate):
    with pytest.raises(ValueError):
        TranscriptState(tmp_path / "state.db", rate, None)


@pytest.fixture
def puller(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.base.BasePuller._resolve_source_id", lambda self: 1)
    monkeypatch.setattr("ingestion.earnings_events_puller.time.sleep", lambda _: None)
    p = EarningsEventsPuller(MagicMock(), av_api_key="synthetic-secret", av_max_calls=2,
                             av_state_path=tmp_path / "state.db")
    monkeypatch.setattr(p, "_classify_transcript_sentiment", lambda _: (None, None))
    return p


def response(monkeypatch, data=None, status=200):
    r = MagicMock(status_code=status)
    r.json.return_value = data
    monkeypatch.setattr("ingestion.earnings_events_puller.requests.get", lambda *a, **kw: r)
    return r


@pytest.mark.parametrize("status", [401, 403, 429])
def test_http_account_failure_stops(puller, monkeypatch, status):
    response(monkeypatch, status=status)
    assert puller.fetch_av_transcript("AAPL", "2026Q3") is None
    assert puller._av_stop_reason == f"http_{status}"


@pytest.mark.parametrize("data", [{"Note": "slow down"}, {"Information": "premium required"}])
def test_provider_notice_stops_without_assuming_quota(puller, monkeypatch, data):
    response(monkeypatch, data)
    assert puller.fetch_av_transcript("AAPL", "2026Q3") is None
    assert puller._av_stop_reason == "provider_notice"


@pytest.mark.parametrize("data", [None, [], {},
    {"symbol": "MSFT", "quarter": "2026Q3", "transcript": [{"content": "hello"}]},
    {"symbol": "AAPL", "quarter": "2026Q2", "transcript": [{"content": "hello"}]},
    {"symbol": "AAPL", "quarter": "2026Q3", "transcript": ["bad"]},
    {"symbol": "AAPL", "quarter": "2026Q3", "transcript": [{"content": " "}]},
])
def test_invalid_or_empty_payload_not_stored(puller, monkeypatch, data):
    response(monkeypatch, data)
    assert puller.fetch_av_transcript("AAPL", "2026Q3") is None


def test_http_exception_never_logs_key(puller, monkeypatch):
    from loguru import logger
    logs = []
    def failed(*a, **kw):
        raise requests.Timeout("https://example/?apikey=synthetic-secret")
    monkeypatch.setattr("ingestion.earnings_events_puller.requests.get", failed)
    sink = logger.add(logs.append)
    try:
        assert puller.fetch_av_transcript("AAPL", "2026Q3") is None
    finally:
        logger.remove(sink)
    assert "synthetic-secret" not in "".join(logs)


def setup_plans(puller, monkeypatch, tickers=("AAA", "ZZZ")):
    p = TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "a")
    plans = {ticker: (ticker, p) for ticker in tickers}
    counts = {ticker: {"transcripts_inserted": 0} for ticker in tickers}
    return plans, counts


def test_success_preserves_actual_period_and_declares_date_basis(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch, ("AAPL",))
    response(monkeypatch, {"symbol": "AAPL", "quarter": "2026Q3", "transcript": [{"content": "hello"}]})
    upsert = MagicMock(return_value=True)
    monkeypatch.setattr(puller, "_upsert_event", upsert)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 1)
    kwargs = upsert.call_args.kwargs
    assert kwargs["period_end"] == date(2026, 6, 27)
    assert kwargs["filing_date"] == date(2026, 7, 31)
    assert kwargs["raw_payload"]["filing_date_basis"] == "matched_sec_filing_not_call_date"
    assert "synthetic-secret" not in str(kwargs)


def test_provider_failure_makes_only_one_call(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch)
    r = response(monkeypatch, {"Information": "stop"})
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 0)
    assert r.json.call_count == 1


def test_state_failure_never_falls_back_to_unbounded_run(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch)
    puller.av_state_path = puller.av_state_path.parent  # directory, not a database
    fetch = MagicMock()
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (0, 0)
    fetch.assert_not_called()
    assert puller._av_stop_reason == "state_or_storage_error"


def test_pull_keeps_sec_coverage_and_no_calendar_fallback(puller, monkeypatch):
    monkeypatch.setattr(puller, "_load_universe", lambda *a, **kw: ["AAPL", "ZZZ"])
    monkeypatch.setattr(puller, "load_cik_map", lambda: {"AAPL": "1", "ZZZ": "2"})
    sec = MagicMock(return_value={"filings": {"recent": {}}})
    monkeypatch.setattr(puller, "fetch_submissions", sec)
    monkeypatch.setattr(puller, "_load_verified_periods", lambda *a: [])
    monkeypatch.setattr(puller, "_stored_transcript_periods", lambda *a: (set(), set()))
    fetch = MagicMock()
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    result = puller.pull()
    assert sec.call_count == 2
    assert result["av_skipped_period_unknown"] == 2
    assert result["av_calls"] == 0
    fetch.assert_not_called()


def test_new_quarter_not_hidden_by_recent_transcript(puller, monkeypatch):
    p = TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "a")
    monkeypatch.setattr(puller, "_load_universe", lambda *a, **kw: ["AAPL"])
    monkeypatch.setattr(puller, "load_cik_map", lambda: {})
    monkeypatch.setattr(puller, "_load_verified_periods", lambda *a: [p])
    monkeypatch.setattr(puller, "_stored_transcript_periods", lambda *a: ({("2026Q2", date(2026, 3, 28))}, set()))
    fetch = MagicMock(return_value=None)
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    assert puller.pull()["av_calls"] == 1
    fetch.assert_called_once_with("AAPL", "2026Q3")


def test_existing_exact_period_skipped_even_full(puller, monkeypatch):
    p = TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "a")
    monkeypatch.setattr(puller, "_load_universe", lambda *a, **kw: ["AAPL"])
    monkeypatch.setattr(puller, "load_cik_map", lambda: {})
    monkeypatch.setattr(puller, "_load_verified_periods", lambda *a: [p])
    monkeypatch.setattr(puller, "_stored_transcript_periods", lambda *a: ({("2026Q3", p.period_end)}, set()))
    result = puller.pull(mode="full")
    assert result["av_calls"] == 0 and result["av_skipped_stored_periods"] == 1


@pytest.mark.parametrize("raw,verified", [
    ({}, False), (None, False), ("bad-json", False),
    ({"observed_at": "2026-08-01T00:00:00+00:00",
      "filing_date_basis": "matched_sec_filing_not_call_date", "fiscal_period_accession": "a"}, True),
    ({"observed_at": "2026-08-01T00:00:00+00:00",
      "filing_date_basis": "matched_sec_filing_not_call_date", "fiscal_period_accession": "wrong"}, False),
    ({"observed_at": "2026-08-01T00:00:00", "filing_date_basis": "matched_sec_filing_not_call_date",
      "fiscal_period_accession": "a"}, False),
])
def test_legacy_storage_not_verified_coverage(puller, raw, verified):
    p = TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "a")
    puller.engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
        (p.quarter, p.period_end, p.filed, raw)]
    good, legacy = puller._stored_transcript_periods("AAPL", [p])
    assert bool(good) is verified
    assert bool(legacy) is not verified


def test_legacy_date_requires_reconciliation_not_duplicate(puller, monkeypatch):
    p = TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31), "a")
    monkeypatch.setattr(puller, "_load_universe", lambda *a, **kw: ["AAPL"])
    monkeypatch.setattr(puller, "load_cik_map", lambda: {})
    monkeypatch.setattr(puller, "_load_verified_periods", lambda *a: [p])
    monkeypatch.setattr(puller, "_stored_transcript_periods", lambda *a: (set(), {(p.quarter, p.period_end)}))
    result = puller.pull(mode="full")
    assert result["av_calls"] == result["av_skipped_stored_periods"] == 0
    assert result["av_legacy_unverified_periods"] == 1
    assert result["per_ticker"]["AAPL"]["av_legacy_unverified"] == 1


def test_per_run_cap_advances_next_run(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch)
    puller.av_max_calls = 1
    fetch = MagicMock(return_value=None)
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 0)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 0)
    assert [c.args[0] for c in fetch.call_args_list] == ["AAA", "ZZZ"]


def test_zero_budget_does_not_create_state_or_call_provider(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch)
    puller.av_max_calls = 0
    fetch = MagicMock()
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (0, 0)
    assert not puller.av_state_path.exists()
    fetch.assert_not_called()


def test_persisted_daily_cap_blocks_second_run(puller, monkeypatch):
    plans, counts = setup_plans(puller, monkeypatch)
    puller.av_daily_limit = 1
    fetch = MagicMock(return_value=None)
    monkeypatch.setattr(puller, "fetch_av_transcript", fetch)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (1, 0)
    assert puller._pull_planned_transcripts(plans, counts, {}) == (0, 0)
    fetch.assert_called_once()


def test_verified_period_query_uses_existing_sources_and_parameters(puller):
    p = fact()
    execute = puller.engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.fetchall.return_value = [(p,)]
    assert puller._load_verified_periods("BRK-B", [filing(p)])[0].quarter == "2026Q3"
    sql, params = execute.call_args.args
    assert "ANY(:series)" in str(sql)
    assert params["source"] == "SEC_EDGAR_Fundamentals"
    assert "sec_filed_fundamentals.BRK.B.total_assets" in params["series"]
