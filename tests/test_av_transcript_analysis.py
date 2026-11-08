"""Offline AV availability/source contracts; no DB or provider traffic."""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from intelligence.earnings_transcript_analyzer import EarningsTranscriptAnalyzer

UTC = timezone.utc
OBSERVED = datetime(2026, 8, 2, 15, tzinfo=UTC)
AS_OF = datetime(2026, 8, 3, tzinfo=UTC)
BODY = "We expect strong revenue growth. Management remains confident. " * 20


def row(**overrides):
    values = dict(event_id=7, ticker="AAPL", filed=date(2026, 7, 31),
                  end=date(2026, 6, 27), quarter="2026Q3", body=BODY,
                  raw={"observed_at": OBSERVED.isoformat(),
                       "filing_date_basis": "matched_sec_filing_not_call_date",
                       "fiscal_period_source": "SEC_EDGAR_Fundamentals",
                       "fiscal_period_accession": "0001-26-01",
                       "symbol": "AAPL", "quarter": "2026Q3"},
                  created=OBSERVED)
    values.update(overrides)
    return tuple(values.values())


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*a, **kw):
        raise AssertionError("Network forbidden in offline AV tests")
    monkeypatch.setattr("socket.create_connection", denied)
    monkeypatch.setattr("socket.socket.connect", denied)


@pytest.fixture
def analyzer(monkeypatch):
    monkeypatch.setattr(EarningsTranscriptAnalyzer, "_ensure_table", lambda self: None)
    obj = EarningsTranscriptAnalyzer(MagicMock())
    obj._store_analysis = MagicMock(side_effect=AssertionError("AV persistence forbidden"))
    obj._get_prior_tone = MagicMock(side_effect=AssertionError("Legacy prior tone forbidden"))
    return obj


def use_rows(analyzer, rows):
    analyzer.engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows


def test_verified_av_readthrough_has_distinct_dates_and_no_writes(analyzer):
    use_rows(analyzer, [row()])
    result = analyzer.run_av_analysis(as_of=AS_OF)
    a, = result["analyses"]
    assert result["status"] == "ok" and result["stored"] == 0
    assert a["period_end"] == "2026-06-27"
    assert a["filing_date"] == "2026-07-31"
    assert a["call_date"] is None and a["call_date_basis"] == "unknown"
    assert a["known_at"] == OBSERVED.isoformat()
    assert a["tone_shift"] is None and a["prior_tone"] is None
    assert a["source"] == "ALPHAVANTAGE" and a["fiscal_period_accession"] == "0001-26-01"
    assert datetime.fromisoformat(a["computed_at"]) > AS_OF
    analyzer._store_analysis.assert_not_called()
    analyzer.engine.begin.assert_not_called()


@pytest.mark.parametrize("as_of", [
    datetime(2026, 6, 27, tzinfo=UTC), datetime(2026, 7, 31, tzinfo=UTC),
    OBSERVED - timedelta(microseconds=1),
])
def test_fiscal_and_sec_dates_do_not_backdate_av_availability(analyzer, as_of):
    use_rows(analyzer, [row()])
    assert analyzer.run_av_analysis(as_of=as_of)["analyses"] == []


def test_exact_observation_boundary_is_inclusive(analyzer):
    use_rows(analyzer, [row()])
    assert len(analyzer.run_av_analysis(as_of=OBSERVED)["analyses"]) == 1


def test_storage_later_than_retrieval_is_conservative_known_at(analyzer):
    later = AS_OF + timedelta(hours=1)
    use_rows(analyzer, [row(created=later)])
    assert not analyzer.run_av_analysis(as_of=AS_OF)["analyses"]
    assert analyzer.run_av_analysis(as_of=later)["analyses"][0]["known_at"] == later.isoformat()


@pytest.mark.parametrize("change", [
    {"observed_at": None}, {"observed_at": "bad"},
    {"observed_at": "2026-08-02T15:00:00"},
    {"observed_at": "2099-01-01T00:00:00+00:00"},
    {"filing_date_basis": "call_date"}, {"fiscal_period_source": "guessed"},
    {"fiscal_period_accession": ""}, {"symbol": "MSFT"}, {"quarter": "2026Q2"},
])
def test_unverified_or_future_metadata_rejected(analyzer, change):
    raw = {**row()[6], **change}
    use_rows(analyzer, [row(raw=raw)])
    assert not analyzer.run_av_analysis(as_of=AS_OF)["analyses"]


@pytest.mark.parametrize("raw", [None, {}, "not json", []])
def test_legacy_payload_not_analyzed(analyzer, raw):
    use_rows(analyzer, [row(raw=raw)])
    assert not analyzer.run_av_analysis(as_of=AS_OF)["analyses"]


def test_naive_storage_time_rejected(analyzer):
    use_rows(analyzer, [row(created=OBSERVED.replace(tzinfo=None))])
    assert not analyzer.run_av_analysis(as_of=AS_OF)["analyses"]


@pytest.mark.parametrize("as_of", [AS_OF.replace(tzinfo=None), datetime(2099, 1, 1, tzinfo=UTC)])
def test_invalid_as_of_fails_before_query(analyzer, as_of):
    with pytest.raises(ValueError):
        analyzer.run_av_analysis(as_of=as_of)
    analyzer.engine.connect.assert_not_called()


def test_lookback_uses_availability_not_old_fiscal_period(analyzer):
    use_rows(analyzer, [row(end=date(2024, 6, 29), filed=date(2024, 7, 31))])
    assert len(analyzer.run_av_analysis(as_of=AS_OF, days_back=1)["analyses"]) == 1
    assert not analyzer.run_av_analysis(as_of=AS_OF + timedelta(days=5), days_back=1)["analyses"]


def test_filters_tickers_and_deduplicates_periods(analyzer):
    use_rows(analyzer, [row(), row(event_id=8), row(ticker="MSFT")])
    result = analyzer.run_av_analysis(tickers=["aapl"], as_of=AS_OF)
    a, = result["analyses"]
    assert a["event_id"] == 8


def test_source_identity_does_not_collide_with_legacy(analyzer):
    use_rows(analyzer, [row()])
    av = analyzer.run_av_analysis(as_of=AS_OF)["analyses"][0]
    legacy = analyzer.analyze_transcript("AAPL", date(2026, 7, 31), BODY, compare_prior=False)
    assert av["analysis_id"] != legacy.analysis_id


def test_query_failure_is_explicit_not_empty_success(analyzer):
    analyzer.engine.connect.side_effect = RuntimeError("synthetic")
    result = analyzer.run_av_analysis(as_of=AS_OF)
    assert result["status"] == "unavailable" and result["stored"] == 0


def test_existing_run_includes_av_without_counting_as_stored(analyzer, monkeypatch):
    use_rows(analyzer, [])
    expected = {"status": "ok", "analyses": [{"source": "ALPHAVANTAGE"}], "stored": 0}
    monkeypatch.setattr(analyzer, "run_av_analysis", lambda *args: expected)
    result = analyzer.run_analysis()
    assert result["analyzed"] == result["stored"] == 0
    assert result["av"] == expected


def test_av_still_available_when_edgar_query_fails(analyzer, monkeypatch):
    analyzer.engine.connect.side_effect = RuntimeError("EDGAR unavailable")
    expected = {"status": "ok", "analyses": [], "stored": 0}
    monkeypatch.setattr(analyzer, "run_av_analysis", lambda *args: expected)
    result = analyzer.run_analysis()
    assert "error" in result and result["av"] == expected


def test_query_is_parameterized_and_source_scoped(analyzer):
    use_rows(analyzer, [])
    analyzer.run_av_analysis(as_of=AS_OF)
    query, params = analyzer.engine.connect.return_value.__enter__.return_value.execute.call_args.args
    assert "source = 'ALPHAVANTAGE'" in str(query)
    assert "event_type = 'EARNINGS_CALL'" in str(query)
    assert "created_at <= :as_of" in str(query)
    assert params == {"as_of": AS_OF}
