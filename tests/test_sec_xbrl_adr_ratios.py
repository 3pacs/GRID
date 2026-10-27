"""ADR ratio table for the SEC XBRL shares puller (ticker_metrics_daily).

``shares_outstanding`` in ticker_metrics_daily is stored per US-listed unit
(ordinary shares / ratio) and ``market_cap_usd = shares * close`` where the
close is YF:{TICKER}:close from raw_series, which is stored UNADJUSTED. So the
ratio must be the one in force on each obs_date.

Sources for the corrected entries (2026-09-28):
  * SNY  — Sanofi 20-F FY2025 exhibit 2.2: each ADS represents one-half of one
           ordinary share (JPMorgan depositary). 0.5, not 2.
  * AZN  — 6-K 2015-06-26: 1 ADS : 1 share -> 2 ADSs : 1 share from 2015-07-27.
           6-K 2026-01-20: ADSs leave Nasdaq 2026-01-30, ordinary shares trade
           on the NYSE from 2026-02-02 (ratio 1 from then on).
  * HEINY — Heineken ADR page: two ADRs represent one ordinary share. 0.5.

Added 2026-09-29:
  * GSK  — 20-F FY2025 cover: "American Depositary Shares, each representing
           2 Ordinary Shares" (same wording on the FY2022 cover). Was missing -> 1.
  * SHEL — 20-F FY2025 cover: "American Depositary Shares representing two
           ordinary shares" (same on FY2021). Was missing -> 1.
  * SONY — 20-F FY2025 cover: each ADS represents one share of Common Stock.
  * TM   — 20-F filed 2021-06-24: each ADS represents two shares; 5-for-1
           split effective 2021-10-01; 20-F filed 2022-06-23 onward: ten shares.
"""

from __future__ import annotations

from datetime import date

import pytest

import ingestion.altdata.sec_xbrl_shares as mod
from ingestion.altdata.sec_xbrl_shares import (
    _ADR_RATIO_HISTORY,
    _ADR_RATIOS,
    _adr_ratio_for,
)

# ── Static table ──

def test_sny_is_half_a_share_per_ads():
    assert _adr_ratio_for("SNY") == 0.5
    assert _adr_ratio_for("SNY", date(2026, 9, 25)) == 0.5
    assert _adr_ratio_for("sny ", date(2003, 1, 2)) == 0.5


def test_heiny_is_half_a_share_per_adr():
    assert _adr_ratio_for("HEINY") == 0.5


def test_azn_current_ratio_is_one_after_direct_listing():
    assert _adr_ratio_for("AZN") == 1.0


@pytest.mark.parametrize(
    "obs, expected",
    [
        (date(2010, 1, 4), 1.0),
        (date(2015, 7, 24), 1.0),   # last session before the 2015 change
        (date(2015, 7, 27), 0.5),   # 2 ADSs : 1 share effective
        (date(2020, 6, 1), 0.5),
        (date(2026, 1, 30), 0.5),   # last Nasdaq ADS session
        (date(2026, 2, 2), 1.0),    # first NYSE ordinary-share session
        (date(2026, 9, 25), 1.0),
    ],
)
def test_azn_ratio_is_point_in_time(obs, expected):
    assert _adr_ratio_for("AZN", obs) == expected


def test_gsk_and_shel_ads_represent_two_ordinary_shares():
    for t in ("GSK", "SHEL"):
        assert _adr_ratio_for(t) == 2.0
        assert _adr_ratio_for(t, date(2026, 9, 25)) == 2.0
        assert _adr_ratio_for(t.lower(), date(2022, 3, 1)) == 2.0


def test_sony_ads_is_one_share():
    assert "SONY" in _ADR_RATIOS
    assert _adr_ratio_for("SONY", date(2026, 9, 25)) == 1.0


@pytest.mark.parametrize(
    "obs, expected",
    [
        (date(2005, 1, 3), 2.0),
        (date(2021, 9, 30), 2.0),   # record date, last pre-split session
        (date(2021, 10, 1), 10.0),  # 5-for-1 split effective; ADSs unchanged
        (date(2026, 5, 11), 10.0),
    ],
)
def test_tm_ratio_is_point_in_time(obs, expected):
    assert _adr_ratio_for("TM", obs) == expected
    assert _adr_ratio_for("TM") == 10.0


def test_undated_tickers_ignore_obs_date_and_unknown_defaults_to_one():
    assert _adr_ratio_for("TSM", date(2001, 1, 2)) == 5.0
    assert _adr_ratio_for("TSM", date(2026, 9, 25)) == 5.0
    assert _adr_ratio_for("AAPL", date(2026, 9, 25)) == 1.0
    assert _adr_ratio_for("") == 1.0


def test_ratio_history_invariants():
    for ticker, history in _ADR_RATIO_HISTORY.items():
        dates = [d for d, _r in history]
        assert dates[0] == date.min, ticker
        assert dates == sorted(dates) and len(set(dates)) == len(dates), ticker
        assert all(r > 0 for _d, r in history), ticker
        # The undated/current table must agree with the latest history entry.
        assert float(_ADR_RATIOS[ticker]) == float(history[-1][1]), ticker


def test_all_ratios_positive():
    assert all(float(r) > 0 for r in _ADR_RATIOS.values())


# ── Puller applies the ratio per obs_date ──

class _FakeEngine:  # never touched: every DB helper is monkeypatched
    pass


def _run_puller(monkeypatch, ticker, ordinary_shares, closes):
    written: dict[str, list[dict]] = {}
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", lambda: {ticker: "0000000001"})
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _cik: {"facts": {}})
    monkeypatch.setattr(
        mod, "_extract_shares_entries",
        lambda _facts: [(date(2015, 1, 1), ordinary_shares)],
    )
    monkeypatch.setattr(mod, "_fetch_close_prices", lambda *_a, **_k: closes)

    def _capture(_engine, t, rows, **_controls):
        written[t] = rows
        return len(rows)

    monkeypatch.setattr(mod, "_write_rows", _capture)
    mod.SECXBRLSharesPuller(_FakeEngine()).pull_all(
        tickers=[ticker], backfill_days=4000,
    )
    return {r["obs_date"]: r for r in written[ticker]}


def test_puller_uses_azn_ratio_in_force_on_each_date(monkeypatch):
    # Real YF:AZN:close values: ADS price on 01-30, ordinary-share price on 02-02.
    rows = _run_puller(
        monkeypatch, "AZN", 1_549_000_000,
        [(date(2026, 1, 30), 92.77), (date(2026, 2, 2), 188.41)],
    )
    ads = rows[date(2026, 1, 30)]
    ords = rows[date(2026, 2, 2)]
    assert ads["shares"] == 3_098_000_000          # 1.549B / 0.5 ADS-equivalents
    assert ords["shares"] == 1_549_000_000         # direct ordinary shares
    assert ads["mcap"] == pytest.approx(3_098_000_000 * 92.77)
    assert ords["mcap"] == pytest.approx(1_549_000_000 * 188.41)
    # Both sides of the switch describe the same company value (~$287-292B),
    # not a 2x step.
    assert ords["mcap"] / ads["mcap"] == pytest.approx(1.0, rel=0.05)


def test_puller_sny_market_cap_is_ads_equivalent(monkeypatch):
    rows = _run_puller(
        monkeypatch, "SNY", 1_219_427_096, [(date(2026, 9, 25), 41.11)],
    )
    row = rows[date(2026, 9, 25)]
    assert row["shares"] == 2_438_854_192
    # ~$100B (public references ~$99-100B), not the old ~$25B.
    assert row["mcap"] == pytest.approx(2_438_854_192 * 41.11)
    assert 90e9 < row["mcap"] < 110e9


def test_puller_gsk_and_shel_are_ads_equivalent(monkeypatch):
    # Latest XBRL ordinary counts (20-F covers) and YF closes on 2026-09-25.
    gsk = _run_puller(
        monkeypatch, "GSK", 4_315_445_026, [(date(2026, 9, 25), 49.24)],
    )[date(2026, 9, 25)]
    assert gsk["shares"] == 2_157_722_513
    assert 95e9 < gsk["mcap"] < 115e9          # ~$106B, not the old ~$212B
    shel = _run_puller(
        monkeypatch, "SHEL", 5_689_891_670, [(date(2026, 9, 25), 95.78)],
    )[date(2026, 9, 25)]
    assert shel["shares"] == 2_844_945_835
    assert 250e9 < shel["mcap"] < 295e9        # ~$272B, not the old ~$545B


def test_puller_tm_is_continuous_across_2021_split(monkeypatch):
    # Pre-split ordinary count x ratio 2 on 09-30 and post-split count x 10 on
    # 10-01 must describe the same company (YF:TM:close 177.75 -> 177.62).
    written: dict[date, dict] = {}
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", lambda: {"TM": "0001094517"})
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _cik: {"facts": {}})
    monkeypatch.setattr(
        mod, "_extract_shares_entries",
        lambda _f: [(date(2021, 6, 24), 2_795_948_660),
                    (date(2021, 10, 1), 13_979_743_300)],
    )
    monkeypatch.setattr(
        mod, "_fetch_close_prices",
        lambda *_a, **_k: [(date(2021, 9, 30), 177.75), (date(2021, 10, 1), 177.62)],
    )
    monkeypatch.setattr(
        mod, "_write_rows",
        lambda _e, _t, rows, **_controls: written.update({r["obs_date"]: r for r in rows}) or len(rows),
    )
    mod.SECXBRLSharesPuller(_FakeEngine()).pull_all(tickers=["TM"], backfill_days=4000)
    pre, post = written[date(2021, 9, 30)], written[date(2021, 10, 1)]
    assert pre["shares"] == 1_397_974_330      # 2.796B / 2
    assert post["shares"] == 1_397_974_330     # 13.98B / 10
    assert post["mcap"] / pre["mcap"] == pytest.approx(1.0, rel=0.01)


# ── XBRL extraction: a filing's current count is its latest period end ──

def _shares_fact(val, filed, end=None, form="20-F"):
    e = {"val": val, "filed": filed, "form": form}
    if end is not None:
        e["end"] = end
    return e


def test_extract_prefers_latest_period_end_within_one_filing():
    # Shape of SONY's 20-F filed 2025-06-20: the same tag is reported for
    # four fiscal year-ends, listed oldest first. The 2025-03-31 post-split
    # count is the filing's current count, not the 2022-03-31 one.
    facts = {"facts": {"ifrs-full": {"NumberOfSharesOutstanding": {"units": {"shares": [
        _shares_fact(1_261_081_781, "2025-06-20", "2022-03-31"),
        _shares_fact(1_261_081_781, "2025-06-20", "2023-03-31"),
        _shares_fact(1_261_231_889, "2025-06-20", "2024-03-31"),
        _shares_fact(6_149_810_645, "2025-06-20", "2025-03-31"),
        _shares_fact(1_261_231_889, "2024-06-25", "2024-03-31"),
        _shares_fact(1_261_081_781, "2024-06-25", "2023-03-31"),
    ]}}}}}
    assert mod._extract_shares_entries(facts) == [
        (date(2024, 6, 25), 1_261_231_889),
        (date(2025, 6, 20), 6_149_810_645),
    ]


def test_extract_latest_end_does_not_override_tag_priority():
    # A higher-priority tag wins even when a lower-priority tag has a later end.
    facts = {"facts": {
        "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
            _shares_fact(4_315_445_026, "2026-03-06", "2025-12-31"),
        ]}}},
        "ifrs-full": {"WeightedAverageShares": {"units": {"shares": [
            _shares_fact(4_000_000_000, "2026-03-06", "2026-02-28"),
        ]}}},
    }}
    assert mod._extract_shares_entries(facts) == [(date(2026, 3, 6), 4_315_445_026)]


def test_extract_same_tag_without_end_keeps_first():
    facts = {"facts": {"dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [
        _shares_fact(100, "2026-01-02"),
        _shares_fact(200, "2026-01-02"),
    ]}}}}}
    assert mod._extract_shares_entries(facts) == [(date(2026, 1, 2), 100)]

# Transaction/accounting integration: memory engine, no native database.
class _AccountingEngine:
    def __init__(self, *, fail_at=None, commit_error=False, counts=(1, 1, 1)):
        self.fail_at, self.commit_error, self.counts = fail_at, commit_error, counts
        self.calls = self.begins = self.commits = self.rollbacks = 0
        self.persisted = []

    def begin(self):
        self.begins += 1
        self.staged = []
        return self

    def __enter__(self):
        return self

    def execute(self, _sql, params):
        from types import SimpleNamespace
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("statement failure")
        self.staged.append(dict(params))
        return SimpleNamespace(rowcount=self.counts[self.calls - 1])

    def __exit__(self, typ, _value, _trace):
        if typ is not None:
            self.rollbacks += 1
            self.staged.clear()
        elif self.commit_error:
            self.rollbacks += 1
            raise RuntimeError("commit failure")
        else:
            self.commits += 1
            self.persisted.extend(self.staged)
        return False


def _accounting_rows(n=3):
    return [{"obs_date": date(2026, 9, i + 1), "shares": 10, "close": 2, "mcap": 20}
            for i in range(n)]


def test_accounting_writer_returns_after_commit():
    engine = _AccountingEngine()
    assert mod._write_rows(engine, "A", _accounting_rows()) == 3
    assert engine.commits == 1 and len(engine.persisted) == 3


def test_accounting_statement_failure_rolls_back_and_stops():
    engine = _AccountingEngine(fail_at=2)
    with pytest.raises(RuntimeError):
        mod._write_rows(engine, "A", _accounting_rows())
    assert engine.calls == 2 and engine.rollbacks == 1 and engine.commits == 0
    assert engine.persisted == []


def test_accounting_commit_failure_never_returns_staged_count():
    engine = _AccountingEngine(commit_error=True)
    with pytest.raises(RuntimeError):
        mod._write_rows(engine, "A", _accounting_rows(2))
    assert engine.calls == 2 and engine.commits == 0 and engine.persisted == []


def test_accounting_prior_ticker_commit_survives_later_failure():
    engine = _AccountingEngine()
    assert mod._write_rows(engine, "A", _accounting_rows(1)) == 1
    engine.fail_at = 3
    with pytest.raises(RuntimeError):
        mod._write_rows(engine, "B", _accounting_rows(3))
    assert len(engine.persisted) == 1 and engine.persisted[0]["ticker"] == "A"


@pytest.mark.parametrize("count", [None, -1, True, 2])
def test_accounting_unknown_driver_count_not_statement_length(count):
    engine = _AccountingEngine(counts=(count,))
    assert mod._write_rows(engine, "A", _accounting_rows(1)) is None
    assert engine.commits == 1


def test_accounting_zero_affected_rows_are_not_staged_rows():
    engine = _AccountingEngine(counts=(0, 1))
    assert mod._write_rows(engine, "A", _accounting_rows(2)) == 1


def test_accounting_pit_empty_does_not_open_transaction():
    engine = _AccountingEngine()
    assert mod._write_rows(engine, "A", []) == 0
    assert engine.begins == 0


def test_accounting_cancel_during_rows_rolls_back():
    engine = _AccountingEngine()
    decisions = iter((True, False))
    with pytest.raises(mod._MaterializationDeferred):
        mod._write_rows(engine, "A", _accounting_rows(), should_continue=lambda: next(decisions))
    assert engine.calls == 1 and engine.rollbacks == 1 and engine.commits == 0


def test_accounting_cancel_before_commit_rolls_back():
    engine = _AccountingEngine()
    decisions = iter((True, True, False))
    with pytest.raises(mod._MaterializationDeferred):
        mod._write_rows(engine, "A", _accounting_rows(2), should_continue=lambda: next(decisions))
    assert engine.calls == 2 and engine.rollbacks == 1 and engine.commits == 0


def test_accounting_exact_deadline_prevents_first_statement(monkeypatch):
    monkeypatch.setattr(mod.time, "monotonic", lambda: 100)
    engine = _AccountingEngine()
    with pytest.raises(mod._MaterializationDeferred):
        mod._write_rows(engine, "A", _accounting_rows(), deadline_at=100)
    assert engine.calls == 0 and engine.commits == 0


def _setup_accounting_puller(monkeypatch):
    monkeypatch.setattr(mod.time, "monotonic", lambda: 100)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", lambda: {"A": "1", "B": "2"})
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _: {})
    monkeypatch.setattr(mod, "_extract_shares_entries", lambda _: [(date(2026, 1, 1), 10)])
    monkeypatch.setattr(mod, "_fetch_close_prices", lambda *_: [(date(2026, 9, 1), 2)])
    monkeypatch.setattr(mod, "_write_rows", lambda *_args, **_kw: 1)
    return mod.SECXBRLSharesPuller(_FakeEngine())


def test_accounting_positive_prefix_limit_is_partial(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    out = _setup_accounting_puller(monkeypatch).pull_all(tickers=["A", "B"], limit=1)
    assert out[-1]["unattempted"] == 1
    assert _classify_outcome(out)[:2] == ("PARTIAL", 1)


def test_accounting_pit_empty_is_clean_not_failure(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    puller = _setup_accounting_puller(monkeypatch)
    monkeypatch.setattr(mod, "_shares_for_date", lambda *_: None)
    monkeypatch.setattr(mod, "_write_rows", lambda *_args, **_kw: 0)
    out = puller.pull_all(tickers=["A"])
    assert out[0]["reason"] == "pit_empty"
    assert _classify_outcome(out)[:2] == ("NO_NEW_DATA", 0)


def test_accounting_close_error_not_clean_empty(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    puller = _setup_accounting_puller(monkeypatch)
    def failure(*_):
        raise RuntimeError("lookup failed")
    monkeypatch.setattr(mod, "_fetch_close_prices", failure)
    out = puller.pull_all(tickers=["A"])
    assert out[0]["status"] == "FAILED" and out[0]["rows"] == 0
    assert _classify_outcome(out)[:2] == ("FAILED", 0)


def test_accounting_write_error_keeps_prior_subtotal_unknown_total(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome, _extract_rows
    puller = _setup_accounting_puller(monkeypatch)
    def write(_engine, ticker, _rows, **_kw):
        if ticker == "B":
            raise RuntimeError("commit acknowledgement lost")
        return 1
    monkeypatch.setattr(mod, "_write_rows", write)
    out = puller.pull_all(tickers=["A", "B"])
    assert out[-1]["known_committed_rows"] == 1 and out[-1]["rows_written"] is None
    assert _classify_outcome(out)[:2] == ("PARTIAL", None)
    assert _extract_rows(out) is None


def test_accounting_prestart_cancel_has_no_fetch(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    puller = _setup_accounting_puller(monkeypatch)
    def forbidden():
        raise AssertionError("no fetch after cancellation")
    monkeypatch.setattr(mod, "_fetch_ticker_to_cik_map", forbidden)
    out = puller.pull_all(tickers=["A", "B"], should_continue=lambda: False)
    assert out[-1]["attempted"] == 0 and out[-1]["unattempted"] == 2
    assert _classify_outcome(out)[:2] == ("PARTIAL", 0)


def test_accounting_deadline_expiry_after_fact_stops_no_writer(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    puller = _setup_accounting_puller(monkeypatch)
    now = [100]
    monkeypatch.setattr(mod.time, "monotonic", lambda: now[0])
    def facts(_):
        now[0] = 101
        return {}
    monkeypatch.setattr(mod, "_fetch_company_facts", facts)
    def forbidden(*_args, **_kw):
        raise AssertionError("writer after deadline")
    monkeypatch.setattr(mod, "_write_rows", forbidden)
    out = puller.pull_all(tickers=["A", "B"], deadline_at=101)
    assert _classify_outcome(out)[:2] == ("PARTIAL", 0)
    assert out[-1]["unknown"] == 1 and out[-1]["unattempted"] == 1


def test_accounting_missing_cik_or_facts_are_not_clean_empty(monkeypatch):
    from ingestion.smart_scheduler import _classify_outcome
    puller = _setup_accounting_puller(monkeypatch)
    monkeypatch.setattr(mod, "_fetch_company_facts", lambda _: None)
    assert _classify_outcome(puller.pull_all(tickers=["A"]))[:2] == ("FAILED", 0)
    assert _classify_outcome(puller.pull_all(tickers=["UNKNOWN"]))[:2] == ("FAILED", 0)


def test_accounting_close_query_failure_propagates():
    class Engine:
        def connect(self):
            raise RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        mod._fetch_close_prices(Engine(), "A", date(2026, 1, 1), date(2026, 2, 1))
